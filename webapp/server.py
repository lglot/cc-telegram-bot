"""cc-webapp — Mini App Telegram per cc-telegram-bot.

Secondo client (oltre al bot in polling) sullo stesso cervello: riusa
`cc_sdk.run_turn` per guidare una sessione Claude Code e ne fa streaming
verso una SPA via SSE. Auth = validazione `initData` Telegram (HMAC col
bot token) + whitelist user.id (stesso ALLOW del bot).

Processo SEPARATO da bot.py (systemd unit `cc-webapp.service`): non condivide
il loop di polling né i restart-su-mtime del bot.

Env (ereditate da .env del bot via run-webapp.sh):
  TG_TOKEN            bot token (per validare initData)
  TG_ALLOW_CHAT_IDS   csv user_id ammessi
  CC_MODEL            model id (default claude-opus-4-8)
  WEBAPP_HOST         bind host (default 0.0.0.0; public IP Hetzner firewallato)
  WEBAPP_PORT         bind port (default 8099)
  WEBAPP_CWD          cwd sessioni webapp (default ~)
  WEBAPP_STATE        state file (default ~/.cc-webapp.state.json)
  WEBAPP_MODE         permission_mode (default bypassPermissions)
  WEBAPP_INITDATA_TTL secondi validità initData (default 86400)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import sys
import threading
import time
from pathlib import Path
from urllib.parse import parse_qsl

# Repo root nel path: `import cc_sdk` deve risolvere a prescindere dal cwd/launcher.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

import cc_sdk

# ---- config ----------------------------------------------------------------
TOKEN = os.environ["TG_TOKEN"]
ALLOW = {int(x) for x in os.environ.get("TG_ALLOW_CHAT_IDS", "").split(",") if x.strip()}
MODEL = os.environ.get("CC_MODEL", "claude-opus-4-8")
HOST = os.environ.get("WEBAPP_HOST", "0.0.0.0")
PORT = int(os.environ.get("WEBAPP_PORT", "8099"))
CWD = os.path.expanduser(os.environ.get("WEBAPP_CWD", "~"))
MODE = os.environ.get("WEBAPP_MODE", "bypassPermissions")
STATE_FILE = Path(os.path.expanduser(os.environ.get("WEBAPP_STATE", "~/.cc-webapp.state.json")))
INITDATA_TTL = int(os.environ.get("WEBAPP_INITDATA_TTL", "86400"))
TIMEOUT = int(os.environ.get("CC_TIMEOUT", "1800"))
STATIC_DIR = Path(__file__).parent / "static"

# ---- session store (separato da quello del bot) ----------------------------
_state_lock = threading.Lock()


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_state(state: dict) -> None:
    with _state_lock:
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state))
        tmp.replace(STATE_FILE)


def _get_session(user_id: int) -> str | None:
    return _load_state().get(str(user_id), {}).get("session_id")


def _set_session(user_id: int, session_id: str | None) -> None:
    state = _load_state()
    state.setdefault(str(user_id), {})["session_id"] = session_id
    _save_state(state)


# ---- auth: validazione initData Telegram -----------------------------------
def verify_init_data(init_data: str) -> dict | None:
    """Valida la query-string `initData` di una Mini App.

    Algoritmo: secret_key = HMAC_SHA256(key='WebAppData', msg=bot_token);
    hash atteso = HMAC_SHA256(key=secret_key, msg=data_check_string).
    Ritorna il dict dei campi (con `user` già deserializzato) se valido e
    user.id ∈ ALLOW, altrimenti None.
    """
    if not init_data:
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    except Exception:
        return None
    recv_hash = pairs.pop("hash", None)
    if not recv_hash:
        return None
    data_check = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    secret_key = hmac.new(b"WebAppData", TOKEN.encode(), hashlib.sha256).digest()
    calc_hash = hmac.new(secret_key, data_check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc_hash, recv_hash):
        return None
    # freschezza
    try:
        auth_date = int(pairs.get("auth_date", "0"))
    except ValueError:
        auth_date = 0
    if INITDATA_TTL > 0 and auth_date and (time.time() - auth_date) > INITDATA_TTL:
        return None
    user = {}
    if pairs.get("user"):
        try:
            user = json.loads(pairs["user"])
        except Exception:
            user = {}
    uid = user.get("id")
    if uid is None or int(uid) not in ALLOW:
        return None
    pairs["user"] = user
    return pairs


def _auth(request: Request, body: dict | None = None) -> dict | None:
    init_data = request.headers.get("x-init-data") or (body or {}).get("initData") or ""
    return verify_init_data(init_data)


# ---- endpoints -------------------------------------------------------------
async def index(request: Request):
    return PlainTextResponse((STATIC_DIR / "index.html").read_text(),
                             media_type="text/html; charset=utf-8")


async def whoami(request: Request):
    info = _auth(request)
    if not info:
        return JSONResponse({"ok": False}, status_code=401)
    u = info["user"]
    return JSONResponse({
        "ok": True,
        "user": {"id": u.get("id"), "first_name": u.get("first_name", ""),
                 "username": u.get("username", "")},
        "has_session": bool(_get_session(int(u["id"]))),
        "model": MODEL,
    })


async def new_session(request: Request):
    body = await request.json()
    info = _auth(request, body)
    if not info:
        return JSONResponse({"ok": False}, status_code=401)
    _set_session(int(info["user"]["id"]), None)
    return JSONResponse({"ok": True})


async def chat(request: Request):
    body = await request.json()
    info = _auth(request, body)
    if not info:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    user_id = int(info["user"]["id"])
    prompt = (body.get("prompt") or "").strip()
    location = body.get("location")  # {latitude, longitude, ...} | None
    if location and isinstance(location, dict):
        lat, lon = location.get("latitude"), location.get("longitude")
        if lat is not None and lon is not None:
            acc = location.get("accuracy")
            ctx = f"[posizione utente: lat={lat}, lon={lon}"
            ctx += f", accuratezza≈{round(acc)}m]" if acc else "]"
            prompt = f"{ctx}\n\n{prompt}" if prompt else ctx
    if not prompt:
        return JSONResponse({"ok": False, "error": "empty"}, status_code=400)

    session_id = _get_session(user_id)
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()
    cancel = threading.Event()

    def emit(ev: dict) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, ev)

    def worker() -> None:
        try:
            text, new_sid, meta = cc_sdk.run_turn(
                prompt, session_id, MODE, CWD, MODEL,
                on_status=lambda s: emit({"type": "status", "text": s}),
                on_text=lambda c: emit({"type": "text", "chunk": c}),
                on_thinking=lambda c: emit({"type": "thinking", "chunk": c}),
                on_tool=lambda t: emit({"type": "tool", "name": t.get("name"),
                                        "summary": cc_sdk._summarize_input(t.get("name", ""), t.get("input") or {})}),
                on_tool_result=lambda r: emit({"type": "tool_result", "name": r.get("name"),
                                               "is_error": r.get("is_error")}),
                on_todos=lambda todos: emit({"type": "todos", "todos": todos}),
                cancel_event=cancel,
                timeout_s=TIMEOUT,
            )
            _set_session(user_id, new_sid)
            emit({"type": "done", "text": text, "session_id": new_sid, "meta": meta})
        except Exception as e:  # noqa: BLE001
            emit({"type": "error", "text": f"errore: {e}"})
        finally:
            emit({"type": "_end"})

    threading.Thread(target=worker, daemon=True).start()

    async def event_source():
        try:
            while True:
                ev = await queue.get()
                if ev.get("type") == "_end":
                    break
                yield {"data": json.dumps(ev, ensure_ascii=False)}
        except asyncio.CancelledError:
            cancel.set()  # client disconnesso → ferma il turno
            raise

    return EventSourceResponse(event_source())


routes = [
    Route("/", index),
    Route("/api/whoami", whoami, methods=["GET"]),
    Route("/api/new", new_session, methods=["POST"]),
    Route("/api/chat", chat, methods=["POST"]),
    Mount("/static", app=StaticFiles(directory=str(STATIC_DIR)), name="static"),
]

app = Starlette(routes=routes)


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
