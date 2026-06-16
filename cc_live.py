"""cc_live — rendering live di un turno SDK su Telegram.

Glue tra i callback di `cc_sdk.run_turn` e l'I/O Telegram di `bot.py`.
Tiene isolata la logica di: streaming testo (edit throttlato del messaggio),
display tool/thinking/todo, bottone Stop, approvazione interattiva (ask-mode).

`bot.py` passa un namespace `tg` con le sue funzioni I/O (dependency injection,
niente import circolari) e gestisce i callback dei bottoni chiamando
`resolve_permission()` / `request_stop()` di questo modulo.
"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from typing import Any

import cc_sdk

MIN_EDIT_INTERVAL = 1.2     # s minimi tra due edit del messaggio live (rate limit TG)
LIVE_TAIL = 3500            # char max mostrati nel messaggio live (coda del testo)
PERM_TIMEOUT = 300          # s di attesa approvazione prima del deny implicito

# Registry condivisi con bot.handle_callback ------------------------------------
_lock = threading.Lock()
_PENDING_PERM: dict[str, dict] = {}   # token -> {event, allowed, msg_id, chat_id}
_CANCELS: dict[str, threading.Event] = {}  # turn_token -> Event
_counter = 0


def _new_token(prefix: str) -> str:
    global _counter
    with _lock:
        _counter += 1
        return f"{prefix}{_counter}_{int(time.time())}"


def resolve_permission(token: str, allowed: bool) -> bool:
    """Chiamata da bot.handle_callback su tap Permetti/Nega. True se gestito."""
    with _lock:
        entry = _PENDING_PERM.get(token)
    if not entry:
        return False
    entry["allowed"] = allowed
    entry["event"].set()
    return True


def request_stop(turn_token: str) -> bool:
    """Chiamata da bot.handle_callback su tap Stop. True se gestito."""
    with _lock:
        ev = _CANCELS.get(turn_token)
    if ev is None:
        return False
    ev.set()
    return True


def _fmt_todos(items: list) -> str:
    if not items:
        return ""
    sym = {"completed": "✅", "in_progress": "▶️", "pending": "◻️"}
    lines = []
    for it in items:
        st = (it.get("status") or "pending")
        txt = it.get("content") or it.get("activeForm") or ""
        lines.append(f"{sym.get(st, '◻️')} {txt}")
    return "📋 Todo:\n" + "\n".join(lines)


def run_live_turn(
    tg: SimpleNamespace,
    *,
    prompt: str,
    session_id: str | None,
    sdk_mode: str,
    cwd: str,
    model: str | None,
    effort: str | None,
    fork: bool,
    chat_id: int,
    thread_id: int | None,
    ask: bool,
    images: "list[str] | None" = None,
    timeout_s: int = 1800,
) -> tuple[str, str | None, dict]:
    """Esegue un turno SDK con rendering live su Telegram.

    `tg` deve esporre: send, send_with_keyboard, edit_message_with_keyboard,
    delete_message, answer_callback (firme come in bot.py).
    `ask=True` attiva l'approvazione interattiva (sdk_mode deve essere 'default').
    Ritorna (reply_finale, new_session_id, meta).
    """
    turn_token = _new_token("t")
    cancel_event = threading.Event()
    with _lock:
        _CANCELS[turn_token] = cancel_event

    stop_kb = [[{"text": "⏹ Stop", "callback_data": f"stop:{turn_token}"}]]
    live_id = tg.send_with_keyboard(chat_id, "💭 avvio…", stop_kb, thread_id=thread_id)

    state = {
        "header": "💭 ragionamento…",
        "body": [],          # delta testo
        "thinking": [],      # delta thinking (mostrati solo prima del testo)
        "todos": "",
        "last_edit": 0.0,
        "last_render": "",
    }

    def _render() -> str:
        body = "".join(state["body"])
        parts = [state["header"]]
        if state["todos"]:
            parts.append(state["todos"])
        if body.strip():
            tail = body[-LIVE_TAIL:]
            if len(body) > LIVE_TAIL:
                tail = "…" + tail
            parts.append(tail)
        elif state["thinking"]:
            think = "".join(state["thinking"])[-600:]
            parts.append("🧠 " + think)
        return "\n\n".join(p for p in parts if p)

    def _flush(force: bool = False) -> None:
        if live_id is None:
            return
        now = time.time()
        if not force and (now - state["last_edit"]) < MIN_EDIT_INTERVAL:
            return
        text = _render()
        if text == state["last_render"]:
            return
        state["last_edit"] = now
        state["last_render"] = text
        tg.edit_message_with_keyboard(chat_id, live_id, text, stop_kb)

    # --- callbacks cc_sdk ---
    def on_text(delta: str) -> None:
        state["body"].append(delta)
        _flush()

    def on_thinking(delta: str) -> None:
        state["thinking"].append(delta)
        if not state["body"]:
            state["header"] = "🧠 sto ragionando…"
            _flush()

    def on_status(label: str) -> None:
        state["header"] = label
        _flush(force=True)

    def on_todos(items: list) -> None:
        state["todos"] = _fmt_todos(items)
        _flush(force=True)

    def on_plan(plan_text: str) -> None:
        state["header"] = "📐 piano proposto"
        state["body"].append("\n\n" + (plan_text or ""))
        _flush(force=True)

    # --- approvazione interattiva ---
    def ask_permission(name: str, tool_input: dict, summary: str) -> bool:
        token = _new_token("p")
        ev = threading.Event()
        kb = [[
            {"text": "✅ Permetti", "callback_data": f"perm:{token}:allow"},
            {"text": "⛔ Nega", "callback_data": f"perm:{token}:deny"},
        ]]
        body = f"🔐 Permesso richiesto\n🔧 {name}"
        if summary:
            body += f"\n{summary[:300]}"
        msg_id = tg.send_with_keyboard(chat_id, body, kb, thread_id=thread_id)
        with _lock:
            _PENDING_PERM[token] = {"event": ev, "allowed": None, "msg_id": msg_id, "chat_id": chat_id}
        got = ev.wait(PERM_TIMEOUT)
        with _lock:
            entry = _PENDING_PERM.pop(token, None)
        allowed = bool(entry and entry.get("allowed"))
        # aggiorna il messaggio di richiesta con l'esito (rimuove i bottoni)
        if msg_id is not None:
            verdict = "✅ permesso" if allowed else ("⛔ negato" if got else "⏱ timeout → negato")
            try:
                tg.edit_message_with_keyboard(chat_id, msg_id, f"{body}\n\n{verdict}", None)
            except Exception:
                pass
        return allowed

    try:
        reply, new_sid, meta = cc_sdk.run_turn(
            prompt, session_id, sdk_mode, cwd, model, effort, fork,
            on_status=on_status,
            on_text=on_text,
            on_thinking=on_thinking,
            on_todos=on_todos,
            on_plan=on_plan,
            ask_permission=(ask_permission if ask else None),
            images=images,
            cancel_event=cancel_event,
            timeout_s=timeout_s,
        )
    finally:
        with _lock:
            _CANCELS.pop(turn_token, None)
        if live_id is not None:
            tg.delete_message(chat_id, live_id)

    if cancel_event.is_set() and not (reply or "").strip():
        reply = "⏹ interrotto"
    tg.send(chat_id, reply or "(vuoto)", thread_id=thread_id)
    return reply, new_sid, meta
