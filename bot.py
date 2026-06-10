#!/usr/bin/env python3
"""
cc-telegram-bot — Bridge Telegram → Claude Code headless.

Long-polling bot. Per ogni messaggio da utente whitelisted:
  - exec `claude -p "<msg>" --resume <session_id>` (stato per chat/topic)
  - rimanda output a Telegram (split a 4096 char)
  - persiste session_id su disco per continuita multi-turno

Modalita thread (forum supergroup):
  - se il bot e in un supergruppo con i Topics abilitati, ogni topic e una
    sessione separata. `message_thread_id` -> stato indipendente (cwd, session).
  - `/sync` (o il primo messaggio nel gruppo) crea un topic per ogni progetto
    Claude Code presente sul Mac (dir in ~/.claude/projects con cwd valido).
  - scrivere in un topic riprende la sessione bound a quel progetto.
  La chat privata 1:1 resta invariata (nessun thread_id).

Config via env:
  TG_TOKEN           bot token (obbligatorio)
  TG_ALLOW_CHAT_IDS  csv di chat_id/user_id ammessi (obbligatorio)
  CC_CWD             working dir per claude (default ~)
  CC_MODEL           model id opzionale (es. claude-opus-4-7)
  CC_TIMEOUT         timeout subprocess in s (default 3600)
  CC_HEARTBEAT       intervallo update progresso in s (default 120)
  STATE_FILE         path stato sessioni (default ~/.cc-telegram-bot.state.json)
  CC_PROJECTS_DIR    dir sessioni CC (default ~/.claude/projects)
  SYNC_EXCLUDE       csv di substring dir da escludere dal sync topic
  MAX_TOPICS         max topic creati per /sync (default 40)
"""
import atexit
import concurrent.futures
import fcntl
import glob
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

TOKEN = os.environ["TG_TOKEN"]
ALLOW = {int(x) for x in os.environ["TG_ALLOW_CHAT_IDS"].split(",") if x.strip()}
CWD = os.path.expanduser(os.environ.get("CC_CWD", "~"))
MODEL = os.environ.get("CC_MODEL", "")
TIMEOUT = int(os.environ.get("CC_TIMEOUT", "3600"))
HEARTBEAT = int(os.environ.get("CC_HEARTBEAT", "120"))  # s tra update "sto lavorando"
STATE_FILE = Path(os.path.expanduser(os.environ.get("STATE_FILE", "~/.cc-telegram-bot.state.json")))
DEFAULT_MODE = os.environ.get("CC_DEFAULT_MODE", "bypassPermissions")  # plan|acceptEdits|bypassPermissions
VALID_MODES = {"plan", "acceptEdits", "bypassPermissions"}
DEFAULT_EFFORT = os.environ.get("CC_DEFAULT_EFFORT", "")  # vuoto = default CC
VALID_EFFORTS = {"low", "medium", "high", "xhigh", "max"}
EFFORT_ORDER = ["low", "medium", "high", "xhigh", "max"]
MODEL_PRESETS = [
    ("Fable 5", "claude-fable-5"),
    ("Opus 4.8", "claude-opus-4-8"),
    ("Opus 4.7", "claude-opus-4-7"),
    ("Sonnet 4.6", "claude-sonnet-4-6"),
]
API = f"https://api.telegram.org/bot{TOKEN}"
TG_LIMIT = 4000  # leave room for markup overhead

PROJECTS_DIR = Path(os.path.expanduser(os.environ.get("CC_PROJECTS_DIR", "~/.claude/projects")))
SYNC_EXCLUDE = [
    x.strip() for x in os.environ.get(
        "SYNC_EXCLUDE", "claude-mem-observer,CodexBar-ClaudeProbe"
    ).split(",") if x.strip()
]
MAX_TOPICS = int(os.environ.get("MAX_TOPICS", "40"))
# colori icona topic ammessi da Telegram (createForumTopic icon_color)
TOPIC_COLORS = [7322096, 16766590, 13338331, 9367192, 16749490, 16478047]

# Coda publish: file json depositati da `claude-publish` (Mac, via SSH).
PUBLISH_DIR = Path(os.path.expanduser(os.environ.get("PUBLISH_DIR", "~/.cc-telegram-bot.publish")))
# Manifest Syncthing (sintassi .stignore): sessioni da sincronizzare Mac<->lgcloud.
MANIFEST = PROJECTS_DIR / "shared-includes"
# Path remap: il bot deve lanciare claude con cwd canonico (/Users/...) anche su
# lgcloud (/home/luigi), così l'encoded dir ~/.claude/projects/<enc> combacia col Mac
# e le sessioni sono portabili. Su lgcloud: CC_REMAP_FROM=/home/luigi CC_REMAP_TO=/Users/luigilotito
REMAP_FROM = os.environ.get("CC_REMAP_FROM", "")
REMAP_TO = os.environ.get("CC_REMAP_TO", "")
# Long-poll piu corto se c'e' la coda publish, così i publish vengono raccolti in fretta.
POLL_TIMEOUT = int(os.environ.get("CC_POLL_TIMEOUT", "20"))

LOCK_FILE = Path(os.path.expanduser("~/.cc-telegram-bot.lock"))

BOT_PY_PATH = Path(__file__).resolve()
RUN_SH_PATH = BOT_PY_PATH.parent / "run.sh"
INITIAL_MTIME = BOT_PY_PATH.stat().st_mtime

# Concorrenza: ogni messaggio gira su un thread separato per non bloccare il polling.
# _active_skeys serializza richieste sullo stesso topic/chat (stessa sessione Claude).
_state_lock = threading.Lock()      # save_state atomica
_active_skeys: set = set()
_active_lock = threading.Lock()
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=20, thread_name_prefix="cc-worker")


def acquire_singleton_lock() -> None:
    """Garantisce una sola istanza del bot via flock esclusivo non-bloccante.

    Senza questo lock, due istanze fanno polling sullo stesso bot token e
    si rubano gli update a vicenda — utente vede risposte duplicate o
    sparizione di messaggi a seconda di chi vince la race.
    """
    fp = open(LOCK_FILE, "w")
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.stderr.write(f"❌ altro bot.py già in esecuzione (lock {LOCK_FILE}). Esco.\n")
        sys.exit(1)
    fp.write(str(os.getpid()))
    fp.flush()
    # tieni il file descriptor aperto per l'intera vita del processo
    globals()["_LOCK_FP"] = fp
    atexit.register(lambda: (fp.close(), LOCK_FILE.unlink(missing_ok=True)))

BOT_COMMANDS = [
    {"command": "help", "description": "Mostra comandi disponibili"},
    {"command": "new", "description": "Resetta sessione Claude"},
    {"command": "status", "description": "Info sessione corrente"},
    {"command": "usage", "description": "% utilizzo piano CC (5h/7g/sonnet/opus)"},
    {"command": "compact", "description": "Compatta sessione in riassunto"},
    {"command": "mode", "description": "Permission mode (default bypassPermissions)"},
    {"command": "effort", "description": "Effort level (low|medium|high|xhigh|max)"},
    {"command": "cwd", "description": "Mostra/cambia working directory"},
    {"command": "model", "description": "Mostra/cambia model id"},
    {"command": "caveman", "description": "Toggle stile caveman (on|off)"},
    {"command": "handoff", "description": "Handoff → nuova sessione in nuovo topic"},
    {"command": "sync", "description": "Crea/aggiorna i thread delle sessioni CC (forum)"},
    {"command": "threads", "description": "Lista thread sessione mappati (forum)"},
]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_state(state: dict) -> None:
    with _state_lock:
        STATE_FILE.write_text(json.dumps(state, indent=2))


def maybe_self_restart() -> None:
    """Se bot.py o run.sh sono stati modificati dopo l'avvio, ri-exec il processo.

    Da chiamare solo dopo che l'offset Telegram è stato persistito e il turno
    corrente è completato (ack inviato + reply consegnato), così non c'è loop
    di replay. Valida la sintassi prima di reload per evitare crash loop.
    Con threading: aspetta che tutti i worker attivi abbiano finito prima di execv.
    """
    try:
        cur = BOT_PY_PATH.stat().st_mtime
    except FileNotFoundError:
        return
    if cur == INITIAL_MTIME:
        return
    # Se ci sono worker attivi il restart verrà fatto dall'ultimo che termina.
    with _active_lock:
        if _active_skeys:
            return
    try:
        import ast
        ast.parse(BOT_PY_PATH.read_text())
    except SyntaxError as e:
        log(f"self-restart skipped: bot.py syntax error: {e}")
        return
    log(f"bot.py modified, self-restart via exec({RUN_SH_PATH})")
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    os.execv(str(RUN_SH_PATH), [str(RUN_SH_PATH)])


def tg(method: str, **params) -> dict:
    data = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(f"{API}/{method}", data=data)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def tg_long(method: str, timeout: int = 50, **params) -> dict:
    data = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(f"{API}/{method}", data=data)
    with urllib.request.urlopen(req, timeout=timeout + 10) as r:
        return json.loads(r.read())


import html as _html
import re as _re


def md_to_tg_html(text: str) -> str:
    """Converte markdown CC in HTML compatibile Telegram.

    Telegram HTML supporta: <b> <i> <u> <s> <code> <pre> <a> <blockquote>.
    Strategia: estrae prima i blocchi code (per non escapare il loro contenuto come
    se fosse markdown), escapa il resto, poi reinserisce i blocchi escapati.
    """
    if not text:
        return ""
    placeholders: list[tuple[str, str]] = []

    def stash(s: str) -> str:
        key = f"\x00PH{len(placeholders)}\x00"
        placeholders.append((key, s))
        return key

    # blocchi code ```lang\n...\n```
    def repl_pre(m: _re.Match) -> str:
        body = m.group(2) or ""
        return stash(f"<pre>{_html.escape(body)}</pre>")

    text = _re.sub(r"```([a-zA-Z0-9_-]*)\n?(.*?)```", repl_pre, text, flags=_re.DOTALL)

    # inline code `...`
    def repl_code(m: _re.Match) -> str:
        return stash(f"<code>{_html.escape(m.group(1))}</code>")

    text = _re.sub(r"`([^`\n]+)`", repl_code, text)

    # link [testo](url)
    def repl_link(m: _re.Match) -> str:
        label, url = m.group(1), m.group(2)
        return stash(f'<a href="{_html.escape(url, quote=True)}">{_html.escape(label)}</a>')

    text = _re.sub(r"\[([^\]]+)\]\(([^)]+)\)", repl_link, text)

    # escape il resto
    text = _html.escape(text)

    # bold **x** o __x__
    text = _re.sub(r"\*\*([^*\n]+)\*\*", r"<b>\1</b>", text)
    text = _re.sub(r"__([^_\n]+)__", r"<b>\1</b>", text)
    # italic *x* o _x_ (solo se non confonde con **)
    text = _re.sub(r"(?<![*\w])\*([^*\n]+)\*(?!\w)", r"<i>\1</i>", text)
    text = _re.sub(r"(?<![_\w])_([^_\n]+)_(?!\w)", r"<i>\1</i>", text)
    # headers ### Foo → <b>Foo</b>
    text = _re.sub(r"^#{1,6}\s+(.+)$", r"<b>\1</b>", text, flags=_re.MULTILINE)
    # bullets "- " o "* " inizio riga → "• "
    text = _re.sub(r"^(\s*)[-*]\s+", r"\1• ", text, flags=_re.MULTILINE)
    # rule horizontal --- → linea
    text = _re.sub(r"^---+$", "─" * 20, text, flags=_re.MULTILINE)

    # ripristina placeholder
    for key, val in placeholders:
        text = text.replace(key, val)
    return text


def _split_safe(text: str, limit: int) -> list[str]:
    """Split text rispettando tag HTML (non taglia in mezzo a `<...>`) e preferendo
    confini su newline/spazi. Garantisce ogni chunk <= limit char."""
    out: list[str] = []
    while text:
        if len(text) <= limit:
            out.append(text)
            break
        # cerca ultima `>` chiusura tag prima del limit
        last_gt = text.rfind(">", 0, limit)
        # cerca ultima `<` apertura tag prima del limit
        last_lt = text.rfind("<", 0, limit)
        if last_lt > last_gt:
            # tag aperto sta attraversando boundary: split prima del `<`
            split = last_lt
        else:
            # nessun tag aperto, split su newline / spazio per leggibilità
            split = text.rfind("\n", 0, limit)
            if split <= 0:
                split = text.rfind(" ", 0, limit)
            if split <= 0:
                split = limit
        if split <= 0:
            split = limit
        out.append(text[:split])
        text = text[split:].lstrip("\n ")
    return out


def _strip_html(s: str) -> str:
    """Rimuove tag HTML mantenendo testo (per fallback plain text quando HTML rotto)."""
    s = _re.sub(r"<[^>]+>", "", s)
    # decodifica entità basic
    return s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')


def _thread_param(params: dict, thread_id: int | None) -> dict:
    """Aggiunge message_thread_id ai params solo se siamo in un topic forum."""
    if thread_id is not None:
        params["message_thread_id"] = thread_id
    return params


def send(chat_id: int, text: str, parse_mode: str = "HTML", thread_id: int | None = None) -> int | None:
    """Invia messaggio (split safe). Fallback plain text se HTML rotto."""
    if not text:
        return None
    last_id = None
    formatted = md_to_tg_html(text) if parse_mode == "HTML" else text
    for chunk in _split_safe(formatted, TG_LIMIT):
        try:
            params = {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": "true"}
            if parse_mode:
                params["parse_mode"] = parse_mode
            _thread_param(params, thread_id)
            r = tg("sendMessage", **params)
            if r.get("ok"):
                last_id = r["result"]["message_id"]
        except Exception as e:
            log(f"send err ({parse_mode}): {e}")
            # fallback: strip tag + re-invia plain text dello stesso chunk
            if parse_mode:
                plain = _strip_html(chunk)
                try:
                    params = {"chat_id": chat_id, "text": plain[:TG_LIMIT], "disable_web_page_preview": "true"}
                    _thread_param(params, thread_id)
                    r = tg("sendMessage", **params)
                    if r.get("ok"):
                        last_id = r["result"]["message_id"]
                except Exception as e2:
                    log(f"send fallback err: {e2}")
    return last_id


def send_status(chat_id: int, text: str, thread_id: int | None = None) -> int | None:
    """Invia messaggio plain text (per status updates editabili). Restituisce message_id."""
    try:
        params = {"chat_id": chat_id, "text": text[:TG_LIMIT], "disable_web_page_preview": "true"}
        _thread_param(params, thread_id)
        r = tg("sendMessage", **params)
        if r.get("ok"):
            return r["result"]["message_id"]
    except Exception as e:
        log(f"send_status err: {e}")
    return None


def edit_status(chat_id: int, message_id: int, text: str) -> None:
    try:
        tg("editMessageText", chat_id=chat_id, message_id=message_id, text=text[:TG_LIMIT], disable_web_page_preview="true")
    except Exception as e:
        # ignore "message is not modified" errors
        if "not modified" not in str(e):
            log(f"edit_status err: {e}")


def delete_message(chat_id: int, message_id: int) -> None:
    try:
        tg("deleteMessage", chat_id=chat_id, message_id=message_id)
    except Exception as e:
        log(f"delete_message err: {e}")


def send_with_keyboard(chat_id: int, text: str, keyboard: list[list[dict]], thread_id: int | None = None) -> int | None:
    """Invia messaggio con inline_keyboard. `keyboard` è array di righe di bottoni
    {text, callback_data}. Niente HTML parse_mode (i bottoni sostituiscono il formatting).
    """
    try:
        params = {
            "chat_id": chat_id,
            "text": text[:TG_LIMIT],
            "disable_web_page_preview": "true",
            "reply_markup": json.dumps({"inline_keyboard": keyboard}),
        }
        _thread_param(params, thread_id)
        r = tg("sendMessage", **params)
        if r.get("ok"):
            return r["result"]["message_id"]
    except Exception as e:
        log(f"send_with_keyboard err: {e}")
    return None


def edit_message_with_keyboard(chat_id: int, message_id: int, text: str, keyboard: list[list[dict]] | None) -> None:
    try:
        params = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text[:TG_LIMIT],
            "disable_web_page_preview": "true",
        }
        if keyboard is not None:
            params["reply_markup"] = json.dumps({"inline_keyboard": keyboard})
        tg("editMessageText", **params)
    except Exception as e:
        if "not modified" not in str(e):
            log(f"edit_message_with_keyboard err: {e}")


def answer_callback(callback_id: str, text: str = "", show_alert: bool = False) -> None:
    try:
        tg("answerCallbackQuery", callback_query_id=callback_id, text=text[:200], show_alert="true" if show_alert else "false")
    except Exception as e:
        log(f"answer_callback err: {e}")


def create_forum_topic(chat_id: int, name: str, icon_color: int | None = None) -> tuple[int | None, str]:
    """createForumTopic → (message_thread_id, err). Richiede bot admin con can_manage_topics."""
    try:
        params = {"chat_id": chat_id, "name": name[:128]}
        if icon_color is not None:
            params["icon_color"] = icon_color
        r = tg("createForumTopic", **params)
        if r.get("ok"):
            return r["result"]["message_thread_id"], ""
        return None, str(r.get("description") or "errore sconosciuto")
    except Exception as e:
        msg = str(e)
        # urllib HTTPError non espone il body json di default; prova a leggerlo
        body = getattr(e, "read", None)
        if callable(body):
            try:
                msg = json.loads(e.read()).get("description", msg)
            except Exception:
                pass
        return None, msg


def build_effort_keyboard(current: str | None) -> list[list[dict]]:
    row1 = []
    for level in EFFORT_ORDER:
        label = f"✓ {level}" if current == level else level
        row1.append({"text": label, "callback_data": f"effort:{level}"})
    reset_label = "✓ default" if not current else "default"
    row2 = [{"text": reset_label, "callback_data": "effort:reset"}]
    return [row1, row2]


def build_model_keyboard(current: str | None) -> list[list[dict]]:
    rows = []
    for label, mid in MODEL_PRESETS:
        prefix = "✓ " if current == mid else ""
        rows.append([{"text": f"{prefix}{label}", "callback_data": f"model:{mid}"}])
    reset_label = "✓ default" if not current else "default"
    rows.append([{"text": reset_label, "callback_data": "model:reset"}])
    return rows


def fetch_cc_usage() -> dict | None:
    """GET https://api.anthropic.com/api/oauth/usage usando OAuth token CC dal keychain.

    Replica fetchUtilization() del binario CC. Token estratto da keychain macOS
    item 'Claude Code-credentials'. Header obbligatorio anthropic-beta=oauth-2025-04-20.
    """
    token = None
    # 1) macOS keychain (se security disponibile)
    try:
        r = subprocess.run(
            ["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0:
            creds = json.loads(r.stdout.strip())
            token = creds["claudeAiOauth"]["accessToken"]
    except FileNotFoundError:
        pass  # `security` non esiste (Linux), tentiamo fallback
    except Exception as e:
        log(f"keychain err: {e}")
    # 2) Linux / fallback: file ~/.claude/.credentials.json
    if not token:
        try:
            cred_path = Path(os.path.expanduser("~/.claude/.credentials.json"))
            creds = json.loads(cred_path.read_text())
            token = creds["claudeAiOauth"]["accessToken"]
        except Exception as e:
            log(f"oauth token err: {e}")
            return None
    req = urllib.request.Request(
        "https://api.anthropic.com/api/oauth/usage",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "claude-cli/2.0.40 (external, cli)",
            "anthropic-beta": "oauth-2025-04-20",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        log(f"usage fetch err: {e}")
        return {"_error": f"HTTP {e.code} {e.reason}"}
    except Exception as e:
        log(f"usage fetch err: {e}")
        return {"_error": str(e)}


def fmt_usage(data: dict) -> str:
    def pct_bar(p: float, width: int = 20) -> str:
        filled = int(round((p or 0) / 100 * width))
        return "█" * filled + "░" * (width - filled)

    def fmt_reset(iso: str | None) -> str:
        if not iso:
            return ""
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            delta = dt - now
            secs = int(delta.total_seconds())
            if secs <= 0:
                return "(reset imminente)"
            d, rem = divmod(secs, 86400)
            h, rem = divmod(rem, 3600)
            m, _ = divmod(rem, 60)
            parts = []
            if d: parts.append(f"{d}g")
            if h: parts.append(f"{h}h")
            if m and not d: parts.append(f"{m}m")
            return f"reset in {' '.join(parts)}"
        except Exception:
            return ""

    def row(label: str, w: dict) -> str:
        u = w.get("utilization") or 0
        r = fmt_reset(w.get("resets_at"))
        suffix = f"  {r}" if r else ""
        return f"• {label}: {pct_bar(u, 12)} {u:.1f}%{suffix}"

    lines = ["📊 Claude Code usage piano"]
    fh = data.get("five_hour") or {}
    sd = data.get("seven_day") or {}
    sd_sonnet = data.get("seven_day_sonnet") or {}
    sd_opus = data.get("seven_day_opus") or {}
    if fh:
        lines.append(row("5h ", fh))
    if sd:
        lines.append(row("7g ", sd))
    if sd_opus and sd_opus.get("utilization") is not None:
        lines.append(row("7g Opus  ", sd_opus))
    if sd_sonnet and sd_sonnet.get("utilization") is not None:
        lines.append(row("7g Sonnet", sd_sonnet))
    extra = data.get("extra_usage") or {}
    if extra.get("is_enabled"):
        used = extra.get("used_credits") or 0
        lim = extra.get("monthly_limit") or 0
        cur = extra.get("currency") or "EUR"
        lines.append(f"• Extra: {used:.2f}/{lim} {cur}")
    return "\n".join(lines)


def chat_action(chat_id: int, action: str = "typing", thread_id: int | None = None) -> None:
    try:
        params = {"chat_id": chat_id, "action": action}
        _thread_param(params, thread_id)
        tg("sendChatAction", **params)
    except Exception as e:
        log(f"chat_action err: {e}")


class TypingPinger:
    """Tiene attivo l'indicatore 'sta scrivendo' su Telegram (~5s TTL, ping ogni 4s)."""

    def __init__(self, chat_id: int, interval: float = 4.0, thread_id: int | None = None):
        self.chat_id = chat_id
        self.interval = interval
        self.thread_id = thread_id
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run(self) -> None:
        while not self._stop.is_set():
            chat_action(self.chat_id, "typing", thread_id=self.thread_id)
            if self._stop.wait(self.interval):
                break

    def __enter__(self) -> "TypingPinger":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)


def set_my_commands() -> None:
    try:
        tg("setMyCommands", commands=json.dumps(BOT_COMMANDS))
        log(f"setMyCommands ok ({len(BOT_COMMANDS)} cmd)")
    except Exception as e:
        log(f"setMyCommands err: {e}")


# ---------------------------------------------------------------------------
# Thread mode: enumerazione progetti CC + mapping topic forum
# ---------------------------------------------------------------------------

def _session_cwd(path: str) -> str | None:
    """Estrae il primo `cwd` da un file sessione .jsonl (scan lazy)."""
    try:
        with open(path) as fh:
            for line in fh:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                if o.get("cwd"):
                    return o["cwd"]
    except Exception:
        return None
    return None


def nice_name(cwd: str) -> str:
    """Nome leggibile per il topic a partire dal cwd."""
    home = os.path.expanduser("~")
    if cwd in ("/", ""):
        return "root /"
    if cwd == home:
        return "home"
    if "/worktrees/" in cwd:
        return f"wt/{os.path.basename(cwd)}"
    return os.path.basename(cwd.rstrip("/")) or cwd


def normalize_cwd(cwd: str) -> str:
    """Rimappa il prefisso home locale a quello canonico (/Users/luigilotito).

    Su lgcloud (home /home/luigi) il bot deve lanciare claude con cwd /Users/...
    così l'encoded dir di ~/.claude/projects combacia col Mac e la sessione è
    portabile (richiede bind mount /home/luigi -> /Users/luigilotito).
    No-op sul Mac (REMAP_FROM vuoto).
    """
    if REMAP_FROM and REMAP_TO and (cwd == REMAP_FROM or cwd.startswith(REMAP_FROM + "/")):
        return REMAP_TO + cwd[len(REMAP_FROM):]
    return cwd


def _enc_cwd(cwd: str) -> str:
    """Encoded project dir name: ogni '/' e '.' diventa '-' (come fa Claude Code)."""
    return _re.sub(r"[/.]", "-", cwd)


def manifest_add(cwd: str, sid: str) -> bool:
    """Aggiunge una sessione al manifest Syncthing (shared-includes), idempotente.

    Scrive due righe .stignore-include (la dir encoded + il file .jsonl) così
    Syncthing sincronizza solo quella sessione. Ritorna True se ha aggiunto righe.
    """
    if not cwd or not sid:
        return False
    enc = _enc_cwd(normalize_cwd(cwd))
    needed = [f"!/{enc}", f"!/{enc}/{sid}.jsonl"]
    try:
        existing = MANIFEST.read_text().splitlines() if MANIFEST.exists() else []
    except Exception:
        existing = []
    have = set(existing)
    added = False
    for ln in needed:
        if ln not in have:
            existing.append(ln)
            have.add(ln)
            added = True
    if added:
        try:
            MANIFEST.parent.mkdir(parents=True, exist_ok=True)
            MANIFEST.write_text("\n".join(existing) + "\n")
            log(f"manifest += {enc}/{sid[:8]}")
        except Exception as e:
            log(f"manifest_add err: {e}")
            return False
    return added


def process_publish_queue(state: dict) -> None:
    """Scansiona PUBLISH_DIR: per ogni richiesta crea+binda un topic forum.

    Richiesta json depositata da `claude-publish` (Mac via SSH): {uuid, cwd, name}.
    Usa l'unico forum registrato in state["_forums"]. Idempotente per dir_key.
    """
    if not PUBLISH_DIR.is_dir():
        return
    reqs = sorted(glob.glob(str(PUBLISH_DIR / "*.json")))
    if not reqs:
        return
    forums = state.get("_forums") or {}
    forum_id = None
    for cid, reg in forums.items():
        if reg.get("registered"):
            forum_id = int(cid)
            break
    for rp in reqs:
        try:
            req = json.loads(Path(rp).read_text())
        except Exception:
            os.remove(rp)
            continue
        uuid = req.get("uuid")
        cwd = normalize_cwd(req.get("cwd") or "")
        name = req.get("name") or nice_name(cwd)
        if not uuid or not cwd:
            os.remove(rp)
            continue
        if forum_id is None:
            for aid in ALLOW:
                send(aid, f"📤 publish '{name}' in attesa: manda prima un messaggio nel gruppo forum per registrarlo.")
            return  # lascia la richiesta in coda, riprova al prossimo giro
        reg = forum_reg(state, forum_id)
        topics = reg.setdefault("topics", {})
        dk = _enc_cwd(cwd)
        if dk in topics and topics[dk].get("thread_id"):
            tid = topics[dk]["thread_id"]
            topics[dk]["session_id"] = uuid
            state.setdefault(f"{forum_id}:{tid}", {})["session_id"] = uuid
            send(forum_id, f"↻ <b>{_html.escape(name)}</b> ri-pubblicata (sessione {uuid[:8]}).", thread_id=tid)
        else:
            color = TOPIC_COLORS[len(topics) % len(TOPIC_COLORS)]
            tid, err = create_forum_topic(forum_id, f"📂 {name}", icon_color=color)
            if tid is None:
                for aid in ALLOW:
                    send(aid, f"❌ publish '{name}' fallita: {err}\n(il bot deve essere admin del gruppo con permesso Manage Topics)")
                os.remove(rp)
                continue
            topics[dk] = {"thread_id": tid, "cwd": cwd, "name": name, "session_id": uuid}
            cs = state.setdefault(f"{forum_id}:{tid}", {})
            cs["cwd"] = cwd
            cs["session_id"] = uuid
            send(
                forum_id,
                f"🧵 <b>{_html.escape(name)}</b>\n"
                f"cwd: <code>{_html.escape(cwd)}</code>\n"
                f"sessione: <code>{uuid[:8]}</code>\n\n"
                f"Scrivi qui per riprendere questa sessione (sincronizzata col Mac).",
                thread_id=tid,
            )
        save_state(state)
        os.remove(rp)


def enumerate_projects() -> list[dict]:
    """Mappa ~/.claude/projects → progetti con cwd valido e sessione più recente.

    Esclude dir di noise (SYNC_EXCLUDE), dir senza sessioni, cwd non esistenti
    (es. worktree rimossi). Ordina per attività (mtime ultima sessione) desc.
    """
    out: list[dict] = []
    if not PROJECTS_DIR.is_dir():
        return out
    for d in sorted(os.listdir(PROJECTS_DIR)):
        full = PROJECTS_DIR / d
        if not full.is_dir():
            continue
        if any(x in d for x in SYNC_EXCLUDE):
            continue
        sessions = sorted(glob.glob(str(full / "*.jsonl")), key=os.path.getmtime, reverse=True)
        if not sessions:
            continue
        cwd = None
        for s in sessions:
            cwd = _session_cwd(s)
            if cwd:
                break
        cwd = normalize_cwd(cwd or "")
        if not cwd or not os.path.isdir(cwd):
            continue
        latest = sessions[0]
        out.append({
            "dir_key": d,
            "cwd": cwd,
            "name": nice_name(cwd),
            "latest_sid": os.path.basename(latest)[:-6],
            "n": len(sessions),
            "mtime": os.path.getmtime(latest),
        })
    out.sort(key=lambda r: r["mtime"], reverse=True)
    return out


def is_forum_msg(msg: dict) -> bool:
    chat = msg.get("chat") or {}
    return chat.get("type") in ("group", "supergroup") and bool(chat.get("is_forum"))


def forum_reg(state: dict, chat_id: int) -> dict:
    forums = state.setdefault("_forums", {})
    return forums.setdefault(str(chat_id), {"registered": False, "topics": {}})


def do_sync(chat_id: int, state: dict) -> str:
    """Crea/aggiorna un topic per ogni progetto CC. Idempotente.

    - Topic già mappato (dir_key noto) → aggiorna solo cwd/name mapping.
    - Nuovo → createForumTopic + intro nel topic + seed stato runtime.
    Ritorna un testo riassuntivo per il topic General.
    """
    reg = forum_reg(state, chat_id)
    topics = reg.setdefault("topics", {})
    projects = enumerate_projects()
    if not projects:
        return "⚠️ nessun progetto Claude Code trovato in ~/.claude/projects."

    created, updated, errors = [], [], []
    color_i = len(topics)
    for proj in projects[:MAX_TOPICS]:
        dk = proj["dir_key"]
        if dk in topics and topics[dk].get("thread_id"):
            # refresh: aggiorna metadati (non clobbera la sessione di chat attive)
            topics[dk]["cwd"] = proj["cwd"]
            topics[dk]["name"] = proj["name"]
            updated.append(proj["name"])
            continue
        color = TOPIC_COLORS[color_i % len(TOPIC_COLORS)]
        color_i += 1
        tid, err = create_forum_topic(chat_id, f"📂 {proj['name']}", icon_color=color)
        if tid is None:
            errors.append(f"{proj['name']}: {err}")
            continue
        topics[dk] = {
            "thread_id": tid,
            "cwd": proj["cwd"],
            "name": proj["name"],
            "session_id": proj["latest_sid"],
        }
        # seed stato runtime del topic così il primo messaggio riprende la sessione
        skey = f"{chat_id}:{tid}"
        cs = state.setdefault(skey, {})
        cs.setdefault("cwd", proj["cwd"])
        cs.setdefault("session_id", proj["latest_sid"])
        created.append(proj["name"])
        # intro nel nuovo topic
        send(
            chat_id,
            f"🧵 <b>{_html.escape(proj['name'])}</b>\n"
            f"cwd: <code>{_html.escape(proj['cwd'])}</code>\n"
            f"sessione: <code>{proj['latest_sid'][:8]}</code> ({proj['n']} sessioni nel progetto)\n\n"
            f"Scrivi qui per riprendere questa sessione Claude Code.",
            thread_id=tid,
        )

    save_state(state)
    lines = [f"🔄 <b>Sync thread completato</b> — {len(topics)} topic mappati."]
    if created:
        lines.append(f"✅ creati ({len(created)}): " + ", ".join(created))
    if updated:
        lines.append(f"↻ aggiornati ({len(updated)}): " + ", ".join(updated))
    if errors:
        lines.append("❌ errori:\n" + "\n".join(f"  • {e}" for e in errors))
        lines.append("ℹ️ se 'not enough rights': rendi il bot admin con permesso 'Manage Topics'.")
    return "\n".join(lines)


def fmt_threads(state: dict, chat_id: int) -> str:
    reg = forum_reg(state, chat_id)
    topics = reg.get("topics") or {}
    if not topics:
        return "Nessun thread mappato. Usa /sync in un supergruppo con Topics."
    lines = [f"🧵 {len(topics)} thread:"]
    for dk, t in topics.items():
        sid = (t.get("session_id") or "")[:8]
        lines.append(f"• {t.get('name')} — sid {sid} — <code>{_html.escape(t.get('cwd',''))}</code>")
    return "\n".join(lines)


def _summarize_tool_input(name: str, inp: dict) -> str:
    """Crea label compatta per il tool. No payload pieno (rumoroso)."""
    if not isinstance(inp, dict):
        return ""
    if name == "Bash":
        cmd = (inp.get("command") or "").strip().splitlines()[0][:80]
        return cmd
    if name in ("Read", "Edit", "Write", "NotebookEdit"):
        p = inp.get("file_path") or inp.get("path") or ""
        home = os.path.expanduser("~")
        if p.startswith(home):
            p = "~" + p[len(home):]
        return p
    if name == "Grep":
        pat = inp.get("pattern") or ""
        return f'"{pat[:50]}"'
    if name == "Glob":
        return inp.get("pattern") or ""
    if name in ("WebFetch", "WebSearch"):
        return (inp.get("url") or inp.get("query") or "")[:80]
    if name == "Task" or name == "Agent":
        return (inp.get("description") or "")[:60]
    if name == "TodoWrite":
        todos = inp.get("todos") or []
        return f"{len(todos)} todo"
    # MCP tool: nome già descrittivo
    return ""


def run_claude_streaming(
    prompt: str,
    session_id: str | None,
    mode: str,
    cwd: str,
    model: str | None,
    on_status: "callable | None" = None,
    effort: str | None = None,
) -> tuple[str, str | None, dict]:
    """Lancia claude con --output-format stream-json e parsa eventi in tempo reale.

    `on_status(label)` viene chiamato a ogni cambio tool corrente.
    Ritorna (testo_finale, new_session_id, meta).
    """
    cwd = normalize_cwd(cwd)  # canonicalizza (/home/luigi -> /Users/luigilotito su lgcloud)
    cmd = [
        "claude", "-p", prompt,
        "--output-format", "stream-json",
        "--verbose",  # required for stream-json with -p
        "--permission-mode", mode,
    ]
    eff_model = model if model is not None else MODEL
    if eff_model:
        cmd += ["--model", eff_model]
    eff_effort = effort or DEFAULT_EFFORT
    if eff_effort:
        cmd += ["--effort", eff_effort]
    if session_id:
        cmd += ["--resume", session_id]
    log(f"claude stream (resume={'y' if session_id else 'n'}, mode={mode}, model={eff_model or 'default'}, effort={eff_effort or 'default'}, cwd={cwd}): {prompt[:80]!r}")

    proc = subprocess.Popen(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
    )

    final_text = ""
    new_sid = session_id
    meta: dict = {}
    pending_tools: dict[str, str] = {}  # tool_use_id -> label
    last_label = ""
    last_text = ""  # ultimo blocco text del modello (fallback se result vuoto)
    start_ts = time.time()
    tool_count = 0

    def emit(label: str) -> None:
        nonlocal last_label
        if on_status and label and label != last_label:
            last_label = label
            on_status(label)

    # Heartbeat: con tool lunghi (un singolo Bash da 10min) emit() non scatta
    # mai; questo thread aggiorna comunque lo status così l'utente vede che il
    # lavoro procede.
    hb_stop = threading.Event()

    def _heartbeat() -> None:
        while not hb_stop.wait(HEARTBEAT):
            if on_status:
                el = int(time.time() - start_ts)
                m, s = divmod(el, 60)
                base = last_label or "💭 al lavoro…"
                on_status(f"{base}\n⏳ {m}m{s:02d}s · {tool_count} tool · sto ancora lavorando…")

    if on_status:
        threading.Thread(target=_heartbeat, daemon=True, name="cc-heartbeat").start()

    deadline = time.time() + TIMEOUT
    try:
        for line in proc.stdout:
            if time.time() > deadline:
                proc.kill()
                return f"⏱ timeout dopo {TIMEOUT}s", session_id, {}
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue
            t = evt.get("type")
            if t == "system" and evt.get("subtype") == "init":
                new_sid = evt.get("session_id") or new_sid
                emit("💭 ragionamento…")
            elif t == "assistant":
                content = (evt.get("message") or {}).get("content") or []
                for block in content:
                    bt = block.get("type")
                    if bt == "tool_use":
                        name = block.get("name") or "?"
                        tool_id = block.get("id") or ""
                        summary = _summarize_tool_input(name, block.get("input") or {})
                        label = f"🔧 {name}"
                        if summary:
                            label += f"  {summary}"
                        pending_tools[tool_id] = name
                        tool_count += 1
                        emit(label)
                    elif bt == "text":
                        # tieni l'ultimo testo del modello: se l'evento result
                        # arriva vuoto (subtype d'errore post-testo), è il
                        # fallback che evita di rispondere "(vuoto)"
                        if (block.get("text") or "").strip():
                            last_text = block["text"]
            elif t == "user":
                # tool_result block(s) — segnaliamo "elaboro risultato"
                content = (evt.get("message") or {}).get("content") or []
                for block in content:
                    if block.get("type") == "tool_result":
                        tid = block.get("tool_use_id") or ""
                        name = pending_tools.pop(tid, None)
                        if name:
                            emit(f"✓ {name} → continuo…")
            elif t == "result":
                final_text = evt.get("result") or ""
                new_sid = evt.get("session_id") or new_sid
                meta = {
                    "usage": evt.get("usage") or {},
                    "cost_usd": evt.get("total_cost_usd") or 0.0,
                    "duration_ms": evt.get("duration_ms") or 0,
                    "num_turns": evt.get("num_turns") or 0,
                }
                if evt.get("is_error"):
                    final_text = f"❌ {final_text or evt.get('subtype', 'error')}"
                if not (evt.get("result") or "").strip():
                    log(f"result vuoto (subtype={evt.get('subtype')}, is_error={evt.get('is_error')}) — uso fallback last_text ({len(last_text)} char)")
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        return f"⏱ timeout dopo {TIMEOUT}s", session_id, {}
    except Exception as e:
        proc.kill()
        log(f"stream err: {e}")
        return f"❌ stream err: {e}", session_id, {}
    finally:
        hb_stop.set()

    if proc.returncode and proc.returncode != 0 and not final_text:
        err = (proc.stderr.read() if proc.stderr else "")[:1500]
        return f"❌ claude exit {proc.returncode}\n{err}", session_id, {}

    return final_text or last_text or "(vuoto)", new_sid, meta


# Wrapper retro-compatibile (non usato in handle, mantenuto per compat)
def run_claude(prompt: str, session_id: str | None, mode: str, cwd: str, model: str | None = None) -> tuple[str, str | None, dict]:
    return run_claude_streaming(prompt, session_id, mode, cwd, model, on_status=None)


def accumulate_usage(chat_state: dict, meta: dict) -> None:
    agg = chat_state.setdefault("usage_agg", {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cost_usd": 0.0,
        "turns": 0,
    })
    u = meta.get("usage") or {}
    for k in ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"):
        agg[k] = agg.get(k, 0) + int(u.get(k, 0) or 0)
    agg["cost_usd"] = round(agg.get("cost_usd", 0.0) + float(meta.get("cost_usd") or 0.0), 6)
    agg["turns"] = agg.get("turns", 0) + 1


def tg_get_file_url(file_id: str) -> str | None:
    """Risolve file_id Telegram in URL pubblico temporaneo."""
    try:
        r = tg("getFile", file_id=file_id)
        if not r.get("ok"):
            return None
        path = r["result"]["file_path"]
        return f"https://api.telegram.org/file/bot{TOKEN}/{path}"
    except Exception as e:
        log(f"getFile err: {e}")
        return None


def download_file(url: str, dest: Path) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=30) as r, open(dest, "wb") as f:
            while chunk := r.read(65536):
                f.write(chunk)
        return True
    except Exception as e:
        log(f"download err: {e}")
        return False


MEDIA_DIR = Path(os.path.expanduser("~/.cc-telegram-bot.media"))
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "whisper-1").strip()
WHISPER_TRANSLATE_LANG = os.environ.get("WHISPER_TRANSLATE_LANG", "").strip()  # es. "English", "Italian"


def transcribe_audio(file_path: str) -> str | None:
    """Trascrive audio via OpenAI Whisper API. Ritorna testo o None su errore/no-key."""
    if not OPENAI_API_KEY:
        return None
    try:
        import mimetypes
        boundary = "----cctgbot" + os.urandom(8).hex()
        mime = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
        name = os.path.basename(file_path)
        with open(file_path, "rb") as fh:
            file_bytes = fh.read()
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="model"\r\n\r\n'
            f"{WHISPER_MODEL}\r\n"
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'
            f"Content-Type: {mime}\r\n\r\n"
        ).encode() + file_bytes + f"\r\n--{boundary}--\r\n".encode()
        req = urllib.request.Request(
            "https://api.openai.com/v1/audio/transcriptions",
            data=body,
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=120) as r:
            payload = json.loads(r.read().decode("utf-8"))
        text = (payload.get("text") or "").strip()
        return text or None
    except Exception as e:
        log(f"whisper err: {e}")
        return None


def translate_text(text: str, target_lang: str) -> str | None:
    """Traduce testo verso target_lang via GPT-4o-mini."""
    if not OPENAI_API_KEY or not text:
        return None
    try:
        body = json.dumps({
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": f"Translate to {target_lang}. Return only the translation, no explanation."},
                {"role": "user", "content": text},
            ],
            "max_tokens": 1000,
        }).encode()
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=body,
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            payload = json.loads(r.read().decode())
        result = ((payload.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        return result.strip() or None
    except Exception as e:
        log(f"translate err: {e}")
        return None


def extract_media_paths(msg: dict) -> tuple[list[str], str]:
    """Scarica eventuali photo/document/audio/voice. Ritorna (paths, caption)."""
    paths: list[str] = []
    caption = (msg.get("caption") or "").strip()

    # photo: array di PhotoSize, prendiamo la più grande
    photos = msg.get("photo") or []
    if photos:
        biggest = max(photos, key=lambda p: p.get("file_size") or 0)
        url = tg_get_file_url(biggest["file_id"])
        if url:
            ext = url.rsplit(".", 1)[-1].split("?")[0][:5] or "jpg"
            dest = MEDIA_DIR / f"{biggest['file_unique_id']}.{ext}"
            if not dest.exists() and download_file(url, dest):
                pass
            if dest.exists():
                paths.append(str(dest))

    # document (immagini/file inviati come "file" non "foto")
    doc = msg.get("document")
    if doc:
        url = tg_get_file_url(doc["file_id"])
        if url:
            name = doc.get("file_name") or f"{doc['file_unique_id']}.bin"
            dest = MEDIA_DIR / f"{doc['file_unique_id']}_{name}"
            if not dest.exists() and download_file(url, dest):
                pass
            if dest.exists():
                paths.append(str(dest))

    # voice / audio (con tentativo di trascrizione via Whisper API se OPENAI_API_KEY)
    for k in ("voice", "audio", "video", "video_note"):
        m = msg.get(k)
        if not m:
            continue
        url = tg_get_file_url(m["file_id"])
        if not url:
            continue
        ext = url.rsplit(".", 1)[-1].split("?")[0][:5] or "bin"
        dest = MEDIA_DIR / f"{m['file_unique_id']}.{ext}"
        if not dest.exists():
            download_file(url, dest)
        if not dest.exists():
            continue
        if k in ("voice", "audio"):
            transcript = transcribe_audio(str(dest))
            if transcript:
                display = f"🎙 {transcript}"
                if WHISPER_TRANSLATE_LANG:
                    translation = translate_text(transcript, WHISPER_TRANSLATE_LANG)
                    if translation and translation.strip().lower() != transcript.strip().lower():
                        display += f"\n🌐 {translation}"
                        log(f"whisper: {k} tradotto in {WHISPER_TRANSLATE_LANG}")
                caption = (display + ("\n" + caption if caption else "")).strip()
                log(f"whisper: {k} trascritto, {len(transcript)} char")
                continue  # binario inutile al modello, salto path
        paths.append(str(dest))

    return paths, caption


def handle(msg: dict, state: dict) -> None:
    chat = msg.get("chat") or {}
    chat_id = chat["id"]
    from_id = (msg.get("from") or {}).get("id")
    # Auth: utente whitelisted (from_id) OPPURE chat privata whitelisted (back-compat).
    # In un gruppo chat_id è negativo (non in ALLOW) ma from_id = user id di Luigi.
    if (from_id not in ALLOW) and (chat_id not in ALLOW):
        log(f"deny chat_id={chat_id} from_id={from_id}")
        return

    # Thread routing: in un topic forum i messaggi portano message_thread_id.
    # General topic / chat privata → thread_id None → comportamento legacy.
    thread_id = msg.get("message_thread_id")
    skey = f"{chat_id}:{thread_id}" if thread_id is not None else str(chat_id)

    # Registrazione forum alla prima interazione nel gruppo (NO auto-create di massa:
    # i thread nascono da /remote-desktop sul Mac o da sessioni avviate qui).
    if is_forum_msg(msg):
        reg = forum_reg(state, chat_id)
        if not reg.get("registered"):
            reg["registered"] = True
            save_state(state)
            log(f"forum registrato chat_id={chat_id}")
            send(
                chat_id,
                "👋 Forum registrato. Pubblica una sessione dal Mac con <code>/remote-desktop</code>, "
                "oppure scrivi qui per avviarne una nuova. (<code>/sync</code> per mappare i progetti già presenti.)",
            )
            return

    text = msg.get("text", "").strip()
    has_potential_media = any(msg.get(k) for k in ("photo", "document", "voice", "audio", "video", "video_note", "caption"))
    if not text and not has_potential_media:
        return
    if text in ("/start", "/help"):
        send(
            chat_id,
            "🤖 cc-telegram-bot\n"
            "/new — reset sessione\n"
            "/status — info sessione\n"
            "/usage — % utilizzo piano CC (5h, 7g)\n"
            "/compact — riassumi e ricomincia\n"
            "/mode [plan|acceptEdits|bypassPermissions] — permission mode\n"
            "/effort [low|medium|high|xhigh|max|reset] — effort level\n"
            "/cwd [path] — working dir\n"
            "/model [id|reset] — model id (es. claude-opus-4-7)\n"
            "/caveman [on|off] — toggle stile caveman (default off)\n"
            "/handoff [nome] — handoff sessione → nuova sessione (nuovo topic se forum)\n"
            "/sync — crea/aggiorna i thread delle sessioni CC (forum)\n"
            "/threads — lista thread mappati (forum)\n"
            "/help — questo messaggio\n\n"
            "In un supergruppo con Topics: ogni thread = una sessione CC separata.\n"
            "Altro testo = prompt a Claude (continua la sessione del thread corrente).",
            thread_id=thread_id,
        )
        return
    if text == "/sync":
        if not is_forum_msg(msg):
            send(chat_id, "ℹ️ /sync funziona solo in un supergruppo con i Topics abilitati.", thread_id=thread_id)
            return
        forum_reg(state, chat_id)["registered"] = True
        send(chat_id, "🔄 sincronizzo i thread…", thread_id=thread_id)
        summary = do_sync(chat_id, state)
        send(chat_id, summary, thread_id=thread_id)
        return
    if text == "/threads":
        send(chat_id, fmt_threads(state, chat_id), thread_id=thread_id)
        return
    if text == "/new":
        cs = state.get(skey, {})
        cs.pop("session_id", None)
        cs.pop("compact_summary", None)
        cs.pop("usage_agg", None)
        state[skey] = cs
        save_state(state)
        send(chat_id, "🆕 sessione resettata", thread_id=thread_id)
        return
    if text == "/usage":
        chat_action(chat_id, "typing", thread_id=thread_id)
        data = fetch_cc_usage()
        if not data or "_error" in data:
            err = (data or {}).get("_error", "token non trovato o endpoint irraggiungibile")
            send(chat_id, f"❌ usage: {err}", thread_id=thread_id)
            return
        send(chat_id, fmt_usage(data), thread_id=thread_id)
        return
    if text == "/compact":
        cs = state.setdefault(skey, {})
        sid = cs.get("session_id")
        if not sid:
            send(chat_id, "⚠️ nessuna sessione da compattare", thread_id=thread_id)
            return
        cwd = cs.get("cwd", CWD)
        mode = cs.get("mode", DEFAULT_MODE)
        model = cs.get("model", MODEL) or None
        eff = cs.get("effort", DEFAULT_EFFORT) or None
        prompt = (
            "Compatta questa conversazione in un riassunto strutturato (max 1500 caratteri). "
            "Includi: stato attuale, decisioni prese, file modificati o creati, task pending, "
            "contesto tecnico chiave da preservare. Formato: bullet point. Solo il riassunto, niente preamboli."
        )
        with TypingPinger(chat_id, thread_id=thread_id):
            summary, _new_sid, meta = run_claude_streaming(prompt, sid, mode, cwd, model, effort=eff)
        accumulate_usage(cs, meta)
        cs["compact_summary"] = summary
        cs.pop("session_id", None)
        save_state(state)
        send(chat_id, f"🗜 sessione compattata. Riassunto:\n\n{summary}\n\n— prossimo prompt parte da zero con questo contesto.", thread_id=thread_id)
        return
    if text.startswith("/handoff"):
        parts = text.split(maxsplit=1)
        topic_name = parts[1].strip() if len(parts) > 1 else ""
        cs = state.setdefault(skey, {})
        sid = cs.get("session_id")
        if not sid:
            send(chat_id, "⚠️ nessuna sessione attiva da cui fare handoff", thread_id=thread_id)
            return
        cwd = cs.get("cwd", CWD)
        mode = cs.get("mode", DEFAULT_MODE)
        model = cs.get("model", MODEL) or None
        eff = cs.get("effort", DEFAULT_EFFORT) or None
        # Se la skill/command /handoff è installata su QUESTO host la usiamo,
        # altrimenti prompt equivalente built-in (la skill sul Mac non è
        # visibile al claude che gira qui).
        has_skill = (
            Path(os.path.expanduser("~/.claude/skills/handoff")).exists()
            or Path(os.path.expanduser("~/.claude/commands/handoff.md")).exists()
        )
        prompt = (
            f"/handoff {topic_name or 'continuare il lavoro corrente'} — oltre a "
            "salvare il file, riporta il documento di handoff completo come testo "
            "della risposta (verrà usato come contesto della nuova sessione)."
        ) if has_skill else (
            "Prepara un documento di handoff per passare questo lavoro a una nuova "
            "sessione che parte da zero (max 3000 caratteri). Includi: obiettivo, "
            "stato attuale, decisioni prese e perché, file creati/modificati con "
            "path, task pending in ordine di priorità, gotcha e contesto tecnico "
            "indispensabile. Solo il documento, niente preamboli."
        )
        status_id = send_status(chat_id, "🤝 genero handoff dalla sessione corrente…", thread_id=thread_id)
        with TypingPinger(chat_id, thread_id=thread_id):
            doc, _hsid, meta = run_claude_streaming(prompt, sid, mode, cwd, model, effort=eff)
        accumulate_usage(cs, meta)
        if status_id is not None:
            delete_message(chat_id, status_id)
        if doc.startswith(("❌", "⏱")) or not doc.strip():
            send(chat_id, f"handoff fallito: {doc}", thread_id=thread_id)
            return
        if not is_forum_msg(msg):
            # niente Topics: handoff in place (come /compact ma con doc ricco)
            cs["compact_summary"] = doc
            cs.pop("session_id", None)
            save_state(state)
            send(chat_id, f"🤝 handoff pronto:\n\n{doc}\n\n— prossimo prompt = nuova sessione con questo contesto.", thread_id=thread_id)
            return
        name = topic_name or f"handoff {time.strftime('%d/%m %H:%M')}"
        tid, err = create_forum_topic(chat_id, f"🤝 {name}", icon_color=TOPIC_COLORS[len(name) % len(TOPIC_COLORS)])
        if tid is None:
            send(chat_id, f"❌ creazione topic fallita: {err}\nℹ️ il bot deve essere admin con 'Manage Topics'.", thread_id=thread_id)
            return
        nskey = f"{chat_id}:{tid}"
        ncs = state.setdefault(nskey, {})
        ncs["cwd"] = cwd
        ncs["compact_summary"] = doc  # primo messaggio nel topic → sessione nuova con questo contesto
        for k in ("mode", "model", "effort", "caveman"):
            if k in cs:
                ncs[k] = cs[k]
        forum_reg(state, chat_id).setdefault("topics", {})[f"handoff-{tid}"] = {
            "thread_id": tid, "cwd": cwd, "name": name, "session_id": None,
        }
        save_state(state)
        send(
            chat_id,
            f"🤝 **{name}**\n"
            f"cwd: `{cwd}`\n\n"
            f"{doc}\n\n"
            f"— scrivi qui: il primo messaggio apre una sessione nuova con questo contesto.",
            thread_id=tid,
        )
        send(chat_id, f"✅ handoff → topic «{name}». Questa sessione resta attiva qui.", thread_id=thread_id)
        return
    if text == "/status":
        cs = state.get(skey, {})
        sid = cs.get("session_id")
        cwd = cs.get("cwd", CWD)
        mode = cs.get("mode", DEFAULT_MODE)
        model = cs.get("model", MODEL)
        agg = cs.get("usage_agg") or {}
        eff = cs.get("effort", DEFAULT_EFFORT)
        lines = [
            f"sid: {sid or '(nuova)'}",
            f"cwd: {cwd}",
            f"mode: {mode}",
            f"model: {model or '(default)'}",
            f"effort: {eff or '(default)'}",
            f"caveman: {'on' if cs.get('caveman') else 'off'}",
        ]
        if thread_id is not None:
            lines.insert(0, f"thread: {thread_id}")
        if agg:
            lines.append("")
            lines.append(f"sessione: {agg.get('turns', 0)} turni")
            lines.append(f"  in:    {agg.get('input_tokens', 0):,}")
            lines.append(f"  out:   {agg.get('output_tokens', 0):,}")
            lines.append(f"  cache: w={agg.get('cache_creation_input_tokens', 0):,} r={agg.get('cache_read_input_tokens', 0):,}")
            lines.append(f"  costo: ${agg.get('cost_usd', 0.0):.4f}")
        send(chat_id, "\n".join(lines), thread_id=thread_id)
        return
    if text.startswith("/model"):
        parts = text.split(maxsplit=1)
        cs = state.setdefault(skey, {})
        if len(parts) == 1:
            cur = cs.get("model", MODEL) or None
            send_with_keyboard(
                chat_id,
                f"🧠 model attuale: {cur or '(default)'}\nScegli:",
                build_model_keyboard(cur),
                thread_id=thread_id,
            )
            return
        new_model = parts[1].strip()
        if new_model == "reset":
            cs.pop("model", None)
            save_state(state)
            send(chat_id, "🧠 model → (default)", thread_id=thread_id)
            return
        cs["model"] = new_model
        save_state(state)
        send(chat_id, f"🧠 model → {new_model}", thread_id=thread_id)
        return
    if text.startswith("/caveman"):
        parts = text.split(maxsplit=1)
        cs = state.setdefault(skey, {})
        if len(parts) == 1:
            new_val = not cs.get("caveman", False)
        elif parts[1].strip().lower() in ("on", "off"):
            new_val = parts[1].strip().lower() == "on"
        else:
            send(chat_id, "uso: /caveman [on|off] (senza argomento = toggle)", thread_id=thread_id)
            return
        cs["caveman"] = new_val
        save_state(state)
        if new_val:
            send(chat_id, "🪨 caveman ON. pochi token. brain still big.", thread_id=thread_id)
        else:
            send(chat_id, "🗣 caveman OFF — risposte in italiano normale.", thread_id=thread_id)
        return
    if text.startswith("/mode"):
        parts = text.split(maxsplit=1)
        if len(parts) == 1:
            cur = state.get(skey, {}).get("mode", DEFAULT_MODE)
            send(chat_id, f"mode: {cur}\nset con: /mode <plan|acceptEdits|bypassPermissions>", thread_id=thread_id)
            return
        new_mode = parts[1].strip()
        if new_mode not in VALID_MODES:
            send(chat_id, f"❌ mode invalida. valide: {', '.join(VALID_MODES)}", thread_id=thread_id)
            return
        state.setdefault(skey, {})["mode"] = new_mode
        save_state(state)
        send(chat_id, f"🔐 mode → {new_mode}", thread_id=thread_id)
        return
    if text.startswith("/effort"):
        parts = text.split(maxsplit=1)
        cs = state.setdefault(skey, {})
        if len(parts) == 1:
            cur = cs.get("effort", DEFAULT_EFFORT) or None
            send_with_keyboard(
                chat_id,
                f"🎚 effort attuale: {cur or '(default)'}\nScegli:",
                build_effort_keyboard(cur),
                thread_id=thread_id,
            )
            return
        new_eff = parts[1].strip()
        if new_eff == "reset":
            cs.pop("effort", None)
            save_state(state)
            send(chat_id, "🎚 effort → (default)", thread_id=thread_id)
            return
        if new_eff not in VALID_EFFORTS:
            send(chat_id, f"❌ effort invalido. validi: {', '.join(sorted(VALID_EFFORTS))}", thread_id=thread_id)
            return
        cs["effort"] = new_eff
        save_state(state)
        send(chat_id, f"🎚 effort → {new_eff}", thread_id=thread_id)
        return
    if text.startswith("/cwd"):
        parts = text.split(maxsplit=1)
        if len(parts) == 1:
            cur = state.get(skey, {}).get("cwd", CWD)
            send(chat_id, f"cwd: {cur}", thread_id=thread_id)
            return
        new_cwd = os.path.expanduser(parts[1].strip())
        if not Path(new_cwd).is_dir():
            send(chat_id, f"❌ non è dir: {new_cwd}", thread_id=thread_id)
            return
        state.setdefault(skey, {})["cwd"] = new_cwd
        state[skey].pop("session_id", None)  # nuova dir, nuova session
        save_state(state)
        send(chat_id, f"📂 cwd → {new_cwd} (sessione resettata)", thread_id=thread_id)
        return

    chat_state = state.setdefault(skey, {})
    # Topic forum senza stato runtime ancora seeded: prova a bindare dal mapping.
    if thread_id is not None and not chat_state.get("cwd"):
        reg = forum_reg(state, chat_id)
        for t in (reg.get("topics") or {}).values():
            if t.get("thread_id") == thread_id:
                chat_state.setdefault("cwd", t.get("cwd", CWD))
                if t.get("session_id") and not chat_state.get("session_id"):
                    chat_state["session_id"] = t["session_id"]
                break
    sid = chat_state.get("session_id")
    cwd = chat_state.get("cwd", CWD)
    mode = chat_state.get("mode", DEFAULT_MODE)
    model = chat_state.get("model", MODEL) or None
    effort = chat_state.get("effort", DEFAULT_EFFORT) or None
    summary = chat_state.pop("compact_summary", None)

    status_msg_id = send_status(chat_id, "💭 avvio…", thread_id=thread_id)

    media_paths, caption = extract_media_paths(msg)
    if not text and not media_paths and not caption:
        delete_message(chat_id, status_msg_id)
        return
    if not text:
        text = caption or "Analizza il file allegato."
        log(f"media: {len(media_paths)} file, caption={caption!r}")

    # Reply-to: includi messaggio citato come contesto
    reply_to = msg.get("reply_to_message") or {}
    quoted = (reply_to.get("text") or reply_to.get("caption") or "").strip()
    quoted_from = ((reply_to.get("from") or {}).get("first_name")
                   or ((reply_to.get("from") or {}).get("username"))
                   or "messaggio precedente")

    parts = []
    harness_note = os.environ.get(
        "BOT_HARNESS_NOTE",
        "[Regola harness cc-telegram-bot]\n"
        "Stai girando come subprocess di un bot Telegram (`bot.py`). "
        "Il bot si auto-riavvia: se modifichi `bot.py`, al termine di questo turno "
        "(dopo che la risposta sarà stata inviata e l'offset Telegram persistito) "
        "`maybe_self_restart()` rileva il cambio mtime, valida la sintassi con "
        "`ast.parse` e fa `os.execv(run.sh)`. Quindi: edita pure `bot.py` e basta, "
        "NON usare comandi del process manager (launchctl/systemctl/pkill) — "
        "termineresti il tuo processo padre prima dell'ack causando replay loop. "
        "Per restart immediato senza modifiche usa `touch bot.py`."
    )
    parts.append(harness_note)
    # Stile risposta: anche con caveman OFF la nota è necessaria, perché le
    # sessioni nate sul Mac (plugin caveman attivo lì) trascinano lo stile
    # compresso per inerzia dalla history al resume.
    if chat_state.get("caveman"):
        parts.append(
            "[Stile risposta]\nRispondi in stile caveman compresso: frasi minime, "
            "niente articoli superflui, massima densità tecnica, ~75% token in meno."
        )
    else:
        parts.append(
            "[Stile risposta]\nIgnora qualsiasi stile 'caveman' compresso presente "
            "nella history di questa sessione: rispondi in italiano normale, frasi "
            "complete e chiare."
        )
    if summary and not sid:
        parts.append(f"[Contesto sessione precedente compattata]\n{summary}")
    if quoted:
        parts.append(f"[In risposta a {quoted_from}]\n{quoted}")
    if media_paths:
        files_block = "\n".join(f"- {p}" for p in media_paths)
        parts.append(f"[File allegati (path locali)]\n{files_block}")
    parts.append(f"[Nuovo messaggio]\n{text}")
    prompt = "\n\n".join(parts)

    def update_status(label: str) -> None:
        if status_msg_id is not None:
            edit_status(chat_id, status_msg_id, label)

    with TypingPinger(chat_id, thread_id=thread_id):
        reply, new_sid, meta = run_claude_streaming(prompt, sid, mode, cwd, model, on_status=update_status, effort=effort)
    chat_state["session_id"] = new_sid
    chat_state.setdefault("cwd", cwd)
    accumulate_usage(chat_state, meta)
    # sessione nuova nata qui (Telegram) → aggiungila al manifest Syncthing (sync verso il Mac)
    if new_sid and new_sid != sid:
        manifest_add(cwd, new_sid)
    # tieni allineato il mapping topic → sessione corrente
    if thread_id is not None:
        reg = forum_reg(state, chat_id)
        for t in (reg.get("topics") or {}).values():
            if t.get("thread_id") == thread_id:
                t["session_id"] = new_sid
                break
    save_state(state)
    if status_msg_id is not None:
        delete_message(chat_id, status_msg_id)
    send(chat_id, reply or "(vuoto)", thread_id=thread_id)


def handle_callback(cb: dict, state: dict) -> None:
    """Gestisce click su inline_keyboard. callback_data formato: '<key>:<value>'."""
    cb_id = cb.get("id", "")
    message = cb.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    msg_id = message.get("message_id")
    thread_id = message.get("message_thread_id")
    from_id = (cb.get("from") or {}).get("id")
    if (from_id not in ALLOW) and (chat_id not in ALLOW):
        answer_callback(cb_id, "non autorizzato", show_alert=True)
        return
    skey = f"{chat_id}:{thread_id}" if thread_id is not None else str(chat_id)
    data = cb.get("data") or ""
    cs = state.setdefault(skey, {})

    if data.startswith("effort:"):
        val = data.split(":", 1)[1]
        if val == "reset":
            cs.pop("effort", None)
            new_cur = None
        elif val in VALID_EFFORTS:
            cs["effort"] = val
            new_cur = val
        else:
            answer_callback(cb_id, "valore invalido", show_alert=True)
            return
        save_state(state)
        if msg_id is not None:
            edit_message_with_keyboard(
                chat_id, msg_id,
                f"🎚 effort attuale: {new_cur or '(default)'}\nScegli:",
                build_effort_keyboard(new_cur),
            )
        answer_callback(cb_id, f"effort → {new_cur or 'default'}")
        return

    if data.startswith("model:"):
        val = data.split(":", 1)[1]
        if val == "reset":
            cs.pop("model", None)
            new_cur = None
        else:
            cs["model"] = val
            new_cur = val
        save_state(state)
        if msg_id is not None:
            edit_message_with_keyboard(
                chat_id, msg_id,
                f"🧠 model attuale: {new_cur or '(default)'}\nScegli:",
                build_model_keyboard(new_cur),
            )
        answer_callback(cb_id, f"model → {new_cur or 'default'}")
        return

    answer_callback(cb_id, "callback non riconosciuto")


def _msg_skey(msg: dict) -> str:
    """Calcola la session key per un messaggio (stessa logica di handle())."""
    chat = msg.get("chat") or {}
    thread_id = msg.get("message_thread_id")
    return f"{chat.get('id')}:{thread_id}" if thread_id is not None else str(chat.get("id", ""))


def main() -> None:
    acquire_singleton_lock()
    PUBLISH_DIR.mkdir(parents=True, exist_ok=True)
    log(f"bot start, allow={ALLOW}, cwd={CWD}, model={MODEL or '(default)'}, remap={REMAP_FROM or '-'}->{REMAP_TO or '-'}, pid={os.getpid()}")
    set_my_commands()
    state = load_state()
    offset = int(state.get("_tg_offset", 0) or 0)
    backoff = 1
    while True:
        try:
            r = tg_long("getUpdates", timeout=POLL_TIMEOUT, offset=offset, **{"allowed_updates": json.dumps(["message", "callback_query"])})
            backoff = 1
            try:
                process_publish_queue(state)
            except Exception as e:
                log(f"publish queue err: {e}")
            for upd in r.get("result", []):
                offset = upd["update_id"] + 1
                state["_tg_offset"] = offset
                save_state(state)
                cb = upd.get("callback_query")
                if cb:
                    try:
                        handle_callback(cb, state)
                    except Exception as e:
                        log(f"callback err: {e}")
                        try:
                            answer_callback(cb["id"], f"errore: {e}", show_alert=True)
                        except Exception:
                            pass
                    maybe_self_restart()
                    continue
                msg = upd.get("message")
                if not msg:
                    continue

                skey = _msg_skey(msg)
                with _active_lock:
                    already = skey in _active_skeys
                    if not already:
                        _active_skeys.add(skey)

                if already:
                    # Stessa sessione ancora in elaborazione: notifica e scarta.
                    try:
                        cid = msg["chat"]["id"]
                        tid = msg.get("message_thread_id")
                        params = {"chat_id": cid, "text": "⏳ sessione occupata, riprova tra poco."}
                        if tid is not None:
                            params["message_thread_id"] = tid
                        tg("sendMessage", **params)
                    except Exception:
                        pass
                    maybe_self_restart()
                    continue

                def _worker(msg=msg, state=state, skey=skey):
                    try:
                        handle(msg, state)
                    except Exception as e:
                        log(f"handle err: {e}")
                        try:
                            tid = msg.get("message_thread_id")
                            params = {"chat_id": msg["chat"]["id"], "text": f"💥 errore bot: {e}"}
                            if tid is not None:
                                params["message_thread_id"] = tid
                            tg("sendMessage", **params)
                        except Exception:
                            pass
                    finally:
                        with _active_lock:
                            _active_skeys.discard(skey)
                        maybe_self_restart()

                _executor.submit(_worker)
        except KeyboardInterrupt:
            log("bye")
            sys.exit(0)
        except Exception as e:
            log(f"poll err: {e}, sleep {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    main()
