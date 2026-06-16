"""cc_live — rendering live di un turno SDK su Telegram.

Glue tra i callback di `cc_sdk.run_turn` e l'I/O Telegram di `bot.py`.

Modello di rendering (stile Claude Code CLI):
- Ogni blocco di testo di Claude (`on_text_block`) diventa un messaggio
  PERSISTENTE separato → la storia della chat resta navigabile.
- Una sola bolla live EFFIMERA (con bottone Stop) mostra "cosa sta succedendo
  ora": il thinking in corsivo, lo status dei tool, l'anteprima del testo in
  streaming. Dopo ogni commit la bolla viene rimossa e ricreata in coda alla
  chat (così l'indicatore "ora" resta sotto la storia, come il cursore CLI).
- Il thinking è l'UNICA cosa effimera "voluta": appare in corsivo nella bolla
  mentre il modello ragiona e sparisce appena arriva testo/azione reale.

`bot.py` passa un namespace `tg` con le sue funzioni I/O (dependency injection,
niente import circolari) e gestisce i callback dei bottoni chiamando
`resolve_permission()` / `request_stop()` di questo modulo.
"""

from __future__ import annotations

import html as _html
import threading
import time
from types import SimpleNamespace
from typing import Any

import cc_sdk

MIN_EDIT_INTERVAL = 1.2     # s minimi tra due edit della bolla live (rate limit TG)
LIVE_TAIL = 3500            # char max mostrati nella bolla live (coda del testo)
THINK_TAIL = 700           # char max di thinking mostrati nella bolla live
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
    context_window: int = 200_000,
    warn_pct: float = 0.8,
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

    state = {
        "live_id": None,      # id bolla live corrente (None = da (ri)creare in coda)
        "header": "💭 avvio…",
        "seg": [],            # delta testo del segmento corrente (anteprima live)
        "thinking": [],       # delta thinking della fase corrente (corsivo, effimero)
        "todos": "",
        "last_edit": 0.0,
        "last_render": "",
        "any_text": False,    # almeno un blocco di testo committato come persistente
        "compacted": False,   # True se è scattata l'auto-compattazione nel turno
    }

    def _render() -> tuple[str, str]:
        """Ritorna (testo, parse_mode) per la bolla live."""
        seg = "".join(state["seg"])
        # Fase di ragionamento: nessun testo ancora, mostra il thinking in corsivo.
        if not seg.strip() and state["thinking"]:
            tail = "".join(state["thinking"])[-THINK_TAIL:]
            return ("🧠 <i>" + _html.escape(tail) + "</i>", "HTML")
        parts = [state["header"]]
        if state["todos"]:
            parts.append(state["todos"])
        if seg.strip():
            tail = seg[-LIVE_TAIL:]
            if len(seg) > LIVE_TAIL:
                tail = "…" + tail
            parts.append(tail)
        return ("\n\n".join(p for p in parts if p), "")

    def _flush(force: bool = False) -> None:
        now = time.time()
        if not force and (now - state["last_edit"]) < MIN_EDIT_INTERVAL:
            return
        text, pm = _render()
        if not text.strip():
            return
        key = f"{pm}\x00{text}"
        if key == state["last_render"]:
            return
        state["last_edit"] = now
        state["last_render"] = key
        if state["live_id"] is None:
            # (ri)crea la bolla in coda alla chat, sotto la storia committata
            state["live_id"] = tg.send_with_keyboard(
                chat_id, text, stop_kb, thread_id=thread_id, parse_mode=pm
            )
        else:
            tg.edit_message_with_keyboard(chat_id, state["live_id"], text, stop_kb, parse_mode=pm)

    def _drop_live() -> None:
        if state["live_id"] is not None:
            tg.delete_message(chat_id, state["live_id"])
            state["live_id"] = None
        state["last_render"] = ""

    def _commit(text: str) -> None:
        """Invia `text` come messaggio persistente e rimuove la bolla live.
        La bolla verrà ricreata in coda al prossimo contenuto (ordine CLI:
        storia sopra, indicatore 'ora' sotto)."""
        if text and text.strip():
            tg.send(chat_id, text, thread_id=thread_id)
            state["any_text"] = True
        state["seg"] = []
        state["thinking"] = []
        state["header"] = "💭 …"
        _drop_live()

    # --- callbacks cc_sdk ---
    def on_text(delta: str) -> None:
        if state["thinking"]:
            state["thinking"] = []   # il ragionamento è finito, ora si scrive
        state["seg"].append(delta)
        _flush()

    def on_text_block(text: str) -> None:
        _commit(text)

    def on_thinking(delta: str) -> None:
        if state["seg"]:
            return                   # già in scrittura: ignora il thinking residuo
        state["thinking"].append(delta)
        _flush()

    def on_status(label: str) -> None:
        state["header"] = label
        if not label.startswith("💭"):
            state["thinking"] = []   # status di tool/azione → esci dal ragionamento
        _flush(force=True)

    def on_todos(items: list) -> None:
        state["todos"] = _fmt_todos(items)
        _flush(force=True)

    def on_plan(plan_text: str) -> None:
        _commit("📐 Piano proposto\n\n" + (plan_text or ""))

    def on_compact(_info: dict) -> None:
        state["compacted"] = True
        state["header"] = "🗜 auto-compattazione contesto…"
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

    # bolla live iniziale (Stop disponibile subito)
    _flush(force=True)

    try:
        reply, new_sid, meta = cc_sdk.run_turn(
            prompt, session_id, sdk_mode, cwd, model, effort, fork,
            on_status=on_status,
            on_text=on_text,
            on_text_block=on_text_block,
            on_thinking=on_thinking,
            on_todos=on_todos,
            on_plan=on_plan,
            on_compact=on_compact,
            ask_permission=(ask_permission if ask else None),
            images=images,
            cancel_event=cancel_event,
            timeout_s=timeout_s,
        )
    finally:
        with _lock:
            _CANCELS.pop(turn_token, None)
        _drop_live()

    if cancel_event.is_set() and not state["any_text"] and not (reply or "").strip():
        reply = "⏹ interrotto"

    # Fallback: nessun blocco di testo committato durante il turno (solo tool,
    # errore, o interruzione) → invia la reply finale come messaggio persistente.
    if not state["any_text"] and (reply or "").strip():
        tg.send(chat_id, reply, thread_id=thread_id)

    # Note di contesto: compattazione + soglia finestra (messaggio a parte).
    notes = []
    if state.get("compacted"):
        notes.append("🗜 Auto-compattazione avvenuta: i turni più vecchi sono stati riassunti dal modello.")
    ctx = (meta or {}).get("context_tokens") or 0
    if ctx and context_window and (ctx / context_window) >= warn_pct:
        pct = ctx / context_window
        notes.append(
            f"⚠️ Contesto al {pct:.0%} ({ctx // 1000}k/{context_window // 1000}k token). "
            "Valuta /compact (reset con riassunto) o /handoff (nuovo topic)."
        )
    if notes:
        tg.send(chat_id, "\n".join(notes), thread_id=thread_id)

    return reply, new_sid, meta
