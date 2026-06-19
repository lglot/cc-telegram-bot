"""cc_sdk — driver Claude Agent SDK per cc-telegram-bot.

Sostituisce l'invocazione `claude -p --output-format stream-json` via subprocess
con il Claude Agent SDK Python (`claude-agent-sdk`). Gira sull'auth OAuth del
login Claude Code (abbonamento, niente ANTHROPIC_API_KEY → niente costo per-token).

Modulo isolato: NESSUNA dipendenza da Telegram. bot.py passa callback e una
funzione `ask_permission` sincrona; qui facciamo il bridge async↔sync.

API principale: `run_turn(...) -> (final_text, new_session_id, meta)`, stessa
forma di ritorno dell'attuale `run_claude_streaming`.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Callable

import anyio

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    HookMatcher,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
    query,
)

def _thinking_config() -> "dict | None":
    """Config extended-thinking per ClaudeAgentOptions (env `CC_THINKING`).

    IMPORTANTE: opus 4.7+ di default OMETTE il testo del thinking (solo
    signature) → niente `thinking_delta` in streaming → niente bolla "🧠" in
    corsivo. Per riceverlo serve `display: "summarized"`.

    Valori `CC_THINKING`: "adaptive"/"on" (default) → adaptive + summarized;
    un intero → budget fisso + summarized; "off"/"disabled" → niente thinking.
    """
    val = (os.environ.get("CC_THINKING", "adaptive") or "").strip().lower()
    if val in ("", "off", "0", "no", "false", "disabled", "none"):
        return None
    if val.isdigit():
        return {"type": "enabled", "budget_tokens": int(val), "display": "summarized"}
    return {"type": "adaptive", "display": "summarized"}


# Tool senza effetti sul filesystem: in ask-mode auto-allow (niente bottoni).
READONLY_TOOLS = {
    "Read", "Grep", "Glob", "LS", "NotebookRead",
    "TodoWrite", "WebSearch", "WebFetch",
}


def _summarize_input(name: str, inp: dict) -> str:
    """Riassunto compatto degli args di un tool (per status/approvazione)."""
    inp = inp or {}
    if name == "Bash":
        return str(inp.get("command", ""))[:160]
    if name in ("Read", "Edit", "Write", "MultiEdit", "NotebookEdit"):
        return str(inp.get("file_path") or inp.get("notebook_path") or "")[:160]
    if name in ("Grep", "Glob"):
        q = inp.get("pattern") or inp.get("query") or ""
        path = inp.get("path") or ""
        return f"{q} {path}".strip()[:160]
    if name in ("WebFetch", "WebSearch"):
        return str(inp.get("url") or inp.get("query") or "")[:160]
    if name == "Task":
        return str(inp.get("description") or inp.get("subagent_type") or "")[:160]
    # fallback: primo valore stringa
    for v in inp.values():
        if isinstance(v, str) and v:
            return v[:160]
    return ""


class Turn:
    """Stato di un singolo turno; raccoglie testo, thinking, sid, meta."""

    def __init__(self) -> None:
        self.text_parts: list[str] = []      # testo finale accumulato (delta)
        self.last_block_text = ""            # ultimo TextBlock completo (fallback)
        self.session_id: str | None = None
        self.meta: dict = {}
        self.final_result: str | None = None
        self.is_error = False
        self.error_text = ""
        # Occupazione contesto = input dell'ULTIMA chiamata API del turno
        # (message_start.usage). NON la somma cumulativa della usage del
        # ResultMessage, che conta tutte le chiamate del turno (ogni tool-call
        # rimanda il contesto → cache_read sommata N volte → % assurde tipo 555%).
        self.input_context_tokens = 0

    @property
    def accumulated(self) -> str:
        return "".join(self.text_parts)


async def _drive(
    prompt: str,
    session_id: str | None,
    mode: str,
    cwd: str,
    model: str | None,
    effort: str | None,
    fork: bool,
    callbacks: dict[str, Callable | None],
    ask_permission: "Callable[[str, dict, str], bool] | None",
    cancel_event: "threading.Event | None",
    timeout_s: int,
) -> Turn:
    turn = Turn()
    on_status = callbacks.get("on_status")
    on_text = callbacks.get("on_text")
    on_text_block = callbacks.get("on_text_block")
    on_thinking = callbacks.get("on_thinking")
    on_tool = callbacks.get("on_tool")
    on_tool_result = callbacks.get("on_tool_result")
    on_todos = callbacks.get("on_todos")
    on_plan = callbacks.get("on_plan")
    on_compact = callbacks.get("on_compact")

    pending_tools: dict[str, str] = {}

    # --- approvazione interattiva via hook PreToolUse ---
    # Gli hook firano su OGNI tool a prescindere dall'allowlist dei settings (a
    # differenza di can_use_tool, che scatta solo quando il CLI prompterebbe).
    # Read-only -> allow automatico; mutating -> ask_permission (bridge a Telegram).
    async def _pre_tool(input_data: dict, tool_use_id: str | None, context: Any):
        name = input_data.get("tool_name") or ""
        if name in READONLY_TOOLS or ask_permission is None:
            decision = "allow"
        else:
            tool_input = input_data.get("tool_input") or {}
            summary = _summarize_input(name, tool_input)
            try:
                allowed = await anyio.to_thread.run_sync(
                    ask_permission, name, tool_input, summary
                )
            except Exception:
                allowed = False
            decision = "allow" if allowed else "deny"
        return {"hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": decision,
            "permissionDecisionReason": "ask-mode (Telegram)",
        }}

    # Hook PreCompact: scatta quando Claude Code sta per auto-compattare la
    # sessione (contesto vicino al limite della finestra). Solo osservatore.
    async def _pre_compact(input_data: dict, tool_use_id: str | None, context: Any):
        if on_compact is not None:
            try:
                on_compact(input_data or {})
            except Exception:
                pass
        return {}

    hooks: dict = {}
    if ask_permission is not None and mode == "default":
        hooks["PreToolUse"] = [HookMatcher(hooks=[_pre_tool])]
    if on_compact is not None:
        hooks["PreCompact"] = [HookMatcher(hooks=[_pre_compact])]
    hooks = hooks or None

    options = ClaudeAgentOptions(
        permission_mode=mode,                         # bypassPermissions|default|plan|acceptEdits
        resume=session_id or None,
        fork_session=bool(session_id and fork),
        cwd=cwd,
        model=model or None,
        effort=effort or None,                        # type: ignore[arg-type]
        include_partial_messages=True,                # delta streaming testo/thinking
        thinking=_thinking_config(),                  # display summarized → thinking_delta visibili
        hooks=hooks,
    )

    deadline = time.time() + timeout_s

    async def _iterate() -> None:
        async for msg in query(prompt=prompt, options=options):
            if cancel_event is not None and cancel_event.is_set():
                turn.error_text = "⏹ interrotto"
                break
            if time.time() > deadline:
                turn.error_text = f"⏱ timeout dopo {timeout_s}s"
                break

            if isinstance(msg, SystemMessage):
                if msg.subtype == "init":
                    sid = (msg.data or {}).get("session_id")
                    if sid:
                        turn.session_id = sid
                    if on_status:
                        on_status("💭 ragionamento…")

            elif isinstance(msg, StreamEvent):
                ev = msg.event or {}
                if ev.get("type") == "message_start":
                    # usage della singola chiamata: input + cache. Sovrascrive a
                    # ogni chiamata → resta quella dell'ultima (= occupazione attuale).
                    u = ((ev.get("message") or {}).get("usage")) or {}
                    s = (
                        (u.get("input_tokens") or 0)
                        + (u.get("cache_read_input_tokens") or 0)
                        + (u.get("cache_creation_input_tokens") or 0)
                    )
                    if s:
                        turn.input_context_tokens = s
                elif ev.get("type") == "content_block_delta":
                    delta = ev.get("delta") or {}
                    dt = delta.get("type")
                    if dt == "text_delta":
                        chunk = delta.get("text") or ""
                        if chunk:
                            turn.text_parts.append(chunk)
                            if on_text:
                                on_text(chunk)
                    elif dt == "thinking_delta":
                        chunk = delta.get("thinking") or ""
                        if chunk and on_thinking:
                            on_thinking(chunk)

            elif isinstance(msg, AssistantMessage):
                for block in msg.content or []:
                    if isinstance(block, ToolUseBlock):
                        pending_tools[block.id] = block.name
                        summary = _summarize_input(block.name, block.input or {})
                        if block.name == "TodoWrite" and on_todos:
                            on_todos((block.input or {}).get("todos") or [])
                        if block.name in ("ExitPlanMode", "exit_plan_mode") and on_plan:
                            on_plan((block.input or {}).get("plan") or "")
                        if on_tool:
                            on_tool({"name": block.name, "input": block.input or {}, "id": block.id})
                        if on_status:
                            label = f"🔧 {block.name}"
                            if summary:
                                label += f"  {summary}"
                            on_status(label)
                    elif isinstance(block, ThinkingBlock):
                        # delta già inviati via StreamEvent; qui ignoriamo il blocco completo
                        pass
                    elif isinstance(block, TextBlock):
                        if (block.text or "").strip():
                            turn.last_block_text = block.text
                            # blocco testo completo → committalo come messaggio
                            # persistente (il rendering live lo mostra separato).
                            if on_text_block:
                                try:
                                    on_text_block(block.text)
                                except Exception:
                                    pass

            elif isinstance(msg, UserMessage):
                for block in (msg.content or []):
                    if isinstance(block, ToolResultBlock):
                        name = pending_tools.pop(block.tool_use_id, None)
                        if on_tool_result:
                            on_tool_result({
                                "tool_use_id": block.tool_use_id,
                                "is_error": bool(block.is_error),
                                "name": name,
                            })
                        if name and on_status:
                            on_status(f"✓ {name} → continuo…")

            elif isinstance(msg, ResultMessage):
                turn.session_id = msg.session_id or turn.session_id
                turn.final_result = msg.result
                turn.is_error = bool(msg.is_error)
                u = msg.usage or {}
                # Occupazione corrente del contesto = input dell'ultima chiamata
                # (catturato dai message_start). La usage del ResultMessage è
                # cumulativa sul turno → NON usabile come dimensione finestra.
                # Fallback alla cumulativa solo se nessun message_start visto.
                ctx_tokens = turn.input_context_tokens or (
                    (u.get("input_tokens") or 0)
                    + (u.get("cache_read_input_tokens") or 0)
                    + (u.get("cache_creation_input_tokens") or 0)
                )
                turn.meta = {
                    "usage": u,
                    "cost_usd": msg.total_cost_usd or 0.0,
                    "duration_ms": msg.duration_ms or 0,
                    "num_turns": msg.num_turns or 0,
                    "context_tokens": ctx_tokens,
                }
                if msg.is_error and not (msg.result or "").strip():
                    turn.error_text = f"❌ {msg.subtype or 'error'}"

    try:
        await _iterate()
    except Exception as e:  # noqa: BLE001
        turn.error_text = turn.error_text or f"❌ sdk err: {e}"
    return turn


def run_turn(
    prompt: str,
    session_id: str | None,
    mode: str,
    cwd: str,
    model: str | None = None,
    effort: str | None = None,
    fork: bool = False,
    *,
    on_status: "Callable[[str], None] | None" = None,
    on_text: "Callable[[str], None] | None" = None,
    on_text_block: "Callable[[str], None] | None" = None,
    on_thinking: "Callable[[str], None] | None" = None,
    on_tool: "Callable[[dict], None] | None" = None,
    on_tool_result: "Callable[[dict], None] | None" = None,
    on_todos: "Callable[[list], None] | None" = None,
    on_plan: "Callable[[str], None] | None" = None,
    on_compact: "Callable[[dict], None] | None" = None,
    ask_permission: "Callable[[str, dict, str], bool] | None" = None,
    images: "list[str] | None" = None,
    cancel_event: "threading.Event | None" = None,
    timeout_s: int = 1800,
) -> tuple[str, str | None, dict]:
    """Esegue un turno via SDK. Ritorna (testo_finale, new_session_id, meta).

    `mode`: permission_mode SDK (bypassPermissions | default | plan | acceptEdits).
    In `default` + `ask_permission` fornito → approvazione interattiva dei tool
    mutating (read-only auto-allow). `images`: path locali allegati al prompt.
    """
    if images:
        refs = "\n".join(f"[immagine allegata: {p}] (usa il tool Read per vederla)" for p in images)
        prompt = f"{prompt}\n\n{refs}" if prompt else refs

    callbacks = {
        "on_status": on_status,
        "on_text": on_text,
        "on_text_block": on_text_block,
        "on_thinking": on_thinking,
        "on_tool": on_tool,
        "on_tool_result": on_tool_result,
        "on_todos": on_todos,
        "on_plan": on_plan,
        "on_compact": on_compact,
    }

    turn: Turn = anyio.run(
        _drive,
        prompt, session_id, mode, cwd, model, effort, fork,
        callbacks, ask_permission, cancel_event, timeout_s,
    )

    final = (turn.final_result or "").strip() or turn.accumulated.strip() or turn.last_block_text.strip()
    if turn.error_text and not final:
        final = turn.error_text
    elif turn.error_text and final:
        final = f"{final}\n\n{turn.error_text}"
    return final or "(vuoto)", turn.session_id or session_id, turn.meta


if __name__ == "__main__":
    # smoke test manuale: python cc_sdk.py "prompt" [cwd]
    import sys
    p = sys.argv[1] if len(sys.argv) > 1 else "Reply with exactly: SDK_OK"
    cwd_ = sys.argv[2] if len(sys.argv) > 2 else "."

    def _t(d): print(d, end="", flush=True)
    def _s(l): print(f"\n[status] {l}")

    txt, sid, meta = run_turn(p, None, "bypassPermissions", cwd_, on_text=_t, on_status=_s)
    print(f"\n--- final ---\n{txt}\n--- sid={sid} meta={meta}")
