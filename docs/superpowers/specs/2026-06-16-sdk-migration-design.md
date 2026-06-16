# cc-telegram-bot: migrazione a Claude Agent SDK

Data: 2026-06-16
Branch: `feat/sdk-migration`

## Obiettivo

Sostituire lo strato di invocazione di Claude Code nel bot (oggi `subprocess` su
`claude -p --output-format stream-json`) con il **Claude Agent SDK** Python
(`claude-agent-sdk`), per abilitare:

1. Streaming del testo token-by-token (edit live del messaggio Telegram).
2. Permessi tool interattivi (bottoni Allow/Deny su Telegram).
3. Parità con l'esperienza Claude Code: display tool-call, thinking, todo list,
   plan mode, input immagini, bottone Stop, diff dei file.

Più: correzione della gestione versioni (User-Agent dinamico) e auto-update.

## Vincoli verificati (feasibility)

- SDK `claude-agent-sdk` 0.2.102. Gira **senza `ANTHROPIC_API_KEY`**, sull'auth
  OAuth del login Claude Code → **abbonamento Max, zero costo per-token**.
  Verificato: query minima ritorna `SDK_OK` con env senza API key.
- `ClaudeAgentOptions` espone tutte le primitive richieste: `resume`,
  `session_id`, `cwd`, `model`, `effort`, `permission_mode`
  (`default|acceptEdits|plan|bypassPermissions`), `can_use_tool` (callback
  permessi), `include_partial_messages` (delta streaming), `fork_session`,
  `ThinkingBlock`/`ToolUseBlock`/`ToolResultBlock` nei messaggi.
- claude CLI installato: **2.1.178** (Mac + lgcloud). Il bot gira h24 su
  **lgcloud** (`cc-telegram-bot.service`, systemd user). Deploy = push + restart.
- Le sessioni restano gli stessi file `~/.claude/projects/<enc>` → la portabilità
  Mac↔lgcloud via Syncthing non cambia.

## Decisioni (confermate con l'utente)

- **Permessi**: default resta `bypassPermissions` (full-auto come oggi). Toggle
  per-sessione `ask` che attiva l'approvazione: in ask-mode i tool read-only
  (Read/Grep/Glob/LS) sono auto-allow, i tool mutating (Write/Edit/Bash/MCP/…)
  chiedono conferma con bottoni Telegram.
- **Feature v1**: tutte. Streaming testo, approvazione tool, display tool-call +
  thinking, todo list, plan mode, input immagini, bottone Stop, diff file.

## Architettura

Migrazione **incrementale**, non rewrite. Nuovo modulo isolato + minima modifica
a `bot.py`.

### `cc_sdk.py` (nuovo) — driver async sopra l'SDK

Espone una funzione sincrona drop-in con la stessa firma/ritorno dell'attuale
`run_claude_streaming`, più callback ricchi:

```
run_turn(
    prompt, session_id, mode, cwd, model, effort, fork,
    on_status,      # str -> None  (label tool corrente / heartbeat) [compat]
    on_text,        # str delta -> None  (streaming testo)
    on_thinking,    # str delta -> None
    on_tool,        # dict(name,input,id) -> None
    on_tool_result, # dict(tool_use_id,is_error,summary) -> None
    on_todos,       # list[dict] -> None
    on_plan,        # str -> None
    can_use_tool,   # async (name, input, ctx) -> Allow|Deny  (ask-mode)
    images,         # list[path] da allegare al prompt
    cancel_event,   # threading.Event per Stop
) -> (final_text, new_session_id, meta)
```

Internals: `anyio.run` di una coroutine che itera `query()` (o
`ClaudeSDKClient`) con `include_partial_messages=True`. Mappa i messaggi SDK
(`SystemMessage` init, `StreamEvent` delta, `AssistantMessage` con
Text/Thinking/ToolUse, `UserMessage` con ToolResult, `ResultMessage`) nei
callback. Cattura il `session_id` dal `ResultMessage` per la continuità resume.

`meta` invariato: `{usage, cost_usd, duration_ms, num_turns}`.

### Permessi (ask-mode) — bridge async↔Telegram

Il bot è sync/threaded; l'SDK callback `can_use_tool` è async e gira nel worker
thread. Quando ask-mode è on e il tool è mutating:

1. La callback genera un token, manda un messaggio Telegram con bottoni
   `Allow`/`Deny` (via l'API sync `tg`, thread-safe), registra una entry
   `pending_perms[token] = threading.Event + result`.
2. Attende l'Event via `anyio.to_thread.run_sync` (non blocca l'event loop).
3. Il main loop, nel gestore `callback_query`, su `perm:<token>:<allow|deny>`
   setta il result e l'Event.
4. La callback ritorna `PermissionResultAllow`/`PermissionResultDeny`.

Timeout di sicurezza (es. 5 min) → deny implicito.

### Mappatura `mode` → SDK
- normale → `permission_mode="bypassPermissions"` + `can_use_tool=None`
- ask-mode → `permission_mode="default"` + `can_use_tool=callback` (auto-allow
  read-only dentro la callback)
- plan-mode → `permission_mode="plan"` (il piano arriva come testo/ExitPlanMode;
  render con bottoni approva/rifiuta che riavviano il turno senza plan)

### Integrazione in `bot.py`
- `run_claude_streaming` resta come fallback subprocess; nuovo path SDK dietro
  env `CC_USE_SDK=1` (default on dopo verifica), così il deploy è reversibile.
- Il caller in `handle()` arricchisce il rendering: throttle edit del messaggio a
  ~1/sec con il testo accumulato da `on_text`; sezioni separate per
  thinking/tool/todo; diff già gestiti dal codice `feat(live)` esistente.
- Nuovi comandi: `/ask` (toggle approvazione), `/plan` (toggle plan mode),
  `Stop` come bottone inline durante il turno. Foto Telegram → scarico in temp →
  `images=[path]`.

### Versioni / auto-update
- `fetch_cc_usage()`: User-Agent dinamico da `claude --version` (cache al boot),
  formato `claude-cli/<ver> (external, cli)`. Niente più `2.0.40` hardcoded.
- `/status` mostra la versione claude attiva.
- lgcloud: systemd timer giornaliero `claude update`; se la versione cambia, il
  bot notifica su Telegram (chat di Luigi).

## Test
- Offline: `cc_sdk.py` testato standalone (streaming delta, resume, can_use_tool
  allow/deny, eventi tool/thinking/todo) senza Telegram.
- Integrazione: smoke su lgcloud con `CC_USE_SDK=1` prima di rendere default.
- Rollback: `CC_USE_SDK=0` o git revert + restart.

## Out of scope (v1)
- Subagents espliciti dal bot, MCP server custom dedicati al bot (l'SDK li
  supporta, ma non richiesti ora).
