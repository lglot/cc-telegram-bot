# cc-telegram-bot

Telegram bridge for [Claude Code](https://docs.claude.com/en/docs/claude-code) running headless. Send a message to a Telegram chat → the bot pipes it to `claude -p` and replies with the output. Multi-turn sessions are persisted per chat.

> Single-file Python (~1k lines), zero dependencies outside the stdlib. Runs anywhere `claude` runs.

## Features

- **Per-chat session continuity** — first message starts a `claude` session, follow-ups resume it (`--resume <session_id>`)
- **Streaming progress** — periodic status updates while Claude works (tool calls, elapsed time)
- **Inline media** — Telegram photos/documents are downloaded and passed as file paths in the prompt
- **Reply-to-quote** — `reply` to a message in Telegram and the quoted text is included as context
- **Whitelist** — only configured `chat_id`s are allowed
- **Self-restart on edit** — `touch bot.py` (or any edit) triggers `os.execv` after current turn flushes; sessions and offsets are preserved
- **Telegram Markdown V2 output** with smart fallback if Claude emits malformed markup

## Requirements

- Python 3.10+
- [`claude` CLI](https://docs.claude.com/en/docs/claude-code) installed and authenticated (`claude` subprocess must be runnable as the bot's user)
- Telegram bot token ([@BotFather](https://t.me/BotFather)) and your `chat_id` ([@userinfobot](https://t.me/userinfobot))

## Quickstart

```bash
git clone https://github.com/lglot/cc-telegram-bot.git
cd cc-telegram-bot
cp .env.example .env
# edit .env: set TG_TOKEN and TG_ALLOW_CHAT_IDS
./run.sh
```

Send a message to your bot in Telegram. First reply takes ~5–30s (Claude starts a session). Subsequent messages resume.

## Configuration

All env vars (see `.env.example`):

| Var | Required | Default | Purpose |
|---|---|---|---|
| `TG_TOKEN` | yes | — | Bot token from @BotFather |
| `TG_ALLOW_CHAT_IDS` | yes | — | Comma-separated chat IDs allowed |
| `CC_CWD` | no | `$HOME` | Working dir for `claude` subprocess |
| `CC_MODEL` | no | account default | Override model (e.g. `claude-opus-4-7`) |
| `CC_TIMEOUT` | no | `300` | Subprocess timeout, seconds |
| `STATE_FILE` | no | `~/.cc-telegram-bot.state.json` | Per-chat session state |
| `BOT_HARNESS_NOTE` | no | generic note | First system prompt section. Override to tell Claude how the process manager works (launchd / systemd / docker) so it doesn't try to `kill` itself. |

## Deployment

### macOS (launchd)

```xml
<!-- ~/Library/LaunchAgents/com.example.cc-telegram-bot.plist -->
<plist version="1.0"><dict>
  <key>Label</key><string>com.example.cc-telegram-bot</string>
  <key>ProgramArguments</key><array>
    <string>/path/to/cc-telegram-bot/run.sh</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/tmp/cc-telegram-bot.log</string>
  <key>StandardErrorPath</key><string>/tmp/cc-telegram-bot.err</string>
</dict></plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.example.cc-telegram-bot.plist
```

### Linux (systemd user unit)

```ini
# ~/.config/systemd/user/cc-telegram-bot.service
[Unit]
Description=Telegram bridge for Claude Code
After=network-online.target

[Service]
Type=simple
WorkingDirectory=%h/code/cc-telegram-bot
ExecStart=%h/code/cc-telegram-bot/run.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
```

```bash
loginctl enable-linger $USER
systemctl --user daemon-reload
systemctl --user enable --now cc-telegram-bot
journalctl --user -u cc-telegram-bot -f
```

### Docker (sketch)

A container needs `claude` authenticated. Mount `~/.claude/` from a host where you've already run `claude login`. Not recommended for production multi-tenant use — `claude`'s token is sensitive.

## Self-restart on edit

When `bot.py`'s mtime changes after the current turn completes, the bot:

1. Validates the new file with `ast.parse`
2. Persists Telegram update offset
3. `os.execv` replaces itself with the new code

This means you can `touch bot.py` (or `git pull`) to force a clean restart without losing in-flight messages.

## State file

`~/.cc-telegram-bot.state.json` holds per-chat session IDs:

```json
{ "chat_id_123": { "session_id": "...", "last_active": 1715000000 } }
```

Delete the file (or specific keys) to force fresh sessions.

## Security

- Telegram updates with `from.id` not in `TG_ALLOW_CHAT_IDS` are dropped before any subprocess is spawned
- Claude inherits the bot user's filesystem permissions — same as running `claude` interactively. Don't run as root.
- Inline media is downloaded into a per-chat tmp dir and the path is passed to Claude. Claude can read the file. Vet your allowlist.

## License

MIT — see [LICENSE](LICENSE).

## Author

[Luigi Lotito](https://github.com/lglot). PRs welcome.
