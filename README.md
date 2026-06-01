# Market Pulse

A Python agent that monitors Trump and Federal Reserve news via public RSS
feeds, uses dual AI models (`claude-opus-4.6` + `gpt-5.4`) through the GitHub
Copilot API to assess market impact, and sends Telegram alerts when both models
agree on high-impact events. Includes an interactive Telegram bot for on-demand
reports.

> ⚠️ For informational/educational use only. Nothing this agent outputs is
> financial advice.

## How It Works

1. **RSS fetching** — pulls headlines from Nitter (X/Twitter) and Google News RSS feeds.
2. **Dual-model analysis** — each article is analyzed by both `claude-opus-4.6` and `gpt-5.4`; only articles where both models score impact ≥ 6 trigger alerts.
3. **Telegram notifications** — high-impact verdicts are sent as bilingual (EN/ZH) alerts with affected sectors, tickers, direction, and action.
4. **Paper trading** — optionally executes simulated trades based on AI recommendations using live Yahoo Finance prices.

## Files

| File | Description |
|---|---|
| `market_pulse.py` | Cron-driven news pipeline (fetch → analyze → alert) |
| `bot_server.py` | Long-running Telegram bot server (`/report`, `/status` commands) |
| `config.py` | Your local configuration (git-ignored) |
| `config.example.py` | Template config with placeholder values |
| `seen_articles.json` | Auto-created de-duplication store |
| `market_pulse.log` | Rotating log file |

## Requirements

- Python 3.10+
- `pip install yfinance` (only non-stdlib dependency; used by the paper trader)
- `gh` CLI installed and authenticated (`gh auth login`), or set `COPILOT_TOKEN` directly
- A GitHub Copilot subscription on the authenticated account
- A Telegram bot token and chat ID

## Setup

1. **Clone the repo:**
   ```bash
   git clone https://github.com/baiwei0427/market-pulse.git
   cd market-pulse
   ```

2. **Create your config:**
   ```bash
   cp config.example.py config.py
   ```
   Edit `config.py` and fill in your real `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.

3. **Create a Telegram bot** (if you don't have one):
   - Talk to [@BotFather](https://t.me/BotFather), run `/newbot`, copy the token.
   - Send your bot any message, then visit
     `https://api.telegram.org/bot<TOKEN>/getUpdates` to find your `chat.id`.

4. **Authenticate `gh`:**
   ```bash
   gh auth login
   gh auth token   # confirm a token prints
   ```

5. **Install dependencies:**
   ```bash
   pip install yfinance
   ```

6. **Test run:**
   ```bash
   python3 market_pulse.py
   tail -f market_pulse.log
   ```

## Running

### Cron — automated news monitoring (`market_pulse.py`)

Run every 5 minutes to continuously monitor news:

```cron
*/5 * * * * cd /home/youruser/market-pulse && /usr/bin/python3 market_pulse.py >> /dev/null 2>&1
```

The script uses an exclusive file lock (`.market_pulse.lock`) so overlapping runs are safe.

### Telegram bot — interactive commands (`bot_server.py`)

The bot server provides on-demand commands:

| Command | Description |
|---|---|
| `/report` | Fetch latest RSS articles, analyze with dual models, send ranked report |
| `/status` | Show bot uptime, last report stats, and active model configuration |

#### Run with systemd (recommended)

Create `/etc/systemd/system/market-pulse-bot.service`:

```ini
[Unit]
Description=Market Pulse Telegram Bot
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/home/youruser/market-pulse
ExecStart=/usr/bin/python3 /home/youruser/market-pulse/bot_server.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable market-pulse-bot
sudo systemctl start market-pulse-bot
sudo systemctl status market-pulse-bot
```

## Tuning

`config.py` exposes:

- `RSS_FEEDS` — add/remove sources. Nitter feeds may return 429; alternative X/Twitter RSS sources are listed as comments.
- `IMPACT_THRESHOLD` — 0–10; only verdicts at or above this trigger alerts. Default `6`.
- `MAX_ARTICLES_PER_RUN` — caps Copilot API calls per run. Default `20`.
- `SEEN_RETENTION_DAYS` — how long to remember article IDs. Default `14`.
- `COPILOT_MODEL` / `COPILOT_MODEL_2` — the two models used for dual analysis.

## Paper Trading

When `PAPER_TRADING_ENABLED` is true (default), the agent runs a simulated
portfolio alongside the news pipeline, persisted to `portfolio.json`.

- Initial capital: `$10,000`
- Allocation per signal: `10%` of cash, split across recommended tickers
- Stop-loss: `-5%`, take-profit: `+10%`, max hold: `48h`
- Prices from Yahoo Finance (`yfinance`)

All parameters are configurable in `config.py`.

## Troubleshooting

- **`gh auth token` returns empty** — run `gh auth login`; ensure the account has Copilot.
- **Copilot HTTP 401/403** — token expired or no Copilot subscription; re-auth `gh`.
- **No Telegram messages** — check `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` in `config.py`.
- **RSS feed 429 errors** — Nitter rate-limits; the agent logs a warning and continues. Try alternative RSS sources in `config.py`.

## License

[MIT](LICENSE)
