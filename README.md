# Trump Market-Impact News Agent

A small Python agent that pulls recent Trump-related news from public RSS
feeds, asks the GitHub Copilot chat API (model `claude-opus-4.6 + gpt-5.4 (dual model)`) whether each
item is market-moving, and pushes a Telegram alert when impact is significant.

> ⚠️ For informational/educational use only. Nothing this agent outputs is
> financial advice.

## Files

- `market_pulse.py` — main script (run this).
- `paper_trader.py` — paper trading simulator (auto-invoked by `market_pulse.py`).
- `portfolio_report.py` — daily portfolio summary sent to Telegram.
- `config.py` — configuration (Telegram credentials, model, thresholds, feeds,
  paper-trading parameters).
- `seen_articles.json` — auto-created store of already-processed article IDs.
- `portfolio.json` — auto-created paper-trading portfolio state.
- `market_pulse.log` — rotating log file (also mirrors to stdout).

## Requirements

- Python 3.10+.
- `pip install yfinance` (only dependency outside the standard library; used
  by the paper trader for real-time prices).
- `gh` CLI installed and authenticated (`gh auth login`) so the agent can
  fetch a token with `gh auth token`. Alternatively set `COPILOT_TOKEN`
  directly.
- A GitHub Copilot subscription on the authenticated account.
- A Telegram bot and the chat ID you want to notify.

## Setup

1. **Clone / copy** the three files into a directory, e.g. `~/trump-agent/`.

2. **Authenticate `gh`** (one-time):
   ```bash
   gh auth login
   gh auth token   # confirm a token prints
   ```

3. **Create a Telegram bot:**
   - Talk to [@BotFather](https://t.me/BotFather), `/newbot`, copy the token.
   - Send your bot any message from your account, then visit
     `https://api.telegram.org/bot<TOKEN>/getUpdates` and grab the
     `chat.id` value (or use a group/channel ID).

4. **Configure** — edit `config.py` *or* export env vars:
   ```bash
   export TELEGRAM_BOT_TOKEN="123456:ABC..."
   export TELEGRAM_CHAT_ID="123456789"
   # Optional overrides:
   export COPILOT_MODEL="claude-opus-4.6 + gpt-5.4 (dual model)"
   export COPILOT_TOKEN=""   # leave empty to use `gh auth token`
   ```

5. **Dry run:**
   ```bash
   python3 trump_agent.py
   tail -f trump_agent.log
   ```

## Cron (every 5 minutes)

```cron
*/5 * * * * cd /home/youruser/trump-agent && /usr/bin/python3 trump_agent.py >> /dev/null 2>&1
```

The script takes an exclusive file lock (`.trump_agent.lock`), so overlapping
runs are safe — a second invocation simply exits.

If your cron environment lacks your shell PATH, export the Telegram vars in a
wrapper script or put them in the crontab itself:

```cron
TELEGRAM_BOT_TOKEN=123456:ABC...
TELEGRAM_CHAT_ID=123456789
*/5 * * * * cd /home/youruser/trump-agent && /usr/bin/python3 trump_agent.py
```

## Tuning

`config.py` exposes:

- `RSS_FEEDS` — add/remove sources. Defaults to Google News searches.
- `IMPACT_THRESHOLD` — 0-10; only verdicts at or above this trigger a
  Telegram message. Default `6`.
- `MAX_ARTICLES_PER_RUN` — caps Copilot calls per cron tick. Default `20`.
- `SEEN_RETENTION_DAYS` — how long to remember article IDs. Default `14`.

## How analysis works

For each new article the agent sends headline + summary to the Copilot
endpoint with `response_format=json_object` and this schema:

```json
{
  "impact_score": 0,
  "summary": "...",
  "sectors": ["..."],
  "tickers": ["..."],
  "direction": "bullish|bearish|mixed|neutral",
  "action": "buy|sell|hold",
  "rationale": "..."
}
```

Verdicts at or above `IMPACT_THRESHOLD` are formatted as a MarkdownV2
Telegram message containing what Trump said, affected sectors/tickers,
direction, suggested action, and a link back to the source.

## Paper trading

When `PAPER_TRADING_ENABLED` is true (the default) the agent runs a fully
simulated portfolio alongside the news pipeline. State is persisted to
`portfolio.json`.

Rules (all configurable in `config.py`):

- Initial capital `$10,000` (`PAPER_INITIAL_CAPITAL`).
- On every high-impact `buy` verdict, allocate `10%` of current cash
  (`PAPER_ALLOCATION_PCT`) split equally across the recommended tickers.
- On every high-impact `sell` verdict, liquidate any held positions in
  those tickers.
- Stop-loss at `-5%`, take-profit at `+10%`, max hold time `48h`
  (`PAPER_STOP_LOSS_PCT`, `PAPER_TAKE_PROFIT_PCT`, `PAPER_MAX_HOLD_HOURS`).
- Prices come from Yahoo Finance (`yfinance`).
- Tickers are normalised and filtered against an index/junk blocklist;
  unknown symbols are skipped (logged) rather than executed.

Every cron tick, before processing new articles, the agent sweeps open
positions and closes any that hit the thresholds above. Trade executions
and position closes are pushed to Telegram as plain-text messages so the
MarkdownV2 escape rules don't matter.

### Daily report

`portfolio_report.py` produces a human-readable summary (cash, market value,
unrealized & realized P&L, total return, per-position table) and sends it to
Telegram. Schedule it once per day, e.g. 21:00 UTC (after the US close):

```cron
0 21 * * 1-5 cd /home/youruser/market-pulse && /usr/bin/python3 portfolio_report.py
```

Inspect the current portfolio at any time without sending Telegram:

```bash
python3 paper_trader.py    # prints JSON snapshot
```

## Troubleshooting

- **`gh auth token` returns empty** — run `gh auth login` and ensure the
  account has Copilot access.
- **Copilot HTTP 401/403** — the token has expired or the account lacks
  Copilot; re-auth `gh`.
- **No Telegram messages** — confirm `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`
  are set (the log will warn `Telegram not configured`) and check that the
  bot has been started by the target chat.
- **Empty feed results** — Google News occasionally rate-limits; the agent
  logs a warning and continues with whatever feeds did respond.
- **Paper trader skips a ticker** — the log will show either
  `BUY <SYM> skipped: no price` (yfinance had no quote, often for
  non-US-listed or invalid symbols) or `BUY skipped: per-ticker allocation
  $X < minimum $Y` (cash too low to deploy meaningfully). Adjust
  `PAPER_MIN_ALLOC_PER_TICKER` if needed.
