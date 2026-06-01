"""Configuration for the Market Pulse news agent.

Override any of these via environment variables of the same name.
Copy this file to config.py and fill in your real values.
"""

import os

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

# --- GitHub Copilot API ---
COPILOT_API_URL = "https://api.githubcopilot.com/chat/completions"
COPILOT_MODEL = os.environ.get("COPILOT_MODEL", "claude-opus-4.6")
COPILOT_MODEL_2 = os.environ.get("COPILOT_MODEL_2", "gpt-5.4")
# If empty, the agent will run `gh auth token` to retrieve a token.
COPILOT_TOKEN = os.environ.get("COPILOT_TOKEN", "")

# --- News sources (RSS feeds, no API key needed) ---
RSS_FEEDS = [
    # X/Twitter via Nitter RSS (may return 429 — see alternatives below)
    "https://nitter.net/realDonaldTrump/rss",
    "https://nitter.net/federalreserve/rss",
    "https://nitter.net/POTUS/rss",
    # Google News fallback (Reuters + Bloomberg + CNBC)
    "https://news.google.com/rss/search?q=Trump+OR+Federal+Reserve+when:1d+site:reuters.com&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Trump+OR+Fed+when:1d+site:bloomberg.com&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Trump+OR+Fed+when:1d+site:cnbc.com&hl=en-US&gl=US&ceid=US:en",
    #
    # Alternative X/Twitter RSS sources (uncomment if Nitter is down):
    # "https://rsshub.app/twitter/user/realDonaldTrump",
    # "https://rsshub.app/twitter/user/federalreserve",
    # "https://rsshub.app/twitter/user/POTUS",
    # "https://twiiit.com/realDonaldTrump/rss",
    # "https://twiiit.com/federalreserve/rss",
    # "https://nitter.privacydev.net/realDonaldTrump/rss",
    # "https://nitter.privacydev.net/federalreserve/rss",
]

# --- Files ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SEEN_FILE = os.path.join(BASE_DIR, "seen_articles.json")
LOG_FILE = os.path.join(BASE_DIR, "market_pulse.log")

# --- Behavior ---
MAX_ARTICLES_PER_RUN = 20          # cap LLM calls per run
SEEN_RETENTION_DAYS = 14           # prune old entries from the seen file
REQUEST_TIMEOUT = 30               # seconds
HTTP_USER_AGENT = "trump-market-agent/1.0 (+https://example.com)"

# Only notify when impact score (0-10) is at least this high.
IMPACT_THRESHOLD = 6

# --- Paper trading ---
# Master switch. When False, paper trading code paths are no-ops.
PAPER_TRADING_ENABLED = os.environ.get("PAPER_TRADING_ENABLED", "1") not in ("0", "false", "False", "")
PAPER_INITIAL_CAPITAL = float(os.environ.get("PAPER_INITIAL_CAPITAL", "10000"))
# Fraction of current cash deployed on each 'buy' signal (split equally across tickers).
PAPER_ALLOCATION_PCT = float(os.environ.get("PAPER_ALLOCATION_PCT", "0.10"))
# Risk management thresholds applied per-position on every position check.
PAPER_STOP_LOSS_PCT = float(os.environ.get("PAPER_STOP_LOSS_PCT", "-0.05"))
PAPER_TAKE_PROFIT_PCT = float(os.environ.get("PAPER_TAKE_PROFIT_PCT", "0.10"))
PAPER_MAX_HOLD_HOURS = float(os.environ.get("PAPER_MAX_HOLD_HOURS", "48"))
# Minimum dollars to allocate per ticker; below this we skip the order.
PAPER_MIN_ALLOC_PER_TICKER = float(os.environ.get("PAPER_MIN_ALLOC_PER_TICKER", "50"))
# Maximum number of tickers per single buy signal (caps over-fragmentation).
PAPER_MAX_TICKERS_PER_SIGNAL = int(os.environ.get("PAPER_MAX_TICKERS_PER_SIGNAL", "5"))
PORTFOLIO_FILE = os.path.join(BASE_DIR, "portfolio.json")
