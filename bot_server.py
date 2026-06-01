#!/usr/bin/env python3
"""Telegram bot server for Market Pulse.

Long-running service that listens for commands via Telegram Bot API
long polling (getUpdates). Supports:
  /report  — fetch RSS, analyze top 10 articles, send bilingual summary
  /status  — show bot uptime and last run stats
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone

import config
from market_pulse import (
    Article,
    analyze_article_dual,
    acquire_lock as mp_acquire_lock,
    fetch_feed,
    gather_articles,
    get_copilot_token,
    http_request,
    load_seen,
    save_seen,
    process as mp_process,
    setup_logging,
    logger,
    send_telegram_text,
    _md_escape,
)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_start_time: datetime = datetime.now(timezone.utc)
_last_report: dict = {}  # stats from last /report run
_running = True

# Shared lock so the background analysis and /report never run concurrently.
_analysis_lock = threading.Lock()

BACKGROUND_INTERVAL = 300  # seconds between automatic analysis runs


def _uptime() -> str:
    delta = datetime.now(timezone.utc) - _start_time
    hours, rem = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(rem, 60)
    days, hours = divmod(hours, 24)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m {seconds}s")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Telegram Bot API helpers
# ---------------------------------------------------------------------------

_BASE_URL = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"


def tg_get_updates(offset: int | None = None, timeout: int = 30) -> list[dict]:
    params = {"timeout": timeout, "allowed_updates": '["message"]'}
    if offset is not None:
        params["offset"] = offset
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{_BASE_URL}/getUpdates?{qs}"
    try:
        status, body = http_request(url, timeout=timeout + 10)
    except Exception as e:
        logger.error("getUpdates failed: %s", e)
        return []
    if status != 200:
        logger.error("getUpdates HTTP %s: %s", status, body[:200])
        return []
    try:
        data = json.loads(body)
        if data.get("ok"):
            return data.get("result", [])
    except (json.JSONDecodeError, KeyError):
        pass
    return []


def tg_send(chat_id: int | str, text: str) -> bool:
    """Send a plain-text message to a specific chat."""
    url = f"{_BASE_URL}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }).encode("utf-8")
    try:
        status, resp = http_request(
            url, method="POST",
            headers={"Content-Type": "application/json"},
            data=payload,
        )
    except Exception as e:
        logger.error("sendMessage failed: %s", e)
        return False
    if status != 200:
        logger.error("sendMessage HTTP %s: %s", status, resp[:300])
        return False
    return True


def tg_send_long(chat_id: int | str, text: str) -> bool:
    """Send a message, splitting into 4096-char chunks if needed."""
    limit = 4096
    if len(text) <= limit:
        return tg_send(chat_id, text)
    ok = True
    for i in range(0, len(text), limit):
        if not tg_send(chat_id, text[i:i + limit]):
            ok = False
    return ok


# ---------------------------------------------------------------------------
# /report command
# ---------------------------------------------------------------------------


def handle_report(chat_id: int | str) -> None:
    global _last_report
    tg_send(chat_id, "⏳ Fetching news and analyzing… this may take a minute.")

    with _analysis_lock:
        try:
            token = get_copilot_token()
        except Exception as e:
            tg_send(chat_id, f"❌ Failed to get API token: {e}")
            return

        articles = gather_articles()
        if not articles:
            tg_send(chat_id, "No articles found from RSS feeds.")
            _last_report = {"time": datetime.now(timezone.utc).isoformat(), "articles": 0}
            return

        # Analyze all articles (up to 20) and rank by impact
        tg_send(chat_id, f"📰 Found {len(articles)} articles. Analyzing top candidates…")

        results: list[tuple[Article, dict]] = []
        analyzed = 0
        cap = min(len(articles), config.MAX_ARTICLES_PER_RUN)

        for article in articles[:cap]:
            analyzed += 1
            try:
                verdict = analyze_article_dual(article, token)
            except Exception as e:
                logger.error("Analysis error for '%s': %s", article.title[:80], e)
                continue
            if verdict is None:
                continue
            results.append((article, verdict))

        # Sort by impact score descending, take top 10
        results.sort(key=lambda x: float(x[1].get("impact_score", 0)), reverse=True)
        top = results[:10]

        if not top:
            tg_send(chat_id, "Analysis complete but no articles produced valid results.")
            _last_report = {
                "time": datetime.now(timezone.utc).isoformat(),
                "articles": len(articles),
                "analyzed": analyzed,
                "top": 0,
            }
            return

        # Aggregate sentiment and tickers
        buy_tickers: dict[str, list[str]] = {}   # ticker -> reasons
        sell_tickers: dict[str, list[str]] = {}
        sentiments = {"bullish": 0, "bearish": 0, "neutral": 0, "mixed": 0}

        for article, verdict in top:
            direction = (verdict.get("direction") or "neutral").lower()
            if direction in sentiments:
                sentiments[direction] += 1
            else:
                sentiments["neutral"] += 1

            action = (verdict.get("action") or "hold").lower()
            tickers = verdict.get("tickers") or []
            rationale = verdict.get("rationale") or ""
            for t in tickers:
                if action == "buy":
                    buy_tickers.setdefault(t, []).append(rationale[:60])
                elif action == "sell":
                    sell_tickers.setdefault(t, []).append(rationale[:60])

        # Determine overall sentiment
        dominant = max(sentiments, key=sentiments.get)
        dominant_zh = {"bullish": "看涨", "bearish": "看跌", "neutral": "中性", "mixed": "混合"}

        # Build report
        lines = [
            "📊 MARKET PULSE REPORT / 市场脉搏报告",
            f"🕐 {datetime.now(timezone(timedelta(hours=-7))).strftime('%Y-%m-%d %H:%M PST')}",
            f"📰 {len(articles)} articles scanned, {analyzed} analyzed",
            "",
            f"🎯 Overall Sentiment / 总体情绪: {dominant.upper()} / {dominant_zh.get(dominant, dominant)}",
            f"   (Bullish: {sentiments['bullish']} | Bearish: {sentiments['bearish']} "
            f"| Neutral: {sentiments['neutral']} | Mixed: {sentiments['mixed']})",
            "",
            "═══ TOP NEWS BY IMPACT / 影响力排名 ═══",
        ]

        for i, (article, verdict) in enumerate(top, 1):
            score = verdict.get("impact_score", 0)
            if isinstance(score, float):
                score_str = f"{score:.1f}"
            else:
                score_str = str(score)
            summary_en = verdict.get("summary") or article.title
            summary_zh = verdict.get("summary_zh") or ""
            direction = (verdict.get("direction") or "neutral").upper()
            direction_zh = verdict.get("direction_zh") or ""
            action = (verdict.get("action") or "hold").upper()
            action_zh = verdict.get("action_zh") or ""
            tickers = ", ".join(verdict.get("tickers") or []) or "—"

            # Per-model details
            model_parts = []
            for m in verdict.get("models") or []:
                m_name = m.get("model", "?")
                m_score = m.get("score", "?")
                model_parts.append(f"{m_name}: {m_score}/10")
            model_line = " | ".join(model_parts) if model_parts else ""

            lines.append(f"\n#{i} — Impact {score_str}/10 [{direction}/{direction_zh}]")
            lines.append(f"   {summary_en}")
            if summary_zh:
                lines.append(f"   {summary_zh}")
            lines.append(f"   Action: {action} ({action_zh}) | Tickers: {tickers}")
            if model_line:
                lines.append(f"   Models: {model_line}")
            rationale = verdict.get("rationale") or ""
            rationale_zh = verdict.get("rationale_zh") or ""
            if rationale:
                lines.append(f"   → {rationale}")
            if rationale_zh:
                lines.append(f"   → {rationale_zh}")

        # Recommendations section
        lines.append("\n═══ RECOMMENDATIONS / 建议 ═══")
        if buy_tickers:
            lines.append("\n🟢 BUY / 买入:")
            for t, reasons in buy_tickers.items():
                lines.append(f"   {t}: {reasons[0]}")
        if sell_tickers:
            lines.append("\n🔴 SELL / 卖出:")
            for t, reasons in sell_tickers.items():
                lines.append(f"   {t}: {reasons[0]}")
        if not buy_tickers and not sell_tickers:
            lines.append("   No strong buy/sell signals. / 无强烈买卖信号。")

        lines.append("\n⚠️ This is AI analysis, not financial advice. / 此为AI分析，非投资建议。")

        report_text = "\n".join(lines)
        tg_send_long(chat_id, report_text)

        _last_report = {
            "time": datetime.now(timezone.utc).isoformat(),
            "articles": len(articles),
            "analyzed": analyzed,
            "top": len(top),
            "sentiment": dominant,
            "buy_tickers": list(buy_tickers.keys()),
            "sell_tickers": list(sell_tickers.keys()),
        }
        logger.info("/report completed: %s", _last_report)


# ---------------------------------------------------------------------------
# /status command
# ---------------------------------------------------------------------------


def handle_status(chat_id: int | str) -> None:
    lines = [
        "🤖 Market Pulse Bot Status",
        f"⏱ Uptime: {_uptime()}",
        f"🚀 Started: {_start_time.strftime('%Y-%m-%d %H:%M UTC')}",
    ]
    if _last_report:
        lines.append(f"\n📊 Last report: {_last_report.get('time', 'n/a')}")
        lines.append(f"   Articles found: {_last_report.get('articles', 0)}")
        lines.append(f"   Analyzed: {_last_report.get('analyzed', 0)}")
        lines.append(f"   Top results: {_last_report.get('top', 0)}")
        lines.append(f"   Sentiment: {_last_report.get('sentiment', 'n/a')}")
        buy = _last_report.get("buy_tickers", [])
        sell = _last_report.get("sell_tickers", [])
        if buy:
            lines.append(f"   Buy tickers: {', '.join(buy)}")
        if sell:
            lines.append(f"   Sell tickers: {', '.join(sell)}")
    else:
        lines.append("\nNo report has been run yet.")
    lines.append(f"\nModels: {config.COPILOT_MODEL} + {config.COPILOT_MODEL_2}")
    tg_send(chat_id, "\n".join(lines))


# ---------------------------------------------------------------------------
# Background analysis (runs every BACKGROUND_INTERVAL seconds)
# ---------------------------------------------------------------------------


def _run_analysis() -> None:
    """Run the market_pulse pipeline once (fetch → analyze → notify).

    Mirrors what market_pulse.py's main loop would do: acquire the file lock
    to stay safe against concurrent cron invocations, then process only
    unseen articles and send Telegram alerts for high-impact ones.
    """
    logger.info("Background analysis: starting run")
    with _analysis_lock:
        fd = mp_acquire_lock()
        if fd is None:
            logger.warning("Background analysis: file lock busy; skipping this run")
            return
        try:
            token = get_copilot_token()
            seen = load_seen()
            articles = gather_articles()
            if articles:
                notified = mp_process(articles, seen, token)
                save_seen(seen)
                logger.info("Background analysis: done — %s notifications sent", notified)
            else:
                logger.info("Background analysis: no articles found")
        except Exception:
            logger.exception("Background analysis: unhandled error")
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


def _background_loop() -> None:
    """Daemon loop: run analysis, sleep BACKGROUND_INTERVAL seconds, repeat."""
    logger.info("Background analysis thread started (interval=%ds)", BACKGROUND_INTERVAL)
    while _running:
        _run_analysis()
        # Sleep in 1-second ticks so shutdown is responsive.
        for _ in range(BACKGROUND_INTERVAL):
            if not _running:
                break
            time.sleep(1)
    logger.info("Background analysis thread stopped")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def handle_update(update: dict) -> None:
    msg = update.get("message")
    if not msg:
        return
    text = (msg.get("text") or "").strip()
    chat_id = msg["chat"]["id"]

    if text == "/report" or text.startswith("/report@"):
        logger.info("/report from chat %s", chat_id)
        handle_report(chat_id)
    elif text == "/status" or text.startswith("/status@"):
        logger.info("/status from chat %s", chat_id)
        handle_status(chat_id)
    elif text == "/start" or text.startswith("/start@"):
        tg_send(chat_id,
                "👋 Market Pulse Bot ready!\n\n"
                "/report — Generate market analysis report\n"
                "/status — Show bot status and uptime")
    elif text.startswith("/"):
        tg_send(chat_id,
                "Unknown command. Available:\n"
                "/report — Generate market analysis report\n"
                "/status — Show bot status and uptime")


def main() -> None:
    global _running
    setup_logging()
    logger.info("Bot server starting…")

    def _shutdown(sig, frame):
        global _running
        logger.info("Received signal %s, shutting down…", sig)
        _running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    offset: int | None = None
    poll_timeout = 30
    error_backoff = 1

    logger.info("Bot server ready. Listening for commands…")

    bg_thread = threading.Thread(target=_background_loop, name="analysis-bg", daemon=True)
    bg_thread.start()

    while _running:
        try:
            updates = tg_get_updates(offset=offset, timeout=poll_timeout)
            error_backoff = 1  # reset on success
        except Exception as e:
            logger.error("Polling error: %s", e)
            time.sleep(min(error_backoff, 60))
            error_backoff *= 2
            continue

        for update in updates:
            update_id = update.get("update_id", 0)
            offset = update_id + 1
            try:
                handle_update(update)
            except Exception as e:
                logger.error("Error handling update %s: %s", update_id, e, exc_info=True)


if __name__ == "__main__":
    main()
