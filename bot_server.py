#!/usr/bin/env python3
"""Telegram bot server for Market Pulse.

Long-running service that listens for commands via Telegram Bot API
long polling (getUpdates). Supports:
  /report   — fetch RSS, analyze top 10 articles, send bilingual summary
  /status   — show bot uptime and last run stats
  /ask      — ask AI about current market conditions
  /ticker   — get AI analysis for a specific stock symbol
  /accuracy — show prediction accuracy stats
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
import zoneinfo
from datetime import datetime, timedelta, timezone

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
    send_telegram as mp_send_telegram,
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

BACKGROUND_INTERVAL = 600  # seconds between automatic analysis runs

# Eastern Time offset helpers
ET_OFFSET = zoneinfo.ZoneInfo('America/New_York')

ACCURACY_FILE = os.path.join(config.BASE_DIR, "accuracy.json")


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
# Accuracy tracking
# ---------------------------------------------------------------------------


def _load_accuracy() -> list[dict]:
    """Load accuracy records from accuracy.json."""
    if not os.path.exists(ACCURACY_FILE):
        return []
    try:
        with open(ACCURACY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Failed to read accuracy file: %s", e)
    return []


def _save_accuracy(records: list[dict]) -> None:
    """Save accuracy records to accuracy.json."""
    tmp = ACCURACY_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    os.replace(tmp, ACCURACY_FILE)


def _record_prediction(verdict: dict, article_title: str) -> None:
    """Record a high-impact prediction for later accuracy checking."""
    records = _load_accuracy()
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "article_title": article_title[:200],
        "direction": (verdict.get("direction") or "neutral").lower(),
        "action": (verdict.get("action") or "hold").lower(),
        "tickers": verdict.get("tickers") or [],
        "impact_score": verdict.get("impact_score", 0),
        "models": [
            {"model": m.get("model", "?"), "score": m.get("score", 0),
             "direction": (m.get("verdict", {}).get("direction") or "neutral").lower()}
            for m in (verdict.get("models") or [])
        ],
        "checked": False,
        "correct": None,
        "actual_moves": None,
    }
    records.append(record)
    _save_accuracy(records)
    logger.info("Recorded prediction for accuracy tracking: %s", article_title[:80])


def _check_predictions() -> None:
    """Check predictions that are 24+ hours old against actual price moves."""
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed; cannot check predictions")
        return

    records = _load_accuracy()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    changed = False

    for record in records:
        if record.get("checked"):
            continue
        try:
            ts = datetime.fromisoformat(record["timestamp"])
        except (ValueError, KeyError):
            continue
        if ts > cutoff:
            continue  # Not yet 24 hours old

        tickers = record.get("tickers") or []
        if not tickers:
            record["checked"] = True
            record["correct"] = None
            record["actual_moves"] = {}
            changed = True
            continue

        predicted_direction = record.get("direction", "neutral")
        actual_moves = {}
        correct_count = 0
        total_checked = 0

        for ticker in tickers[:5]:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period="5d")
                if len(hist) < 2:
                    continue
                # Compare close prices: day of prediction vs next day
                price_before = hist["Close"].iloc[-2]
                price_after = hist["Close"].iloc[-1]
                if price_before == 0:
                    continue
                pct_change = ((price_after - price_before) / price_before) * 100
                actual_moves[ticker] = round(pct_change, 2)

                total_checked += 1
                if predicted_direction == "bullish" and pct_change > 0:
                    correct_count += 1
                elif predicted_direction == "bearish" and pct_change < 0:
                    correct_count += 1
                elif predicted_direction in ("neutral", "mixed"):
                    if abs(pct_change) < 1.0:
                        correct_count += 1
            except Exception as e:
                logger.warning("Failed to check ticker %s: %s", ticker, e)

        record["checked"] = True
        record["actual_moves"] = actual_moves
        if total_checked > 0:
            record["correct"] = correct_count >= (total_checked / 2)
        else:
            record["correct"] = None
        changed = True

    if changed:
        _save_accuracy(records)
        logger.info("Accuracy check completed; updated %d records",
                    sum(1 for r in records if r.get("checked")))


def _get_accuracy_stats() -> dict:
    """Generate accuracy statistics report."""
    records = _load_accuracy()
    checked = [r for r in records if r.get("checked") and r.get("correct") is not None]

    if not checked:
        return {"overall": 0.0, "total": 0, "correct": 0, "pending": 0}

    total = len(checked)
    correct = sum(1 for r in checked if r["correct"])
    accuracy = (correct / total * 100) if total > 0 else 0

    # Breakdown by model
    model_stats: dict[str, dict[str, int]] = {}
    for record in checked:
        for m in record.get("models") or []:
            model_name = m.get("model", "unknown")
            if model_name not in model_stats:
                model_stats[model_name] = {"total": 0, "correct": 0}
            model_stats[model_name]["total"] += 1
            # Check if model's individual direction prediction was correct
            model_dir = m.get("direction", "neutral")
            actual_moves = record.get("actual_moves") or {}
            if actual_moves:
                avg_move = sum(actual_moves.values()) / len(actual_moves)
                if model_dir == "bullish" and avg_move > 0:
                    model_stats[model_name]["correct"] += 1
                elif model_dir == "bearish" and avg_move < 0:
                    model_stats[model_name]["correct"] += 1
                elif model_dir in ("neutral", "mixed") and abs(avg_move) < 1.0:
                    model_stats[model_name]["correct"] += 1

    # Breakdown by direction
    dir_stats: dict[str, dict[str, int]] = {}
    for record in checked:
        d = record.get("direction", "neutral")
        if d not in dir_stats:
            dir_stats[d] = {"total": 0, "correct": 0}
        dir_stats[d]["total"] += 1
        if record["correct"]:
            dir_stats[d]["correct"] += 1

    pending = sum(1 for r in records if not r.get("checked"))

    return {
        "overall": accuracy,
        "correct": correct,
        "total": total,
        "pending": pending,
        "by_model": model_stats,
        "by_direction": dir_stats,
    }


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
# Scheduled tasks: Daily Summary & Pre-Market Alert
# ---------------------------------------------------------------------------


def _get_current_et() -> datetime:
    """Get current time in US Eastern Time."""
    # Use UTC-5 for EST; during EDT it's UTC-4. For simplicity we use
    # a fixed offset. Production could use pytz/zoneinfo if available.
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except ImportError:
        return datetime.now(ET_OFFSET)


def _copilot_chat(prompt: str, token: str, model: str | None = None) -> str | None:
    """Send a freeform prompt to Copilot API and return the text response."""
    payload = {
        "model": model or config.COPILOT_MODEL,
        "messages": [
            {"role": "system", "content": "You are a financial markets analyst. Be concise and actionable."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Copilot-Integration-Id": "vscode-chat",
        "Editor-Version": "market-pulse/1.0",
    }
    body = json.dumps(payload).encode("utf-8")
    try:
        status, resp = http_request(
            config.COPILOT_API_URL, method="POST", headers=headers, data=body, timeout=60
        )
    except Exception as e:
        logger.error("Copilot chat request failed: %s", e)
        return None
    if status != 200:
        logger.error("Copilot chat returned HTTP %s: %s", status, resp[:300])
        return None
    try:
        parsed = json.loads(resp)
        return parsed["choices"][0]["message"]["content"]
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.error("Malformed Copilot chat response: %s", e)
        return None


def _send_daily_summary() -> None:
    """Generate and send the 4:30 PM ET daily market summary."""
    logger.info("Generating daily market summary…")
    chat_id = config.TELEGRAM_CHAT_ID
    try:
        token = get_copilot_token()
    except Exception as e:
        logger.error("Daily summary: failed to get token: %s", e)
        return

    with _analysis_lock:
        articles = gather_articles()
        if not articles:
            tg_send_long(chat_id, "📊 Daily Summary: No articles found today.")
            return

        # Analyze top articles for the summary
        results: list[tuple[Article, dict]] = []
        cap = min(len(articles), 15)
        for article in articles[:cap]:
            try:
                verdict = analyze_article_dual(article, token)
            except Exception as e:
                logger.error("Daily summary analysis error: %s", e)
                continue
            if verdict:
                results.append((article, verdict))

    results.sort(key=lambda x: float(x[1].get("impact_score", 0)), reverse=True)
    top = results[:8]

    if not top:
        tg_send_long(chat_id, "📊 Daily Summary: No significant market news today.")
        return

    # Build summary text for AI to synthesize
    news_text = "\n".join(
        f"- {a.title} (Impact: {v.get('impact_score', 0)}/10, "
        f"Direction: {v.get('direction', 'neutral')}, Tickers: {', '.join(v.get('tickers', []))})"
        for a, v in top
    )

    outlook_prompt = (
        f"Based on today's key market news, provide:\n"
        f"1. A brief overall market summary (2-3 sentences)\n"
        f"2. Top 3 movers and why\n"
        f"3. Tomorrow's outlook and what to watch\n\n"
        f"Today's news:\n{news_text}"
    )

    outlook = _copilot_chat(outlook_prompt, token)

    now_et = _get_current_et()
    lines = [
        "📊 DAILY MARKET SUMMARY / 每日市场总结",
        f"🕐 {now_et.strftime('%Y-%m-%d %H:%M ET')}",
        "═══════════════════════════════════════",
        "",
    ]

    if outlook:
        lines.append(outlook)
    else:
        # Fallback: raw top movers
        lines.append("TOP MOVERS TODAY:")
        for i, (a, v) in enumerate(top[:5], 1):
            tickers = ", ".join(v.get("tickers") or []) or "—"
            lines.append(
                f"{i}. [{v.get('direction', '?').upper()}] {a.title[:80]}"
                f"\n   Tickers: {tickers} | Impact: {v.get('impact_score', 0)}/10"
            )

    lines.append("\n⚠️ AI analysis, not financial advice.")
    tg_send_long(chat_id, "\n".join(lines))
    logger.info("Daily summary sent successfully")


def _send_premarket_alert() -> None:
    """Generate and send the 9:00 AM ET pre-market alert.

    Does a fresh RSS fetch followed by per-article AI analysis so the alert
    reflects the latest overnight/pre-market news rather than cached data.
    """
    logger.info("Generating pre-market alert…")
    chat_id = config.TELEGRAM_CHAT_ID
    try:
        token = get_copilot_token()
    except Exception as e:
        logger.error("Pre-market alert: failed to get token: %s", e)
        return

    # Fresh fetch — deliberately bypasses the seen-articles store so we always
    # analyse the most recent overnight headlines regardless of prior runs.
    with _analysis_lock:
        articles = gather_articles()
        if not articles:
            tg_send_long(chat_id, "🌅 Pre-Market: No significant news to report.")
            return

        # AI-analyse the top articles to surface genuine market-movers.
        results: list[tuple[Article, dict]] = []
        cap = min(len(articles), 12)
        for article in articles[:cap]:
            try:
                verdict = analyze_article_dual(article, token)
            except Exception as e:
                logger.error("Pre-market analysis error: %s", e)
                continue
            if verdict:
                results.append((article, verdict))

    results.sort(key=lambda x: float(x[1].get("impact_score", 0)), reverse=True)
    top = results[:6]

    # Build an AI-synthesised outlook from the scored articles.
    if top:
        news_text = "\n".join(
            f"- {a.title} (Impact: {v.get('impact_score', 0)}/10, "
            f"Direction: {v.get('direction', 'neutral')}, "
            f"Sectors: {', '.join(v.get('sectors') or [])}, "
            f"Tickers: {', '.join(v.get('tickers') or [])})"
            for a, v in top
        )
    else:
        news_text = "\n".join(f"- {a.title}" for a in articles[:10])

    prompt = (
        "It's 30 minutes before US market open. Based on these pre-scored overnight "
        "headlines, provide:\n"
        "1. Key events that could move markets today\n"
        "2. Sectors and tickers to watch\n"
        "3. Key risks or catalysts\n"
        "Be concise and actionable.\n\n"
        f"Scored headlines:\n{news_text}"
    )

    analysis = _copilot_chat(prompt, token)

    now_et = _get_current_et()
    lines = [
        "🌅 PRE-MARKET ALERT / 盘前提醒",
        f"🕐 {now_et.strftime('%Y-%m-%d %H:%M ET')}",
        "═══════════════════════════════════════",
        "",
    ]

    if analysis:
        lines.append(analysis)
    else:
        # Fallback: list top movers directly
        lines.append("TOP PRE-MARKET MOVERS:")
        for i, (a, v) in enumerate(top[:5], 1):
            tickers = ", ".join(v.get("tickers") or []) or "—"
            lines.append(
                f"{i}. [{v.get('direction', '?').upper()}] {a.title[:80]}"
                f"\n   Tickers: {tickers} | Impact: {v.get('impact_score', 0)}/10"
            )

    lines.append("\n⚠️ AI analysis, not financial advice.")
    tg_send_long(chat_id, "\n".join(lines))
    logger.info("Pre-market alert sent successfully")


def _scheduler_loop() -> None:
    """Scheduler thread: triggers daily summary (4:30 PM ET) and pre-market (9:00 AM ET)."""
    logger.info("Scheduler thread started")
    last_summary_date: str | None = None
    last_premarket_date: str | None = None
    last_accuracy_check: datetime | None = None

    while _running:
        try:
            now_et = _get_current_et()
            today_str = now_et.strftime("%Y-%m-%d")
            hour, minute = now_et.hour, now_et.minute

            # Daily market summary at 4:30 PM ET (16:30) — weekdays only
            if (hour == 16 and 30 <= minute < 35
                    and now_et.weekday() < 5  # Mon=0 … Fri=4
                    and last_summary_date != today_str):
                last_summary_date = today_str
                try:
                    _send_daily_summary()
                except Exception:
                    logger.exception("Daily summary failed")

            # Pre-market alert at 9:00 AM ET — weekdays only
            if (hour == 9 and 0 <= minute < 5
                    and now_et.weekday() < 5
                    and last_premarket_date != today_str):
                last_premarket_date = today_str
                try:
                    _send_premarket_alert()
                except Exception:
                    logger.exception("Pre-market alert failed")

            # Check prediction accuracy every 24 hours
            if last_accuracy_check is None or \
               (datetime.now(timezone.utc) - last_accuracy_check).total_seconds() > 86400:
                last_accuracy_check = datetime.now(timezone.utc)
                try:
                    _check_predictions()
                except Exception:
                    logger.exception("Accuracy check failed")

        except Exception:
            logger.exception("Scheduler loop error")

        # Sleep 30 seconds between checks
        for _ in range(30):
            if not _running:
                break
            time.sleep(1)

    logger.info("Scheduler thread stopped")


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
            f"🕐 {_get_current_et().strftime('%Y-%m-%d %H:%M ET')}",
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
# /ask command
# ---------------------------------------------------------------------------


def handle_ask(chat_id: int | str, question: str) -> None:
    """Answer a user's freeform market question using Copilot API."""
    if not question.strip():
        tg_send(chat_id, "Usage: /ask <your question about markets>\nExample: /ask What's driving tech stocks today?")
        return

    tg_send(chat_id, "🤔 Thinking…")
    try:
        token = get_copilot_token()
    except Exception as e:
        tg_send(chat_id, f"❌ Failed to get API token: {e}")
        return

    # Gather recent headlines for context
    articles = gather_articles()
    context = ""
    if articles:
        headlines = "\n".join(f"- {a.title}" for a in articles[:10])
        context = f"\n\nRecent market headlines for context:\n{headlines}"

    prompt = (
        f"User question about current market conditions: {question}\n"
        f"{context}\n\n"
        f"Provide a helpful, concise answer. If the question is about specific stocks, "
        f"mention relevant recent news. If speculative, note uncertainty."
    )

    answer = _copilot_chat(prompt, token, model="claude-opus-4.6")
    if answer:
        tg_send_long(chat_id, f"💬 {answer}\n\n⚠️ AI analysis, not financial advice.")
    else:
        tg_send(chat_id, "❌ Failed to generate an answer. Please try again.")


# ---------------------------------------------------------------------------
# /ticker command
# ---------------------------------------------------------------------------


def handle_ticker(chat_id: int | str, symbol: str) -> None:
    """Fetch news, price, and AI analysis for a specific stock ticker."""
    symbol = symbol.strip().upper()
    if not symbol or not symbol.isalpha() or len(symbol) > 10:
        tg_send(chat_id, "Usage: /ticker <SYMBOL>\nExample: /ticker NVDA")
        return

    tg_send(chat_id, f"🔍 Analyzing {symbol}…")

    # Fetch current price via yfinance
    price_info = ""
    try:
        import yfinance as yf
        stock = yf.Ticker(symbol)
        hist = stock.history(period="5d")
        if not hist.empty:
            current = hist["Close"].iloc[-1]
            prev = hist["Close"].iloc[-2] if len(hist) >= 2 else current
            change = ((current - prev) / prev) * 100
            price_info = (
                f"💰 {symbol}: ${current:.2f} ({change:+.2f}%)\n"
                f"   5-day range: ${hist['Close'].min():.2f} – ${hist['Close'].max():.2f}\n"
                f"   Volume (last): {int(hist['Volume'].iloc[-1]):,}\n"
            )
        else:
            price_info = f"⚠️ No price data found for {symbol}\n"
    except ImportError:
        price_info = "⚠️ yfinance not available for price data\n"
    except Exception as e:
        price_info = f"⚠️ Price fetch failed: {e}\n"

    # Search Google News RSS specifically for this ticker
    try:
        token = get_copilot_token()
    except Exception as e:
        tg_send(chat_id, f"{price_info}\n❌ Failed to get API token for analysis: {e}")
        return

    gnews_url = (
        f"https://news.google.com/rss/search?q={symbol}+stock+when:3d"
        f"&hl=en-US&gl=US&ceid=US:en"
    )
    relevant = fetch_feed(gnews_url)
    # Fall back to general feeds filtered by symbol if Google News yields nothing
    if not relevant:
        all_articles = gather_articles()
        relevant = [a for a in all_articles if symbol in a.title.upper() or symbol in a.summary.upper()]

    news_context = "\n".join(f"- {a.title}" for a in relevant[:8])

    prompt = (
        f"Provide a brief analysis of {symbol} stock based on recent news and market conditions.\n\n"
        f"Recent relevant news:\n{news_context}\n\n"
        f"Current price info: {price_info}\n\n"
        f"Include: 1) What's driving the stock, 2) Key risks, 3) Short-term outlook.\n"
        f"Be concise (5-8 sentences max)."
    )

    analysis = _copilot_chat(prompt, token)

    lines = [
        f"📈 TICKER ANALYSIS: {symbol}",
        "═══════════════════════════════",
        price_info,
    ]

    if relevant:
        lines.append("📰 RECENT NEWS:")
        for a in relevant[:5]:
            lines.append(f"  • {a.title[:80]}")
        lines.append("")

    if analysis:
        lines.append("🤖 AI ANALYSIS:")
        lines.append(analysis)
    else:
        lines.append("❌ Could not generate AI analysis.")

    lines.append("\n⚠️ AI analysis, not financial advice.")
    tg_send_long(chat_id, "\n".join(lines))


# ---------------------------------------------------------------------------
# /accuracy command
# ---------------------------------------------------------------------------


def handle_accuracy(chat_id: int | str) -> None:
    """Show prediction accuracy statistics."""
    stats = _get_accuracy_stats()
    
    if stats["total"] == 0:
        msg = "📊 No accuracy data yet. Predictions need 24+ hours to be verified."
    else:
        lines = [
            "📊 PREDICTION ACCURACY STATS",
            f"═══════════════════════════════",
            f"Overall: {stats['correct']}/{stats['total']} correct ({stats['overall']:.1f}%)",
            "",
        ]

        if stats["by_model"]:
            lines.append("BY MODEL:")
            for model, st in stats["by_model"].items():
                m_acc = (st["correct"] / st["total"] * 100) if st["total"] > 0 else 0
                lines.append(f"  {model}: {st['correct']}/{st['total']} ({m_acc:.1f}%)")
            lines.append("")

        if stats["by_direction"]:
            lines.append("BY PREDICTED DIRECTION:")
            for direction, st in stats["by_direction"].items():
                d_acc = (st["correct"] / st["total"] * 100) if st["total"] > 0 else 0
                lines.append(f"  {direction}: {st['correct']}/{st['total']} ({d_acc:.1f}%)")
            lines.append("")

        if stats["pending"]:
            lines.append(f"⏳ {stats['pending']} prediction(s) pending verification (< 24h old)")

        msg = "\n".join(lines)

    tg_send_long(chat_id, msg)


# ---------------------------------------------------------------------------
# Background analysis (runs every BACKGROUND_INTERVAL seconds)
# ---------------------------------------------------------------------------


def _run_analysis() -> None:
    """Run the market_pulse pipeline once (fetch → analyze → notify).

    Enhanced version that also records predictions for accuracy tracking.
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
                # Process articles and track high-impact predictions
                notified = _process_with_tracking(articles, seen, token)
                save_seen(seen)
                logger.info("Background analysis: done — %s notifications sent", notified)
            else:
                logger.info("Background analysis: no articles found")
        except Exception:
            logger.exception("Background analysis: unhandled error")
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


def _process_with_tracking(articles, seen, token) -> int:
    """Process articles like mp_process but also record predictions for accuracy."""
    notified = 0
    processed = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for article in articles:
        if article.id in seen:
            continue
        if processed >= config.MAX_ARTICLES_PER_RUN:
            break
        processed += 1
        logger.info("Analyzing: %s", article.title[:120])
        verdict = analyze_article_dual(article, token)
        seen[article.id] = now_iso
        if verdict is None:
            continue
        if verdict.get("notify"):
            if mp_send_telegram(article, verdict):
                notified += 1
                # Record prediction for accuracy tracking
                _record_prediction(verdict, article.title)
                logger.info("Notification sent for: %s", article.title[:120])
            else:
                logger.warning("Failed to send notification for: %s", article.title[:120])
    return notified


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
    elif text.startswith("/ask"):
        logger.info("/ask from chat %s", chat_id)
        # Extract the question after "/ask" or "/ask@botname"
        parts = text.split(None, 1)
        question = parts[1] if len(parts) > 1 else ""
        handle_ask(chat_id, question)
    elif text.startswith("/ticker"):
        logger.info("/ticker from chat %s", chat_id)
        parts = text.split(None, 1)
        symbol = parts[1] if len(parts) > 1 else ""
        handle_ticker(chat_id, symbol)
    elif text == "/accuracy" or text.startswith("/accuracy@"):
        logger.info("/accuracy from chat %s", chat_id)
        handle_accuracy(chat_id)
    elif text == "/start" or text.startswith("/start@"):
        tg_send(chat_id,
                "👋 Market Pulse Bot ready!\n\n"
                "/report — Generate market analysis report\n"
                "/status — Show bot status and uptime\n"
                "/ask <question> — Ask about market conditions\n"
                "/ticker <SYMBOL> — Analyze a specific stock\n"
                "/accuracy — Show prediction accuracy stats")
    elif text.startswith("/"):
        tg_send(chat_id,
                "Unknown command. Available:\n"
                "/report — Generate market analysis report\n"
                "/status — Show bot status and uptime\n"
                "/ask <question> — Ask about market conditions\n"
                "/ticker <SYMBOL> — Analyze a specific stock\n"
                "/accuracy — Show prediction accuracy stats")


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

    sched_thread = threading.Thread(target=_scheduler_loop, name="scheduler", daemon=True)
    sched_thread.start()

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
