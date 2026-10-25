#!/usr/bin/env python3
"""Trump market-impact news agent.

Pipeline:
  1. Pull recent Trump-related items from RSS feeds.
  2. De-dupe against a local JSON store of previously processed IDs.
  3. For each new item, ask three GitHub Copilot models (Claude via chat completions, GPT and MAI via responses) to
     classify expected stock-market impact and recommend an action.
  4. If the AI returns a high-impact verdict, push a Telegram notification.

Designed to be run from cron every 30 minutes for Truth Social quota control.
Google News remains the primary source and Truth Social is a supplement.
Safe to invoke concurrently thanks to a file lock; failures are logged but
never raised to cron.
"""

from __future__ import annotations

import fcntl
import hashlib
import html
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Iterable
from xml.etree import ElementTree as ET

import urllib.parse
import urllib.request
import urllib.error

import config
from event_dedup import ClusterStore, should_notify

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("market_pulse")


def setup_logging() -> None:
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = RotatingFileHandler(config.LOG_FILE, maxBytes=2_000_000, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------


def http_request(
    url: str,
    *,
    method: str = "GET",
    headers: dict | None = None,
    data: bytes | None = None,
    timeout: int | None = None,
) -> tuple[int, bytes]:
    req_headers = {"User-Agent": config.HTTP_USER_AGENT}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout or config.REQUEST_TIMEOUT) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        body = b""
        try:
            body = e.read()
        except Exception:
            pass
        return e.code, body


# ---------------------------------------------------------------------------
# Seen-articles store
# ---------------------------------------------------------------------------


def load_seen() -> dict[str, str]:
    if not os.path.exists(config.SEEN_FILE):
        return {}
    try:
        with open(config.SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Failed to read seen file (%s); starting fresh", e)
    return {}


def save_seen(seen: dict[str, str]) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=config.SEEN_RETENTION_DAYS)
    pruned: dict[str, str] = {}
    for k, v in seen.items():
        try:
            ts = datetime.fromisoformat(v)
        except ValueError:
            continue
        if ts >= cutoff:
            pruned[k] = v
    tmp = config.SEEN_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(pruned, f, indent=2, sort_keys=True)
    os.replace(tmp, config.SEEN_FILE)


# ---------------------------------------------------------------------------
# RSS fetching
# ---------------------------------------------------------------------------


@dataclass
class Article:
    id: str
    title: str
    link: str
    source: str
    published: str
    summary: str


_TAG_RE = re.compile(r"<[^>]+>")


def _clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = _TAG_RE.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def _article_id(link: str, title: str) -> str:
    base = (link or "") + "|" + (title or "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def fetch_feed(url: str) -> list[Article]:
    try:
        status, body = http_request(url)
        if status == 429:
            logger.warning("Feed %s returned HTTP 429 (rate limited); skipping", url)
            return []
        if status != 200:
            logger.warning("Feed %s returned HTTP %s; skipping", url, status)
            return []
    except Exception as e:
        logger.warning("Failed to fetch %s: %s; skipping", url, e)
        return []
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        logger.warning("Failed to parse %s: %s", url, e)
        return []

    items: list[Article] = []
    # RSS 2.0
    for item in root.iter("item"):
        title = _clean_text((item.findtext("title") or ""))
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        desc = _clean_text(item.findtext("description") or "")
        source_el = item.find("source")
        source = (
            _clean_text(source_el.text) if source_el is not None and source_el.text else ""
        )
        if not title or not link:
            continue
        items.append(
            Article(
                id=_article_id(link, title),
                title=title,
                link=link,
                source=source or urllib.parse.urlparse(url).netloc,
                published=pub,
                summary=desc,
            )
        )
    return items


def fetch_truth_social() -> list[Article]:
    """Fetch recent Trump posts from ScrapeCreators Truth Social and normalize them as Article objects."""
    if not config.TRUTH_SOCIAL_ENABLED:
        logger.info("Truth Social supplement disabled; skipping ScrapeCreators fetch")
        return []

    url = "https://api.scrapecreators.com/v1/truthsocial/user/posts?handle=realDonaldTrump&limit=20"
    headers = {"x-api-key": config.SCRAPECREATORS_API_KEY}

    try:
        status, body = http_request(url, headers=headers)
        if status == 429:
            logger.warning("ScrapeCreators Truth Social API returned HTTP 429 (rate limited); skipping")
            return []
        if status == 401 or status == 403:
            logger.warning("ScrapeCreators Truth Social API authentication failed (HTTP %s); skipping", status)
            return []
        if status != 200:
            logger.warning("ScrapeCreators Truth Social API returned HTTP %s; skipping", status)
            return []
    except Exception as e:
        logger.warning("Failed to fetch ScrapeCreators Truth Social posts: %s; skipping", e)
        return []

    try:
        payload = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as e:
        logger.warning("Failed to parse ScrapeCreators Truth Social JSON: %s", e)
        return []

    items = payload
    if isinstance(payload, dict):
        for key in ("posts", "data", "results", "items", "statuses"):
            value = payload.get(key)
            if isinstance(value, list):
                items = value
                break
        else:
            logger.warning("ScrapeCreators Truth Social API returned unsupported payload shape; skipping")
            return []

    if not isinstance(items, list):
        logger.warning("ScrapeCreators Truth Social API returned non-array payload; skipping")
        return []

    articles: list[Article] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        text = _clean_text(
            item.get("text")
            or item.get("content")
            or item.get("body")
            or item.get("post")
            or item.get("message")
            or ""
        )
        if not text:
            continue

        status_id = item.get("id") or item.get("status_id") or item.get("post_id")
        if status_id is None:
            continue

        link = (
            item.get("url")
            or item.get("link")
            or item.get("permalink")
            or f"https://truthsocial.com/@realDonaldTrump/status/{status_id}"
        )
        title = text if len(text) <= 120 else text[:117] + "..."
        published = (
            item.get("created_at")
            or item.get("published_at")
            or item.get("createdAt")
            or item.get("timestamp")
            or ""
        )

        articles.append(
            Article(
                id=_article_id(link, title),
                title=title,
                link=link,
                source=item.get("source") or "Truth Social",
                published=published,
                summary=text,
            )
        )

    return articles


def gather_articles() -> list[Article]:
    seen_ids: set[str] = set()
    all_items: list[Article] = []

    for url in config.RSS_FEEDS:
        for a in fetch_feed(url):
            if a.id in seen_ids:
                continue
            seen_ids.add(a.id)
            all_items.append(a)

    # Truth Social: only fetch every 12 hours to conserve API credits
    _ts_interval = 43200  # 12 hours
    _ts_last_file = os.path.join(config.BASE_DIR, ".truth_social_last")
    _ts_should_fetch = True
    if os.path.exists(_ts_last_file):
        try:
            last_ts = float(open(_ts_last_file).read().strip())
            if time.time() - last_ts < _ts_interval:
                _ts_should_fetch = False
        except Exception:
            pass

    if _ts_should_fetch:
        for a in fetch_truth_social():
            if a.id in seen_ids:
                continue
            seen_ids.add(a.id)
            all_items.append(a)
        try:
            with open(_ts_last_file, "w") as f:
                f.write(str(time.time()))
        except Exception:
            pass
    else:
        logger.debug("Truth Social: skipping (last fetch < 12h ago)")

    logger.info(
        "Gathered %d unique articles from %d feeds plus Truth Social",
        len(all_items),
        len(config.RSS_FEEDS),
    )
    return all_items


# ---------------------------------------------------------------------------
# Copilot API
# ---------------------------------------------------------------------------


def get_copilot_token() -> str:
    if config.COPILOT_TOKEN:
        return config.COPILOT_TOKEN
    gh = shutil.which("gh")
    if not gh:
        raise RuntimeError("gh CLI not found and COPILOT_TOKEN not set")
    out = subprocess.run(
        [gh, "auth", "token"], capture_output=True, text=True, check=True
    )
    token = out.stdout.strip()
    if not token:
        raise RuntimeError("`gh auth token` returned empty token")
    return token


SYSTEM_PROMPT = (
    "You are a calibrated financial-news analyst for likely short-term US stock-market impact. "
    "Evaluate the headline, source, publication context, policy details, and transmission mechanism. "
    "Use only the supplied evidence; do not invent facts, sources, or reaction timing.\n\n"
    "Rubric:\n"
    "- Evidence/source quality: primary official documents or named established reporting beats social posts, anonymous aggregation, opinion, commentary, and unnamed reposts. "
    "Explicitly identify the evidence basis and its confidence level in the rationale; use lower scores when details are unconfirmed.\n"
    "- Directness: estimate how directly the news changes policy, regulation, taxes, tariffs, spending, rates, corporate costs, supply, demand, or investor expectations.\n"
    "- Market mechanism: explain the causal chain to affected sectors and tickers rather than merely saying the headline is important.\n"
    "- Magnitude: distinguish a modest change from a material nationwide or systemic change.\n"
    "- Persistence: give higher scores to durable, enforceable, broad, or policy-setting developments than to routine, symbolic, reversible, or one-off events.\n"
    "Score anchors: 0-2 no direct market impact or merely trivia/personality/social content; "
    "3-4 weak indirect or routine news with limited mechanism; "
    "5-6 confirmed, credible news with a moderate market mechanism; "
    "7-8 high likelihood of meaningful short-term market impact across sectors or major named companies; "
    "9-10 immediate, major, broad or systemic market impact.\n"
    "Penalties: substantially downgrade rumors and leaks without confirmation; headlines lacking policy or financial details; opinion and commentary; "
    "old or duplicate news presented as new; and placeholder, test, or obviously fabricated sources. "
    "Require evidence and confidence: the rationale must state the strongest supporting evidence and a confidence qualifier (high, medium, or low), "
    "while never claiming certainty beyond the evidence.\n\n"
    "Respond ONLY with a JSON object, no prose, matching this exact schema:\n"
    "{\n"
    '  "impact_score": <integer 0-10>,\n'
    '  "summary": "<one-sentence plain-English summary of what Trump said/did>",\n'
    '  "summary_zh": "<同一句话的中文翻译>",\n'
    '  "sectors": ["..."],\n'
    '  "sectors_zh": ["<中文板块名称>"],\n'
    '  "tickers": ["..."],\n'
    '  "direction": "bullish|bearish|mixed|neutral",\n'
    '  "direction_zh": "看涨|看跌|混合|中性",\n'
    '  "action": "buy|sell|hold",\n'
    '  "action_zh": "买入|卖出|持有",\n'
    '  "rationale": "<one or two sentences>",\n'
    '  "rationale_zh": "<理由的中文翻译>"\n'
    "}"
)


def uses_responses_endpoint(model: str, capabilities: dict | None = None) -> bool:
    """Choose Copilot's Responses API for OpenAI/GPT and MAI models."""
    if capabilities is None:
        capabilities = getattr(config, "COPILOT_MODEL_CAPABILITIES", {}).get(model, {})
    if capabilities:
        endpoint = capabilities.get("endpoint")
        if isinstance(endpoint, str):
            if endpoint.lower() in ("responses", "/responses"):
                return True
            if endpoint.lower() in ("chat", "chat/completions", "/chat/completions"):
                return False
        response_format = capabilities.get("response_format") or capabilities.get("api")
        if isinstance(response_format, str) and "responses" in response_format.lower():
            return True
        if capabilities.get("use_responses") is True:
            return True

    name = model.lower()
    return (
        name.startswith("gpt-")
        or name.startswith("mai-")
        or "mai-code" in name
        or name.startswith("openai/")
        or name.startswith("o1")
        or name.startswith("o3")
        or name.startswith("o4")
    )


def copilot_request_url(model: str, capabilities: dict | None = None) -> str:
    if uses_responses_endpoint(model, capabilities):
        return getattr(
            config,
            "COPILOT_RESPONSES_API_URL",
            "https://api.githubcopilot.com/responses",
        )
    return config.COPILOT_API_URL


def copilot_reasoning_effort(model: str) -> str:
    """Return the model-specific reasoning effort, with environment overrides."""
    name = model.lower()
    if name == config.COPILOT_MODEL.lower():
        default = "high"
        override = os.environ.get(
            "COPILOT_REASONING_EFFORT_MODEL",
            getattr(config, "COPILOT_REASONING_EFFORT_MODEL", default),
        )
    elif name == getattr(config, "COPILOT_MODEL_2", "").lower():
        default = "medium"
        override = os.environ.get(
            "COPILOT_REASONING_EFFORT_MODEL_2",
            getattr(config, "COPILOT_REASONING_EFFORT_MODEL_2", default),
        )
    elif name == getattr(config, "COPILOT_MODEL_3", "").lower():
        default = "medium"
        override = os.environ.get(
            "COPILOT_REASONING_EFFORT_MODEL_3",
            getattr(config, "COPILOT_REASONING_EFFORT_MODEL_3", default),
        )
    else:
        return "medium"

    effort = override.strip().lower() if override and override.strip() else default
    return effort if effort in ("low", "medium", "high", "xhigh", "max") else default


def copilot_payload(
    model: str,
    system_prompt: str,
    user_prompt: str,
    *,
    temperature: float,
    json_response: bool = False,
    capabilities: dict | None = None,
) -> dict:
    if uses_responses_endpoint(model, capabilities):
        textual_input = user_prompt.strip()
        if "json" not in textual_input.lower():
            if json_response:
                textual_input += "\n\nReturn the requested result as a valid JSON object."
            else:
                textual_input += "\n\nUse JSON only if it is useful for the response."
        payload = {
            "model": model,
            "input": textual_input,
        }
        if system_prompt:
            payload["instructions"] = system_prompt
        payload["reasoning"] = {"effort": copilot_reasoning_effort(model)}
        if json_response:
            payload["text"] = {"format": {"type": "json_object"}}
        return payload

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "reasoning_effort": copilot_reasoning_effort(model),
    }
    if json_response:
        payload["response_format"] = {"type": "json_object"}
    return payload


def extract_copilot_text(parsed: dict) -> str | None:
    """Extract text from Chat Completions or Responses API payloads."""
    try:
        return parsed["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        pass

    output_text = parsed.get("output_text")
    if isinstance(output_text, str):
        return output_text

    texts: list[str] = []
    output = parsed.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if isinstance(content, str):
                texts.append(content)
            elif isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    text = part.get("text")
                    if isinstance(text, str):
                        texts.append(text)
    return "\n".join(texts) if texts else None


def analyze_article(article: Article, token: str, model: str) -> dict | None:
    user_content = (
        f"Headline: {article.title}\n"
        f"Source: {article.source}\n"
        f"Published: {article.published}\n"
        f"Summary: {article.summary or '(none)'}\n"
        f"Link: {article.link}"
    )
    payload = copilot_payload(
        model,
        SYSTEM_PROMPT,
        user_content,
        temperature=0.2,
        json_response=True,
    )
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
        "Copilot-Integration-Id": "vscode-chat",
        "Editor-Version": "market-pulse/1.0",
    }
    body = json.dumps(payload).encode("utf-8")
    try:
        status, resp = http_request(
            copilot_request_url(model),
            method="POST",
            headers=headers,
            data=body,
            timeout=60,
        )
    except Exception as e:
        logger.error("Copilot request failed (%s): %s", model, e)
        return None
    if status != 200:
        logger.error("Copilot (%s) returned HTTP %s: %s", model, status, resp[:300])
        return None
    try:
        parsed = json.loads(resp)
        content = extract_copilot_text(parsed)
        if content is None:
            raise KeyError("text content")
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.error("Malformed Copilot response (%s): %s", model, e)
        return None

    # Strip optional code fences.
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)
    try:
        verdict = json.loads(content)
    except json.JSONDecodeError:
        # Try to extract a JSON object substring.
        m = re.search(r"\{.*\}", content, re.DOTALL)
        if not m:
            logger.error("Copilot (%s) returned non-JSON content: %s", model, content[:300])
            return None
        try:
            verdict = json.loads(m.group(0))
        except json.JSONDecodeError as e:
            logger.error("Could not parse JSON from Copilot content (%s): %s", model, e)
            return None
    return verdict


def _safe_score(verdict: dict | None) -> int:
    if not verdict:
        return 0
    try:
        return int(verdict.get("impact_score", 0))
    except (TypeError, ValueError):
        return 0


def analyze_article_three(article: Article, token: str) -> dict | None:
    """Run all configured models and combine their verdicts conservatively."""
    model_names = (
        config.COPILOT_MODEL,
        config.COPILOT_MODEL_2,
        getattr(config, "COPILOT_MODEL_3", "mai-code-2-flash"),
    )
    verdicts = [analyze_article(article, token, model) for model in model_names]
    available = [
        (model, verdict)
        for model, verdict in zip(model_names, verdicts)
        if verdict is not None
    ]
    if not available:
        return None

    per_model = [
        {"model": model, "score": _safe_score(verdict), "verdict": verdict}
        for model, verdict in available
    ]
    scores = [item["score"] for item in per_model]
    avg_score = sum(scores) / len(scores)
    threshold = config.IMPACT_THRESHOLD

    if len(scores) == 3:
        majority = sum(score >= threshold for score in scores) >= 2
        notify = majority and avg_score >= threshold
    elif len(scores) == 2:
        notify = all(score >= threshold for score in scores) and avg_score >= threshold
    else:
        notify = False
        logger.warning("Only one model returned a verdict; notification requires at least two scores")

    primary = max(available, key=lambda item: _safe_score(item[1]))[1]

    merged = dict(primary)
    merged["impact_score"] = avg_score
    merged["models"] = per_model
    merged["notify"] = notify
    return merged


def analyze_article_dual(article: Article, token: str) -> dict | None:
    """Backward-compatible alias; now runs all three configured models."""
    return analyze_article_three(article, token)


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------


def telegram_configured() -> bool:
    return (
        config.TELEGRAM_BOT_TOKEN
        and not config.TELEGRAM_BOT_TOKEN.startswith("YOUR_")
        and config.TELEGRAM_CHAT_ID
        and not str(config.TELEGRAM_CHAT_ID).startswith("YOUR_")
    )


def _md_escape(s: str) -> str:
    # MarkdownV2 reserved chars
    return re.sub(r"([_*\[\]()~`>#+\-=|{}.!\\])", r"\\\1", s or "")


def send_telegram(article: Article, verdict: dict) -> bool:
    if not telegram_configured():
        logger.warning("Telegram not configured; skipping notification")
        return False

    sectors = ", ".join(verdict.get("sectors") or []) or "n/a"
    sectors_zh = ", ".join(verdict.get("sectors_zh") or []) or "n/a"
    tickers = ", ".join(verdict.get("tickers") or []) or "n/a"
    action = (verdict.get("action") or "hold").upper()
    action_zh = verdict.get("action_zh") or ""
    direction = (verdict.get("direction") or "neutral").upper()
    direction_zh = verdict.get("direction_zh") or ""
    score = verdict.get("impact_score", "?")
    if isinstance(score, float):
        score_str = f"{score:.1f}"
    else:
        score_str = str(score)
    summary = verdict.get("summary") or article.title
    summary_zh = verdict.get("summary_zh") or ""
    rationale = verdict.get("rationale") or ""
    rationale_zh = verdict.get("rationale_zh") or ""

    text = (
        f"*🇺🇸 Trump market alert — avg impact {_md_escape(score_str)}/10*\n\n"
        f"*What:* {_md_escape(summary)}\n"
        f"*中文:* {_md_escape(summary_zh)}\n"
        f"*Sectors:* {_md_escape(sectors)} \\({_md_escape(sectors_zh)}\\)\n"
        f"*Tickers:* {_md_escape(tickers)}\n"
        f"*Direction:* {_md_escape(direction)} \\({_md_escape(direction_zh)}\\)\n"
        f"*Suggested action:* *{_md_escape(action)}* \\({_md_escape(action_zh)}\\)\n\n"
        f"_{_md_escape(rationale)}_\n"
        f"_{_md_escape(rationale_zh)}_\n"
    )

    models = verdict.get("models") or []
    if models:
        text += "\n*Model verdicts:*\n"
        for m in models:
            m_name = m.get("model", "?")
            m_v = m.get("verdict") or {}
            m_score = m.get("score", "?")
            m_action = (m_v.get("action") or "hold").upper()
            m_action_zh = m_v.get("action_zh") or ""
            m_rationale = m_v.get("rationale") or ""
            m_rationale_zh = m_v.get("rationale_zh") or ""
            action_display = f"*{_md_escape(m_action)}*"
            if m_action_zh:
                action_display += f" \\({_md_escape(m_action_zh)}\\)"
            text += (
                f"• *{_md_escape(m_name)}* — impact {_md_escape(str(m_score))}/10, "
                f"action {action_display}\n"
                f"  _{_md_escape(m_rationale)}_\n"
            )
            if m_rationale_zh:
                text += f"  _{_md_escape(m_rationale_zh)}_\n"

    text += f"\n[{_md_escape(article.source or 'source')}]({article.link})"

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": False,
        }
    ).encode("utf-8")
    try:
        status, resp = http_request(
            url, method="POST", headers={"Content-Type": "application/json"}, data=payload
        )
    except Exception as e:
        logger.error("Telegram request failed: %s", e)
        return False
    if status != 200:
        logger.error("Telegram returned HTTP %s: %s", status, resp[:300])
        return False
    return True


def send_telegram_text(text: str) -> bool:
    """Send a plain-text Telegram message (no MarkdownV2 parsing).

    Used by the paper trader and the daily portfolio report so they can write
    arbitrary text without worrying about MarkdownV2 escape rules.
    """
    if not telegram_configured():
        logger.warning("Telegram not configured; skipping plain message")
        return False
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")
    try:
        status, resp = http_request(
            url, method="POST", headers={"Content-Type": "application/json"}, data=payload
        )
    except Exception as e:
        logger.error("Telegram (plain) request failed: %s", e)
        return False
    if status != 200:
        logger.error("Telegram (plain) returned HTTP %s: %s", status, resp[:300])
        return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def acquire_lock() -> int | None:
    lock_path = os.path.join(config.BASE_DIR, ".market_pulse.lock")
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    return fd


def process(
    articles: Iterable[Article],
    seen: dict[str, str],
    token: str,
    
) -> int:
    notified = 0
    processed = 0
    now_iso = datetime.now(timezone.utc).isoformat()
    cluster_store = ClusterStore()
    for article in articles:
        if article.id in seen:
            continue
        if processed >= config.MAX_ARTICLES_PER_RUN:
            logger.info("Reached MAX_ARTICLES_PER_RUN cap; deferring remainder")
            break
        processed += 1
        logger.info("Analyzing: %s", article.title[:120])
        
        cluster, is_new = cluster_store.add_article(article.title, article.source)
        if not is_new and article.source != "Truth Social":
            logger.info("Skipping duplicate: %s", article.title[:80])
            seen[article.id] = now_iso
            continue
        
        verdict = analyze_article_three(article, token)
        # Mark as seen regardless of analysis outcome to avoid retry loops.
        seen[article.id] = now_iso
        
        if verdict is not None:
            cluster.ai_verdict = verdict
            cluster_store.save()
        
        if verdict is None:
            continue
        score = verdict.get("impact_score", 0)
        try:
            score_f = float(score)
        except (TypeError, ValueError):
            score_f = 0.0
        models_info = [
            (m.get("model"), m.get("score")) for m in (verdict.get("models") or [])
        ]
        logger.info(
            "Avg impact=%.2f models=%s action=%s sectors=%s tickers=%s notify=%s",
            score_f,
            models_info,
            verdict.get("action"),
            verdict.get("sectors"),
            verdict.get("tickers"),
            verdict.get("notify"),
        )
        if verdict.get("notify"):
            if not should_notify(cluster):
                logger.info("Cluster already notified: %s", article.title[:80])
            elif _is_duplicate_notification(article.title):
                logger.info("Skipping duplicate notification: %s", article.title[:80])
            elif send_telegram(article, verdict):
                notified += 1
                cluster.notified = True
                cluster_store.save()
                _record_notification(article.title)
                logger.info("Notification sent for: %s", article.title[:120])
            else:
                logger.warning("Failed to send notification for: %s", article.title[:120])
             
    return notified


# ---------------------------------------------------------------------------
# Semantic dedup: skip articles too similar to recently notified ones
# ---------------------------------------------------------------------------
_recent_notifications: list[str] = []  # titles of recently notified articles
_MAX_RECENT = 20


def _is_duplicate_notification(title: str) -> bool:
    """Check if a notification is semantically similar to a recent one."""
    title_words = set(title.lower().split())
    for prev in _recent_notifications:
        prev_words = set(prev.lower().split())
        if not title_words or not prev_words:
            continue
        overlap = len(title_words & prev_words) / len(title_words | prev_words)
        if overlap > 0.5:  # >50% Jaccard similarity = duplicate
            return True
    return False


def _record_notification(title: str) -> None:
    """Record a notified article title for dedup."""
    _recent_notifications.append(title)
    while len(_recent_notifications) > _MAX_RECENT:
        _recent_notifications.pop(0)
