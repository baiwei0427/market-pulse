"""Event-level deduplication for the news monitoring bot.

This module provides lightweight, pure-stdlib clustering for similar headlines.
It is intentionally independent from the existing RSS/Telegram pipeline so it can
be adopted incrementally without changing runtime behavior elsewhere.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

COMMON_WORDS = {"breaking", "reports", "says", "according", "latest", "update",
                "new", "just", "exclusive", "opinion", "analysis", "live",
                "watch", "video", "view", "column"}


def _canonicalize_word(word: str) -> str:
    text = word.lower().strip()
    if re.fullmatch(r"(?:strike|strikes|attack|attacks|attacked|striking)", text):
        return "attack"
    if re.fullmatch(r"(?:iran|iranian|iranians|tehran)", text):
        return "iran"
    if re.fullmatch(r"(?:israel|israeli|israelis)", text):
        return "israel"
    if re.fullmatch(r"(?:tariff|tariffs|duty|duties)", text):
        return "tariff"
    if re.fullmatch(r"(?:fed|federal|reserve)", text):
        return "fed"
    if re.fullmatch(r"(?:rate|rates|interest)", text):
        return "rate"
    if re.fullmatch(r"(?:hike|hikes|raise|raises|increase|increases)", text):
        return "hike"
    if re.fullmatch(r"(?:cut|cuts|lower|lowers|decrease|decreases)", text):
        return "cut"
    return text


@dataclass
class EventCluster:
    """Represents one deduplicated event cluster."""

    cluster_id: int
    first_headline: str
    token_set: set[str] = field(default_factory=set)
    sources: list[str] = field(default_factory=list)
    first_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ai_verdict: dict[str, Any] | None = None
    notified: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "first_headline": self.first_headline,
            "token_set": sorted(self.token_set),
            "sources": list(self.sources),
            "first_seen": _dt_to_iso(self.first_seen),
            "ai_verdict": self.ai_verdict,
            "notified": self.notified,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EventCluster":
        first_seen = _parse_dt(payload.get("first_seen")) if payload.get("first_seen") else datetime.fromtimestamp(0, tz=timezone.utc)
        token_set = set(payload.get("token_set") or [])
        return cls(
            cluster_id=int(payload.get("cluster_id", 0)),
            first_headline=str(payload.get("first_headline", "")),
            token_set=token_set,
            sources=list(payload.get("sources") or []),
            first_seen=first_seen,
            ai_verdict=payload.get("ai_verdict"),
            notified=bool(payload.get("notified", False)),
        )


def _dt_to_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            logger.warning("Failed to parse timestamp %r: %s", value, exc)
            return datetime.now(timezone.utc)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def normalize_title(title: str) -> str:
    """Normalize a headline for token-based deduplication."""
    text = (title or "").lower()
    text = text.replace("&", " and ")
    text = text.replace("—", " ")
    text = text.replace("–", " ")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)

    words = [word for word in text.split() if word not in COMMON_WORDS]
    normalized_words = [_canonicalize_word(word) for word in words if word]
    return " ".join(normalized_words).strip()


def title_to_tokens(title: str) -> set[str]:
    """Return meaningful tokens and bigrams for a title."""
    normalized = normalize_title(title)
    words = [word for word in normalized.split() if len(word) >= 2]
    if not words:
        return set()

    tokens = set(words)
    for left, right in zip(words, words[1:]):
        tokens.add(f"{left}_{right}")
    return tokens


def find_matching_cluster(title: str, clusters: Iterable[EventCluster]) -> EventCluster | None:
    """Find the best matching cluster using Jaccard similarity."""
    candidate_tokens = title_to_tokens(title)
    if not candidate_tokens:
        return None

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=6)

    best_cluster: EventCluster | None = None
    best_score = 0.0

    for cluster in clusters:
        if cluster.first_seen < window_start:
            continue
        if not cluster.token_set:
            continue

        overlap = len(candidate_tokens & cluster.token_set)
        union = len(candidate_tokens | cluster.token_set)
        if not union:
            continue
        score = overlap / union
        if score > 0.3 and score > best_score:
            best_score = score
            best_cluster = cluster

    if best_cluster is not None:
        logger.debug(
            "Matched title %r to cluster %s with Jaccard %.3f",
            title[:80],
            best_cluster.cluster_id,
            best_score,
        )
    return best_cluster


class ClusterStore:
    """Persistent store for event clusters."""

    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        self.path = Path(path or os.path.join(os.path.dirname(__file__), "clusters.json"))
        self.logger = logging.getLogger(__name__ + ".ClusterStore")
        self.clusters: list[EventCluster] = []
        self._lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        with self._lock:
            try:
                if not self.path.exists():
                    self.clusters = []
                    return
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(raw, list):
                    raise ValueError("clusters.json must be a JSON array")
                self.clusters = [EventCluster.from_dict(item) for item in raw]
                self.logger.info("Loaded %d clusters from %s", len(self.clusters), self.path)
            except (OSError, ValueError, TypeError) as exc:
                self.logger.warning("Failed to load clusters from %s: %s", self.path, exc)
                self.clusters = []

    def save(self) -> None:
        with self._lock:
            try:
                self.path.write_text(
                    json.dumps([cluster.to_dict() for cluster in self.clusters], indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
            except OSError as exc:
                self.logger.error("Failed to save clusters to %s: %s", self.path, exc)
                raise

    def prune_old_clusters(self, max_age_hours: int = 24) -> None:
        with self._lock:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
            original = len(self.clusters)
            self.clusters = [cluster for cluster in self.clusters if cluster.first_seen >= cutoff]
            if len(self.clusters) != original:
                self.logger.info("Pruned %d old clusters older than %dh", original - len(self.clusters), max_age_hours)

    def add_article(self, title: str, source: str) -> tuple[EventCluster, bool]:
        """Add an article, returning the cluster and whether it is new."""
        with self._lock:
            source = (source or "unknown").strip() or "unknown"
            candidate = find_matching_cluster(title, self.clusters)
            if candidate is not None:
                if source not in candidate.sources:
                    candidate.sources.append(source)
                self.logger.debug("Merged headline %r into existing cluster %s", title[:80], candidate.cluster_id)
                self.prune_old_clusters()
                self.save()
                return candidate, False

            cluster = EventCluster(
                cluster_id=self._next_cluster_id(),
                first_headline=title.strip(),
                token_set=title_to_tokens(title),
                sources=[source],
                first_seen=datetime.now(timezone.utc),
                ai_verdict=None,
                notified=False,
            )
            self.clusters.append(cluster)
            self.prune_old_clusters()
            self.save()
            self.logger.info("Created new cluster %s for %r", cluster.cluster_id, title[:80])
            return cluster, True

    def _next_cluster_id(self) -> int:
        return max((cluster.cluster_id for cluster in self.clusters), default=0) + 1


def should_analyze(cluster: EventCluster) -> bool:
    """Only analyze clusters that have not yet received an AI verdict."""
    return not bool(cluster.ai_verdict)


def should_notify(cluster: EventCluster) -> bool:
    """Only notify for clusters that have not yet been notified."""
    return not bool(cluster.notified)
