from __future__ import annotations

from collections import deque
from typing import Iterable, Optional

from .core_types import ListingEvent


class EventEngine:
    def __init__(self, max_events_per_exchange: int = 200):
        self._max = max(20, int(max_events_per_exchange))
        self._events: dict[str, deque] = {}
        self._seen_keys: set[str] = set()

    def _bucket(self, exchange: str) -> deque:
        ex = str(exchange or "").strip().lower()
        if ex not in self._events:
            self._events[ex] = deque(maxlen=self._max)
        return self._events[ex]

    def add(self, event: ListingEvent) -> bool:
        key = f"{event.exchange_id}|{event.canonical_symbol}|{event.url}|{event.status}"
        if key in self._seen_keys:
            return False
        self._seen_keys.add(key)
        self._bucket(event.exchange_id).appendleft(event.to_dict())
        return True

    def add_many(self, events: Iterable[ListingEvent]) -> int:
        added = 0
        for ev in events:
            if self.add(ev):
                added += 1
        return added

    def get(self, exchange: str, limit: int = 80) -> list[dict]:
        lim = max(1, int(limit or 80))
        return list(self._bucket(exchange))[:lim]

    def all(self) -> dict[str, list[dict]]:
        return {ex: list(bucket) for ex, bucket in self._events.items()}

    def load_snapshot(self, payload: dict):
        if not isinstance(payload, dict):
            return
        self._seen_keys = set(str(x) for x in (payload.get("seenKeys") or []) if str(x))
        events = payload.get("events") or {}
        if not isinstance(events, dict):
            return
        self._events = {}
        for ex, rows in events.items():
            ex_key = str(ex or "").strip().lower()
            bucket = deque(maxlen=self._max)
            if isinstance(rows, list):
                for row in rows[: self._max]:
                    if isinstance(row, dict):
                        bucket.append(dict(row))
            self._events[ex_key] = bucket

    def dump_snapshot(self) -> dict:
        return {
            "seenKeys": sorted(self._seen_keys),
            "events": {ex: list(bucket) for ex, bucket in self._events.items()},
        }

    def clear_exchange(self, exchange: str):
        ex = str(exchange or "").strip().lower()
        rows = self._events.get(ex) or []
        for row in rows:
            key = f"{ex}|{row.get('canonicalSymbol','')}|{row.get('url','')}|{row.get('status','')}"
            self._seen_keys.discard(key)
        self._events[ex] = deque(maxlen=self._max)

    def update_status(
        self,
        exchange: str,
        canonical_symbol: str,
        status: str,
        trade_url: Optional[str] = None,
        order_symbol: Optional[str] = None,
    ):
        ex = str(exchange or "").strip().lower()
        bucket = self._events.get(ex) or []
        for row in bucket:
            if str(row.get("canonicalSymbol") or "") != str(canonical_symbol or ""):
                continue
            row["status"] = status
            if trade_url:
                row["tradeUrl"] = trade_url
            if order_symbol:
                row["orderSymbol"] = order_symbol
            break
