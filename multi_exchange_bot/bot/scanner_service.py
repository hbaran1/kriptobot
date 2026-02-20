from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Optional

from .core_types import ExchangeScanStats, ListingEvent, ScanStats
from .event_engine import EventEngine
from .symbols import build_trade_url, canonical_symbol, format_order_symbol
from .util import iso_utc


DEFAULT_INTERVALS_MS = {
    "gate": {"NORMAL": 300000, "ALARM": 30000, "HOT": 2000},
    "mexc": {"NORMAL": 300000, "ALARM": 30000, "HOT": 2000},
    "kucoin": {"NORMAL": 300000, "ALARM": 30000, "HOT": 2000},
    "bitget": {"NORMAL": 300000, "ALARM": 30000, "HOT": 2000},
    "binance": {"NORMAL": 300000, "ALARM": 20000, "HOT": 1000},
    "okex": {"NORMAL": 300000, "ALARM": 20000, "HOT": 1000},
    "bybit": {"NORMAL": 300000, "ALARM": 20000, "HOT": 1000},
    "btcturk": {"NORMAL": 600000, "ALARM": 60000, "HOT": 5000},
    "paribu": {"NORMAL": 600000, "ALARM": 60000, "HOT": 5000},
}


class ScannerService:
    def __init__(self, aggregator, state_dir: str, max_events_per_exchange: int = 200):
        self.agg = aggregator
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)
        self.events = EventEngine(max_events_per_exchange=max_events_per_exchange)

        self._lock = threading.Lock()
        self._running = False
        self._thread = None
        self._stop_evt = threading.Event()

        self._stats = ScanStats(run_id="", started_at="", ended_at="", duration_ms=0)
        self._exchange_stats: dict[str, ExchangeScanStats] = {}
        self._mode_ttl_ms: dict[str, int] = {}
        self._hot_exchange: Optional[str] = None
        self._recent_logs = deque(maxlen=200)
        self._last_run_id = ""
        self._last_run_started_ms = 0
        self._last_run_ended_ms = 0

        for ex in self.agg.exchanges:
            row = ExchangeScanStats(
                mode="NORMAL",
                current_interval_ms=self._mode_interval(ex, "NORMAL"),
                next_run_at_ms=0,
            )
            self._exchange_stats[ex] = row

        self._load_state()

    def _state_file(self) -> str:
        return os.path.join(self.state_dir, "scanner_runtime.json")

    def _event(self, message: str):
        self._recent_logs.appendleft({"at": iso_utc(), "text": message})

    def _save_state(self):
        payload = {
            "exchangeStats": {k: v.to_dict() for k, v in self._exchange_stats.items()},
            "modeTtlMs": dict(self._mode_ttl_ms),
            "hotExchange": self._hot_exchange,
            "events": self.events.dump_snapshot(),
        }
        path = self._state_file()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, path)

    def _load_state(self):
        path = self._state_file()
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f) or {}
        except Exception:
            return
        stats = payload.get("exchangeStats") or {}
        for ex, row in stats.items():
            if ex not in self._exchange_stats or not isinstance(row, dict):
                continue
            cur = self._exchange_stats[ex]
            cur.mode = str(row.get("mode") or cur.mode).upper()
            cur.current_interval_ms = int(row.get("currentIntervalMs") or cur.current_interval_ms)
            cur.market_count = int(row.get("marketCount") or 0)
            cur.candidate_new_count = int(row.get("candidateNewCount") or 0)
            cur.verified_new_count = int(row.get("verifiedNewCount") or 0)
            cur.fetch_duration_ms = int(row.get("fetchDurationMs") or 0)
            cur.last_success_at = str(row.get("lastSuccessAt") or "")
            cur.last_error_type = str(row.get("lastErrorType") or "")
            cur.last_error_message = str(row.get("lastErrorMessage") or "")
            cur.consecutive_errors = int(row.get("consecutiveErrors") or 0)
            cur.rate_limit_hits = int(row.get("rateLimitHits") or 0)
            cur.backoff_until = int(row.get("backoffUntil") or 0)
            cur.backoff_level = int(row.get("backoffLevel") or 0)
            cur.next_run_at_ms = int(row.get("nextRunAtMs") or 0)
        mode_ttl = payload.get("modeTtlMs") or {}
        self._mode_ttl_ms = {str(k): int(v) for k, v in mode_ttl.items() if str(k) in self._exchange_stats}
        hot = str(payload.get("hotExchange") or "").strip().lower()
        self._hot_exchange = hot if hot in self._exchange_stats else None
        self.events.load_snapshot(payload.get("events") or {})

    def _mode_interval(self, exchange: str, mode: str) -> int:
        ex = str(exchange or "").strip().lower()
        m = str(mode or "NORMAL").upper()
        return int(DEFAULT_INTERVALS_MS.get(ex, {}).get(m, 300000))

    def _classify_error(self, err_msg: str) -> str:
        txt = str(err_msg or "").lower()
        if "429" in txt:
            return "rateLimit"
        if "401" in txt or "403" in txt:
            return "auth"
        if "timeout" in txt:
            return "timeout"
        if "parse" in txt or "json" in txt:
            return "parse"
        if "connection" in txt or "dns" in txt or "network" in txt:
            return "network"
        return "unknown"

    def set_mode(self, exchange: str, mode: str, ttl_sec: int = 0) -> dict:
        ex = str(exchange or "").strip().lower()
        new_mode = str(mode or "NORMAL").upper()
        if ex not in self._exchange_stats:
            return {"ok": False, "error": "unknown_exchange"}
        if new_mode not in {"NORMAL", "ALARM", "HOT"}:
            return {"ok": False, "error": "invalid_mode"}

        with self._lock:
            if new_mode == "HOT":
                if self._hot_exchange and self._hot_exchange != ex:
                    return {"ok": False, "error": f"hot_already_active:{self._hot_exchange}"}
                self._hot_exchange = ex
                if ttl_sec <= 0:
                    ttl_sec = 180
                self._mode_ttl_ms[ex] = int(time.time() * 1000) + (int(ttl_sec) * 1000)
            else:
                if self._hot_exchange == ex:
                    self._hot_exchange = None
                if ttl_sec > 0:
                    self._mode_ttl_ms[ex] = int(time.time() * 1000) + (int(ttl_sec) * 1000)
                elif ex in self._mode_ttl_ms:
                    self._mode_ttl_ms.pop(ex, None)

            row = self._exchange_stats[ex]
            row.mode = new_mode
            row.current_interval_ms = self._mode_interval(ex, new_mode)
            row.next_run_at_ms = 0
            self._event(f"{ex}: mode -> {new_mode}")
            self._save_state()
            return {"ok": True, "exchange": ex, "mode": new_mode}

    def _expire_modes(self):
        now_ms = int(time.time() * 1000)
        expired = [ex for ex, ttl in self._mode_ttl_ms.items() if ttl > 0 and now_ms >= ttl]
        for ex in expired:
            row = self._exchange_stats.get(ex)
            if not row:
                continue
            row.mode = "NORMAL"
            row.current_interval_ms = self._mode_interval(ex, "NORMAL")
            row.next_run_at_ms = 0
            self._mode_ttl_ms.pop(ex, None)
            if self._hot_exchange == ex:
                self._hot_exchange = None
            self._event(f"{ex}: mode expired -> NORMAL")

    def _run_exchange(self, exchange: str, run_id: str):
        row = self._exchange_stats[exchange]
        now_ms = int(time.time() * 1000)
        row.in_flight = True
        row.market_count = 0
        row.candidate_new_count = 0
        row.verified_new_count = 0

        items, err, _state, meta = self.agg.scan_exchange(exchange)

        row.fetch_duration_ms = int(meta.get("fetch_duration_ms") or 0)
        row.market_count = int(meta.get("market_count") or len(items or []))
        row.candidate_new_count = int(meta.get("candidate_new_count") or 0)
        row.verified_new_count = int(meta.get("verified_new_count") or 0)

        if err:
            row.last_error_message = str(err)
            row.last_error_type = str(meta.get("error_type") or self._classify_error(err))
            row.consecutive_errors += 1
            if row.last_error_type == "rateLimit":
                row.rate_limit_hits += 1
                row.backoff_level = min(row.backoff_level + 1, 8)
                row.backoff_until = now_ms + min((2 ** row.backoff_level) * 5000, 300000)
            else:
                row.backoff_level = min(row.backoff_level + 1, 8)
                row.backoff_until = now_ms + min((2 ** row.backoff_level) * 3000, 180000)
            self._event(f"{exchange}: {row.last_error_type} - {row.last_error_message[:120]}")
        else:
            row.last_success_at = iso_utc()
            row.last_error_message = ""
            row.last_error_type = ""
            row.consecutive_errors = 0
            row.backoff_level = max(0, row.backoff_level - 1)
            row.backoff_until = 0
            self._event(f"{exchange}: ok ({len(items or [])} items)")

        mode_interval = self._mode_interval(exchange, row.mode)
        if row.fetch_duration_ms > int(mode_interval * 0.6):
            mode_interval = int(mode_interval * 1.5)
        if row.backoff_until > now_ms:
            mode_interval = max(mode_interval, row.backoff_until - now_ms)
        jitter = int(mode_interval * 0.15)
        row.current_interval_ms = mode_interval
        row.next_run_at_ms = int(time.time() * 1000) + mode_interval + (jitter // 2)
        row.in_flight = False

        for item in items or []:
            pair_guess = str(item.get("pair_guess") or "").strip()
            if not pair_guess:
                pair_guess = str(item.get("symbol_key") or "").strip()
            canon = canonical_symbol(pair_guess, exchange)
            if not canon:
                continue
            order_symbol = format_order_symbol(exchange, canon)
            trade_url = build_trade_url(exchange, canon)
            status = "verified" if str(item.get("normalized_tr_time") or "UNKNOWN") != "UNKNOWN" else "candidate"
            ev = ListingEvent(
                exchange_id=exchange,
                canonical_symbol=canon,
                detected_at=str(item.get("detected_at") or iso_utc()),
                market_type="spot",
                status=status,
                order_symbol=order_symbol,
                trade_url=trade_url,
                source="announcement",
                run_id=run_id,
                title=str(item.get("title") or ""),
                url=str(item.get("url") or ""),
                source_type=str(item.get("source_type") or ""),
            )
            self.events.add(ev)

    def run_once(self, reason: str = "manual") -> dict:
        with self._lock:
            run_id = f"scan-{uuid.uuid4().hex[:12]}"
            self._last_run_id = run_id
            self._last_run_started_ms = int(time.time() * 1000)
            self._expire_modes()
            started_at = iso_utc()

        for ex in self.agg.exchanges:
            with self._lock:
                row = self._exchange_stats[ex]
                now_ms = int(time.time() * 1000)
                if row.next_run_at_ms and row.next_run_at_ms > now_ms and reason != "manual_force":
                    continue
            self._run_exchange(ex, run_id)

        with self._lock:
            ended_at = iso_utc()
            self._last_run_ended_ms = int(time.time() * 1000)
            self._stats = ScanStats(
                run_id=run_id,
                started_at=started_at,
                ended_at=ended_at,
                duration_ms=max(0, self._last_run_ended_ms - self._last_run_started_ms),
                per_exchange={ex: self._exchange_stats[ex] for ex in self.agg.exchanges},
            )
            self._save_state()
            return self.status()

    def _worker(self):
        while not self._stop_evt.is_set():
            try:
                self.run_once(reason="auto")
            except Exception as e:
                self._event(f"scanner loop error: {type(e).__name__}: {e}")
            self._stop_evt.wait(1.0)

    def start(self) -> dict:
        with self._lock:
            if self._running:
                return self.status()
            self._stop_evt.clear()
            self._thread = threading.Thread(target=self._worker, daemon=True)
            self._thread.start()
            self._running = True
            self._event("scanner started")
            return self.status()

    def stop(self) -> dict:
        with self._lock:
            self._running = False
            self._stop_evt.set()
            self._event("scanner stopped")
            self._save_state()
            return self.status()

    def status(self) -> dict:
        with self._lock:
            return {
                "running": bool(self._running),
                "hotExchange": self._hot_exchange,
                "lastRunId": self._last_run_id,
                "lastRunStartedMs": self._last_run_started_ms,
                "lastRunEndedMs": self._last_run_ended_ms,
                "stats": {
                    ex: row.to_dict()
                    for ex, row in self._exchange_stats.items()
                },
                "recentLogs": list(self._recent_logs),
            }

    def events_by_exchange(self, limit: int = 120) -> dict[str, list[dict]]:
        out = {}
        for ex in self.agg.exchanges:
            out[ex] = self.events.get(ex, limit=limit)
        return out
