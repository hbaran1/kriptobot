import os
import time
import json
import hmac
import hashlib
import statistics
import threading

import requests
from requests.adapters import HTTPAdapter

from .base import Exchange


class Gate(Exchange):
    name = "gate"

    def __init__(self):
        self.key = os.getenv("GATE_KEY", "")
        self.secret = os.getenv("GATE_SECRET", "").encode()
        self.base = os.getenv("GATE_BASE", "https://api.gateio.ws/api/v4").rstrip("/")

        # Keep TCP/TLS connections warm for lower order latency.
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self._offset_lock = threading.Lock()
        self.offset_ms = 0
        self.offset_rtt_median_ms = None
        self.offset_synced_at_ms = None

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.upper().replace("-", "_")

    def _timeout(self, timeout_sec, default_sec: float) -> float:
        try:
            t = float(timeout_sec)
            if t > 0:
                return t
        except Exception:
            pass
        return default_sec

    def _extract_server_time_ms(self, payload: dict) -> int:
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected /spot/time payload: {payload}")

        for key in ("server_time_ms", "serverTimeMs", "server_time", "serverTime", "time"):
            if key not in payload:
                continue
            try:
                raw = float(payload[key])
            except Exception:
                continue
            # Some variants return seconds, some milliseconds.
            if raw < 10_000_000_000:
                return int(raw * 1000)
            return int(raw)

        raise RuntimeError(f"server time field missing in payload: {payload}")

    def _sign_headers(
        self,
        method: str,
        path_with_prefix: str,
        query: str,
        body: str = "",
        timestamp_sec: int = None,
        body_hash: str = None,
    ) -> dict:
        ts = str(int(timestamp_sec if timestamp_sec is not None else self.current_timestamp_sec()))
        body_sha = body_hash or hashlib.sha512((body or "").encode()).hexdigest()
        sign_str = f"{method}\n{path_with_prefix}\n{query}\n{body_sha}\n{ts}"
        sign = hmac.new(self.secret, sign_str.encode(), hashlib.sha512).hexdigest()
        return {
            "KEY": self.key,
            "Timestamp": ts,
            "SIGN": sign,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _fetch_spot_time_ms(self, timeout_sec: float = 1.5) -> int:
        r = self.session.get(self.base + "/spot/time", timeout=self._timeout(timeout_sec, 1.5))
        r.raise_for_status()
        payload = safe_json(r)
        return self._extract_server_time_ms(payload)

    def sync_server_offset(self, samples: int = 5, per_request_timeout: float = 1.5) -> dict:
        sample_n = max(1, int(samples))
        offsets = []
        rtts = []

        for _ in range(sample_n):
            t0 = int(time.time() * 1000)
            server_time_ms = self._fetch_spot_time_ms(timeout_sec=per_request_timeout)
            t1 = int(time.time() * 1000)

            rtt = max(0, t1 - t0)
            local_mid = t0 + (rtt / 2.0)
            offset_i = int(server_time_ms - local_mid)

            offsets.append(offset_i)
            rtts.append(rtt)

        if not offsets:
            raise RuntimeError("offset sync failed: no samples")

        median_offset = int(statistics.median(offsets))
        median_rtt = int(statistics.median(rtts)) if rtts else None

        with self._offset_lock:
            self.offset_ms = median_offset
            self.offset_rtt_median_ms = median_rtt
            self.offset_synced_at_ms = int(time.time() * 1000)

        return {
            "offset_ms": median_offset,
            "rtt_median_ms": median_rtt,
            "samples": sample_n,
            "synced_at_ms": self.offset_synced_at_ms,
        }

    def current_timestamp_sec(self) -> int:
        with self._offset_lock:
            offset = int(self.offset_ms or 0)
        local_ms = int(time.time() * 1000)
        return int((local_ms + offset) // 1000)

    def _build_market_order_plan(self, symbol: str, side: str, amount: str) -> dict:
        pair = self.normalize_symbol(symbol)
        path = "/spot/orders"
        body = {
            "currency_pair": pair,
            "type": "market",
            "account": "spot",
            "side": side,
            "time_in_force": "ioc",
            "amount": str(amount),
        }
        payload = json.dumps(body, separators=(",", ":"))
        body_hash = hashlib.sha512(payload.encode()).hexdigest()
        return {
            "exchange": "gate",
            "method": "POST",
            "path": path,
            "path_with_prefix": "/api/v4" + path,
            "query": "",
            "symbol": pair,
            "side": side,
            "payload": payload,
            "body_hash": body_hash,
            "safe_no_trade": False,
        }

    def build_market_buy_plan(self, symbol: str, quote_qty: str) -> dict:
        return self._build_market_order_plan(symbol=symbol, side="buy", amount=quote_qty)

    def build_market_sell_plan(self, symbol: str, base_qty: str) -> dict:
        return self._build_market_order_plan(symbol=symbol, side="sell", amount=base_qty)

    def execute_prepared_plan(self, plan: dict, timeout_sec=None) -> dict:
        ts = self.current_timestamp_sec()
        headers = self._sign_headers(
            plan.get("method", "POST"),
            plan.get("path_with_prefix", ""),
            plan.get("query", ""),
            body="",
            timestamp_sec=ts,
            body_hash=plan.get("body_hash", ""),
        )
        t0 = time.perf_counter()
        r = self.session.post(
            self.base + plan.get("path", "/spot/orders"),
            headers=headers,
            data=plan.get("payload", "{}"),
            timeout=self._timeout(timeout_sec, 0.8),
        )
        return {
            "status": r.status_code,
            "body": safe_json(r),
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "timestamp_sec": ts,
        }

    def market_buy_quote(self, symbol: str, quote_qty: str, timeout_sec=None) -> dict:
        plan = self.build_market_buy_plan(symbol, quote_qty)
        return self.execute_prepared_plan(plan, timeout_sec=timeout_sec)

    def market_sell_base(self, symbol: str, base_qty: str, timeout_sec=None) -> dict:
        plan = self.build_market_sell_plan(symbol, base_qty)
        return self.execute_prepared_plan(plan, timeout_sec=timeout_sec)

    def probe_order_rtt(self, symbol: str, quote_qty: str) -> dict:
        # Safe probe: invalid amount=0 on live order path cannot execute a trade.
        plan = self.build_market_buy_plan(symbol, "0")
        out = self.execute_prepared_plan(plan, timeout_sec=0.8)
        out["probe_type"] = "order_endpoint_invalid_amount"
        out["safe_no_trade"] = True
        return out

    def warmup_connection(self, sync_samples: int = 0, timeout_sec: float = 1.5) -> dict:
        t0 = time.perf_counter()
        try:
            r = self.session.get(self.base + "/spot/time", timeout=self._timeout(timeout_sec, 1.5))
            status = r.status_code
            ok = status < 500
            err = None
        except Exception as e:
            status = None
            ok = False
            err = f"{type(e).__name__}: {e}"

        out = {
            "ok": ok,
            "status": status,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "error": err,
        }

        if sync_samples and ok:
            try:
                out["sync"] = self.sync_server_offset(samples=sync_samples, per_request_timeout=timeout_sec)
            except Exception as e:
                out["sync_error"] = f"{type(e).__name__}: {e}"

        return out


def safe_json(r):
    try:
        return r.json()
    except Exception:
        return {"text": r.text[:2000]}
