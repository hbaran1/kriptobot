import os
import time
import json
import hmac
import hashlib
import base64
import statistics
import threading

import requests
from requests.adapters import HTTPAdapter

from .base import Exchange


class Bitget(Exchange):
    name = "bitget"

    def __init__(self):
        self.key = os.getenv("BITGET_KEY", "")
        self.secret = os.getenv("BITGET_SECRET", "").encode()
        self.passphrase = os.getenv("BITGET_PASSPHRASE", "")
        self.base = os.getenv("BITGET_BASE", "https://api.bitget.com").rstrip("/")

        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self._offset_lock = threading.Lock()
        self.offset_ms = 0
        self.offset_rtt_median_ms = None
        self.offset_synced_at_ms = None

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("_", "").replace("-", "").upper()

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
            raise RuntimeError(f"unexpected /api/v2/public/time payload: {payload}")
        raw = None
        data = payload.get("data")
        if isinstance(data, str) and data.strip():
            raw = data.strip()
        elif isinstance(data, dict):
            raw = data.get("serverTime") or data.get("time")
        elif isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                raw = first.get("serverTime") or first.get("time")
            elif isinstance(first, (str, int, float)):
                raw = first
        if raw is None:
            raw = payload.get("requestTime") or payload.get("time")
        if raw is None:
            raise RuntimeError(f"server time field missing in payload: {payload}")
        val = int(float(raw))
        if val < 10_000_000_000:
            return int(val * 1000)
        return val

    def _fetch_server_time_ms(self, timeout_sec: float = 1.5) -> int:
        r = self.session.get(self.base + "/api/v2/public/time", timeout=self._timeout(timeout_sec, 1.5))
        r.raise_for_status()
        return self._extract_server_time_ms(safe_json(r))

    def sync_server_offset(self, samples: int = 5, per_request_timeout: float = 1.5) -> dict:
        sample_n = max(1, int(samples))
        offsets = []
        rtts = []
        for _ in range(sample_n):
            t0 = int(time.time() * 1000)
            server_ms = self._fetch_server_time_ms(timeout_sec=per_request_timeout)
            t1 = int(time.time() * 1000)
            rtt = max(0, t1 - t0)
            local_mid = t0 + (rtt / 2.0)
            offsets.append(int(server_ms - local_mid))
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

    def current_timestamp_ms(self) -> int:
        with self._offset_lock:
            offset = int(self.offset_ms or 0)
        return int(time.time() * 1000) + offset

    def _headers(self, method: str, path: str, body: str = "", timestamp_ms: int = None) -> dict:
        ts = str(int(timestamp_ms if timestamp_ms is not None else self.current_timestamp_ms()))
        prehash = ts + method.upper() + path + (body or "")
        sign = base64.b64encode(hmac.new(self.secret, prehash.encode(), hashlib.sha256).digest()).decode()
        return {
            "ACCESS-KEY": self.key,
            "ACCESS-SIGN": sign,
            "ACCESS-TIMESTAMP": ts,
            "ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def market_buy_quote(self, symbol: str, quote_qty: str, timeout_sec=None) -> dict:
        sym = self.normalize_symbol(symbol)
        # V2 Spot endpoint (V1 is decommissioned).
        path = "/api/v2/spot/trade/place-order"
        body = json.dumps({"symbol": sym, "side": "buy", "orderType": "market", "size": quote_qty})
        headers = self._headers("POST", path, body)
        r = self.session.post(self.base + path, headers=headers, data=body, timeout=self._timeout(timeout_sec, 10.0))
        return {"status": r.status_code, "body": safe_json(r)}

    def market_sell_base(self, symbol: str, base_qty: str, timeout_sec=None) -> dict:
        sym = self.normalize_symbol(symbol)
        path = "/api/v2/spot/trade/place-order"
        body = json.dumps({"symbol": sym, "side": "sell", "orderType": "market", "size": base_qty})
        headers = self._headers("POST", path, body)
        r = self.session.post(self.base + path, headers=headers, data=body, timeout=self._timeout(timeout_sec, 10.0))
        return {"status": r.status_code, "body": safe_json(r)}

    def probe_order_rtt(self, symbol: str, quote_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        # Safe probe on live order path: invalid size=0 ensures no trade execution.
        path = "/api/v2/spot/trade/place-order"
        body = json.dumps({"symbol": sym, "side": "buy", "orderType": "market", "size": "0"}, separators=(",", ":"))
        headers = self._headers("POST", path, body)
        t0 = time.perf_counter()
        r = self.session.post(self.base + path, headers=headers, data=body, timeout=0.8)
        return {
            "status": r.status_code,
            "body": safe_json(r),
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "probe_type": "order_endpoint_invalid_amount",
            "safe_no_trade": True,
        }

    def warmup_connection(self, timeout_sec: float = 1.5, sync_samples: int = 0, **kwargs) -> dict:
        t0 = time.perf_counter()
        try:
            r = self.session.get(self.base + "/api/v2/public/time", timeout=self._timeout(timeout_sec, 1.5))
            out = {
                "ok": r.status_code < 500,
                "status": r.status_code,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            }
            if sync_samples and out["ok"]:
                try:
                    out["sync"] = self.sync_server_offset(samples=sync_samples, per_request_timeout=timeout_sec)
                except Exception as e:
                    out["sync_error"] = f"{type(e).__name__}: {e}"
            return out
        except Exception as e:
            return {
                "ok": False,
                "status": None,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "error": f"{type(e).__name__}: {e}",
            }


def safe_json(r):
    try:
        return r.json()
    except Exception:
        return {"text": r.text[:2000]}
