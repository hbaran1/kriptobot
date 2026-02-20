import os
import time
import random
from typing import Callable, Optional

from .util import log
from .exchanges.gate import Gate
from .exchanges.binance import Binance
from .exchanges.mexc import MEXC
from .exchanges.kucoin import KuCoin
from .exchanges.bitget import Bitget
from .exchanges.paribu import Paribu


RUNTIME_KEYS = {
    "GATE_KEY", "GATE_SECRET", "GATE_BASE",
    "BINANCE_KEY", "BINANCE_SECRET", "BINANCE_BASE",
    "MEXC_KEY", "MEXC_SECRET", "MEXC_BASE",
    "KUCOIN_KEY", "KUCOIN_SECRET", "KUCOIN_PASSPHRASE", "KUCOIN_BASE",
    "BITGET_KEY", "BITGET_SECRET", "BITGET_PASSPHRASE", "BITGET_BASE",
    "PARIBU_KEY", "PARIBU_SECRET", "PARIBU_BASE",
}


class TradeEngine:
    def __init__(self):
        self.dry_run = os.getenv("DRY_RUN", "1") == "1"
        self.adapters = {
            "gate": Gate(),
            "binance": Binance(),
            "mexc": MEXC(),
            "kucoin": KuCoin(),
            "bitget": Bitget(),
            "paribu": Paribu(),
        }

        self.sniper_retry_attempts = max(1, int(os.getenv("SNIPER_RETRY_ATTEMPTS", "6")))
        self.sniper_retry_window_ms = max(200, int(os.getenv("SNIPER_RETRY_WINDOW_MS", "2000")))
        self.sniper_attempt_timeout_ms = max(200, int(os.getenv("SNIPER_ATTEMPT_TIMEOUT_MS", "800")))
        self.sniper_retry_jitter_min_ms = max(10, int(os.getenv("SNIPER_RETRY_JITTER_MIN_MS", "50")))
        self.sniper_retry_jitter_max_ms = max(self.sniper_retry_jitter_min_ms, int(os.getenv("SNIPER_RETRY_JITTER_MAX_MS", "150")))

    def _ex(self, name: str):
        k = (name or "").strip().lower()
        if k not in self.adapters:
            raise ValueError(f"unknown exchange: {name}")
        return self.adapters[k]

    def market_buy(self, exchange: str, symbol: str, spend_usdt: str, timeout_sec: Optional[float] = None):
        ex = self._ex(exchange)
        if self.dry_run:
            log(f"DRY_RUN buy: {exchange} symbol={symbol} norm={ex.normalize_symbol(symbol)} quote={spend_usdt}")
            return {"dry_run": True, "exchange": exchange, "symbol_norm": ex.normalize_symbol(symbol), "quote": spend_usdt}
        return ex.market_buy_quote(symbol, spend_usdt, timeout_sec=timeout_sec)

    def market_sell(self, exchange: str, symbol: str, qty: str, timeout_sec: Optional[float] = None):
        ex = self._ex(exchange)
        if self.dry_run:
            log(f"DRY_RUN sell: {exchange} symbol={symbol} norm={ex.normalize_symbol(symbol)} qty={qty}")
            return {"dry_run": True, "exchange": exchange, "symbol_norm": ex.normalize_symbol(symbol), "qty": qty}
        return ex.market_sell_base(symbol, qty, timeout_sec=timeout_sec)

    def _error_text(self, result: dict) -> str:
        body = result.get("body")
        if isinstance(body, dict):
            parts = []
            for k in ("label", "message", "msg", "code", "text"):
                v = body.get(k)
                if v:
                    parts.append(str(v))
            return " | ".join(parts).lower()
        return str(body or "").lower()

    def _error_label(self, result: dict) -> str:
        body = result.get("body")
        if isinstance(body, dict):
            for k in ("label", "message", "msg", "code", "text"):
                v = body.get(k)
                if v:
                    return str(v)
        status = result.get("status")
        return f"status_{status}" if status is not None else "unknown"

    def _extract_order_id(self, body) -> str:
        if isinstance(body, dict):
            for k in ("id", "orderId", "order_id", "clientOrderId"):
                v = body.get(k)
                if v:
                    return str(v)
            data = body.get("data")
            if isinstance(data, dict):
                for k in ("id", "orderId", "order_id", "clientOrderId"):
                    v = data.get(k)
                    if v:
                        return str(v)
        return ""

    def _is_retryable_buy_error(self, result: dict) -> bool:
        status = int(result.get("status") or 0)
        if 200 <= status < 300:
            return False

        txt = self._error_text(result)

        if status in (404, 408, 425, 429, 500, 502, 503, 504):
            return True

        # Hard-stop errors.
        hard_words = (
            "invalid key", "invalid_key", "api-key", "forbidden", "permission", "unauthorized",
            "invalid signature", "signature", "insufficient", "balance",
        )
        if any(w in txt for w in hard_words):
            return False

        # Listing-edge transient errors.
        transient_words = (
            "market not ready", "not ready", "trading not", "trading disabled", "disabled",
            "currency pair", "symbol not found", "not found", "does not exist", "not exist",
            "service unavailable", "temporarily", "timeout", "too many requests",
        )
        return any(w in txt for w in transient_words)

    def market_buy_fast(
        self,
        exchange: str,
        symbol: str,
        spend_usdt: str,
        prepared_plan: Optional[dict] = None,
        telemetry_cb: Optional[Callable[..., None]] = None,
    ) -> dict:
        # Fast path for listing-open race: retry only transient failures.
        if self.dry_run:
            out = dict(self.market_buy(exchange, symbol, spend_usdt) or {})
            out.setdefault("attempt", 1)
            return out

        ex = self._ex(exchange)
        deadline_ns = time.monotonic_ns() + (self.sniper_retry_window_ms * 1_000_000)
        timeout_sec = max(0.2, self.sniper_attempt_timeout_ms / 1000.0)

        last = None
        for attempt in range(1, self.sniper_retry_attempts + 1):
            send_wall_ms = int(time.time() * 1000)
            send_mono_ns = time.monotonic_ns()
            if telemetry_cb:
                telemetry_cb("order_attempt", attempt_no=attempt, send_wall_ms=send_wall_ms, send_mono_ns=send_mono_ns)

            t0 = time.perf_counter()
            if exchange == "gate" and prepared_plan and hasattr(ex, "execute_prepared_plan"):
                result = ex.execute_prepared_plan(prepared_plan, timeout_sec=timeout_sec)
            else:
                result = ex.market_buy_quote(symbol, spend_usdt, timeout_sec=timeout_sec)

            result = dict(result or {})
            result["attempt"] = attempt
            if not isinstance(result.get("engine_latency_ms"), int):
                result["engine_latency_ms"] = int((time.perf_counter() - t0) * 1000)

            status = int(result.get("status") or 0)
            label = self._error_label(result)
            if telemetry_cb:
                telemetry_cb("order_response", attempt_no=attempt, status=status, label=label, ack_wall_ms=int(time.time() * 1000))

            last = result
            if 200 <= status < 300:
                order_id = self._extract_order_id(result.get("body"))
                if telemetry_cb:
                    telemetry_cb("order_ack", attempt_no=attempt, order_id=order_id or "")
                return result

            now_ns = time.monotonic_ns()
            out_of_window = now_ns >= deadline_ns
            retryable = self._is_retryable_buy_error(result)
            if attempt >= self.sniper_retry_attempts or out_of_window or not retryable:
                if telemetry_cb:
                    telemetry_cb(
                        "order_fail_final",
                        attempts=attempt,
                        reason=label,
                        retryable=retryable,
                        out_of_window=out_of_window,
                    )
                return result

            remaining_ms = max(0, int((deadline_ns - now_ns) / 1_000_000))
            jitter_ms = random.randint(self.sniper_retry_jitter_min_ms, self.sniper_retry_jitter_max_ms)
            sleep_ms = min(jitter_ms, remaining_ms)
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)

        return last or {"status": 0, "body": {"message": "no result"}}

    def probe_order_latency(self, exchange: str, symbol: str, spend_usdt: str) -> dict:
        # Real RTT probe for order path without executing trades.
        ex = self._ex(exchange)
        norm_symbol = ex.normalize_symbol(symbol)
        try:
            result = dict(ex.probe_order_rtt(norm_symbol, spend_usdt) or {})
        except Exception as e:
            return {
                "status": None,
                "body": {"message": f"{type(e).__name__}: {e}"},
                "engine_latency_ms": None,
                "probe_type": "probe_exception",
                "safe_no_trade": True,
                "dry_run": False,
                "symbol_norm": norm_symbol,
            }
        latency_ms = result.get("latency_ms")
        if isinstance(latency_ms, (int, float)) and latency_ms > 0:
            result["engine_latency_ms"] = int(latency_ms)
        else:
            result["engine_latency_ms"] = None
        result["dry_run"] = False
        result["symbol_norm"] = norm_symbol
        result["safe_no_trade"] = bool(result.get("safe_no_trade", True))
        return result

    def warmup_exchange(self, exchange: str, **kwargs) -> dict:
        ex = self._ex(exchange)
        fn = getattr(ex, "warmup_connection", None)
        if callable(fn):
            return fn(**kwargs)
        return {"ok": False, "message": "warmup_not_supported"}

    def apply_runtime_config(self, values: dict, dry_run=None):
        clean = {}
        for k, v in (values or {}).items():
            if k not in RUNTIME_KEYS:
                continue
            clean[k] = (v or "").strip()
            os.environ[k] = clean[k]

        if "GATE_KEY" in clean:
            self.adapters["gate"].key = clean["GATE_KEY"]
        if "GATE_SECRET" in clean:
            self.adapters["gate"].secret = clean["GATE_SECRET"].encode()
        if "GATE_BASE" in clean and clean["GATE_BASE"]:
            self.adapters["gate"].base = clean["GATE_BASE"].rstrip("/")

        if "BINANCE_KEY" in clean:
            self.adapters["binance"].key = clean["BINANCE_KEY"]
        if "BINANCE_SECRET" in clean:
            self.adapters["binance"].secret = clean["BINANCE_SECRET"].encode()
        if "BINANCE_BASE" in clean and clean["BINANCE_BASE"]:
            self.adapters["binance"].base = clean["BINANCE_BASE"].rstrip("/")

        if "MEXC_KEY" in clean:
            self.adapters["mexc"].key = clean["MEXC_KEY"]
        if "MEXC_SECRET" in clean:
            self.adapters["mexc"].secret = clean["MEXC_SECRET"].encode()
        if "MEXC_BASE" in clean and clean["MEXC_BASE"]:
            self.adapters["mexc"].base = clean["MEXC_BASE"].rstrip("/")

        if "KUCOIN_KEY" in clean:
            self.adapters["kucoin"].key = clean["KUCOIN_KEY"]
        if "KUCOIN_SECRET" in clean:
            self.adapters["kucoin"].secret = clean["KUCOIN_SECRET"].encode()
        if "KUCOIN_PASSPHRASE" in clean:
            self.adapters["kucoin"].passphrase = clean["KUCOIN_PASSPHRASE"]
        if "KUCOIN_BASE" in clean and clean["KUCOIN_BASE"]:
            self.adapters["kucoin"].base = clean["KUCOIN_BASE"].rstrip("/")

        if "BITGET_KEY" in clean:
            self.adapters["bitget"].key = clean["BITGET_KEY"]
        if "BITGET_SECRET" in clean:
            self.adapters["bitget"].secret = clean["BITGET_SECRET"].encode()
        if "BITGET_PASSPHRASE" in clean:
            self.adapters["bitget"].passphrase = clean["BITGET_PASSPHRASE"]
        if "BITGET_BASE" in clean and clean["BITGET_BASE"]:
            self.adapters["bitget"].base = clean["BITGET_BASE"].rstrip("/")

        if "PARIBU_KEY" in clean:
            self.adapters["paribu"].key = clean["PARIBU_KEY"]
        if "PARIBU_SECRET" in clean:
            self.adapters["paribu"].secret = clean["PARIBU_SECRET"]
        if "PARIBU_BASE" in clean and clean["PARIBU_BASE"]:
            self.adapters["paribu"].base = clean["PARIBU_BASE"].rstrip("/")

        if dry_run is not None:
            self.dry_run = bool(dry_run)
            os.environ["DRY_RUN"] = "1" if self.dry_run else "0"
            log(f"DRY_RUN set to {1 if self.dry_run else 0}")
