import os
import time
from .util import log
from .exchanges.gate import Gate
from .exchanges.binance import Binance
from .exchanges.mexc import MEXC
from .exchanges.kucoin import KuCoin
from .exchanges.bitget import Bitget


RUNTIME_KEYS = {
    "GATE_KEY", "GATE_SECRET", "GATE_BASE",
    "BINANCE_KEY", "BINANCE_SECRET", "BINANCE_BASE",
    "MEXC_KEY", "MEXC_SECRET", "MEXC_BASE",
    "KUCOIN_KEY", "KUCOIN_SECRET", "KUCOIN_PASSPHRASE", "KUCOIN_BASE",
    "BITGET_KEY", "BITGET_SECRET", "BITGET_PASSPHRASE", "BITGET_BASE",
}


class TradeEngine:
    def __init__(self):
        self.dry_run = os.getenv("DRY_RUN","1") == "1"
        self.adapters = {"gate": Gate(), "binance": Binance(), "mexc": MEXC(), "kucoin": KuCoin(), "bitget": Bitget()}
        self.sniper_retry_attempts = max(1, int(os.getenv("SNIPER_RETRY_ATTEMPTS", "6")))
        self.sniper_retry_delay_ms = max(10, int(os.getenv("SNIPER_RETRY_DELAY_MS", "120")))

    def _ex(self, name: str):
        k = (name or "").strip().lower()
        if k not in self.adapters:
            raise ValueError(f"unknown exchange: {name}")
        return self.adapters[k]

    def market_buy(self, exchange: str, symbol: str, spend_usdt: str):
        ex = self._ex(exchange)
        if self.dry_run:
            log(f"DRY_RUN buy: {exchange} symbol={symbol} norm={ex.normalize_symbol(symbol)} quote={spend_usdt}")
            return {"dry_run": True, "exchange": exchange, "symbol_norm": ex.normalize_symbol(symbol), "quote": spend_usdt}
        return ex.market_buy_quote(symbol, spend_usdt)

    def market_sell(self, exchange: str, symbol: str, qty: str):
        ex = self._ex(exchange)
        if self.dry_run:
            log(f"DRY_RUN sell: {exchange} symbol={symbol} norm={ex.normalize_symbol(symbol)} qty={qty}")
            return {"dry_run": True, "exchange": exchange, "symbol_norm": ex.normalize_symbol(symbol), "qty": qty}
        return ex.market_sell_base(symbol, qty)

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

    def _is_retryable_buy_error(self, result: dict) -> bool:
        status = int(result.get("status") or 0)
        if 200 <= status < 300:
            return False
        txt = self._error_text(result)
        if status in (408, 425, 429, 500, 502, 503, 504):
            return True
        # Do not retry auth/permission/balance errors.
        hard_words = (
            "invalid key", "invalid_key", "api-key", "signature", "permission", "forbidden",
            "insufficient", "balance", "too small", "minimum", "bad request",
        )
        if any(w in txt for w in hard_words):
            return False
        # Listing-edge transient errors.
        transient_words = (
            "invalid currency", "currency pair", "not found", "does not exist", "not exist",
            "trading not", "not open", "closed", "service unavailable", "timeout", "temporarily",
        )
        return any(w in txt for w in transient_words)

    def market_buy_fast(self, exchange: str, symbol: str, spend_usdt: str) -> dict:
        # Fast path for listing-open race: retry only transient failures.
        if self.dry_run:
            out = dict(self.market_buy(exchange, symbol, spend_usdt) or {})
            out.setdefault("attempt", 1)
            return out
        last = None
        for attempt in range(1, self.sniper_retry_attempts + 1):
            t0 = time.perf_counter()
            result = self.market_buy(exchange, symbol, spend_usdt)
            result = dict(result or {})
            result["attempt"] = attempt
            result["engine_latency_ms"] = int((time.perf_counter() - t0) * 1000)
            last = result
            status = int(result.get("status") or 0)
            if 200 <= status < 300:
                return result
            if attempt >= self.sniper_retry_attempts or not self._is_retryable_buy_error(result):
                return result
            time.sleep(self.sniper_retry_delay_ms / 1000.0)
        return last or {"status": 0, "body": {"message": "no result"}}

    def probe_order_latency(self, exchange: str, symbol: str, spend_usdt: str) -> dict:
        # Real RTT probe for order path without executing trades.
        ex = self._ex(exchange)
        norm_symbol = ex.normalize_symbol(symbol)
        t0 = time.perf_counter()
        try:
            result = dict(ex.probe_order_rtt(norm_symbol, spend_usdt) or {})
        except Exception as e:
            return {
                "status": None,
                "body": {"message": f"{type(e).__name__}: {e}"},
                "engine_latency_ms": int((time.perf_counter() - t0) * 1000),
                "probe_type": "probe_exception",
                "safe_no_trade": True,
                "dry_run": False,
                "symbol_norm": norm_symbol,
            }
        result["engine_latency_ms"] = int(result.get("latency_ms") or int((time.perf_counter() - t0) * 1000))
        result["dry_run"] = False
        result["symbol_norm"] = norm_symbol
        result["safe_no_trade"] = bool(result.get("safe_no_trade", True))
        return result

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

        if dry_run is not None:
            self.dry_run = bool(dry_run)
            os.environ["DRY_RUN"] = "1" if self.dry_run else "0"
            log(f"DRY_RUN set to {1 if self.dry_run else 0}")
