import os, time, hmac, hashlib, urllib.parse
import requests
from .base import Exchange

class Binance(Exchange):
    name = "binance"
    def __init__(self):
        self.key = os.getenv("BINANCE_KEY","")
        self.secret = os.getenv("BINANCE_SECRET","").encode()
        self.base = os.getenv("BINANCE_BASE","https://api.binance.com").rstrip("/")

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("_","").replace("-","").upper()

    def _signed(self, params: dict) -> dict:
        params = dict(params)
        params["timestamp"] = int(time.time()*1000)
        qs = urllib.parse.urlencode(params, doseq=True)
        sig = hmac.new(self.secret, qs.encode(), hashlib.sha256).hexdigest()
        return {"qs": qs + "&signature=" + sig, "headers": {"X-MBX-APIKEY": self.key}}

    def market_buy_quote(self, symbol: str, quote_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        params = {"symbol": sym, "side":"BUY", "type":"MARKET", "quoteOrderQty": quote_qty}
        signed = self._signed(params)
        r = requests.post(self.base + "/api/v3/order", headers=signed["headers"], params=signed["qs"], timeout=10)
        return {"status": r.status_code, "body": safe_json(r)}

    def market_sell_base(self, symbol: str, base_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        params = {"symbol": sym, "side":"SELL", "type":"MARKET", "quantity": base_qty}
        signed = self._signed(params)
        r = requests.post(self.base + "/api/v3/order", headers=signed["headers"], params=signed["qs"], timeout=10)
        return {"status": r.status_code, "body": safe_json(r)}

    def probe_order_rtt(self, symbol: str, quote_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        # Binance test endpoint validates order path/signature without executing a trade.
        params = {"symbol": sym, "side":"BUY", "type":"MARKET", "quoteOrderQty": quote_qty}
        signed = self._signed(params)
        t0 = time.perf_counter()
        r = requests.post(self.base + "/api/v3/order/test", headers=signed["headers"], params=signed["qs"], timeout=6)
        return {
            "status": r.status_code,
            "body": safe_json(r),
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "probe_type": "order_test_endpoint",
            "safe_no_trade": True,
        }

def safe_json(r):
    try: return r.json()
    except Exception: return {"text": r.text[:2000]}
