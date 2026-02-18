import os, time, json, hmac, hashlib, base64
import requests
from .base import Exchange

class Bitget(Exchange):
    name = "bitget"
    def __init__(self):
        self.key = os.getenv("BITGET_KEY","")
        self.secret = os.getenv("BITGET_SECRET","").encode()
        self.passphrase = os.getenv("BITGET_PASSPHRASE","")
        self.base = os.getenv("BITGET_BASE","https://api.bitget.com").rstrip("/")

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("_","").replace("-","").upper()

    def _headers(self, method: str, path: str, body: str = "") -> dict:
        ts = str(int(time.time()*1000))
        prehash = ts + method.upper() + path + (body or "")
        sign = base64.b64encode(hmac.new(self.secret, prehash.encode(), hashlib.sha256).digest()).decode()
        return {"ACCESS-KEY": self.key, "ACCESS-SIGN": sign, "ACCESS-TIMESTAMP": ts, "ACCESS-PASSPHRASE": self.passphrase, "Content-Type":"application/json", "Accept":"application/json"}

    def market_buy_quote(self, symbol: str, quote_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        # V2 Spot endpoint (V1 is decommissioned).
        path = "/api/v2/spot/trade/place-order"
        body = json.dumps({"symbol": sym, "side":"buy", "orderType":"market", "size": quote_qty})
        headers = self._headers("POST", path, body)
        r = requests.post(self.base + path, headers=headers, data=body, timeout=10)
        return {"status": r.status_code, "body": safe_json(r)}

    def market_sell_base(self, symbol: str, base_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        path = "/api/v2/spot/trade/place-order"
        body = json.dumps({"symbol": sym, "side":"sell", "orderType":"market", "size": base_qty})
        headers = self._headers("POST", path, body)
        r = requests.post(self.base + path, headers=headers, data=body, timeout=10)
        return {"status": r.status_code, "body": safe_json(r)}

    def probe_order_rtt(self, symbol: str, quote_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        # Safe probe on live order path: invalid size=0 ensures no trade execution.
        path = "/api/v2/spot/trade/place-order"
        body = json.dumps({"symbol": sym, "side":"buy", "orderType":"market", "size":"0"}, separators=(",", ":"))
        headers = self._headers("POST", path, body)
        t0 = time.perf_counter()
        r = requests.post(self.base + path, headers=headers, data=body, timeout=6)
        return {
            "status": r.status_code,
            "body": safe_json(r),
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "probe_type": "order_endpoint_invalid_amount",
            "safe_no_trade": True,
        }

def safe_json(r):
    try: return r.json()
    except Exception: return {"text": r.text[:2000]}
