import os, time, json, hmac, hashlib, base64
import requests
from .base import Exchange

class KuCoin(Exchange):
    name = "kucoin"
    def __init__(self):
        self.key = os.getenv("KUCOIN_KEY","")
        self.secret = os.getenv("KUCOIN_SECRET","").encode()
        self.passphrase = os.getenv("KUCOIN_PASSPHRASE","")
        self.base = os.getenv("KUCOIN_BASE","https://api.kucoin.com").rstrip("/")

    def normalize_symbol(self, symbol: str) -> str:
        s = symbol.upper().replace("_","-")
        if "-" not in s and s.endswith("USDT") and len(s) > 4:
            s = s[:-4] + "-USDT"
        return s

    def _headers(self, method: str, path: str, body: str = "") -> dict:
        ts = str(int(time.time()*1000))
        prehash = ts + method.upper() + path + (body or "")
        sign = base64.b64encode(hmac.new(self.secret, prehash.encode(), hashlib.sha256).digest()).decode()
        return {"KC-API-KEY": self.key, "KC-API-SIGN": sign, "KC-API-TIMESTAMP": ts, "KC-API-PASSPHRASE": self.passphrase, "KC-API-KEY-VERSION":"2", "Content-Type":"application/json", "Accept":"application/json"}

    def market_buy_quote(self, symbol: str, quote_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        path = "/api/v1/orders"
        body = json.dumps({"clientOid": str(int(time.time()*1000)), "symbol": sym, "side":"buy", "type":"market", "funds": quote_qty})
        headers = self._headers("POST", path, body)
        r = requests.post(self.base + path, headers=headers, data=body, timeout=10)
        return {"status": r.status_code, "body": safe_json(r)}

    def market_sell_base(self, symbol: str, base_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        path = "/api/v1/orders"
        body = json.dumps({"clientOid": str(int(time.time()*1000)), "symbol": sym, "side":"sell", "type":"market", "size": base_qty})
        headers = self._headers("POST", path, body)
        r = requests.post(self.base + path, headers=headers, data=body, timeout=10)
        return {"status": r.status_code, "body": safe_json(r)}

    def probe_order_rtt(self, symbol: str, quote_qty: str) -> dict:
        sym = self.normalize_symbol(symbol)
        # KuCoin test endpoint validates order path/signature without executing a trade.
        path = "/api/v1/orders/test"
        body = json.dumps(
            {
                "clientOid": str(int(time.time()*1000)),
                "symbol": sym,
                "side":"buy",
                "type":"market",
                "funds": quote_qty,
            },
            separators=(",", ":"),
        )
        headers = self._headers("POST", path, body)
        t0 = time.perf_counter()
        r = requests.post(self.base + path, headers=headers, data=body, timeout=6)
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
