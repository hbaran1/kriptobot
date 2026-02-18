import os, time, json, hmac, hashlib
import requests
from requests.adapters import HTTPAdapter
from .base import Exchange

class Gate(Exchange):
    name = "gate"
    def __init__(self):
        self.key = os.getenv("GATE_KEY","")
        self.secret = os.getenv("GATE_SECRET","").encode()
        self.base = os.getenv("GATE_BASE","https://api.gateio.ws/api/v4").rstrip("/")
        # Keep TCP/TLS connections warm for lower order latency.
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.upper().replace("-", "_")

    def _sign_headers(self, method: str, path_with_prefix: str, query: str, body: str) -> dict:
        ts = str(int(time.time()))
        body_hash = hashlib.sha512((body or "").encode()).hexdigest()
        sign_str = f"{method}\n{path_with_prefix}\n{query}\n{body_hash}\n{ts}"
        sign = hmac.new(self.secret, sign_str.encode(), hashlib.sha512).hexdigest()
        return {"KEY": self.key, "Timestamp": ts, "SIGN": sign, "Accept":"application/json", "Content-Type":"application/json"}

    def market_buy_quote(self, symbol: str, quote_qty: str) -> dict:
        pair = self.normalize_symbol(symbol)
        path = "/spot/orders"
        body = {"currency_pair": pair, "type":"market", "account":"spot", "side":"buy", "time_in_force":"ioc", "amount": quote_qty}
        payload = json.dumps(body, separators=(",", ":"))
        headers = self._sign_headers("POST", "/api/v4"+path, "", payload)
        t0 = time.perf_counter()
        r = self.session.post(self.base + path, headers=headers, data=payload, timeout=4)
        return {"status": r.status_code, "body": safe_json(r), "latency_ms": int((time.perf_counter() - t0) * 1000)}

    def market_sell_base(self, symbol: str, base_qty: str) -> dict:
        pair = self.normalize_symbol(symbol)
        path = "/spot/orders"
        body = {"currency_pair": pair, "type":"market", "account":"spot", "side":"sell", "time_in_force":"ioc", "amount": base_qty}
        payload = json.dumps(body, separators=(",", ":"))
        headers = self._sign_headers("POST", "/api/v4"+path, "", payload)
        t0 = time.perf_counter()
        r = self.session.post(self.base + path, headers=headers, data=payload, timeout=4)
        return {"status": r.status_code, "body": safe_json(r), "latency_ms": int((time.perf_counter() - t0) * 1000)}

    def probe_order_rtt(self, symbol: str, quote_qty: str) -> dict:
        pair = self.normalize_symbol(symbol)
        path = "/spot/orders"
        # Safe probe: order endpoint is called with amount=0, so it cannot execute.
        body = {"currency_pair": pair, "type":"market", "account":"spot", "side":"buy", "time_in_force":"ioc", "amount":"0"}
        payload = json.dumps(body, separators=(",", ":"))
        headers = self._sign_headers("POST", "/api/v4"+path, "", payload)
        t0 = time.perf_counter()
        r = self.session.post(self.base + path, headers=headers, data=payload, timeout=4)
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
