import os
import time
import json
import hmac
import base64
import hashlib
import urllib.parse

import requests
from requests.adapters import HTTPAdapter

from .base import Exchange


QUOTE_SUFFIXES = ("USDT", "TL", "TRY", "BTC", "ETH")


class Paribu(Exchange):
    name = "paribu"

    def __init__(self):
        self.key = os.getenv("PARIBU_KEY", "")
        self.secret = os.getenv("PARIBU_SECRET", "")
        self.base = os.getenv("PARIBU_BASE", "https://api.paribu.com").rstrip("/")
        self._preferred_sign_mode = str(os.getenv("PARIBU_SIGN_MODE", "") or "").strip().lower() or None

        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _timeout(self, timeout_sec, default_sec: float) -> float:
        try:
            t = float(timeout_sec)
            if t > 0:
                return t
        except Exception:
            pass
        return default_sec

    def normalize_symbol(self, symbol: str) -> str:
        raw = (symbol or "").strip().upper()
        if not raw:
            return ""

        for sep in ("_", "-", "/"):
            if sep in raw:
                left, right = raw.split(sep, 1)
                left = "".join(ch for ch in left if ch.isalnum())
                right = "".join(ch for ch in right if ch.isalnum())
                if left in ("TL", "TRY") and right not in ("TL", "TRY"):
                    left, right = right, "TL"
                if right == "TRY":
                    right = "TL"
                if left and right:
                    return f"{left.lower()}_{right.lower()}"

        compact = "".join(ch for ch in raw if ch.isalnum())
        for quote in QUOTE_SUFFIXES:
            if compact.endswith(quote) and len(compact) > len(quote):
                base = compact[: -len(quote)]
                q = "TL" if quote == "TRY" else quote
                return f"{base.lower()}_{q.lower()}"
        return compact.lower()

    def _split_path_and_query(self, path: str, params: dict = None):
        raw = str(path or "").strip() or "/"
        if not raw.startswith("/"):
            raw = "/" + raw
        parsed = urllib.parse.urlsplit(raw)
        path_only = parsed.path or "/"

        query_parts = []
        if parsed.query:
            query_parts.append(parsed.query)
        if params:
            q = urllib.parse.urlencode(params, doseq=True)
            if q:
                query_parts.append(q)
        query_str = "&".join(part for part in query_parts if part)
        return path_only, query_str

    def _sign_modes(self):
        modes = []
        if self._preferred_sign_mode:
            modes.append(self._preferred_sign_mode)
        single = str(os.getenv("PARIBU_SIGN_MODE", "") or "").strip().lower()
        if single:
            modes.append(single)
        env_modes = str(os.getenv("PARIBU_SIGN_MODES", "") or "").strip()
        if env_modes:
            for part in env_modes.split(","):
                val = part.strip().lower()
                if val:
                    modes.append(val)
        # Conservative default: API reference stresses URL+body, while getting-started
        # page mentions query+body. We try all known variants if auth fails.
        for default_mode in (
            "path_body",
            "query_body",
            "query_plain_body",
            "path_body_noslash",
            "url_body",
            "url_body_noslash",
        ):
            if default_mode not in modes:
                modes.append(default_mode)
        return modes

    def _build_sign_input(self, path_only: str, query_str: str, payload: str, mode: str) -> str:
        q = f"?{query_str}" if query_str else ""
        q_plain = query_str or ""
        b = payload or ""
        mode_key = str(mode or "").strip().lower()
        if mode_key == "query_body":
            return f"{q}{b}" if q else b
        if mode_key == "query_plain_body":
            return f"{q_plain}{b}" if q_plain else b
        if mode_key == "path_body_noslash":
            return f"{path_only.lstrip('/')}{q}{b}"
        if mode_key == "url_body":
            return f"{self.base}{path_only}{q}{b}"
        if mode_key == "url_body_noslash":
            return f"{self.base}/{path_only.lstrip('/')}{q}{b}"
        # default: path + query + body
        return f"{path_only}{q}{b}"

    def _sign_headers(self, path_only: str, query_str: str = "", payload: str = "", mode: str = "path_body") -> dict:
        to_sign = self._build_sign_input(path_only, query_str, payload, mode)
        signature = base64.b64encode(
            hmac.new(self.secret.encode(), to_sign.encode(), hashlib.sha256).digest()
        ).decode()
        return {
            "Authorization": self.key,
            "X-Signature": signature,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _signed_request(self, method: str, path: str, body: dict = None, params: dict = None, timeout_sec=None) -> dict:
        method_up = str(method or "GET").upper()
        path_only, query_str = self._split_path_and_query(path, params=params)
        payload = json.dumps(body or {}, separators=(",", ":")) if method_up != "GET" else ""
        url = f"{self.base}{path_only}" + (f"?{query_str}" if query_str else "")
        last_resp = None
        sign_attempts = []

        t0 = time.perf_counter()
        for idx, mode in enumerate(self._sign_modes(), start=1):
            headers = self._sign_headers(path_only, query_str=query_str, payload=payload, mode=mode)
            if method_up == "GET":
                resp = self.session.get(url, headers=headers, timeout=self._timeout(timeout_sec, 10.0))
            elif method_up == "DELETE":
                resp = self.session.delete(url, headers=headers, data=payload or "{}", timeout=self._timeout(timeout_sec, 10.0))
            else:
                resp = self.session.post(url, headers=headers, data=payload, timeout=self._timeout(timeout_sec, 10.0))
            sign_attempts.append({"mode": mode, "status": resp.status_code})
            last_resp = resp
            body_text = (resp.text or "").strip().lower()
            looks_like_auth_failure = bool(
                body_text in ("error", "invalid signature", "invalid_signature")
                or "signature" in body_text
                or "unauthorized" in body_text
            )
            # Learn the valid sign mode to avoid an extra auth-fail attempt on next calls.
            if (200 <= resp.status_code < 300) or (resp.status_code == 400 and not looks_like_auth_failure):
                self._preferred_sign_mode = mode
            # Retry with alternative sign text only for auth/signature failures.
            retryable_codes = {401, 403}
            if method_up == "GET":
                retryable_codes.add(500)
            if resp.status_code not in retryable_codes:
                break
            if resp.status_code == 500 and not looks_like_auth_failure:
                break
            # Guard against duplicate order creation if a non-auth failure arrives.
            if method_up in ("POST", "DELETE") and idx >= 3:
                break

        out = {
            "status": (last_resp.status_code if last_resp is not None else None),
            "body": safe_json(last_resp) if last_resp is not None else {"message": "no_response"},
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "sign_attempts": sign_attempts,
            "sign_mode": sign_attempts[-1]["mode"] if sign_attempts else None,
        }
        return out

    def _signed_post(self, path: str, body: dict, timeout_sec=None) -> dict:
        return self._signed_request("POST", path, body=body, timeout_sec=timeout_sec)

    def _signed_get(self, path: str, timeout_sec=None, params: dict = None) -> dict:
        return self._signed_request("GET", path, params=params, timeout_sec=timeout_sec)

    def _signed_delete(self, path: str, body: dict = None, timeout_sec=None) -> dict:
        return self._signed_request("DELETE", path, body=body or {}, timeout_sec=timeout_sec)

    def market_buy_quote(self, symbol: str, quote_qty: str, timeout_sec=None) -> dict:
        market = self.normalize_symbol(symbol)
        body = {
            "market": market,
            "trade": "buy",
            "type": "market",
            "total": str(quote_qty),
        }
        return self._signed_post("/order", body, timeout_sec=timeout_sec)

    def market_sell_base(self, symbol: str, base_qty: str, timeout_sec=None) -> dict:
        market = self.normalize_symbol(symbol)
        body = {
            "market": market,
            "trade": "sell",
            "type": "market",
            "amount": str(base_qty),
        }
        return self._signed_post("/order", body, timeout_sec=timeout_sec)

    def probe_order_rtt(self, symbol: str, quote_qty: str) -> dict:
        market = self.normalize_symbol(symbol)
        # Safe probe on private order route with invalid total (0), should never execute.
        body = {
            "market": market,
            "trade": "buy",
            "type": "market",
            "total": "0",
        }
        out = self._signed_post("/order", body, timeout_sec=0.8)
        out["probe_type"] = "private_order_invalid_amount_probe"
        out["safe_no_trade"] = True
        return out

    def warmup_connection(self, timeout_sec: float = 1.5, **kwargs) -> dict:
        t0 = time.perf_counter()
        try:
            r = self.session.get(self.base + "/market/ticker", timeout=self._timeout(timeout_sec, 1.5))
            return {
                "ok": r.status_code < 500,
                "status": r.status_code,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            }
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
