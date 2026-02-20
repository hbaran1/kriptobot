import re
import time
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
SEARCHED_EXCHANGES = [
    "binance",
    "okex",
    "bybit",
    "kucoin",
    "mexc",
    "bitget",
    "gate",
    "btcturk",
    "paribu",
]
QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BTC", "ETH", "TRY", "EUR", "BNB", "FDUSD")


class MarketPresenceResolver:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Mozilla/5.0"})
        self._coin_list_cache = {"at": 0.0, "data": []}
        self._symbol_cache = {}  # key -> {"at": ts, "data": ...}
        self.cache_ttl_sec = 15 * 60
        self.coin_list_ttl_sec = 6 * 3600

    def _get_json(self, url: str, params=None, timeout_sec: float = 6.0):
        r = self.s.get(url, params=params, timeout=timeout_sec)
        r.raise_for_status()
        return r.json()

    def _get_text(self, url: str, timeout_sec: float = 8.0) -> str:
        r = self.s.get(url, timeout=timeout_sec)
        r.raise_for_status()
        return r.text

    def _parse_base_symbol(self, symbol: str) -> str:
        raw = re.sub(r"\s+", "", str(symbol or "").upper())
        if not raw:
            return ""
        for sep in ("_", "-", "/"):
            if sep in raw:
                left = raw.split(sep, 1)[0]
                return re.sub(r"[^A-Z0-9]", "", left)
        compact = re.sub(r"[^A-Z0-9]", "", raw)
        for q in QUOTE_SUFFIXES:
            if compact.endswith(q) and len(compact) > len(q):
                return compact[: -len(q)]
        return compact

    def _extract_contracts(self, text: str) -> List[str]:
        sample = str(text or "")
        found = []
        found.extend(re.findall(r"\b0x[a-fA-F0-9]{40}\b", sample))

        contextual = re.findall(
            r"(?:contract address|token address|ca)\s*[:：]?\s*([1-9A-HJ-NP-Za-km-z]{32,44}|T[1-9A-HJ-NP-Za-km-z]{33})",
            sample,
            flags=re.IGNORECASE,
        )
        found.extend(contextual)

        uniq = []
        seen = set()
        for x in found:
            n = str(x).strip()
            if not n:
                continue
            key = n.lower()
            if key in seen:
                continue
            seen.add(key)
            uniq.append(n)
        return uniq

    def _fetch_listing_text(self, listing_url: Optional[str]) -> str:
        if not listing_url:
            return ""
        try:
            html = self._get_text(listing_url, timeout_sec=8.0)
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            text = soup.get_text(" ")
            return re.sub(r"\s+", " ", text).strip()[:120000]
        except Exception:
            return ""

    def _coingecko_coin_list(self):
        now = time.time()
        if now - self._coin_list_cache["at"] < self.coin_list_ttl_sec and self._coin_list_cache["data"]:
            return self._coin_list_cache["data"]
        data = self._get_json(f"{COINGECKO_BASE}/coins/list", params={"include_platform": "true"}, timeout_sec=18.0)
        self._coin_list_cache = {"at": now, "data": data or []}
        return self._coin_list_cache["data"]

    def _resolve_by_contract(self, contracts: List[str]) -> Tuple[Optional[str], List[dict]]:
        if not contracts:
            return None, []
        needle = {c.lower() for c in contracts}
        matches = []
        for row in self._coingecko_coin_list():
            plats = row.get("platforms") or {}
            for _, addr in plats.items():
                if not addr:
                    continue
                if str(addr).lower() in needle:
                    matches.append(row)
                    break
        if len(matches) == 1:
            return matches[0].get("id"), []
        if len(matches) > 1:
            cands = [{"id": x.get("id"), "symbol": x.get("symbol"), "name": x.get("name")} for x in matches[:3]]
            return None, cands
        return None, []

    def _resolve_by_coin_id(self, query: str, base_symbol: str) -> Tuple[Optional[str], List[dict], str]:
        if not query:
            return None, [], "symbol_only"
        payload = self._get_json(f"{COINGECKO_BASE}/search", params={"query": query}, timeout_sec=6.0)
        coins = (payload or {}).get("coins") or []
        if not coins:
            return None, [], "symbol_only"

        exact = [c for c in coins if str(c.get("symbol") or "").upper() == base_symbol]
        if len(exact) == 1:
            return exact[0].get("id"), [], "coin_id"
        if len(exact) > 1:
            cands = [{"id": c.get("id"), "symbol": c.get("symbol"), "name": c.get("name")} for c in exact[:3]]
            return None, cands, "symbol_only"

        # No strict symbol match: avoid speculative candidate output.
        # This prevents confusing unrelated "closest candidates".
        return None, [], "symbol_only"

    def _normalize_exchange_name(self, market_name: str, market_id: str) -> str:
        mid = str(market_id or "").lower()
        mname = str(market_name or "").lower()
        if mid in ("binance",):
            return "binance"
        if mid in ("okx", "okex"):
            return "okex"
        if mid in ("bybit", "bybit_spot", "bybit-eu"):
            return "bybit"
        if mid in ("kucoin",):
            return "kucoin"
        if mid in ("mexc",):
            return "mexc"
        if mid in ("bitget",):
            return "bitget"
        if mid in ("gate-io", "gate"):
            return "gate"
        if mid in ("btcturk",):
            return "btcturk"
        if mid in ("paribu",):
            return "paribu"

        mapping = {
            "binance": "binance",
            "okx": "okex",
            "okex": "okex",
            "bybit": "bybit",
            "kucoin": "kucoin",
            "mexc": "mexc",
            "bitget": "bitget",
            "gate": "gate",
            "btcturk": "btcturk",
            "btc turk": "btcturk",
            "paribu": "paribu",
        }
        for k, v in mapping.items():
            if k in mname:
                return v
        return ""

    def _infer_market_type(self, pair: str) -> str:
        p = str(pair or "").upper()
        if "PERP" in p or "SWAP" in p or "FUT" in p:
            return "futures"
        return "spot"

    def _fetch_markets_by_coin_id(self, coin_id: str, target_exchange: str) -> List[dict]:
        rows = []
        for page in (1, 2):
            payload = self._get_json(
                f"{COINGECKO_BASE}/coins/{coin_id}/tickers",
                params={"include_exchange_logo": "false", "page": page},
                timeout_sec=8.0,
            )
            tickers = (payload or {}).get("tickers") or []
            if not tickers:
                break
            rows.extend(tickers)

        out = []
        for t in rows:
            market = t.get("market") or {}
            ex = self._normalize_exchange_name(market.get("name"), market.get("identifier"))
            if not ex or ex == target_exchange:
                continue
            if ex not in SEARCHED_EXCHANGES:
                continue

            base = str(t.get("base") or "").upper()
            quote = str(t.get("target") or "").upper()
            if not base or not quote:
                continue
            pair = f"{base}/{quote}"
            conv = t.get("converted_volume") or {}
            vol = conv.get("usd")
            if vol is None:
                vol = t.get("volume")

            out.append(
                {
                    "exchange": ex,
                    "pair": pair,
                    "market_type": self._infer_market_type(pair),
                    "last_price": t.get("last"),
                    "volume_24h": vol,
                    "bid": None,
                    "ask": None,
                    "spread_pct": t.get("bid_ask_spread_percentage"),
                    "source": "coingecko",
                }
            )

        out = self._dedupe_rows(out)
        out = self._sort_rows(out)
        out = self._enrich_rows_with_direct_tickers(out)
        return self._sort_rows(out)

    def _cache_get(self, key: str):
        row = self._symbol_cache.get(key)
        if not row:
            return None
        if time.time() - row["at"] > self.cache_ttl_sec:
            return None
        return row["data"]

    def _cache_set(self, key: str, data):
        self._symbol_cache[key] = {"at": time.time(), "data": data}

    def _fetch_exchange_symbol_map(self, exchange: str) -> Dict[str, List[str]]:
        key = f"sym::{exchange}"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        out = {}
        if exchange == "binance":
            payload = self._get_json("https://api.binance.com/api/v3/exchangeInfo", timeout_sec=8.0)
            for row in (payload or {}).get("symbols", []):
                if str(row.get("status", "")).upper() != "TRADING":
                    continue
                b = str(row.get("baseAsset") or "").upper()
                q = str(row.get("quoteAsset") or "").upper()
                sym = str(row.get("symbol") or "").upper()
                if b and q and sym:
                    out.setdefault(b, []).append(sym)
        elif exchange == "mexc":
            payload = self._get_json("https://api.mexc.com/api/v3/exchangeInfo", timeout_sec=8.0)
            for row in (payload or {}).get("symbols", []):
                b = str(row.get("baseAsset") or "").upper()
                q = str(row.get("quoteAsset") or "").upper()
                sym = str(row.get("symbol") or "").upper()
                if b and q and sym:
                    out.setdefault(b, []).append(sym)
        elif exchange == "kucoin":
            payload = self._get_json("https://api.kucoin.com/api/v2/symbols", timeout_sec=8.0)
            for row in (payload or {}).get("data", []):
                b = str(row.get("baseCurrency") or "").upper()
                q = str(row.get("quoteCurrency") or "").upper()
                sym = str(row.get("symbol") or "").upper()
                if b and q and sym:
                    out.setdefault(b, []).append(sym)
        elif exchange == "bitget":
            payload = self._get_json("https://api.bitget.com/api/v2/spot/public/symbols", timeout_sec=8.0)
            for row in (payload or {}).get("data", []):
                b = str(row.get("baseCoin") or "").upper()
                q = str(row.get("quoteCoin") or "").upper()
                sym = str(row.get("symbol") or "").upper()
                if b and q and sym:
                    out.setdefault(b, []).append(sym)
        elif exchange == "gate":
            payload = self._get_json("https://api.gateio.ws/api/v4/spot/currency_pairs", timeout_sec=12.0)
            for row in payload or []:
                b = str(row.get("base") or "").upper()
                q = str(row.get("quote") or "").upper()
                sym = str(row.get("id") or "").upper()
                if b and q and sym:
                    out.setdefault(b, []).append(sym)
        elif exchange == "okex":
            payload = self._get_json("https://www.okx.com/api/v5/public/instruments", params={"instType": "SPOT"}, timeout_sec=10.0)
            for row in (payload or {}).get("data", []):
                state = str(row.get("state") or "").lower()
                if state and state not in ("live", "trading"):
                    continue
                b = str(row.get("baseCcy") or "").upper()
                q = str(row.get("quoteCcy") or "").upper()
                sym = str(row.get("instId") or "").upper()
                if b and q and sym:
                    out.setdefault(b, []).append(sym)
        elif exchange == "bybit":
            payload = self._get_json("https://api.bybit.com/v5/market/instruments-info", params={"category": "spot"}, timeout_sec=10.0)
            for row in (((payload or {}).get("result") or {}).get("list") or []):
                status = str(row.get("status") or "").lower()
                if status and status not in ("trading", "online", "1"):
                    continue
                b = str(row.get("baseCoin") or "").upper()
                q = str(row.get("quoteCoin") or "").upper()
                sym = str(row.get("symbol") or "").upper()
                if b and q and sym:
                    out.setdefault(b, []).append(sym)
        elif exchange == "btcturk":
            payload = self._get_json("https://api.btcturk.com/api/v2/server/exchangeinfo", timeout_sec=10.0)
            for row in (((payload or {}).get("data") or {}).get("symbols") or []):
                status = str(row.get("status") or "").upper()
                if status and status not in ("TRADING", "ENABLED", "ACTIVE"):
                    continue
                b = str(row.get("numerator") or "").upper()
                q = str(row.get("denominator") or "").upper()
                sym = (
                    str(row.get("name") or "").upper().replace("_", "")
                    or str(row.get("nameNormalized") or "").upper().replace("_", "")
                )
                if b and q and sym:
                    out.setdefault(b, []).append(sym)
        elif exchange == "paribu":
            payload = self._get_json("https://api.paribu.com/market/ticker", timeout_sec=10.0)
            rows = payload if isinstance(payload, list) else []
            for row in rows:
                market = str((row or {}).get("market") or "").upper()
                if "_" not in market:
                    continue
                b, q = market.split("_", 1)
                if b and q:
                    out.setdefault(b, []).append(f"{b}_{q}")
        self._cache_set(key, out)
        return out

    def _fetch_pair_ticker(self, exchange: str, symbol: str) -> Optional[dict]:
        try:
            if exchange == "binance":
                t24 = self._get_json("https://api.binance.com/api/v3/ticker/24hr", params={"symbol": symbol}, timeout_sec=4.0)
                bbo = self._get_json("https://api.binance.com/api/v3/ticker/bookTicker", params={"symbol": symbol}, timeout_sec=4.0)
                return {
                    "last": t24.get("lastPrice"),
                    "volume_24h": t24.get("quoteVolume"),
                    "bid": bbo.get("bidPrice"),
                    "ask": bbo.get("askPrice"),
                }
            if exchange == "mexc":
                t24 = self._get_json("https://api.mexc.com/api/v3/ticker/24hr", params={"symbol": symbol}, timeout_sec=4.0)
                return {
                    "last": t24.get("lastPrice"),
                    "volume_24h": t24.get("quoteVolume"),
                    "bid": t24.get("bidPrice"),
                    "ask": t24.get("askPrice"),
                }
            if exchange == "kucoin":
                stats = self._get_json("https://api.kucoin.com/api/v1/market/stats", params={"symbol": symbol}, timeout_sec=4.0)
                lvl1 = self._get_json("https://api.kucoin.com/api/v1/market/orderbook/level1", params={"symbol": symbol}, timeout_sec=4.0)
                sd = stats.get("data") or {}
                ld = lvl1.get("data") or {}
                return {
                    "last": sd.get("last"),
                    "volume_24h": sd.get("volValue"),
                    "bid": ld.get("bestBid") or ld.get("buy"),
                    "ask": ld.get("bestAsk") or ld.get("sell"),
                }
            if exchange == "bitget":
                t = self._get_json("https://api.bitget.com/api/v2/spot/market/tickers", params={"symbol": symbol}, timeout_sec=4.0)
                data = (t.get("data") or [{}])[0]
                return {
                    "last": data.get("close") or data.get("lastPr"),
                    "volume_24h": data.get("quoteVolume"),
                    "bid": data.get("bidPr"),
                    "ask": data.get("askPr"),
                }
            if exchange == "gate":
                t = self._get_json("https://api.gateio.ws/api/v4/spot/tickers", params={"currency_pair": symbol}, timeout_sec=4.0)
                data = (t or [{}])[0]
                return {
                    "last": data.get("last"),
                    "volume_24h": data.get("quote_volume"),
                    "bid": data.get("highest_bid"),
                    "ask": data.get("lowest_ask"),
                }
            if exchange == "okex":
                t = self._get_json("https://www.okx.com/api/v5/market/ticker", params={"instId": symbol}, timeout_sec=4.0)
                data = ((t or {}).get("data") or [{}])[0]
                return {
                    "last": data.get("last"),
                    "volume_24h": data.get("volCcy24h") or data.get("vol24h"),
                    "bid": data.get("bidPx"),
                    "ask": data.get("askPx"),
                }
            if exchange == "bybit":
                t = self._get_json("https://api.bybit.com/v5/market/tickers", params={"category": "spot", "symbol": symbol}, timeout_sec=4.0)
                data = ((((t or {}).get("result") or {}).get("list") or [{}])[0])
                return {
                    "last": data.get("lastPrice"),
                    "volume_24h": data.get("turnover24h") or data.get("volume24h"),
                    "bid": data.get("bid1Price"),
                    "ask": data.get("ask1Price"),
                }
            if exchange == "btcturk":
                t = self._get_json("https://api.btcturk.com/api/v2/ticker", params={"pairSymbol": symbol}, timeout_sec=4.0)
                data = ((t or {}).get("data") or [{}])[0]
                return {
                    "last": data.get("last"),
                    "volume_24h": data.get("volume"),
                    "bid": data.get("bid"),
                    "ask": data.get("ask"),
                }
            if exchange == "paribu":
                t = self._get_json("https://api.paribu.com/market/ticker", params={"market": str(symbol or "").lower()}, timeout_sec=4.0)
                row = (t or [{}])[0] if isinstance(t, list) else {}
                return {
                    "last": row.get("last"),
                    "volume_24h": row.get("volume"),
                    "bid": row.get("highest_bid") or row.get("highestBid"),
                    "ask": row.get("lowest_ask") or row.get("lowestAsk"),
                }
        except Exception:
            return None
        return None

    def _to_exchange_symbol(self, exchange: str, pair: str) -> str:
        p = str(pair or "").upper().strip()
        if "/" in p:
            base, quote = p.split("/", 1)
        elif "-" in p:
            base, quote = p.split("-", 1)
        elif "_" in p:
            base, quote = p.split("_", 1)
        else:
            for q in ("USDT", "USDC", "BTC", "ETH", "USD", "TRY", "EUR"):
                if p.endswith(q) and len(p) > len(q):
                    base, quote = p[: -len(q)], q
                    break
            else:
                return p
        base = re.sub(r"[^A-Z0-9]", "", base)
        quote = re.sub(r"[^A-Z0-9]", "", quote)
        if exchange == "gate":
            return f"{base}_{quote}"
        if exchange == "kucoin":
            return f"{base}-{quote}"
        if exchange == "okex":
            return f"{base}-{quote}"
        if exchange == "paribu":
            q = "TL" if quote == "TRY" else quote
            return f"{base}_{q}".lower()
        if exchange == "btcturk":
            return f"{base}{quote}"
        return f"{base}{quote}"

    def _enrich_rows_with_direct_tickers(self, rows: List[dict]) -> List[dict]:
        # Keep ARM latency bounded: enrich top rows only.
        for row in rows[:24]:
            ex = str(row.get("exchange") or "").lower()
            pair = str(row.get("pair") or "")
            if not ex or not pair:
                continue
            symbol = self._to_exchange_symbol(ex, pair)
            ticker = self._fetch_pair_ticker(ex, symbol) or {}
            if ticker.get("last") is not None:
                row["last_price"] = ticker.get("last")
            if ticker.get("volume_24h") is not None:
                row["volume_24h"] = ticker.get("volume_24h")
            if ticker.get("bid") is not None:
                row["bid"] = ticker.get("bid")
            if ticker.get("ask") is not None:
                row["ask"] = ticker.get("ask")
            if row.get("spread_pct") is None:
                try:
                    bid = float(row.get("bid"))
                    ask = float(row.get("ask"))
                    if ask > 0:
                        row["spread_pct"] = ((ask - bid) / ask) * 100.0
                except Exception:
                    pass
        return rows

    def _build_symbol_only_rows(self, base_symbol: str, target_exchange: str) -> List[dict]:
        out = []
        for ex in SEARCHED_EXCHANGES:
            if ex == target_exchange:
                continue
            try:
                sym_map = self._fetch_exchange_symbol_map(ex)
            except Exception:
                continue
            pairs = sym_map.get(base_symbol) or []
            # Keep only major quotes and small sample.
            selected = []
            for pair in pairs:
                up = pair.upper()
                if any(up.endswith(q) for q in ("USDT", "USDC", "BTC", "ETH")):
                    selected.append(pair)
            selected = selected[:4]
            for pair in selected:
                ticker = self._fetch_pair_ticker(ex, pair)
                bid = (ticker or {}).get("bid")
                ask = (ticker or {}).get("ask")
                spread = None
                try:
                    b = float(bid)
                    a = float(ask)
                    if a > 0:
                        spread = ((a - b) / a) * 100.0
                except Exception:
                    spread = None

                out.append(
                    {
                        "exchange": ex,
                        "pair": self._human_pair(ex, pair),
                        "market_type": "spot",
                        "last_price": (ticker or {}).get("last"),
                        "volume_24h": (ticker or {}).get("volume_24h"),
                        "bid": bid,
                        "ask": ask,
                        "spread_pct": spread,
                        "source": "symbol_only",
                    }
                )
        out = self._dedupe_rows(out)
        out = self._sort_rows(out)
        out = self._enrich_rows_with_direct_tickers(out)
        return self._sort_rows(out)

    def _human_pair(self, exchange: str, pair: str) -> str:
        p = str(pair).upper()
        if exchange == "kucoin":
            if "-" in p:
                return p.replace("-", "/")
        if exchange == "okex":
            if "-" in p:
                return p.replace("-", "/")
        if exchange == "gate":
            if "_" in p:
                return p.replace("_", "/")
        if exchange in ("paribu",):
            if "_" in p:
                return p.replace("_", "/")
        for q in ("USDT", "USDC", "BTC", "ETH", "USD"):
            if p.endswith(q) and len(p) > len(q):
                return f"{p[:-len(q)]}/{q}"
        return p

    def _to_float(self, x):
        try:
            return float(x)
        except Exception:
            return None

    def _dedupe_rows(self, rows: List[dict]) -> List[dict]:
        best = {}
        for r in rows:
            key = f"{r.get('exchange')}::{r.get('pair')}"
            cur = best.get(key)
            if cur is None:
                best[key] = r
                continue
            cur_vol = self._to_float(cur.get("volume_24h")) or -1
            new_vol = self._to_float(r.get("volume_24h")) or -1
            if new_vol > cur_vol:
                best[key] = r
        return list(best.values())

    def _sort_rows(self, rows: List[dict]) -> List[dict]:
        def sort_key(r):
            v = self._to_float(r.get("volume_24h"))
            return (v is None, -(v or 0.0))

        return sorted(rows, key=sort_key)

    def _summarize(self, rows: List[dict]) -> dict:
        if not rows:
            return {"top_exchanges": [], "reference_price": None, "price_range": {"min": None, "max": None}}
        top = []
        seen = set()
        for r in rows:
            ex = r.get("exchange")
            if ex in seen:
                continue
            seen.add(ex)
            top.append(ex)
            if len(top) >= 3:
                break
        ref = rows[0].get("last_price")
        prices = [self._to_float(r.get("last_price")) for r in rows]
        prices = [p for p in prices if p is not None]
        return {
            "top_exchanges": top,
            "reference_price": ref,
            "price_range": {
                "min": min(prices) if prices else None,
                "max": max(prices) if prices else None,
            },
        }

    def analyze(
        self,
        target_exchange: str,
        symbol: str,
        listing_title: Optional[str] = None,
        listing_url: Optional[str] = None,
        contract_hint: Optional[str] = None,
    ) -> dict:
        target_exchange = str(target_exchange or "").strip().lower()
        base_symbol = self._parse_base_symbol(symbol)
        listing_text = self._fetch_listing_text(listing_url)
        contracts = self._extract_contracts(" ".join([str(contract_hint or ""), str(listing_title or ""), listing_text]))

        method = "symbol_only"
        ambiguous = False
        candidates = []
        candidate_reason = ""
        coin_id = None
        coin_name = None
        contract_used = None
        rows = []
        explain = ""

        if contracts:
            cid, cands = self._resolve_by_contract(contracts)
            if cid:
                coin_id = cid
                method = "contract"
                contract_used = contracts[0]
            elif cands:
                ambiguous = True
                candidates = cands
                candidate_reason = "multiple_contract_matches"
                explain = "Contract match is ambiguous; multiple assets share the same contract."

        if not coin_id:
            try:
                query = f"{base_symbol} {listing_title or ''}".strip()
                cid, cands, method_hint = self._resolve_by_coin_id(query, base_symbol)
                if cid:
                    coin_id = cid
                    if method != "contract":
                        method = "coin_id"
                elif cands:
                    ambiguous = True
                    if not candidates:
                        candidates = cands
                        candidate_reason = "multiple_exact_symbol_matches"
                    method = "symbol_only"
                    if not explain:
                        explain = "Exact coin ID could not be resolved because multiple assets share the same symbol."
                else:
                    method = "symbol_only"
            except Exception:
                method = "symbol_only"

        if coin_id:
            try:
                rows = self._fetch_markets_by_coin_id(coin_id, target_exchange=target_exchange)
                try:
                    coin_meta = self._get_json(f"{COINGECKO_BASE}/coins/{coin_id}", params={"localization": "false"}, timeout_sec=6.0)
                    coin_name = coin_meta.get("name") or coin_name
                except Exception:
                    pass
            except Exception:
                rows = []

        if not rows:
            rows = self._build_symbol_only_rows(base_symbol, target_exchange=target_exchange)
            if method != "contract":
                method = "symbol_only"

        found = bool(rows)
        if not explain:
            if found:
                explain = "Token is already listed on other exchanges."
            else:
                explain = "This token was not found on other major exchanges."

        if method == "symbol_only":
            explain += " Result is based on symbol matching only."

        # Show candidates only when we have a strict, defensible ambiguity reason.
        if candidate_reason not in ("multiple_contract_matches", "multiple_exact_symbol_matches"):
            candidates = []
            ambiguous = False

        return {
            "target_exchange": target_exchange,
            "symbol_input": symbol,
            "base_symbol": base_symbol,
            "method": method,
            "contract_used": contract_used,
            "detected_contracts": contracts,
            "coin_id": coin_id,
            "coin_name": coin_name,
            "ambiguous": ambiguous,
            "candidate_reason": candidate_reason,
            "candidates": candidates[:3],
            "searched_exchanges": SEARCHED_EXCHANGES,
            "found_on_other_exchanges": found,
            "rows": rows[:60],
            "summary": self._summarize(rows),
            "explain": explain,
            "checked_at": int(time.time() * 1000),
        }
