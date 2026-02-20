import datetime
import time
import re
import json
import html as html_lib
import os
from urllib.parse import urljoin
import requests
from collections import defaultdict
from dateutil import tz
from bs4 import BeautifulSoup

from .util import iso_utc
from .parsing.time_extract import extract_time
from .web.sources import SOURCES
from .web.parsers import parse_listings, likely_listing

NEW_TTL_SECONDS = 24 * 3600
TR_TZ = tz.gettz("Europe/Istanbul")

PRIMARY_SOURCE = {
    "gate": "api_announcement",
    "mexc": "api_announcement",
    "kucoin": "api_announcement",
    "bitget": "api_announcement",
    "binance": "api_announcement",
    "okex": "symbol_diff",
    "bybit": "symbol_diff",
    "btcturk": "symbol_diff",
    "paribu": "symbol_diff",
}

SOURCE_PRIORITY = {
    "api_announcement": 0,
    "symbol_diff": 1,
    "web_fallback": 2,
}


class ListingsAggregator:
    def __init__(self):
        self.exchanges = [
            "gate",
            "mexc",
            "kucoin",
            "bitget",
            "binance",
            "okex",
            "bybit",
            "btcturk",
            "paribu",
        ]
        self.first_seen = {}  # url -> unix_ts
        self.url_meta = {}  # url -> {"raw_time_text", "normalized_tr_time", "source_type"}
        self.items = defaultdict(list)
        self.symbol_snapshots = {
            "gate": set(),
            "mexc": set(),
            "binance": set(),
            "okex": set(),
            "bybit": set(),
            "btcturk": set(),
            "paribu": set(),
        }
        self.exchange_state = {ex: self._new_exchange_state(ex) for ex in self.exchanges}
        self.last_scan_meta = {ex: {} for ex in self.exchanges}
        self._state_dir = os.getenv(
            "SCANNER_STATE_DIR",
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "state", "scanner"),
        )
        os.makedirs(self._state_dir, exist_ok=True)
        self._load_persisted_state()

        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Mozilla/5.0"})

    def _snapshot_path(self, exchange: str) -> str:
        return os.path.join(self._state_dir, f"seen_markets_{exchange}.json")

    def _seen_urls_path(self) -> str:
        return os.path.join(self._state_dir, "seen_urls.json")

    def _load_persisted_state(self):
        for ex in list(self.symbol_snapshots.keys()):
            path = self._snapshot_path(ex)
            if not os.path.exists(path):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    rows = json.load(f)
                if isinstance(rows, list):
                    self.symbol_snapshots[ex] = {str(x).upper() for x in rows if str(x)}
            except Exception:
                continue

        seen_path = self._seen_urls_path()
        if os.path.exists(seen_path):
            try:
                with open(seen_path, "r", encoding="utf-8") as f:
                    payload = json.load(f) or {}
                first_seen = payload.get("first_seen") or {}
                url_meta = payload.get("url_meta") or {}
                if isinstance(first_seen, dict):
                    for k, v in first_seen.items():
                        try:
                            self.first_seen[str(k)] = float(v)
                        except Exception:
                            continue
                if isinstance(url_meta, dict):
                    for k, v in url_meta.items():
                        if isinstance(v, dict):
                            self.url_meta[str(k)] = {
                                "raw_time_text": str(v.get("raw_time_text") or "UNKNOWN"),
                                "normalized_tr_time": str(v.get("normalized_tr_time") or "UNKNOWN"),
                                "source_type": str(v.get("source_type") or "web_fallback"),
                            }
            except Exception:
                pass

    def _persist_exchange_snapshot(self, exchange: str):
        path = self._snapshot_path(exchange)
        rows = sorted(self.symbol_snapshots.get(exchange, set()))
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
        os.replace(tmp, path)

    def _persist_seen_urls(self):
        now = time.time()
        max_age = NEW_TTL_SECONDS * 7
        first_seen_trim = {}
        for url, ts in list(self.first_seen.items()):
            try:
                if (now - float(ts)) <= max_age:
                    first_seen_trim[url] = float(ts)
            except Exception:
                continue
        self.first_seen = first_seen_trim
        meta_trim = {url: self.url_meta.get(url, {}) for url in first_seen_trim.keys()}
        payload = {"first_seen": first_seen_trim, "url_meta": meta_trim}
        path = self._seen_urls_path()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, path)

    def _is_listing_only(self, title: str, extra: str = "") -> bool:
        t = f"{title or ''} {extra or ''}".lower()
        positives = (
            "new listing",
            "initial listing",
            "will list",
            "will be listed",
            "gets listed",
            "spot listing",
            "first in market",
            "now live on",
            "listing arrangement",
            "trading opens",
            "trading will open",
            "trading will start",
        )
        negatives = (
            "delist",
            "delisting",
            "futures",
            "perpetual",
            "remove",
            "removal",
            "collateral",
            "margin",
            "account",
            "maintenance",
            "system update",
            "promotion",
            "promo",
            "campaign",
            "launchpool",
            "launchpad",
            "kickstarter",
            "reward",
            "airdrop",
            "copy trade",
            "grid trading",
            "p2p",
            "fiat",
            "help",
            "faq",
        )
        if any(k in t for k in negatives):
            return False
        return any(k in t for k in positives)

    def _strict_listing_item(self, exchange: str, item: dict) -> bool:
        title = str(item.get("title") or "")
        url = str(item.get("url") or "")
        source_type = str(item.get("source_type") or "web_fallback")
        if source_type == "symbol_diff":
            # Symbol diff is accepted as a primary listing signal when it comes
            # from exchange symbol APIs and passed snapshot-diff filtering.
            sym = str(item.get("symbol_key") or "")
            if not sym:
                m = re.search(r"\b([A-Z0-9]{2,20}(?:[_-]?USDT))\b", f"{title} {url}".upper())
                sym = m.group(1) if m else ""
            if not sym:
                return False
            sym_up = sym.upper()
            has_major_quote = any(q in sym_up for q in ("USDT", "USDC", "USD", "TRY", "TL", "BTC", "ETH"))
            if not has_major_quote:
                return False
            return True
        if not likely_listing(exchange, url, title):
            return False
        if not self._is_listing_only(title, url):
            return False
        return True

    def _new_exchange_state(self, exchange: str) -> dict:
        return {
            "primary_source": PRIMARY_SOURCE.get(exchange, "web_fallback"),
            "active_source": PRIMARY_SOURCE.get(exchange, "web_fallback"),
            "primary_ok": None,
            "primary_error": "",
            "fallback_ok": None,
            "fallback_error": "",
            "fallback_used": False,
            "degraded": False,
            "degraded_reason": "",
        }

    def _short_error(self, exc: Exception) -> str:
        return f"{type(exc).__name__}: {exc}"[:260]

    def _get_text(self, url: str) -> str:
        r = self.s.get(url, timeout=15)
        if r.status_code >= 400 or r.status_code == 202:
            raise requests.HTTPError(f"HTTP {r.status_code} for {url}")
        return r.text

    def _get_json(self, url: str, params=None) -> dict | list:
        r = self.s.get(url, params=params, timeout=15)
        if r.status_code >= 400:
            raise requests.HTTPError(f"HTTP {r.status_code} for {url}")
        try:
            return r.json()
        except Exception as e:
            raise RuntimeError(f"Invalid JSON for {url}: {e}") from e

    def _clip_raw(self, raw: str) -> str:
        if not raw:
            return "UNKNOWN"
        raw = " ".join(raw.split())
        return raw[:180] if raw else "UNKNOWN"

    def _looks_html_degraded(self, html: str) -> bool:
        sample = (html or "")[:3000].lower()
        bad_markers = (
            "access denied",
            "forbidden",
            "captcha",
            "cloudflare",
            "attention required",
            "enable javascript",
            "please turn javascript",
        )
        return any(m in sample for m in bad_markers)

    def _prioritize_dedupe(self, items: list[dict]) -> list[dict]:
        by_url = {}
        by_title = {}
        for it in items:
            url = it.get("url")
            if not url:
                continue
            cur = by_url.get(url)
            p_new = SOURCE_PRIORITY.get(it.get("source_type", "web_fallback"), 99)
            if cur is None:
                by_url[url] = it
            else:
                p_old = SOURCE_PRIORITY.get(cur.get("source_type", "web_fallback"), 99)
                if p_new < p_old:
                    by_url[url] = it

        # Some exchanges expose the same announcement on different URLs.
        # Keep the highest-priority source for same normalized title.
        for it in by_url.values():
            tkey = " ".join(str(it.get("title") or "").lower().split())
            if not tkey:
                tkey = str(it.get("url") or "")
            cur = by_title.get(tkey)
            p_new = SOURCE_PRIORITY.get(it.get("source_type", "web_fallback"), 99)
            if cur is None:
                by_title[tkey] = it
                continue
            p_old = SOURCE_PRIORITY.get(cur.get("source_type", "web_fallback"), 99)
            if p_new < p_old:
                by_title[tkey] = it

        out = list(by_title.values())
        out.sort(key=lambda x: SOURCE_PRIORITY.get(x.get("source_type", "web_fallback"), 99))
        return out

    def _parse_next_data(self, html: str) -> dict:
        soup = BeautifulSoup(html or "", "html.parser")
        node = soup.find("script", id="__NEXT_DATA__")
        if not node:
            raise RuntimeError("__NEXT_DATA__ not found")
        try:
            return json.loads(node.get_text(strip=True) or "{}")
        except Exception as e:
            raise RuntimeError(f"__NEXT_DATA__ parse error: {e}") from e

    def _extract_article_code_from_url(self, url: str) -> str:
        m = re.search(r"/detail/([a-f0-9]{16,64})", str(url or "").lower())
        if m:
            return m.group(1)
        return ""

    def _json_text_nodes_to_plain(self, raw: str) -> str:
        if not raw:
            return ""
        text_nodes = []
        try:
            obj = json.loads(raw)

            def walk(v):
                if isinstance(v, dict):
                    for k, subv in v.items():
                        if k == "text" and isinstance(subv, str):
                            text_nodes.append(subv)
                        else:
                            walk(subv)
                elif isinstance(v, list):
                    for subv in v:
                        walk(subv)

            walk(obj)
        except Exception:
            text_nodes = []
        if not text_nodes:
            return re.sub(r"\s+", " ", str(raw)).strip()
        merged = " ".join(html_lib.unescape(t) for t in text_nodes if t)
        return re.sub(r"\s+", " ", merged).strip()

    def _fetch_binance_detail_time(self, article_code: str):
        if not article_code:
            return "UNKNOWN", "UNKNOWN"
        payload = self._get_json(
            "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query",
            params={"articleCode": article_code},
        )
        data = (payload or {}).get("data") or {}

        candidates = []
        body = data.get("body")
        if body:
            candidates.append(self._json_text_nodes_to_plain(str(body)))
        content_json = data.get("contentJson")
        if content_json:
            candidates.append(self._json_text_nodes_to_plain(str(content_json)))
        title = str(data.get("title") or "")
        if title:
            candidates.append(title)
        desc = str(data.get("seoDesc") or "")
        if desc:
            candidates.append(desc)

        for txt in candidates:
            raw, norm = extract_time("binance", txt)
            if norm != "UNKNOWN":
                return self._clip_raw(raw), norm
        return "UNKNOWN", "UNKNOWN"

    def _fetch_gate_api_announcements(self):
        candidates = [
            "https://apim.gateapi.io/announcements/newspotlistings",
            "https://www.gate.com/announcements/newspotlistings",
        ]
        last_err = None
        for src in candidates:
            try:
                html = self._get_text(src)
                nd = self._parse_next_data(html)
                rows = (((nd.get("props") or {}).get("pageProps") or {}).get("listData") or {}).get("list") or []
                out = []
                for row in rows:
                    title = str(row.get("title") or "").strip()
                    rel = str(row.get("url") or "").strip()
                    if not title or not rel:
                        continue
                    if not self._is_listing_only(title, "gate"):
                        continue
                    url = rel if rel.startswith("http") else urljoin(src, rel)
                    published_at = ""
                    try:
                        ts = int(str(row.get("release_timestamp") or "0"))
                        if ts > 0:
                            published_at = datetime.datetime.fromtimestamp(
                                ts, tz=datetime.timezone.utc
                            ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                    except Exception:
                        pass

                    brief = str(row.get("brief") or "")
                    raw, norm = extract_time("gate", brief)
                    out.append(
                        {
                            "exchange": "gate",
                            "title": title[:120],
                            "url": url,
                            "published_at": published_at,
                            "raw_time_text": self._clip_raw(raw),
                            "normalized_tr_time": norm,
                            "summary_text": brief,
                        }
                    )
                return self._with_source(out[:80], "api_announcement")
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"Gate announcement feed failed: {last_err}")

    def _fetch_mexc_api_announcements(self):
        candidates = [
            "https://www.mexc.com/announcements/new-listings",
            "https://www.mexc.com/newlisting",
        ]
        last_err = None
        for src in candidates:
            try:
                html = self._get_text(src)
                nd = self._parse_next_data(html)
                rows = (((nd.get("props") or {}).get("pageProps") or {}).get("_sectionArticles")) or []
                out = []
                for row in rows:
                    title = str(row.get("title") or "").strip()
                    article_id = row.get("id")
                    if not title or not article_id:
                        continue
                    if not self._is_listing_only(title, "mexc"):
                        continue
                    url = f"https://www.mexc.com/support/articles/{article_id}"
                    published_at = ""
                    try:
                        ms = int(str(row.get("displayTime") or row.get("publishTime") or "0"))
                        if ms > 0:
                            published_at = datetime.datetime.fromtimestamp(
                                ms / 1000.0, tz=datetime.timezone.utc
                            ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                    except Exception:
                        pass

                    content_text = str(
                        row.get("content")
                        or row.get("summary")
                        or row.get("desc")
                        or row.get("brief")
                        or ""
                    )
                    raw, norm = extract_time("mexc", content_text)
                    out.append(
                        {
                            "exchange": "mexc",
                            "title": title[:120],
                            "url": url,
                            "published_at": published_at,
                            "raw_time_text": self._clip_raw(raw),
                            "normalized_tr_time": norm,
                            "summary_text": content_text,
                        }
                    )
                return self._with_source(out[:80], "api_announcement")
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"MEXC announcement feed failed: {last_err}")

    def _with_source(self, items: list[dict], source_type: str) -> list[dict]:
        out = []
        for row in items:
            it = dict(row)
            it["source_type"] = source_type
            out.append(it)
        return out

    def _fetch_web_fallback_items(self, exchange: str):
        batch = []
        errors = []
        degraded = False
        for u in SOURCES[exchange]["list_urls"]:
            try:
                html = self._get_text(u)
                parsed = parse_listings(exchange, u, html)
                if not parsed and self._looks_html_degraded(html):
                    degraded = True
                    errors.append(f"HTML_BREAK for {u}")
                batch.extend(self._with_source(parsed, "web_fallback"))
            except Exception as e:
                msg = self._short_error(e)
                errors.append(msg)
                if "HTTP 403" in msg or "HTTP 4" in msg:
                    degraded = True
        return batch[:80], errors, degraded

    def _fetch_bitget_api_announcements(self):
        payload = self._get_json(
            "https://api.bitget.com/api/v2/public/annoucements",
            params={"language": "en_US"},
        )
        rows = (payload or {}).get("data") or []
        out = []
        for row in rows:
            title = str(row.get("annTitle") or "").strip()
            if not title:
                continue
            url = str(row.get("annUrl") or "").strip()
            if not url:
                ann_id = str(row.get("annId") or "").strip()
                if not ann_id:
                    continue
                url = f"https://www.bitget.com/en/support/articles/{ann_id}"

            ann_type = str(row.get("annType") or "").lower()
            ann_desc = str(row.get("annDesc") or "").lower()
            strict_type_ok = ("listing" in ann_type or "listing" in ann_desc) and ("delist" not in ann_type and "delist" not in ann_desc)
            strict_title_ok = self._is_listing_only(title, f"{ann_type} {ann_desc}")
            if not strict_type_ok and not strict_title_ok:
                continue
            out.append({"exchange": "bitget", "title": title[:120], "url": url})
        return self._with_source(out[:80], "api_announcement")

    def _fetch_kucoin_api_announcements(self):
        endpoint_candidates = [
            ("https://api.kucoin.com/api/v3/announcements", "items"),
            ("https://api.kucoin.com/api/ua/v1/market/announcement", "list"),
        ]
        last_err = None
        for endpoint, list_key in endpoint_candidates:
            try:
                payload = self._get_json(endpoint, params={"lang": "en_US", "pageSize": 40, "currentPage": 1})
                data = (payload or {}).get("data") or {}
                rows = data.get(list_key) or []
                if not isinstance(rows, list):
                    rows = []
                out = []
                for row in rows:
                    title = str(row.get("annTitle") or row.get("title") or "").strip()
                    url = str(row.get("annUrl") or row.get("url") or "").strip()
                    if not title or not url:
                        continue
                    ann_type = row.get("annType") or row.get("type") or []
                    if isinstance(ann_type, list):
                        tlist = [str(x).lower() for x in ann_type]
                    else:
                        tlist = [str(ann_type).lower()]
                    tset = " ".join(tlist)

                    has_delist_or_futures = any(("delist" in t or "futures" in t or "perpetual" in t) for t in tlist)
                    strict_title_ok = self._is_listing_only(title, tset)
                    strict_type_ok = any(("listing" in t and "delist" not in t) for t in tlist)
                    if has_delist_or_futures:
                        continue
                    if not strict_type_ok and not strict_title_ok:
                        continue
                    out.append({"exchange": "kucoin", "title": title[:120], "url": url})
                return self._with_source(out[:80], "api_announcement")
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"KuCoin announcement API failed: {last_err}")

    def _fetch_binance_api_announcements(self):
        payload = self._get_json(
            "https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query",
            params={"catalogId": 48, "pageNo": 1, "pageSize": 50},
        )
        rows = ((payload or {}).get("data") or {}).get("articles") or []
        out = []
        for row in rows:
            title = str(row.get("title") or "").strip()
            code = str(row.get("code") or "").strip()
            article_id = row.get("id")
            if not title:
                continue
            if code:
                url = f"https://www.binance.com/en/support/announcement/detail/{code}"
            elif article_id:
                url = f"https://www.binance.com/en/support/announcement/{article_id}"
            else:
                continue
            if not self._is_listing_only(title, "binance"):
                continue
            if not likely_listing("binance", url, title):
                continue
            published_at = ""
            try:
                ms = int(str(row.get("publishDate") or "0"))
                if ms > 0:
                    published_at = datetime.datetime.fromtimestamp(
                        ms / 1000.0, tz=datetime.timezone.utc
                    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except Exception:
                pass
            out.append(
                {
                    "exchange": "binance",
                    "title": title[:120],
                    "url": url,
                    "article_code": code,
                    "published_at": published_at,
                }
            )
        return self._with_source(out[:80], "api_announcement")

    def _fetch_gate_symbol_rows(self):
        rows = self._get_json("https://api.gateio.ws/api/v4/spot/currency_pairs")
        out = []
        for row in rows or []:
            pair = str(row.get("id") or "").strip().upper()
            base = str(row.get("base") or "").strip().upper()
            quote = str(row.get("quote") or "").strip().upper()
            trade_status = str(row.get("trade_status") or "").strip().lower()
            if not pair or not base or quote != "USDT":
                continue
            if trade_status and trade_status != "tradable":
                continue
            buy_start = row.get("buy_start")
            raw_time = "UNKNOWN"
            norm_time = "UNKNOWN"
            try:
                ts = int(buy_start)
                if ts > 0:
                    dt_utc = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
                    dt_tr = dt_utc.astimezone(TR_TZ) if TR_TZ else dt_utc
                    raw_time = dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
                    norm_time = dt_tr.replace(microsecond=0).isoformat()
            except Exception:
                pass
            out.append(
                {
                    "symbol_key": pair,
                    "title": f"{pair} detected in Gate symbol list",
                    "url": f"https://www.gate.com/trade/{pair}",
                    "raw_time_text": raw_time,
                    "normalized_tr_time": norm_time,
                }
            )
        return out

    def _fetch_binance_symbol_rows(self):
        payload = self._get_json("https://api.binance.com/api/v3/exchangeInfo")
        out = []
        for row in (payload or {}).get("symbols", []):
            symbol = str(row.get("symbol") or "").strip().upper()
            base = str(row.get("baseAsset") or "").strip().upper()
            quote = str(row.get("quoteAsset") or "").strip().upper()
            status = str(row.get("status") or "").strip().upper()
            if not symbol or not base or quote != "USDT":
                continue
            if status and status != "TRADING":
                continue
            out.append(
                {
                    "symbol_key": symbol,
                    "title": f"{symbol} detected in Binance symbol list",
                    "url": f"https://www.binance.com/en/trade/{base}_{quote}?type=spot",
                    "raw_time_text": "UNKNOWN",
                    "normalized_tr_time": "UNKNOWN",
                }
            )
        return out

    def _fetch_mexc_symbol_rows(self):
        payload = self._get_json("https://api.mexc.com/api/v3/exchangeInfo")
        out = []
        for row in (payload or {}).get("symbols", []):
            symbol = str(row.get("symbol") or "").strip().upper()
            base = str(row.get("baseAsset") or "").strip().upper()
            quote = str(row.get("quoteAsset") or "").strip().upper()
            status = str(row.get("status") or "").strip().upper()
            spot_allowed = bool(row.get("isSpotTradingAllowed", True))
            if not symbol or not base or quote != "USDT":
                continue
            active_status = status in ("1", "TRADING", "ENABLED", "")
            if not active_status or not spot_allowed:
                continue
            out.append(
                {
                    "symbol_key": symbol,
                    "title": f"{symbol} detected in MEXC symbol list",
                    "url": f"https://www.mexc.com/exchange/{base}_{quote}",
                    "raw_time_text": "UNKNOWN",
                    "normalized_tr_time": "UNKNOWN",
                }
            )
        return out

    def _fetch_okex_symbol_rows(self):
        payload = self._get_json(
            "https://www.okx.com/api/v5/public/instruments",
            params={"instType": "SPOT"},
        )
        out = []
        for row in (payload or {}).get("data", []):
            state = str(row.get("state") or "").strip().lower()
            if state and state not in ("live", "trading"):
                continue
            base = str(row.get("baseCcy") or "").strip().upper()
            quote = str(row.get("quoteCcy") or "").strip().upper()
            symbol = str(row.get("instId") or "").strip().upper()
            if not symbol or not base or quote != "USDT":
                continue
            raw_time = "UNKNOWN"
            norm_time = "UNKNOWN"
            try:
                # listTime may be ms/ns depending environment.
                val = int(str(row.get("listTime") or "0"))
                if val > 0:
                    if val > 10_000_000_000_000:
                        val = int(val / 1_000_000)
                    if val < 10_000_000_000:
                        val = int(val * 1000)
                    dt_utc = datetime.datetime.fromtimestamp(val / 1000.0, tz=datetime.timezone.utc)
                    dt_tr = dt_utc.astimezone(TR_TZ) if TR_TZ else dt_utc
                    raw_time = dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
                    norm_time = dt_tr.replace(microsecond=0).isoformat()
            except Exception:
                pass
            out.append(
                {
                    "symbol_key": symbol,
                    "title": f"{symbol} detected in OKX symbol list",
                    "url": f"https://www.okx.com/trade-spot/{base.lower()}-{quote.lower()}",
                    "raw_time_text": raw_time,
                    "normalized_tr_time": norm_time,
                }
            )
        return out

    def _fetch_bybit_symbol_rows(self):
        payload = self._get_json(
            "https://api.bybit.com/v5/market/instruments-info",
            params={"category": "spot"},
        )
        out = []
        rows = (((payload or {}).get("result") or {}).get("list") or [])
        for row in rows:
            status = str(row.get("status") or "").strip().lower()
            if status and status not in ("trading", "online", "1"):
                continue
            base = str(row.get("baseCoin") or "").strip().upper()
            quote = str(row.get("quoteCoin") or "").strip().upper()
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol or not base or quote != "USDT":
                continue
            raw_time = "UNKNOWN"
            norm_time = "UNKNOWN"
            try:
                val = int(str(row.get("launchTime") or row.get("launchTimeMs") or "0"))
                if val > 0:
                    if val > 10_000_000_000_000:
                        val = int(val / 1_000_000)
                    if val < 10_000_000_000:
                        val = int(val * 1000)
                    dt_utc = datetime.datetime.fromtimestamp(val / 1000.0, tz=datetime.timezone.utc)
                    dt_tr = dt_utc.astimezone(TR_TZ) if TR_TZ else dt_utc
                    raw_time = dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
                    norm_time = dt_tr.replace(microsecond=0).isoformat()
            except Exception:
                pass
            out.append(
                {
                    "symbol_key": symbol,
                    "title": f"{symbol} detected in Bybit symbol list",
                    "url": f"https://www.bybit.com/trade/spot/{base}/{quote}",
                    "raw_time_text": raw_time,
                    "normalized_tr_time": norm_time,
                }
            )
        return out

    def _fetch_btcturk_symbol_rows(self):
        payload = self._get_json("https://api.btcturk.com/api/v2/server/exchangeinfo")
        rows = (((payload or {}).get("data") or {}).get("symbols") or [])
        out = []
        for row in rows:
            status = str(row.get("status") or "").strip().upper()
            if status and status not in ("TRADING", "ACTIVE", "ENABLED"):
                continue
            base = str(row.get("numerator") or "").strip().upper()
            quote = str(row.get("denominator") or "").strip().upper()
            sym = str(row.get("nameNormalized") or row.get("name") or "").strip().upper().replace("_", "")
            if not sym or not base or quote != "TRY":
                continue
            out.append(
                {
                    "symbol_key": sym,
                    "title": f"{sym} detected in BtcTurk symbol list",
                    "url": "https://www.btcturk.com",
                    "raw_time_text": "UNKNOWN",
                    "normalized_tr_time": "UNKNOWN",
                }
            )
        return out

    def _fetch_paribu_symbol_rows(self):
        payload = self._get_json("https://api.paribu.com/market/ticker")
        out = []
        rows = payload if isinstance(payload, list) else []
        for row in rows:
            market = str((row or {}).get("market") or "").upper()
            if "_" not in market:
                continue
            base, quote = market.split("_", 1)
            symbol = f"{base}{quote}"
            if quote != "TL":
                continue
            out.append(
                {
                    "symbol_key": symbol,
                    "title": f"{symbol} detected in Paribu ticker list",
                    "url": "https://www.paribu.com",
                    "raw_time_text": "UNKNOWN",
                    "normalized_tr_time": "UNKNOWN",
                }
            )
        return out

    def _build_symbol_diff_items(self, exchange: str, rows: list[dict]):
        by_symbol = {}
        for row in rows:
            sym = str(row.get("symbol_key") or "").strip().upper()
            if not sym:
                continue
            by_symbol[sym] = row

        current = set(by_symbol.keys())
        previous = self.symbol_snapshots.get(exchange, set())
        self.symbol_snapshots[exchange] = current

        # First snapshot seeds baseline; no synthetic “new” flood.
        if not previous:
            return []

        new_symbols = sorted(current - previous)
        out = []
        for sym in new_symbols:
            row = by_symbol.get(sym, {})
            out.append(
                {
                    "exchange": exchange,
                    "title": str(row.get("title") or f"{sym} detected in symbol list")[:120],
                    "url": str(row.get("url") or ""),
                    "raw_time_text": str(row.get("raw_time_text") or "UNKNOWN"),
                    "normalized_tr_time": str(row.get("normalized_tr_time") or "UNKNOWN"),
                    "symbol_key": sym,
                    "source_type": "symbol_diff",
                }
            )
        return out

    def _fetch_primary_items(self, exchange: str):
        kind = PRIMARY_SOURCE.get(exchange)
        if kind == "web_fallback":
            rows, errs, degraded = self._fetch_web_fallback_items(exchange)
            if errs:
                raise RuntimeError(" | ".join(errs)[:260])
            if degraded and not rows:
                raise RuntimeError("web primary degraded")
            return rows
        if kind == "api_announcement":
            if exchange == "gate":
                return self._fetch_gate_api_announcements()
            if exchange == "mexc":
                return self._fetch_mexc_api_announcements()
            if exchange == "bitget":
                return self._fetch_bitget_api_announcements()
            if exchange == "kucoin":
                return self._fetch_kucoin_api_announcements()
            if exchange == "binance":
                return self._fetch_binance_api_announcements()
            raise RuntimeError(f"No api_announcement handler for {exchange}")

        if kind == "symbol_diff":
            if exchange == "gate":
                rows = self._fetch_gate_symbol_rows()
            elif exchange == "mexc":
                rows = self._fetch_mexc_symbol_rows()
            elif exchange == "binance":
                rows = self._fetch_binance_symbol_rows()
            elif exchange == "okex":
                rows = self._fetch_okex_symbol_rows()
            elif exchange == "bybit":
                rows = self._fetch_bybit_symbol_rows()
            elif exchange == "btcturk":
                rows = self._fetch_btcturk_symbol_rows()
            elif exchange == "paribu":
                rows = self._fetch_paribu_symbol_rows()
            else:
                raise RuntimeError(f"No symbol_diff handler for {exchange}")
            return self._build_symbol_diff_items(exchange, rows)

        raise RuntimeError(f"Unsupported primary source for {exchange}: {kind}")

    def _fetch_symbol_diff_fallback(self, exchange: str):
        if exchange == "gate":
            rows = self._fetch_gate_symbol_rows()
        elif exchange == "mexc":
            rows = self._fetch_mexc_symbol_rows()
        elif exchange == "binance":
            rows = self._fetch_binance_symbol_rows()
        elif exchange == "okex":
            rows = self._fetch_okex_symbol_rows()
        elif exchange == "bybit":
            rows = self._fetch_bybit_symbol_rows()
        elif exchange == "btcturk":
            rows = self._fetch_btcturk_symbol_rows()
        elif exchange == "paribu":
            rows = self._fetch_paribu_symbol_rows()
        else:
            return []
        return self._build_symbol_diff_items(exchange, rows)

    def _normalize_time_meta(self, exchange: str, item: dict):
        has_prefilled = item.get("raw_time_text") and item.get("normalized_tr_time")
        if has_prefilled:
            raw = self._clip_raw(item.get("raw_time_text", "UNKNOWN"))
            norm = item.get("normalized_tr_time", "UNKNOWN")
            return raw, norm

        source_type = item.get("source_type", "web_fallback")
        if source_type == "symbol_diff":
            return "UNKNOWN", "UNKNOWN"

        if exchange == "binance":
            try:
                code = str(item.get("article_code") or "").strip().lower()
                if not code:
                    code = self._extract_article_code_from_url(item.get("url", ""))
                if code:
                    raw, norm = self._fetch_binance_detail_time(code)
                    if norm != "UNKNOWN":
                        return self._clip_raw(raw), norm
            except Exception:
                pass

        summary_text = str(item.get("summary_text") or "").strip()
        if summary_text:
            raw, norm = extract_time(exchange, summary_text)
            if norm != "UNKNOWN":
                return self._clip_raw(raw), norm

        try:
            dhtml = self._get_text(item["url"])
            raw, norm = extract_time(exchange, dhtml)
            return self._clip_raw(raw), norm
        except Exception:
            return "UNKNOWN", "UNKNOWN"

    def scan_exchange(self, ex: str):
        started = time.perf_counter()
        state = self._new_exchange_state(ex)
        err = ""
        final = []
        meta = {
            "market_count": 0,
            "candidate_new_count": 0,
            "verified_new_count": 0,
            "fetch_duration_ms": 0,
            "error_type": "",
            "error_message": "",
        }
        try:
            primary_items = []
            fallback_items = []
            primary_kind = PRIMARY_SOURCE.get(ex, "web_fallback")

            if primary_kind == "web_fallback":
                web_primary_items, web_primary_errors, web_primary_degraded = self._fetch_web_fallback_items(ex)
                primary_items = web_primary_items
                state["primary_ok"] = bool(primary_items) or not bool(web_primary_errors)
                state["primary_error"] = " | ".join(web_primary_errors)[:260] if web_primary_errors else ""
                if web_primary_degraded:
                    state["degraded"] = True
                    state["degraded_reason"] = state["primary_error"] or "web_primary_degraded"
                if not state["primary_ok"]:
                    err = state["primary_error"] or "web primary failed"
            else:
                try:
                    primary_items = self._fetch_primary_items(ex)
                    state["primary_ok"] = True
                except Exception as e:
                    state["primary_ok"] = False
                    state["primary_error"] = self._short_error(e)
                    state["degraded"] = True
                    state["degraded_reason"] = state["primary_error"]

            if primary_kind == "web_fallback":
                state["fallback_ok"] = None
                state["fallback_error"] = ""
                state["fallback_used"] = False
            else:
                needs_fallback = not state.get("primary_ok")
                if needs_fallback:
                    symbol_fallback = self._fetch_symbol_diff_fallback(ex)
                    if symbol_fallback:
                        fallback_items = symbol_fallback
                        state["fallback_ok"] = True
                        state["fallback_error"] = ""
                        state["fallback_used"] = True
                    else:
                        web_items, web_errors, web_degraded = self._fetch_web_fallback_items(ex)
                        fallback_items = web_items
                        state["fallback_ok"] = not bool(web_errors)
                        state["fallback_error"] = " | ".join(web_errors)[:260] if web_errors else ""
                        state["fallback_used"] = bool(fallback_items)
                        if web_degraded:
                            state["degraded"] = True
                            if not state["degraded_reason"]:
                                state["degraded_reason"] = state["fallback_error"] or "web_fallback_degraded"
                else:
                    state["fallback_ok"] = None
                    state["fallback_error"] = ""
                    state["fallback_used"] = False

            strict_primary = [x for x in primary_items if self._strict_listing_item(ex, x)]
            strict_fallback = [x for x in fallback_items if self._strict_listing_item(ex, x)]
            combined = self._prioritize_dedupe(strict_primary + strict_fallback)
            if strict_primary:
                state["active_source"] = str(state.get("primary_source") or "web_fallback")
            elif strict_fallback:
                state["active_source"] = str((strict_fallback[0] or {}).get("source_type") or "web_fallback")
            elif state.get("primary_ok"):
                state["active_source"] = str(state.get("primary_source") or "web_fallback")
            elif state.get("fallback_used"):
                state["active_source"] = str((fallback_items[0] or {}).get("source_type") or "web_fallback") if fallback_items else "web_fallback"
            else:
                state["active_source"] = str(state.get("primary_source") or "web_fallback")

            if not state["primary_ok"] and not err:
                err = state["primary_error"] or "primary source failed"

            now = time.time()
            for it in combined[:30]:
                url = it["url"]
                source_type = it.get("source_type", "web_fallback")

                if url not in self.first_seen:
                    self.first_seen[url] = now
                    it["is_new"] = True
                    it["first_seen_now"] = True
                    raw, norm = self._normalize_time_meta(ex, it)
                    it["raw_time_text"] = raw
                    it["normalized_tr_time"] = norm
                    self.url_meta[url] = {
                        "raw_time_text": raw,
                        "normalized_tr_time": norm,
                        "source_type": source_type,
                    }
                else:
                    it["is_new"] = (now - self.first_seen[url]) <= NEW_TTL_SECONDS
                    it["first_seen_now"] = False
                    meta_row = self.url_meta.get(url, {})
                    it["raw_time_text"] = self._clip_raw(meta_row.get("raw_time_text", "UNKNOWN"))
                    it["normalized_tr_time"] = meta_row.get("normalized_tr_time", "UNKNOWN")
                    it["source_type"] = meta_row.get("source_type", source_type)

                it["detected_at"] = iso_utc()
                final.append(it)

            meta["market_count"] = len(primary_items) + len(fallback_items)
            meta["candidate_new_count"] = sum(1 for row in final if bool(row.get("first_seen_now")))
            meta["verified_new_count"] = sum(
                1 for row in final if bool(row.get("first_seen_now")) and str(row.get("normalized_tr_time") or "UNKNOWN") != "UNKNOWN"
            )
        except Exception as e:
            err = self._short_error(e)
            state["degraded"] = True
            state["degraded_reason"] = err
            final = self.items.get(ex, [])
            err_l = err.lower()
            if "429" in err_l:
                meta["error_type"] = "rateLimit"
            elif "401" in err_l or "403" in err_l:
                meta["error_type"] = "auth"
            elif "timeout" in err_l:
                meta["error_type"] = "timeout"
            elif "json" in err_l or "parse" in err_l:
                meta["error_type"] = "parse"
            elif "connection" in err_l:
                meta["error_type"] = "network"
            else:
                meta["error_type"] = "unknown"
            meta["error_message"] = err

        if ex in self.symbol_snapshots:
            try:
                self._persist_exchange_snapshot(ex)
            except Exception:
                pass
        try:
            self._persist_seen_urls()
        except Exception:
            pass

        meta["fetch_duration_ms"] = int((time.perf_counter() - started) * 1000)
        self.exchange_state[ex] = state
        self.last_scan_meta[ex] = dict(meta)
        self.items[ex] = final
        return final, err, state, dict(meta)

    def poll_once(self):
        out = {ex: [] for ex in self.exchanges}
        errs = {}
        for ex in self.exchanges:
            final, err, _state, _meta = self.scan_exchange(ex)
            out[ex] = final
            if err:
                errs[ex] = err

        self.items = out
        return out, errs
