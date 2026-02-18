import json
import time, datetime, requests
from collections import defaultdict
from bs4 import BeautifulSoup
from dateutil import tz
from .util import iso_utc
from .parsing.time_extract import extract_time
from .web.sources import SOURCES
from .web.parsers import parse_listings, likely_listing

NEW_TTL_SECONDS = 24 * 3600
TR_TZ = tz.gettz("Europe/Istanbul")

class ListingsAggregator:
    def __init__(self):
        self.exchanges = ["gate","mexc","kucoin","bitget","binance"]
        self.first_seen = {}  # url -> unix_ts
        self.url_meta = {}  # url -> {"raw_time_text": "...", "normalized_tr_time": "..."}
        self.items = defaultdict(list)
        self.s = requests.Session()
        self.s.headers.update({"User-Agent":"Mozilla/5.0"})

    def _get_text(self, url: str) -> str:
        r = self.s.get(url, timeout=15)
        # 202/403/5xx should be treated as fetch errors for tab ERR badge.
        if r.status_code >= 400 or r.status_code == 202:
            raise requests.HTTPError(f"HTTP {r.status_code} for {url}")
        return r.text

    def _clip_raw(self, raw: str) -> str:
        if not raw:
            return "UNKNOWN"
        raw = " ".join(raw.split())
        return raw[:180] if raw else "UNKNOWN"

    def _fetch_binance_info_items(self):
        html = self._get_text("https://www.binance.info/en/support/announcement")
        soup = BeautifulSoup(html, "html.parser")
        app_data = soup.find("script", {"id": "__APP_DATA"})
        if not app_data or not app_data.string:
            return []

        obj = json.loads(app_data.string)
        candidates = []
        stack = [obj]
        while stack:
            x = stack.pop()
            if isinstance(x, dict):
                title = x.get("title")
                code = x.get("code")
                if isinstance(title, str) and isinstance(code, str):
                    url = f"https://www.binance.info/en/support/announcement/detail/{code}"
                    if likely_listing("binance", url, title):
                        candidates.append({"exchange": "binance", "title": title[:120], "url": url})
                stack.extend(x.values())
            elif isinstance(x, list):
                stack.extend(x)

        seen = set()
        out = []
        for it in candidates:
            if it["url"] in seen:
                continue
            seen.add(it["url"])
            out.append(it)
        return out[:50]

    def _fetch_bitget_api_items(self):
        data = self.s.get(
            "https://api.bitget.com/api/v2/public/annoucements",
            params={"language": "en_US"},
            timeout=15,
        ).json().get("data", [])
        out = []
        for row in data:
            title = (row.get("annTitle") or "").strip()
            ann_id = (row.get("annId") or "").strip()
            if not title or not ann_id:
                continue
            url = f"https://www.bitget.com/support/articles/{ann_id}"
            if likely_listing("bitget", url, title):
                out.append({"exchange": "bitget", "title": title[:120], "url": url})
        return out[:50]

    def _fetch_gate_api_items(self):
        now_ts = int(time.time())
        max_age_seconds = 14 * 24 * 3600
        r = self.s.get("https://api.gateio.ws/api/v4/spot/currency_pairs", timeout=15)
        r.raise_for_status()
        rows = r.json()
        out = []
        for row in rows:
            pair = (row.get("id") or "").strip().upper()
            if not pair:
                continue
            quote = (row.get("quote") or "").strip().upper()
            if quote != "USDT":
                continue
            buy_start = row.get("buy_start")
            try:
                buy_start = int(buy_start)
            except Exception:
                continue
            if buy_start <= 0:
                continue
            if (now_ts - buy_start) > max_age_seconds:
                continue

            dt_utc = datetime.datetime.fromtimestamp(buy_start, tz=datetime.timezone.utc)
            dt_tr = dt_utc.astimezone(TR_TZ) if TR_TZ else dt_utc
            trade_url = (row.get("trade_url") or "").strip()
            url = trade_url if trade_url.startswith("http") else f"https://www.gate.com/trade/{pair}"

            out.append({
                "exchange": "gate",
                "title": f"{pair} Spot Listing (API fallback)",
                "url": url,
                "raw_time_text": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "normalized_tr_time": dt_tr.replace(microsecond=0).isoformat(),
            })

        out.sort(key=lambda x: x["normalized_tr_time"], reverse=True)
        return out[:50]

    def poll_once(self):
        out = {ex: [] for ex in self.exchanges}
        errs = {}
        for ex in self.exchanges:
            try:
                batch = []
                src_errors = []
                if ex == "binance":
                    try:
                        batch.extend(self._fetch_binance_info_items())
                    except Exception as src_e:
                        src_errors.append(f"{type(src_e).__name__}: {src_e}")
                else:
                    for u in SOURCES[ex]["list_urls"]:
                        try:
                            html = self._get_text(u)
                            batch.extend(parse_listings(ex, u, html))
                        except Exception as src_e:
                            src_errors.append(f"{type(src_e).__name__}: {src_e}")

                    if ex == "bitget" and not batch:
                        try:
                            batch.extend(self._fetch_bitget_api_items())
                            src_errors = []
                        except Exception as src_e:
                            src_errors.append(f"{type(src_e).__name__}: {src_e}")
                    if ex == "gate" and not batch:
                        try:
                            batch.extend(self._fetch_gate_api_items())
                            src_errors = []
                        except Exception as src_e:
                            src_errors.append(f"{type(src_e).__name__}: {src_e}")

                seen = set()
                uniq = []
                for it in batch:
                    if it["url"] in seen:
                        continue
                    seen.add(it["url"])
                    uniq.append(it)

                now = time.time()
                final = []
                for it in uniq[:30]:
                    url = it["url"]
                    if url not in self.first_seen:
                        self.first_seen[url] = now
                        it["is_new"] = True
                        has_prefilled = it.get("raw_time_text") and it.get("normalized_tr_time")
                        if has_prefilled:
                            raw = self._clip_raw(it.get("raw_time_text", "UNKNOWN"))
                            norm = it.get("normalized_tr_time", "UNKNOWN")
                            it["raw_time_text"] = raw
                            it["normalized_tr_time"] = norm
                            self.url_meta[url] = {"raw_time_text": raw, "normalized_tr_time": norm}
                        else:
                            try:
                                dhtml = self._get_text(url)
                                raw, norm = extract_time(ex, dhtml)
                                raw = self._clip_raw(raw)
                                it["raw_time_text"] = raw
                                it["normalized_tr_time"] = norm
                                self.url_meta[url] = {"raw_time_text": raw, "normalized_tr_time": norm}
                            except Exception:
                                it["raw_time_text"] = "UNKNOWN"
                                it["normalized_tr_time"] = "UNKNOWN"
                                self.url_meta[url] = {"raw_time_text": "UNKNOWN", "normalized_tr_time": "UNKNOWN"}
                    else:
                        it["is_new"] = (now - self.first_seen[url]) <= NEW_TTL_SECONDS
                        meta = self.url_meta.get(url, {})
                        it["raw_time_text"] = self._clip_raw(meta.get("raw_time_text", "UNKNOWN"))
                        it["normalized_tr_time"] = meta.get("normalized_tr_time", "UNKNOWN")

                    it["detected_at"] = iso_utc()
                    final.append(it)

                out[ex] = final
                if src_errors and not final:
                    errs[ex] = " | ".join(src_errors)[:240]
            except Exception as e:
                errs[ex] = f"{type(e).__name__}: {e}"
                out[ex] = self.items.get(ex, [])
        self.items = out
        return out, errs
