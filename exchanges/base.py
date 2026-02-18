from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class BaseExchange:
    name = "base"
    list_urls: list[str] = []
    list_selectors: list[str] = ["a"]
    max_items = 30

    _DEFAULT_KEYWORDS = (
        "listing",
        "list",
        "new",
        "launch",
        "trading",
        "spot",
    )

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9,tr;q=0.8",
            }
        )

    def fetch_listings(self) -> list[dict]:
        all_links: list[tuple[str, str]] = []
        errors: list[str] = []

        for url in self.list_urls:
            try:
                html = self._get(url)
                links = self.parse_list_page(html, url)
                if not links:
                    links = self._extract_links_regex(html, url)
                all_links.extend(links)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{url}: {exc}")

        items = self._to_items(all_links)
        if items:
            return items

        if errors:
            raise RuntimeError(" | ".join(errors)[:300])

        raise RuntimeError("No listing candidates found")

    def fetch_detail_text(self, url: str) -> str:
        return self._get(url)

    def parse_list_page(self, html: str, base_url: str) -> list[tuple[str, str]]:
        soup = BeautifulSoup(html or "", "html.parser")
        links: list[tuple[str, str]] = []
        for selector in self.list_selectors:
            for anchor in soup.select(selector):
                parsed = self._link_from_anchor(anchor, base_url)
                if parsed:
                    links.append(parsed)
        return links

    def _get(self, url: str) -> str:
        response = self.session.get(url, timeout=25)
        response.raise_for_status()
        return response.text

    def _link_from_anchor(self, anchor, base_url: str) -> tuple[str, str] | None:
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith("javascript:"):
            return None

        title = (anchor.get_text(" ", strip=True) or "").strip()
        title = re.sub(r"\s+", " ", title)
        if len(title) < 4:
            return None

        full_url = urljoin(base_url, href)
        if not self._is_listing_candidate(title, full_url):
            return None

        return title, full_url

    def _extract_links_regex(self, html: str, base_url: str) -> list[tuple[str, str]]:
        links: list[tuple[str, str]] = []
        for match in re.finditer(
            r"<a[^>]+href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<title>.*?)</a>",
            html or "",
            flags=re.IGNORECASE | re.DOTALL,
        ):
            href = match.group("href").strip()
            if not href or href.startswith("javascript:"):
                continue

            title_html = match.group("title")
            title = BeautifulSoup(title_html, "html.parser").get_text(" ", strip=True)
            title = re.sub(r"\s+", " ", title)
            if len(title) < 4:
                continue

            full_url = urljoin(base_url, href)
            if self._is_listing_candidate(title, full_url):
                links.append((title, full_url))

        return links

    def _is_listing_candidate(self, title: str, url: str) -> bool:
        hay = f"{title} {url}".lower()
        return any(keyword in hay for keyword in self._DEFAULT_KEYWORDS)

    def _to_items(self, links: Iterable[tuple[str, str]]) -> list[dict]:
        dedup: dict[str, str] = {}
        for title, url in links:
            if url not in dedup:
                dedup[url] = title
            if len(dedup) >= self.max_items:
                break

        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return [
            {
                "exchange": self.name,
                "title": title,
                "url": url,
                "detected_at": now_iso,
                "raw_time_text": "UNKNOWN",
                "normalized_tr_time": "UNKNOWN",
                "is_new": False,
                "error": None,
            }
            for url, title in dedup.items()
        ]
