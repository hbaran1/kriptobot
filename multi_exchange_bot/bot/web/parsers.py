import re
import html as html_lib
from bs4 import BeautifulSoup

def parse_listings(exchange: str, source_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(" ") or "").strip()
        u = normalize_url(exchange, href)
        if not u:
            continue
        title = clean_title(text) or title_from_url(u)
        if not title:
            continue
        it = {"exchange": exchange, "title": title[:120], "url": u}
        if likely_listing(exchange, it["url"], it["title"]):
            items.append(it)
    return items[:80]

def clean_title(t: str) -> str:
    t = html_lib.unescape(t or "")
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip(" -|")
    return t if len(t) >= 8 else ""

def title_from_url(u: str) -> str:
    slug = u.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ")
    return slug[:120]

def normalize_url(exchange: str, href: str):
    if href.startswith("javascript:") or href.startswith("#"):
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if not href.startswith("/"):
        return None
    if exchange == "gate": return "https://www.gate.com" + href
    if exchange == "mexc": return "https://www.mexc.com" + href
    if exchange == "kucoin": return "https://www.kucoin.com" + href
    if exchange == "bitget": return "https://www.bitget.com" + href
    if exchange == "binance": return "https://www.binance.com" + href
    return None

def likely_listing(exchange: str, url: str, title: str) -> bool:
    u = url.lower()
    t = title.lower()

    positive = ("list", "listing", "will list", "gets listed", "world premiere", "trading")
    negative = (
        "latest events",
        "mx exclusives",
        "reward",
        "airdrop",
        "kickstarter",
        "launchpool",
        "launchpad",
        "vip",
        "digest",
        "maintenance",
        "api updates",
        "product updates",
        "help",
        "faq",
        "futures",
        "delist",
        "copy trade",
        "grid trading",
        "fiat",
        "p2p",
        "news",
        "history",
        "web3",
    )

    if any(x in t for x in negative):
        return False

    has_pos = any(x in t for x in positive) or any(x in u for x in ("new-listing", "new-listings", "will-list"))

    if exchange == "gate":
        return "/announcements/article/" in u and has_pos
    if exchange == "mexc":
        return ("/support/articles/" in u or "/announcements/article/" in u) and has_pos
    if exchange == "kucoin":
        return "/announcement/en-" in u and has_pos
    if exchange == "bitget":
        return "/support/articles/" in u and has_pos
    if exchange == "binance":
        return ("/support/announcement/detail/" in u or "/en/support/announcement/" in u) and has_pos
    return True
