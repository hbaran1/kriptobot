import re
import html as html_lib
from bs4 import BeautifulSoup
from urllib.parse import urljoin


NEGATIVE_KEYWORDS = (
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
    "perpetual",
    "delist",
    "copy trade",
    "grid trading",
    "fiat",
    "p2p",
    "news",
    "campaign",
    "promotion",
    "promo",
    "remove",
    "removal",
    "collateral",
    "margin",
    "account",
    "history",
    "web3",
    "settlement",
    "staking",
    "earn",
)

POSITIVE_KEYWORDS = (
    "new listing",
    "initial listing",
    "first in market",
    "now live on",
    "coming soon",
    "listing arrangement",
    "will list",
    "will be listed",
    "gets listed",
    "spot listing",
    "spot market listing",
    "open for trading",
    "trading opens",
    "trading will open",
    "trading will start",
)


def _is_announcement_detail_url(exchange: str, url: str) -> bool:
    u = (url or "").lower()
    if exchange == "gate":
        return "/announcements/article/" in u
    if exchange == "mexc":
        return "/support/articles/" in u or "/announcements/article/" in u
    if exchange == "kucoin":
        return "/announcement/en-" in u
    if exchange == "bitget":
        return "/support/articles/" in u
    if exchange == "binance":
        return "/support/announcement/detail/" in u or "/en/support/announcement/" in u
    if exchange == "okex":
        return "/help/" in u
    if exchange == "bybit":
        return "announcements.bybit.com" in u or "/help-center/article/" in u
    if exchange == "btcturk":
        return "/duyurular/" in u or "/duyuru/" in u
    if exchange == "paribu":
        return "/blog/" in u
    return False


def _looks_pair_hint(text: str) -> bool:
    t = (text or "").upper()
    if re.search(r"\b[A-Z0-9]{2,15}\s*/\s*(USDT|USDC|USD|BTC|ETH|TRY|EUR|BNB)\b", t):
        return True
    if re.search(r"\([A-Z0-9]{2,15}\)", t):
        return True
    return False


def parse_listings(exchange: str, source_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(" ") or "").strip()
        u = normalize_url(exchange, href, source_url)
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        if not _is_announcement_detail_url(exchange, u):
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

def normalize_url(exchange: str, href: str, source_url: str = ""):
    if href.startswith("javascript:") or href.startswith("#"):
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if source_url:
        try:
            return urljoin(source_url, href)
        except Exception:
            pass
    if not href.startswith("/"):
        return None
    if exchange == "gate": return "https://www.gate.com" + href
    if exchange == "mexc": return "https://www.mexc.com" + href
    if exchange == "kucoin": return "https://www.kucoin.com" + href
    if exchange == "bitget": return "https://www.bitget.com" + href
    if exchange == "binance": return "https://www.binance.com" + href
    if exchange == "okex": return "https://www.okx.com" + href
    if exchange == "bybit": return "https://announcements.bybit.com" + href
    if exchange == "btcturk": return "https://www.btcturk.com" + href
    if exchange == "paribu": return "https://www.paribu.com" + href
    return None

def likely_listing(exchange: str, url: str, title: str) -> bool:
    u = url.lower()
    t = title.lower()
    if not _is_announcement_detail_url(exchange, u):
        return False
    if any(x in t for x in NEGATIVE_KEYWORDS):
        return False
    if "list" in t and "delist" in t:
        return False

    has_pos = any(x in t for x in POSITIVE_KEYWORDS)
    if exchange in ("okex", "bybit", "btcturk", "paribu"):
        has_pos = has_pos or ("list" in t) or ("listing" in t) or ("trading" in t and "open" in t)
    if not has_pos and _looks_pair_hint(title):
        has_pos = ("list" in t) or ("trading" in t and "open" in t)

    return bool(has_pos)
