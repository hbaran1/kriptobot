from .base import BaseExchange


class MexcExchange(BaseExchange):
    name = "mexc"
    list_urls = [
        "https://www.mexc.com/newlisting",
        "https://www.mexc.com/announcements/new-listings",
    ]
    list_selectors = [
        "a[href*='/support/articles/']",
        "a[href*='/announcements']",
        "a[href*='/newlisting']",
        "article a",
    ]

    def _is_listing_candidate(self, title: str, url: str) -> bool:
        hay = f"{title} {url}".lower()
        mexc_tokens = (
            "new listing",
            "listing",
            "will list",
            "trading",
            "newlisting",
        )
        return any(tok in hay for tok in mexc_tokens)
