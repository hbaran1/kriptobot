from .base import BaseExchange


class KucoinExchange(BaseExchange):
    name = "kucoin"
    list_urls = ["https://www.kucoin.com/announcement/new-listings"]
    list_selectors = [
        "a[href*='/news/']",
        "a[href*='/announcement/']",
        "article a",
    ]

    def _is_listing_candidate(self, title: str, url: str) -> bool:
        hay = f"{title} {url}".lower()
        kucoin_tokens = (
            "listing",
            "new listing",
            "world premiere",
            "trading",
            "news",
        )
        return any(tok in hay for tok in kucoin_tokens)
