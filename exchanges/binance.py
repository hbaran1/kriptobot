from .base import BaseExchange


class BinanceExchange(BaseExchange):
    name = "binance"
    list_urls = [
        "https://www.binance.com/en-TR/support/announcement",
        "https://www.binance.com/en-TR/events/new-listing-promos",
    ]
    list_selectors = [
        "a[href*='/support/announcement/']",
        "a[href*='/events/']",
        "article a",
    ]

    def _is_listing_candidate(self, title: str, url: str) -> bool:
        hay = f"{title} {url}".lower()
        binance_tokens = (
            "new listing",
            "will list",
            "listing",
            "trading",
            "announcement",
            "promo",
        )
        return any(tok in hay for tok in binance_tokens)
