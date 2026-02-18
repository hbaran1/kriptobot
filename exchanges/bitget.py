from .base import BaseExchange


class BitgetExchange(BaseExchange):
    name = "bitget"
    list_urls = ["https://www.bitget.com/support/sections/5955813039257"]
    list_selectors = [
        "a[href*='/support/articles/']",
        "a[href*='/support/']",
        "article a",
    ]

    def _is_listing_candidate(self, title: str, url: str) -> bool:
        hay = f"{title} {url}".lower()
        bitget_tokens = (
            "new listing",
            "spot listing",
            "will list",
            "listing",
            "trading",
        )
        return any(tok in hay for tok in bitget_tokens)
