from .base import BaseExchange


class GateExchange(BaseExchange):
    name = "gate"
    list_urls = ["https://www.gate.com/announcements/newspotlistings"]
    list_selectors = [
        "a[href*='/announcements/article/']",
        "a[href*='/announcements']",
        "article a",
    ]

    def _is_listing_candidate(self, title: str, url: str) -> bool:
        hay = f"{title} {url}".lower()
        gate_tokens = (
            "listing",
            "newspot",
            "will list",
            "startup",
            "spot",
        )
        return any(tok in hay for tok in gate_tokens)
