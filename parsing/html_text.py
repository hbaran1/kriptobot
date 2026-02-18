import re

from bs4 import BeautifulSoup


_WS_RE = re.compile(r"\s+")


def html_to_text(html: str) -> str:
    """Convert HTML into normalized plain text."""
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return _WS_RE.sub(" ", text).strip()
