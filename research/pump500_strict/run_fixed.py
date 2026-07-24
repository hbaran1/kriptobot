#!/usr/bin/env python3
"""Run P01 with URL-safe access for non-ASCII Binance symbols."""
from urllib.parse import quote, urlsplit, urlunsplit

import p01b

_original_get = p01b.get


def encoded_get(url: str, timeout: int = 90) -> bytes:
    parts = urlsplit(url)
    encoded_path = quote(parts.path, safe="/%")
    return _original_get(urlunsplit((parts.scheme, parts.netloc, encoded_path, parts.query, parts.fragment)), timeout)


p01b.get = encoded_get

if __name__ == "__main__":
    p01b.main()
