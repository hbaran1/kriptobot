from __future__ import annotations

import re
from typing import Optional


QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BTC", "ETH", "TRY", "TL", "EUR", "BNB")

SYMBOL_MODE_FALLBACK = {
    "gate": "underscore_upper",
    "mexc": "concat_upper",
    "kucoin": "dash_upper",
    "bitget": "concat_upper",
    "binance": "concat_upper",
    "okex": "dash_upper",
    "bybit": "concat_upper",
    "btcturk": "concat_upper",
    "paribu": "underscore_lower",
}

TRADE_URL_TEMPLATES = {
    "gate": "https://www.gate.com/trade/{BASE}_{QUOTE}",
    "mexc": "https://www.mexc.com/exchange/{BASE}_{QUOTE}",
    "kucoin": "https://www.kucoin.com/trade/{BASE}-{QUOTE}",
    "bitget": "https://www.bitget.com/spot/{BASE}{QUOTE}",
    "binance": "https://www.binance.com/en/trade/{BASE}_{QUOTE}?type=spot",
    "okex": "https://www.okx.com/trade-spot/{base}-{quote}",
    "bybit": "https://www.bybit.com/trade/spot/{BASE}/{QUOTE}",
    "btcturk": "https://pro.btcturk.com/pro/trade/{BASE}{QUOTE}",
    "paribu": "https://www.paribu.com/markets/{base}_{quote}",
}


def split_symbol_parts(raw_symbol: str) -> tuple[str, str]:
    raw = re.sub(r"\s+", "", str(raw_symbol or "").upper())
    if not raw:
        return "", ""

    for sep in ("_", "-", "/"):
        if sep in raw:
            left, right = raw.split(sep, 1)
            left = re.sub(r"[^A-Z0-9]", "", left)
            right = re.sub(r"[^A-Z0-9]", "", right)
            if left and right:
                return left, right

    compact = re.sub(r"[^A-Z0-9]", "", raw)
    if not compact:
        return "", ""
    for quote in sorted(QUOTE_SUFFIXES, key=len, reverse=True):
        if compact.endswith(quote) and len(compact) > len(quote):
            return compact[:-len(quote)], quote
    return compact, ""


def canonical_symbol(raw_symbol: str, exchange: str = "") -> str:
    base, quote = split_symbol_parts(raw_symbol)
    ex = str(exchange or "").strip().lower()
    if not base:
        return ""
    if not quote:
        quote = "TL" if ex == "paribu" else ("TRY" if ex == "btcturk" else "USDT")
    if ex == "paribu" and quote == "TRY":
        quote = "TL"
    return f"{base}/{quote}"


def format_order_symbol(exchange: str, canonical: str, market_meta: Optional[dict] = None) -> str:
    ex = str(exchange or "").strip().lower()
    canon = canonical_symbol(canonical, ex)
    if not canon:
        return ""
    base, quote = canon.split("/", 1)

    # Runtime detection: exchange-native market id takes precedence.
    if isinstance(market_meta, dict):
        market_id = str(market_meta.get("id") or "").strip()
        if market_id:
            return market_id

    mode = SYMBOL_MODE_FALLBACK.get(ex, "concat_upper")
    if mode == "slash_upper":
        return f"{base}/{quote}"
    if mode == "dash_upper":
        return f"{base}-{quote}"
    if mode == "underscore_upper":
        return f"{base}_{quote}"
    if mode == "underscore_lower":
        return f"{base.lower()}_{quote.lower()}"
    if mode == "dash_lower":
        return f"{base.lower()}-{quote.lower()}"
    if mode == "slash_lower":
        return f"{base.lower()}/{quote.lower()}"
    if mode == "concat_lower":
        return f"{base.lower()}{quote.lower()}"
    return f"{base}{quote}"


def build_trade_url(exchange: str, canonical: str, trade_url_symbol: str = "") -> str:
    ex = str(exchange or "").strip().lower()
    tpl = TRADE_URL_TEMPLATES.get(ex)
    if not tpl:
        return ""
    canon = canonical_symbol(canonical, ex)
    if not canon:
        return ""
    base, quote = canon.split("/", 1)
    if ex == "paribu" and quote == "TRY":
        quote = "TL"
    return tpl.format(
        BASE=base,
        QUOTE=quote,
        base=base.lower(),
        quote=quote.lower(),
        symbol=(trade_url_symbol or "").strip(),
    )
