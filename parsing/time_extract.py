from __future__ import annotations

import re
from datetime import datetime, timezone

from dateutil import parser, tz


UNKNOWN = "UNKNOWN"
_TR_TZ = tz.gettz("Europe/Istanbul")


EXCHANGE_PATTERNS = {
    "gate": [
        r"(?:trading\s+(?:for\s+\w+\s+)?(?:starts?|begins?|opens?\s+at)\s*)(?P<dt>[^.;\n]{6,90})",
        r"(?:will\s+list(?:ed)?\s*(?:on|at)?\s*)(?P<dt>[^.;\n]{6,90})",
    ],
    "mexc": [
        r"(?:trading\s+will\s+start\s*(?:at)?\s*)(?P<dt>[^.;\n]{6,90})",
        r"(?:initial\s+listing\s+time\s*[:\-]?\s*)(?P<dt>[^.;\n]{6,90})",
    ],
    "kucoin": [
        r"(?:trading\s*[:\-]?\s*)(?P<dt>[^.;\n]{6,90})",
        r"(?:opens?\s+for\s+trading\s*(?:at)?\s*)(?P<dt>[^.;\n]{6,90})",
    ],
    "bitget": [
        r"(?:trading\s+starts?\s*(?:at)?\s*)(?P<dt>[^.;\n]{6,90})",
        r"(?:spot\s+trading\s+opens?\s*(?:at)?\s*)(?P<dt>[^.;\n]{6,90})",
    ],
    "binance": [
        r"(?:trading\s+will\s+open\s*(?:at)?\s*)(?P<dt>[^.;\n]{6,90})",
        r"(?:trading\s+starts?\s*(?:at)?\s*)(?P<dt>[^.;\n]{6,90})",
    ],
}


GENERIC_PATTERNS = [
    r"(?:trading\s+(?:starts?|begins?|opens?|will\s+open)\s*(?:at|on)?\s*)(?P<dt>[^.;\n]{6,90})",
    r"(?:listing\s+time\s*[:\-]?\s*)(?P<dt>[^.;\n]{6,90})",
    r"(?:will\s+be\s+listed\s*(?:at|on)?\s*)(?P<dt>[^.;\n]{6,90})",
    r"(?P<dt>(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4},?\s*\d{1,2}:\d{2}(?::\d{2})?\s*(?:UTC|GMT|UTC[+-]\d{1,2})?)",
    r"(?P<dt>\d{4}[-/]\d{1,2}[-/]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:UTC|GMT|UTC[+-]\d{1,2})?)",
]


def extract_time_info(exchange: str, text: str) -> tuple[str, str]:
    raw = _extract_raw_time(exchange, text)
    if raw == UNKNOWN:
        return UNKNOWN, UNKNOWN
    normalized = _normalize_to_tr(raw)
    return raw, normalized


def _extract_raw_time(exchange: str, text: str) -> str:
    if not text:
        return UNKNOWN

    patterns = EXCHANGE_PATTERNS.get(exchange, []) + GENERIC_PATTERNS
    for pat in patterns:
        match = re.search(pat, text, flags=re.IGNORECASE)
        if not match:
            continue
        value = match.group("dt").strip(" -:\n\t")
        value = re.sub(r"\s+", " ", value)
        if len(value) > 120:
            value = value[:120].rstrip()
        if value:
            return value

    return UNKNOWN


def _normalize_to_tr(raw: str) -> str:
    if raw == UNKNOWN:
        return UNKNOWN

    has_utc = bool(re.search(r"\b(?:UTC|GMT)(?:[+-]\d{1,2})?\b", raw, flags=re.IGNORECASE))

    tzinfos = {
        "UTC": tz.UTC,
        "GMT": tz.UTC,
    }

    try:
        dt = parser.parse(raw, fuzzy=True, tzinfos=tzinfos)
    except (ValueError, TypeError, OverflowError):
        return UNKNOWN

    if dt.tzinfo is None:
        if has_utc:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)

    try:
        dt_tr = dt.astimezone(_TR_TZ)
    except Exception:
        return UNKNOWN

    return dt_tr.isoformat(timespec="seconds")
