import re
from dateutil import parser, tz
from .html_text import html_to_text
from ..util import tz_name

TR_TZ = tz.gettz(tz_name())

TRADE_CONTEXT_PATTERNS = {
  "gate": [
    r"trading\s+will\s+start\s+at[^.]{0,180}",
    r"(?:will\s+be|is)\s+listed\s+at[^.]{0,180}",
    r"start\s+trading\s+at[^.]{0,180}",
  ],
  "binance": [
    r"trading\s+will\s+open\s+at[^.]{0,180}",
    r"will\s+list[^.]{0,120}\s+at[^.]{0,120}",
    r"trading\s+starts?\s+at[^.]{0,180}",
  ],
  "mexc": [
    r"trading\s+will\s+start\s+at[^.]{0,180}",
    r"trading\s+in\s+the\s+[^.]{0,60}\s*:\s*[^.]{0,180}",
    r"(?:will\s+be|gets)\s+listed[^.]{0,120}\s+at[^.]{0,120}",
  ],
  "kucoin": [
    r"trading\s*:\s*[^.]{0,180}",
    r"trading\s+will\s+start\s+at[^.]{0,180}",
    r"trading\s+starts?\s+at[^.]{0,180}",
  ],
  "bitget": [
    r"trading\s+available\s*:?\s*[^.]{0,180}",
    r"trading\s+starts?\s+at[^.]{0,180}",
    r"open\s+for\s+trading[^.]{0,180}",
  ],
}

DATE_PATTERNS = [
  r"[A-Za-z]{3,9}\s+\d{1,2},\s*\d{4},?\s*\d{1,2}:\d{2}(?::\d{2})?\s*(?:\((?:UTC|GMT)\)|UTC|GMT|UTC[+-]\d{1,2})?",
  r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?\s*(?:\((?:UTC|GMT)\)|UTC|GMT|UTC[+-]\d{1,2})?",
  r"\d{1,2}:\d{2}(?::\d{2})?\s+on\s+[A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}\s*(?:\((?:UTC|GMT)\)|UTC|GMT|UTC[+-]\d{1,2})?",
]

def normalize_datetime(raw: str) -> str:
    try:
        default_tz = tz.UTC if re.search(r"\bUTC\b", raw, re.IGNORECASE) else None
        dt = parser.parse(raw, fuzzy=True)
        if dt.tzinfo is None and default_tz is not None:
            dt = dt.replace(tzinfo=default_tz)
        if dt.tzinfo is None:
            return "UNKNOWN"
        dt_tr = dt.astimezone(TR_TZ)
        return dt_tr.replace(microsecond=0).isoformat()
    except Exception:
        return "UNKNOWN"

def _extract_date_from_context(ctx: str) -> str:
    for pat in DATE_PATTERNS:
        m = re.search(pat, ctx, flags=re.IGNORECASE)
        if m:
            return re.sub(r"\s+", " ", m.group(0)).strip()[:180]
    return ""

def extract_time(exchange: str, html: str):
    text = html_to_text(html)
    patterns = TRADE_CONTEXT_PATTERNS.get(exchange, [])
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            ctx = re.sub(r"\s+", " ", m.group(0)).strip()[:220]
            raw = _extract_date_from_context(ctx)
            if not raw:
                continue
            norm = normalize_datetime(raw)
            if norm != "UNKNOWN":
                return raw, norm
    return "UNKNOWN", "UNKNOWN"
