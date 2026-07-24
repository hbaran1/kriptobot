#!/usr/bin/env python3
"""Fresh rolling-365-day Binance pump census.

Scan window:
    2025-07-24T00:00:00Z <= 1h candle open < 2026-07-24T00:00:00Z

All inputs are fetched during this run directly from official Binance sources.
Previous cohorts, reports, counts, caches and production databases are never read.
"""
from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import urllib.error
from pathlib import Path
from urllib.parse import quote

import pandas as pd

import p01b
import run_fixed  # noqa: F401  # URL-encodes non-ASCII Binance archive paths

SCAN_START = 1_753_315_200_000  # 2025-07-24T00:00:00Z
SCAN_END = 1_784_851_200_000    # 2026-07-24T00:00:00Z exclusive
API_TAIL_START = 1_782_864_000_000  # 2026-07-01T00:00:00Z
MONTHS = [
    "2025-06", "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12",
    "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06",
]
DATA_API = "https://data-api.binance.vision/api/v3"

p01b.SCAN_START = SCAN_START
p01b.SCAN_END = SCAN_END
p01b.MONTHS = MONTHS

_original_build_universe = p01b.build_universe


def api_json(url: str) -> tuple[object, bytes]:
    raw = p01b.get(url, timeout=120)
    return json.loads(raw.decode("utf-8", "replace")), raw


def build_universe() -> tuple[list[str], dict]:
    archive_symbols, evidence = _original_build_universe()
    active: list[str] = []
    api_record = {
        "url": f"{DATA_API}/exchangeInfo",
        "source_type": "REAL_OBSERVED_BINANCE_API",
    }
    try:
        data, raw = api_json(api_record["url"])
        api_record.update(status="OK_API", bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        for item in data.get("symbols", []):
            symbol = item.get("symbol", "")
            base = item.get("baseAsset", symbol[:-4])
            if item.get("quoteAsset") != "USDT" or item.get("status") != "TRADING":
                continue
            if not item.get("isSpotTradingAllowed", True):
                continue
            if p01b.LEVERAGED.search(symbol) or base in p01b.STABLE_FIAT:
                continue
            active.append(symbol)
    except Exception as exc:
        api_record.update(status="ERROR", error=f"{type(exc).__name__}:{exc}")

    merged = sorted(set(archive_symbols) | set(active))
    evidence.update({
        "archive_kept_count": len(archive_symbols),
        "active_api_kept_count": len(set(active)),
        "kept_count": len(merged),
        "kept": merged,
        "active_exchange_info": api_record,
        "universe_rule": "union of historical monthly archive and active Binance spot-USDT symbols",
        "direct_binance_only": True,
    })
    (p01b.OUT / "P02_UNIVERSE.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
    return merged, evidence


def api_tail(symbol: str) -> tuple[pd.DataFrame | None, dict]:
    url = (
        f"{DATA_API}/klines?symbol={quote(symbol, safe='')}&interval=1h"
        f"&startTime={API_TAIL_START}&endTime={SCAN_END - 1}&limit=1000"
    )
    record = {
        "symbol": symbol,
        "month": "2026-07-01..2026-07-23_API",
        "url": url,
        "source_type": "REAL_OBSERVED_BINANCE_API",
    }
    try:
        data, raw = api_json(url)
        if isinstance(data, dict):
            record.update(status="SOURCE_UNAVAILABLE", api_code=data.get("code"), api_message=data.get("msg"))
            return None, record
        if not isinstance(data, list):
            raise ValueError("unexpected_api_shape")
        if not data:
            record.update(status="SOURCE_UNAVAILABLE", reason="empty_api_result")
            return None, record

        frame = pd.DataFrame(data).iloc[:, [0, 1, 2, 3, 4, 7]].copy()
        frame.columns = ["open_time", "open", "high", "low", "close", "quote_volume"]
        for column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna()
        frame["open_time"] = frame["open_time"].astype("int64")
        frame = frame[(frame["open_time"] >= API_TAIL_START) & (frame["open_time"] < SCAN_END)]
        record.update(
            status="OK_API",
            bytes=len(raw),
            sha256=hashlib.sha256(raw).hexdigest(),
            rows=int(len(frame)),
            first_open_time=int(frame["open_time"].min()) if not frame.empty else None,
            last_open_time=int(frame["open_time"].max()) if not frame.empty else None,
        )
        return frame, record
    except urllib.error.HTTPError as exc:
        record.update(status="SOURCE_UNAVAILABLE" if exc.code in (400, 404) else "HTTP_ERROR", http_code=exc.code)
    except Exception as exc:
        record.update(status="ERROR", error=f"{type(exc).__name__}:{exc}")
    return None, record


def process_symbol(symbol: str) -> tuple[str, dict, list[dict]]:
    frames: list[pd.DataFrame] = []
    records: list[dict] = []

    with cf.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(p01b.verified_download, symbol, month) for month in MONTHS]
        for future in cf.as_completed(futures):
            payload, record = future.result()
            records.append(record)
            if payload is not None:
                try:
                    frames.append(p01b.read_zip(payload, f"{symbol}:{record['month']}"))
                except Exception as exc:
                    record.update(status="PARSE_ERROR", error=f"{type(exc).__name__}:{exc}")

    tail, tail_record = api_tail(symbol)
    records.append(tail_record)
    if tail is not None and not tail.empty:
        frames.append(tail)

    quality = {
        "downloads": records,
        "bars": 0,
        "segments": 0,
        "gaps": 0,
        "raw_candidates": 0,
        "eligible": 0,
        "skipped": {},
    }
    events: list[dict] = []
    if frames:
        segments = p01b.contiguous_segments(pd.concat(frames, ignore_index=True))
        quality["segments"] = len(segments)
        quality["gaps"] = max(0, len(segments) - 1)
        skipped: dict[str, int] = {}
        for segment in segments:
            segment_events, stats = p01b.scan_segment(symbol, segment)
            events.extend(segment_events)
            quality["bars"] += stats["bars"]
            quality["raw_candidates"] += stats["raw_candidates"]
            quality["eligible"] += stats["eligible"]
            for key, value in stats.get("skipped", {}).items():
                skipped[key] = skipped.get(key, 0) + value
        quality["skipped"] = skipped
    return symbol, quality, events


p01b.build_universe = build_universe
p01b.process_symbol = process_symbol


def postprocess() -> None:
    root = Path(__file__).resolve().parent / "artifacts"

    result_path = root / "P01_STRICT_RESULT.json"
    result = json.loads(result_path.read_text())
    result.update({
        "study": "pump500_strict_fresh_rolling_365d_20260724",
        "fresh_data_only": True,
        "prior_results_used": False,
        "period": {
            "scan_start_utc": "2025-07-24T00:00:00Z",
            "scan_end_utc_exclusive": "2026-07-24T00:00:00Z",
            "last_included_utc_day": "2026-07-23",
            "warmup_archive": "2025-06-01..2025-07-23",
            "duration_days": 365,
        },
        "data_sources": {
            "2025-06_through_2026-06": "official Binance monthly native-1h ZIP archives; official SHA-256 CHECKSUM required",
            "2026-07-01_through_2026-07-23": "official Binance public market-data API; raw response SHA-256 recorded",
        },
        "direct_binance_only": True,
    })
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))

    quality_path = root / "P01_DATA_QUALITY.json"
    quality = json.loads(quality_path.read_text())
    failures = []
    counts: dict[str, int] = {}
    for symbol_quality in quality.get("per_symbol", {}).values():
        for record in symbol_quality.get("downloads", []):
            status = record.get("status", "UNKNOWN")
            counts[status] = counts.get(status, 0) + 1
            if status not in ("OK", "OK_API", "SOURCE_UNAVAILABLE"):
                failures.append(record)
    quality.update({
        "file_status_counts": counts,
        "hard_failures": failures,
        "source_unavailable": [
            record
            for symbol_quality in quality.get("per_symbol", {}).values()
            for record in symbol_quality.get("downloads", [])
            if record.get("status") == "SOURCE_UNAVAILABLE"
        ],
        "direct_binance_only": True,
        "archive_checksum_policy": "official Binance .CHECKSUM SHA-256 required",
        "api_integrity_policy": "raw Binance response SHA-256 recorded",
    })
    quality_path.write_text(json.dumps(quality, ensure_ascii=False, indent=2))

    table_path = root / "P01_STRICT_TABLE.md"
    old = table_path.read_text().splitlines()
    body = "\n".join(old[2:]) if old and old[0].startswith("# ") else "\n".join(old)
    table_path.write_text(
        "# P01 — Son 365 Gün / 1 Saatlik Mumda Fitil Dahil +%20\n\n"
        "**Dönem:** 2025-07-24 00:00 UTC–2026-07-24 00:00 UTC "
        "(son dâhil gün: 2026-07-23)\n\n" + body
    )


if __name__ == "__main__":
    p01b.main()
    postprocess()
