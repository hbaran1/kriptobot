#!/usr/bin/env python3
"""P01 strict census: official Binance 1h candles, wick-inclusive +20% events.

Only fresh official archive bytes fetched in this run are used. No prior cohort,
count, cache, report, production database, or live service is read.
"""
from __future__ import annotations

import concurrent.futures as cf
import hashlib
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "artifacts"
OUT.mkdir(parents=True, exist_ok=True)

S3_LIST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
ARCHIVE = "https://data.binance.vision/data/spot"
MONTHS = ["2025-12", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"]
SCAN_START = 1767225600000  # 2026-01-01T00:00:00Z
SCAN_END = 1782864000000    # 2026-07-01T00:00:00Z exclusive
BAR_MS = 3_600_000
EVENT_THRESHOLD = 0.20
SEVERITY_THRESHOLDS = (0.20, 0.25, 0.30, 0.40, 0.50)
BRAKE_MULT = 3.0
LIQ_MIN = 100_000.0
PRIOR_EVENT_BARS = 6
DAY_BARS = 24
BLOCK12_BARS = 12
BASELINE_BLOCKS = 60

LEVERAGED = re.compile(r"(?:UP|DOWN|BULL|BEAR|3L|3S)USDT$")
STABLE_FIAT = {
    "USDC", "TUSD", "BUSD", "DAI", "FDUSD", "USDP", "SUSD", "UST", "USTC", "VAI",
    "USDE", "USDS", "USDSB", "USDSOLD", "RLUSD", "PYUSD", "USD1", "XUSD", "BFUSD",
    "AEUR", "EURI", "EUR", "GBP", "AUD", "BRL", "TRY", "RUB", "UAH", "NGN", "ZAR",
    "BIDR", "IDRT", "BKRW",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def get(url: str, timeout: int = 90) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "pump500-strict-1h20/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read()


def build_universe() -> tuple[list[str], dict]:
    symbols: list[str] = []
    marker = ""
    pages = 0
    while True:
        from urllib.parse import quote
        url = f"{S3_LIST}?delimiter=/&prefix=data/spot/monthly/klines/"
        if marker:
            url += "&marker=" + quote(marker, safe="")
        text = get(url).decode("utf-8", "replace")
        pages += 1
        prefixes = re.findall(r"<Prefix>data/spot/monthly/klines/([^<]+)/</Prefix>", text)
        symbols.extend(prefixes)
        if "<IsTruncated>true</IsTruncated>" not in text:
            break
        if not prefixes:
            raise RuntimeError("S3 listing truncated without continuation prefix")
        marker = f"data/spot/monthly/klines/{prefixes[-1]}/"

    usdt = sorted(set(symbol for symbol in symbols if symbol.endswith("USDT")))
    kept: list[str] = []
    excluded: dict[str, str] = {}
    for symbol in usdt:
        base = symbol[:-4]
        if LEVERAGED.search(symbol):
            excluded[symbol] = "leveraged_token"
        elif base in STABLE_FIAT:
            excluded[symbol] = "stable_or_fiat_base"
        else:
            kept.append(symbol)

    evidence = {
        "generated_utc": utc_now(),
        "source": S3_LIST,
        "listing_pages": pages,
        "all_archive_symbols": len(set(symbols)),
        "usdt_symbols": len(usdt),
        "kept_count": len(kept),
        "excluded_count": len(excluded),
        "kept": kept,
        "excluded": excluded,
        "source_type": "REAL_OBSERVED",
    }
    (OUT / "P02_UNIVERSE.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
    return kept, evidence


def archive_urls(symbol: str, month: str) -> tuple[str, str]:
    name = f"{symbol}-1h-{month}.zip"
    base = f"{ARCHIVE}/monthly/klines/{symbol}/1h/{name}"
    return base, base + ".CHECKSUM"


def verified_download(symbol: str, month: str) -> tuple[bytes | None, dict]:
    data_url, checksum_url = archive_urls(symbol, month)
    record = {"symbol": symbol, "month": month, "url": data_url, "checksum_url": checksum_url}
    try:
        checksum_text = get(checksum_url).decode("utf-8", "replace").strip()
        expected = checksum_text.split()[0].lower()
        if not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError("invalid_checksum_format")
        payload = get(data_url)
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected:
            record.update(status="CHECKSUM_MISMATCH", expected_sha256=expected, actual_sha256=actual)
            return None, record
        record.update(status="OK", bytes=len(payload), sha256=actual)
        return payload, record
    except urllib.error.HTTPError as exc:
        record.update(status="SOURCE_UNAVAILABLE" if exc.code == 404 else "HTTP_ERROR", http_code=exc.code)
    except Exception as exc:  # evidence records the exact failure
        record.update(status="ERROR", error=f"{type(exc).__name__}:{exc}")
    return None, record


def read_zip(payload: bytes, source_label: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [name for name in archive.namelist() if not name.endswith("/")]
        if len(members) != 1:
            raise ValueError(f"{source_label}: expected one member, got {len(members)}")
        raw = archive.read(members[0])
    frame = pd.read_csv(
        io.BytesIO(raw), header=None, usecols=[0, 1, 2, 3, 4, 7],
        names=["open_time", "open", "high", "low", "close", "quote_volume"],
    )
    frame["open_time"] = pd.to_numeric(frame["open_time"], errors="coerce")
    frame = frame[frame["open_time"].notna()].copy()
    for column in ("open_time", "open", "high", "low", "close", "quote_volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna()
    frame["open_time"] = frame["open_time"].astype("int64")
    frame.loc[frame["open_time"] > 100_000_000_000_000, "open_time"] //= 1000
    return frame


def contiguous_segments(frame: pd.DataFrame) -> list[pd.DataFrame]:
    if frame.empty:
        return []
    frame = frame.drop_duplicates("open_time").sort_values("open_time").reset_index(drop=True)
    group = frame["open_time"].diff().fillna(BAR_MS).ne(BAR_MS).cumsum()
    return [segment.reset_index(drop=True) for _, segment in frame.groupby(group, sort=False)]


def complete_day_volume(segment: pd.DataFrame) -> dict[int, float]:
    times = segment["open_time"].to_numpy(dtype=np.int64)
    days = times // 86_400_000
    volumes = segment["quote_volume"].to_numpy(dtype=float)
    result: dict[int, float] = {}
    for day in np.unique(days):
        indexes = np.flatnonzero(days == day)
        if len(indexes) == DAY_BARS and times[indexes[-1]] - times[indexes[0]] == (DAY_BARS - 1) * BAR_MS:
            result[int(day)] = float(volumes[indexes].sum())
    return result


def baseline_by_block(segment: pd.DataFrame) -> dict[int, float]:
    times = segment["open_time"].to_numpy(dtype=np.int64)
    blocks = times // 43_200_000
    opens = segment["open"].to_numpy(dtype=float)
    highs = segment["high"].to_numpy(dtype=float)
    lows = segment["low"].to_numpy(dtype=float)
    complete_ranges: dict[int, float] = {}
    for block in np.unique(blocks):
        indexes = np.flatnonzero(blocks == block)
        if len(indexes) != BLOCK12_BARS:
            continue
        if times[indexes[-1]] - times[indexes[0]] != (BLOCK12_BARS - 1) * BAR_MS:
            continue
        reference = opens[indexes[0]]
        if reference > 0:
            complete_ranges[int(block)] = float((highs[indexes].max() - lows[indexes].min()) / reference)
    result: dict[int, float] = {}
    for block in np.unique(blocks):
        prior = [complete_ranges[b] for b in range(int(block) - BASELINE_BLOCKS, int(block)) if b in complete_ranges]
        if len(prior) == BASELINE_BLOCKS:
            result[int(block)] = float(np.median(prior))
    return result


def scan_segment(symbol: str, segment: pd.DataFrame) -> tuple[list[dict], dict]:
    if len(segment) < BASELINE_BLOCKS * BLOCK12_BARS + DAY_BARS:
        return [], {"bars": len(segment), "raw_candidates": 0, "eligible": 0}

    times = segment["open_time"].to_numpy(dtype=np.int64)
    opens = segment["open"].to_numpy(dtype=float)
    highs = segment["high"].to_numpy(dtype=float)
    closes = segment["close"].to_numpy(dtype=float)
    wick_gain = highs / opens - 1.0
    body_gain = closes / opens - 1.0
    prior_max = pd.Series(wick_gain).rolling(PRIOR_EVENT_BARS, min_periods=1).max().shift(1).to_numpy()
    days = times // 86_400_000
    blocks = times // 43_200_000
    day_volume = complete_day_volume(segment)
    baseline = baseline_by_block(segment)

    candidate_indexes = np.flatnonzero(
        (times >= SCAN_START) & (times < SCAN_END) & (opens > 0) &
        np.isfinite(wick_gain) & (wick_gain >= EVENT_THRESHOLD)
    )
    events: list[dict] = []
    skip_counts: defaultdict[str, int] = defaultdict(int)
    for index in candidate_indexes:
        volume = day_volume.get(int(days[index]))
        normal_range = baseline.get(int(blocks[index]))
        if volume is None:
            skip_counts["event_day_volume_incomplete"] += 1
            continue
        if normal_range is None:
            skip_counts["prior_30d_baseline_incomplete"] += 1
            continue
        if volume < LIQ_MIN:
            skip_counts["below_liquidity_floor"] += 1
            continue
        if np.isfinite(prior_max[index]) and prior_max[index] >= EVENT_THRESHOLD:
            skip_counts["continuation_prior_6h"] += 1
            continue

        gain = float(wick_gain[index])
        event = {
            "symbol": symbol,
            "t_ref_ms": int(times[index]),
            "event_timestamp_utc": iso_utc(int(times[index])),
            "candle_close_utc": iso_utc(int(times[index] + BAR_MS - 1)),
            "interval": "1h",
            "open": float(opens[index]),
            "high": float(highs[index]),
            "close": float(closes[index]),
            "wick_gain_pct": gain * 100.0,
            "body_gain_pct": float(body_gain[index]) * 100.0,
            "event_rule": "(1h_high / 1h_open - 1) >= 0.20; wick included",
            "event_day_quote_volume": float(volume),
            "median_12h_range_prior30d": float(normal_range),
            "brake_pass": bool(gain >= BRAKE_MULT * normal_range),
            "source_type": "REAL_OBSERVED",
        }
        events.append(event)

    return events, {
        "bars": len(segment),
        "raw_candidates": int(len(candidate_indexes)),
        "eligible": len(events),
        "skipped": dict(skip_counts),
    }


def process_symbol(symbol: str) -> tuple[str, dict, list[dict]]:
    frames: list[pd.DataFrame] = []
    download_records: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(verified_download, symbol, month) for month in MONTHS]
        for future in cf.as_completed(futures):
            payload, record = future.result()
            download_records.append(record)
            if payload is not None:
                try:
                    frames.append(read_zip(payload, f"{symbol}:{record['month']}"))
                except Exception as exc:
                    record.update(status="PARSE_ERROR", error=f"{type(exc).__name__}:{exc}")

    quality = {"downloads": download_records, "bars": 0, "segments": 0, "gaps": 0, "raw_candidates": 0, "eligible": 0, "skipped": {}}
    events: list[dict] = []
    if frames:
        segments = contiguous_segments(pd.concat(frames, ignore_index=True))
        quality["segments"] = len(segments)
        quality["gaps"] = max(0, len(segments) - 1)
        skipped: defaultdict[str, int] = defaultdict(int)
        for segment in segments:
            segment_events, stats = scan_segment(symbol, segment)
            events.extend(segment_events)
            quality["bars"] += stats["bars"]
            quality["raw_candidates"] += stats["raw_candidates"]
            quality["eligible"] += stats["eligible"]
            for key, value in stats.get("skipped", {}).items():
                skipped[key] += value
        quality["skipped"] = dict(skipped)
    return symbol, quality, events


def dedupe_first_per_symbol_day(events: list[dict]) -> tuple[list[dict], int]:
    kept: list[dict] = []
    seen: set[tuple[str, int]] = set()
    removed = 0
    for event in sorted(events, key=lambda item: (item["symbol"], item["t_ref_ms"])):
        key = (event["symbol"], event["t_ref_ms"] // 86_400_000)
        if key in seen:
            removed += 1
            continue
        seen.add(key)
        kept.append(event)
    return kept, removed


def main() -> None:
    started = utc_now()
    symbols, universe = build_universe()
    all_events: list[dict] = []
    per_symbol: dict[str, dict] = {}
    began = time.time()

    with cf.ThreadPoolExecutor(max_workers=int(os.getenv("PUMP500_WORKERS", "6"))) as executor:
        futures = {executor.submit(process_symbol, symbol): symbol for symbol in symbols}
        for number, future in enumerate(cf.as_completed(futures), start=1):
            symbol = futures[future]
            try:
                symbol, quality, events = future.result()
                per_symbol[symbol] = quality
                all_events.extend(events)
            except Exception as exc:
                per_symbol[symbol] = {"fatal": f"{type(exc).__name__}:{exc}"}
            if number % 25 == 0:
                print(number, len(symbols), round(time.time() - began), flush=True)

    deduped, same_day_removed = dedupe_first_per_symbol_day(all_events)
    braked = [event for event in deduped if event["brake_pass"]]
    severity = {}
    for threshold in SEVERITY_THRESHOLDS:
        key = str(int(threshold * 100))
        raw_subset = [event for event in deduped if event["wick_gain_pct"] >= threshold * 100]
        braked_subset = [event for event in braked if event["wick_gain_pct"] >= threshold * 100]
        severity[key] = {
            "events_after_liquidity_cleanliness_dedupe": len(raw_subset),
            "events_after_3x_volatility_brake": len(braked_subset),
            "symbols_after_brake": len({event["symbol"] for event in braked_subset}),
        }

    for filename, rows in (("P01_EVENTS_20PCT_ALL.jsonl", deduped), ("P01_EVENTS_20PCT_BRAKED.jsonl", braked)):
        with (OUT / filename).open("w") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

    file_status: defaultdict[str, int] = defaultdict(int)
    unavailable: list[dict] = []
    for quality in per_symbol.values():
        for record in quality.get("downloads", []):
            status = record.get("status", "UNKNOWN")
            file_status[status] += 1
            if status != "OK":
                unavailable.append(record)

    data_quality = {
        "generated_utc": utc_now(),
        "file_status_counts": dict(file_status),
        "source_unavailable": unavailable,
        "per_symbol": per_symbol,
        "gap_policy": "never cross or fill a non-1h gap",
        "checksum_policy": "official .CHECKSUM SHA-256 required",
    }
    (OUT / "P01_DATA_QUALITY.json").write_text(json.dumps(data_quality, ensure_ascii=False, indent=2))

    result = {
        "study": "pump500_strict_fresh_20260724",
        "started_utc": started,
        "completed_utc": utc_now(),
        "fresh_data_only": True,
        "prior_results_used": False,
        "period": {"scan": "2026-01-01T00:00:00Z..2026-07-01T00:00:00Z", "warmup": "2025-12"},
        "definition": {
            "event": "official Binance 1h candle (high/open - 1) >= 0.20",
            "wick_included": True,
            "t_ref": "1h candle open timestamp in UTC, second precision",
            "liquidity": "complete UTC event-day quote volume >= 100000 USDT",
            "cleanliness": "no prior qualifying 1h +20% wick candle in previous complete 6h",
            "volatility_brake": "1h wick gain >= 3x median range of previous 60 complete UTC 12h blocks",
            "dedupe": "first eligible event per symbol per UTC day",
        },
        "main_event_count_before_brake": len(deduped),
        "main_event_count_after_brake": len(braked),
        "main_symbol_count_after_brake": len({event["symbol"] for event in braked}),
        "same_symbol_day_duplicates_removed": same_day_removed,
        "severity_census": severity,
        "p01_status": "FROZEN_BY_OPERATOR_1H_WICK_20PCT",
        "p01b_status": "THRESHOLD_SELECTION_SUPERSEDED_BY_OPERATOR_FIXED_20PCT",
        "p02_status": "DONE_VERIFIED",
        "p03_status": "DONE_VERIFIED",
        "p05_plus_status": "CLOSED_UNTIL_P01_CENSUS_VALIDATED",
        "universe_summary": {"kept_count": universe["kept_count"], "excluded_count": universe["excluded_count"]},
    }
    (OUT / "P01_STRICT_RESULT.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))

    lines = [
        "# P01 — 1 Saatlik Mumda Fitil Dahil +%20 Pump Sayımı",
        "",
        "Ana tanım: `(1h high / 1h open - 1) >= 0.20`; fitil dahildir.",
        "",
        "| Mum içi yükseliş | Temizlik+likidite sonrası | 3× oynaklık freni sonrası | Coin |",
        "|---:|---:|---:|---:|",
    ]
    for threshold in SEVERITY_THRESHOLDS:
        row = severity[str(int(threshold * 100))]
        lines.append(
            f"| ≥ +%{int(threshold * 100)} | {row['events_after_liquidity_cleanliness_dedupe']} | "
            f"**{row['events_after_3x_volatility_brake']}** | {row['symbols_after_brake']} |"
        )
    lines.extend(["", "P01 operatör direktifiyle +%20 olarak donmuştur.", f"Dosya durumları: {dict(file_status)}"])
    (OUT / "P01_STRICT_TABLE.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
