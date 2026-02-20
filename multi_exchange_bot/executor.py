import os
import time
import json
import uuid
import asyncio
import threading
import datetime
import hashlib
import hmac
import base64
import subprocess
from collections import deque
from email.utils import parsedate_to_datetime
from statistics import median
from typing import Optional, Callable

import requests
import websockets
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from bot.market_presence import MarketPresenceResolver, SEARCHED_EXCHANGES
from bot.trade_engine import TradeEngine
from bot.util import log

PILOT_TOKEN = os.getenv("PILOT_TOKEN", "change-me")
EXECUTOR_PORT = int(os.getenv("EXECUTOR_PORT", "8080"))
WS_TRIGGER_TIMEOUT_SEC = max(5, int(os.getenv("WS_TRIGGER_TIMEOUT_SEC", "20")))

ARM_LOCK_PATH = os.getenv("ARM_LOCK_PATH", "/opt/quickbot/arm.lock")
GATE_OFFSET_SAMPLES = max(3, int(os.getenv("GATE_OFFSET_SAMPLES", "5")))
GATE_WARMUP_SYNC_SAMPLES = max(1, int(os.getenv("GATE_WARMUP_SYNC_SAMPLES", "3")))

TELEMETRY_QUEUE_MAX = max(200, int(os.getenv("TELEMETRY_QUEUE_MAX", "5000")))
TELEMETRY_FLUSH_INTERVAL_SEC = max(0.1, float(os.getenv("TELEMETRY_FLUSH_INTERVAL_SEC", "0.5")))
ORDER_HISTORY_LIMIT = max(20, int(os.getenv("ORDER_HISTORY_LIMIT", "200")))
TRADE_CYCLE_LIMIT = max(20, int(os.getenv("TRADE_CYCLE_LIMIT", "120")))

GATE_WS_URL = "wss://api.gateio.ws/ws/v4/"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
MEXC_WS_URLS = [
    "wss://wbs-api.mexc.com/ws",
    "wss://wbs.mexc.com/ws",
]
BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"

engine = TradeEngine()
MARKET_PRESENCE_RESOLVER = MarketPresenceResolver()

# Single active ARM at a time (MVP behavior).
ARM_STATE = {
    "armed": False,
    "exchange": None,
    "symbol": None,
    "spend_usdt": None,
    "phase": "idle",
    "target_iso": None,
    "target_epoch_ms": None,
    "arm_hash": None,
    "market_presence": None,
}
ARM_PREPARED_PLAN = None

STATE_LOCK = threading.Lock()
MAIN_LOOP = None
NON_GATE_TASK = None
WS_SUPPORTED = {"gate", "binance", "mexc", "kucoin", "bitget"}
PUBLIC_LISTING_EXCHANGES = ("okex", "bybit", "btcturk", "paribu")
PUBLIC_EXCHANGE_BASES = {
    "okex": os.getenv("OKEX_BASE", "https://www.okx.com").rstrip("/"),
    "bybit": os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/"),
    "btcturk": os.getenv("BTCTURK_BASE", "https://api.btcturk.com").rstrip("/"),
    "paribu": os.getenv("PARIBU_BASE", "https://api.paribu.com").rstrip("/"),
}
PUBLIC_AUTH_ENV_MAP = {
    "okex": {"key": "OKEX_KEY", "secret": "OKEX_SECRET", "passphrase": "OKEX_PASSPHRASE"},
    "bybit": {"key": "BYBIT_KEY", "secret": "BYBIT_SECRET"},
    "btcturk": {"key": "BTCTURK_KEY", "secret": "BTCTURK_SECRET"},
    "paribu": {"key": "PARIBU_KEY", "secret": "PARIBU_SECRET"},
}
PUBLIC_AUTH_CONFIG = {
    ex: {k: str(os.getenv(env_name, "")).strip() for k, env_name in mapping.items()}
    for ex, mapping in PUBLIC_AUTH_ENV_MAP.items()
}
ALL_TEST_EXCHANGES = list(dict.fromkeys(list(engine.adapters.keys()) + list(PUBLIC_LISTING_EXCHANGES)))
LAST_EXECUTION = {}
ORDER_LATENCY_BY_EXCHANGE = {}
LAST_MARKET_PRESENCE = {}
ORDER_HISTORY = deque(maxlen=ORDER_HISTORY_LIMIT)
TRADE_CYCLES = deque(maxlen=TRADE_CYCLE_LIMIT)
OPEN_POSITIONS = {}
LAST_ORDER_SIGNAL = {}
ORDER_SEQ = 0
QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BTC", "ETH", "TRY", "TL", "EUR", "BNB")

TELEMETRY_QUEUE = deque(maxlen=TELEMETRY_QUEUE_MAX)
TELEMETRY_RECENT = deque(maxlen=max(1000, TELEMETRY_QUEUE_MAX))
TELEMETRY_LOCK = threading.Lock()
TELEMETRY_STOP = threading.Event()
TELEMETRY_THREAD = None


class ArmPayload(BaseModel):
    exchange: str
    symbol: str
    spend_usdt: str
    target_iso: Optional[str] = None
    listing_title: Optional[str] = None
    listing_url: Optional[str] = None
    contract_hint: Optional[str] = None


class SellPayload(BaseModel):
    exchange: str
    symbol: str
    qty: str


class DryRunPayload(BaseModel):
    enabled: bool


class AdminConfigPayload(BaseModel):
    dry_run: Optional[bool] = None
    values: dict = {}


class ExchangeTestPayload(BaseModel):
    exchange: Optional[str] = None


class LatencyProbePayload(BaseModel):
    exchange: str
    symbol: str = ""
    spend_usdt: str = "5"


class RealTradeTestPayload(BaseModel):
    exchange: str
    symbol: str
    spend_usdt: str
    auto_sell: bool = False
    sell_wait_sec: float = 0.0
    sell_qty: str = ""
    round_trip: bool = False
    round_trip_mode: str = "buy_then_sell"
    exec_mode: str = "now"
    execute_at_iso: str = ""
    execute_at_ms: Optional[int] = None
    client_click_ms: Optional[int] = None
    client_click_perf_ms: Optional[float] = None
    panel_received_ms: Optional[int] = None
    panel_send_ms: Optional[int] = None

class TimeSyncPayload(BaseModel):
    exchange: Optional[str] = None
    samples: int = 5
    warmup_sync_samples: int = 3
    timeout_sec: float = 1.2


class GateSyncPayload(BaseModel):
    samples: int = 5
    warmup_sync_samples: int = 3

class WarmupPayload(BaseModel):
    exchange: Optional[str] = None
    timeout_sec: float = 1.2
    sync_samples: int = 0


class MarketPresencePayload(BaseModel):
    exchange: str = "gate"
    symbol: str = "BTC_USDT"
    listing_title: str = ""
    listing_url: str = ""
    contract_hint: str = ""


class BalanceAvailablePayload(BaseModel):
    exchange: str
    symbol: str
    side: str = "buy"
    percent: float = 100.0


class WalletBalancesPayload(BaseModel):
    exchange: Optional[str] = None
    non_zero_only: bool = True
    limit: int = 120


app = FastAPI()


def now_iso_utc() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def now_wall_ms() -> int:
    return int(time.time() * 1000)


def parse_iso_to_epoch_ms(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def canonical_json(data: dict) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256((value or "").encode()).hexdigest()


def hash_secret_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
    else:
        raw = str(value).encode()
    return hashlib.sha256(raw).hexdigest()


def runtime_config_fingerprint() -> dict:
    out = {
        "dry_run": bool(engine.dry_run),
        "retry": {
            "attempts": engine.sniper_retry_attempts,
            "window_ms": engine.sniper_retry_window_ms,
            "attempt_timeout_ms": engine.sniper_attempt_timeout_ms,
            "jitter_min_ms": engine.sniper_retry_jitter_min_ms,
            "jitter_max_ms": engine.sniper_retry_jitter_max_ms,
        },
        "exchanges": {},
    }

    for name, ex in engine.adapters.items():
        row = {
            "base": str(getattr(ex, "base", "") or "").rstrip("/"),
            "key_sha": hash_secret_value(getattr(ex, "key", "") or ""),
            "secret_sha": hash_secret_value(getattr(ex, "secret", b"") or b""),
        }
        if hasattr(ex, "passphrase"):
            row["passphrase_sha"] = hash_secret_value(getattr(ex, "passphrase", "") or "")
        out["exchanges"][name] = row

    return out


def current_arm_config_hash() -> str:
    return sha256_text(canonical_json(runtime_config_fingerprint()))


def read_arm_lock() -> Optional[dict]:
    try:
        with open(ARM_LOCK_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def write_arm_lock(payload: dict) -> None:
    folder = os.path.dirname(ARM_LOCK_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)
    tmp_path = ARM_LOCK_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(canonical_json(payload))
    os.replace(tmp_path, ARM_LOCK_PATH)


def build_plan_summary(exchange: str, symbol: str, spend: str, prepared_plan: Optional[dict], target_iso: Optional[str], target_epoch_ms: Optional[int]) -> dict:
    summary = {
        "exchange": exchange,
        "symbol": symbol,
        "spend_usdt": str(spend),
        "target_iso": target_iso,
        "target_epoch_ms": target_epoch_ms,
        "retry": {
            "attempts": engine.sniper_retry_attempts,
            "window_ms": engine.sniper_retry_window_ms,
            "attempt_timeout_ms": engine.sniper_attempt_timeout_ms,
            "jitter_min_ms": engine.sniper_retry_jitter_min_ms,
            "jitter_max_ms": engine.sniper_retry_jitter_max_ms,
        },
    }
    if isinstance(prepared_plan, dict):
        summary["prepared"] = {
            "method": prepared_plan.get("method"),
            "path": prepared_plan.get("path"),
            "path_with_prefix": prepared_plan.get("path_with_prefix"),
            "query": prepared_plan.get("query"),
            "body_hash": prepared_plan.get("body_hash"),
        }
    return summary


def validate_arm_lock(exchange: str, symbol: str, spend: str):
    lock = read_arm_lock()
    if not lock:
        return False, "arm_lock_missing", None

    expected_hash = lock.get("config_hash")
    current_hash = current_arm_config_hash()
    if expected_hash != current_hash:
        return False, "config_hash_mismatch", lock

    plan = lock.get("plan") or {}
    if str(plan.get("exchange") or "") != str(exchange):
        return False, "plan_exchange_mismatch", lock
    if str(plan.get("symbol") or "") != str(symbol):
        return False, "plan_symbol_mismatch", lock
    if str(plan.get("spend_usdt") or "") != str(spend):
        return False, "plan_spend_mismatch", lock

    return True, "ok", lock


def emit_event(event: str, **fields):
    rec = {
        "event": event,
        "wall_ms": now_wall_ms(),
        "mono_ns": time.monotonic_ns(),
    }
    rec.update(fields or {})
    with TELEMETRY_LOCK:
        TELEMETRY_QUEUE.append(rec)
        TELEMETRY_RECENT.append(dict(rec))


def get_recent_telemetry(limit: int = 200):
    lim = max(1, min(int(limit or 200), len(TELEMETRY_RECENT) or 1))
    with TELEMETRY_LOCK:
        return list(TELEMETRY_RECENT)[-lim:]


def _drain_telemetry(limit: int = 200):
    out = []
    with TELEMETRY_LOCK:
        n = min(limit, len(TELEMETRY_QUEUE))
        for _ in range(n):
            out.append(TELEMETRY_QUEUE.popleft())
    return out


def flush_telemetry_batch(limit: int = 200):
    batch = _drain_telemetry(limit=limit)
    if not batch:
        return 0

    for rec in batch:
        log("TELEMETRY " + json.dumps(rec, ensure_ascii=False, separators=(",", ":")))

    flush_rec = {
        "event": "log_flush_done",
        "wall_ms": now_wall_ms(),
        "mono_ns": time.monotonic_ns(),
        "count": len(batch),
    }
    log("TELEMETRY " + json.dumps(flush_rec, ensure_ascii=False, separators=(",", ":")))
    return len(batch)


def telemetry_worker():
    while not TELEMETRY_STOP.is_set():
        try:
            flush_telemetry_batch(limit=400)
        except Exception as e:
            log(f"TELEMETRY_FLUSH_ERROR: {type(e).__name__}: {e}")
        TELEMETRY_STOP.wait(TELEMETRY_FLUSH_INTERVAL_SEC)

    # Final drain on shutdown.
    try:
        while flush_telemetry_batch(limit=500) > 0:
            pass
    except Exception as e:
        log(f"TELEMETRY_FINAL_FLUSH_ERROR: {type(e).__name__}: {e}")


def _run_cmd_text(args, timeout_sec: float = 2.0):
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout_sec, check=False)
        out = (proc.stdout or proc.stderr or "").strip()
        return proc.returncode == 0, out
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def log_time_sync_health():
    checks = [
        ("timedatectl_status", ["timedatectl", "status"]),
        ("chronyc_tracking", ["chronyc", "tracking"]),
    ]
    for name, cmd in checks:
        ok, output = _run_cmd_text(cmd, timeout_sec=2.5)
        lines = [ln.strip() for ln in str(output).splitlines() if ln.strip()][:10]
        joined = " | ".join(lines)
        log(f"TIME_SYNC_HEALTH {name} ok={ok} {joined}")
        emit_event("time_sync_health", check=name, ok=ok, output=joined)


def ntp_snapshot() -> dict:
    out = {}
    checks = [
        ("timedatectl_status", ["timedatectl", "status"]),
        ("chronyc_tracking", ["chronyc", "tracking"]),
    ]
    for name, cmd in checks:
        ok, output = _run_cmd_text(cmd, timeout_sec=2.5)
        lines = [ln.strip() for ln in str(output).splitlines() if ln.strip()][:16]
        out[name] = {"ok": ok, "output": lines}
    return out


def check_token(x: Optional[str]):
    if x != PILOT_TOKEN:
        raise HTTPException(status_code=403, detail="Bad PILOT token")


def get_arm_snapshot() -> dict:
    with STATE_LOCK:
        return dict(ARM_STATE)


def set_arm_state(**kwargs):
    with STATE_LOCK:
        ARM_STATE.update(kwargs)


def clear_arm_state():
    global ARM_PREPARED_PLAN
    with STATE_LOCK:
        ARM_STATE.update(
            {
                "armed": False,
                "exchange": None,
                "symbol": None,
                "spend_usdt": None,
                "phase": "idle",
                "target_iso": None,
                "target_epoch_ms": None,
                "arm_hash": None,
                "market_presence": None,
            }
        )
        ARM_PREPARED_PLAN = None


def set_last_market_presence(payload: Optional[dict]):
    row = dict(payload or {})
    row.setdefault("at", now_iso_utc())
    with STATE_LOCK:
        LAST_MARKET_PRESENCE.clear()
        LAST_MARKET_PRESENCE.update(row)


def default_probe_symbol(exchange: str) -> str:
    ex = (exchange or "").strip().lower()
    if ex == "gate":
        return "BTC_USDT"
    if ex == "kucoin":
        return "BTC-USDT"
    return "BTCUSDT"


def get_exchange_base_url(exchange: str) -> str:
    ex = (exchange or "").strip().lower()
    if ex in engine.adapters:
        return str(getattr(engine.adapters[ex], "base", "") or "").rstrip("/")
    return str(PUBLIC_EXCHANGE_BASES.get(ex, "") or "").rstrip("/")


def get_exchange_probe_path(exchange: str) -> str:
    ex = (exchange or "").strip().lower()
    probe_map = {
        "gate": "/spot/time",
        "mexc": "/api/v3/ping",
        "kucoin": "/api/v1/timestamp",
        "bitget": "/api/v2/public/time",
        "binance": "/api/v3/ping",
        "okex": "/api/v5/public/time",
        "bybit": "/v5/market/time",
        "btcturk": "/api/v2/server/time",
        "paribu": "/market/ticker",
    }
    return probe_map.get(ex, "")


def build_exchange_probe_url(exchange: str) -> str:
    base = get_exchange_base_url(exchange)
    path = get_exchange_probe_path(exchange)
    if not base:
        return ""
    if not path:
        return base
    return base.rstrip("/") + path


def set_public_exchange_base(exchange: str, value: str):
    ex = (exchange or "").strip().lower()
    if ex not in PUBLIC_EXCHANGE_BASES:
        return
    if value:
        PUBLIC_EXCHANGE_BASES[ex] = str(value).strip().rstrip("/")


def set_public_exchange_auth(exchange: str, key=None, secret=None, passphrase=None):
    ex = (exchange or "").strip().lower()
    if ex not in PUBLIC_AUTH_CONFIG:
        return
    mapping = PUBLIC_AUTH_ENV_MAP.get(ex, {})
    row = PUBLIC_AUTH_CONFIG[ex]
    updates = {"key": key, "secret": secret, "passphrase": passphrase}
    for field, incoming in updates.items():
        if incoming is None:
            continue
        if field not in mapping:
            continue
        value = str(incoming or "").strip()
        row[field] = value
        os.environ[mapping[field]] = value


def _parse_server_time_ms(exchange: str, resp: requests.Response, payload) -> Optional[int]:
    ex = (exchange or "").strip().lower()
    if ex == "okex":
        try:
            row = ((payload or {}).get("data") or [{}])[0]
            val = row.get("ts")
            iv = int(str(val))
            return iv if iv > 10_000_000_000 else iv * 1000
        except Exception:
            return None
    if ex == "bybit":
        try:
            res = (payload or {}).get("result") or {}
            ns = res.get("timeNano")
            sec = res.get("timeSecond")
            if ns:
                return int(int(str(ns)) / 1_000_000)
            if sec:
                return int(str(sec)) * 1000
        except Exception:
            return None
        return None
    if ex == "btcturk":
        try:
            body = payload or {}
            val = body.get("serverTime")
            if val is None:
                data = body.get("data") or {}
                val = data.get("serverTime")
            if val is None:
                iso = body.get("serverTime2") or (body.get("data") or {}).get("serverTime2")
                if iso:
                    dt = datetime.datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                    return int(dt.timestamp() * 1000)
            if val is None:
                return None
            iv = int(str(val))
            return iv if iv > 10_000_000_000 else iv * 1000
        except Exception:
            return None
    if ex == "paribu":
        try:
            date_header = (resp.headers or {}).get("Date")
            if not date_header:
                return None
            dt = parsedate_to_datetime(date_header)
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    return None


def _sync_public_exchange_time(exchange: str, samples: int = 5, timeout_sec: float = 1.2) -> dict:
    ex = (exchange or "").strip().lower()
    base = get_exchange_base_url(ex)
    endpoint_map = {
        "okex": "/api/v5/public/time",
        "bybit": "/v5/market/time",
        "btcturk": "/api/v2/server/time",
        "paribu": "/market/ticker",
    }
    path = endpoint_map.get(ex)
    if not base or not path:
        return {"ok": False, "exchange": ex, "error": "unsupported_exchange"}

    req_samples = max(3, int(samples))
    offsets = []
    rtts = []
    ok_count = 0
    for _ in range(req_samples):
        t0 = int(time.time() * 1000)
        try:
            resp = requests.get(base + path, timeout=max(0.2, float(timeout_sec)))
            t1 = int(time.time() * 1000)
            rtt = max(0, t1 - t0)
            if resp.status_code >= 400:
                continue
            try:
                payload = resp.json()
            except Exception:
                payload = {}
            server_ms = _parse_server_time_ms(ex, resp, payload)
            rtts.append(rtt)
            if server_ms is not None:
                local_mid = t0 + (rtt // 2)
                offsets.append(int(server_ms - local_mid))
            ok_count += 1
        except Exception:
            continue

    if ok_count <= 0:
        return {"ok": False, "exchange": ex, "error": "time_sync_failed"}

    out = {
        "ok": True,
        "exchange": ex,
        "samples": ok_count,
        "rtt_median_ms": int(median(rtts)) if rtts else None,
        "offset_ms": int(median(offsets)) if offsets else None,
        "offset_source": "server_time" if offsets else "rtt_only",
    }
    return out


def _warmup_public_exchange(exchange: str, timeout_sec: float = 1.2) -> dict:
    ex = (exchange or "").strip().lower()
    url = build_exchange_probe_url(ex)
    if not url:
        return {"ok": False, "exchange": ex, "message": "missing_base_url"}
    t0 = time.perf_counter()
    try:
        resp = requests.get(url, timeout=max(0.2, float(timeout_sec)), allow_redirects=True)
        lat = int((time.perf_counter() - t0) * 1000)
        return {
            "ok": 200 <= resp.status_code < 400,
            "exchange": ex,
            "status": resp.status_code,
            "latency_ms": lat,
            "message": "ok" if 200 <= resp.status_code < 400 else f"http_{resp.status_code}",
        }
    except Exception as e:
        lat = int((time.perf_counter() - t0) * 1000)
        return {
            "ok": False,
            "exchange": ex,
            "status": None,
            "latency_ms": lat,
            "message": f"{type(e).__name__}: {e}",
        }


def _to_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        out = float(value)
        if out != out:  # NaN guard
            return None
        return out
    except Exception:
        return None


def _to_epoch_ms(value):
    try:
        if value is None:
            return None
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            if any(ch in raw for ch in ("-", "T", ":")):
                dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return int(dt.timestamp() * 1000)
        iv = int(float(value))
        if iv <= 0:
            return None
        if iv < 10_000_000_000:
            return iv * 1000
        return iv
    except Exception:
        return None


def _split_symbol_parts_for_exchange(exchange: str, symbol: str):
    ex = str(exchange or "").strip().lower()
    raw = str(symbol or "").strip().upper()
    if not raw:
        return "", ""
    for sep in ("_", "-", "/"):
        if sep in raw:
            left, right = raw.split(sep, 1)
            base = "".join(ch for ch in left if ch.isalnum())
            quote = "".join(ch for ch in right if ch.isalnum())
            if ex == "paribu":
                if base in ("TL", "TRY") and quote not in ("TL", "TRY"):
                    base, quote = quote, "TL"
                if quote == "TRY":
                    quote = "TL"
            if quote == "TRY":
                quote = "TL"
            return base, quote
    compact = "".join(ch for ch in raw if ch.isalnum())
    for q in sorted(QUOTE_SUFFIXES, key=len, reverse=True):
        if compact.endswith(q) and len(compact) > len(q):
            base = compact[: -len(q)]
            quote = "TL" if q == "TRY" else q
            return base, quote
    return compact, ""


def _symbol_base_quote(exchange: str, symbol: str):
    base, quote = _split_symbol_parts_for_exchange(exchange, symbol)
    ex = str(exchange or "").strip().lower()
    if not quote:
        if ex == "paribu":
            quote = "TL"
        elif ex == "btcturk":
            quote = "TRY"
        else:
            quote = "USDT"
    return base, quote


def _asset_match(value, target: str) -> bool:
    v = str(value or "").strip().upper()
    t = str(target or "").strip().upper()
    if not v or not t:
        return False
    if v == t:
        return True
    aliases = {
        "TRY": {"TL"},
        "TL": {"TRY"},
    }
    return t in aliases.get(v, set()) or v in aliases.get(t, set())


def _extract_available_from_row(row: dict):
    if not isinstance(row, dict):
        return None
    for key in (
        "available", "availableBalance", "availBal", "free", "canUse",
        "usable", "balanceAvailable", "amount", "balance", "total",
    ):
        val = row.get(key)
        num = _to_float(val)
        if num is not None and num >= 0:
            return num
    return None


def _iter_balance_rows(body):
    if isinstance(body, list):
        for row in body:
            if isinstance(row, dict):
                yield row
        return
    if not isinstance(body, dict):
        return

    yield body
    data = body.get("data")
    if isinstance(data, dict):
        yield data
        for key in ("balances", "assets", "list", "items"):
            val = data.get(key)
            if isinstance(val, list):
                for row in val:
                    if isinstance(row, dict):
                        yield row
    elif isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                yield row

    for key in ("balances", "assets", "list", "items"):
        val = body.get(key)
        if isinstance(val, list):
            for row in val:
                if isinstance(row, dict):
                    yield row


def _find_asset_available(body, asset: str):
    target = str(asset or "").strip().upper()
    if not target:
        return None
    best = None
    for row in _iter_balance_rows(body):
        cur = (
            row.get("currency")
            or row.get("asset")
            or row.get("coin")
            or row.get("symbol")
            or row.get("name")
        )
        if not _asset_match(cur, target):
            continue
        amt = _extract_available_from_row(row)
        if amt is None:
            continue
        best = amt if best is None else max(best, amt)
    return best


def _fetch_balance_payload(exchange: str):
    ex = str(exchange or "").strip().lower()
    if not ex:
        return {"ok": False, "error": "invalid_exchange"}
    try:
        t0 = time.perf_counter()
        if ex == "gate":
            g = engine.adapters["gate"]
            path = "/spot/accounts"
            headers = g._sign_headers("GET", "/api/v4" + path, "", "")
            r = g.session.get(g.base + path, headers=headers, timeout=6)
            body = safe_json_response(r)
            return {
                "ok": r.status_code < 400,
                "status": r.status_code,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "body": body,
            }
        if ex == "binance":
            b = engine.adapters["binance"]
            signed = b._signed({})
            r = b.session.get(b.base + "/api/v3/account", headers=signed["headers"], params=signed["qs"], timeout=6)
            body = safe_json_response(r)
            return {
                "ok": r.status_code < 400,
                "status": r.status_code,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "body": body,
            }
        if ex == "mexc":
            m = engine.adapters["mexc"]
            signed = m._signed({})
            r = m.session.get(m.base + "/api/v3/account", headers=signed["headers"], params=signed["qs"], timeout=6)
            body = safe_json_response(r)
            return {
                "ok": r.status_code < 400,
                "status": r.status_code,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "body": body,
            }
        if ex == "kucoin":
            k = engine.adapters["kucoin"]
            path = "/api/v1/accounts?type=trade"
            headers = k._headers("GET", path, "")
            r = k.session.get(k.base + path, headers=headers, timeout=6)
            body = safe_json_response(r)
            return {
                "ok": (r.status_code < 400 and str((body or {}).get("code", "200000")) == "200000"),
                "status": r.status_code,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "body": body,
            }
        if ex == "bitget":
            b = engine.adapters["bitget"]
            path = "/api/v2/spot/account/assets"
            headers = b._headers("GET", path, "")
            r = b.session.get(b.base + path, headers=headers, timeout=6)
            body = safe_json_response(r)
            code = str((body or {}).get("code", ""))
            return {
                "ok": (r.status_code < 400 and code in ("00000", "0", "")),
                "status": r.status_code,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "body": body,
            }
        if ex == "paribu":
            p = engine.adapters["paribu"]
            out = p._signed_get("/user/assets", timeout_sec=6)
            status = int(out.get("status") or 0)
            body = out.get("body")
            ok = status < 400
            if isinstance(body, dict):
                if body.get("success") is False:
                    ok = False
                state = str(body.get("status") or "").strip().lower()
                if state in ("error", "fail", "failed"):
                    ok = False
            return {
                "ok": ok,
                "status": status,
                "latency_ms": int(out.get("latency_ms") or 0),
                "body": body,
            }
        return {"ok": False, "error": f"unsupported_exchange:{ex}"}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _normalize_balance_rows(exchange: str, body):
    ex = str(exchange or "").strip().lower()
    agg = {}
    for row in _iter_balance_rows(body):
        cur = row.get("currency") or row.get("asset") or row.get("coin") or row.get("symbol") or row.get("name")
        cur_txt = str(cur or "").strip().upper()
        if not cur_txt:
            continue
        if cur_txt == "TRY":
            cur_txt = "TL"
        avail = _to_float(
            row.get("available")
            if row.get("available") is not None
            else (
                row.get("availableBalance")
                if row.get("availableBalance") is not None
                else (
                    row.get("availBal")
                    if row.get("availBal") is not None
                    else (
                        row.get("free")
                        if row.get("free") is not None
                        else (
                            row.get("canUse")
                            if row.get("canUse") is not None
                            else (
                                row.get("usable")
                                if row.get("usable") is not None
                                else row.get("amount")
                            )
                        )
                    )
                )
            )
        )
        locked = _to_float(
            row.get("locked")
            if row.get("locked") is not None
            else (
                row.get("freeze")
                if row.get("freeze") is not None
                else (
                    row.get("frozen")
                    if row.get("frozen") is not None
                    else (
                        row.get("holds")
                        if row.get("holds") is not None
                        else row.get("lockedBalance")
                    )
                )
            )
        )
        total = _to_float(
            row.get("total")
            if row.get("total") is not None
            else (
                row.get("balance")
                if row.get("balance") is not None
                else (
                    row.get("totalAmount")
                    if row.get("totalAmount") is not None
                    else row.get("sum")
                )
            )
        )
        if total is None and (avail is not None or locked is not None):
            total = (avail or 0.0) + (locked or 0.0)
        if avail is None and total is not None and locked is not None:
            avail = max(0.0, total - locked)
        if avail is None and locked is None and total is None:
            continue

        if cur_txt not in agg:
            agg[cur_txt] = {"currency": cur_txt, "available": 0.0, "locked": 0.0, "total": 0.0}
        agg[cur_txt]["available"] += max(0.0, float(avail or 0.0))
        agg[cur_txt]["locked"] += max(0.0, float(locked or 0.0))
        if total is not None:
            agg[cur_txt]["total"] += max(0.0, float(total))
        else:
            agg[cur_txt]["total"] += max(0.0, float(avail or 0.0) + float(locked or 0.0))

    rows = list(agg.values())
    rows.sort(key=lambda x: (x.get("total", 0.0), x.get("available", 0.0)), reverse=True)
    return rows


def _fetch_available_balance(exchange: str, asset: str):
    ex = str(exchange or "").strip().lower()
    cur = str(asset or "").strip().upper()
    if not ex or not cur:
        return {"ok": False, "error": "invalid_exchange_or_asset"}
    fetched = _fetch_balance_payload(ex)
    if not fetched.get("ok"):
        return fetched
    body = fetched.get("body")
    return {
        "ok": True,
        "status": fetched.get("status"),
        "latency_ms": fetched.get("latency_ms"),
        "available": _find_asset_available(body, cur),
        "body": body,
    }


def _candidate_dicts(body):
    out = []
    if isinstance(body, dict):
        out.append(body)
        data = body.get("data")
        if isinstance(data, dict):
            out.append(data)
        elif isinstance(data, list):
            for row in data[:3]:
                if isinstance(row, dict):
                    out.append(row)
    elif isinstance(body, list):
        for row in body[:3]:
            if isinstance(row, dict):
                out.append(row)
    return out


def _first_value(dicts, keys):
    for d in dicts:
        for k in keys:
            if k in d and d.get(k) not in (None, ""):
                return d.get(k)
    return None


def _extract_order_metrics(exchange: str, result: dict):
    payload = dict(result or {})
    body = payload.get("body")
    candidates = _candidate_dicts(body)

    order_id = _first_value(
        candidates,
        (
            "id", "orderId", "order_id", "clientOrderId", "clientOid",
            "ordId", "orderNo",
        ),
    )
    price = _to_float(
        _first_value(
            candidates,
            (
                "avgPrice", "avg_price", "priceAvg", "deal_price", "fillPrice",
                "price", "tradePrice", "average", "avg", "executionPrice",
            ),
        )
    )
    base_qty = _to_float(
        _first_value(
            candidates,
            (
                "executedQty", "executed_qty", "deal_size", "filledSize", "fillSz",
                "size", "qty", "quantity", "filled_amount", "amount", "filled",
            ),
        )
    )
    if base_qty is None:
        base_qty = _to_float(payload.get("_requested_base_qty"))
    quote_qty = _to_float(
        _first_value(
            candidates,
            (
                "cummulativeQuoteQty", "cumulativeQuoteQty", "executedQuoteQty",
                "deal_money", "filled_total", "funds", "fillNotionalUsd",
                "quoteQty", "quoteOrderQty", "notional", "total", "value", "cost",
            ),
        )
    )
    if quote_qty is None:
        quote_qty = _to_float(payload.get("_requested_quote_qty"))
    ts_ms = _to_epoch_ms(
        _first_value(
            candidates,
            (
                "transactTime", "tradeTime", "uTime", "updateTime", "createTime",
                "create_time", "ctime", "ts", "time", "createdAt", "updatedAt",
                "created_at", "updated_at", "date",
            ),
        )
    )

    if (price is None or price <= 0) and base_qty and quote_qty and base_qty > 0:
        price = quote_qty / base_qty
    if (quote_qty is None or quote_qty <= 0) and price and base_qty and base_qty > 0:
        quote_qty = price * base_qty
    if (base_qty is None or base_qty <= 0) and price and quote_qty and price > 0:
        base_qty = quote_qty / price

    return {
        "order_id": str(order_id) if order_id not in (None, "") else "",
        "price": price if price and price > 0 else None,
        "base_qty": base_qty if base_qty and base_qty > 0 else None,
        "quote_qty": quote_qty if quote_qty and quote_qty > 0 else None,
        "exchange_time_ms": ts_ms,
    }


def _is_order_success(exchange: str, result: dict) -> bool:
    payload = dict(result or {})
    status = int(payload.get("status") or 0)
    if not (200 <= status < 300):
        return False

    body = payload.get("body")
    if not isinstance(body, dict):
        return True

    ex = (exchange or "").strip().lower()
    code = body.get("code")
    if ex == "kucoin" and code is not None and str(code) != "200000":
        return False
    if ex == "bitget" and code is not None and str(code) not in ("00000", "0"):
        return False
    if ex == "paribu":
        if body.get("success") is False:
            return False
        status_val = body.get("status")
        if isinstance(status_val, str) and status_val.strip().lower() in ("error", "fail", "failed"):
            return False
    return True


def _extract_error_detail(result: dict) -> str:
    payload = dict(result or {})
    body = payload.get("body")
    if isinstance(body, dict):
        for key in ("message", "msg", "detail", "error", "label", "code", "text"):
            val = body.get(key)
            if val not in (None, ""):
                return str(val)[:240]
        has_non_empty = any(v not in (None, "", [], {}) for v in body.values())
        if not has_non_empty:
            return ""
        raw = str(body)
        if raw:
            return raw[:240]
    elif body not in (None, ""):
        return str(body)[:240]
    status = payload.get("status")
    if status is not None:
        return f"HTTP {status}"
    return ""


def _position_key(exchange: str, symbol_sent: str) -> str:
    return f"{(exchange or '').strip().lower()}:{(symbol_sent or '').strip().upper()}"


def _record_order_event(exchange: str, symbol_sent: str, side: str, mode: str, result: dict):
    global ORDER_SEQ

    s = (side or "").strip().lower()
    if s not in ("buy", "sell"):
        return

    payload = dict(result or {})
    dry_run = bool(payload.get("dry_run", False))
    metrics = _extract_order_metrics(exchange, payload)
    success = (not dry_run) and _is_order_success(exchange, payload)
    at_iso = now_iso_utc()
    at_ms = now_wall_ms()
    if metrics.get("exchange_time_ms"):
        try:
            at_iso = datetime.datetime.fromtimestamp(
                metrics["exchange_time_ms"] / 1000.0,
                tz=datetime.timezone.utc,
            ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            pass

    with STATE_LOCK:
        ORDER_SEQ += 1
        row = {
            "id": ORDER_SEQ,
            "at": at_iso,
            "at_ms": at_ms,
            "exchange": (exchange or "").strip().lower(),
            "symbol": (symbol_sent or "").strip().upper(),
            "side": s,
            "mode": mode,
            "dry_run": dry_run,
            "success": success,
            "status": payload.get("status"),
            "engine_latency_ms": payload.get("engine_latency_ms"),
            "order_id": metrics.get("order_id") or "",
            "price": metrics.get("price"),
            "base_qty": metrics.get("base_qty"),
            "quote_qty": metrics.get("quote_qty"),
        }
        ORDER_HISTORY.append(row)
        LAST_ORDER_SIGNAL.clear()
        LAST_ORDER_SIGNAL.update(
            {
                "id": row["id"],
                "at": row["at"],
                "exchange": row["exchange"],
                "symbol": row["symbol"],
                "side": row["side"],
                "success": row["success"],
                "dry_run": row["dry_run"],
            }
        )

        key = _position_key(row["exchange"], row["symbol"])

        if row["success"] and row["side"] == "buy":
            base_qty = row.get("base_qty")
            quote_qty = row.get("quote_qty")
            price = row.get("price")
            if base_qty and quote_qty and (not price or price <= 0):
                price = quote_qty / base_qty
            if base_qty and price and (not quote_qty or quote_qty <= 0):
                quote_qty = base_qty * price

            if base_qty and base_qty > 0:
                OPEN_POSITIONS[key] = {
                    "exchange": row["exchange"],
                    "symbol": row["symbol"],
                    "buy_order_id": row.get("order_id") or "",
                    "buy_time": row["at"],
                    "buy_price": price,
                    "base_qty": base_qty,
                    "quote_qty": quote_qty,
                }

        if row["success"] and row["side"] == "sell":
            open_pos = dict(OPEN_POSITIONS.get(key) or {})
            if open_pos:
                buy_base = _to_float(open_pos.get("base_qty")) or 0.0
                buy_quote = _to_float(open_pos.get("quote_qty"))
                buy_price = _to_float(open_pos.get("buy_price"))
                sell_base = _to_float(row.get("base_qty")) or 0.0
                sell_quote = _to_float(row.get("quote_qty"))
                sell_price = _to_float(row.get("price"))

                if buy_base > 0 and buy_quote is None and buy_price and buy_price > 0:
                    buy_quote = buy_base * buy_price
                if sell_base <= 0:
                    sell_base = buy_base
                if sell_quote is None and sell_price and sell_price > 0 and sell_base > 0:
                    sell_quote = sell_base * sell_price

                matched_base = min(buy_base, sell_base) if buy_base > 0 and sell_base > 0 else 0.0
                if matched_base <= 0 and buy_base > 0:
                    matched_base = buy_base
                ratio = (matched_base / buy_base) if buy_base > 0 else 1.0
                buy_quote_used = (buy_quote * ratio) if buy_quote is not None else None
                pnl = (sell_quote - buy_quote_used) if (sell_quote is not None and buy_quote_used is not None) else None

                TRADE_CYCLES.append(
                    {
                        "id": row["id"],
                        "exchange": row["exchange"],
                        "symbol": row["symbol"],
                        "buy_time": open_pos.get("buy_time"),
                        "buy_price": buy_price,
                        "buy_qty": matched_base if matched_base > 0 else None,
                        "buy_quote": buy_quote_used,
                        "sell_time": row["at"],
                        "sell_price": sell_price,
                        "sell_qty": matched_base if matched_base > 0 else None,
                        "sell_quote": sell_quote,
                        "pnl_quote": pnl,
                        "pnl_pct": ((pnl / buy_quote_used) * 100.0) if (pnl is not None and buy_quote_used and buy_quote_used > 0) else None,
                    }
                )

                remaining_base = max(0.0, buy_base - matched_base)
                if remaining_base > 1e-12:
                    remaining_quote = None
                    if buy_quote is not None and buy_quote_used is not None:
                        remaining_quote = max(0.0, buy_quote - buy_quote_used)
                    OPEN_POSITIONS[key] = {
                        "exchange": open_pos.get("exchange", row["exchange"]),
                        "symbol": open_pos.get("symbol", row["symbol"]),
                        "buy_order_id": open_pos.get("buy_order_id", ""),
                        "buy_time": open_pos.get("buy_time", row["at"]),
                        "buy_price": open_pos.get("buy_price"),
                        "base_qty": remaining_base,
                        "quote_qty": remaining_quote,
                    }
                else:
                    OPEN_POSITIONS.pop(key, None)


def set_last_execution(exchange: str, symbol_input: str, symbol_sent: str, mode: str, result: dict, side: Optional[str] = None):
    payload = dict(result or {})
    data = {
        "at": now_iso_utc(),
        "exchange": exchange,
        "symbol_input": symbol_input,
        "symbol_sent": symbol_sent,
        "mode": mode,
        "dry_run": bool(payload.get("dry_run", False)),
        "order_status": payload.get("status"),
        "attempt": payload.get("attempt"),
        "engine_latency_ms": payload.get("engine_latency_ms"),
        "trigger_to_result_ms": payload.get("trigger_to_result_ms"),
    }
    with STATE_LOCK:
        LAST_EXECUTION.clear()
        LAST_EXECUTION.update(data)
        ms = data.get("engine_latency_ms")
        if not data.get("dry_run") and isinstance(ms, int) and ms > 0:
            ORDER_LATENCY_BY_EXCHANGE[exchange] = {
                "at": data.get("at"),
                "engine_latency_ms": ms,
                "symbol_sent": data.get("symbol_sent"),
                "mode": mode,
                "order_status": data.get("order_status"),
            }

    side_guess = (side or "").strip().lower()
    if not side_guess:
        if mode == "sell":
            side_guess = "sell"
        elif mode in ("immediate", "ws_trigger", "kill_force_buy"):
            side_guess = "buy"
    _record_order_event(exchange, symbol_sent or symbol_input, side_guess, mode, payload)


def safe_json_response(resp):
    try:
        return resp.json()
    except Exception:
        return {"text": (resp.text or "")[:500]}


def has_auth_config(exchange: str) -> bool:
    ex = exchange.strip().lower()
    if ex == "gate":
        return bool(engine.adapters["gate"].key and engine.adapters["gate"].secret)
    if ex == "binance":
        return bool(engine.adapters["binance"].key and engine.adapters["binance"].secret)
    if ex == "mexc":
        return bool(engine.adapters["mexc"].key and engine.adapters["mexc"].secret)
    if ex == "kucoin":
        return bool(
            engine.adapters["kucoin"].key
            and engine.adapters["kucoin"].secret
            and engine.adapters["kucoin"].passphrase
        )
    if ex == "bitget":
        return bool(
            engine.adapters["bitget"].key
            and engine.adapters["bitget"].secret
            and engine.adapters["bitget"].passphrase
        )
    if ex == "okex":
        row = PUBLIC_AUTH_CONFIG.get("okex", {})
        return bool(row.get("key") and row.get("secret") and row.get("passphrase"))
    if ex == "bybit":
        row = PUBLIC_AUTH_CONFIG.get("bybit", {})
        return bool(row.get("key") and row.get("secret"))
    if ex == "btcturk":
        row = PUBLIC_AUTH_CONFIG.get("btcturk", {})
        return bool(row.get("key") and row.get("secret"))
    if ex == "paribu":
        return bool(engine.adapters["paribu"].key and engine.adapters["paribu"].secret)
    return False


def run_exchange_network_test(exchange: str) -> dict:
    ex = exchange.strip().lower()
    url = build_exchange_probe_url(ex)
    if not url:
        return {
            "network_ok": False,
            "network_status": None,
            "network_latency_ms": None,
            "network_error": "base_url_missing",
        }
    t0 = time.perf_counter()
    try:
        resp = requests.get(url, timeout=6, allow_redirects=True)
        latency_ms = int((time.perf_counter() - t0) * 1000)
        network_ok = 200 <= resp.status_code < 400
        return {
            "network_ok": network_ok,
            "network_status": resp.status_code,
            "network_latency_ms": latency_ms,
            "network_error": None,
        }
    except Exception as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return {
            "network_ok": False,
            "network_status": None,
            "network_latency_ms": latency_ms,
            "network_error": f"{type(e).__name__}: {e}",
        }


def run_exchange_auth_test(exchange: str) -> dict:
    ex = exchange.strip().lower()
    if not has_auth_config(ex):
        return {"auth_ok": None, "auth_status": None, "auth_latency_ms": None, "auth_error": "API key missing"}

    try:
        if ex == "gate":
            g = engine.adapters["gate"]
            path = "/spot/accounts"
            headers = g._sign_headers("GET", "/api/v4" + path, "", "")
            t0 = time.perf_counter()
            resp = g.session.get(g.base + path, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "binance":
            b = engine.adapters["binance"]
            signed = b._signed({})
            t0 = time.perf_counter()
            resp = b.session.get(b.base + "/api/v3/account", headers=signed["headers"], params=signed["qs"], timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "mexc":
            m = engine.adapters["mexc"]
            signed = m._signed({})
            t0 = time.perf_counter()
            resp = m.session.get(m.base + "/api/v3/account", headers=signed["headers"], params=signed["qs"], timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "kucoin":
            k = engine.adapters["kucoin"]
            path = "/api/v1/accounts?type=trade"
            headers = k._headers("GET", path, "")
            t0 = time.perf_counter()
            resp = k.session.get(k.base + path, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "bitget":
            b = engine.adapters["bitget"]
            path = "/api/v2/spot/account/assets"
            headers = b._headers("GET", path, "")
            t0 = time.perf_counter()
            resp = b.session.get(b.base + path, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "okex":
            row = PUBLIC_AUTH_CONFIG["okex"]
            base = get_exchange_base_url("okex")
            path = "/api/v5/account/balance"
            query = "ccy=USDT"
            ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            prehash = f"{ts}GET{path}?{query}"
            sign = base64.b64encode(
                hmac.new((row.get("secret") or "").encode(), prehash.encode(), hashlib.sha256).digest()
            ).decode()
            headers = {
                "OK-ACCESS-KEY": row.get("key") or "",
                "OK-ACCESS-SIGN": sign,
                "OK-ACCESS-TIMESTAMP": ts,
                "OK-ACCESS-PASSPHRASE": row.get("passphrase") or "",
                "Content-Type": "application/json",
            }
            t0 = time.perf_counter()
            resp = requests.get(base + path, params={"ccy": "USDT"}, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "bybit":
            row = PUBLIC_AUTH_CONFIG["bybit"]
            base = get_exchange_base_url("bybit")
            path = "/v5/user/query-api"
            ts = str(int(time.time() * 1000))
            recv_window = "5000"
            prehash = f"{ts}{row.get('key') or ''}{recv_window}"
            sign = hmac.new(
                (row.get("secret") or "").encode(),
                prehash.encode(),
                hashlib.sha256,
            ).hexdigest()
            headers = {
                "X-BAPI-API-KEY": row.get("key") or "",
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-RECV-WINDOW": recv_window,
                "X-BAPI-SIGN": sign,
                "X-BAPI-SIGN-TYPE": "2",
            }
            t0 = time.perf_counter()
            resp = requests.get(base + path, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "btcturk":
            row = PUBLIC_AUTH_CONFIG["btcturk"]
            base = get_exchange_base_url("btcturk")
            path = "/api/v1/users/balances"
            stamp = str(int(time.time() * 1000))
            prehash = f"{row.get('key') or ''}{stamp}"
            sign = base64.b64encode(
                hmac.new((row.get("secret") or "").encode(), prehash.encode(), hashlib.sha256).digest()
            ).decode()
            headers = {
                "X-PCK": row.get("key") or "",
                "X-Stamp": stamp,
                "X-Signature": sign,
            }
            t0 = time.perf_counter()
            resp = requests.get(base + path, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "paribu":
            p = engine.adapters["paribu"]
            out = p._signed_get("/user/assets", timeout_sec=6)
            status = int(out.get("status") or 0)
            body = out.get("body")
            latency = int(out.get("latency_ms") or 0)
            ok = 200 <= status < 300
            if isinstance(body, dict):
                if body.get("success") is False:
                    ok = False
                state = str(body.get("status") or "").strip().lower()
                if state in ("error", "fail", "failed"):
                    ok = False
            err = None if ok else str(body)[:200]
            return {
                "auth_ok": ok,
                "auth_status": status,
                "auth_latency_ms": latency if latency > 0 else None,
                "auth_error": err,
            }
        else:
            return {"auth_ok": None, "auth_status": None, "auth_latency_ms": None, "auth_error": "unsupported exchange"}

        body = safe_json_response(resp)
        ok = 200 <= resp.status_code < 300
        if ex == "okex" and isinstance(body, dict):
            ok = ok and str(body.get("code", "")) in ("0", "")
        if ex == "bybit" and isinstance(body, dict):
            ok = ok and str(body.get("retCode", "")) in ("0", "")
        if ex == "btcturk" and isinstance(body, dict):
            success_flag = body.get("success")
            if success_flag is None:
                success_flag = body.get("successful")
            if success_flag is not None:
                ok = ok and bool(success_flag)
        err = None if ok else str(body)[:200]
        return {
            "auth_ok": ok,
            "auth_status": resp.status_code,
            "auth_latency_ms": latency,
            "auth_error": err,
        }
    except Exception as e:
        return {"auth_ok": False, "auth_status": None, "auth_latency_ms": None, "auth_error": f"{type(e).__name__}: {e}"}


def cancel_non_gate_task():
    global NON_GATE_TASK
    task = NON_GATE_TASK
    NON_GATE_TASK = None
    if task is not None and MAIN_LOOP is not None and not task.done():
        MAIN_LOOP.call_soon_threadsafe(task.cancel)


def schedule_non_gate_trigger(exchange: str, symbol: str, spend: str):
    if MAIN_LOOP is None:
        raise RuntimeError("executor loop not ready")

    cancel_non_gate_task()

    def _start():
        global NON_GATE_TASK
        NON_GATE_TASK = asyncio.create_task(non_gate_ws_trigger_once(exchange, symbol, spend))

    MAIN_LOOP.call_soon_threadsafe(_start)


async def wait_binance_first_tick(symbol: str):
    ws_symbol = symbol.replace("_", "").replace("-", "").lower()
    url = f"{BINANCE_WS_URL}/{ws_symbol}@ticker"
    async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2 ** 22) as ws:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=WS_TRIGGER_TIMEOUT_SEC)
            if isinstance(raw, (bytes, bytearray)):
                continue
            msg = json.loads(raw)
            if str(msg.get("s", "")).upper() == ws_symbol.upper():
                return msg


async def wait_mexc_first_tick(symbol: str):
    ws_symbol = symbol.replace("_", "").replace("-", "").upper()
    channels = [
        f"spot@public.deals.v3.api@{ws_symbol}",
        f"spot@public.miniTicker.v3.api@{ws_symbol}",
    ]
    last_err = None

    for url in MEXC_WS_URLS:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2 ** 22) as ws:
                for ch in channels:
                    sub = {"method": "SUBSCRIPTION", "params": [ch]}
                    await ws.send(json.dumps(sub))

                started = time.monotonic()
                while time.monotonic() - started < WS_TRIGGER_TIMEOUT_SEC:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    if isinstance(raw, (bytes, bytearray)):
                        return {"binary": True, "symbol": ws_symbol}

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if isinstance(msg, dict) and msg.get("code") == 0 and str(msg.get("msg", "")).lower() == "success":
                        continue

                    blob = json.dumps(msg, ensure_ascii=False).upper()
                    if ws_symbol not in blob:
                        continue

                    if any(k in msg for k in ("d", "data", "c", "s")) or "DEALS" in blob or "TICKER" in blob:
                        return msg
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"MEXC ws trigger failed: {last_err}")


def kucoin_public_ws_creds():
    base = os.getenv("KUCOIN_BASE", "https://api.kucoin.com").rstrip("/")
    r = requests.post(base + "/api/v1/bullet-public", timeout=8)
    r.raise_for_status()
    body = r.json()
    data = body.get("data") or {}
    token = data.get("token")
    servers = data.get("instanceServers") or []
    endpoint = servers[0].get("endpoint") if servers else None
    if not token or not endpoint:
        raise RuntimeError(f"kucoin bullet-public invalid: {body}")
    return endpoint, token


async def wait_kucoin_first_tick(symbol: str):
    topic_symbol = symbol.upper().replace("_", "-")
    endpoint, token = kucoin_public_ws_creds()
    connect_id = uuid.uuid4().hex
    url = f"{endpoint}?token={token}&connectId={connect_id}"

    async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2 ** 22) as ws:
        sub = {
            "id": str(int(time.time() * 1000)),
            "type": "subscribe",
            "topic": f"/market/ticker:{topic_symbol}",
            "privateChannel": False,
            "response": True,
        }
        await ws.send(json.dumps(sub))

        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=WS_TRIGGER_TIMEOUT_SEC)
            if isinstance(raw, (bytes, bytearray)):
                continue
            msg = json.loads(raw)
            if msg.get("type") != "message":
                continue
            topic = str(msg.get("topic", ""))
            if topic.endswith(topic_symbol):
                return msg


async def wait_bitget_first_tick(symbol: str):
    ws_symbol = symbol.replace("_", "").replace("-", "").upper()
    sub = {
        "op": "subscribe",
        "args": [{"instType": "SPOT", "channel": "ticker", "instId": ws_symbol}],
    }

    async with websockets.connect(BITGET_WS_URL, ping_interval=20, ping_timeout=20, max_size=2 ** 22) as ws:
        await ws.send(json.dumps(sub))
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=WS_TRIGGER_TIMEOUT_SEC)
            if isinstance(raw, (bytes, bytearray)):
                continue
            msg = json.loads(raw)

            if msg.get("event"):
                continue

            arg = msg.get("arg") or {}
            channel = str(arg.get("channel", "")).lower()
            inst = str(arg.get("instId", "")).upper()
            if channel == "ticker" and inst == ws_symbol and msg.get("data"):
                return msg


async def wait_first_tick(exchange: str, symbol: str):
    if exchange == "binance":
        return await wait_binance_first_tick(symbol)
    if exchange == "mexc":
        return await wait_mexc_first_tick(symbol)
    if exchange == "kucoin":
        return await wait_kucoin_first_tick(symbol)
    if exchange == "bitget":
        return await wait_bitget_first_tick(symbol)
    raise RuntimeError(f"ws trigger unsupported: {exchange}")


def make_order_telemetry_cb(exchange: str, symbol: str) -> Callable[..., None]:
    def _cb(event: str, **fields):
        emit_event(event, exchange=exchange, symbol=symbol, **fields)

    return _cb


def emit_target_reached(snap: dict):
    target_ms = snap.get("target_epoch_ms")
    now_ms = now_wall_ms()
    if isinstance(target_ms, int):
        emit_event(
            "t_target_reached",
            exchange=snap.get("exchange"),
            symbol=snap.get("symbol"),
            target_epoch_ms=target_ms,
            reached_wall_ms=now_ms,
            delta_ms=now_ms - target_ms,
        )
    else:
        emit_event(
            "t_target_reached",
            exchange=snap.get("exchange"),
            symbol=snap.get("symbol"),
            target_epoch_ms=None,
            reached_wall_ms=now_ms,
            delta_ms=None,
        )


def prepare_arm_context(ex: str, norm_symbol: str, spend: str, target_iso: Optional[str]):
    prepared_plan = None

    if ex == "gate":
        set_arm_state(phase="arm_preparing")
        sync_info = engine.adapters["gate"].sync_server_offset(samples=GATE_OFFSET_SAMPLES, per_request_timeout=1.2)
        emit_event(
            "time_sync_done",
            exchange="gate",
            phase="arm",
            offset_ms=sync_info.get("offset_ms"),
            rtt_median_ms=sync_info.get("rtt_median_ms"),
            samples=sync_info.get("samples"),
        )

        prepared_plan = engine.adapters["gate"].build_market_buy_plan(norm_symbol, spend)

        set_arm_state(phase="warmup")
        warm = engine.adapters["gate"].warmup_connection(sync_samples=GATE_WARMUP_SYNC_SAMPLES, timeout_sec=1.2)
        emit_event(
            "warmup_done",
            exchange="gate",
            ok=warm.get("ok"),
            status=warm.get("status"),
            latency_ms=warm.get("latency_ms"),
        )
        if isinstance(warm.get("sync"), dict):
            s = warm.get("sync")
            emit_event(
                "time_sync_done",
                exchange="gate",
                phase="warmup",
                offset_ms=s.get("offset_ms"),
                rtt_median_ms=s.get("rtt_median_ms"),
                samples=s.get("samples"),
            )
    else:
        set_arm_state(phase="warmup")
        warm = engine.warmup_exchange(ex, timeout_sec=1.2)
        emit_event(
            "warmup_done",
            exchange=ex,
            ok=warm.get("ok"),
            status=warm.get("status"),
            latency_ms=warm.get("latency_ms"),
        )

    target_epoch_ms = parse_iso_to_epoch_ms(target_iso)
    plan_summary = build_plan_summary(ex, norm_symbol, spend, prepared_plan, target_iso, target_epoch_ms)
    arm_hash = current_arm_config_hash()

    lock_payload = {
        "version": 1,
        "created_at": now_iso_utc(),
        "config_hash": arm_hash,
        "plan": plan_summary,
    }
    write_arm_lock(lock_payload)

    return prepared_plan, arm_hash, target_epoch_ms


def run_market_presence_analysis(ex: str, norm_symbol: str, payload: ArmPayload) -> dict:
    listing_title = (payload.listing_title or "").strip() or None
    listing_url = (payload.listing_url or "").strip() or None
    contract_hint = (payload.contract_hint or "").strip() or None
    should_scan = (ex == "gate") or bool(listing_title or listing_url or contract_hint)

    if not should_scan:
        result = {
            "checked": False,
            "reason": "skipped_no_listing_context",
            "target_exchange": ex,
            "symbol_input": norm_symbol,
            "at": now_iso_utc(),
        }
        set_last_market_presence(result)
        set_arm_state(market_presence=result)
        return result

    set_arm_state(phase="presence_scan")
    emit_event("presence_scan_start", exchange=ex, symbol=norm_symbol)
    t0 = time.perf_counter()
    try:
        out = MARKET_PRESENCE_RESOLVER.analyze(
            target_exchange=ex,
            symbol=norm_symbol,
            listing_title=listing_title,
            listing_url=listing_url,
            contract_hint=contract_hint,
        )
        scan_ms = int((time.perf_counter() - t0) * 1000)
        out = dict(out or {})
        out["checked"] = True
        out["scan_ms"] = scan_ms
        out["at"] = now_iso_utc()
        out["warning_symbol_only"] = str(out.get("method") or "") == "symbol_only"
        out["warning_ambiguous"] = bool(out.get("ambiguous"))
        emit_event(
            "presence_scan_done",
            exchange=ex,
            symbol=norm_symbol,
            method=out.get("method"),
            found=bool(out.get("found_on_other_exchanges")),
            rows=len(out.get("rows") or []),
            ambiguous=bool(out.get("ambiguous")),
            scan_ms=scan_ms,
        )
        set_last_market_presence(out)
        set_arm_state(market_presence=out)
        return out
    except Exception as e:
        scan_ms = int((time.perf_counter() - t0) * 1000)
        fail = {
            "checked": True,
            "error": f"{type(e).__name__}: {e}",
            "target_exchange": ex,
            "symbol_input": norm_symbol,
            "method": "error",
            "found_on_other_exchanges": False,
            "rows": [],
            "searched_exchanges": list(SEARCHED_EXCHANGES),
            "explain": "Cross-exchange scan failed during ARM.",
            "scan_ms": scan_ms,
            "at": now_iso_utc(),
            "warning_symbol_only": False,
            "warning_ambiguous": False,
        }
        emit_event("presence_scan_error", exchange=ex, symbol=norm_symbol, error=fail["error"], scan_ms=scan_ms)
        set_last_market_presence(fail)
        set_arm_state(market_presence=fail)
        return fail


async def non_gate_ws_trigger_once(exchange: str, symbol: str, spend: str):
    try:
        set_arm_state(phase="ws_connecting")
        _ = await wait_first_tick(exchange, symbol)

        snap = get_arm_snapshot()
        if not (snap.get("armed") and snap.get("exchange") == exchange and snap.get("symbol") == symbol):
            return

        ok, reason, _ = validate_arm_lock(exchange, symbol, spend)
        if not ok:
            emit_event("order_fail_final", exchange=exchange, symbol=symbol, reason=reason, attempts=0)
            log(f"{exchange.upper()}_WS: lock validation failed ({reason})")
            return

        set_arm_state(phase="exec")
        emit_target_reached(snap)

        t0 = time.perf_counter()
        out = engine.market_buy_fast(
            exchange,
            symbol,
            spend,
            telemetry_cb=make_order_telemetry_cb(exchange, symbol),
        )
        out = dict(out or {})
        out.setdefault("_requested_quote_qty", spend)
        out["trigger_to_result_ms"] = int((time.perf_counter() - t0) * 1000)
        set_last_execution(exchange, symbol, symbol, "ws_trigger", out)
        emit_event("exec_complete", exchange=exchange, symbol=symbol, status=out.get("status"), attempt=out.get("attempt"))
        log(f"{exchange.upper()}_WS_BUY_RESULT: {out}")
    except asyncio.CancelledError:
        emit_event("ws_cancelled", exchange=exchange, symbol=symbol)
        log(f"{exchange.upper()}_WS: cancelled")
        raise
    except Exception as e:
        emit_event("ws_error", exchange=exchange, symbol=symbol, error=f"{type(e).__name__}: {e}")
        log(f"{exchange.upper()}_WS: error {type(e).__name__}: {e}")
    finally:
        snap = get_arm_snapshot()
        if snap.get("exchange") == exchange and snap.get("symbol") == symbol:
            clear_arm_state()


@app.get("/health")
def health():
    return {"ok": True, "dry_run": engine.dry_run}


@app.get("/status")
def status():
    snap = get_arm_snapshot()
    with STATE_LOCK:
        last_exec = dict(LAST_EXECUTION)
        order_latency = dict(ORDER_LATENCY_BY_EXCHANGE)
        market_presence = dict(LAST_MARKET_PRESENCE)
        order_history = list(ORDER_HISTORY)[-40:]
        trade_cycles = list(TRADE_CYCLES)[-40:]
        open_positions = list(OPEN_POSITIONS.values())
        last_order_signal = dict(LAST_ORDER_SIGNAL)
    return {
        "dry_run": engine.dry_run,
        "gate": {
            "armed": snap.get("armed"),
            "exchange": snap.get("exchange"),
            "symbol": snap.get("symbol"),
            "spend_usdt": snap.get("spend_usdt"),
            "phase": snap.get("phase"),
            "target_iso": snap.get("target_iso"),
        },
        "arm_state": snap,
        "last_execution": last_exec,
        "order_latency": order_latency,
        "market_presence": market_presence,
        "order_history": order_history,
        "trade_cycles": trade_cycles,
        "open_positions": open_positions,
        "last_order_signal": last_order_signal,
    }


@app.post("/disarm")
def disarm(x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    cancel_non_gate_task()
    clear_arm_state()
    emit_event("disarm", reason="manual")
    log("DISARM")
    return {"ok": True}


@app.post("/kill")
def kill(x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    cancel_non_gate_task()

    forced_buy = None
    snap = get_arm_snapshot()
    if snap.get("armed") and snap.get("exchange") and snap.get("symbol") and snap.get("spend_usdt"):
        ex = str(snap["exchange"]).strip().lower()
        symbol = str(snap["symbol"]).strip()
        spend = str(snap["spend_usdt"]).strip()
        set_arm_state(phase="force_buy")
        emit_event("force_buy", exchange=ex, symbol=symbol, spend_usdt=spend)
        log(f"KILL: force market BUY ex={ex} symbol={symbol} spend={spend}")
        try:
            plan = ARM_PREPARED_PLAN if (ex == "gate" and isinstance(ARM_PREPARED_PLAN, dict)) else None
            forced_buy = engine.market_buy_fast(
                ex,
                symbol,
                spend,
                prepared_plan=plan,
                telemetry_cb=make_order_telemetry_cb(ex, symbol),
            )
            forced_buy = dict(forced_buy or {})
            forced_buy.setdefault("_requested_quote_qty", spend)
            set_last_execution(ex, symbol, symbol, "kill_force_buy", forced_buy)
            log(f"KILL_FORCE_BUY_RESULT: {forced_buy}")
        except Exception as e:
            forced_buy = {"error": f"{type(e).__name__}: {e}"}
            emit_event("force_buy_error", exchange=ex, symbol=symbol, error=forced_buy["error"])
            log(f"KILL_FORCE_BUY_ERROR: {forced_buy['error']}")

    clear_arm_state()
    log("KILL: CLOSED")
    return {"ok": True, "forced_buy": forced_buy}


@app.post("/arm")
def arm(payload: ArmPayload, x_pilot_token: Optional[str] = Header(default=None)):
    global ARM_PREPARED_PLAN

    check_token(x_pilot_token)
    ex = payload.exchange.strip().lower()
    symbol = payload.symbol.strip()
    spend = payload.spend_usdt.strip()
    target_iso = (payload.target_iso or "").strip() or None

    if ex not in engine.adapters:
        raise HTTPException(status_code=400, detail=f"unknown exchange: {ex}")

    if get_arm_snapshot().get("armed"):
        raise HTTPException(status_code=409, detail="another arm is already active")

    norm_symbol = engine.adapters[ex].normalize_symbol(symbol)

    try:
        market_presence = run_market_presence_analysis(ex, norm_symbol, payload)
        prepared_plan, arm_hash, target_epoch_ms = prepare_arm_context(ex, norm_symbol, spend, target_iso)
        ARM_PREPARED_PLAN = prepared_plan
    except HTTPException:
        clear_arm_state()
        raise
    except Exception as e:
        clear_arm_state()
        raise HTTPException(status_code=500, detail=f"arm_prepare_failed: {type(e).__name__}: {e}")

    set_arm_state(
        armed=True,
        exchange=ex,
        symbol=norm_symbol,
        spend_usdt=spend,
        phase="armed",
        target_iso=target_iso,
        target_epoch_ms=target_epoch_ms,
        arm_hash=arm_hash,
        market_presence=market_presence,
    )
    emit_event("arm_done", exchange=ex, symbol=norm_symbol, spend_usdt=spend, target_iso=target_iso)

    if ex in WS_SUPPORTED:
        if ex == "gate":
            log(f"ARM(GATE): wait WS ticker symbol={norm_symbol} spend_usdt={spend} DRY_RUN={engine.dry_run}")
        else:
            schedule_non_gate_trigger(ex, norm_symbol, spend)
            log(f"ARM({ex.upper()}): wait WS ticker symbol={norm_symbol} spend_usdt={spend} DRY_RUN={engine.dry_run}")

        return {
            "ok": True,
            "mode": "ws_trigger",
            "exchange": ex,
            "symbol": norm_symbol,
            "spend_usdt": spend,
            "arm_hash": arm_hash,
            "market_presence": market_presence,
        }

    # Fallback immediate mode.
    log(f"ARM(IMMEDIATE): ex={ex} symbol={norm_symbol} spend={spend} DRY_RUN={engine.dry_run}")
    out = engine.market_buy_fast(ex, norm_symbol, spend, telemetry_cb=make_order_telemetry_cb(ex, norm_symbol))
    out = dict(out or {})
    out.setdefault("_requested_quote_qty", spend)
    set_last_execution(ex, symbol, norm_symbol, "immediate", out)
    clear_arm_state()
    return {"ok": True, "mode": "immediate", "result": out, "market_presence": market_presence}


@app.post("/sell")
def sell(payload: SellPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    ex = payload.exchange.strip().lower()
    symbol = payload.symbol.strip()
    qty = payload.qty.strip()
    log(f"SELL: ex={ex} symbol={symbol} qty={qty} DRY_RUN={engine.dry_run}")
    t0 = time.perf_counter()
    result = engine.market_sell(ex, symbol, qty)
    result = dict(result or {})
    if not isinstance(result.get("engine_latency_ms"), int):
        if isinstance(result.get("latency_ms"), int):
            result["engine_latency_ms"] = int(result.get("latency_ms"))
        else:
            result["engine_latency_ms"] = int((time.perf_counter() - t0) * 1000)
    result.setdefault("_requested_base_qty", qty)
    if ex == "paribu" and int(result.get("status") or 0) == 401 and not result.get("error_hint"):
        result["error_hint"] = "Paribu API key is reachable, but order permission is denied (401)."
    norm_symbol = engine.adapters.get(ex).normalize_symbol(symbol) if ex in engine.adapters else symbol
    set_last_execution(ex, symbol, norm_symbol, "sell", result)
    return {"ok": True, "result": result}


@app.post("/dry-run")
def set_dry_run(payload: DryRunPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    if get_arm_snapshot().get("armed"):
        raise HTTPException(status_code=409, detail="cannot change mode while armed")
    engine.apply_runtime_config({}, payload.enabled)
    return {"ok": True, "dry_run": engine.dry_run}


@app.post("/admin/config")
def set_admin_config(payload: AdminConfigPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    if get_arm_snapshot().get("armed"):
        raise HTTPException(status_code=409, detail="cannot change config while armed")
    values = dict(payload.values or {})
    engine.apply_runtime_config(values, payload.dry_run)
    set_public_exchange_base("okex", values.get("OKEX_BASE", ""))
    set_public_exchange_base("bybit", values.get("BYBIT_BASE", ""))
    set_public_exchange_base("btcturk", values.get("BTCTURK_BASE", ""))
    set_public_exchange_base("paribu", values.get("PARIBU_BASE", ""))
    set_public_exchange_auth(
        "okex",
        key=values.get("OKEX_KEY"),
        secret=values.get("OKEX_SECRET"),
        passphrase=values.get("OKEX_PASSPHRASE"),
    )
    set_public_exchange_auth(
        "bybit",
        key=values.get("BYBIT_KEY"),
        secret=values.get("BYBIT_SECRET"),
    )
    set_public_exchange_auth(
        "btcturk",
        key=values.get("BTCTURK_KEY"),
        secret=values.get("BTCTURK_SECRET"),
    )
    set_public_exchange_auth(
        "paribu",
        key=values.get("PARIBU_KEY"),
        secret=values.get("PARIBU_SECRET"),
    )
    return {"ok": True, "dry_run": engine.dry_run}


@app.post("/exchange-test")
def exchange_test(payload: ExchangeTestPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    target = (payload.exchange or "").strip().lower()
    names = [target] if target in ALL_TEST_EXCHANGES else list(ALL_TEST_EXCHANGES)

    results = {}
    for ex in names:
        row = {"exchange": ex, "base_url": get_exchange_base_url(ex)}
        row.update(run_exchange_network_test(ex))
        row.update(run_exchange_auth_test(ex))
        results[ex] = row

    return {"ok": True, "at": now_iso_utc(), "results": results}


@app.post("/order-latency")
def order_latency_probe(payload: LatencyProbePayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)

    ex = payload.exchange.strip().lower()
    symbol_in = (payload.symbol or "").strip()
    spend = (payload.spend_usdt or "5").strip()
    auto_symbol = False

    if ex in engine.adapters:
        if not symbol_in:
            symbol_in = default_probe_symbol(ex)
            auto_symbol = True
        symbol_sent = engine.adapters[ex].normalize_symbol(symbol_in)
        result = engine.probe_order_latency(ex, symbol_sent, spend)
        result = dict(result or {})
        result["attempt"] = 1
        ms_val = result.get("engine_latency_ms")
        if isinstance(ms_val, (int, float)) and ms_val > 0:
            result["engine_latency_ms"] = int(ms_val)
        else:
            result["engine_latency_ms"] = None
        result["auto_symbol"] = auto_symbol
    elif ex in PUBLIC_LISTING_EXCHANGES:
        # Real route probe for listing-only exchanges.
        # Priority:
        # 1) signed private route RTT (if API keys exist)
        # 2) public API probe RTT (fallback when keys are absent/invalid)
        auth_probe = run_exchange_auth_test(ex)
        auth_ok = auth_probe.get("auth_ok")
        auth_status = auth_probe.get("auth_status")
        auth_latency = auth_probe.get("auth_latency_ms")
        auth_error = str(auth_probe.get("auth_error") or "").strip()
        if auth_ok is True and isinstance(auth_latency, int) and auth_latency > 0:
            auto_symbol = True
            symbol_sent = "private-auth-probe"
            result = {
                "status": auth_status,
                "body": {"message": "private route ok"},
                "engine_latency_ms": auth_latency,
                "probe_type": "private_auth_route_probe",
                "safe_no_trade": True,
                "attempt": 1,
                "auto_symbol": True,
            }
        else:
            net_probe = run_exchange_network_test(ex)
            net_ok = bool(net_probe.get("network_ok"))
            net_status = net_probe.get("network_status")
            net_latency = net_probe.get("network_latency_ms")
            net_error = str(net_probe.get("network_error") or "").strip()
            if not net_ok or not isinstance(net_latency, int) or net_latency <= 0:
                detail = auth_error or net_error or f"http_{net_status}" if net_status is not None else "probe_failed"
                raise HTTPException(status_code=502, detail=f"{ex}: route probe failed ({detail})")
            auto_symbol = True
            symbol_sent = "public-api-probe"
            result = {
                "status": net_status,
                "body": {"message": "public route ok"},
                "engine_latency_ms": net_latency,
                "probe_type": "public_api_route_probe",
                "safe_no_trade": True,
                "attempt": 1,
                "auto_symbol": True,
            }
    else:
        raise HTTPException(status_code=400, detail=f"unknown exchange: {ex}")

    set_last_execution(ex, symbol_in or ex, symbol_sent, "latency_probe", result)
    return {
        "ok": True,
        "probe": {
            "exchange": ex,
            "symbol_input": symbol_in or "",
            "symbol_sent": symbol_sent,
            "engine_latency_ms": result.get("engine_latency_ms"),
            "status": result.get("status"),
            "probe_type": result.get("probe_type"),
            "safe_no_trade": bool(result.get("safe_no_trade", True)),
            "auto_symbol": auto_symbol,
        },
        "result": result,
    }


@app.post("/balance-available")
def balance_available(payload: BalanceAvailablePayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    ex = (payload.exchange or "").strip().lower()
    if ex not in engine.adapters:
        raise HTTPException(status_code=400, detail=f"unknown exchange: {ex}")

    norm_symbol = engine.adapters[ex].normalize_symbol(payload.symbol or "")
    base, quote = _symbol_base_quote(ex, norm_symbol)
    side = str(payload.side or "buy").strip().lower()
    if side not in ("buy", "sell"):
        side = "buy"
    target_asset = quote if side == "buy" else base
    pct = float(payload.percent or 100.0)
    if pct <= 0:
        raise HTTPException(status_code=400, detail="percent must be greater than 0")
    if pct > 100:
        pct = 100.0

    fetched = _fetch_available_balance(ex, target_asset)
    if not fetched.get("ok"):
        detail = fetched.get("error") or f"balance_fetch_failed ({fetched.get('status')})"
        body = fetched.get("body")
        if isinstance(body, dict):
            body_msg = body.get("message") or body.get("msg") or body.get("detail") or body.get("error")
            if body_msg:
                detail = f"{detail}: {body_msg}"
        raise HTTPException(status_code=502, detail=str(detail)[:280])

    available = _to_float(fetched.get("available"))
    if available is None:
        raise HTTPException(status_code=404, detail=f"{target_asset} balance not found for {ex}")

    computed = max(0.0, available * (pct / 100.0))
    computed_text = f"{computed:.12f}".rstrip("0").rstrip(".")
    if not computed_text:
        computed_text = "0"

    return {
        "ok": True,
        "exchange": ex,
        "symbol": norm_symbol,
        "base": base,
        "quote": quote,
        "side": side,
        "target_asset": target_asset,
        "available": available,
        "percent": pct,
        "computed": computed,
        "computed_text": computed_text,
        "status": fetched.get("status"),
    }


@app.post("/wallet-balances")
def wallet_balances(payload: WalletBalancesPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    target = str(payload.exchange or "").strip().lower()
    if target:
        if target not in engine.adapters:
            raise HTTPException(status_code=400, detail=f"unknown exchange: {target}")
        names = [target]
    else:
        names = list(engine.adapters.keys())

    limit = max(1, min(int(payload.limit or 120), 500))
    non_zero_only = bool(payload.non_zero_only)

    out = {}
    for ex in names:
        fetched = _fetch_balance_payload(ex)
        if not fetched.get("ok"):
            out[ex] = {
                "ok": False,
                "status": fetched.get("status"),
                "latency_ms": fetched.get("latency_ms"),
                "error": fetched.get("error") or _extract_error_detail({"status": fetched.get("status"), "body": fetched.get("body")}),
                "rows": [],
            }
            continue

        rows = _normalize_balance_rows(ex, fetched.get("body"))
        if non_zero_only:
            rows = [r for r in rows if (_to_float(r.get("available")) or 0) > 0 or (_to_float(r.get("locked")) or 0) > 0 or (_to_float(r.get("total")) or 0) > 0]
        rows = rows[:limit]
        out[ex] = {
            "ok": True,
            "status": fetched.get("status"),
            "latency_ms": fetched.get("latency_ms"),
            "count": len(rows),
            "rows": rows,
        }

    return {"ok": True, "at": now_iso_utc(), "results": out}


@app.post("/real-trade-test")
def real_trade_test(payload: RealTradeTestPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    if engine.dry_run:
        raise HTTPException(status_code=409, detail="DRY_RUN is active. Switch to live mode for real trade test.")

    ex = (payload.exchange or "").strip().lower()
    if ex not in engine.adapters:
        raise HTTPException(status_code=400, detail=f"unknown exchange: {ex}")

    symbol_input = (payload.symbol or "").strip()
    if not symbol_input:
        raise HTTPException(status_code=400, detail="symbol is required")

    spend = (payload.spend_usdt or "").strip()
    spend_val = _to_float(spend)
    if spend_val is None or spend_val <= 0:
        raise HTTPException(status_code=400, detail="spend_usdt must be greater than 0")

    auto_sell = bool(payload.auto_sell)
    force_round_trip = bool(payload.round_trip)
    mode = str(payload.round_trip_mode or "buy_then_sell").strip().lower()
    if mode not in ("buy_then_sell", "sell_then_buy"):
        mode = "buy_then_sell"
    run_two_legs = bool(force_round_trip or auto_sell)
    sell_wait_sec = max(0.0, min(float(payload.sell_wait_sec or 0.0), 30.0))
    sell_qty_input = (payload.sell_qty or "").strip()
    norm_symbol = engine.adapters[ex].normalize_symbol(symbol_input)
    executor_received_wall_ms = int(time.time() * 1000)
    executor_received_mono_ns = time.monotonic_ns()

    report = {
        "exchange": ex,
        "symbol_input": symbol_input,
        "symbol_sent": norm_symbol,
        "spend_usdt": spend,
        "auto_sell": auto_sell,
        "round_trip": run_two_legs,
        "mode": mode,
        "started_at": now_iso_utc(),
        "client_click_ms": payload.client_click_ms,
        "panel_received_ms": payload.panel_received_ms,
        "panel_send_ms": payload.panel_send_ms,
        "executor_received_ms": executor_received_wall_ms,
    }

    schedule_mode = str(payload.exec_mode or "now").strip().lower()
    if schedule_mode not in ("now", "scheduled"):
        schedule_mode = "now"
    target_execute_ms = None
    if schedule_mode == "scheduled":
        if isinstance(payload.execute_at_ms, int):
            target_execute_ms = int(payload.execute_at_ms)
        if target_execute_ms is None:
            target_execute_ms = parse_iso_to_epoch_ms(payload.execute_at_iso)
    report["schedule_mode"] = schedule_mode
    report["target_execute_iso"] = (payload.execute_at_iso or "").strip() if schedule_mode == "scheduled" else ""
    report["target_execute_ms"] = target_execute_ms if schedule_mode == "scheduled" else None
    report["schedule_wait_ms"] = 0

    if schedule_mode == "scheduled":
        if not isinstance(target_execute_ms, int):
            raise HTTPException(status_code=400, detail="scheduled mode requires a valid execution time")
        wait_ms = target_execute_ms - now_wall_ms()
        report["schedule_wait_ms"] = max(0, wait_ms)
        if wait_ms > 0:
            emit_event(
                "real_test_schedule_wait",
                exchange=ex,
                symbol=norm_symbol,
                wait_ms=wait_ms,
                target_execute_ms=target_execute_ms,
            )
            coarse_sleep_ms = max(0, wait_ms - 2)
            if coarse_sleep_ms > 0:
                time.sleep(coarse_sleep_ms / 1000.0)
            while now_wall_ms() < target_execute_ms:
                time.sleep(0.0005)

    timing_events = {"buy": [], "sell": []}

    def _record_timing(side: str, event: str, **fields):
        row = {
            "event": event,
            "event_wall_ms": int(time.time() * 1000),
            "event_mono_ns": time.monotonic_ns(),
        }
        row.update(fields or {})
        if side in timing_events:
            timing_events[side].append(row)
        emit_event(event, exchange=ex, symbol=norm_symbol, side=side, **fields)

    def _make_timing_cb(side: str):
        def _cb(event: str, **fields):
            _record_timing(side, event, **fields)

        return _cb

    flow_start_mono_ns = time.monotonic_ns()
    flow_t0 = time.perf_counter()
    if schedule_mode == "scheduled" and isinstance(target_execute_ms, int):
        reached_ms = now_wall_ms()
        emit_event(
            "real_test_target_reached",
            exchange=ex,
            symbol=norm_symbol,
            target_execute_ms=target_execute_ms,
            reached_wall_ms=reached_ms,
            drift_ms=int(reached_ms - target_execute_ms),
        )
    emit_event(
        "real_test_start",
        exchange=ex,
        symbol=norm_symbol,
        spend_usdt=spend,
        auto_sell=auto_sell,
        round_trip=run_two_legs,
        mode=mode,
    )

    def _order_error_hint(order_out: dict) -> str:
        if ex == "paribu" and int((order_out or {}).get("status") or 0) == 401:
            return "Paribu key is reachable, but order authorization failed. Check trade/order permission and key scope."
        return ""

    def _get_open_base_qty_text() -> str:
        with STATE_LOCK:
            pos = dict(OPEN_POSITIONS.get(_position_key(ex, norm_symbol)) or {})
        base_qty = _to_float(pos.get("base_qty"))
        if base_qty and base_qty > 0:
            return f"{base_qty:.12f}".rstrip("0").rstrip(".")
        return ""

    report["buy"] = {"skipped": True, "reason": "not_executed"}
    report["sell"] = {"skipped": True, "reason": "not_executed"}

    if mode == "sell_then_buy":
        # Leg-1: sell base asset first (e.g. USDT -> TL on usdt_tl).
        sell_qty = sell_qty_input or spend
        if not sell_qty:
            sell_qty = _get_open_base_qty_text()

        if not sell_qty:
            report["sell"] = {"skipped": True, "reason": "sell_qty_not_available"}
            report["buy"] = {"skipped": True, "reason": "sell_failed"}
        else:
            sell_t0 = time.perf_counter()
            _record_timing("sell", "order_attempt", attempt_no=1, send_wall_ms=int(time.time() * 1000), send_mono_ns=time.monotonic_ns())
            sell_out = engine.market_sell(ex, norm_symbol, sell_qty, timeout_sec=4.0)
            sell_out = dict(sell_out or {})
            sell_out.setdefault("_requested_base_qty", sell_qty)
            if not isinstance(sell_out.get("engine_latency_ms"), int):
                sell_out["engine_latency_ms"] = int((time.perf_counter() - sell_t0) * 1000)
            _record_timing(
                "sell",
                "order_response",
                attempt_no=1,
                status=int(sell_out.get("status") or 0),
                label=_extract_error_detail(sell_out),
                ack_wall_ms=int(time.time() * 1000),
            )
            set_last_execution(ex, norm_symbol, norm_symbol, "real_test_sell", sell_out, side="sell")

            sell_ok = _is_order_success(ex, sell_out)
            sell_metrics = _extract_order_metrics(ex, sell_out)
            report["sell"] = {
                "skipped": False,
                "ok": sell_ok,
                "status": sell_out.get("status"),
                "latency_ms": sell_out.get("engine_latency_ms"),
                "error": _extract_error_detail(sell_out) if not sell_ok else "",
                "error_hint": _order_error_hint(sell_out) if not sell_ok else "",
                "order_id": sell_metrics.get("order_id"),
                "price": sell_metrics.get("price"),
                "base_qty": sell_metrics.get("base_qty"),
                "quote_qty": sell_metrics.get("quote_qty"),
            }

            if not run_two_legs:
                report["buy"] = {"skipped": True, "reason": "round_trip_disabled"}
            elif not sell_ok:
                report["buy"] = {"skipped": True, "reason": "sell_failed"}
            else:
                if sell_wait_sec > 0:
                    time.sleep(sell_wait_sec)
                buy_spend_val = _to_float(sell_metrics.get("quote_qty"))
                buy_spend_text = (
                    f"{buy_spend_val:.12f}".rstrip("0").rstrip(".")
                    if buy_spend_val and buy_spend_val > 0
                    else spend
                )
                buy_t0 = time.perf_counter()
                buy_out = engine.market_buy_fast(ex, norm_symbol, buy_spend_text, telemetry_cb=_make_timing_cb("buy"))
                buy_out = dict(buy_out or {})
                buy_out.setdefault("_requested_quote_qty", buy_spend_text)
                if not isinstance(buy_out.get("engine_latency_ms"), int):
                    buy_out["engine_latency_ms"] = int((time.perf_counter() - buy_t0) * 1000)
                set_last_execution(ex, symbol_input, norm_symbol, "real_test_buy", buy_out, side="buy")

                buy_ok = _is_order_success(ex, buy_out)
                buy_metrics = _extract_order_metrics(ex, buy_out)
                report["buy"] = {
                    "skipped": False,
                    "ok": buy_ok,
                    "status": buy_out.get("status"),
                    "attempt": buy_out.get("attempt"),
                    "latency_ms": buy_out.get("engine_latency_ms"),
                    "error": _extract_error_detail(buy_out) if not buy_ok else "",
                    "error_hint": _order_error_hint(buy_out) if not buy_ok else "",
                    "order_id": buy_metrics.get("order_id"),
                    "price": buy_metrics.get("price"),
                    "base_qty": buy_metrics.get("base_qty"),
                    "quote_qty": buy_metrics.get("quote_qty"),
                    "buy_spend_quote": buy_spend_text,
                }
    else:
        # Leg-1: buy base asset first (e.g. TL -> USDT on usdt_tl).
        buy_t0 = time.perf_counter()
        buy_out = engine.market_buy_fast(ex, norm_symbol, spend, telemetry_cb=_make_timing_cb("buy"))
        buy_out = dict(buy_out or {})
        buy_out.setdefault("_requested_quote_qty", spend)
        if not isinstance(buy_out.get("engine_latency_ms"), int):
            buy_out["engine_latency_ms"] = int((time.perf_counter() - buy_t0) * 1000)
        set_last_execution(ex, symbol_input, norm_symbol, "real_test_buy", buy_out, side="buy")

        buy_ok = _is_order_success(ex, buy_out)
        buy_metrics = _extract_order_metrics(ex, buy_out)
        report["buy"] = {
            "skipped": False,
            "ok": buy_ok,
            "status": buy_out.get("status"),
            "attempt": buy_out.get("attempt"),
            "latency_ms": buy_out.get("engine_latency_ms"),
            "error": _extract_error_detail(buy_out) if not buy_ok else "",
            "error_hint": _order_error_hint(buy_out) if not buy_ok else "",
            "order_id": buy_metrics.get("order_id"),
            "price": buy_metrics.get("price"),
            "base_qty": buy_metrics.get("base_qty"),
            "quote_qty": buy_metrics.get("quote_qty"),
        }

        if not run_two_legs:
            report["sell"] = {"skipped": True, "reason": "round_trip_disabled"}
        elif not buy_ok:
            report["sell"] = {"skipped": True, "reason": "buy_failed"}
        else:
            if sell_wait_sec > 0:
                time.sleep(sell_wait_sec)
            sell_qty = sell_qty_input or _get_open_base_qty_text()
            if not sell_qty:
                report["sell"] = {"skipped": True, "reason": "sell_qty_not_available"}
            else:
                sell_t0 = time.perf_counter()
                _record_timing("sell", "order_attempt", attempt_no=1, send_wall_ms=int(time.time() * 1000), send_mono_ns=time.monotonic_ns())
                sell_out = engine.market_sell(ex, norm_symbol, sell_qty, timeout_sec=4.0)
                sell_out = dict(sell_out or {})
                sell_out.setdefault("_requested_base_qty", sell_qty)
                if not isinstance(sell_out.get("engine_latency_ms"), int):
                    sell_out["engine_latency_ms"] = int((time.perf_counter() - sell_t0) * 1000)
                _record_timing(
                    "sell",
                    "order_response",
                    attempt_no=1,
                    status=int(sell_out.get("status") or 0),
                    label=_extract_error_detail(sell_out),
                    ack_wall_ms=int(time.time() * 1000),
                )
                set_last_execution(ex, norm_symbol, norm_symbol, "real_test_sell", sell_out, side="sell")
                sell_ok = _is_order_success(ex, sell_out)
                sell_metrics = _extract_order_metrics(ex, sell_out)

                cycle = None
                with STATE_LOCK:
                    for row in reversed(TRADE_CYCLES):
                        if (row.get("exchange") == ex) and (str(row.get("symbol") or "").upper() == norm_symbol.upper()):
                            cycle = dict(row)
                            break

                report["sell"] = {
                    "skipped": False,
                    "ok": sell_ok,
                    "status": sell_out.get("status"),
                    "latency_ms": sell_out.get("engine_latency_ms"),
                    "error": _extract_error_detail(sell_out) if not sell_ok else "",
                    "error_hint": _order_error_hint(sell_out) if not sell_ok else "",
                    "order_id": sell_metrics.get("order_id"),
                    "price": sell_metrics.get("price"),
                    "base_qty": sell_metrics.get("base_qty"),
                    "quote_qty": sell_metrics.get("quote_qty"),
                    "pnl_quote": (cycle or {}).get("pnl_quote"),
                    "pnl_pct": (cycle or {}).get("pnl_pct"),
                }

    report["total_flow_ms"] = int((time.perf_counter() - flow_t0) * 1000)
    report["executor_total_ms"] = report["total_flow_ms"]
    report["timing_events"] = timing_events

    try:
        report["vps_to_exchange_buy_ms"] = int((report.get("buy") or {}).get("latency_ms")) if isinstance((report.get("buy") or {}).get("latency_ms"), (int, float)) else None
    except Exception:
        report["vps_to_exchange_buy_ms"] = None
    try:
        report["vps_to_exchange_sell_ms"] = int((report.get("sell") or {}).get("latency_ms")) if isinstance((report.get("sell") or {}).get("latency_ms"), (int, float)) else None
    except Exception:
        report["vps_to_exchange_sell_ms"] = None

    if isinstance(payload.panel_send_ms, int):
        report["panel_to_executor_ms"] = max(0, executor_received_wall_ms - int(payload.panel_send_ms))
    if isinstance(payload.panel_received_ms, int) and isinstance(payload.client_click_ms, int):
        report["ui_to_vps_ms"] = max(0, int(payload.panel_received_ms) - int(payload.client_click_ms))
    executor_done_wall_ms = int(time.time() * 1000)
    report["executor_done_ms"] = executor_done_wall_ms
    if isinstance(payload.client_click_ms, int):
        report["click_to_done_ms"] = max(0, executor_done_wall_ms - int(payload.client_click_ms))
    report["executor_queue_ms"] = max(0, int((flow_start_mono_ns - executor_received_mono_ns) / 1_000_000))

    first_side = "sell" if mode == "sell_then_buy" else "buy"
    second_side = "buy" if first_side == "sell" else "sell"
    first_leg = dict(report.get(first_side) or {})
    second_leg = dict(report.get(second_side) or {})
    report["first_side"] = first_side
    report["second_side"] = second_side

    first_order_send_ms = None
    for ev in list(timing_events.get(first_side) or []):
        if str(ev.get("event")) == "order_attempt":
            send_ms = ev.get("send_wall_ms")
            if isinstance(send_ms, int):
                first_order_send_ms = send_ms
            else:
                try:
                    first_order_send_ms = int(send_ms)
                except Exception:
                    first_order_send_ms = int(ev.get("event_wall_ms")) if isinstance(ev.get("event_wall_ms"), int) else None
            break
    report["first_order_send_ms"] = first_order_send_ms
    if isinstance(target_execute_ms, int) and isinstance(first_order_send_ms, int):
        report["schedule_drift_ms"] = int(first_order_send_ms - target_execute_ms)
    else:
        report["schedule_drift_ms"] = None

    ok = bool(first_leg.get("ok"))
    if run_two_legs:
        if second_leg.get("skipped"):
            ok = False
        else:
            ok = ok and bool(second_leg.get("ok"))

    emit_event(
        "real_test_done",
        exchange=ex,
        symbol=norm_symbol,
        ok=ok,
        mode=mode,
        round_trip=run_two_legs,
        buy_ok=bool(report["buy"].get("ok")) if isinstance(report.get("buy"), dict) else False,
        sell_ok=bool(report["sell"].get("ok")) if isinstance(report.get("sell"), dict) else False,
        total_flow_ms=report["total_flow_ms"],
    )
    return {"ok": ok, "dry_run": engine.dry_run, "report": report}


@app.get("/diag/feature-check")
def diag_feature_check(x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    lock_payload = read_arm_lock()
    lock_hash = (lock_payload or {}).get("config_hash")
    cur_hash = current_arm_config_hash()
    snap = get_arm_snapshot()
    gate_adapter = engine.adapters["gate"]
    return {
        "ok": True,
        "at": now_iso_utc(),
        "dry_run": engine.dry_run,
        "arm_state": snap,
        "arm_lock": {
            "path": ARM_LOCK_PATH,
            "exists": bool(lock_payload),
            "config_hash_match": bool(lock_hash and lock_hash == cur_hash),
            "config_hash": lock_hash,
            "current_hash": cur_hash,
            "plan": (lock_payload or {}).get("plan"),
        },
        "gate_offset": {
            "offset_ms": getattr(gate_adapter, "offset_ms", 0),
            "rtt_median_ms": getattr(gate_adapter, "offset_rtt_median_ms", None),
            "synced_at_ms": getattr(gate_adapter, "offset_synced_at_ms", None),
        },
        "retry_config": {
            "attempts": engine.sniper_retry_attempts,
            "window_ms": engine.sniper_retry_window_ms,
            "attempt_timeout_ms": engine.sniper_attempt_timeout_ms,
            "jitter_min_ms": engine.sniper_retry_jitter_min_ms,
            "jitter_max_ms": engine.sniper_retry_jitter_max_ms,
        },
        "warm_sessions": {
            name: bool(getattr(adapter, "session", None))
            for name, adapter in engine.adapters.items()
        },
        "telemetry": {
            "recent_count": len(TELEMETRY_RECENT),
            "queue_count": len(TELEMETRY_QUEUE),
        },
        "features": {
            "gate_offset_sync": True,
            "arm_lock_immutable": True,
            "gate_prepared_exec": True,
            "warmup_sessions": True,
            "retry_window_jitter": True,
            "event_telemetry": True,
            "target_iso_from_panel": True,
            "arm_market_presence": True,
        },
        "ntp": ntp_snapshot(),
    }


@app.get("/diag/arm-lock")
def diag_arm_lock(x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    lock_payload = read_arm_lock()
    current_hash = current_arm_config_hash()
    lock_hash = (lock_payload or {}).get("config_hash")
    return {
        "ok": True,
        "at": now_iso_utc(),
        "path": ARM_LOCK_PATH,
        "exists": bool(lock_payload),
        "lock": lock_payload,
        "current_hash": current_hash,
        "hash_match": bool(lock_hash and lock_hash == current_hash),
    }


@app.get("/diag/telemetry")
def diag_telemetry(limit: int = 200, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    events = get_recent_telemetry(limit=limit)
    return {"ok": True, "at": now_iso_utc(), "count": len(events), "events": events}


@app.get("/diag/ntp")
def diag_ntp(x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    return {"ok": True, "at": now_iso_utc(), "ntp": ntp_snapshot()}


@app.post("/diag/gate-time-sync")
def diag_gate_time_sync(payload: GateSyncPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    gate = engine.adapters["gate"]
    sync = gate.sync_server_offset(samples=max(3, int(payload.samples)), per_request_timeout=1.2)
    warm = gate.warmup_connection(sync_samples=max(1, int(payload.warmup_sync_samples)), timeout_sec=1.2)
    emit_event(
        "time_sync_done",
        exchange="gate",
        phase="diag_manual",
        offset_ms=sync.get("offset_ms"),
        rtt_median_ms=sync.get("rtt_median_ms"),
        samples=sync.get("samples"),
    )
    emit_event(
        "warmup_done",
        exchange="gate",
        phase="diag_manual",
        ok=warm.get("ok"),
        status=warm.get("status"),
        latency_ms=warm.get("latency_ms"),
    )
    return {"ok": True, "at": now_iso_utc(), "sync": sync, "warmup": warm}


@app.post("/diag/time-sync")
def diag_time_sync(payload: TimeSyncPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    target = (payload.exchange or "").strip().lower()
    names = [target] if target in ALL_TEST_EXCHANGES else list(ALL_TEST_EXCHANGES)
    sample_n = max(3, int(payload.samples))
    warm_sync_n = max(1, int(payload.warmup_sync_samples))
    timeout_sec = max(0.2, float(payload.timeout_sec))

    results = {}
    any_ok = False

    for ex in names:
        row = {"exchange": ex, "ok": False}
        try:
            if ex in engine.adapters:
                adapter = engine.adapters[ex]
                sync = adapter.sync_server_offset(samples=sample_n, per_request_timeout=timeout_sec)
                warm = adapter.warmup_connection(sync_samples=warm_sync_n, timeout_sec=timeout_sec)
            else:
                sync = _sync_public_exchange_time(ex, samples=sample_n, timeout_sec=timeout_sec)
                warm = _warmup_public_exchange(ex, timeout_sec=timeout_sec)
            row_ok = bool((sync or {}).get("ok")) or bool((warm or {}).get("ok"))
            row.update({"ok": row_ok, "sync": sync, "warmup": warm})
            any_ok = any_ok or row_ok
            emit_event(
                "time_sync_done",
                exchange=ex,
                phase="diag_manual",
                offset_ms=sync.get("offset_ms"),
                rtt_median_ms=sync.get("rtt_median_ms"),
                samples=sync.get("samples"),
            )
            emit_event(
                "warmup_done",
                exchange=ex,
                phase="diag_manual",
                ok=warm.get("ok"),
                status=warm.get("status"),
                latency_ms=warm.get("latency_ms"),
            )
        except Exception as e:
            row.update({"error": f"{type(e).__name__}: {e}"})
        results[ex] = row

    return {"ok": any_ok, "at": now_iso_utc(), "results": results}


@app.post("/diag/warmup")
def diag_warmup(payload: WarmupPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    target = (payload.exchange or "").strip().lower()
    names = [target] if target in ALL_TEST_EXCHANGES else list(ALL_TEST_EXCHANGES)
    out = {}
    for ex in names:
        if ex in engine.adapters:
            kwargs = {"timeout_sec": max(0.2, float(payload.timeout_sec))}
            if ex == "gate":
                kwargs["sync_samples"] = max(0, int(payload.sync_samples))
            res = engine.warmup_exchange(ex, **kwargs)
        else:
            res = _warmup_public_exchange(ex, timeout_sec=max(0.2, float(payload.timeout_sec)))
        out[ex] = res
        emit_event(
            "warmup_done",
            exchange=ex,
            phase="diag_manual",
            ok=res.get("ok"),
            status=res.get("status"),
            latency_ms=res.get("latency_ms"),
        )
    return {"ok": True, "at": now_iso_utc(), "results": out}


@app.post("/diag/market-presence")
def diag_market_presence(payload: MarketPresencePayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    ex = str(payload.exchange or "gate").strip().lower()
    symbol = str(payload.symbol or "BTC_USDT").strip()
    listing_title = str(payload.listing_title or "").strip() or None
    listing_url = str(payload.listing_url or "").strip() or None
    contract_hint = str(payload.contract_hint or "").strip() or None
    t0 = time.perf_counter()
    out = MARKET_PRESENCE_RESOLVER.analyze(
        target_exchange=ex,
        symbol=symbol,
        listing_title=listing_title,
        listing_url=listing_url,
        contract_hint=contract_hint,
    )
    scan_ms = int((time.perf_counter() - t0) * 1000)
    out = dict(out or {})
    out["checked"] = True
    out["scan_ms"] = scan_ms
    out["at"] = now_iso_utc()
    emit_event(
        "presence_scan_done",
        exchange=ex,
        symbol=symbol,
        method=out.get("method"),
        found=bool(out.get("found_on_other_exchanges")),
        rows=len(out.get("rows") or []),
        ambiguous=bool(out.get("ambiguous")),
        scan_ms=scan_ms,
        phase="diag_manual",
    )
    return {"ok": True, "at": now_iso_utc(), "result": out}


async def gate_ws_loop():
    global ARM_PREPARED_PLAN

    subscribed_pair = None
    while True:
        try:
            log("GATE_WS: connecting...")
            async with websockets.connect(GATE_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                log("GATE_WS: connected")
                subscribed_pair = None
                while True:
                    snap = get_arm_snapshot()
                    if snap.get("armed") and snap.get("exchange") == "gate":
                        pair = engine.adapters["gate"].normalize_symbol(snap.get("symbol") or "")
                        if pair and subscribed_pair != pair:
                            sub = {"time": int(time.time()), "channel": "spot.tickers", "event": "subscribe", "payload": [pair]}
                            await ws.send(json.dumps(sub))
                            subscribed_pair = pair
                            set_arm_state(phase="armed")
                            log(f"GATE_WS: subscribed {pair}")

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=0.2)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("channel") != "spot.tickers" or msg.get("event") != "update":
                        continue

                    result = msg.get("result") or {}
                    pair = str(result.get("currency_pair") or "").upper()
                    if not pair:
                        continue

                    snap = get_arm_snapshot()
                    if snap.get("armed") and snap.get("exchange") == "gate" and subscribed_pair == pair:
                        ok, reason, _ = validate_arm_lock("gate", pair, str(snap.get("spend_usdt") or ""))
                        if not ok:
                            emit_event("order_fail_final", exchange="gate", symbol=pair, reason=reason, attempts=0)
                            log(f"GATE_WS: lock validation failed ({reason})")
                            clear_arm_state()
                            continue

                        set_arm_state(phase="exec")
                        emit_target_reached(snap)

                        spend = str(snap.get("spend_usdt") or "")
                        t0 = time.perf_counter()
                        out = engine.market_buy_fast(
                            "gate",
                            pair,
                            spend,
                            prepared_plan=ARM_PREPARED_PLAN,
                            telemetry_cb=make_order_telemetry_cb("gate", pair),
                        )
                        out = dict(out or {})
                        out.setdefault("_requested_quote_qty", spend)
                        out["trigger_to_result_ms"] = int((time.perf_counter() - t0) * 1000)
                        set_last_execution("gate", pair, pair, "ws_trigger", out)
                        emit_event("exec_complete", exchange="gate", symbol=pair, status=out.get("status"), attempt=out.get("attempt"))
                        log(f"GATE_BUY_RESULT: {out}")
                        clear_arm_state()
        except Exception as e:
            emit_event("ws_error", exchange="gate", error=f"{type(e).__name__}: {e}")
            log(f"GATE_WS: error {type(e).__name__}: {e}")
            await asyncio.sleep(1)


@app.on_event("startup")
async def on_startup():
    global MAIN_LOOP, TELEMETRY_THREAD

    MAIN_LOOP = asyncio.get_running_loop()
    TELEMETRY_STOP.clear()
    TELEMETRY_THREAD = threading.Thread(target=telemetry_worker, daemon=True)
    TELEMETRY_THREAD.start()

    log_time_sync_health()
    emit_event("executor_start", dry_run=engine.dry_run)

    asyncio.create_task(gate_ws_loop())
    log("EXECUTOR started + Gate WS loop running")


@app.on_event("shutdown")
async def on_shutdown():
    TELEMETRY_STOP.set()
    if TELEMETRY_THREAD and TELEMETRY_THREAD.is_alive():
        TELEMETRY_THREAD.join(timeout=2.0)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("executor:app", host="0.0.0.0", port=EXECUTOR_PORT, log_level="warning")
