import os
import time
import json
import uuid
import asyncio
import threading
import datetime
from typing import Optional

import requests
import websockets
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from bot.trade_engine import TradeEngine
from bot.util import log

PILOT_TOKEN = os.getenv("PILOT_TOKEN", "change-me")
EXECUTOR_PORT = int(os.getenv("EXECUTOR_PORT", "8080"))
WS_TRIGGER_TIMEOUT_SEC = max(5, int(os.getenv("WS_TRIGGER_TIMEOUT_SEC", "20")))

GATE_WS_URL = "wss://api.gateio.ws/ws/v4/"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
MEXC_WS_URLS = [
    "wss://wbs-api.mexc.com/ws",
    "wss://wbs.mexc.com/ws",
]
BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"

engine = TradeEngine()

# Single active ARM at a time (MVP behavior).
ARM_STATE = {"armed": False, "exchange": None, "symbol": None, "spend_usdt": None, "phase": "idle"}
STATE_LOCK = threading.Lock()
MAIN_LOOP = None
NON_GATE_TASK = None
WS_SUPPORTED = {"gate", "binance", "mexc", "kucoin", "bitget"}
LAST_EXECUTION = {}
ORDER_LATENCY_BY_EXCHANGE = {}


class ArmPayload(BaseModel):
    exchange: str
    symbol: str
    spend_usdt: str


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


app = FastAPI()


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
    set_arm_state(armed=False, exchange=None, symbol=None, spend_usdt=None, phase="idle")


def now_iso_utc() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def default_probe_symbol(exchange: str) -> str:
    ex = (exchange or "").strip().lower()
    if ex == "gate":
        return "BTC_USDT"
    if ex == "kucoin":
        return "BTC-USDT"
    return "BTCUSDT"


def set_last_execution(exchange: str, symbol_input: str, symbol_sent: str, mode: str, result: dict):
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
    return False


def run_exchange_network_test(exchange: str) -> dict:
    ex = exchange.strip().lower()
    adapter = engine.adapters[ex]
    url = adapter.base
    t0 = time.perf_counter()
    try:
        resp = requests.get(url, timeout=6, allow_redirects=True)
        latency_ms = int((time.perf_counter() - t0) * 1000)
        # 4xx still means network+host is reachable.
        network_ok = resp.status_code < 500
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
        return {"auth_ok": None, "auth_status": None, "auth_latency_ms": None, "auth_error": "API bilgisi eksik"}

    try:
        if ex == "gate":
            g = engine.adapters["gate"]
            path = "/spot/accounts"
            headers = g._sign_headers("GET", "/api/v4" + path, "", "")
            t0 = time.perf_counter()
            resp = requests.get(g.base + path, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "binance":
            b = engine.adapters["binance"]
            signed = b._signed({})
            t0 = time.perf_counter()
            resp = requests.get(b.base + "/api/v3/account", headers=signed["headers"], params=signed["qs"], timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "mexc":
            m = engine.adapters["mexc"]
            signed = m._signed({})
            t0 = time.perf_counter()
            resp = requests.get(m.base + "/api/v3/account", headers=signed["headers"], params=signed["qs"], timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "kucoin":
            k = engine.adapters["kucoin"]
            path = "/api/v1/accounts?type=trade"
            headers = k._headers("GET", path, "")
            t0 = time.perf_counter()
            resp = requests.get(k.base + path, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        elif ex == "bitget":
            b = engine.adapters["bitget"]
            path = "/api/v2/spot/account/assets"
            headers = b._headers("GET", path, "")
            t0 = time.perf_counter()
            resp = requests.get(b.base + path, headers=headers, timeout=6)
            latency = int((time.perf_counter() - t0) * 1000)
        else:
            return {"auth_ok": None, "auth_status": None, "auth_latency_ms": None, "auth_error": "desteklenmeyen borsa"}

        ok = 200 <= resp.status_code < 300
        body = safe_json_response(resp)
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
                        # New MEXC feeds may use protobuf frames. Receiving data means stream is alive.
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

                    # Ignore obvious ack-only frames; accept data-like frames.
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


async def non_gate_ws_trigger_once(exchange: str, symbol: str, spend: str):
    try:
        set_arm_state(phase="ws_connecting")
        _ = await wait_first_tick(exchange, symbol)

        snap = get_arm_snapshot()
        if not (snap.get("armed") and snap.get("exchange") == exchange and snap.get("symbol") == symbol):
            return

        set_arm_state(phase="triggered")
        t0 = time.perf_counter()
        out = engine.market_buy_fast(exchange, symbol, spend)
        out = dict(out or {})
        out["trigger_to_result_ms"] = int((time.perf_counter() - t0) * 1000)
        set_last_execution(exchange, symbol, symbol, "ws_trigger", out)
        log(f"{exchange.upper()}_WS_BUY_RESULT: {out}")
    except asyncio.CancelledError:
        log(f"{exchange.upper()}_WS: cancelled")
        raise
    except Exception as e:
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
    return {
        "dry_run": engine.dry_run,
        "gate": {
            "armed": snap.get("armed"),
            "exchange": snap.get("exchange"),
            "symbol": snap.get("symbol"),
            "phase": snap.get("phase"),
        },
        "last_execution": last_exec,
        "order_latency": order_latency,
    }


@app.post("/disarm")
def disarm(x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    cancel_non_gate_task()
    clear_arm_state()
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
        log(f"KILL: force market BUY ex={ex} symbol={symbol} spend={spend}")
        try:
            forced_buy = engine.market_buy_fast(ex, symbol, spend)
            set_last_execution(ex, symbol, symbol, "kill_force_buy", forced_buy)
            log(f"KILL_FORCE_BUY_RESULT: {forced_buy}")
        except Exception as e:
            forced_buy = {"error": f"{type(e).__name__}: {e}"}
            log(f"KILL_FORCE_BUY_ERROR: {forced_buy['error']}")

    clear_arm_state()
    log("KILL: CLOSED")
    return {"ok": True, "forced_buy": forced_buy}


@app.post("/arm")
def arm(payload: ArmPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    ex = payload.exchange.strip().lower()
    symbol = payload.symbol.strip()
    spend = payload.spend_usdt.strip()

    if ex not in engine.adapters:
        raise HTTPException(status_code=400, detail=f"unknown exchange: {ex}")

    norm_symbol = engine.adapters[ex].normalize_symbol(symbol)

    # WS-trigger path for all supported exchanges.
    if ex in WS_SUPPORTED:
        set_arm_state(armed=True, exchange=ex, symbol=norm_symbol, spend_usdt=spend, phase="armed")

        if ex == "gate":
            log(f"ARM(GATE): wait WS ticker symbol={norm_symbol} spend_usdt={spend} DRY_RUN={engine.dry_run}")
        else:
            schedule_non_gate_trigger(ex, norm_symbol, spend)
            log(f"ARM({ex.upper()}): wait WS ticker symbol={norm_symbol} spend_usdt={spend} DRY_RUN={engine.dry_run}")

        return {"ok": True, "mode": "ws_trigger", "exchange": ex, "symbol": norm_symbol, "spend_usdt": spend}

    # Fallback immediate mode.
    log(f"ARM(IMMEDIATE): ex={ex} symbol={norm_symbol} spend={spend} DRY_RUN={engine.dry_run}")
    result = engine.market_buy_fast(ex, norm_symbol, spend)
    set_last_execution(ex, symbol, norm_symbol, "immediate", result)
    return {"ok": True, "mode": "immediate", "result": result}


@app.post("/sell")
def sell(payload: SellPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    ex = payload.exchange.strip().lower()
    symbol = payload.symbol.strip()
    qty = payload.qty.strip()
    log(f"SELL: ex={ex} symbol={symbol} qty={qty} DRY_RUN={engine.dry_run}")
    result = engine.market_sell(ex, symbol, qty)
    norm_symbol = engine.adapters.get(ex).normalize_symbol(symbol) if ex in engine.adapters else symbol
    set_last_execution(ex, symbol, norm_symbol, "sell", result)
    return {"ok": True, "result": result}


@app.post("/dry-run")
def set_dry_run(payload: DryRunPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    engine.apply_runtime_config({}, payload.enabled)
    return {"ok": True, "dry_run": engine.dry_run}


@app.post("/admin/config")
def set_admin_config(payload: AdminConfigPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    engine.apply_runtime_config(payload.values or {}, payload.dry_run)
    return {"ok": True, "dry_run": engine.dry_run}


@app.post("/exchange-test")
def exchange_test(payload: ExchangeTestPayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)
    target = (payload.exchange or "").strip().lower()
    names = [target] if target in engine.adapters else list(engine.adapters.keys())

    results = {}
    for ex in names:
        row = {"exchange": ex, "base_url": engine.adapters[ex].base}
        row.update(run_exchange_network_test(ex))
        row.update(run_exchange_auth_test(ex))
        results[ex] = row

    return {"ok": True, "at": now_iso_utc(), "results": results}


@app.post("/order-latency")
def order_latency_probe(payload: LatencyProbePayload, x_pilot_token: Optional[str] = Header(default=None)):
    check_token(x_pilot_token)

    ex = payload.exchange.strip().lower()
    if ex not in engine.adapters:
        raise HTTPException(status_code=400, detail=f"unknown exchange: {ex}")

    symbol_in = (payload.symbol or "").strip()
    spend = (payload.spend_usdt or "5").strip()
    auto_symbol = False
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

    set_last_execution(ex, symbol_in, symbol_sent, "latency_probe", result)
    return {
        "ok": True,
        "probe": {
            "exchange": ex,
            "symbol_input": symbol_in,
            "symbol_sent": symbol_sent,
            "engine_latency_ms": result.get("engine_latency_ms"),
            "status": result.get("status"),
            "probe_type": result.get("probe_type"),
            "safe_no_trade": bool(result.get("safe_no_trade", True)),
            "auto_symbol": auto_symbol,
        },
        "result": result,
    }


async def gate_ws_loop():
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
                        set_arm_state(phase="triggered")
                        log(f"GATE_WS: TRIGGER pair={pair} last={result.get('last')} ask={result.get('lowest_ask')}")
                        spend = str(snap.get("spend_usdt") or "")
                        t0 = time.perf_counter()
                        out = engine.market_buy_fast("gate", pair, spend)
                        out = dict(out or {})
                        out["trigger_to_result_ms"] = int((time.perf_counter() - t0) * 1000)
                        set_last_execution("gate", pair, pair, "ws_trigger", out)
                        log(f"GATE_BUY_RESULT: {out}")
                        clear_arm_state()
        except Exception as e:
            log(f"GATE_WS: error {type(e).__name__}: {e}")
            await asyncio.sleep(1)


@app.on_event("startup")
async def on_startup():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    asyncio.create_task(gate_ws_loop())
    log("EXECUTOR started + Gate WS loop running")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("executor:app", host="0.0.0.0", port=EXECUTOR_PORT, log_level="warning")
