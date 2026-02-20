import os, re, time, threading, datetime, json, requests
from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from dateutil import parser as dt_parser, tz
from bot.listings_agg import ListingsAggregator
from bot.scanner_service import ScannerService
from bot.symbols import canonical_symbol as to_canonical_symbol
from bot.symbols import format_order_symbol, build_trade_url
from bot.util import iso_utc, tz_name

PANEL_PORT = int(os.getenv("PANEL_PORT", "5177"))
PANEL_HOST = os.getenv("PANEL_HOST", "127.0.0.1")
EXECUTOR_URL = os.getenv("EXECUTOR_URL", "http://127.0.0.1:8080").rstrip("/")
PILOT_TOKEN = os.getenv("PILOT_TOKEN", "change-me")
POLL_SECONDS = max(30, int(os.getenv("LISTING_POLL_SECONDS", "600")))
LISTING_MAX_AGE_DAYS = max(1, int(os.getenv("LISTING_MAX_AGE_DAYS", "5")))
TRADE_EXCHANGES = ["gate", "mexc", "kucoin", "bitget", "binance", "paribu"]
TRADE_EXCHANGE_SET = set(TRADE_EXCHANGES)
LISTING_EXCHANGES = ["gate", "mexc", "kucoin", "bitget", "binance", "okex", "bybit", "btcturk", "paribu"]
LISTING_EXCHANGE_SET = set(LISTING_EXCHANGES)
REAL_TEST_EXCHANGES = list(LISTING_EXCHANGES)
REAL_TEST_EXCHANGE_SET = set(REAL_TEST_EXCHANGES)
NOTIFY_ENABLED = os.getenv("NOTIFY_ENABLED", "1") == "1"
NOTIFY_TELEGRAM_BOT_TOKEN = os.getenv("NOTIFY_TELEGRAM_BOT_TOKEN", "").strip()
NOTIFY_TELEGRAM_CHAT_ID = os.getenv("NOTIFY_TELEGRAM_CHAT_ID", "").strip()
NOTIFY_WEBHOOK_URL = os.getenv("NOTIFY_WEBHOOK_URL", "").strip()
NOTIFY_STATE_FILE = os.getenv(
    "NOTIFY_STATE_FILE",
    os.path.join(os.path.dirname(__file__), "state", "notified_urls.json"),
)

app = Flask(__name__)
agg = ListingsAggregator()
SCANNER_SERVICE = ScannerService(
    aggregator=agg,
    state_dir=os.path.join(os.path.dirname(__file__), "state", "scanner_runtime"),
    max_events_per_exchange=220,
)
STATE = {
    "last_poll": None,
    "errors": {},
    "checks": {},
    "exchange_modes": {},
    "last_action": None,
    "countdown": None,
    "last_order": {},
    "tests": {},
    "real_trade_tests": [],
    "scanner_status": {},
    "scan_stats": {},
    "listing_events_by_exchange": {},
    "selected_exchange_id": "gate",
    "trade_draft": {},
    "focus_request_id": "",
}
TR_TZ = tz.gettz(tz_name())


@app.after_request
def add_no_cache_headers(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

ENV_FILE_CANDIDATES = [
    os.getenv("BOT_ENV_FILE", "").strip(),
    "/opt/quickbot/.env",
    os.path.join(os.path.dirname(__file__), ".env"),
    ".env",
]
CONFIG_KEYS = [
    "GATE_KEY", "GATE_SECRET", "GATE_BASE",
    "BINANCE_KEY", "BINANCE_SECRET", "BINANCE_BASE",
    "MEXC_KEY", "MEXC_SECRET", "MEXC_BASE",
    "KUCOIN_KEY", "KUCOIN_SECRET", "KUCOIN_PASSPHRASE", "KUCOIN_BASE",
    "BITGET_KEY", "BITGET_SECRET", "BITGET_PASSPHRASE", "BITGET_BASE",
    "OKEX_KEY", "OKEX_SECRET", "OKEX_PASSPHRASE", "OKEX_BASE",
    "BYBIT_KEY", "BYBIT_SECRET", "BYBIT_BASE",
    "BTCTURK_KEY", "BTCTURK_SECRET", "BTCTURK_BASE",
    "PARIBU_KEY", "PARIBU_SECRET", "PARIBU_BASE",
]
SENSITIVE_KEYS = {
    "GATE_KEY", "GATE_SECRET",
    "BINANCE_KEY", "BINANCE_SECRET",
    "MEXC_KEY", "MEXC_SECRET",
    "KUCOIN_KEY", "KUCOIN_SECRET", "KUCOIN_PASSPHRASE",
    "BITGET_KEY", "BITGET_SECRET", "BITGET_PASSPHRASE",
    "OKEX_KEY", "OKEX_SECRET", "OKEX_PASSPHRASE",
    "BYBIT_KEY", "BYBIT_SECRET",
    "BTCTURK_KEY", "BTCTURK_SECRET",
    "PARIBU_KEY", "PARIBU_SECRET",
}
RUNTIME_DEFAULTS = {
    "GATE_BASE": "https://api.gateio.ws/api/v4",
    "BINANCE_BASE": "https://api.binance.com",
    "MEXC_BASE": "https://api.mexc.com",
    "KUCOIN_BASE": "https://api.kucoin.com",
    "BITGET_BASE": "https://api.bitget.com",
    "OKEX_BASE": "https://www.okx.com",
    "BYBIT_BASE": "https://api.bybit.com",
    "BTCTURK_BASE": "https://api.btcturk.com",
    "PARIBU_BASE": "https://api.paribu.com",
}

TEST_CATALOG = [
    {
        "id": "feature_check",
        "title": "System Overview",
        "desc": "Shows whether offset sync, lock, retry, and telemetry features are enabled in one report.",
    },
    {
        "id": "ntp_check",
        "title": "Server Clock Check",
        "desc": "Validates if NTP/Chrony is running and the server clock is synchronized.",
    },
    {
        "id": "exchange_time_sync",
        "title": "Exchange Time Sync",
        "desc": "Measures offset and jitter for all exchanges using each exchange server time endpoint.",
    },
    {
        "id": "warmup_all",
        "title": "Connection Warmup",
        "desc": "Runs warmup calls across all exchanges and returns connection timings.",
    },
    {
        "id": "arm_lock",
        "title": "ARM Lock Check",
        "desc": "Checks arm.lock, config hash consistency, and plan summary.",
    },
    {
        "id": "telemetry",
        "title": "Telemetry Events",
        "desc": "Lists recent telemetry events with wall and monotonic timestamps.",
    },
    {
        "id": "exchange_test",
        "title": "Exchange Access Test",
        "desc": "Checks network reachability and API auth status per exchange.",
    },
    {
        "id": "aggregator_state",
        "title": "Listing Source Status",
        "desc": "Shows which source each exchange uses and whether it is degraded.",
    },
    {
        "id": "market_presence",
        "title": "Cross-Exchange Scan",
        "desc": "Tests ARM-stage matching flow: contract > coin ID > symbol.",
    },
]


def resolve_env_file_path():
    for path in ENV_FILE_CANDIDATES:
        if path and os.path.exists(path):
            return path
    for path in ENV_FILE_CANDIDATES:
        if path:
            return path
    return ".env"


ENV_FILE_PATH = resolve_env_file_path()
ENV_LINE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")


def parse_env_value(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        inner = value[1:-1]
        if value[0] == '"':
            inner = inner.replace('\\"', '"').replace("\\\\", "\\")
        return inner
    if " #" in value:
        value = value.split(" #", 1)[0].rstrip()
    return value


def quote_env_value(value: str) -> str:
    text = str(value or "")
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f"\"{escaped}\""


def read_env_file(path: str):
    lines = []
    values = {}
    if not path or not os.path.exists(path):
        return lines, values
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            lines.append(line)
            m = ENV_LINE_RE.match(line)
            if not m:
                continue
            values[m.group(1)] = parse_env_value(m.group(2))
    return lines, values


def upsert_env_file(path: str, updates: dict):
    if not path:
        raise ValueError("Env file path is empty.")
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    lines, _ = read_env_file(path)
    out = []
    touched = set()
    for line in lines:
        m = ENV_LINE_RE.match(line)
        if not m:
            out.append(line)
            continue
        key = m.group(1)
        if key in updates:
            out.append(f"{key}={quote_env_value(updates[key])}")
            touched.add(key)
        else:
            out.append(line)
    for key, value in updates.items():
        if key in touched:
            continue
        out.append(f"{key}={quote_env_value(value)}")
    content = "\n".join(out).strip("\n")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content + "\n")


def load_admin_state():
    _, env_values = read_env_file(ENV_FILE_PATH)
    cfg = {}
    flags = {k: False for k in SENSITIVE_KEYS}
    for key in CONFIG_KEYS:
        value = str(env_values.get(key, os.getenv(key, ""))).strip()
        if not value and key in RUNTIME_DEFAULTS:
            value = RUNTIME_DEFAULTS[key]
        if key in SENSITIVE_KEYS:
            flags[key] = bool(value)
            cfg[key] = ""
        else:
            cfg[key] = value
    dry_raw = str(env_values.get("DRY_RUN", os.getenv("DRY_RUN", "1"))).strip()
    dry_run = dry_raw != "0"
    return cfg, flags, dry_run

HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>quickbot</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; }
    body {
      font-family: "Montserrat", sans-serif;
      background-color: #ffffff;
      color: #334155;
      background-image: radial-gradient(#cbd5e1 1px, transparent 1px);
      background-size: 24px 24px;
      min-height: 100dvh;
      height: auto;
      overflow-x: hidden;
      overflow-y: auto;
      margin: 0;
    }
    .panel {
      background: #ffffff;
      border: 1px solid #334155;
      border-radius: 12px;
      box-shadow: 0 4px 0 #e2e8f0;
    }
    .sidebar-link {
      border: 1px solid transparent;
      transition: all 0.2s;
    }
    .sidebar-link:hover { border-color: #334155; background: #f8fafc; }
    .sidebar-link.active { border-color: #334155; }
    .input-box {
      background: transparent;
      border: 1px solid #e2e8f0;
      border-radius: 8px;
      font-weight: 600;
      transition: border-color 0.2s ease;
    }
    .input-box:focus { outline: none; border-color: #334155; }
    .trade-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
    }
    .trade-field {
      min-width: 0;
    }
    .trade-field-control {
      width: 100%;
      min-height: 56px;
      height: 56px;
      padding: 0 16px;
      font-size: 16px;
      line-height: 1.2;
      font-weight: 700;
    }
    input[type="number"].trade-field-control {
      -moz-appearance: textfield;
      appearance: textfield;
    }
    input[type="number"].trade-field-control::-webkit-outer-spin-button,
    input[type="number"].trade-field-control::-webkit-inner-spin-button {
      -webkit-appearance: none;
      margin: 0;
    }
    .btn-main {
      background: transparent;
      border: 1px solid #334155;
      color: #334155;
      border-radius: 50px;
      -webkit-appearance: none;
      appearance: none;
      font-weight: 700;
      text-transform: uppercase;
      font-size: 13px;
      letter-spacing: 1.3px;
      transition: all 0.25s ease;
      min-height: 52px;
    }
    .btn-main::before,
    .btn-main::after {
      content: none !important;
      display: none !important;
    }
    .btn-main .trade-check,
    .btn-main .status-dot,
    .btn-main .dot,
    .btn-main [data-dot] {
      display: none !important;
    }
    .btn-main:hover {
      background: #334155;
      color: #fff;
      box-shadow: 0 4px 10px rgba(51, 65, 85, 0.2);
      transform: translateY(-1px);
    }
    .buy-btn { border-color: #86efac; color: #166534; background: #f0fdf4; }
    .buy-btn:hover { border-color: #16a34a; background: #16a34a; color: #fff; }
    .stop-btn { border-color: #fecaca; color: #991b1b; background: #fef2f2; }
    .stop-btn:hover { border-color: #ef4444; background: #ef4444; color: #fff; }
    .trade-actions {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-top: auto;
    }
    .action-wrap {
      position: relative;
    }
    .open-pos-note {
      margin-top: 10px;
      font-size: 12px;
      color: #475569;
      font-weight: 600;
    }
    .trade-history {
      margin-top: 12px;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      background: #f8fafc;
      padding: 10px;
    }
    .trade-history-head {
      margin: 0 0 8px;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .5px;
      color: #475569;
      font-weight: 700;
    }
    .pnl-pos { color: #166534; font-weight: 700; }
    .pnl-neg { color: #991b1b; font-weight: 700; }
    .chip {
      border: 1px solid #334155;
      border-radius: 999px;
      padding: 4px 6px;
      background: #fff;
      font-size: 10px;
      font-weight: 700;
      color: #334155;
      cursor: pointer;
      min-height: 28px;
      line-height: 1;
      transition: all 0.2s ease;
      width: 100%;
      text-align: center;
    }
    .chip:hover { transform: translateY(-1px); }
    .chip.active { box-shadow: inset 0 0 0 2px #cbd5e1; }
    .chip.ex-gate { background: #f8f0e2; }
    .chip.ex-mexc { background: #eaf2f8; }
    .chip.ex-kucoin { background: #f1eaf8; }
    .chip.ex-bitget { background: #f7ede1; }
    .chip.ex-binance { background: #e8f4ee; }
    .latency-controls {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 4px;
      align-items: center;
    }
    .tag-online {
      border: 1px solid #86efac;
      color: #166534;
      background: #f0fdf4;
      border-radius: 999px;
    }
    .tag-offline {
      border: 1px solid #fecaca;
      color: #991b1b;
      background: #fef2f2;
      border-radius: 999px;
    }
    .tab-btn {
      border: 1px solid #334155;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 11px;
      font-weight: 700;
      color: #334155;
      background: #fff;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      cursor: pointer;
      transition: all 0.18s ease;
      min-height: 30px;
    }
    .tab-btn:hover { transform: translateY(-1px); }
    .tab-btn.active {
      color: #1e293b;
      box-shadow: inset 0 0 0 1.5px #334155;
    }
    .tab-btn[data-tab="gate"] { background: #f7eedf; }
    .tab-btn[data-tab="mexc"] { background: #eaf2fb; }
    .tab-btn[data-tab="kucoin"] { background: #f0e9f8; }
    .tab-btn[data-tab="bitget"] { background: #f8efe3; }
    .tab-btn[data-tab="binance"] { background: #e8f3ec; }
    .tab-btn[data-tab="okex"] { background: #eef2ff; }
    .tab-btn[data-tab="bybit"] { background: #f0fdf4; }
    .tab-btn[data-tab="btcturk"] { background: #ecfeff; }
    .tab-btn[data-tab="paribu"] { background: #faf5ff; }
    .pill {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      border: 1px solid transparent;
      font-size: 10px;
      padding: 2px 7px;
      font-weight: 700;
      line-height: 1;
    }
    .pill.err { background: #fef2f2; border-color: #fecaca; color: #991b1b; }
    .pill.new { background: #f0fdf4; border-color: #86efac; color: #166534; }
    .pill.warn { background: #fff7ed; border-color: #fed7aa; color: #9a3412; }
    .pill.src-api { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
    .pill.src-diff { background: #f5f3ff; border-color: #ddd6fe; color: #6d28d9; }
    .pill.src-web { background: #ecfeff; border-color: #a5f3fc; color: #0e7490; }
    .exchange-panel { display: none; }
    .exchange-panel.active { display: block; }
    .check-ok { color: #166534; }
    .check-bad { color: #991b1b; }
    .pair-btn {
      border: 1px solid #334155;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 10px;
      font-weight: 700;
      color: #334155;
      background: transparent;
      cursor: pointer;
    }
    .pair-btn:hover { background: #334155; color: #fff; }
    .logs-row { display: flex; gap: 12px; border-bottom: 1px solid #e2e8f0; padding-bottom: 6px; margin-bottom: 6px; }
    .muted { color: #64748b; }
    .countdown-box {
      margin-top: 14px;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 10px 12px;
      text-align: center;
      background: #f8fafc;
    }
    .countdown-value { font-size: 24px; font-weight: 300; letter-spacing: 0.8px; margin: 2px 0; }
    .date-row { display: block; font-size: 13px; font-weight: 600; }
    .poll-date-row { display: block; font-size: 11px; font-weight: 600; }
    .status-stack {
      display: grid;
      gap: 10px;
    }
    .status-block {
      border: 1px solid #d6c9b2;
      border-radius: 10px;
      padding: 12px;
      background: #fcf7ed;
    }
    .status-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .status-head {
      font-size: 11px;
      line-height: 1.35;
      font-weight: 700;
      letter-spacing: .4px;
      text-transform: uppercase;
      color: #475569;
      margin: 0;
    }
    .brand-wrap {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .brand-title {
      margin: 0;
      font-size: 30px;
      font-weight: 800;
      letter-spacing: .2px;
      line-height: 1.1;
      display: inline-flex;
      align-items: center;
      gap: 0;
      color: #1e293b;
    }
    .brand-wordmark {
      display: inline-flex;
      align-items: baseline;
      gap: 0;
      line-height: 1;
    }
    .brand-word-main { color: #1f3b73; }
    .brand-word-accent { color: #8b5e34; }
    .brand-tagline {
      margin: 0 0 1px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: .2px;
      color: #64748b;
      line-height: 1.2;
    }
    .status-live {
      font-size: 10px;
      font-weight: 700;
      letter-spacing: .4px;
      color: #475569;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .status-live-dot {
      width: 7px;
      height: 7px;
      border-radius: 999px;
      display: inline-block;
    }
    .status-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 4px;
      margin-bottom: 9px;
    }
    .status-line {
      font-family: "Montserrat", sans-serif;
      font-size: 13px;
      line-height: 1.45;
      font-weight: 500;
      color: #64748b;
      letter-spacing: 0;
    }
    .status-line.dim {
      font-size: 13px;
      line-height: 1.45;
      font-weight: 500;
      color: #64748b;
      letter-spacing: 0;
    }
    .pending-block {
      margin-top: 0;
    }
    .pending-card {
      border: 1px solid #d4deea;
      border-radius: 10px;
      background: #eef4fb;
      padding: 9px 10px;
      display: grid;
      gap: 8px;
    }
    .pending-card.active {
      border-color: #bfd0ea;
      background: #e8f1ff;
    }
    .pending-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .pending-title {
      margin: 0;
      font-size: 11px;
      line-height: 1.35;
      color: #475569;
      font-weight: 700;
      letter-spacing: .4px;
      text-transform: uppercase;
    }
    .pending-state {
      display: inline-flex;
      align-items: center;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 10px;
      font-weight: 700;
      border: 1px solid #cbd5e1;
      color: #334155;
      background: #fff;
      line-height: 1.3;
    }
    .pending-state.yes {
      border-color: #86efac;
      color: #166534;
      background: #f0fdf4;
    }
    .pending-grid {
      margin: 0;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px 8px;
    }
    .pending-item {
      display: grid;
      gap: 1px;
      min-width: 0;
    }
    .pending-item span {
      font-size: 11px;
      color: #475569;
      line-height: 1.25;
      font-weight: 500;
    }
    .pending-item b {
      font-size: 12px;
      font-weight: 700;
      color: #1e293b;
      line-height: 1.25;
      word-break: break-word;
    }
    .pending-empty {
      margin: 0;
      font-size: 11px;
      color: #64748b;
      line-height: 1.35;
    }
    .table-wrap { overflow: auto; border: 1px solid #e2e8f0; border-radius: 10px; }
    table { width: 100%; border-collapse: collapse; min-width: 760px; }
    th, td {
      text-align: left;
      vertical-align: top;
      padding: 10px;
      border-bottom: 1px solid #e2e8f0;
      font-size: 12px;
    }
    th {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .7px;
      color: #64748b;
      background: #f8fafc;
    }
    .empty {
      border: 1px dashed #94a3b8;
      border-radius: 10px;
      padding: 14px;
      font-size: 14px;
      color: #64748b;
      background: #f8fafc;
    }
    .presence-box {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 10px 12px;
      background: #f8fafc;
      margin-bottom: 10px;
    }
    .presence-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 8px;
      align-items: center;
    }
    .presence-summary {
      font-size: 12px;
      line-height: 1.45;
      color: #334155;
      margin: 0 0 8px;
    }
    .presence-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
      margin-bottom: 8px;
    }
    .presence-kpi {
      border: 1px solid #dbe4ef;
      border-radius: 8px;
      padding: 8px;
      background: #fff;
    }
    .presence-kpi b {
      display: block;
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: .5px;
      color: #64748b;
      margin-bottom: 4px;
    }
    .presence-kpi span {
      font-size: 13px;
      font-weight: 700;
      color: #1e293b;
      line-height: 1.35;
    }
    .presence-candidates {
      border: 1px dashed #cbd5e1;
      border-radius: 8px;
      padding: 8px;
      margin-bottom: 8px;
      background: #fff;
      font-size: 12px;
      color: #334155;
      line-height: 1.4;
    }
    .presence-candidates b {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .4px;
      color: #64748b;
    }
    .presence-candidates ul {
      margin: 6px 0 0;
      padding-left: 16px;
    }
    .presence-candidates li {
      margin: 2px 0;
    }
    .presence-table-wrap {
      overflow: auto;
      border: 1px solid #dbe4ef;
      border-radius: 10px;
      background: #fff;
    }
    .presence-table {
      width: 100%;
      min-width: 920px;
      border-collapse: collapse;
    }
    .presence-table th,
    .presence-table td {
      border-bottom: 1px solid #edf2f7;
      padding: 8px 9px;
      font-size: 12px;
      line-height: 1.35;
      text-align: left;
      color: #334155;
      white-space: nowrap;
    }
    .presence-table th {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: .7px;
      color: #64748b;
      background: #f8fafc;
    }
    .presence-table tr:last-child td { border-bottom: 0; }
    .latency-line b {
      font-size: 11px;
      letter-spacing: .7px;
      text-transform: uppercase;
      color: #64748b;
      margin-right: 8px;
    }
    .latency-line { margin: 0; font-size: 16px; font-weight: 400; }
    .news-list { display: flex; flex-direction: column; gap: 6px; }
    .news-item {
      border: 1px solid #e2e8f0;
      border-radius: 8px;
      padding: 6px 8px;
      background: #f8fafc;
    }
    .news-title-row {
      display: flex;
      align-items: center;
      gap: 6px;
      min-width: 0;
      margin-bottom: 4px;
    }
    .news-title {
      font-size: 11px;
      line-height: 1.25;
      font-weight: 700;
      color: #334155;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      min-width: 0;
      flex: 1;
    }
    .news-meta {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      font-size: 10px;
      color: #64748b;
    }
    .news-actions {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      flex-shrink: 0;
    }
    .news-open {
      font-size: 10px;
      font-weight: 700;
      color: #334155;
      text-decoration: none;
      border: 1px solid #cbd5e1;
      border-radius: 999px;
      padding: 3px 7px;
      background: #fff;
      line-height: 1;
    }
    .news-open:hover { border-color: #334155; }
    @media (max-width: 1280px) {
      .brand-title { font-size: 24px; }
      .brand-tagline { font-size: 10px; }
    }
    @media (max-width: 1024px) {
      .trade-grid {
        grid-template-columns: 1fr;
        gap: 14px;
      }
      .trade-actions {
        grid-template-columns: 1fr;
      }
      .status-line { font-size: 13px; }
      .pending-grid { grid-template-columns: 1fr; }
      .presence-grid { grid-template-columns: 1fr; }
      .news-meta {
        flex-wrap: wrap;
        align-items: flex-start;
      }
      .news-actions { width: 100%; justify-content: flex-start; }
    }
    @media (max-width: 640px) {
      .status-block { padding: 10px; }
      .pending-card { padding: 8px 9px; }
      .status-top { flex-wrap: wrap; align-items: flex-start; }
      .status-live { font-size: 10px; }
      .tab-btn { padding: 6px 9px; font-size: 10px; min-height: 28px; }
      .latency-line { font-size: 14px; }
      .countdown-value { font-size: 20px; }
      .logs-row {
        flex-direction: column;
        gap: 4px;
      }
    }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 10px; }
    ::-webkit-scrollbar-track { background: transparent; }
  </style>
</head>
<body class="flex flex-col min-h-screen">
  <div class="flex-1 flex overflow-visible">
    <main class="w-full py-5 px-4 sm:px-6 md:px-10 xl:px-16 2xl:px-24 flex flex-col min-w-0 overflow-visible xl:overflow-y-auto relative gap-5 sm:gap-6">
      <header class="panel p-4 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3 shrink-0">
        <div class="brand-wrap">
        <h2 class="text-xl font-bold text-slate-800 tracking-tight m-0 flex items-center gap-2 brand-title">
          <span class="brand-wordmark"><span class="brand-word-main">quick</span><span class="brand-word-accent">bot</span></span>
        </h2>
        <p class="brand-tagline">Spot listings. Faster entries. Cleaner execution.</p>
        </div>
        <div class="px-3 py-1 flex items-center gap-2 text-xs font-bold {{ 'tag-online' if exec_state['online'] else 'tag-offline' }}">
          <span class="w-1.5 h-1.5 rounded-full {{ 'bg-green-600 animate-pulse' if exec_state['online'] else 'bg-red-600' }}"></span>
          {{ 'System Online' if exec_state['online'] else 'System Offline' }}
        </div>
      </header>

      <div class="flex-1 grid grid-cols-1 xl:grid-cols-12 gap-5 sm:gap-6 min-h-0 pb-4">
        {% set latency_selected = last_order.get("exchange") if last_order.get("exchange") in trade_exchanges else trade_exchanges[0] %}
        {% set latency_selected_row = order_latency.get(latency_selected) or {} %}
        <div class="xl:col-span-5 xl:row-span-2 panel p-4 sm:p-5 md:p-6 min-h-0 xl:min-h-[520px] flex flex-col">
          <div class="status-stack">
            <div class="status-block">
              <div class="status-top">
                <p class="status-head">Last Check</p>
                <span class="status-live">
                  <span class="status-live-dot {{ 'bg-green-500' if exec_state['online'] else 'bg-red-500' }}"></span>
                  {{ 'live' if exec_state['online'] else 'off' }}
                </span>
              </div>
              <div class="status-grid">
                {% if last_poll_parts %}
                  <span class="status-line">{{last_poll_parts["date"]}} / {{last_poll_parts["time"]}} (TRT)</span>
                {% else %}
                  <span class="status-line dim">no data yet</span>
                {% endif %}
              </div>
            </div>
            <div class="pending-block">
              <div class="pending-card {{ 'active' if pending_info else '' }}">
                <div class="pending-top">
                  <p class="pending-title">Pending</p>
                  <span class="pending-state {{ 'yes' if pending_info else '' }}">{{ "yes" if pending_info else "no" }}</span>
                </div>
                {% if pending_info %}
                  <div class="pending-grid">
                    <div class="pending-item"><span>Exchange</span><b>{{pending_info["exchange"]}}</b></div>
                    <div class="pending-item"><span>Status</span><b>{{pending_info["phase"]}}</b></div>
                    <div class="pending-item"><span>Pair</span><b>{{pending_info["symbol"]}}</b></div>
                    <div class="pending-item"><span>Amount</span><b>{{pending_info["spend_usdt"]}} {{pending_info["spend_currency"]}}</b></div>
                    {% if pending_info.get("target_display") %}
                      <div class="pending-item"><span>Target</span><b>{{pending_info["target_display"]}}</b></div>
                    {% endif %}
                  </div>
                {% else %}
                  <p class="pending-empty">No active pending order.</p>
                {% endif %}
              </div>
            </div>
          </div>

          <div class="mt-5 flex flex-wrap gap-2">
            {% for ex in listing_exchanges %}
              {% set row = order_latency.get(ex) or {} %}
              <button
                class="tab-btn latency-chip {% if ex == selected_exchange_id %}active{% endif %}"
                data-tab="{{ex}}"
                data-latency-ex="{{ex}}"
                data-ms="{{row.get('engine_latency_text') or '-'}}"
                data-probe="1"
              >
                {{ex}}
                {% if errors.get(ex) %}<span class="pill err" title="{{errors.get(ex)[:120]}}">ERR</span>{% endif %}
                {% if exchange_modes.get(ex, {}).get("degraded") %}
                  <span class="pill warn" title="{{exchange_modes.get(ex, {}).get('degraded_reason','degraded mode')}}">{{'DEGRADED'}}</span>
                {% endif %}
              </button>
            {% endfor %}
          </div>

          <div class="mt-3 panel !shadow-none !border-slate-200 !rounded-[10px] p-2 bg-slate-50" id="latency-detail">
            <p class="latency-line"><b>Latency</b><span id="latency-ms-view">{{latency_selected_row.get("engine_latency_text") or "-"}}</span></p>
          </div>
          <form id="probe-form" method="POST" action="/probe-latency" style="display:none">
            <input type="hidden" name="probe_exchange" id="probe-exchange" value="{{latency_selected}}">
            <input type="hidden" name="probe_symbol" id="probe-symbol" value="">
            <input type="hidden" name="probe_spend_usdt" id="probe-spend" value="{{last_order.get('spend_usdt','5')}}">
          </form>

          <div class="mt-3 flex-1 overflow-visible xl:overflow-y-auto pr-1">
            {% for ex in listing_exchanges %}
              <section class="exchange-panel {% if ex == selected_exchange_id %}active{% endif %}" id="panel-{{ex}}">
                <p class="text-sm font-bold mb-2 flex flex-wrap items-center gap-2">
                  {% if checks.get(ex) is sameas true %}
                    <span class="check-ok">✓ Feed checked</span>
                  {% elif checks.get(ex) is sameas false %}
                    <span class="check-bad">✕ Feed check failed</span>
                  {% else %}
                    <span class="muted">Waiting for check...</span>
                  {% endif %}
                  {% set mode = exchange_modes.get(ex, {}) %}
                  {% if mode.get("degraded") %}
                    <span class="pill warn" title="{{mode.get('degraded_reason','degraded mode')}}">DEGRADED</span>
                  {% endif %}
                  <span class="pill {{ 'src-api' if mode.get('active_source') == 'api_announcement' else ('src-diff' if mode.get('active_source') == 'symbol_diff' else 'src-web') }}">
                    source: {{mode.get("active_label","Web fallback")}}
                  </span>
                </p>
                {% if mode.get("degraded") %}
                  <p class="muted text-xs mb-2">Primary source unavailable, running in fallback mode.</p>
                {% endif %}
                {% if listing_events.get(ex) %}
                  <div class="news-list">
                    {% for ev in listing_events.get(ex, [])[:16] %}
                      <div class="news-item">
                        <div class="news-title-row">
                          <div class="news-title">{{ ev.get("canonical_symbol") or "-" }}</div>
                          {% if ev.get("status") == "verified" %}
                            <span class="pill new">verified</span>
                          {% else %}
                            <span class="pill warn">candidate</span>
                          {% endif %}
                        </div>
                        <div class="news-meta">
                          <span>{{ ev.get("detected_display") or "-" }}</span>
                          <div class="news-actions">
                            <a class="news-open" href="{{ ev.get('internal_link') }}">Pair Link</a>
                            {% if ev.get("trade_url") %}
                              <a class="news-open" href="{{ ev.get('trade_url') }}" target="_blank" rel="noopener">Exchange Link</a>
                            {% endif %}
                          </div>
                        </div>
                      </div>
                    {% endfor %}
                  </div>
                {% else %}
                  <div class="empty">
                    {% if errors.get(ex) %}
                      Data could not be fetched for this exchange.
                      {% if friendly_errors.get(ex) %} {{friendly_errors.get(ex)}}{% endif %}
                    {% else %}
                      No new listings found for this exchange.
                    {% endif %}
                  </div>
                {% endif %}
              </section>
            {% endfor %}
          </div>
        </div>

        <div class="xl:col-span-7 panel p-4 sm:p-6 md:p-8 flex flex-col trade-panel">
          <div class="flex items-center justify-between mb-6 md:mb-8">
            <h3 class="text-lg font-bold text-slate-800 m-0">Quick Trade</h3>
            <a href="/settings" class="h-10 rounded-full border border-[#334155] inline-flex items-center justify-center gap-2 text-[#334155] hover:bg-slate-50 no-underline px-4 text-[13px] leading-none font-semibold" aria-label="Settings" title="Settings">
              <svg xmlns="http://www.w3.org/2000/svg" class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path stroke-linecap="round" stroke-linejoin="round" d="M12 15.5A3.5 3.5 0 1 0 12 8.5a3.5 3.5 0 0 0 0 7z"/>
                <path stroke-linecap="round" stroke-linejoin="round" d="M19.4 15a1.6 1.6 0 0 0 .32 1.76l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.6 1.6 0 0 0-1.76-.32 1.6 1.6 0 0 0-.97 1.47V21a2 2 0 1 1-4 0v-.09a1.6 1.6 0 0 0-.97-1.47 1.6 1.6 0 0 0-1.76.32l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.6 1.6 0 0 0 4.6 15a1.6 1.6 0 0 0-1.47-.97H3a2 2 0 1 1 0-4h.09A1.6 1.6 0 0 0 4.56 9.06a1.6 1.6 0 0 0-.32-1.76l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.6 1.6 0 0 0 1.76.32h.01a1.6 1.6 0 0 0 .96-1.47V3a2 2 0 1 1 4 0v.09a1.6 1.6 0 0 0 .97 1.47h.01a1.6 1.6 0 0 0 1.76-.32l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.6 1.6 0 0 0-.32 1.76v.01a1.6 1.6 0 0 0 1.47.96H21a2 2 0 1 1 0 4h-.09a1.6 1.6 0 0 0-1.47.97V15z"/>
              </svg>
              <span>Settings</span>
            </a>
          </div>

          <form method="POST" action="/arm" class="trade-grid mb-6 md:mb-8" id="arm-form">
            <div class="trade-field">
              <label class="block text-[10px] font-bold text-slate-400 mb-2 uppercase tracking-wide">Exchange</label>
              <select class="input-box trade-field-control text-slate-700 outline-none cursor-pointer bg-white" name="exchange" id="exchange-select">
                {% for ex in all_exchanges %}
                  <option
                    value="{{ex}}"
                    {% if ex not in trade_exchanges %}disabled{% endif %}
                    {% if ((trade_draft.get("exchange") or last_order.get("exchange")) == ex) and ex in trade_exchanges %}selected{% endif %}
                  >{{ex}}</option>
                {% endfor %}
              </select>
            </div>
            <div class="trade-field">
              <label class="block text-[10px] font-bold text-slate-400 mb-2 uppercase tracking-wide">Pair</label>
              <input id="symbol-input" name="symbol" type="text" list="quick-pair-list" class="input-box trade-field-control text-slate-700 outline-none bg-white" placeholder="ABC_USDT / ABCUSDT / ABC-USDT" value="{{trade_draft.get('order_symbol') or last_order.get('symbol','')}}" required>
              <datalist id="quick-pair-list"></datalist>
            </div>
            <div class="trade-field">
              <label id="quick-amount-label" class="block text-[10px] font-bold text-slate-400 mb-2 uppercase tracking-wide">Amount (USDT)</label>
              <input id="spend-input" name="spend_usdt" type="number" min="0.01" step="0.01" value="{{last_order.get('spend_usdt','5')}}" class="input-box trade-field-control text-slate-700 outline-none bg-white" required>
            </div>
            <input type="hidden" id="listing-title-input" name="listing_title" value="{{last_order.get('listing_title','')}}">
            <input type="hidden" id="listing-url-input" name="listing_url" value="{{last_order.get('listing_url','')}}">
            <input type="hidden" id="contract-hint-input" name="contract_hint" value="{{last_order.get('contract_hint','')}}">
          </form>

          <div class="trade-actions">
            <div class="action-wrap">
              <button type="submit" form="arm-form" class="btn-main buy-btn py-4 flex items-center justify-center gap-2 shadow-sm w-full">
                <span>Buy</span>
              </button>
            </div>
            <form method="POST" action="/sell-market" id="sell-form" class="action-wrap">
              <input type="hidden" name="exchange" id="sell-exchange" value="{{last_order.get('exchange','')}}">
              <input type="hidden" name="symbol" id="sell-symbol" value="{{last_order.get('symbol_normalized','')}}">
              <button type="submit" class="btn-main stop-btn py-4 flex items-center justify-center gap-2 shadow-sm w-full">
                <span>Sell</span>
              </button>
            </form>
          </div>

          {% if current_open_position %}
            <p class="open-pos-note">
              Open position: {{current_open_position.get("symbol","-")}} · qty {{current_open_position.get("base_qty_display","-")}}
              · buy {{current_open_position.get("buy_price_display","-")}} · {{current_open_position.get("buy_time_display","-")}}
            </p>
          {% endif %}

          {% if countdown %}
            <div class="countdown-box" data-target="{{countdown['target_iso']}}" id="countdown-box">
              <div>Time remaining for <b>{{countdown["exchange"]}} / {{countdown["symbol"]}}</b></div>
              <div class="countdown-value" id="countdown-value">--:--:--</div>
              <div class="muted text-sm">{{countdown["target_iso"]}}</div>
            </div>
          {% endif %}

          <div class="trade-history">
            <p class="trade-history-head">Trade History</p>
            {% if trade_cycles %}
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Exchange</th>
                      <th>Pair</th>
                      <th>Buy Price</th>
                      <th>Buy Qty</th>
                      <th>Buy Time</th>
                      <th>Sell Price</th>
                      <th>Sell Qty</th>
                      <th>Sell Time</th>
                      <th>P/L</th>
                    </tr>
                  </thead>
                  <tbody>
                    {% for row in trade_cycles %}
                      <tr>
                        <td>{{row.get("exchange","-")}}</td>
                        <td>{{row.get("symbol","-")}}</td>
                        <td>{{row.get("buy_price_display","-")}}</td>
                        <td>{{row.get("buy_qty_display","-")}}</td>
                        <td>{{row.get("buy_time_display","-")}}</td>
                        <td>{{row.get("sell_price_display","-")}}</td>
                        <td>{{row.get("sell_qty_display","-")}}</td>
                        <td>{{row.get("sell_time_display","-")}}</td>
                        <td class="{{ 'pnl-pos' if row.get('pnl_positive') else ('pnl-neg' if row.get('pnl_display') != '-' else '') }}">
                          {{row.get("pnl_display","-")}}{% if row.get("pnl_pct_display") and row.get("pnl_pct_display") != "-" %} ({{row.get("pnl_pct_display")}}){% endif %}
                        </td>
                      </tr>
                    {% endfor %}
                  </tbody>
                </table>
              </div>
            {% else %}
              <div class="empty">No completed buy/sell cycle yet.</div>
            {% endif %}
          </div>
        </div>

        <div class="xl:col-span-7 panel p-4 min-h-[144px] font-mono text-xs overflow-hidden bg-slate-50">
          {% if last_action %}
            <div class="logs-row">
              <span class="text-slate-400 w-20">status</span>
              <span class="font-bold {{ 'text-green-700' if last_action.get('ok') else 'text-red-700' }}">{{last_action.get("text","-")}}</span>
            </div>
          {% endif %}
          {% if last_exec %}
            <div class="logs-row">
              <span class="text-slate-400 w-20">last order</span>
              <span>{{last_exec.get("exchange","-")}} / {{last_exec.get("symbol","-")}} / {{last_exec.get("engine_latency_text","-")}}</span>
            </div>
          {% endif %}
          <div class="logs-row">
            <span class="text-slate-400 w-20">poll</span>
            <span>{{last_poll or "no data yet"}}</span>
          </div>
        </div>

        <div class="xl:col-span-12 panel p-4 sm:p-5">
          <div class="flex flex-wrap items-center gap-2 justify-between mb-2">
            <h3 class="text-sm font-bold text-slate-800 m-0">Other Exchanges Check (ARM Stage)</h3>
            {% if market_presence.get("checked") %}
              <span class="pill {{ 'new' if market_presence.get('found') else 'warn' }}">
                {{ "Found" if market_presence.get("found") else "Not found" }}
              </span>
            {% else %}
              <span class="pill">No scan yet</span>
            {% endif %}
          </div>

          {% if market_presence.get("checked") %}
            <div class="presence-box">
              <div class="presence-meta">
                <span class="pill">{{market_presence.get("method_label","-")}}</span>
                {% if market_presence.get("warning_symbol_only") %}
                  <span class="pill warn">symbol-only result</span>
                {% endif %}
                {% if market_presence.get("warning_ambiguous") %}
                  <span class="pill warn">ambiguous match</span>
                {% endif %}
                {% if market_presence.get("scan_ms") %}
                  <span class="pill">scan {{market_presence.get("scan_ms")}} ms</span>
                {% endif %}
                {% if market_presence.get("at_display") and market_presence.get("at_display") != "no data yet" %}
                  <span class="pill">{{market_presence.get("at_display")}}</span>
                {% endif %}
              </div>
              <p class="presence-summary">{{market_presence.get("explain","Cross-exchange scan finished.")}}</p>

              {% if market_presence.get("coin_label") %}
                <p class="presence-summary"><b>Resolved asset:</b> {{market_presence.get("coin_label")}}</p>
              {% endif %}

              <div class="presence-grid">
                <div class="presence-kpi">
                  <b>Top 3 liquid exchanges</b>
                  <span>{{ market_presence.get("top_exchanges", [])|join(", ") if market_presence.get("top_exchanges") else "-" }}</span>
                </div>
                <div class="presence-kpi">
                  <b>Reference price</b>
                  <span>{{market_presence.get("reference_price","-")}}</span>
                </div>
                <div class="presence-kpi">
                  <b>Price range</b>
                  <span>{{market_presence.get("price_min","-")}} → {{market_presence.get("price_max","-")}}</span>
                </div>
              </div>

              {% if market_presence.get("searched_global") or market_presence.get("searched_turkey") %}
                <div class="presence-grid">
                  <div class="presence-kpi">
                    <b>Searched (Global)</b>
                    <span>{{ market_presence.get("searched_global", [])|join(", ") if market_presence.get("searched_global") else "-" }}</span>
                  </div>
                  <div class="presence-kpi">
                    <b>Searched (Turkey)</b>
                    <span>{{ market_presence.get("searched_turkey", [])|join(", ") if market_presence.get("searched_turkey") else "-" }}</span>
                  </div>
                </div>
              {% endif %}

              {% if market_presence.get("warning_ambiguous") and market_presence.get("candidates") %}
                <div class="presence-candidates">
                  <b>Closest candidates</b>
                  <ul>
                    {% for c in market_presence.get("candidates", []) %}
                      <li>{{c.get("name","-")}} ({{c.get("symbol","-")}}) {% if c.get("id") %}- {{c.get("id")}}{% endif %}</li>
                    {% endfor %}
                  </ul>
                </div>
              {% endif %}

              {% if market_presence.get("error") %}
                <div class="empty">Scan error: {{market_presence.get("error")}}</div>
              {% elif market_presence.get("rows") %}
                <div class="presence-table-wrap">
                  <table class="presence-table">
                    <thead>
                      <tr>
                        <th>Exchange</th>
                        <th>Pair</th>
                        <th>Market</th>
                        <th>Last</th>
                        <th>24h Vol</th>
                        <th>Bid</th>
                        <th>Ask</th>
                        <th>Spread</th>
                        <th>Source</th>
                      </tr>
                    </thead>
                    <tbody>
                      {% for row in market_presence.get("rows", []) %}
                        <tr>
                          <td>{{row.get("exchange","-")}}</td>
                          <td>{{row.get("pair","-")}}</td>
                          <td>{{row.get("market_type","-")}}</td>
                          <td>{{row.get("last_price_display","-")}}</td>
                          <td>{{row.get("volume_24h_display","-")}}</td>
                          <td>{{row.get("bid_display","-")}}</td>
                          <td>{{row.get("ask_display","-")}}</td>
                          <td>{{row.get("spread_display","-")}}</td>
                          <td>{{row.get("source","-")}}</td>
                        </tr>
                      {% endfor %}
                    </tbody>
                  </table>
                </div>
              {% else %}
                <div class="empty">
                  This token is not found on other major exchanges.
                  {% if market_presence.get("searched_display") %}Searched: {{market_presence.get("searched_display")}}.{% endif %}
                </div>
              {% endif %}
            </div>
          {% else %}
            <div class="empty">
              No ARM-stage cross-exchange scan yet. Start an ARM action (especially Gate listings) to run this check.
            </div>
          {% endif %}
        </div>
      </div>
    </main>
  </div>

  <div
    id="last-order-signal"
    data-id="{{last_order_signal.get('id','')}}"
    data-side="{{last_order_signal.get('side','')}}"
    data-success="{{ '1' if last_order_signal.get('success') else '0' }}"
    style="display:none"
  ></div>
  <div
    id="trade-draft-signal"
    data-focus-request-id="{{focus_request_id}}"
    data-order-symbol="{{trade_draft.get('order_symbol','')}}"
    data-exchange="{{trade_draft.get('exchange','')}}"
    style="display:none"
  ></div>

  <script>
    const tabs = document.querySelectorAll('.tab-btn');
    const panels = document.querySelectorAll('.exchange-panel[id^="panel-"]');
    const exchangeSelect = document.getElementById('exchange-select');
    const symbolInput = document.getElementById('symbol-input');
    const quickPairList = document.getElementById('quick-pair-list');
    const spendInput = document.getElementById('spend-input');
    const quickAmountLabel = document.getElementById('quick-amount-label');
    const sellExchangeInput = document.getElementById('sell-exchange');
    const sellSymbolInput = document.getElementById('sell-symbol');
    const listingTitleInput = document.getElementById('listing-title-input');
    const listingUrlInput = document.getElementById('listing-url-input');
    const contractHintInput = document.getElementById('contract-hint-input');
    const probeForm = document.getElementById('probe-form');
    const probeExchangeInput = document.getElementById('probe-exchange');
    const probeSymbolInput = document.getElementById('probe-symbol');
    const probeSpendInput = document.getElementById('probe-spend');
    const latencyMsView = document.getElementById('latency-ms-view');
    const latencyChips = document.querySelectorAll('.latency-chip[data-latency-ex]');
    const pairOptionsByExchange = {{ pair_options_map | tojson }};

    const detectQuoteCurrency = (rawPair, ex) => {
      const exNorm = String(ex || '').toLowerCase();
      const raw = String(rawPair || '').trim().toUpperCase();
      const fallback = exNorm === 'paribu' ? 'TL' : (exNorm === 'btcturk' ? 'TRY' : 'USDT');
      if (!raw) return fallback;
      let quote = '';
      for (const sep of ['_', '-', '/']) {
        if (raw.includes(sep)) {
          const parts = raw.split(sep);
          if (parts.length >= 2) quote = (parts[1] || '').replace(/[^A-Z0-9]/g, '');
          break;
        }
      }
      if (!quote) {
        const compact = raw.replace(/[^A-Z0-9]/g, '');
        const suffixes = ['USDT', 'USDC', 'TRY', 'TL', 'BTC', 'ETH', 'EUR', 'USD'];
        for (const s of suffixes) {
          if (compact.length > s.length && compact.endsWith(s)) {
            quote = s;
            break;
          }
        }
      }
      if (exNorm === 'paribu' && quote === 'TRY') quote = 'TL';
      return quote || fallback;
    };

    const refreshQuickAmountLabel = () => {
      if (!quickAmountLabel) return;
      const ex = (exchangeSelect && exchangeSelect.value) ? exchangeSelect.value : '';
      const pair = (symbolInput && symbolInput.value) ? symbolInput.value : '';
      const quote = detectQuoteCurrency(pair, ex);
      quickAmountLabel.textContent = `Amount (${quote})`;
    };

    const setDatalistOptions = (listEl, values) => {
      if (!listEl) return;
      listEl.innerHTML = '';
      (values || []).forEach((val) => {
        const txt = String(val || '').trim();
        if (!txt) return;
        const opt = document.createElement('option');
        opt.value = txt;
        listEl.appendChild(opt);
      });
    };

    const refreshQuickPairList = () => {
      const ex = (exchangeSelect && exchangeSelect.value) ? String(exchangeSelect.value).toLowerCase() : '';
      const values = (pairOptionsByExchange && pairOptionsByExchange[ex]) ? pairOptionsByExchange[ex] : [];
      setDatalistOptions(quickPairList, values);
    };

    tabs.forEach((tab) => {
      tab.addEventListener('click', () => {
        const key = tab.getAttribute('data-tab');
        tabs.forEach((t) => t.classList.remove('active'));
        panels.forEach((p) => p.classList.remove('active'));
        tab.classList.add('active');
        const panel = document.getElementById('panel-' + key);
        if (panel) panel.classList.add('active');
        fetch('/select-exchange', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest'
          },
          body: `exchange=${encodeURIComponent(key || '')}`
        }).catch(() => {});
      });
    });

    document.querySelectorAll('.pair-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const pair = btn.getAttribute('data-pair') || '';
        const ex = btn.getAttribute('data-exchange') || '';
        const title = btn.getAttribute('data-title') || '';
        const url = btn.getAttribute('data-url') || '';
        if (exchangeSelect && ex) exchangeSelect.value = ex;
        if (symbolInput && pair) {
          symbolInput.value = pair;
          symbolInput.focus();
        }
        if (listingTitleInput) listingTitleInput.value = title;
        if (listingUrlInput) listingUrlInput.value = url;
        if (contractHintInput) contractHintInput.value = '';
        syncSellForm();
        const topCard = document.querySelector('.trade-panel');
        if (topCard) topCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
      });
    });

    const clearListingContext = () => {
      if (listingTitleInput) listingTitleInput.value = '';
      if (listingUrlInput) listingUrlInput.value = '';
      if (contractHintInput) contractHintInput.value = '';
    };
    if (symbolInput) symbolInput.addEventListener('input', clearListingContext);
    if (exchangeSelect) exchangeSelect.addEventListener('change', clearListingContext);

    const syncSellForm = () => {
      if (sellExchangeInput && exchangeSelect) {
        sellExchangeInput.value = exchangeSelect.value || '';
      }
      if (sellSymbolInput && symbolInput) {
        sellSymbolInput.value = (symbolInput.value || '').trim();
      }
    };
    syncSellForm();
    if (exchangeSelect) exchangeSelect.addEventListener('change', syncSellForm);
    if (symbolInput) symbolInput.addEventListener('input', syncSellForm);
    if (symbolInput) symbolInput.addEventListener('change', syncSellForm);
    if (exchangeSelect) exchangeSelect.addEventListener('change', refreshQuickAmountLabel);
    if (symbolInput) symbolInput.addEventListener('input', refreshQuickAmountLabel);
    if (symbolInput) symbolInput.addEventListener('change', refreshQuickAmountLabel);
    if (exchangeSelect) exchangeSelect.addEventListener('change', refreshQuickPairList);
    refreshQuickPairList();
    refreshQuickAmountLabel();

    const setActiveLatencyChip = (chip) => {
      if (!chip) return;
      latencyChips.forEach((c) => c.classList.remove('active'));
      chip.classList.add('active');
      const ex = chip.getAttribute('data-latency-ex') || '';
      const ms = chip.getAttribute('data-ms') || '-';
      if (latencyMsView) latencyMsView.textContent = ms;
      if (probeExchangeInput && ex) probeExchangeInput.value = ex;
    };

    latencyChips.forEach((chip) => {
      chip.addEventListener('click', () => {
        setActiveLatencyChip(chip);
        const probeEnabled = (chip.getAttribute('data-probe') || '0') === '1';
        if (!probeEnabled) {
          if (latencyMsView) latencyMsView.textContent = '-';
          return;
        }
        if (probeSymbolInput) probeSymbolInput.value = '';
        if (probeSpendInput) {
          const spend = (spendInput && spendInput.value) ? spendInput.value : (probeSpendInput.value || '5');
          probeSpendInput.value = spend;
        }
        const formData = new URLSearchParams();
        formData.set('probe_exchange', (probeExchangeInput && probeExchangeInput.value) ? probeExchangeInput.value : '');
        formData.set('probe_symbol', (probeSymbolInput && probeSymbolInput.value) ? probeSymbolInput.value : '');
        formData.set('probe_spend_usdt', (probeSpendInput && probeSpendInput.value) ? probeSpendInput.value : '5');

        fetch('/probe-latency', {
          method: 'POST',
          cache: 'no-store',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/json'
          },
          body: formData.toString()
        })
        .then(async (res) => {
          let data = {};
          try { data = await res.json(); } catch (_) {}
          if (!res.ok || !data.ok) throw new Error((data && data.error) ? data.error : 'Request failed');
          const txt = (data && data.latency_text) ? data.latency_text : '-';
          chip.setAttribute('data-ms', txt);
          if (latencyMsView) latencyMsView.textContent = txt;
        })
        .catch(() => {
          chip.setAttribute('data-ms', '-');
          if (latencyMsView) latencyMsView.textContent = '-';
        });
      });
    });

    if (spendInput && probeSpendInput) {
      spendInput.addEventListener('input', () => {
        probeSpendInput.value = spendInput.value || '5';
      });
    }

    const countdownValue = document.getElementById('countdown-value');
    const countdownBox = document.getElementById('countdown-box');
    if (countdownValue && countdownBox) {
      const targetRaw = countdownBox.getAttribute('data-target');
      const targetTs = Date.parse(targetRaw);
      const tick = () => {
        if (Number.isNaN(targetTs)) {
          countdownValue.textContent = '--:--:--';
          return;
        }
        const diff = targetTs - Date.now();
        if (diff <= 0) {
          countdownValue.textContent = '00:00:00';
          return;
        }
        const total = Math.floor(diff / 1000);
        const days = Math.floor(total / 86400);
        const hours = Math.floor((total % 86400) / 3600);
        const mins = Math.floor((total % 3600) / 60);
        const secs = total % 60;
        const hh = String(hours).padStart(2, '0');
        const mm = String(mins).padStart(2, '0');
        const ss = String(secs).padStart(2, '0');
        countdownValue.textContent = days > 0 ? `${days}d ${hh}:${mm}:${ss}` : `${hh}:${mm}:${ss}`;
      };
      tick();
      setInterval(tick, 1000);
    }

    const playTradeTone = (side) => {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      if (!AudioCtx) return;
      try {
        const ctx = new AudioCtx();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = 'sine';
        osc.frequency.value = side === 'sell' ? 392 : 880;
        gain.gain.value = 0.001;
        osc.connect(gain);
        gain.connect(ctx.destination);
        const now = ctx.currentTime;
        gain.gain.exponentialRampToValueAtTime(0.08, now + 0.01);
        gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.24);
        osc.start(now);
        osc.stop(now + 0.26);
      } catch (_) {}
    };

    const signalEl = document.getElementById('last-order-signal');
    if (signalEl) {
      const signalId = String(signalEl.dataset.id || '');
      const side = String(signalEl.dataset.side || '').toLowerCase();
      const success = String(signalEl.dataset.success || '0') === '1';
      const key = 'quickbot:last-order-signal-id';
      const prev = sessionStorage.getItem(key);
      if (signalId && signalId !== prev) {
        sessionStorage.setItem(key, signalId);
        if (success && (side === 'buy' || side === 'sell')) {
          playTradeTone(side);
        }
      }
    }

    const tradeDraftSignal = document.getElementById('trade-draft-signal');
    if (tradeDraftSignal && symbolInput) {
      const reqId = String(tradeDraftSignal.dataset.focusRequestId || '');
      const orderSymbol = String(tradeDraftSignal.dataset.orderSymbol || '');
      const draftExchange = String(tradeDraftSignal.dataset.exchange || '');
      const key = 'quickbot:focus-request-id';
      const consumed = sessionStorage.getItem(key);
      if (reqId && reqId !== consumed) {
        sessionStorage.setItem(key, reqId);
        if (draftExchange && exchangeSelect) {
          exchangeSelect.value = draftExchange;
          refreshQuickPairList();
        }
        if (orderSymbol) {
          symbolInput.value = orderSymbol;
          syncSellForm();
          refreshQuickAmountLabel();
        }
        let tries = 0;
        const focusRetry = () => {
          tries += 1;
          try {
            symbolInput.focus();
            symbolInput.select();
          } catch (_) {}
          if (document.activeElement !== symbolInput && tries < 12) {
            requestAnimationFrame(focusRetry);
          }
        };
        requestAnimationFrame(focusRetry);
      }
    }
  </script>
</body>
</html>"""

SETTINGS_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Settings Center</title>
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Montserrat", sans-serif;
      color: #334155;
      background-color: #ffffff;
      background-image: radial-gradient(#cbd5e1 1px, transparent 1px);
      background-size: 24px 24px;
      min-height: 100dvh;
    }
    .wrap {
      max-width: 980px;
      margin: 0 auto;
      padding: 24px 20px;
      display: grid;
      gap: 16px;
    }
    .panel {
      background: #ffffff;
      border: 1px solid #334155;
      border-radius: 12px;
      box-shadow: 0 4px 0 #e2e8f0;
      padding: 20px;
    }
    .top {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .title {
      margin: 0;
      font-size: 24px;
      font-weight: 800;
      color: #1e293b;
      line-height: 1.1;
    }
    .sub {
      margin: 8px 0 0;
      font-size: 13px;
      line-height: 1.45;
      color: #64748b;
      max-width: 650px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .card {
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      background: #f8fafc;
      padding: 14px;
      display: grid;
      gap: 8px;
    }
    .card h2 {
      margin: 0;
      font-size: 16px;
      font-weight: 800;
      color: #1e293b;
    }
    .card p {
      margin: 0;
      font-size: 13px;
      line-height: 1.45;
      color: #64748b;
      min-height: 38px;
    }
    .btn {
      border: 1px solid #334155;
      border-radius: 10px;
      background: #fff;
      color: #334155;
      padding: 9px 12px;
      font-size: 12px;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
      min-height: 38px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 100%;
      transition: all .2s ease;
    }
    .btn:hover {
      background: #334155;
      color: #fff;
    }
    .btn.back {
      width: auto;
      border-radius: 999px;
      padding: 8px 14px;
    }
    @media (max-width: 780px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="panel">
      <div class="top">
        <div>
          <h1 class="title">Settings Center</h1>
          <p class="sub">Access both the test tools and API credentials from one place.</p>
        </div>
        <a class="btn back" href="/">Back to Panel</a>
      </div>
    </section>

    <section class="panel">
      <div class="grid">
        <article class="card">
          <h2>API Settings</h2>
          <p>Update exchange API keys, secrets, and base URLs.</p>
          <a class="btn" href="/admin">Open API Settings</a>
        </article>
        <article class="card">
          <h2>Test Center</h2>
          <p>Run system checks and review connection and status results.</p>
          <a class="btn" href="/tests">Open Test Page</a>
        </article>
        <article class="card">
          <h2>Real Trade Test</h2>
          <p>Run a real buy (and optional sell) on a selected exchange and get execution speed report.</p>
          <a class="btn" href="/real-tests">Open Real Test</a>
        </article>
        <article class="card">
          <h2>Scanner Monitor</h2>
          <p>View exchange-by-exchange listing scan health and latest listing outputs in one page.</p>
          <a class="btn" href="/scanner">Open Scanner</a>
        </article>
      </div>
    </section>
  </main>
</body>
</html>"""

ADMIN_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin Settings</title>
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; }
    body {
      font-family: "Montserrat", sans-serif;
      background: #ffffff;
      color: #334155;
      background-image: radial-gradient(#cbd5e1 1px, transparent 1px);
      background-size: 24px 24px;
      min-height: 100dvh;
      margin: 0;
    }
    .wrap {
      max-width: 1120px;
      margin: 0 auto;
      padding: 24px 20px 28px;
      display: grid;
      gap: 14px;
    }
    .panel {
      background: #fff;
      border: 1px solid #334155;
      border-radius: 12px;
      box-shadow: 0 3px 0 #e2e8f0;
      padding: 16px;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 0;
      margin: 0;
      font-size: 24px;
      font-weight: 800;
      line-height: 1.1;
      color: #1e293b;
    }
    .brand-wordmark {
      display: inline-flex;
      align-items: baseline;
      gap: 0;
      line-height: 1;
    }
    .brand-word-main { color: #1f3b73; }
    .brand-word-accent { color: #8b5e34; }
    .brand-word-sub { color: #334155; opacity: .78; }
    .sub {
      margin: 6px 0 0;
      color: #64748b;
      font-size: 12px;
      font-weight: 500;
      line-height: 1.35;
    }
    .top {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .field {
      display: grid;
      gap: 4px;
    }
    .field label {
      font-size: 10px;
      font-weight: 700;
      letter-spacing: .35px;
      color: #64748b;
      text-transform: uppercase;
    }
    input, button, .btn {
      border: 1px solid #cbd5e1;
      border-radius: 9px;
      background: #fff;
      color: #334155;
      padding: 9px 10px;
      font-size: 12px;
      font-weight: 600;
      outline: none;
      width: 100%;
      min-height: 40px;
    }
    input:focus {
      border-color: #334155;
      box-shadow: 0 0 0 2px rgba(51,65,85,.08);
    }
    .msg {
      padding: 9px 11px;
      border-radius: 9px;
      margin-top: 10px;
      font-size: 12px;
      font-weight: 600;
    }
    .msg.ok { background: #f0fdf4; border: 1px solid #86efac; color: #166534; }
    .msg.err { background: #fef2f2; border: 1px solid #fecaca; color: #991b1b; }
    .cards {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .ex-card {
      border: 1px solid #d7e0eb;
      border-radius: 10px;
      padding: 11px;
      background: #fcfdff;
      display: grid;
      gap: 8px;
    }
    .ex-card h3 {
      margin: 0;
      font-size: 14px;
      font-weight: 700;
      color: #1e293b;
      letter-spacing: .2px;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      margin-left: 5px;
      border-radius: 999px;
      border: 1px solid #bfdbfe;
      background: #eff6ff;
      color: #1d4ed8;
      padding: 1px 6px;
      font-size: 9px;
      font-weight: 700;
      vertical-align: middle;
      line-height: 1.2;
    }
    .actions {
      margin-top: 12px;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .btn {
      border: 1px solid #334155;
      border-radius: 999px;
      background: #fff;
      color: #334155;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: .6px;
      text-transform: uppercase;
      text-decoration: none;
      min-height: 40px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      transition: all .2s ease;
    }
    .btn:hover { background: #334155; color: #fff; }
    .btn.primary {
      background: #334155;
      color: #fff;
    }
    .btn.primary:hover {
      background: #1e293b;
      border-color: #1e293b;
    }
    @media (max-width: 720px) {
      .cards { grid-template-columns: 1fr; }
      .actions { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="panel">
      <form method="POST" action="/admin">
        <div class="top">
          <h1 class="brand">API Settings</h1>
          <a class="btn" href="/settings">Back to Settings</a>
        </div>
        <p class="sub">Fill key/secret fields only when you want to update them. Leave blank to keep existing values.</p>
        {% if msg %}
          <div class="msg {{ 'ok' if ok else 'err' }}">{{msg}}</div>
        {% endif %}

        <section class="cards" style="margin-top:12px;">
          <article class="ex-card">
            <h3>Gate.io</h3>
            <div class="field">
              <label>API Key {% if flags["GATE_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="GATE_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["GATE_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="GATE_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>Base URL</label>
              <input name="GATE_BASE" value="{{cfg['GATE_BASE']}}">
            </div>
          </article>

          <article class="ex-card">
            <h3>Binance</h3>
            <div class="field">
              <label>API Key {% if flags["BINANCE_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BINANCE_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["BINANCE_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BINANCE_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>Base URL</label>
              <input name="BINANCE_BASE" value="{{cfg['BINANCE_BASE']}}">
            </div>
          </article>

          <article class="ex-card">
            <h3>MEXC</h3>
            <div class="field">
              <label>API Key {% if flags["MEXC_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="MEXC_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["MEXC_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="MEXC_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>Base URL</label>
              <input name="MEXC_BASE" value="{{cfg['MEXC_BASE']}}">
            </div>
          </article>

          <article class="ex-card">
            <h3>KuCoin</h3>
            <div class="field">
              <label>API Key {% if flags["KUCOIN_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="KUCOIN_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["KUCOIN_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="KUCOIN_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>Passphrase {% if flags["KUCOIN_PASSPHRASE"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="KUCOIN_PASSPHRASE" placeholder="type to update">
            </div>
            <div class="field">
              <label>Base URL</label>
              <input name="KUCOIN_BASE" value="{{cfg['KUCOIN_BASE']}}">
            </div>
          </article>

          <article class="ex-card">
            <h3>Bitget</h3>
            <div class="field">
              <label>API Key {% if flags["BITGET_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BITGET_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["BITGET_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BITGET_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>Passphrase {% if flags["BITGET_PASSPHRASE"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BITGET_PASSPHRASE" placeholder="type to update">
            </div>
            <div class="field">
              <label>Base URL</label>
              <input name="BITGET_BASE" value="{{cfg['BITGET_BASE']}}">
            </div>
          </article>

          <article class="ex-card">
            <h3>OKX</h3>
            <div class="field">
              <label>API Key {% if flags["OKEX_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="OKEX_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["OKEX_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="OKEX_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>Passphrase {% if flags["OKEX_PASSPHRASE"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="OKEX_PASSPHRASE" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Base URL</label>
              <input name="OKEX_BASE" value="{{cfg['OKEX_BASE']}}">
            </div>
          </article>

          <article class="ex-card">
            <h3>Bybit</h3>
            <div class="field">
              <label>API Key {% if flags["BYBIT_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BYBIT_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["BYBIT_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BYBIT_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Base URL</label>
              <input name="BYBIT_BASE" value="{{cfg['BYBIT_BASE']}}">
            </div>
          </article>

          <article class="ex-card">
            <h3>BtcTurk</h3>
            <div class="field">
              <label>API Key {% if flags["BTCTURK_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BTCTURK_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["BTCTURK_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="BTCTURK_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Base URL</label>
              <input name="BTCTURK_BASE" value="{{cfg['BTCTURK_BASE']}}">
            </div>
          </article>

          <article class="ex-card">
            <h3>Paribu</h3>
            <div class="field">
              <label>API Key {% if flags["PARIBU_KEY"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="PARIBU_KEY" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Secret {% if flags["PARIBU_SECRET"] %}<span class="chip">saved</span>{% endif %}</label>
              <input type="password" name="PARIBU_SECRET" placeholder="type to update">
            </div>
            <div class="field">
              <label>API Base URL</label>
              <input name="PARIBU_BASE" value="{{cfg['PARIBU_BASE']}}">
            </div>
          </article>
        </section>

        <div class="actions">
          <a class="btn" href="/settings">Back to Settings</a>
          <button type="submit" class="btn primary">Save and Apply Settings</button>
        </div>
      </form>
    </section>
  </main>
</body>
</html>"""

TESTS_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>quickbot test center</title>
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Montserrat", sans-serif;
      color: #334155;
      background-color: #ffffff;
      background-image: radial-gradient(#cbd5e1 1px, transparent 1px);
      background-size: 24px 24px;
      min-height: 100dvh;
    }
    .wrap {
      max-width: 1240px;
      margin: 0 auto;
      padding: 20px;
      display: grid;
      gap: 16px;
    }
    .panel {
      background: #ffffff;
      border: 1px solid #334155;
      border-radius: 12px;
      box-shadow: 0 4px 0 #e2e8f0;
      padding: 16px;
    }
    .top {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }
    .title {
      margin: 0;
      font-size: 24px;
      line-height: 1.1;
      font-weight: 800;
      color: #1e293b;
    }
    .sub {
      margin: 8px 0 0;
      font-size: 13px;
      line-height: 1.45;
      color: #64748b;
      max-width: 760px;
    }
    .nav {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .btn {
      border: 1px solid #334155;
      border-radius: 10px;
      background: #fff;
      color: #334155;
      padding: 9px 12px;
      font-size: 12px;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
      min-height: 38px;
    }
    .btn:hover {
      background: #334155;
      color: #fff;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .stat {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      background: #f8fafc;
      padding: 10px;
    }
    .stat h3 {
      margin: 0 0 5px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .5px;
      color: #64748b;
      font-weight: 700;
    }
    .stat p {
      margin: 0;
      font-size: 14px;
      line-height: 1.35;
      font-weight: 700;
      color: #1e293b;
      word-break: break-word;
    }
    .ok { color: #166534 !important; }
    .bad { color: #991b1b !important; }
    .actions-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .action-card {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      background: #f8fafc;
      padding: 11px;
      display: grid;
      gap: 8px;
    }
    .action-card h3 {
      margin: 0;
      font-size: 14px;
      line-height: 1.3;
      font-weight: 800;
      color: #1e293b;
    }
    .action-card p {
      margin: 0;
      font-size: 12px;
      line-height: 1.45;
      color: #64748b;
      min-height: 34px;
    }
    .result-list {
      display: grid;
      gap: 10px;
    }
    .result-card {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      background: #f8fafc;
      padding: 11px;
    }
    .result-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .result-title {
      margin: 0;
      font-size: 14px;
      font-weight: 800;
      color: #1e293b;
    }
    .pill {
      border-radius: 999px;
      border: 1px solid #cbd5e1;
      background: #fff;
      color: #334155;
      font-size: 11px;
      font-weight: 700;
      line-height: 1;
      padding: 4px 9px;
    }
    .pill.ok {
      border-color: #86efac;
      background: #f0fdf4;
      color: #166534;
    }
    .pill.bad {
      border-color: #fecaca;
      background: #fef2f2;
      color: #991b1b;
    }
    .time-line {
      margin: 4px 0 0;
      font-size: 11px;
      color: #64748b;
      font-weight: 600;
    }
    .result-summary {
      margin: 8px 0 0;
      font-size: 13px;
      line-height: 1.45;
      color: #334155;
      font-weight: 600;
      background: #ffffff;
      border: 1px solid #e2e8f0;
      border-radius: 8px;
      padding: 8px 10px;
    }
    .result-lines {
      margin: 8px 0 0;
      padding-left: 18px;
      display: grid;
      gap: 4px;
      color: #334155;
      font-size: 12px;
      line-height: 1.45;
      font-weight: 500;
    }
    .result-lines li {
      margin: 0;
    }
    details {
      margin-top: 8px;
      border: 1px dashed #cbd5e1;
      border-radius: 8px;
      padding: 8px;
      background: #fff;
    }
    summary {
      cursor: pointer;
      font-size: 12px;
      font-weight: 700;
      color: #334155;
      list-style: none;
    }
    summary::-webkit-details-marker { display: none; }
    pre {
      margin: 8px 0 0;
      background: #0f172a;
      color: #e2e8f0;
      border-radius: 8px;
      padding: 10px;
      overflow: auto;
      font-size: 11px;
      line-height: 1.45;
      max-height: 340px;
    }
    .empty {
      border: 1px dashed #cbd5e1;
      border-radius: 10px;
      background: #fff;
      padding: 14px;
      color: #64748b;
      font-size: 13px;
      line-height: 1.45;
    }
    @media (max-width: 980px) {
      .stats { grid-template-columns: 1fr; }
      .actions-grid { grid-template-columns: 1fr; }
      .action-card p { min-height: 0; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="panel">
      <div class="top">
        <div>
          <h1 class="title">quickbot test center</h1>
          <p class="sub">Run technical checks in one click without touching the main trading screen.</p>
        </div>
        <div class="nav">
          <a class="btn" href="/">Back to Panel</a>
          <a class="btn" href="/settings">Settings</a>
          <a class="btn" href="/scanner">Scanner</a>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="stats">
      <article class="stat">
        <h3>Last Check</h3>
        <p>{{last_poll_display or "no data"}}</p>
      </article>
      <article class="stat">
        <h3>System Status</h3>
        <p class="{{'ok' if exec_state.get('online') else 'bad'}}">{{"online" if exec_state.get("online") else "offline"}}</p>
      </article>
      <article class="stat">
        <h3>Last Action</h3>
        <p>{{last_action.get("text","none yet") if last_action else "none yet"}}</p>
      </article>
      </div>
    </section>

    <section class="panel">
      <h2 class="title" style="font-size:18px;margin-bottom:10px;">Run Tests</h2>
      <div class="actions-grid">
      {% for t in test_catalog %}
        <article class="action-card">
          <h3>{{t["title"]}}</h3>
          <p>{{t["desc"]}}</p>
          <form method="POST" action="/tests/run">
            <input type="hidden" name="action" value="{{t['id']}}">
            <button class="btn" type="submit" style="width:100%;">Run</button>
          </form>
        </article>
      {% endfor %}
      </div>
    </section>

    <section class="panel">
      <h2 class="title" style="font-size:18px;margin-bottom:10px;">Scanner Control</h2>
      <div class="nav" style="margin-bottom:10px;">
        <form method="POST" action="/scanner/start" style="margin:0;"><button class="btn" type="submit">Start</button></form>
        <form method="POST" action="/scanner/stop" style="margin:0;"><button class="btn" type="submit">Stop</button></form>
        <form method="POST" action="/scanner/run-once" style="margin:0;"><button class="btn" type="submit">RunOnce</button></form>
        <span class="pill {{'ok' if scanner_status.get('running') else 'bad'}}">{{"running" if scanner_status.get("running") else "stopped"}}</span>
        {% if scanner_status.get("hotExchange") %}
          <span class="pill">HOT: {{scanner_status.get("hotExchange")}}</span>
        {% endif %}
      </div>
      <div class="table-wrap" style="overflow:auto;border:1px solid #e2e8f0;border-radius:10px;">
        <table style="width:100%;border-collapse:collapse;min-width:1150px;">
          <thead>
            <tr>
              <th>Exchange</th>
              <th>Mode</th>
              <th>Interval</th>
              <th>Markets</th>
              <th>New (candidate/verified)</th>
              <th>Fetch</th>
              <th>Last success</th>
              <th>Error type</th>
              <th>Consecutive</th>
              <th>429</th>
              <th>Backoff until</th>
              <th>Set mode</th>
            </tr>
          </thead>
          <tbody>
            {% for row in scanner_rows %}
            <tr>
              <td style="font-weight:700;">{{row.exchange}}</td>
              <td>{{row.mode}}</td>
              <td>{{row.interval}} ms</td>
              <td>{{row.market_count}}</td>
              <td>{{row.candidate_new}} / {{row.verified_new}}</td>
              <td>{{row.fetch_ms}} ms</td>
              <td>{{row.last_success}}</td>
              <td title="{{row.last_error_message}}">{{row.last_error_type}}</td>
              <td>{{row.consecutive_errors}}</td>
              <td>{{row.rate_limit_hits}}</td>
              <td>{{row.backoff_until}}</td>
              <td>
                <form method="POST" action="/scanner/mode" style="display:flex;gap:6px;align-items:center;">
                  <input type="hidden" name="exchange" value="{{row.exchange}}">
                  <select name="mode" style="min-height:30px;padding:4px 6px;font-size:11px;">
                    <option value="NORMAL">NORMAL</option>
                    <option value="ALARM">ALARM</option>
                    <option value="HOT">HOT</option>
                  </select>
                  <input name="ttl_sec" type="number" min="0" step="1" placeholder="ttl" style="width:62px;min-height:30px;padding:4px 6px;font-size:11px;">
                  <button class="btn" type="submit" style="min-height:30px;padding:4px 8px;font-size:11px;">Set</button>
                </form>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2 class="title" style="font-size:18px;margin-bottom:10px;">Test Results</h2>
      {% if tests_pretty %}
      <div class="result-list">
      {% for row in tests_pretty %}
        <article class="result-card">
          <div class="result-head">
            <h3 class="result-title">{{row["title"]}}</h3>
            <span class="pill {{'ok' if row.get('ok') else 'bad'}}">{{"ok" if row.get("ok") else "error"}}</span>
          </div>
          <p class="time-line">{{row.get("at_display","")}}</p>
          <p class="result-summary">{{row.get("summary","Test result is ready.")}}</p>
          {% if row.get("lines") %}
            <ul class="result-lines">
              {% for line in row.get("lines", []) %}
                <li>{{line}}</li>
              {% endfor %}
            </ul>
          {% endif %}
          <details>
            <summary>Show technical output</summary>
            <pre>{{ row.get("data") | tojson(indent=2) }}</pre>
          </details>
        </article>
      {% endfor %}
      </div>
      {% else %}
        <div class="empty">No tests have been run yet. Pick one card above and press “Run”.</div>
      {% endif %}
    </section>
  </main>
</body>
</html>"""

SCANNER_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>quickbot scanner monitor</title>
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Montserrat", sans-serif;
      color: #334155;
      background-color: #ffffff;
      background-image: radial-gradient(#cbd5e1 1px, transparent 1px);
      background-size: 24px 24px;
      min-height: 100dvh;
    }
    .wrap {
      max-width: 1240px;
      margin: 0 auto;
      padding: 20px;
      display: grid;
      gap: 16px;
    }
    .panel {
      background: #ffffff;
      border: 1px solid #334155;
      border-radius: 12px;
      box-shadow: 0 4px 0 #e2e8f0;
      padding: 16px;
    }
    .top {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }
    .title {
      margin: 0;
      font-size: 24px;
      line-height: 1.1;
      font-weight: 800;
      color: #1e293b;
    }
    .sub {
      margin: 8px 0 0;
      font-size: 13px;
      line-height: 1.45;
      color: #64748b;
      max-width: 760px;
    }
    .nav {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    .btn {
      border: 1px solid #334155;
      border-radius: 10px;
      background: #fff;
      color: #334155;
      padding: 9px 12px;
      font-size: 12px;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
      min-height: 38px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }
    .btn:hover {
      background: #334155;
      color: #fff;
    }
    .btn.primary {
      background: #334155;
      color: #fff;
    }
    .btn.primary:hover {
      background: #1e293b;
      border-color: #1e293b;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }
    .stat {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      background: #f8fafc;
      padding: 10px;
    }
    .stat h3 {
      margin: 0 0 5px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .5px;
      color: #64748b;
      font-weight: 700;
    }
    .stat p {
      margin: 0;
      font-size: 14px;
      line-height: 1.35;
      font-weight: 700;
      color: #1e293b;
      word-break: break-word;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .card {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      background: #f8fafc;
      padding: 11px;
      display: grid;
      gap: 8px;
    }
    .card-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }
    .name {
      margin: 0;
      font-size: 15px;
      line-height: 1.3;
      font-weight: 800;
      color: #1e293b;
      text-transform: lowercase;
    }
    .chip {
      border-radius: 999px;
      border: 1px solid #cbd5e1;
      background: #fff;
      color: #334155;
      font-size: 10px;
      font-weight: 700;
      line-height: 1;
      padding: 4px 8px;
      text-transform: uppercase;
      letter-spacing: .3px;
    }
    .chip.ok {
      border-color: #86efac;
      background: #f0fdf4;
      color: #166534;
    }
    .chip.bad {
      border-color: #fecaca;
      background: #fef2f2;
      color: #991b1b;
    }
    .chip.warn {
      border-color: #fcd34d;
      background: #fffbeb;
      color: #92400e;
    }
    .meta {
      margin: 0;
      font-size: 12px;
      line-height: 1.4;
      color: #475569;
      font-weight: 600;
    }
    .pairs {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .pair {
      border: 1px solid #cbd5e1;
      background: #fff;
      border-radius: 999px;
      font-size: 11px;
      color: #334155;
      font-weight: 700;
      line-height: 1;
      padding: 5px 8px;
    }
    .err {
      border: 1px dashed #fecaca;
      border-radius: 8px;
      background: #fef2f2;
      color: #991b1b;
      font-size: 11px;
      line-height: 1.4;
      font-weight: 600;
      padding: 8px 9px;
    }
    @media (max-width: 1080px) {
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 760px) {
      .stats { grid-template-columns: 1fr; }
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="panel">
      <div class="top">
        <div>
          <h1 class="title">quickbot scanner monitor</h1>
          <p class="sub">Dedicated scanner section for listing-feed health, source mode, and latest listing pairs.</p>
        </div>
        <div class="nav">
          <a class="btn" href="/">Back to Panel</a>
          <a class="btn" href="/settings">Settings</a>
          <a class="btn" href="/tests">Tests</a>
          <a class="btn" href="/real-tests">Real Trade</a>
          <form method="POST" action="/scanner/run" style="margin:0;">
            <button class="btn primary" type="submit">Run Now</button>
          </form>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="stats">
        <article class="stat">
          <h3>Last Poll</h3>
          <p>{{last_poll_display or "no data"}}</p>
        </article>
        <article class="stat">
          <h3>Total Exchanges</h3>
          <p>{{exchange_count}}</p>
        </article>
        <article class="stat">
          <h3>Feeds OK</h3>
          <p>{{ok_count}}</p>
        </article>
        <article class="stat">
          <h3>Total Listings</h3>
          <p>{{total_items}}</p>
        </article>
      </div>
    </section>

    <section class="panel">
      <div class="grid">
        {% for ex in listing_exchanges %}
          {% set mode = exchange_modes.get(ex, {}) %}
          {% set items = listings.get(ex, []) %}
          {% set failed = checks.get(ex) is sameas false %}
          <article class="card">
            <div class="card-top">
              <h2 class="name">{{ex}}</h2>
              {% if failed %}
                <span class="chip bad">failed</span>
              {% elif checks.get(ex) is sameas true %}
                <span class="chip ok">ok</span>
              {% else %}
                <span class="chip">pending</span>
              {% endif %}
            </div>
            <p class="meta">source: {{mode.get("active_label","Web fallback")}}</p>
            {% if mode.get("degraded") %}
              <span class="chip warn">degraded</span>
            {% endif %}
            <p class="meta">listings: {{items|length}}</p>
            {% if items %}
              <div class="pairs">
                {% for item in items[:6] %}
                  <span class="pair">{{item.get("pair_guess") or "-"}}</span>
                {% endfor %}
              </div>
            {% else %}
              <p class="meta">No listing output in current window.</p>
            {% endif %}
            {% if errors.get(ex) %}
              <div class="err">{{friendly_errors.get(ex) or errors.get(ex)}}</div>
            {% endif %}
          </article>
        {% endfor %}
      </div>
    </section>
  </main>
</body>
</html>"""

REAL_TRADE_TEST_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>quickbot live trading console</title>
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Montserrat", sans-serif;
      color: #334155;
      background: #fff;
      background-image: radial-gradient(#cbd5e1 1px, transparent 1px);
      background-size: 24px 24px;
      min-height: 100dvh;
    }
    .wrap {
      max-width: 1100px;
      margin: 0 auto;
      padding: 20px;
      display: grid;
      gap: 14px;
    }
    .panel {
      background: #fff;
      border: 1px solid #334155;
      border-radius: 12px;
      box-shadow: 0 4px 0 #e2e8f0;
      padding: 16px;
    }
    .top {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .title {
      margin: 0;
      font-size: 24px;
      font-weight: 800;
      color: #1e293b;
      line-height: 1.1;
    }
    .sub {
      margin: 8px 0 0;
      font-size: 13px;
      line-height: 1.45;
      color: #64748b;
      max-width: 760px;
    }
    .nav { display: flex; gap: 8px; flex-wrap: wrap; }
    .btn {
      border: 1px solid #334155;
      border-radius: 10px;
      background: #fff;
      color: #334155;
      padding: 9px 12px;
      font-size: 12px;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
      min-height: 38px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }
    .btn:hover { background: #334155; color: #fff; }
    .btn.primary { background: #334155; color: #fff; }
    .btn.primary:hover { background: #1e293b; border-color: #1e293b; }
    .btn.buy {
      border-color: #22c55e;
      background: #dcfce7;
      color: #166534;
      min-width: 140px;
    }
    .btn.buy:hover { background: #22c55e; color: #fff; }
    .btn.sell {
      border-color: #f87171;
      background: #fee2e2;
      color: #991b1b;
      min-width: 140px;
    }
    .btn.sell:hover { background: #ef4444; color: #fff; }
    .btn.neutral {
      min-width: 140px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .field { display: grid; gap: 4px; }
    .field label {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: .5px;
      font-weight: 700;
      color: #64748b;
    }
    .pair-hint {
      margin: 2px 0 0;
      font-size: 11px;
      line-height: 1.35;
      color: #64748b;
      font-weight: 600;
    }
    .form-foot {
      margin-top: 8px;
      display: flex;
      justify-content: flex-end;
      width: 100%;
    }
    .form-foot .pair-hint {
      margin: 0;
      text-align: right;
    }
    input, select {
      border: 1px solid #cbd5e1;
      border-radius: 10px;
      min-height: 42px;
      padding: 9px 10px;
      font-size: 13px;
      font-weight: 600;
      background: #fff;
      color: #334155;
      outline: none;
      width: 100%;
    }
    input:focus, select:focus {
      border-color: #334155;
      box-shadow: 0 0 0 2px rgba(51,65,85,.08);
    }
    .toggle {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      color: #334155;
      font-weight: 600;
      margin-top: 8px;
    }
    .toggle input { width: auto; min-height: 0; }
    .warn {
      margin-top: 8px;
      border: 1px solid #fecaca;
      background: #fef2f2;
      color: #991b1b;
      border-radius: 10px;
      padding: 9px 11px;
      font-size: 12px;
      line-height: 1.4;
      font-weight: 600;
    }
    .ok {
      margin-top: 8px;
      border: 1px solid #86efac;
      background: #f0fdf4;
      color: #166534;
      border-radius: 10px;
      padding: 9px 11px;
      font-size: 12px;
      line-height: 1.4;
      font-weight: 600;
    }
    .results { display: grid; gap: 10px; }
    .card {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      background: #f8fafc;
      padding: 11px;
    }
    .card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }
    .card h3 {
      margin: 0;
      font-size: 14px;
      font-weight: 800;
      color: #1e293b;
    }
    .pill {
      border: 1px solid #cbd5e1;
      border-radius: 999px;
      padding: 4px 8px;
      background: #fff;
      font-size: 11px;
      font-weight: 700;
    }
    .pill.ok { border-color: #86efac; color: #166534; background: #f0fdf4; }
    .pill.bad { border-color: #fecaca; color: #991b1b; background: #fef2f2; }
    .meta {
      margin-top: 6px;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px 10px;
      font-size: 12px;
      line-height: 1.4;
      color: #334155;
    }
    .meta b { color: #1e293b; }
    .empty {
      border: 1px dashed #cbd5e1;
      border-radius: 10px;
      background: #fff;
      padding: 14px;
      color: #64748b;
      font-size: 13px;
      line-height: 1.45;
    }
    .actions {
      margin-top: 10px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
    }
    .stat-card {
      border: 1px solid #cbd5e1;
      border-radius: 10px;
      background: #f8fafc;
      padding: 10px;
      display: grid;
      gap: 4px;
    }
    .stat-card .k {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: .4px;
      color: #64748b;
      font-weight: 700;
    }
    .stat-card .v {
      font-size: 15px;
      color: #0f172a;
      font-weight: 800;
      line-height: 1.2;
    }
    .table-wrap {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      overflow: auto;
      background: #fff;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
    }
    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid #e2e8f0;
      text-align: left;
      font-size: 12px;
      color: #334155;
      white-space: nowrap;
    }
    th {
      background: #f8fafc;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .4px;
      color: #64748b;
      font-weight: 700;
    }
    .ok-txt { color: #166534; font-weight: 700; }
    .bad-txt { color: #991b1b; font-weight: 700; }
    @media (max-width: 960px) {
      .grid { grid-template-columns: 1fr; }
      .meta { grid-template-columns: 1fr; }
      .stat-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="panel">
      <div class="top">
        <div>
          <h1 class="title">Live Trading Console</h1>
          <p class="sub">Send real market buy/sell orders and track execution results with post-trade statistics.</p>
        </div>
        <div class="nav">
          <a class="btn" href="/settings">Settings</a>
          <a class="btn" href="/">Back to Panel</a>
        </div>
      </div>
      {% if exec_state.get("dry_run") %}
        <div class="warn">Executor is in test mode (DRY_RUN=1). Real trade test will not run until live mode is enabled.</div>
      {% else %}
        <div class="ok">Executor is in live mode. This page sends real orders.</div>
      {% endif %}
      {% if last_action %}
        <div class="{{'ok' if last_action.get('ok') else 'warn'}}">{{last_action.get("text","-")}}</div>
      {% endif %}
    </section>

    <section class="panel">
      <form method="POST" action="/real-tests/buy">
        <input type="hidden" name="client_click_ms" id="client-click-ms" value="">
        <input type="hidden" name="client_click_perf_ms" id="client-click-perf-ms" value="">
        <input type="hidden" name="execute_at_ms" id="execute-at-ms" value="">
        <input type="hidden" name="execute_at_iso" id="execute-at-iso" value="">
        <div class="grid">
          <div class="field">
            <label>Exchange</label>
            <select name="exchange" required>
              {% for ex in trade_exchanges %}
                <option value="{{ex}}" {% if form_data.get("exchange") == ex %}selected{% endif %}>{{ex}}</option>
              {% endfor %}
            </select>
          </div>
          <div class="field">
            <label>Pair</label>
            <input id="pair-input" name="symbol" value="{{form_data.get('symbol','')}}" list="real-pair-list" placeholder="BTC_USDT / BTCUSDT / BTC-USDT" required>
            <datalist id="real-pair-list"></datalist>
          </div>
          <div class="field">
            <label id="real-amount-label">Amount (USDT)</label>
            <input id="amount-input" name="spend_usdt" type="number" min="0.01" step="0.01" value="{{form_data.get('spend_usdt','5')}}" required>
          </div>
          <div class="field">
            <label>Amount Source</label>
            <select id="amount-mode" name="amount_mode">
              <option value="fixed" {% if form_data.get("amount_mode","fixed") == "fixed" %}selected{% endif %}>Manual value</option>
              <option value="all" {% if form_data.get("amount_mode") == "all" %}selected{% endif %}>Use all available balance</option>
              <option value="percent" {% if form_data.get("amount_mode") == "percent" %}selected{% endif %}>Use balance percentage</option>
            </select>
          </div>
          <div class="field">
            <label>Balance %</label>
            <input id="amount-percent" name="amount_percent" type="number" min="0.01" max="100" step="0.01" value="{{form_data.get('amount_percent','100')}}">
          </div>
          <div class="field">
            <label>Round-trip mode</label>
            <select id="roundtrip-mode" name="round_trip_mode">
              <option value="buy_then_sell" {% if form_data.get("round_trip_mode","buy_then_sell") == "buy_then_sell" %}selected{% endif %}>Buy -> Sell (get base then return)</option>
              <option value="sell_then_buy" {% if form_data.get("round_trip_mode") == "sell_then_buy" %}selected{% endif %}>Sell -> Buy (sell base then buy back)</option>
            </select>
          </div>
          <div class="field">
            <label>Execution mode</label>
            <select id="exec-mode" name="exec_mode">
              <option value="now" {% if form_data.get("exec_mode","now") == "now" %}selected{% endif %}>Run now</option>
              <option value="scheduled" {% if form_data.get("exec_mode") == "scheduled" %}selected{% endif %}>Run at scheduled time</option>
            </select>
          </div>
          <div class="field">
            <label>Scheduled time (local)</label>
            <input id="schedule-at-local" name="schedule_at_local" type="datetime-local" step="0.001" value="{{form_data.get('schedule_at_local','')}}">
          </div>
        </div>
        <label class="toggle"><input type="checkbox" name="auto_sell" value="1" {% if form_data.get("auto_sell") == "1" %}checked{% endif %}><span id="auto-reverse-label">Auto-sell after buy</span></label>
        <div class="actions">
          <button class="btn buy" type="submit" formaction="/real-tests/buy">Buy</button>
          <button class="btn sell" type="submit" formaction="/real-tests/sell">Sell</button>
          <button class="btn neutral" type="submit" formaction="/real-tests/run">Round-trip Test</button>
        </div>
        <div class="form-foot">
          <p class="pair-hint" id="pair-hint">Format: BTC_USDT / BTCUSDT / BTC-USDT</p>
        </div>
      </form>
    </section>

    <section class="panel">
      <h2 class="title" style="font-size:18px;margin-bottom:10px;">Live Stats</h2>
      <div class="stat-grid">
        <article class="stat-card"><span class="k">Completed cycles</span><span class="v">{{stats.get("cycles_total","0")}}</span></article>
        <article class="stat-card"><span class="k">Win rate</span><span class="v">{{stats.get("win_rate","-")}}</span></article>
        <article class="stat-card"><span class="k">Total P/L</span><span class="v">{{stats.get("pnl_total","-")}}</span></article>
        <article class="stat-card"><span class="k">Avg P/L</span><span class="v">{{stats.get("pnl_avg","-")}}</span></article>
        <article class="stat-card"><span class="k">Orders sent</span><span class="v">{{stats.get("orders_total","0")}}</span></article>
        <article class="stat-card"><span class="k">Avg buy latency</span><span class="v">{{stats.get("buy_latency_avg","-")}}</span></article>
        <article class="stat-card"><span class="k">Avg sell latency</span><span class="v">{{stats.get("sell_latency_avg","-")}}</span></article>
        <article class="stat-card"><span class="k">Open position</span><span class="v">{{stats.get("open_position","no")}}</span></article>
      </div>
    </section>

    <section class="panel">
      <h2 class="title" style="font-size:18px;margin-bottom:10px;">Wallet Balances ({{wallet_exchange}})</h2>
      <div style="font-size:12px;color:#64748b;font-weight:600;margin-bottom:10px;">Fetch latency: {{wallet_latency}}</div>
      {% if wallet_error %}
        <div class="warn">Wallet data could not be fetched: {{wallet_error}}</div>
      {% elif wallet_rows %}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Asset</th>
                <th>Available</th>
                <th>Locked</th>
                <th>Total</th>
              </tr>
            </thead>
            <tbody>
              {% for row in wallet_rows %}
                <tr>
                  <td>{{row.get("currency","-")}}</td>
                  <td>{{row.get("available","-")}}</td>
                  <td>{{row.get("locked","-")}}</td>
                  <td>{{row.get("total","-")}}</td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      {% else %}
        <div class="empty">No non-zero wallet balance found for this exchange.</div>
      {% endif %}
    </section>

    <section class="panel">
      <h2 class="title" style="font-size:18px;margin-bottom:10px;">Recent Orders</h2>
      {% if recent_orders %}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Exchange</th>
                <th>Pair</th>
                <th>Side</th>
                <th>Status</th>
                <th>Price</th>
                <th>Qty</th>
                <th>Quote</th>
                <th>Latency</th>
                <th>Order ID</th>
              </tr>
            </thead>
            <tbody>
              {% for row in recent_orders %}
                <tr>
                  <td>{{row.get("at_display","-")}}</td>
                  <td>{{row.get("exchange","-")}}</td>
                  <td>{{row.get("symbol","-")}}</td>
                  <td>{{row.get("side","-")}}</td>
                  <td class="{{'ok-txt' if row.get('success') else 'bad-txt'}}">{{row.get("status_text","-")}}</td>
                  <td>{{row.get("price_display","-")}}</td>
                  <td>{{row.get("qty_display","-")}}</td>
                  <td>{{row.get("quote_display","-")}}</td>
                  <td>{{row.get("latency_text","-")}}</td>
                  <td>{{row.get("order_id","-")}}</td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      {% else %}
        <div class="empty">No real order record yet.</div>
      {% endif %}
    </section>

    <section class="panel">
      <h2 class="title" style="font-size:18px;margin-bottom:10px;">Completed Trade Cycles</h2>
      {% if recent_cycles %}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Exchange</th>
                <th>Pair</th>
                <th>Buy Time</th>
                <th>Buy Price</th>
                <th>Sell Time</th>
                <th>Sell Price</th>
                <th>Qty</th>
                <th>P/L</th>
              </tr>
            </thead>
            <tbody>
              {% for row in recent_cycles %}
                <tr>
                  <td>{{row.get("exchange","-")}}</td>
                  <td>{{row.get("symbol","-")}}</td>
                  <td>{{row.get("buy_time_display","-")}}</td>
                  <td>{{row.get("buy_price_display","-")}}</td>
                  <td>{{row.get("sell_time_display","-")}}</td>
                  <td>{{row.get("sell_price_display","-")}}</td>
                  <td>{{row.get("qty_display","-")}}</td>
                  <td class="{{'ok-txt' if row.get('pnl_positive') else 'bad-txt'}}">{{row.get("pnl_display","-")}} {% if row.get("pnl_pct_display") and row.get("pnl_pct_display") != "-" %}({{row.get("pnl_pct_display")}}){% endif %}</td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      {% else %}
        <div class="empty">No completed buy/sell cycle yet.</div>
      {% endif %}
    </section>

    <section class="panel">
      <h2 class="title" style="font-size:18px;margin-bottom:10px;">Reports</h2>
      {% if real_tests %}
        <div class="results">
          {% for row in real_tests %}
            <article class="card">
              <div class="card-head">
                <h3>{{row.get("exchange","-")}} / {{row.get("symbol","-")}}</h3>
                <span class="pill {{ 'ok' if row.get('ok') else 'bad' }}">{{ "ok" if row.get("ok") else "error" }}</span>
              </div>
              <div class="meta">
                <div><b>Time:</b> {{row.get("at_display","-")}}</div>
                <div><b>Flow:</b> {{ row.get("mode_display") or ("Sell only" if row.get("kind") == "sell" else ("Buy only" if row.get("kind") == "buy" else "-")) }}</div>
                <div><b>Amount:</b> {{row.get("amount_display","-")}}</div>
                <div><b>UI -> VPS:</b> {{row.get("ui_to_vps","-")}}</div>
                <div><b>Panel -> Executor:</b> {{row.get("panel_to_executor","-")}}</div>
                <div><b>Executor flow:</b> {{row.get("executor_flow","-")}}</div>
                <div><b>VPS -> Exchange (buy):</b> {{row.get("vps_to_exchange_buy","-")}}</div>
                <div><b>VPS -> Exchange (sell):</b> {{row.get("vps_to_exchange_sell","-")}}</div>
                <div><b>Click -> done:</b> {{row.get("click_to_done","-")}}</div>
                <div><b>Scheduled for:</b> {{row.get("scheduled_for","-")}}</div>
                <div><b>First order at:</b> {{row.get("first_order_at","-")}}</div>
                <div><b>Schedule drift:</b> {{row.get("schedule_drift","-")}}</div>
                <div><b>Buy route:</b> {{row.get("buy_latency","-")}}</div>
                <div><b>Buy status:</b> {{row.get("buy_status","-")}}</div>
                <div><b>Buy note:</b> {{row.get("buy_error","-") if row.get("buy_error") else "-"}}</div>
                <div><b>Buy price:</b> {{row.get("buy_price","-")}}</div>
                <div><b>Buy qty:</b> {{row.get("buy_qty","-")}}</div>
                <div><b>Sell route:</b> {{row.get("sell_latency","-")}}</div>
                <div><b>Sell status:</b> {{row.get("sell_status","-")}}</div>
                <div><b>Sell note:</b> {{row.get("sell_error","-") if row.get("sell_error") else "-"}}</div>
                <div><b>Round-trip:</b> {{row.get("round_trip","-")}}</div>
              </div>
            </article>
          {% endfor %}
        </div>
      {% else %}
        <div class="empty">No real trade test report yet.</div>
      {% endif %}
    </section>
  </main>
  <script>
    (function () {
      const exchangeEl = document.querySelector('select[name="exchange"]');
      const pairEl = document.getElementById('pair-input');
      const pairListEl = document.getElementById('real-pair-list');
      const hintEl = document.getElementById('pair-hint');
      const amountLabelEl = document.getElementById('real-amount-label');
      const modeEl = document.getElementById('roundtrip-mode');
      const amountModeEl = document.getElementById('amount-mode');
      const amountPercentEl = document.getElementById('amount-percent');
      const amountInputEl = document.getElementById('amount-input');
      const autoReverseLabelEl = document.getElementById('auto-reverse-label');
      const execModeEl = document.getElementById('exec-mode');
      const scheduleAtLocalEl = document.getElementById('schedule-at-local');
      const formEl = document.querySelector('form[action="/real-tests/buy"]');
      const clientClickMsEl = document.getElementById('client-click-ms');
      const clientClickPerfMsEl = document.getElementById('client-click-perf-ms');
      const executeAtMsEl = document.getElementById('execute-at-ms');
      const executeAtIsoEl = document.getElementById('execute-at-iso');
      const pairOptionsByExchange = {{ pair_options_map | tojson }};
      const formatHints = {
        gate: "Format: BTC_USDT (Gate standard)",
        mexc: "Format: BTCUSDT (normalized if you write BTC_USDT or BTC-USDT)",
        kucoin: "Format: BTC-USDT (KuCoin standard)",
        bitget: "Format: BTCUSDT (normalized if you write BTC_USDT or BTC-USDT)",
        binance: "Format: BTCUSDT (normalized if you write BTC_USDT or BTC-USDT)",
        okex: "Format: BTCUSDT (for test route)",
        bybit: "Format: BTCUSDT (for test route)",
        btcturk: "Format: BTCTRY or BTC_USDT (normalized by app)",
        paribu: "Format: btc_tl (Paribu standard, lowercase with underscore)",
      };
      const placeholders = {
        gate: "BTC_USDT",
        mexc: "BTCUSDT",
        kucoin: "BTC-USDT",
        bitget: "BTCUSDT",
        binance: "BTCUSDT",
        okex: "BTCUSDT",
        bybit: "BTCUSDT",
        btcturk: "BTCTRY",
        paribu: "btc_tl",
      };
      function detectQuoteCurrency(rawPair, ex) {
        const exNorm = String(ex || '').toLowerCase();
        const raw = String(rawPair || '').trim().toUpperCase();
        const fallback = exNorm === 'paribu' ? 'TL' : (exNorm === 'btcturk' ? 'TRY' : 'USDT');
        if (!raw) return fallback;
        let quote = '';
        for (const sep of ['_', '-', '/']) {
          if (raw.includes(sep)) {
            const parts = raw.split(sep);
            if (parts.length >= 2) quote = (parts[1] || '').replace(/[^A-Z0-9]/g, '');
            break;
          }
        }
        if (!quote) {
          const compact = raw.replace(/[^A-Z0-9]/g, '');
          const suffixes = ['USDT', 'USDC', 'TRY', 'TL', 'BTC', 'ETH', 'EUR', 'USD'];
          for (const s of suffixes) {
            if (compact.length > s.length && compact.endsWith(s)) {
              quote = s;
              break;
            }
          }
        }
        if (exNorm === 'paribu' && quote === 'TRY') quote = 'TL';
        return quote || fallback;
      }
      function detectBaseCurrency(rawPair) {
        const raw = String(rawPair || '').trim().toUpperCase();
        if (!raw) return 'BASE';
        for (const sep of ['_', '-', '/']) {
          if (raw.includes(sep)) {
            const parts = raw.split(sep);
            const base = (parts[0] || '').replace(/[^A-Z0-9]/g, '');
            return base || 'BASE';
          }
        }
        const compact = raw.replace(/[^A-Z0-9]/g, '');
        const suffixes = ['USDT', 'USDC', 'TRY', 'TL', 'BTC', 'ETH', 'EUR', 'USD'];
        for (const s of suffixes) {
          if (compact.length > s.length && compact.endsWith(s)) {
            return compact.slice(0, -s.length) || 'BASE';
          }
        }
        return compact || 'BASE';
      }
      function setDatalistOptions(values) {
        if (!pairListEl) return;
        pairListEl.innerHTML = '';
        (values || []).forEach((val) => {
          const txt = String(val || '').trim();
          if (!txt) return;
          const opt = document.createElement('option');
          opt.value = txt;
          pairListEl.appendChild(opt);
        });
      }
      function refreshPairOptions() {
        const ex = ((exchangeEl && exchangeEl.value) || "").toLowerCase();
        const values = (pairOptionsByExchange && pairOptionsByExchange[ex]) ? pairOptionsByExchange[ex] : [];
        setDatalistOptions(values);
      }
      function refreshPairHint() {
        const ex = ((exchangeEl && exchangeEl.value) || "").toLowerCase();
        if (hintEl) hintEl.textContent = formatHints[ex] || "Format: BTCUSDT";
        if (pairEl && !pairEl.value.trim()) pairEl.placeholder = placeholders[ex] || "BTCUSDT";
        if (amountLabelEl) {
          const rawPair = (pairEl && pairEl.value) || '';
          const mode = ((modeEl && modeEl.value) || 'buy_then_sell').toLowerCase();
          if (mode === 'sell_then_buy') {
            const base = detectBaseCurrency(rawPair);
            amountLabelEl.textContent = `Amount (${base} qty)`;
          } else {
            const quote = detectQuoteCurrency(rawPair, ex);
            amountLabelEl.textContent = `Amount (${quote})`;
          }
        }
      }
      function refreshAutoReverseLabel() {
        const mode = ((modeEl && modeEl.value) || 'buy_then_sell').toLowerCase();
        if (!autoReverseLabelEl) return;
        autoReverseLabelEl.textContent = (mode === 'sell_then_buy')
          ? 'Auto-buy after sell'
          : 'Auto-sell after buy';
      }
      function syncAmountModeUI() {
        const mode = ((amountModeEl && amountModeEl.value) || 'fixed').toLowerCase();
        const isFixed = mode === 'fixed';
        if (amountPercentEl) {
          amountPercentEl.disabled = mode !== 'percent';
          amountPercentEl.style.opacity = mode === 'percent' ? '1' : '.55';
        }
        if (amountInputEl) {
          amountInputEl.required = isFixed;
          amountInputEl.disabled = !isFixed;
          amountInputEl.style.opacity = isFixed ? '1' : '.6';
          if (mode === 'all') {
            amountInputEl.placeholder = 'auto from full balance';
          } else if (mode === 'percent') {
            amountInputEl.placeholder = 'auto from selected % balance';
          } else {
            amountInputEl.placeholder = '';
          }
        }
      }
      function syncScheduleUI() {
        const mode = ((execModeEl && execModeEl.value) || 'now').toLowerCase();
        const isScheduled = mode === 'scheduled';
        if (scheduleAtLocalEl) {
          scheduleAtLocalEl.disabled = !isScheduled;
          scheduleAtLocalEl.required = isScheduled;
          scheduleAtLocalEl.style.opacity = isScheduled ? '1' : '.6';
        }
      }
      function syncPairByModeIfEmpty() {
        const ex = ((exchangeEl && exchangeEl.value) || "").toLowerCase();
        if (!pairEl || pairEl.value.trim()) return;
        const values = (pairOptionsByExchange && pairOptionsByExchange[ex]) ? pairOptionsByExchange[ex] : [];
        if (values && values.length > 0) {
          pairEl.value = values[0];
        }
      }
      if (exchangeEl) exchangeEl.addEventListener("change", refreshPairHint);
      if (exchangeEl) exchangeEl.addEventListener("change", refreshPairOptions);
      if (exchangeEl) exchangeEl.addEventListener("change", syncPairByModeIfEmpty);
      if (pairEl) pairEl.addEventListener("input", refreshPairHint);
      if (pairEl) pairEl.addEventListener("change", refreshPairHint);
      if (modeEl) modeEl.addEventListener("change", refreshPairHint);
      if (modeEl) modeEl.addEventListener("change", refreshAutoReverseLabel);
      if (modeEl) modeEl.addEventListener("change", syncPairByModeIfEmpty);
      if (amountModeEl) amountModeEl.addEventListener("change", syncAmountModeUI);
      if (execModeEl) execModeEl.addEventListener("change", syncScheduleUI);
      if (formEl) {
        formEl.addEventListener("submit", function () {
          if (clientClickMsEl) clientClickMsEl.value = String(Date.now());
          if (clientClickPerfMsEl && window.performance && typeof window.performance.now === "function") {
            clientClickPerfMsEl.value = String(window.performance.now());
          }
          if (executeAtMsEl) executeAtMsEl.value = "";
          if (executeAtIsoEl) executeAtIsoEl.value = "";
          const mode = ((execModeEl && execModeEl.value) || "now").toLowerCase();
          if (mode === "scheduled" && scheduleAtLocalEl && scheduleAtLocalEl.value) {
            const dt = new Date(scheduleAtLocalEl.value);
            if (!Number.isNaN(dt.getTime())) {
              if (executeAtMsEl) executeAtMsEl.value = String(dt.getTime());
              if (executeAtIsoEl) executeAtIsoEl.value = dt.toISOString();
            }
          }
        });
      }
      refreshPairOptions();
      syncPairByModeIfEmpty();
      refreshPairHint();
      refreshAutoReverseLabel();
      syncAmountModeUI();
      syncScheduleUI();
    })();
  </script>
</body>
</html>"""


def executor_get_status():
    default = {
        "online": False,
        "dry_run": True,
        "gate": {"armed": False, "phase": "idle", "symbol": None, "spend_usdt": None, "target_iso": None},
        "arm_state": {},
        "last_execution": {},
        "order_latency": {},
        "market_presence": {},
        "order_history": [],
        "trade_cycles": [],
        "open_positions": [],
        "last_order_signal": {},
    }
    try:
        r = requests.get(EXECUTOR_URL + "/status", timeout=5)
        r.raise_for_status()
        data = r.json()
        return {
            "online": True,
            "dry_run": bool(data.get("dry_run", True)),
            "gate": data.get("gate", default["gate"]),
            "arm_state": data.get("arm_state") or {},
            "last_execution": data.get("last_execution") or {},
            "order_latency": data.get("order_latency") or {},
            "market_presence": data.get("market_presence") or {},
            "order_history": data.get("order_history") or [],
            "trade_cycles": data.get("trade_cycles") or [],
            "open_positions": data.get("open_positions") or [],
            "last_order_signal": data.get("last_order_signal") or {},
        }
    except Exception as e:
        default["error"] = str(e)
        return default


def executor_get(path: str, use_token: bool = False, timeout_sec: int = 8):
    headers = {"X-PILOT-TOKEN": PILOT_TOKEN} if use_token else {}
    r = requests.get(EXECUTOR_URL + path, headers=headers, timeout=timeout_sec)
    try:
        out = r.json()
    except Exception:
        out = {}
    if r.status_code >= 400:
        detail = out.get("detail") if isinstance(out, dict) else None
        if not detail:
            detail = (r.text or "").strip()[:240] or f"HTTP {r.status_code}"
        raise RuntimeError(f"{r.status_code}: {detail}")
    return out or {"ok": True}


def executor_post(path: str, payload=None, timeout_sec: int = 8):
    headers = {"X-PILOT-TOKEN": PILOT_TOKEN}
    r = requests.post(EXECUTOR_URL + path, json=payload, headers=headers, timeout=timeout_sec)
    try:
        out = r.json()
    except Exception:
        out = {}
    if r.status_code >= 400:
        detail = out.get("detail") if isinstance(out, dict) else None
        if not detail:
            detail = (r.text or "").strip()[:240] or f"HTTP {r.status_code}"
        raise RuntimeError(f"{r.status_code}: {detail}")
    return out or {"ok": True}


def set_action(ok: bool, text: str):
    STATE["last_action"] = {"ok": ok, "text": text[:260]}


def _load_notified_urls():
    try:
        if not os.path.exists(NOTIFY_STATE_FILE):
            return set()
        with open(NOTIFY_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(x).strip() for x in data if str(x).strip()}
    except Exception:
        pass
    return set()


def _save_notified_urls(urls: set):
    try:
        folder = os.path.dirname(NOTIFY_STATE_FILE)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(NOTIFY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(urls), f, ensure_ascii=False)
    except Exception:
        pass


NOTIFIED_URLS = _load_notified_urls()


def _send_notify_message(text: str):
    if not NOTIFY_ENABLED:
        return
    payload_text = (text or "").strip()
    if not payload_text:
        return

    sent_any = False
    if NOTIFY_TELEGRAM_BOT_TOKEN and NOTIFY_TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{NOTIFY_TELEGRAM_BOT_TOKEN}/sendMessage"
            r = requests.post(
                url,
                json={"chat_id": NOTIFY_TELEGRAM_CHAT_ID, "text": payload_text},
                timeout=8,
            )
            sent_any = sent_any or (r.status_code < 400)
        except Exception:
            pass

    if NOTIFY_WEBHOOK_URL:
        try:
            r = requests.post(
                NOTIFY_WEBHOOK_URL,
                json={"text": payload_text, "kind": "new_listing"},
                timeout=8,
            )
            sent_any = sent_any or (r.status_code < 400)
        except Exception:
            pass

    # Always keep short in-memory trace for UI/debug
    recent = list(STATE.get("notifications") or [])
    recent.append({"at": iso_utc(), "text": payload_text[:500], "sent": bool(sent_any)})
    STATE["notifications"] = recent[-30:]


def _notify_new_listings(listings: dict):
    global NOTIFIED_URLS
    if not NOTIFY_ENABLED:
        return

    new_events = []
    for ex, rows in (listings or {}).items():
        for item in (rows or []):
            url = str((item or {}).get("url") or "").strip()
            if not url:
                continue
            first_seen_now = bool((item or {}).get("first_seen_now"))
            if not first_seen_now and url in NOTIFIED_URLS:
                continue
            pair = str((item or {}).get("pair_guess") or guess_pair(item or {})).strip()
            pair = normalize_symbol_for_exchange(ex, pair) if pair else "-"
            trade_start = format_trade_start(item or {})
            title = str((item or {}).get("title") or "").strip()
            msg = (
                "NEW LISTING\n"
                f"Exchange: {ex}\n"
                f"Pair: {pair}\n"
                f"Trade Start: {trade_start}\n"
                f"Title: {title}\n"
                f"URL: {url}"
            )
            new_events.append((url, msg))

    if not new_events:
        return

    for url, msg in new_events:
        _send_notify_message(msg)
        NOTIFIED_URLS.add(url)
    _save_notified_urls(NOTIFIED_URLS)


QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BTC", "ETH", "TRY", "EUR", "BNB")
PAIR_STOPWORDS = {
    "LISTING", "LISTINGS", "SPOT", "FUTURES", "TRADE", "MARKET", "TOKEN", "EVENT",
    "UTC", "GMT", "NEW", "ZONE", "PRE", "SOON", "WILL", "LIST",
}
PAIR_DEFAULTS = {
    "gate": ["BTC_USDT", "ETH_USDT", "SOL_USDT"],
    "mexc": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "kucoin": ["BTC-USDT", "ETH-USDT", "SOL-USDT"],
    "bitget": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "okex": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "bybit": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "btcturk": ["BTCTRY", "ETHTRY", "USDTTRY"],
    "paribu": ["btc_tl", "eth_tl", "usdt_tl"],
}


def split_symbol_parts(symbol: str):
    raw = re.sub(r"\s+", "", (symbol or "").upper())
    if not raw:
        return "", ""
    for sep in ("_", "-", "/"):
        if sep in raw:
            left, right = raw.split(sep, 1)
            left = re.sub(r"[^A-Z0-9]", "", left)
            right = re.sub(r"[^A-Z0-9]", "", right)
            return left, right
    compact = re.sub(r"[^A-Z0-9]", "", raw)
    if not compact:
        return "", ""
    for q in sorted(QUOTE_SUFFIXES, key=len, reverse=True):
        if compact.endswith(q) and len(compact) > len(q):
            return compact[: -len(q)], q
    return compact, ""


def normalize_symbol_for_exchange(exchange: str, symbol: str) -> str:
    ex = (exchange or "").strip().lower()
    base, quote = split_symbol_parts(symbol)
    if not base and not quote:
        return ""
    if not quote:
        quote = "USDT"
    if ex == "gate":
        return f"{base}_{quote}"
    if ex == "paribu":
        if base in {"TL", "TRY"} and quote not in {"TL", "TRY"}:
            base, quote = quote, "TL"
        if quote == "TRY":
            quote = "TL"
        return f"{base.lower()}_{quote.lower()}"
    if ex == "kucoin":
        return f"{base}-{quote}"
    return f"{base}{quote}"


def quote_currency_for_symbol(symbol: str, exchange: str = "") -> str:
    ex = (exchange or "").strip().lower()
    _, quote = split_symbol_parts(symbol)
    q = str(quote or "").upper()
    if ex == "paribu" and q == "TRY":
        q = "TL"
    if not q:
        if ex == "paribu":
            return "TL"
        if ex == "btcturk":
            return "TRY"
        return "USDT"
    return q


def format_latency_text(ms_value):
    try:
        ms = int(ms_value)
    except Exception:
        return "-"
    if ms <= 0:
        return "-"
    sec = ms / 1000.0
    return f"{ms} ms ({sec:.3f} sec)"


def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def fmt_num(value, digits: int = 8):
    val = to_float(value)
    if val is None:
        return "-"
    if abs(val) >= 1:
        s = f"{val:,.{min(max(digits, 2), 6)}f}"
    else:
        s = f"{val:.8f}"
    s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def fmt_volume(value):
    val = to_float(value)
    if val is None:
        return "-"
    if abs(val) >= 1_000_000_000:
        return f"{val/1_000_000_000:.2f}B"
    if abs(val) >= 1_000_000:
        return f"{val/1_000_000:.2f}M"
    if abs(val) >= 1_000:
        return f"{val/1_000:.2f}K"
    return fmt_num(val, digits=2)


def fmt_spread(value):
    val = to_float(value)
    if val is None:
        return "-"
    return f"{val:.3f}%"


def fmt_price(value):
    val = to_float(value)
    if val is None:
        return "-"
    if abs(val) >= 1:
        return f"{val:,.6f}".rstrip("0").rstrip(".")
    return f"{val:.10f}".rstrip("0").rstrip(".")


def fmt_qty(value):
    val = to_float(value)
    if val is None:
        return "-"
    if abs(val) >= 1:
        return f"{val:,.6f}".rstrip("0").rstrip(".")
    return f"{val:.10f}".rstrip("0").rstrip(".")


def fmt_pnl(value):
    val = to_float(value)
    if val is None:
        return "-"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:,.6f} USDT".rstrip("0").rstrip(".")


def fmt_pct(value):
    val = to_float(value)
    if val is None:
        return "-"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"


def extract_result_error_text(result: dict):
    body = (result or {}).get("body")
    if isinstance(body, dict):
        for key in ("message", "msg", "detail", "error", "label", "code", "text"):
            val = body.get(key)
            if val not in (None, ""):
                return str(val)[:240]
        has_non_empty = any(v not in (None, "", [], {}) for v in body.values())
        if not has_non_empty:
            return ""
        raw = str(body)
        return raw[:240] if raw else ""
    if body not in (None, ""):
        return str(body)[:240]
    return ""


def find_open_position(open_positions, exchange: str, symbol: str):
    ex = (exchange or "").strip().lower()
    sym = (symbol or "").strip().upper()
    if not ex or not sym:
        return None
    for row in (open_positions or []):
        r_ex = str((row or {}).get("exchange", "")).strip().lower()
        r_sym = str((row or {}).get("symbol", "")).strip().upper()
        if r_ex == ex and r_sym == sym:
            return dict(row)
    return None


def parse_iso_time(value: str):
    if not value or value == "UNKNOWN":
        return None
    try:
        dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return None
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def format_clock(value: str):
    dt = parse_iso_time(value)
    if dt is None:
        return value or "no data yet"
    dt_tr = dt.astimezone(TR_TZ) if TR_TZ else dt
    return dt_tr.strftime("%d.%m.%Y %H:%M:%S (TRT)")


def format_clock_ms(value: str):
    dt = parse_iso_time(value)
    if dt is None:
        return value or "no data yet"
    dt_tr = dt.astimezone(TR_TZ) if TR_TZ else dt
    return dt_tr.strftime("%d.%m.%Y %H:%M:%S.%f")[:-3] + " (TRT)"


def format_epoch_ms_clock(ms_value):
    try:
        ms = int(ms_value)
    except Exception:
        return "-"
    if ms <= 0:
        return "-"
    dt = datetime.datetime.fromtimestamp(ms / 1000.0, tz=datetime.timezone.utc)
    dt_tr = dt.astimezone(TR_TZ) if TR_TZ else dt
    return dt_tr.strftime("%d.%m.%Y %H:%M:%S.%f")[:-3] + " (TRT)"


def format_drift_ms(value):
    try:
        ms = int(value)
    except Exception:
        return "-"
    sign = "+" if ms > 0 else ""
    return f"{sign}{ms} ms"


def split_clock_parts(value: str):
    dt = parse_iso_time(value)
    if dt is None:
        return None
    dt_tr = dt.astimezone(TR_TZ) if TR_TZ else dt
    return {
        "date": dt_tr.strftime("%d.%m.%Y"),
        "time": dt_tr.strftime("%H:%M:%S"),
    }


def parse_raw_time(value: str):
    if not value or value == "UNKNOWN":
        return None
    sample = value[:200]
    try:
        dt = dt_parser.parse(sample, fuzzy=True)
        if dt.tzinfo is None:
            if re.search(r"\bUTC\b", sample, flags=re.IGNORECASE):
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            else:
                return None
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def parse_item_time(item):
    dt = parse_iso_time(item.get("normalized_tr_time", "UNKNOWN"))
    if dt is not None:
        return dt
    dt = parse_raw_time(item.get("raw_time_text", "UNKNOWN"))
    if dt is not None:
        return dt
    # Last real-data fallback: announcement publish time.
    return parse_iso_time(str(item.get("published_at") or ""))


def guess_pair(item):
    url = str(item.get("url", "")).upper()
    title = str(item.get("title", "")).upper()
    raw = str(item.get("raw_time_text", "")).upper()
    summary = str(item.get("summary_text", "")).upper()
    lower_url = str(item.get("url", "")).lower()
    published = str(item.get("published_at", "")).upper()
    quote_group = "|".join(QUOTE_SUFFIXES)

    # URL tabanli en guvenilir yakalama
    m = re.search(r"/TRADE/([A-Z0-9]+_[A-Z0-9]+)", url)
    if m:
        pair = m.group(1)
        parts = pair.split("_", 1)
        if len(parts) == 2 and parts[1] in QUOTE_SUFFIXES and parts[0] not in PAIR_STOPWORDS:
            return pair

    # Common exchange URL patterns
    m = re.search(r"/exchange/([A-Z0-9]{2,20})[_-]([A-Z0-9]{2,10})", lower_url)
    if m:
        return f"{m.group(1).upper()}_{m.group(2).upper()}"
    m = re.search(r"/futures/([A-Z0-9]{2,20})[_-]([A-Z0-9]{2,10})", lower_url)
    if m:
        return f"{m.group(1).upper()}_{m.group(2).upper()}"

    blobs = [title, raw, summary, url, published]
    for txt in blobs:
        # ABC/USDT
        m = re.search(rf"\b([A-Z0-9]{{2,16}})\s*/\s*({quote_group})\b", txt)
        if m and m.group(1) not in PAIR_STOPWORDS:
            return f"{m.group(1)}_{m.group(2)}"
        # ABC_USDT veya ABC-USDT
        m = re.search(rf"\b([A-Z0-9]{{2,16}})[_-]({quote_group})\b", txt)
        if m and m.group(1) not in PAIR_STOPWORDS:
            return f"{m.group(1)}_{m.group(2)}"
        # ABCUSDT
        m = re.search(rf"\b([A-Z0-9]{{2,16}})({quote_group})\b", txt)
        if m and m.group(1) not in PAIR_STOPWORDS:
            return f"{m.group(1)}_{m.group(2)}"

    # Ticker in parentheses, e.g. Espresso (ESP), BankrCoin (BNKR)
    for m in re.finditer(r"\(([A-Z0-9]{2,16})\)", f"{title} {summary}"):
        tok = m.group(1)
        if tok in PAIR_STOPWORDS or tok in QUOTE_SUFFIXES:
            continue
        return f"{tok}_USDT"

    # "List XYZ" gibi basliklar icin son fallback.
    m = re.search(r"\b(?:LIST|LISTING|LISTINGS|WILL LIST|WILL LISTE?D?)\s+([A-Z0-9]{2,16})\b", title)
    if m:
        tok = m.group(1)
        if tok not in PAIR_STOPWORDS and tok not in QUOTE_SUFFIXES:
            return f"{tok}_USDT"

    # MEXC-like titles: "First in Market: PUNCH Now Live on ..."
    m = re.search(r"\bFIRST\s+IN\s+MARKET\s*:\s*([A-Z0-9]{2,16})\b", f"{title} {summary}")
    if m:
        tok = m.group(1)
        if tok not in PAIR_STOPWORDS and tok not in QUOTE_SUFFIXES:
            return f"{tok}_USDT"

    # As a final fallback, detect 3-10 letter ticker after coin name patterns.
    m = re.search(r"\b(?:TOKEN|COIN)\s*[:\-]?\s*([A-Z0-9]{2,16})\b", f"{title} {summary}")
    if m:
        tok = m.group(1)
        if tok not in PAIR_STOPWORDS and tok not in QUOTE_SUFFIXES:
            return f"{tok}_USDT"
    return ""


def build_pair_options_map(listings: dict, exec_state: dict, last_order: dict = None):
    pair_map = {ex: [] for ex in REAL_TEST_EXCHANGES}

    def add_pair(exchange: str, raw_symbol: str):
        ex = str(exchange or "").strip().lower()
        if ex not in pair_map:
            return
        norm = normalize_symbol_for_exchange(ex, str(raw_symbol or "").strip())
        if not norm:
            return
        rows = pair_map[ex]
        if norm not in rows:
            rows.append(norm)

    for ex, defaults in PAIR_DEFAULTS.items():
        for sym in defaults:
            add_pair(ex, sym)

    for ex, rows in (listings or {}).items():
        ex_norm = str(ex or "").strip().lower()
        if ex_norm not in pair_map:
            continue
        for item in list(rows or [])[:240]:
            pair = str((item or {}).get("pair_guess") or "").strip()
            if not pair:
                pair = guess_pair(item or {})
            if pair:
                add_pair(ex_norm, pair)

    order_rows = list((exec_state or {}).get("order_history") or [])
    for row in order_rows[-300:]:
        add_pair((row or {}).get("exchange"), (row or {}).get("symbol"))

    cycle_rows = list((exec_state or {}).get("trade_cycles") or [])
    for row in cycle_rows[-240:]:
        add_pair((row or {}).get("exchange"), (row or {}).get("symbol"))

    pos_rows = list((exec_state or {}).get("open_positions") or [])
    for row in pos_rows[-120:]:
        add_pair((row or {}).get("exchange"), (row or {}).get("symbol"))

    last_exec = dict((exec_state or {}).get("last_execution") or {})
    add_pair(last_exec.get("exchange"), last_exec.get("symbol_sent") or last_exec.get("symbol"))

    lo = dict(last_order or {})
    add_pair(lo.get("exchange"), lo.get("symbol_normalized") or lo.get("symbol"))

    for ex in list(pair_map.keys()):
        pair_map[ex] = pair_map[ex][:80]
    return pair_map


def format_trade_start(item):
    dt = parse_item_time(item)
    if dt is None:
        return "UNKNOWN"
    dt_tr = dt.astimezone(TR_TZ) if TR_TZ else dt
    return dt_tr.strftime("%Y-%m-%d %H:%M:%S (TRT)")


def humanize_error(err: str):
    if not err:
        return ""
    m = re.search(r"HTTP\s+(\d{3})", err)
    if m:
        code = m.group(1)
        if code == "403":
            return "Access blocked (HTTP 403)."
        if code == "404":
            return "Resource not found (HTTP 404)."
        if code == "429":
            return "Rate limit exceeded (HTTP 429)."
        if code.startswith("5"):
            return f"Exchange server error (HTTP {code})."
        return f"HTTP error ({code})."
    if "Timeout" in err or "ReadTimeout" in err or "ConnectTimeout" in err:
        return "Timeout: exchange did not respond."
    if "ConnectionError" in err:
        return "Connection error: exchange unreachable."
    return "An error occurred while fetching data."


def filter_out_expired(listings):
    now = datetime.datetime.now(datetime.timezone.utc)
    max_age = datetime.timedelta(days=LISTING_MAX_AGE_DAYS)
    filtered = {}
    for ex, items in listings.items():
        kept = []
        for item in items:
            dt = parse_item_time(item)
            if dt is not None and dt < now:
                continue
            if dt is None:
                pub_dt = parse_iso_time(str(item.get("published_at") or ""))
                if pub_dt is not None and (now - pub_dt) > max_age:
                    continue
            kept.append(item)
        filtered[ex] = kept
    return filtered


def build_symbol_tokens(symbol: str):
    s = (symbol or "").upper().strip()
    tokens = {p for p in re.split(r"[^A-Z0-9]+", s) if len(p) >= 3}
    compact = re.sub(r"[^A-Z0-9]", "", s)
    if compact:
        tokens.add(compact)
        for q in QUOTE_SUFFIXES:
            if compact.endswith(q) and len(compact) > len(q):
                tokens.add(compact[:-len(q)])
                break
    return tokens


def find_countdown_target(exchange: str, symbol: str, listings):
    tokens = build_symbol_tokens(symbol)
    if not tokens:
        return None
    now = datetime.datetime.now(datetime.timezone.utc)
    best_item = None
    best_dt = None
    for item in listings.get(exchange, []):
        dt = parse_item_time(item)
        if dt is None or dt <= now:
            continue
        hay = f"{item.get('title', '').upper()} {item.get('url', '').upper()}"
        if not any(t in hay for t in tokens):
            continue
        if best_dt is None or dt < best_dt:
            best_dt = dt
            best_item = item
    if best_item is None:
        return None
    target_iso = best_item.get("normalized_tr_time", "UNKNOWN")
    if not target_iso or target_iso == "UNKNOWN":
        target_iso = best_dt.isoformat()
    return {
        "exchange": exchange,
        "symbol": symbol.upper(),
        "target_iso": target_iso,
        "listing_title": best_item.get("title", ""),
        "listing_url": best_item.get("url", ""),
        "contract_hint": best_item.get("contract_hint", ""),
    }


def refresh_scanner_state_cache():
    status = SCANNER_SERVICE.status()
    STATE["scanner_status"] = status
    STATE["scan_stats"] = dict(status.get("stats") or {})

    listings = dict(getattr(agg, "items", {}) or {ex: [] for ex in agg.exchanges})
    app.config["LISTINGS"] = listings

    errors = {}
    checks = {}
    for ex in agg.exchanges:
        row = dict((status.get("stats") or {}).get(ex) or {})
        err = str(row.get("lastErrorMessage") or "").strip()
        if err:
            errors[ex] = err
            checks[ex] = False
        else:
            checks[ex] = True

    STATE["errors"] = errors
    STATE["checks"] = checks
    STATE["exchange_modes"] = dict(getattr(agg, "exchange_state", {}) or {})
    STATE["last_poll"] = iso_utc() if status.get("lastRunEndedMs") else STATE.get("last_poll")

    event_map = SCANNER_SERVICE.events_by_exchange(limit=140)
    STATE["listing_events_by_exchange"] = event_map


def ensure_scanner_started():
    status = SCANNER_SERVICE.status()
    if not status.get("running"):
        SCANNER_SERVICE.start()
    refresh_scanner_state_cache()


def bg_loop():
    # Legacy bootstrap thread kept for backward compatibility.
    # Scanner loop now runs inside ScannerService worker.
    ensure_scanner_started()
    while True:
        try:
            refresh_scanner_state_cache()
            _notify_new_listings(app.config.get("LISTINGS") or {})
        except Exception as e:
            STATE["last_action"] = {"ok": False, "text": f"Scanner cache refresh error: {e}", "at": iso_utc()}
        time.sleep(2.0)


@app.route("/", methods=["GET"])
def index():
    ensure_scanner_started()
    req_selected_exchange = str(request.args.get("exchange") or "").strip().lower()
    if req_selected_exchange in LISTING_EXCHANGE_SET:
        STATE["selected_exchange_id"] = req_selected_exchange
    selected_exchange_id = str(STATE.get("selected_exchange_id") or "gate").strip().lower()
    if selected_exchange_id not in LISTING_EXCHANGE_SET:
        selected_exchange_id = "gate"
        STATE["selected_exchange_id"] = selected_exchange_id

    source = app.config.get("LISTINGS") or {ex: [] for ex in agg.exchanges}
    listings = filter_out_expired(source)
    friendly_errors = {ex: humanize_error(msg) for ex, msg in STATE["errors"].items()}
    checks = STATE.get("checks") or {ex: None for ex in agg.exchanges}
    exchange_modes = STATE.get("exchange_modes") or {ex: {} for ex in agg.exchanges}
    source_labels = {
        "api_announcement": "API announcements",
        "symbol_diff": "Symbol diff feed",
        "web_fallback": "Web fallback",
    }
    for ex in agg.exchanges:
        mode = dict(exchange_modes.get(ex) or {})
        primary_key = str(mode.get("primary_source") or "web_fallback")
        active_key = str(mode.get("active_source") or primary_key)
        mode["primary_source"] = primary_key
        mode["active_source"] = active_key
        mode["primary_label"] = source_labels.get(primary_key, primary_key)
        mode["active_label"] = source_labels.get(active_key, active_key)
        exchange_modes[ex] = mode
    exec_state = executor_get_status()
    last_exec = dict(exec_state.get("last_execution") or {})
    if last_exec:
        last_exec["at_parts"] = split_clock_parts(last_exec.get("at"))
        last_exec["is_real"] = not bool(last_exec.get("dry_run"))
        last_exec["engine_latency_text"] = format_latency_text(last_exec.get("engine_latency_ms")) if last_exec["is_real"] else "-"
        last_exec["trigger_latency_text"] = format_latency_text(last_exec.get("trigger_to_result_ms"))
    order_latency = {}
    raw_latency = dict(exec_state.get("order_latency") or {})
    for ex in agg.exchanges:
        row = dict(raw_latency.get(ex) or {})
        row["at_parts"] = split_clock_parts(row.get("at"))
        row["engine_latency_text"] = format_latency_text(row.get("engine_latency_ms"))
        order_latency[ex] = row
    order_history = list(exec_state.get("order_history") or [])
    trade_cycles_raw = list(exec_state.get("trade_cycles") or [])
    open_positions = list(exec_state.get("open_positions") or [])
    last_signal = dict(exec_state.get("last_order_signal") or {})
    buy_check = bool(last_signal.get("success") and str(last_signal.get("side", "")).lower() == "buy")
    sell_check = bool(last_signal.get("success") and str(last_signal.get("side", "")).lower() == "sell")
    trade_cycles = []
    for row in reversed(trade_cycles_raw[-30:]):
        r = dict(row or {})
        r["buy_time_display"] = format_clock(str(r.get("buy_time") or ""))
        r["sell_time_display"] = format_clock(str(r.get("sell_time") or ""))
        r["buy_price_display"] = fmt_price(r.get("buy_price"))
        r["buy_qty_display"] = fmt_qty(r.get("buy_qty"))
        r["sell_price_display"] = fmt_price(r.get("sell_price"))
        r["sell_qty_display"] = fmt_qty(r.get("sell_qty"))
        r["pnl_display"] = fmt_pnl(r.get("pnl_quote"))
        r["pnl_pct_display"] = fmt_pct(r.get("pnl_pct"))
        pnl_val = to_float(r.get("pnl_quote"))
        r["pnl_positive"] = pnl_val is not None and pnl_val >= 0
        trade_cycles.append(r)
    for ex in listings:
        for item in listings[ex]:
            disp = format_trade_start(item)
            item["trade_start_display"] = disp
            item["trade_start_ok"] = disp != "UNKNOWN"
            src = str(item.get("source_type") or "web_fallback")
            item["source_type"] = src
            item["source_label"] = "API" if src == "api_announcement" else ("DIFF" if src == "symbol_diff" else "WEB")
            raw_pair = guess_pair(item)
            item["pair_guess"] = normalize_symbol_for_exchange(ex, raw_pair) if raw_pair else ""
            item["detected_display"] = format_clock(item.get("detected_at", ""))
            item["detected_parts"] = split_clock_parts(item.get("detected_at", ""))

    trade_exchange_q = str(request.args.get("trade_exchange") or "").strip().lower()
    trade_canonical_q = str(request.args.get("trade_canonical") or "").strip()
    trade_symbol_q = str(request.args.get("trade_symbol") or "").strip()
    focus_nonce_q = str(request.args.get("focus_nonce") or "").strip()
    if trade_exchange_q in TRADE_EXCHANGE_SET and (trade_canonical_q or trade_symbol_q):
        order_symbol = trade_symbol_q
        if not order_symbol and trade_canonical_q:
            order_symbol = format_order_symbol(trade_exchange_q, trade_canonical_q)
        if order_symbol:
            STATE["trade_draft"] = {
                "exchange": trade_exchange_q,
                "canonical": to_canonical_symbol(trade_canonical_q or order_symbol, trade_exchange_q),
                "order_symbol": order_symbol,
            }
            STATE["focus_request_id"] = focus_nonce_q or f"focus-{int(time.time() * 1000)}"

    listing_events_map = dict(STATE.get("listing_events_by_exchange") or {})
    listing_events_view = {}
    for ex in agg.exchanges:
        rows = list(listing_events_map.get(ex) or [])
        view_rows = []
        for row in rows[:24]:
            ev = dict(row or {})
            canonical = str(ev.get("canonicalSymbol") or "")
            ex_symbol = str(ev.get("orderSymbol") or "")
            if canonical and not ex_symbol:
                ex_symbol = format_order_symbol(ex, canonical)
            trade_url = str(ev.get("tradeUrl") or "")
            if canonical and not trade_url:
                trade_url = build_trade_url(ex, canonical)
            focus_nonce = f"f{int(time.time()*1000)}{len(view_rows)}"
            internal_link = (
                f"/?exchange={ex}"
                f"&trade_exchange={ex}"
                f"&trade_canonical={canonical}"
                f"&trade_symbol={ex_symbol}"
                f"&focus_nonce={focus_nonce}"
            )
            view_rows.append(
                {
                    "exchange": ex,
                    "canonical_symbol": canonical,
                    "order_symbol": ex_symbol,
                    "detected_at": ev.get("detectedAt") or "",
                    "detected_display": format_clock(str(ev.get("detectedAt") or "")),
                    "status": str(ev.get("status") or "candidate"),
                    "title": str(ev.get("title") or ""),
                    "url": str(ev.get("url") or ""),
                    "source_type": str(ev.get("sourceType") or ""),
                    "trade_url": trade_url,
                    "internal_link": internal_link,
                }
            )
        listing_events_view[ex] = view_rows
    countdown = STATE.get("countdown")
    if countdown:
        cdt = parse_iso_time(countdown.get("target_iso", ""))
        if cdt is None or cdt <= datetime.datetime.now(datetime.timezone.utc):
            countdown = None
            STATE["countdown"] = None
    last_order = dict(STATE.get("last_order") or {})
    if last_order and not last_order.get("symbol_normalized"):
        last_order["symbol_normalized"] = normalize_symbol_for_exchange(
            last_order.get("exchange", ""),
            last_order.get("symbol", ""),
        )
    pair_options_map = build_pair_options_map(listings, exec_state, last_order)
    current_open_position = find_open_position(
        open_positions,
        last_order.get("exchange", ""),
        last_order.get("symbol_normalized", ""),
    )
    if current_open_position:
        current_open_position["base_qty_display"] = fmt_qty(current_open_position.get("base_qty"))
        current_open_position["buy_price_display"] = fmt_price(current_open_position.get("buy_price"))
        current_open_position["buy_time_display"] = format_clock(str(current_open_position.get("buy_time") or ""))
    pending_info = None
    gate_state = dict(exec_state.get("gate") or {})
    if gate_state.get("armed"):
        p_exchange = str(gate_state.get("exchange") or last_order.get("exchange") or "gate").strip().lower() or "gate"
        p_symbol = (
            str(gate_state.get("symbol") or "").strip()
            or str(last_order.get("symbol_normalized") or "").strip()
            or str(last_order.get("symbol") or "").strip()
            or "-"
        )
        p_spend = (
            str(gate_state.get("spend_usdt") or "").strip()
            or str(last_order.get("spend_usdt") or "").strip()
            or "?"
        )
        p_phase = str(gate_state.get("phase") or "armed").strip() or "armed"
        pending_info = {
            "exchange": p_exchange,
            "symbol": p_symbol,
            "spend_usdt": p_spend,
            "spend_currency": quote_currency_for_symbol(p_symbol, p_exchange),
            "phase": p_phase,
            "target_display": "",
        }
        if countdown and countdown.get("exchange") == p_exchange:
            pending_info["target_display"] = format_clock(countdown.get("target_iso", ""))

    presence_raw = dict(exec_state.get("market_presence") or {})
    presence_view = {
        "checked": bool(presence_raw.get("checked")),
        "error": str(presence_raw.get("error") or "").strip(),
        "found": bool(presence_raw.get("found_on_other_exchanges")),
        "method": str(presence_raw.get("method") or "").strip(),
        "explain": str(presence_raw.get("explain") or "").strip(),
        "rows": [],
        "candidates": list(presence_raw.get("candidates") or [])[:3],
        "searched_display": "",
        "searched_global": [],
        "searched_turkey": [],
        "scan_ms": presence_raw.get("scan_ms"),
        "at_display": format_clock(str(presence_raw.get("at") or "")),
    }
    method_map = {
        "contract": "Contract match",
        "coin_id": "Coin ID match",
        "symbol_only": "Symbol-only match",
        "error": "Scan error",
    }
    presence_view["method_label"] = method_map.get(presence_view["method"], presence_view["method"] or "-")
    presence_view["warning_symbol_only"] = bool(
        presence_raw.get("warning_symbol_only") or presence_view["method"] == "symbol_only"
    )
    presence_view["warning_ambiguous"] = bool(
        presence_raw.get("warning_ambiguous") or presence_raw.get("ambiguous")
    )
    summary = dict(presence_raw.get("summary") or {})
    top_exchanges = [str(x) for x in (summary.get("top_exchanges") or []) if x]
    presence_view["top_exchanges"] = top_exchanges[:3]
    presence_view["reference_price"] = fmt_num(summary.get("reference_price"), digits=8)
    price_range = dict(summary.get("price_range") or {})
    presence_view["price_min"] = fmt_num(price_range.get("min"), digits=8)
    presence_view["price_max"] = fmt_num(price_range.get("max"), digits=8)
    coin_id = str(presence_raw.get("coin_id") or "").strip()
    coin_name = str(presence_raw.get("coin_name") or "").strip()
    if coin_name and coin_id:
        presence_view["coin_label"] = f"{coin_name} ({coin_id})"
    elif coin_name:
        presence_view["coin_label"] = coin_name
    else:
        presence_view["coin_label"] = coin_id

    searched_raw = [str(x).strip() for x in (presence_raw.get("searched_exchanges") or []) if str(x).strip()]
    tr_set = {"btcturk", "paribu"}
    label_map = {
        "okex": "OKX",
        "btcturk": "BtcTurk",
        "paribu": "Paribu",
        "binance": "Binance",
        "bybit": "Bybit",
        "kucoin": "KuCoin",
        "mexc": "MEXC",
        "bitget": "Bitget",
        "gate": "Gate",
    }
    global_list = []
    turkey_list = []
    for ex in searched_raw:
        disp = label_map.get(ex, ex)
        if ex in tr_set:
            turkey_list.append(disp)
        else:
            global_list.append(disp)
    presence_view["searched_global"] = global_list
    presence_view["searched_turkey"] = turkey_list
    presence_view["searched_display"] = ", ".join(global_list + turkey_list)

    for row in list(presence_raw.get("rows") or [])[:24]:
        r = dict(row or {})
        r["exchange"] = str(r.get("exchange") or "-")
        r["pair"] = str(r.get("pair") or "-")
        r["market_type"] = str(r.get("market_type") or "-")
        r["last_price_display"] = fmt_num(r.get("last_price"), digits=8)
        r["volume_24h_display"] = fmt_volume(r.get("volume_24h"))
        r["bid_display"] = fmt_num(r.get("bid"), digits=8)
        r["ask_display"] = fmt_num(r.get("ask"), digits=8)
        r["spread_display"] = fmt_spread(r.get("spread_pct"))
        r["source"] = str(r.get("source") or "-")
        presence_view["rows"].append(r)

    return render_template_string(
        HTML,
        listing_exchanges=agg.exchanges,
        all_exchanges=agg.exchanges,
        trade_exchanges=TRADE_EXCHANGES,
        listings=listings,
        errors=STATE["errors"],
        checks=checks,
        exchange_modes=exchange_modes,
        friendly_errors=friendly_errors,
        last_poll=STATE["last_poll"],
        last_poll_display=format_clock(STATE["last_poll"]),
        last_poll_parts=split_clock_parts(STATE["last_poll"]),
        executor_url=EXECUTOR_URL,
        exec_state=exec_state,
        last_exec=last_exec,
        order_latency=order_latency,
        order_history=order_history,
        trade_cycles=trade_cycles,
        open_positions=open_positions,
        current_open_position=current_open_position,
        last_order_signal=last_signal,
        buy_check=buy_check,
        sell_check=sell_check,
        last_action=STATE.get("last_action"),
        countdown=countdown,
        last_order=last_order,
        pair_options_map=pair_options_map,
        pending_info=pending_info,
        market_presence=presence_view,
        selected_exchange_id=selected_exchange_id,
        listing_events=listing_events_view,
        trade_draft=STATE.get("trade_draft") or {},
        focus_request_id=STATE.get("focus_request_id") or "",
    )


@app.route("/arm", methods=["POST"])
def arm():
    ex = request.form.get("exchange", "").strip().lower()
    symbol = request.form.get("symbol", "").strip()
    spend = request.form.get("spend_usdt", "5").strip()
    listing_title = request.form.get("listing_title", "").strip()
    listing_url = request.form.get("listing_url", "").strip()
    contract_hint = request.form.get("contract_hint", "").strip()
    norm_symbol = normalize_symbol_for_exchange(ex, symbol)
    STATE["last_order"] = {
        "exchange": ex,
        "symbol": symbol,
        "symbol_normalized": norm_symbol,
        "spend_usdt": spend,
        "listing_title": listing_title,
        "listing_url": listing_url,
        "contract_hint": contract_hint,
    }

    if ex not in TRADE_EXCHANGE_SET:
        set_action(False, "Invalid exchange selection.")
        return redirect(url_for("index"))
    if not symbol:
        set_action(False, "Symbol/pair cannot be empty.")
        return redirect(url_for("index"))
    if not norm_symbol:
        set_action(False, "Symbol/pair format could not be parsed.")
        return redirect(url_for("index"))

    countdown = find_countdown_target(ex, norm_symbol, app.config.get("LISTINGS") or {})
    payload = {"exchange": ex, "symbol": norm_symbol, "spend_usdt": spend}
    if countdown and countdown.get("target_iso"):
        payload["target_iso"] = countdown.get("target_iso")
    if not listing_title and countdown and countdown.get("listing_title"):
        listing_title = str(countdown.get("listing_title") or "").strip()
    if not listing_url and countdown and countdown.get("listing_url"):
        listing_url = str(countdown.get("listing_url") or "").strip()
    if not contract_hint and countdown and countdown.get("contract_hint"):
        contract_hint = str(countdown.get("contract_hint") or "").strip()
    if listing_title:
        payload["listing_title"] = listing_title
    if listing_url:
        payload["listing_url"] = listing_url
    if contract_hint:
        payload["contract_hint"] = contract_hint

    try:
        out = executor_post("/arm", payload)
        mode = out.get("mode")
        STATE["countdown"] = countdown
        presence = out.get("market_presence") if isinstance(out, dict) else {}
        presence_note = ""
        if isinstance(presence, dict) and presence.get("checked"):
            if presence.get("found_on_other_exchanges"):
                rows_count = len(presence.get("rows") or [])
                presence_note = f" Found on other exchanges ({rows_count} markets)."
            else:
                presence_note = " Not found on other major exchanges."
        if ex == "gate" or mode == "ws_trigger":
            base_msg = f"Order prepared for {ex}: {norm_symbol}."
        else:
            base_msg = f"Buy request sent to {ex} ({norm_symbol})."
        if countdown:
            set_action(True, base_msg + " Countdown started." + presence_note)
        else:
            set_action(True, base_msg + " No future listing time found for this pair." + presence_note)
    except Exception as e:
        STATE["countdown"] = None
        set_action(False, f"Start action failed: {e}")
    return redirect(url_for("index"))


@app.route("/stop-buy", methods=["POST"])
@app.route("/disarm", methods=["POST"])
def stop_buy():
    try:
        out = executor_post("/kill", {})
        STATE["countdown"] = None
        forced = out.get("forced_buy") if isinstance(out, dict) else None
        if isinstance(forced, dict) and forced.get("error"):
            set_action(False, f"Buy stopped, forced buy failed: {forced['error']}")
        elif forced is not None:
            set_action(True, "Buy stopped. Forced market buy sent for active process.")
        else:
            set_action(True, "Buy stopped.")
    except Exception as e:
        set_action(False, f"Stop-buy action failed: {e}")
    return redirect(url_for("index"))


@app.route("/sell-market", methods=["POST"])
def sell_market():
    ex = request.form.get("exchange", "").strip().lower() or str((STATE.get("last_order") or {}).get("exchange") or "").strip().lower()
    symbol_raw = request.form.get("symbol", "").strip() or str((STATE.get("last_order") or {}).get("symbol_normalized") or "").strip()
    norm_symbol = normalize_symbol_for_exchange(ex, symbol_raw)
    if ex and norm_symbol:
        STATE["last_order"] = dict(STATE.get("last_order") or {})
        STATE["last_order"]["exchange"] = ex
        STATE["last_order"]["symbol"] = symbol_raw
        STATE["last_order"]["symbol_normalized"] = norm_symbol

    if ex not in TRADE_EXCHANGE_SET:
        set_action(False, "Invalid exchange for sell.")
        return redirect(url_for("index"))
    if not norm_symbol:
        set_action(False, "Pair is required for sell.")
        return redirect(url_for("index"))

    try:
        exec_state = executor_get_status()
        pos = find_open_position(exec_state.get("open_positions") or [], ex, norm_symbol)
        qty = to_float((pos or {}).get("base_qty"))
        if qty is None or qty <= 0:
            set_action(False, f"No open position found for {ex} {norm_symbol}.")
            return redirect(url_for("index"))

        qty_text = f"{qty:.10f}".rstrip("0").rstrip(".")
        if not qty_text:
            set_action(False, f"Sell quantity is invalid for {ex} {norm_symbol}.")
            return redirect(url_for("index"))

        executor_post("/sell", {"exchange": ex, "symbol": norm_symbol, "qty": qty_text})
        set_action(True, f"Sell sent: {ex} {norm_symbol}, qty {qty_text}.")
    except Exception as e:
        set_action(False, f"Sell action failed: {e}")
    return redirect(url_for("index"))


@app.route("/exchange-test", methods=["POST"])
def exchange_test():
    target = request.form.get("exchange_test_target", "").strip().lower()
    payload = {"exchange": target} if target in LISTING_EXCHANGE_SET else {}
    try:
        out = executor_post("/exchange-test", payload)
        STATE["exchange_test"] = out if isinstance(out, dict) else None
        ok_count = 0
        total = 0
        for ex in LISTING_EXCHANGES:
            row = (STATE["exchange_test"] or {}).get("results", {}).get(ex) if isinstance(STATE["exchange_test"], dict) else None
            if not row:
                continue
            total += 1
            if row.get("network_ok") and (row.get("auth_ok") in (True, None)):
                ok_count += 1
        set_action(True, f"Exchange connectivity test completed: {ok_count}/{total} successful.")
    except Exception as e:
        set_action(False, f"Exchange connectivity test failed: {e}")
    return redirect(url_for("index"))


@app.route("/probe-latency", methods=["POST"])
def probe_latency():
    wants_json = (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or "application/json" in (request.headers.get("Accept") or "")
    )
    ex = request.form.get("probe_exchange", "").strip().lower()
    symbol = request.form.get("probe_symbol", "").strip()
    spend = request.form.get("probe_spend_usdt", "5").strip()
    if ex not in LISTING_EXCHANGE_SET:
        msg = "Invalid exchange selection."
        set_action(False, msg)
        if wants_json:
            return jsonify({"ok": False, "error": msg}), 400
        return redirect(url_for("index"))
    norm_symbol = normalize_symbol_for_exchange(ex, symbol) if symbol else ""
    if symbol and not norm_symbol:
        msg = "Pair format could not be parsed."
        set_action(False, msg)
        if wants_json:
            return jsonify({"ok": False, "error": msg}), 400
        return redirect(url_for("index"))
    try:
        out = executor_post("/order-latency", {"exchange": ex, "symbol": norm_symbol, "spend_usdt": spend})
        probe = out.get("probe") if isinstance(out, dict) else {}
        ms = probe.get("engine_latency_ms") if isinstance(probe, dict) else None
        txt = format_latency_text(ms)
        sent_symbol = probe.get("symbol_sent") if isinstance(probe, dict) else "-"
        auto_symbol = bool(probe.get("auto_symbol")) if isinstance(probe, dict) else False
        symbol_note = f"{sent_symbol} (auto)" if auto_symbol else sent_symbol
        if txt != "-":
            set_action(True, f"{ex} order-route measurement: {txt} ({symbol_note}).")
        else:
            set_action(False, f"{ex} order-route measurement returned no verified exchange RTT ({symbol_note}).")
        if wants_json:
            return jsonify(
                {
                    "ok": True,
                    "exchange": ex,
                    "latency_text": txt,
                    "symbol_note": symbol_note,
                }
            )
    except Exception as e:
        msg = f"Order-route measurement failed: {e}"
        set_action(False, msg)
        if wants_json:
            return jsonify({"ok": False, "error": msg}), 500
    return redirect(url_for("index"))


@app.route("/select-exchange", methods=["POST"])
def select_exchange():
    ex = str(request.form.get("exchange") or "").strip().lower()
    if ex in LISTING_EXCHANGE_SET:
        STATE["selected_exchange_id"] = ex
        return jsonify({"ok": True, "exchange": ex})
    return jsonify({"ok": False, "error": "invalid_exchange"}), 400


@app.route("/set-dry-run", methods=["POST"])
def set_dry_run():
    raw = request.form.get("dry_run_mode", "1").strip()
    enabled = raw == "1"
    try:
        out = executor_post("/dry-run", {"enabled": enabled})
        if out.get("dry_run", enabled):
            set_action(True, "Test mode enabled. Real orders are not sent.")
        else:
            set_action(True, "Live mode enabled. Orders are sent to exchanges.")
    except Exception as e:
        set_action(False, f"Mode could not be updated: {e}")
    return redirect(url_for("index"))


def _store_test_result(action: str, ok: bool, data):
    tests = dict(STATE.get("tests") or {})
    tests[action] = {"ok": bool(ok), "at": iso_utc(), "data": data}
    STATE["tests"] = tests


def _run_diagnostic_action(action: str):
    if action == "feature_check":
        return executor_get("/diag/feature-check", use_token=True)
    if action == "ntp_check":
        return executor_get("/diag/ntp", use_token=True)
    if action in ("exchange_time_sync", "gate_time_sync"):
        return executor_post("/diag/time-sync", {"samples": 5, "warmup_sync_samples": 3, "timeout_sec": 1.2}, timeout_sec=30)
    if action == "warmup_all":
        return executor_post("/diag/warmup", {"timeout_sec": 1.2}, timeout_sec=20)
    if action == "arm_lock":
        return executor_get("/diag/arm-lock", use_token=True)
    if action == "telemetry":
        return executor_get("/diag/telemetry?limit=120", use_token=True)
    if action == "exchange_test":
        return executor_post("/exchange-test", {}, timeout_sec=20)
    if action == "aggregator_state":
        return {
            "ok": True,
            "exchange_modes": STATE.get("exchange_modes") or {},
            "errors": STATE.get("errors") or {},
            "checks": STATE.get("checks") or {},
            "at": iso_utc(),
        }
    if action == "market_presence":
        last_order = dict(STATE.get("last_order") or {})
        ex = str(last_order.get("exchange") or "gate").strip().lower() or "gate"
        symbol = str(last_order.get("symbol_normalized") or last_order.get("symbol") or "BTC_USDT").strip() or "BTC_USDT"
        payload = {
            "exchange": ex,
            "symbol": symbol,
            "listing_title": str(last_order.get("listing_title") or ""),
            "listing_url": str(last_order.get("listing_url") or ""),
            "contract_hint": str(last_order.get("contract_hint") or ""),
        }
        return executor_post("/diag/market-presence", payload, timeout_sec=30)
    raise ValueError(f"Unknown test action: {action}")


def _test_summary(test_id: str, ok: bool, data) -> str:
    if not isinstance(data, dict):
        return "Test completed. Technical details are shown below."

    try:
        if test_id == "feature_check":
            feat = data.get("features") or {}
            total = len(feat)
            enabled = sum(1 for v in feat.values() if bool(v))
            return f"Feature check completed: {enabled}/{total} features are enabled."

        if test_id == "ntp_check":
            ntp = data.get("ntp") or {}
            t_ok = bool((ntp.get("timedatectl_status") or {}).get("ok"))
            c_ok = bool((ntp.get("chronyc_tracking") or {}).get("ok"))
            if t_ok and c_ok:
                return "Server clock is synchronized: NTP and Chrony checks passed."
            return "Clock sync check is incomplete. Review technical output for details."

        if test_id in ("exchange_time_sync", "gate_time_sync"):
            results = data.get("results") if isinstance(data.get("results"), dict) else {}
            total = len(results)
            ok_count = 0
            best = None
            for ex, row in results.items():
                if not isinstance(row, dict) or not row.get("ok"):
                    continue
                ok_count += 1
                sync = row.get("sync") or {}
                rtt = sync.get("rtt_median_ms")
                if isinstance(rtt, (int, float)):
                    cand = (int(rtt), ex)
                    if best is None or cand[0] < best[0]:
                        best = cand
            if best is not None:
                return f"Exchange time sync completed: {ok_count}/{total} exchanges ok. Fastest median RTT: {best[1]} ({best[0]} ms)."
            return f"Exchange time sync completed: {ok_count}/{total} exchanges returned valid sync."

        if test_id == "warmup_all":
            results = data.get("results") or {}
            total = len(results)
            ok_count = sum(1 for _, r in results.items() if isinstance(r, dict) and bool(r.get("ok")))
            return f"Warmup completed: {ok_count}/{total} exchanges ready."

        if test_id == "arm_lock":
            exists = bool(data.get("exists"))
            match = bool(data.get("hash_match"))
            if exists and match:
                return "ARM lock exists and matches the configuration hash."
            if exists and not match:
                return "ARM lock exists but hash does not match current settings."
            return "ARM lock does not exist yet. Run ARM and check again."

        if test_id == "telemetry":
            cnt = data.get("count")
            if isinstance(cnt, int):
                return f"Telemetry is active: last {cnt} events listed."
            return "Telemetry check completed. Details are below."

        if test_id == "exchange_test":
            results = data.get("results") or {}
            total = 0
            ok_count = 0
            for _, r in results.items():
                if not isinstance(r, dict):
                    continue
                total += 1
                net_ok = bool(r.get("network_ok"))
                auth_ok = r.get("auth_ok") in (True, None)
                if net_ok and auth_ok:
                    ok_count += 1
            return f"Exchange access test completed: {ok_count}/{total} exchanges reachable."

        if test_id == "aggregator_state":
            modes = data.get("exchange_modes") or {}
            degraded = [k for k, v in modes.items() if isinstance(v, dict) and v.get("degraded")]
            if degraded:
                return f"Listing source check: {len(degraded)} exchange(s) in degraded mode ({', '.join(degraded)})."
            return "Listing source check: all exchanges in normal mode."

        if test_id == "market_presence":
            res = data.get("result") if isinstance(data.get("result"), dict) else data
            found = bool((res or {}).get("found_on_other_exchanges"))
            method = str((res or {}).get("method") or "-")
            rows = len((res or {}).get("rows") or [])
            if found:
                return f"Cross-exchange scan completed: {rows} market(s) found ({method})."
            return f"Cross-exchange scan completed: not found on other major exchanges ({method})."
    except Exception:
        pass

    return "Test completed. A plain-language summary is provided below."


def _feature_label(key: str) -> str:
    labels = {
        "gate_offset_sync": "clock alignment",
        "arm_lock_immutable": "ARM lock",
        "gate_prepared_exec": "fast order prep",
        "warmup_sessions": "connection warmup",
        "retry_window_jitter": "retry with jitter",
        "event_telemetry": "event telemetry",
        "target_iso_from_panel": "target time transfer",
        "arm_market_presence": "cross-exchange scan",
    }
    return labels.get(str(key or "").strip(), str(key or "").strip())


def _status_word(ok: bool) -> str:
    return "healthy" if ok else "needs attention"


def _human_test_details(test_id: str, ok: bool, data) -> list[str]:
    lines = []
    if not isinstance(data, dict):
        return ["A result was received. The system was able to run this test."]

    try:
        if test_id == "feature_check":
            feat = data.get("features") or {}
            enabled = [k for k, v in feat.items() if bool(v)]
            disabled = [k for k, v in feat.items() if not bool(v)]
            if enabled:
                shown = ", ".join(_feature_label(k) for k in enabled[:4])
                lines.append(f"Enabled key features: {shown}.")
            if disabled:
                shown = ", ".join(_feature_label(k) for k in disabled[:3])
                lines.append(f"Features to review: {shown}.")
            lines.append(f"Overall status: {_status_word(ok)}.")
            return lines

        if test_id == "ntp_check":
            ntp = data.get("ntp") or {}
            td_ok = bool((ntp.get("timedatectl_status") or {}).get("ok"))
            ch_ok = bool((ntp.get("chronyc_tracking") or {}).get("ok"))
            lines.append("Server automatic time sync was checked.")
            if td_ok and ch_ok:
                lines.append("Clock alignment looks good; drift risk is low.")
            else:
                lines.append("Clock alignment may be incomplete; this can affect order timing.")
            return lines

        if test_id in ("exchange_time_sync", "gate_time_sync"):
            results = data.get("results") if isinstance(data.get("results"), dict) else {}
            ok_rows = []
            fail_rows = []
            for ex, row in results.items():
                if not isinstance(row, dict):
                    continue
                if row.get("ok"):
                    sync = row.get("sync") or {}
                    off = sync.get("offset_ms")
                    rtt = sync.get("rtt_median_ms")
                    text = ex
                    if isinstance(off, (int, float)) and isinstance(rtt, (int, float)):
                        text = f"{ex}: offset {int(off)} ms, median RTT {int(rtt)} ms"
                    ok_rows.append(text)
                else:
                    fail_rows.append(f"{ex}: {row.get('error') or 'failed'}")
            if ok_rows:
                lines.append("Clock sync successful on: " + "; ".join(ok_rows) + ".")
            if fail_rows:
                lines.append("Need review: " + "; ".join(fail_rows) + ".")
            return lines or ["Time sync completed."]

        if test_id == "warmup_all":
            results = data.get("results") or {}
            good = []
            bad = []
            for ex, row in results.items():
                if not isinstance(row, dict):
                    continue
                ms = row.get("latency_ms")
                text = f"{ex}: ready" + (f" ({int(ms)} ms)" if isinstance(ms, (int, float)) else "")
                if row.get("ok"):
                    good.append(text)
                else:
                    bad.append(f"{ex}: not ready")
            if good:
                lines.append("Ready exchanges: " + ", ".join(good) + ".")
            if bad:
                lines.append("Currently not ready: " + ", ".join(bad) + ".")
            return lines or ["Connection warmup status checked."]

        if test_id == "arm_lock":
            exists = bool(data.get("exists"))
            match = bool(data.get("hash_match"))
            if not exists:
                lines.append("No ARM preparation record exists yet.")
            elif match:
                lines.append("Prepared settings are locked and consistent.")
            else:
                lines.append("Lock exists but does not match latest settings; re-ARM is recommended.")
            return lines

        if test_id == "telemetry":
            cnt = data.get("count")
            if isinstance(cnt, int):
                lines.append(f"Last {cnt} execution steps were recorded.")
            events = data.get("events") or []
            if events:
                lines.append("Logging system is active and steps are traceable.")
            return lines or ["Logging system checked."]

        if test_id == "exchange_test":
            results = data.get("results") or {}
            ok_rows = []
            warn_rows = []
            for ex, row in results.items():
                if not isinstance(row, dict):
                    continue
                net = bool(row.get("network_ok"))
                auth = row.get("auth_ok")
                if net and (auth is True or auth is None):
                    ok_rows.append(ex)
                else:
                    warn_rows.append(ex)
            if ok_rows:
                lines.append("Exchanges with healthy access: " + ", ".join(ok_rows) + ".")
            if warn_rows:
                lines.append("Exchanges needing review: " + ", ".join(warn_rows) + ".")
            return lines or ["Exchange access checked."]

        if test_id == "aggregator_state":
            modes = data.get("exchange_modes") or {}
            degraded = []
            normal = []
            for ex, mode in modes.items():
                if isinstance(mode, dict) and mode.get("degraded"):
                    degraded.append(ex)
                else:
                    normal.append(ex)
            if normal:
                lines.append("Exchanges in normal mode: " + ", ".join(normal) + ".")
            if degraded:
                lines.append("Exchanges in degraded mode: " + ", ".join(degraded) + ".")
            return lines or ["Listing sources checked."]

        if test_id == "market_presence":
            res = data.get("result") if isinstance(data.get("result"), dict) else data
            found = bool((res or {}).get("found_on_other_exchanges"))
            rows = list((res or {}).get("rows") or [])
            if found and rows:
                top = []
                for row in rows[:4]:
                    ex = str((row or {}).get("exchange") or "-")
                    pair = str((row or {}).get("pair") or "-")
                    top.append(f"{ex} ({pair})")
                lines.append("This token appears on other exchanges: " + ", ".join(top) + ".")
            elif found:
                lines.append("This token appears on other exchanges.")
            else:
                lines.append("This token was not found on other major exchanges.")
            method = str((res or {}).get("method") or "")
            if method == "symbol_only":
                lines.append("This result is symbol-only matching; confidence may be lower.")
            if (res or {}).get("ambiguous"):
                lines.append("Multiple close matches were found; nearest candidates were listed.")
            return lines
    except Exception:
        pass

    return ["Test completed. Result is available in readable form."]


@app.route("/tests", methods=["GET"])
def tests_page():
    ensure_scanner_started()
    scan_status = SCANNER_SERVICE.status()
    scan_rows = []
    for ex in LISTING_EXCHANGES:
        row = dict((scan_status.get("stats") or {}).get(ex) or {})
        backoff_until = row.get("backoffUntil")
        backoff_disp = "-"
        if isinstance(backoff_until, int) and backoff_until > int(time.time() * 1000):
            dt = datetime.datetime.fromtimestamp(backoff_until / 1000.0, tz=datetime.timezone.utc)
            dt_tr = dt.astimezone(TR_TZ) if TR_TZ else dt
            backoff_disp = dt_tr.strftime("%d.%m %H:%M:%S")
        scan_rows.append(
            {
                "exchange": ex,
                "mode": str(row.get("mode") or "-"),
                "interval": int(row.get("currentIntervalMs") or 0),
                "market_count": int(row.get("marketCount") or 0),
                "candidate_new": int(row.get("candidateNewCount") or 0),
                "verified_new": int(row.get("verifiedNewCount") or 0),
                "fetch_ms": int(row.get("fetchDurationMs") or 0),
                "last_success": format_clock(str(row.get("lastSuccessAt") or "")),
                "last_error_type": str(row.get("lastErrorType") or "-"),
                "last_error_message": str(row.get("lastErrorMessage") or "-"),
                "consecutive_errors": int(row.get("consecutiveErrors") or 0),
                "rate_limit_hits": int(row.get("rateLimitHits") or 0),
                "backoff_until": backoff_disp,
            }
        )

    exec_state = executor_get_status()
    poll_disp = format_clock(STATE.get("last_poll", ""))
    if poll_disp == "no data yet":
        poll_disp = "none yet"
    tests_raw = STATE.get("tests") or {}
    catalog_index = {row["id"]: row for row in TEST_CATALOG}
    tests_pretty = []
    for row in TEST_CATALOG:
        key = row["id"]
        if key not in tests_raw:
            continue
        raw = dict(tests_raw.get(key) or {})
        tests_pretty.append(
            {
                "id": key,
                "title": row["title"],
                "ok": bool(raw.get("ok")),
                "at": raw.get("at"),
                "at_display": format_clock(raw.get("at", "")),
                "summary": _test_summary(key, bool(raw.get("ok")), raw.get("data")),
                "lines": _human_test_details(key, bool(raw.get("ok")), raw.get("data")),
                "data": raw.get("data"),
            }
        )
    # Unknown keys (if any) are kept at bottom for visibility.
    for key, raw in tests_raw.items():
        if key in catalog_index:
            continue
        tests_pretty.append(
            {
                "id": key,
                "title": key,
                "ok": bool(raw.get("ok")),
                "at": raw.get("at"),
                "at_display": format_clock(raw.get("at", "")),
                "summary": _test_summary(key, bool(raw.get("ok")), raw.get("data")),
                "lines": _human_test_details(key, bool(raw.get("ok")), raw.get("data")),
                "data": raw.get("data"),
            }
        )

    return render_template_string(
        TESTS_HTML,
        test_catalog=TEST_CATALOG,
        tests_pretty=tests_pretty,
        last_poll=STATE.get("last_poll"),
        last_poll_display=poll_disp,
        exec_state=exec_state,
        last_action=STATE.get("last_action"),
        scanner_status=scan_status,
        scanner_rows=scan_rows,
    )


@app.route("/scanner", methods=["GET"])
def scanner_page():
    ensure_scanner_started()
    scan_status = SCANNER_SERVICE.status()
    source = app.config.get("LISTINGS") or {ex: [] for ex in agg.exchanges}
    listings = filter_out_expired(source)
    checks = STATE.get("checks") or {ex: None for ex in agg.exchanges}
    exchange_modes = STATE.get("exchange_modes") or {ex: {} for ex in agg.exchanges}
    errors = dict(STATE.get("errors") or {})
    friendly_errors = {ex: humanize_error(msg) for ex, msg in errors.items()}
    source_labels = {
        "api_announcement": "API announcements",
        "symbol_diff": "Symbol diff feed",
        "web_fallback": "Web fallback",
    }
    total_items = 0
    for ex in agg.exchanges:
        mode = dict(exchange_modes.get(ex) or {})
        primary_key = str(mode.get("primary_source") or "web_fallback")
        active_key = str(mode.get("active_source") or primary_key)
        mode["primary_source"] = primary_key
        mode["active_source"] = active_key
        mode["primary_label"] = source_labels.get(primary_key, primary_key)
        mode["active_label"] = source_labels.get(active_key, active_key)
        exchange_modes[ex] = mode

        ex_items = listings.get(ex, []) or []
        total_items += len(ex_items)
        for item in ex_items:
            raw_pair = guess_pair(item)
            item["pair_guess"] = normalize_symbol_for_exchange(ex, raw_pair) if raw_pair else ""

    ok_count = sum(1 for ex in agg.exchanges if checks.get(ex) is True)

    return render_template_string(
        SCANNER_HTML,
        listing_exchanges=agg.exchanges,
        listings=listings,
        checks=checks,
        exchange_modes=exchange_modes,
        errors=errors,
        friendly_errors=friendly_errors,
        last_poll=STATE.get("last_poll"),
        last_poll_display=format_clock(STATE.get("last_poll")),
        exchange_count=len(agg.exchanges),
        ok_count=ok_count,
        total_items=total_items,
        scanner_status=scan_status,
    )


@app.route("/scanner/run", methods=["POST"])
def scanner_run():
    try:
        SCANNER_SERVICE.run_once(reason="manual_force")
        refresh_scanner_state_cache()
        set_action(True, "Scanner run completed.")
    except Exception as e:
        set_action(False, f"Scanner run failed: {e}")
    return redirect(url_for("scanner_page"))


@app.route("/scanner/start", methods=["POST"])
def scanner_start():
    try:
        SCANNER_SERVICE.start()
        refresh_scanner_state_cache()
        set_action(True, "Scanner started.")
    except Exception as e:
        set_action(False, f"Scanner start failed: {e}")
    return redirect(url_for("tests_page"))


@app.route("/scanner/stop", methods=["POST"])
def scanner_stop():
    try:
        SCANNER_SERVICE.stop()
        refresh_scanner_state_cache()
        set_action(True, "Scanner stopped.")
    except Exception as e:
        set_action(False, f"Scanner stop failed: {e}")
    return redirect(url_for("tests_page"))


@app.route("/scanner/run-once", methods=["POST"])
def scanner_run_once():
    try:
        SCANNER_SERVICE.run_once(reason="manual_force")
        refresh_scanner_state_cache()
        set_action(True, "Scanner run-once completed.")
    except Exception as e:
        set_action(False, f"Scanner run-once failed: {e}")
    return redirect(url_for("tests_page"))


@app.route("/scanner/mode", methods=["POST"])
def scanner_set_mode():
    ex = str(request.form.get("exchange") or "").strip().lower()
    mode = str(request.form.get("mode") or "").strip().upper()
    ttl = str(request.form.get("ttl_sec") or "").strip()
    try:
        ttl_sec = int(ttl or "0")
    except Exception:
        ttl_sec = 0
    try:
        out = SCANNER_SERVICE.set_mode(ex, mode, ttl_sec=ttl_sec)
        if out.get("ok"):
            refresh_scanner_state_cache()
            set_action(True, f"Scanner mode updated: {ex} -> {mode}.")
        else:
            set_action(False, f"Scanner mode failed: {out.get('error')}")
    except Exception as e:
        set_action(False, f"Scanner mode failed: {e}")
    return redirect(url_for("tests_page"))


@app.route("/settings", methods=["GET"])
def settings_page():
    return render_template_string(SETTINGS_HTML)


def _fmt_real_test_report(report: dict):
    r = dict(report or {})
    buy = dict(r.get("buy") or {})
    sell = dict(r.get("sell") or {})
    mode = str(r.get("mode") or "buy_then_sell").strip().lower()
    if mode not in {"buy_then_sell", "sell_then_buy"}:
        mode = "buy_then_sell"
    mode_display = "Buy -> Sell" if mode == "buy_then_sell" else "Sell -> Buy"
    row = {
        "ok": bool(r.get("ok")),
        "mode": mode,
        "mode_display": mode_display,
        "exchange": str(r.get("exchange") or "-"),
        "symbol": str(r.get("symbol_sent") or r.get("symbol_input") or "-"),
        "spend_usdt": str(r.get("spend_usdt") or "-"),
        "amount_display": "-",
        "at_display": format_clock(str(r.get("started_at") or "")),
        "buy_latency": format_latency_text(buy.get("latency_ms")),
        "buy_status": str(buy.get("status") if buy.get("status") is not None else "-"),
        "buy_error": str(buy.get("error") or buy.get("error_hint") or "").strip(),
        "buy_price": fmt_price(buy.get("price")),
        "buy_qty": fmt_qty(buy.get("base_qty")),
        "sell_latency": "-",
        "sell_status": "-",
        "sell_error": "",
        "round_trip": format_latency_text(r.get("total_flow_ms")),
        "pnl": "-",
        "ui_to_vps": format_latency_text(r.get("ui_to_vps_ms")),
        "panel_to_executor": format_latency_text(r.get("panel_to_executor_ms")),
        "executor_flow": format_latency_text(r.get("executor_total_ms")),
        "vps_to_exchange_buy": format_latency_text(r.get("vps_to_exchange_buy_ms")),
        "vps_to_exchange_sell": format_latency_text(r.get("vps_to_exchange_sell_ms")),
        "click_to_done": format_latency_text(r.get("click_to_done_ms")),
        "scheduled_for": (
            format_epoch_ms_clock(r.get("target_execute_ms"))
            if r.get("target_execute_ms")
            else (
                format_clock_ms(str(r.get("target_execute_iso") or ""))
                if str(r.get("schedule_mode") or "").lower() == "scheduled"
                else "-"
            )
        ),
        "first_order_at": format_epoch_ms_clock(r.get("first_order_send_ms")),
        "schedule_drift": format_drift_ms(r.get("schedule_drift_ms")),
    }
    if row["spend_usdt"] != "-":
        if mode == "sell_then_buy":
            base, _quote = split_symbol_parts(row["symbol"])
            row["amount_display"] = f"{row['spend_usdt']} {(base or 'BASE')}"
        else:
            q = quote_currency_for_symbol(row["symbol"], row["exchange"])
            row["amount_display"] = f"{row['spend_usdt']} {q}"
    if isinstance(sell, dict):
        if not sell.get("skipped"):
            row["sell_latency"] = format_latency_text(sell.get("latency_ms"))
            row["sell_status"] = str(sell.get("status") if sell.get("status") is not None else "-")
            row["sell_error"] = str(sell.get("error") or sell.get("error_hint") or "").strip()
            pnl = sell.get("pnl_quote")
            pct = sell.get("pnl_pct")
            if isinstance(pnl, (int, float)):
                pnl_txt = f"{pnl:.6f} USDT".rstrip("0").rstrip(".")
                if isinstance(pct, (int, float)):
                    pnl_txt += f" ({pct:.2f}%)"
                row["pnl"] = pnl_txt
        else:
            reason = str(sell.get("reason") or "skipped")
            row["sell_status"] = reason
    return row


def _append_real_test_report(row: dict):
    rows = list(STATE.get("real_trade_tests") or [])
    rows.insert(0, dict(row or {}))
    STATE["real_trade_tests"] = rows[:40]


def _build_real_trade_stats(exec_state: dict, form_data: dict):
    order_history_raw = list((exec_state or {}).get("order_history") or [])
    trade_cycles_raw = list((exec_state or {}).get("trade_cycles") or [])
    open_positions_raw = list((exec_state or {}).get("open_positions") or [])

    recent_orders = []
    buy_lat = []
    sell_lat = []
    real_order_count = 0

    for row in reversed(order_history_raw[-120:]):
        r = dict(row or {})
        if bool(r.get("dry_run")):
            continue
        real_order_count += 1
        side = str(r.get("side") or "-").lower()
        latency_ms = r.get("engine_latency_ms")
        lat_txt = format_latency_text(latency_ms)
        if isinstance(latency_ms, int) and latency_ms > 0:
            if side == "buy":
                buy_lat.append(latency_ms)
            elif side == "sell":
                sell_lat.append(latency_ms)
        success = bool(r.get("success"))
        status = r.get("status")
        status_txt = "OK" if success else (f"ERR {status}" if status is not None else "ERR")
        recent_orders.append(
            {
                "at_display": format_clock(str(r.get("at") or "")),
                "exchange": str(r.get("exchange") or "-"),
                "symbol": str(r.get("symbol") or "-"),
                "side": side,
                "success": success,
                "status_text": status_txt,
                "price_display": fmt_price(r.get("price")),
                "qty_display": fmt_qty(r.get("base_qty")),
                "quote_display": fmt_qty(r.get("quote_qty")),
                "latency_text": lat_txt,
                "order_id": str(r.get("order_id") or "-"),
            }
        )

    recent_cycles = []
    pnl_values = []
    wins = 0
    for row in reversed(trade_cycles_raw[-80:]):
        r = dict(row or {})
        pnl_val = to_float(r.get("pnl_quote"))
        if pnl_val is not None:
            pnl_values.append(pnl_val)
            if pnl_val > 0:
                wins += 1
        recent_cycles.append(
            {
                "exchange": str(r.get("exchange") or "-"),
                "symbol": str(r.get("symbol") or "-"),
                "buy_time_display": format_clock(str(r.get("buy_time") or "")),
                "sell_time_display": format_clock(str(r.get("sell_time") or "")),
                "buy_price_display": fmt_price(r.get("buy_price")),
                "sell_price_display": fmt_price(r.get("sell_price")),
                "qty_display": fmt_qty(r.get("sell_qty") or r.get("buy_qty")),
                "pnl_display": fmt_pnl(r.get("pnl_quote")),
                "pnl_pct_display": fmt_pct(r.get("pnl_pct")),
                "pnl_positive": (pnl_val is not None and pnl_val >= 0),
            }
        )

    cycles_total = len(recent_cycles)
    pnl_total_val = sum(pnl_values) if pnl_values else None
    pnl_avg_val = (pnl_total_val / len(pnl_values)) if pnl_values else None
    win_rate_txt = f"{(wins / cycles_total) * 100:.1f}%" if cycles_total > 0 else "-"
    buy_avg_txt = format_latency_text(int(sum(buy_lat) / len(buy_lat))) if buy_lat else "-"
    sell_avg_txt = format_latency_text(int(sum(sell_lat) / len(sell_lat))) if sell_lat else "-"

    ex = str((form_data or {}).get("exchange") or "").strip().lower()
    sym_raw = str((form_data or {}).get("symbol") or "").strip()
    sym_norm = normalize_symbol_for_exchange(ex, sym_raw) if ex and sym_raw else ""
    open_position = find_open_position(open_positions_raw, ex, sym_norm) if ex and sym_norm else None

    stats = {
        "orders_total": str(real_order_count),
        "cycles_total": str(cycles_total),
        "win_rate": win_rate_txt,
        "pnl_total": fmt_pnl(pnl_total_val),
        "pnl_avg": fmt_pnl(pnl_avg_val),
        "buy_latency_avg": buy_avg_txt,
        "sell_latency_avg": sell_avg_txt,
        "open_position": "yes" if open_position else "no",
    }
    return stats, recent_orders[:80], recent_cycles[:80], open_position


def _load_wallet_rows_for_real_tests(form_data: dict):
    ex = str((form_data or {}).get("exchange") or "gate").strip().lower()
    if ex not in TRADE_EXCHANGE_SET:
        ex = "gate"
    try:
        out = executor_post(
            "/wallet-balances",
            {"exchange": ex, "non_zero_only": True, "limit": 120},
            timeout_sec=20,
        )
        results = dict(out.get("results") or {})
        row = dict(results.get(ex) or {})
        if not row.get("ok"):
            err = str(row.get("error") or "wallet_fetch_failed")
            return ex, [], err, format_latency_text(row.get("latency_ms"))
        rows = []
        for item in list(row.get("rows") or [])[:120]:
            rows.append(
                {
                    "currency": str(item.get("currency") or "-").upper(),
                    "available": fmt_qty(item.get("available")),
                    "locked": fmt_qty(item.get("locked")),
                    "total": fmt_qty(item.get("total")),
                }
            )
        return ex, rows, "", format_latency_text(row.get("latency_ms"))
    except Exception as e:
        return ex, [], str(e), "-"


@app.route("/real-tests", methods=["GET"])
def real_tests_page():
    exec_state = executor_get_status()
    form_data = dict(STATE.get("real_trade_form") or {})
    tests = list(STATE.get("real_trade_tests") or [])
    source = app.config.get("LISTINGS") or {ex: [] for ex in agg.exchanges}
    listings = filter_out_expired(source)
    pair_options_map = build_pair_options_map(listings, exec_state, STATE.get("last_order") or {})
    stats, recent_orders, recent_cycles, _ = _build_real_trade_stats(exec_state, form_data)
    wallet_exchange, wallet_rows, wallet_error, wallet_latency = _load_wallet_rows_for_real_tests(form_data)
    return render_template_string(
        REAL_TRADE_TEST_HTML,
        exec_state=exec_state,
        trade_exchanges=REAL_TEST_EXCHANGES,
        form_data=form_data,
        pair_options_map=pair_options_map,
        real_tests=tests,
        stats=stats,
        recent_orders=recent_orders,
        recent_cycles=recent_cycles,
        wallet_exchange=wallet_exchange,
        wallet_rows=wallet_rows,
        wallet_error=wallet_error,
        wallet_latency=wallet_latency,
        last_action=STATE.get("last_action"),
    )


def _read_real_trade_form():
    ex = (request.form.get("exchange") or "").strip().lower()
    symbol = (request.form.get("symbol") or "").strip()
    amount_mode = (request.form.get("amount_mode") or "fixed").strip().lower()
    if amount_mode not in {"fixed", "all", "percent"}:
        amount_mode = "fixed"
    spend_raw = (request.form.get("spend_usdt") or "").strip()
    spend = spend_raw if amount_mode in {"all", "percent"} else (spend_raw or "5")
    amount_percent = (request.form.get("amount_percent") or "100").strip()
    auto_sell = "1" if (request.form.get("auto_sell") or "").strip() == "1" else "0"
    sell_wait_sec = (request.form.get("sell_wait_sec") or "0").strip()
    sell_qty = (request.form.get("sell_qty") or "").strip()
    round_trip_mode = (request.form.get("round_trip_mode") or "buy_then_sell").strip().lower()
    if round_trip_mode not in {"buy_then_sell", "sell_then_buy"}:
        round_trip_mode = "buy_then_sell"
    exec_mode = (request.form.get("exec_mode") or "now").strip().lower()
    if exec_mode not in {"now", "scheduled"}:
        exec_mode = "now"
    schedule_at_local = (request.form.get("schedule_at_local") or "").strip()
    schedule_at_iso = (request.form.get("execute_at_iso") or "").strip()
    schedule_at_ms = None
    try:
        raw_schedule_ms = (request.form.get("execute_at_ms") or "").strip()
        if raw_schedule_ms:
            schedule_at_ms = int(float(raw_schedule_ms))
    except Exception:
        schedule_at_ms = None
    client_click_ms = None
    client_click_perf_ms = None
    try:
        raw_click_ms = (request.form.get("client_click_ms") or "").strip()
        if raw_click_ms:
            client_click_ms = int(float(raw_click_ms))
    except Exception:
        client_click_ms = None
    try:
        raw_click_perf_ms = (request.form.get("client_click_perf_ms") or "").strip()
        if raw_click_perf_ms:
            client_click_perf_ms = float(raw_click_perf_ms)
    except Exception:
        client_click_perf_ms = None
    STATE["real_trade_form"] = {
        "exchange": ex or "gate",
        "symbol": symbol,
        "spend_usdt": spend,
        "amount_mode": amount_mode,
        "amount_percent": amount_percent,
        "auto_sell": auto_sell,
        "sell_wait_sec": sell_wait_sec,
        "sell_qty": sell_qty,
        "round_trip_mode": round_trip_mode,
        "exec_mode": exec_mode,
        "schedule_at_local": schedule_at_local,
        "schedule_at_iso": schedule_at_iso,
        "schedule_at_ms": schedule_at_ms,
    }
    return (
        ex,
        symbol,
        spend,
        amount_mode,
        amount_percent,
        auto_sell,
        sell_wait_sec,
        sell_qty,
        round_trip_mode,
        exec_mode,
        schedule_at_local,
        schedule_at_iso,
        schedule_at_ms,
        client_click_ms,
        client_click_perf_ms,
    )


def _resolve_trade_amount(ex: str, norm_symbol: str, spend: str, amount_mode: str, amount_percent: str, first_side: str):
    mode = str(amount_mode or "fixed").strip().lower()
    if mode not in {"all", "percent"}:
        return spend, ""
    try:
        pct = 100.0 if mode == "all" else float(amount_percent or "100")
    except Exception:
        pct = 100.0
    if pct <= 0:
        pct = 100.0
    if pct > 100:
        pct = 100.0
    out = executor_post(
        "/balance-available",
        {
            "exchange": ex,
            "symbol": norm_symbol,
            "side": first_side,
            "percent": pct,
        },
        timeout_sec=20,
    )
    computed_text = str(out.get("computed_text") or "").strip()
    if not computed_text:
        raise RuntimeError("computed amount is empty")
    target_asset = str(out.get("target_asset") or "").upper()
    note = f"amount resolved from {pct:.2f}% of {target_asset} balance"
    return computed_text, note


def _compute_real_trade_timeout(exec_mode: str, schedule_at_ms) -> int:
    base_timeout = 75
    if str(exec_mode or "").strip().lower() != "scheduled":
        return base_timeout
    try:
        target_ms = int(schedule_at_ms)
    except Exception:
        return base_timeout
    wait_ms = max(0, target_ms - int(time.time() * 1000))
    return max(base_timeout, min(7200, int(wait_ms / 1000) + 90))


def _validate_real_trade_form(ex: str, symbol: str):
    if ex not in REAL_TEST_EXCHANGE_SET:
        return "Real trade action failed: invalid exchange."
    if ex not in TRADE_EXCHANGE_SET:
        return f"Real trade action is not active for {ex} yet. Live order route is currently available for: {', '.join(TRADE_EXCHANGES)}."
    if not symbol:
        return "Real trade action failed: pair is required."
    return ""


@app.route("/real-tests/buy", methods=["POST"])
def real_tests_buy():
    panel_received_ms = int(time.time() * 1000)
    ex, symbol, spend, amount_mode, amount_percent, auto_sell, sell_wait_sec, sell_qty, round_trip_mode, exec_mode, schedule_at_local, schedule_at_iso, schedule_at_ms, client_click_ms, client_click_perf_ms = _read_real_trade_form()
    err = _validate_real_trade_form(ex, symbol)
    if err:
        set_action(False, err)
        return redirect(url_for("real_tests_page"))

    norm_symbol = normalize_symbol_for_exchange(ex, symbol)
    if not norm_symbol:
        set_action(False, "Real trade action failed: pair format could not be parsed.")
        return redirect(url_for("real_tests_page"))

    resolved_note = ""
    try:
        spend, resolved_note = _resolve_trade_amount(ex, norm_symbol, spend, amount_mode, amount_percent, "buy")
    except Exception as e:
        set_action(False, f"Buy failed: amount resolution error ({e})")
        return redirect(url_for("real_tests_page"))

    payload = {
        "exchange": ex,
        "symbol": norm_symbol,
        "spend_usdt": spend,
        "auto_sell": auto_sell == "1",
        "round_trip": False,
        "round_trip_mode": round_trip_mode,
        "sell_wait_sec": sell_wait_sec or "0",
        "sell_qty": sell_qty,
        "exec_mode": exec_mode,
        "execute_at_iso": schedule_at_iso,
        "execute_at_ms": schedule_at_ms,
        "client_click_ms": client_click_ms,
        "client_click_perf_ms": client_click_perf_ms,
        "panel_received_ms": panel_received_ms,
    }
    try:
        panel_send_ms = int(time.time() * 1000)
        payload["panel_send_ms"] = panel_send_ms
        out = executor_post("/real-trade-test", payload, timeout_sec=_compute_real_trade_timeout(exec_mode, schedule_at_ms))
        panel_done_ms = int(time.time() * 1000)
        report = dict(out.get("report") or {})
        if client_click_ms:
            report["ui_to_vps_ms"] = max(0, panel_received_ms - client_click_ms)
            report["click_to_done_ms"] = max(0, panel_done_ms - client_click_ms)
        report["panel_to_executor_ms"] = max(0, panel_done_ms - panel_send_ms)
        report["ok"] = bool(out.get("ok"))
        pretty = _fmt_real_test_report(report)
        pretty["kind"] = "buy" if auto_sell != "1" else "round_trip"
        _append_real_test_report(pretty)
        if pretty.get("ok"):
            note_part = f" ({resolved_note})" if resolved_note else ""
            set_action(True, f"Buy sent: {ex} {norm_symbol}.{note_part}")
        else:
            fail_msg = pretty.get("buy_status") or "buy_failed"
            if pretty.get("buy_error"):
                fail_msg = f"{fail_msg} / {pretty.get('buy_error')}"
            set_action(False, f"Buy failed: {fail_msg}")
    except Exception as e:
        set_action(False, f"Buy failed: {e}")
    return redirect(url_for("real_tests_page"))


@app.route("/real-tests/sell", methods=["POST"])
def real_tests_sell():
    panel_received_ms = int(time.time() * 1000)
    ex, symbol, spend, amount_mode, amount_percent, auto_sell, sell_wait_sec, sell_qty, round_trip_mode, exec_mode, schedule_at_local, schedule_at_iso, schedule_at_ms, client_click_ms, client_click_perf_ms = _read_real_trade_form()
    err = _validate_real_trade_form(ex, symbol)
    if err:
        set_action(False, err)
        return redirect(url_for("real_tests_page"))

    norm_symbol = normalize_symbol_for_exchange(ex, symbol)
    if not norm_symbol:
        set_action(False, "Sell failed: pair format could not be parsed.")
        return redirect(url_for("real_tests_page"))

    resolved_note = ""
    resolved_spend = spend
    if not sell_qty.strip():
        try:
            resolved_spend, resolved_note = _resolve_trade_amount(ex, norm_symbol, spend, amount_mode, amount_percent, "sell")
        except Exception:
            resolved_spend = spend
            resolved_note = ""

    qty_text = sell_qty.strip() or resolved_spend.strip()
    if not qty_text:
        exec_state = executor_get_status()
        pos = find_open_position(exec_state.get("open_positions") or [], ex, norm_symbol)
        qty_num = to_float((pos or {}).get("base_qty"))
        if qty_num and qty_num > 0:
            qty_text = f"{qty_num:.10f}".rstrip("0").rstrip(".")

    if not qty_text:
        set_action(False, f"Sell failed: quantity is required (or open position needed) for {ex} {norm_symbol}.")
        return redirect(url_for("real_tests_page"))

    if auto_sell == "1":
        mode = "sell_then_buy" if round_trip_mode not in {"buy_then_sell", "sell_then_buy"} else round_trip_mode
        if mode != "sell_then_buy":
            mode = "sell_then_buy"
        payload = {
            "exchange": ex,
            "symbol": norm_symbol,
            "spend_usdt": qty_text,
            "auto_sell": True,
            "round_trip": True,
            "round_trip_mode": mode,
            "sell_wait_sec": sell_wait_sec or "0",
            "sell_qty": sell_qty or qty_text,
            "exec_mode": exec_mode,
            "execute_at_iso": schedule_at_iso,
            "execute_at_ms": schedule_at_ms,
            "client_click_ms": client_click_ms,
            "client_click_perf_ms": client_click_perf_ms,
            "panel_received_ms": panel_received_ms,
        }
        try:
            panel_send_ms = int(time.time() * 1000)
            payload["panel_send_ms"] = panel_send_ms
            out = executor_post("/real-trade-test", payload, timeout_sec=_compute_real_trade_timeout(exec_mode, schedule_at_ms))
            panel_done_ms = int(time.time() * 1000)
            report = dict(out.get("report") or {})
            if client_click_ms:
                report["ui_to_vps_ms"] = max(0, panel_received_ms - client_click_ms)
                report["click_to_done_ms"] = max(0, panel_done_ms - client_click_ms)
            report["panel_to_executor_ms"] = max(0, panel_done_ms - panel_send_ms)
            report["ok"] = bool(out.get("ok"))
            pretty = _fmt_real_test_report(report)
            pretty["kind"] = "round_trip"
            _append_real_test_report(pretty)
            if pretty.get("ok"):
                note_part = f" ({resolved_note})" if resolved_note else ""
                set_action(True, f"Sell -> Buy sent: {ex} {norm_symbol}.{note_part}")
            else:
                fail_msg = pretty.get("sell_status") or pretty.get("buy_status") or "round_trip_failed"
                set_action(False, f"Sell -> Buy failed: {fail_msg}")
        except Exception as e:
            set_action(False, f"Sell -> Buy failed: {e}")
        return redirect(url_for("real_tests_page"))

    payload = {
        "exchange": ex,
        "symbol": norm_symbol,
        "spend_usdt": qty_text,
        "auto_sell": False,
        "round_trip": False,
        "round_trip_mode": "sell_then_buy",
        "sell_wait_sec": "0",
        "sell_qty": qty_text,
        "exec_mode": exec_mode,
        "execute_at_iso": schedule_at_iso,
        "execute_at_ms": schedule_at_ms,
        "client_click_ms": client_click_ms,
        "client_click_perf_ms": client_click_perf_ms,
        "panel_received_ms": panel_received_ms,
    }
    try:
        panel_send_ms = int(time.time() * 1000)
        payload["panel_send_ms"] = panel_send_ms
        out = executor_post("/real-trade-test", payload, timeout_sec=_compute_real_trade_timeout(exec_mode, schedule_at_ms))
        panel_done_ms = int(time.time() * 1000)
        report = dict(out.get("report") or {})
        if client_click_ms:
            report["ui_to_vps_ms"] = max(0, panel_received_ms - client_click_ms)
            report["click_to_done_ms"] = max(0, panel_done_ms - client_click_ms)
        report["panel_to_executor_ms"] = max(0, panel_done_ms - panel_send_ms)
        report["ok"] = bool(out.get("ok"))
        pretty = _fmt_real_test_report(report)
        pretty["kind"] = "sell"
        pretty["mode_display"] = "Sell only"
        _append_real_test_report(pretty)
        if pretty.get("ok"):
            note_part = f" ({resolved_note})" if resolved_note else ""
            set_action(True, f"Sell sent: {ex} {norm_symbol}, qty {qty_text}.{note_part}")
        else:
            fail_msg = pretty.get("sell_status") or "sell_failed"
            if pretty.get("sell_error"):
                fail_msg = f"{fail_msg} / {pretty.get('sell_error')}"
            set_action(False, f"Sell failed: {fail_msg}")
    except Exception as e:
        set_action(False, f"Sell failed: {e}")
    return redirect(url_for("real_tests_page"))


@app.route("/real-tests/run", methods=["POST"])
def real_tests_run():
    panel_received_ms = int(time.time() * 1000)
    ex, symbol, spend, amount_mode, amount_percent, _auto_sell, sell_wait_sec, sell_qty, round_trip_mode, exec_mode, schedule_at_local, schedule_at_iso, schedule_at_ms, client_click_ms, client_click_perf_ms = _read_real_trade_form()
    err = _validate_real_trade_form(ex, symbol)
    if err:
        set_action(False, err)
        return redirect(url_for("real_tests_page"))

    norm_symbol = normalize_symbol_for_exchange(ex, symbol)
    if not norm_symbol:
        set_action(False, "Round-trip failed: pair format could not be parsed.")
        return redirect(url_for("real_tests_page"))

    first_side = "sell" if round_trip_mode == "sell_then_buy" else "buy"
    resolved_note = ""
    try:
        spend, resolved_note = _resolve_trade_amount(ex, norm_symbol, spend, amount_mode, amount_percent, first_side)
    except Exception as e:
        set_action(False, f"Round-trip failed: amount resolution error ({e})")
        return redirect(url_for("real_tests_page"))

    payload = {
        "exchange": ex,
        "symbol": norm_symbol,
        "spend_usdt": spend,
        "auto_sell": True,
        "round_trip": True,
        "round_trip_mode": round_trip_mode,
        "sell_wait_sec": sell_wait_sec or "0",
        "sell_qty": sell_qty,
        "exec_mode": exec_mode,
        "execute_at_iso": schedule_at_iso,
        "execute_at_ms": schedule_at_ms,
        "client_click_ms": client_click_ms,
        "client_click_perf_ms": client_click_perf_ms,
        "panel_received_ms": panel_received_ms,
    }
    try:
        panel_send_ms = int(time.time() * 1000)
        payload["panel_send_ms"] = panel_send_ms
        out = executor_post("/real-trade-test", payload, timeout_sec=_compute_real_trade_timeout(exec_mode, schedule_at_ms))
        panel_done_ms = int(time.time() * 1000)
        report = dict(out.get("report") or {})
        if client_click_ms:
            report["ui_to_vps_ms"] = max(0, panel_received_ms - client_click_ms)
            report["click_to_done_ms"] = max(0, panel_done_ms - client_click_ms)
        report["panel_to_executor_ms"] = max(0, panel_done_ms - panel_send_ms)
        report["ok"] = bool(out.get("ok"))
        pretty = _fmt_real_test_report(report)
        pretty["kind"] = "round_trip"
        _append_real_test_report(pretty)
        if pretty.get("ok"):
            note_part = f" ({resolved_note})" if resolved_note else ""
            set_action(True, f"Round-trip sent: {ex} {norm_symbol} ({pretty.get('mode_display', '-')}).{note_part}")
        else:
            fail_msg = pretty.get("buy_status") or pretty.get("sell_status") or "round_trip_failed"
            set_action(False, f"Round-trip failed: {fail_msg}")
    except Exception as e:
        set_action(False, f"Round-trip failed: {e}")
    return redirect(url_for("real_tests_page"))


@app.route("/tests/run", methods=["POST"])
def tests_run():
    action = (request.form.get("action") or "").strip()
    label = next((x["title"] for x in TEST_CATALOG if x["id"] == action), action or "test")
    try:
        out = _run_diagnostic_action(action)
        _store_test_result(action, True, out)
        set_action(True, f"Test completed: {label}.")
    except Exception as e:
        _store_test_result(action or "unknown", False, {"error": str(e)})
        set_action(False, f"Test failed: {label}. {e}")
    return redirect(url_for("tests_page"))


@app.route("/admin", methods=["GET", "POST"])
def admin_settings():
    msg = None
    ok = True
    _, _, dry_state = load_admin_state()
    if request.method == "POST":
        updates = {}
        runtime_values = {}
        for key in CONFIG_KEYS:
            form_val = (request.form.get(key) or "").strip()
            if key in SENSITIVE_KEYS:
                if form_val:
                    updates[key] = form_val
                    runtime_values[key] = form_val
            else:
                if not form_val and key in RUNTIME_DEFAULTS:
                    form_val = RUNTIME_DEFAULTS[key]
                updates[key] = form_val
                runtime_values[key] = form_val

        dry_form = request.form.get("dry_run_mode")
        dry_run = (dry_form.strip() == "1") if dry_form is not None else dry_state
        updates["DRY_RUN"] = "1" if dry_run else "0"
        os.environ["DRY_RUN"] = updates["DRY_RUN"]

        try:
            upsert_env_file(ENV_FILE_PATH, updates)
            for key, value in updates.items():
                os.environ[key] = value
            try:
                executor_post("/admin/config", {"dry_run": dry_run, "values": runtime_values})
                msg = "Settings saved and applied live."
            except Exception as e:
                ok = False
                msg = f"Settings saved, but could not be applied to executor: {e}"
        except Exception as e:
            ok = False
            msg = f"Settings could not be saved: {e}"

    cfg, flags, _ = load_admin_state()
    return render_template_string(
        ADMIN_HTML,
        msg=msg,
        ok=ok,
        cfg=cfg,
        flags=flags,
    )


if __name__ == "__main__":
    t = threading.Thread(target=bg_loop, daemon=True)
    t.start()
    app.run(host=PANEL_HOST, port=PANEL_PORT, debug=False)
