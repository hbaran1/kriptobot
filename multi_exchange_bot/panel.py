import os, re, time, threading, datetime, requests
from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from dateutil import parser as dt_parser, tz
from bot.listings_agg import ListingsAggregator
from bot.util import iso_utc, tz_name

PANEL_PORT = int(os.getenv("PANEL_PORT", "5177"))
PANEL_HOST = os.getenv("PANEL_HOST", "127.0.0.1")
EXECUTOR_URL = os.getenv("EXECUTOR_URL", "http://127.0.0.1:8080").rstrip("/")
PILOT_TOKEN = os.getenv("PILOT_TOKEN", "change-me")
POLL_SECONDS = 30 * 60

app = Flask(__name__)
agg = ListingsAggregator()
STATE = {
    "last_poll": None,
    "errors": {},
    "checks": {},
    "last_action": None,
    "countdown": None,
    "last_order": {},
}
TR_TZ = tz.gettz(tz_name())

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
]
SENSITIVE_KEYS = {
    "GATE_KEY", "GATE_SECRET",
    "BINANCE_KEY", "BINANCE_SECRET",
    "MEXC_KEY", "MEXC_SECRET",
    "KUCOIN_KEY", "KUCOIN_SECRET", "KUCOIN_PASSPHRASE",
    "BITGET_KEY", "BITGET_SECRET", "BITGET_PASSPHRASE",
}
RUNTIME_DEFAULTS = {
    "GATE_BASE": "https://api.gateio.ws/api/v4",
    "BINANCE_BASE": "https://api.binance.com",
    "MEXC_BASE": "https://api.mexc.com",
    "KUCOIN_BASE": "https://api.kucoin.com",
    "BITGET_BASE": "https://api.bitget.com",
}


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
    .btn-main {
      background: transparent;
      border: 1px solid #334155;
      color: #334155;
      border-radius: 50px;
      font-weight: 700;
      text-transform: uppercase;
      font-size: 13px;
      letter-spacing: 1.3px;
      transition: all 0.25s ease;
      min-height: 52px;
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
      font-size: 22px;
      font-weight: 800;
      letter-spacing: .2px;
      line-height: 1.1;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      color: #1e293b;
    }
    .brand-wordmark {
      display: inline-flex;
      align-items: baseline;
      gap: 0;
      line-height: 1;
    }
    .brand-word-main { color: #1e293b; }
    .brand-word-accent { color: #0f766e; }
    .brand-tagline {
      margin: 0 0 1px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: .2px;
      color: #64748b;
      line-height: 1.2;
    }
    .brand-logo-shell {
      width: 34px;
      height: 34px;
      border-radius: 999px;
      border: 1px solid #334155;
      background: #fff;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      overflow: hidden;
      flex: 0 0 auto;
    }
    .brand-logo-img {
      width: 100%;
      height: 100%;
      object-fit: cover;
      display: block;
    }
    .brand-logo-fallback {
      display: none;
      font-size: 16px;
      line-height: 1;
      color: #3f7ca0;
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
      font-size: 14px;
      line-height: 1.35;
      font-weight: 500;
      color: #475569;
      letter-spacing: 0;
    }
    .status-line.dim {
      font-size: 14px;
      line-height: 1.3;
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
      .brand-title { font-size: 19px; }
      .brand-tagline { font-size: 10px; }
    }
    @media (max-width: 1024px) {
      .status-line { font-size: 13px; }
      .pending-grid { grid-template-columns: 1fr; }
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
          <span class="brand-logo-shell" aria-hidden="true">
            <img src="/static/quickbot-logo.png" class="brand-logo-img" alt="" onerror="this.style.display='none';this.nextElementSibling.style.display='inline-flex';">
            <span class="brand-logo-fallback">⚡</span>
          </span>
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
        {% set latency_selected = last_order.get("exchange") if last_order.get("exchange") in exchanges else exchanges[0] %}
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
                  <span class="status-line">{{last_poll_parts["date"]}}</span>
                  <span class="status-line">{{last_poll_parts["time"]}} (TRT)</span>
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
                    <div class="pending-item"><span>Amount</span><b>{{pending_info["spend_usdt"]}} USDT</b></div>
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
            {% for ex in exchanges %}
              {% set row = order_latency.get(ex) or {} %}
              <button
                class="tab-btn latency-chip {% if loop.first %}active{% endif %}"
                data-tab="{{ex}}"
                data-latency-ex="{{ex}}"
                data-ms="{{row.get('engine_latency_text') or '-'}}"
              >
                {{ex}}
                {% if errors.get(ex) %}<span class="pill err" title="{{errors.get(ex)[:120]}}">ERR</span>{% endif %}
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
            {% for ex in exchanges %}
              <section class="exchange-panel {% if loop.first %}active{% endif %}" id="panel-{{ex}}">
                <p class="text-sm font-bold mb-2">
                  {% if checks.get(ex) is sameas true %}
                    <span class="check-ok">✓ Feed checked</span>
                  {% elif checks.get(ex) is sameas false %}
                    <span class="check-bad">✕ Feed check failed</span>
                  {% else %}
                    <span class="muted">Waiting for check...</span>
                  {% endif %}
                </p>
                {% if listings[ex] %}
                  <div class="news-list">
                    {% for item in listings[ex][:16] %}
                      <div class="news-item">
                        <div class="news-title-row">
                          <div class="news-title" title="{{item['title']}}">{{item["title"]}}</div>
                          {% if item["is_new"] %}<span class="pill new">NEW</span>{% endif %}
                        </div>
                        <div class="news-meta">
                          <span>{{item["trade_start_display"]}}</span>
                          <span class="news-actions">
                            {% if item.get("pair_guess") %}
                              <button class="pair-btn" type="button" data-pair="{{item['pair_guess']}}" data-exchange="{{ex}}">{{item["pair_guess"]}}</button>
                            {% endif %}
                            <a href="{{item["url"]}}" target="_blank" rel="noopener" class="news-open">Open</a>
                          </span>
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
            <a href="/admin" class="w-10 h-10 rounded-full border border-[#334155] flex items-center justify-center text-[#334155] hover:bg-slate-50 no-underline text-xl leading-none font-semibold" aria-label="Admin Settings" title="Admin Settings">
              ⚙
            </a>
          </div>

          <form method="POST" action="/arm" class="grid grid-cols-1 md:grid-cols-3 gap-4 sm:gap-6 mb-6 md:mb-8" id="arm-form">
            <div>
              <label class="block text-[10px] font-bold text-slate-400 mb-2 uppercase tracking-wide">Exchange</label>
              <select class="input-box w-full p-3 text-slate-700 outline-none cursor-pointer font-bold text-sm bg-white" name="exchange" id="exchange-select">
                {% for ex in exchanges %}
                  <option value="{{ex}}" {% if last_order.get("exchange") == ex %}selected{% endif %}>{{ex}}</option>
                {% endfor %}
              </select>
            </div>
            <div>
              <label class="block text-[10px] font-bold text-slate-400 mb-2 uppercase tracking-wide">Pair</label>
              <input id="symbol-input" name="symbol" type="text" class="input-box w-full p-3 text-slate-700 outline-none font-bold text-sm bg-white" placeholder="ABC_USDT / ABCUSDT / ABC-USDT" value="{{last_order.get('symbol','')}}" required>
            </div>
            <div>
              <label class="block text-[10px] font-bold text-slate-400 mb-2 uppercase tracking-wide">Amount (USDT)</label>
              <input id="spend-input" name="spend_usdt" type="number" min="0.01" step="0.01" value="{{last_order.get('spend_usdt','5')}}" class="input-box w-full p-3 text-slate-700 outline-none font-bold text-sm bg-white" required>
            </div>
          </form>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 sm:gap-6 mt-auto">
            <button type="submit" form="arm-form" class="btn-main buy-btn py-4 flex items-center justify-center gap-2 shadow-sm">
              <span>Buy</span>
            </button>
            <form method="POST" action="/stop-buy">
              <button type="submit" class="btn-main stop-btn py-4 flex items-center justify-center gap-2 shadow-sm w-full">
                <span>Sell</span>
              </button>
            </form>
          </div>

          {% if countdown %}
            <div class="countdown-box" data-target="{{countdown['target_iso']}}" id="countdown-box">
              <div>Time remaining for <b>{{countdown["exchange"]}} / {{countdown["symbol"]}}</b></div>
              <div class="countdown-value" id="countdown-value">--:--:--</div>
              <div class="muted text-sm">{{countdown["target_iso"]}}</div>
            </div>
          {% endif %}
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
      </div>
    </main>
  </div>

  <script>
    const tabs = document.querySelectorAll('.tab-btn');
    const panels = document.querySelectorAll('.exchange-panel[id^="panel-"]');
    const exchangeSelect = document.getElementById('exchange-select');
    const symbolInput = document.getElementById('symbol-input');
    const spendInput = document.getElementById('spend-input');
    const probeForm = document.getElementById('probe-form');
    const probeExchangeInput = document.getElementById('probe-exchange');
    const probeSymbolInput = document.getElementById('probe-symbol');
    const probeSpendInput = document.getElementById('probe-spend');
    const latencyMsView = document.getElementById('latency-ms-view');
    const latencyChips = document.querySelectorAll('.latency-chip[data-latency-ex]');

    tabs.forEach((tab) => {
      tab.addEventListener('click', () => {
        const key = tab.getAttribute('data-tab');
        tabs.forEach((t) => t.classList.remove('active'));
        panels.forEach((p) => p.classList.remove('active'));
        tab.classList.add('active');
        const panel = document.getElementById('panel-' + key);
        if (panel) panel.classList.add('active');
      });
    });

    document.querySelectorAll('.pair-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const pair = btn.getAttribute('data-pair') || '';
        const ex = btn.getAttribute('data-exchange') || '';
        if (exchangeSelect && ex) exchangeSelect.value = ex;
        if (symbolInput && pair) {
          symbolInput.value = pair;
          symbolInput.focus();
        }
        const topCard = document.querySelector('.trade-panel');
        if (topCard) topCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
      });
    });

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
          if (latencyMsView) latencyMsView.textContent = chip.getAttribute('data-ms') || '-';
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
  </script>
</body>
</html>"""

ADMIN_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin Settings</title>
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
      margin: 0;
    }
    .panel {
      background: #ffffff;
      border: 1px solid #334155;
      border-radius: 12px;
      box-shadow: 0 4px 0 #e2e8f0;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
      margin: 0;
      font-size: 20px;
      font-weight: 700;
      line-height: 1.1;
      color: #1e293b;
    }
    .brand-wordmark {
      display: inline-flex;
      align-items: baseline;
      gap: 0;
      line-height: 1;
    }
    .brand-word-main { color: #1e293b; }
    .brand-word-accent { color: #0f766e; }
    .brand-word-sub { color: #334155; opacity: .78; }
    .brand-icon {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 34px;
      height: 34px;
      border-radius: 999px;
      border: 1px solid #334155;
      background: #fff;
      overflow: hidden;
    }
    .brand-icon img {
      width: 100%;
      height: 100%;
      object-fit: cover;
      display: block;
    }
    .sub {
      margin: 5px 0 0;
      color: #64748b;
      font-size: 12px;
      font-weight: 500;
    }
    .field { display: grid; gap: 5px; }
    .field label { font-size: 11px; font-weight: 700; letter-spacing: .3px; color: #64748b; text-transform: uppercase; }
    input, button {
      border: 1px solid #cbd5e1;
      border-radius: 10px;
      background: #fff;
      color: #334155;
      padding: 10px 11px;
      font-size: 13px;
      font-weight: 600;
      outline: none;
      width: 100%;
    }
    input:focus { border-color: #334155; box-shadow: 0 0 0 2px rgba(51,65,85,.08); }
    .msg { padding: 10px 12px; border-radius: 10px; margin-top: 10px; font-size: 13px; font-weight: 600; }
    .msg.ok { background: #f0fdf4; border: 1px solid #86efac; color: #166534; }
    .msg.err { background: #fef2f2; border: 1px solid #fecaca; color: #991b1b; }
    .cards {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }
    .ex-card {
      border: 1px solid #334155;
      border-radius: 12px;
      padding: 12px;
      background: #f8fafc;
      box-shadow: 0 4px 0 #e2e8f0;
      display: grid;
      gap: 10px;
    }
    .ex-card h3 {
      margin: 0;
      font-size: 15px;
      font-weight: 700;
      color: #1e293b;
      letter-spacing: .2px;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      margin-left: 6px;
      border-radius: 999px;
      border: 1px solid #86efac;
      background: #f0fdf4;
      color: #166534;
      padding: 1px 7px;
      font-size: 10px;
      font-weight: 700;
      vertical-align: middle;
      line-height: 1.2;
    }
    .actions {
      margin-top: 14px;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .btn {
      border: 1px solid #334155;
      border-radius: 50px;
      background: #fff;
      color: #334155;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .6px;
      text-transform: uppercase;
      text-decoration: none;
      min-height: 46px;
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
    @media (max-width: 1200px) {
      .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 720px) {
      .cards { grid-template-columns: 1fr; }
      .actions { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="w-full py-5 px-4 sm:px-6 md:px-10 xl:px-16 2xl:px-24 flex flex-col gap-5">
    <section class="panel p-4">
      <form method="POST" action="/admin">
        <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 mb-2">
          <h1 class="brand">
            <span class="brand-icon" aria-hidden="true">
              <img src="/static/quickbot-logo.png" alt="" onerror="this.style.display='none';this.parentElement.textContent='⚡';this.parentElement.style.color='#3f7ca0';this.parentElement.style.fontSize='16px';">
            </span>
            <span class="brand-wordmark"><span class="brand-word-main">quick</span><span class="brand-word-accent">bot</span></span><span class="brand-word-sub">admin</span>
          </h1>
          <a class="btn" href="/">Back to Panel</a>
        </div>
        <p class="sub">Manage exchange API credentials. Leave secret fields empty to keep existing values.</p>
        {% if msg %}
          <div class="msg {{ 'ok' if ok else 'err' }}">{{msg}}</div>
        {% endif %}

        <section class="cards mt-4">
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
        </section>

        <div class="actions">
          <a class="btn" href="/">Back to Panel</a>
          <button type="submit" class="btn primary">Save and Apply Settings</button>
        </div>
      </form>
    </section>
  </main>
</body>
</html>"""


def executor_get_status():
    default = {
        "online": False,
        "dry_run": True,
        "gate": {"armed": False, "phase": "idle", "symbol": None},
        "last_execution": {},
        "order_latency": {},
    }
    try:
        r = requests.get(EXECUTOR_URL + "/status", timeout=5)
        r.raise_for_status()
        data = r.json()
        return {
            "online": True,
            "dry_run": bool(data.get("dry_run", True)),
            "gate": data.get("gate", default["gate"]),
            "last_execution": data.get("last_execution") or {},
            "order_latency": data.get("order_latency") or {},
        }
    except Exception as e:
        default["error"] = str(e)
        return default


def executor_post(path: str, payload=None):
    headers = {"X-PILOT-TOKEN": PILOT_TOKEN}
    r = requests.post(EXECUTOR_URL + path, json=payload, headers=headers, timeout=8)
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


QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BTC", "ETH", "TRY", "EUR", "BNB")
PAIR_STOPWORDS = {
    "LISTING", "LISTINGS", "SPOT", "FUTURES", "TRADE", "MARKET", "TOKEN", "EVENT",
    "UTC", "GMT", "NEW", "ZONE", "PRE", "SOON", "WILL", "LIST",
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
    if ex == "kucoin":
        return f"{base}-{quote}"
    return f"{base}{quote}"


def format_latency_text(ms_value):
    try:
        ms = int(ms_value)
    except Exception:
        return "-"
    if ms <= 0:
        return "-"
    sec = ms / 1000.0
    return f"{ms} ms ({sec:.3f} sec)"


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
    return parse_raw_time(item.get("raw_time_text", "UNKNOWN"))


def guess_pair(item):
    url = str(item.get("url", "")).upper()
    title = str(item.get("title", "")).upper()
    raw = str(item.get("raw_time_text", "")).upper()
    quote_group = "|".join(QUOTE_SUFFIXES)

    # URL tabanli en guvenilir yakalama
    m = re.search(r"/TRADE/([A-Z0-9]+_[A-Z0-9]+)", url)
    if m:
        pair = m.group(1)
        parts = pair.split("_", 1)
        if len(parts) == 2 and parts[1] in QUOTE_SUFFIXES and parts[0] not in PAIR_STOPWORDS:
            return pair

    blobs = [title, raw, url]
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

    # Baslikta (BNKR), (FT) gibi ticker geciyorsa varsayilan olarak USDT paritesi oner.
    for m in re.finditer(r"\(([A-Z0-9]{2,16})\)", title):
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
    return ""


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
    filtered = {}
    for ex, items in listings.items():
        kept = []
        for item in items:
            dt = parse_item_time(item)
            if dt is not None and dt < now:
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
    }


def bg_loop():
    while True:
        try:
            res, errs = agg.poll_once()
            app.config["LISTINGS"] = res
            STATE["errors"] = errs
            STATE["checks"] = {ex: (ex not in errs) for ex in agg.exchanges}
            STATE["last_poll"] = iso_utc()
        except Exception:
            STATE["checks"] = {ex: False for ex in agg.exchanges}
            STATE["last_poll"] = iso_utc()
        time.sleep(POLL_SECONDS)


@app.route("/", methods=["GET"])
def index():
    source = app.config.get("LISTINGS") or {ex: [] for ex in agg.exchanges}
    listings = filter_out_expired(source)
    friendly_errors = {ex: humanize_error(msg) for ex, msg in STATE["errors"].items()}
    checks = STATE.get("checks") or {ex: None for ex in agg.exchanges}
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
    for ex in listings:
        for item in listings[ex]:
            disp = format_trade_start(item)
            item["trade_start_display"] = disp
            item["trade_start_ok"] = disp != "UNKNOWN"
            raw_pair = guess_pair(item)
            item["pair_guess"] = normalize_symbol_for_exchange(ex, raw_pair) if raw_pair else ""
            item["detected_display"] = format_clock(item.get("detected_at", ""))
            item["detected_parts"] = split_clock_parts(item.get("detected_at", ""))
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
            "phase": p_phase,
            "target_display": "",
        }
        if countdown and countdown.get("exchange") == p_exchange:
            pending_info["target_display"] = format_clock(countdown.get("target_iso", ""))
    return render_template_string(
        HTML,
        exchanges=agg.exchanges,
        listings=listings,
        errors=STATE["errors"],
        checks=checks,
        friendly_errors=friendly_errors,
        last_poll=STATE["last_poll"],
        last_poll_display=format_clock(STATE["last_poll"]),
        last_poll_parts=split_clock_parts(STATE["last_poll"]),
        executor_url=EXECUTOR_URL,
        exec_state=exec_state,
        last_exec=last_exec,
        order_latency=order_latency,
        last_action=STATE.get("last_action"),
        countdown=countdown,
        last_order=last_order,
        pending_info=pending_info,
    )


@app.route("/arm", methods=["POST"])
def arm():
    ex = request.form.get("exchange", "").strip().lower()
    symbol = request.form.get("symbol", "").strip()
    spend = request.form.get("spend_usdt", "5").strip()
    norm_symbol = normalize_symbol_for_exchange(ex, symbol)
    STATE["last_order"] = {
        "exchange": ex,
        "symbol": symbol,
        "symbol_normalized": norm_symbol,
        "spend_usdt": spend,
    }

    if ex not in agg.exchanges:
        set_action(False, "Invalid exchange selection.")
        return redirect(url_for("index"))
    if not symbol:
        set_action(False, "Symbol/pair cannot be empty.")
        return redirect(url_for("index"))
    if not norm_symbol:
        set_action(False, "Symbol/pair format could not be parsed.")
        return redirect(url_for("index"))

    try:
        out = executor_post("/arm", {"exchange": ex, "symbol": norm_symbol, "spend_usdt": spend})
        mode = out.get("mode")
        countdown = find_countdown_target(ex, norm_symbol, app.config.get("LISTINGS") or {})
        STATE["countdown"] = countdown
        if ex == "gate" or mode == "ws_trigger":
            base_msg = f"Order prepared for {ex}: {norm_symbol}."
        else:
            base_msg = f"Buy request sent to {ex} ({norm_symbol})."
        if countdown:
            set_action(True, base_msg + " Countdown started.")
        else:
            set_action(True, base_msg + " No future listing time found for this pair.")
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


@app.route("/exchange-test", methods=["POST"])
def exchange_test():
    target = request.form.get("exchange_test_target", "").strip().lower()
    payload = {"exchange": target} if target in agg.exchanges else {}
    try:
        out = executor_post("/exchange-test", payload)
        STATE["exchange_test"] = out if isinstance(out, dict) else None
        ok_count = 0
        total = 0
        for ex in agg.exchanges:
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
    if ex not in agg.exchanges:
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
