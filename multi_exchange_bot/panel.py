import os, re, time, threading, datetime, requests
from flask import Flask, render_template_string, request, redirect, url_for
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
        raise ValueError("Env dosya yolu boş.")
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
<html lang="tr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Yeni Listeleme İşlem Botu</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Lexend:wght@300;400;500;600;700;800&display=swap');
    :root {
      --bg: #FFF1B5;
      --surface: #C1DBE8;
      --line: #43302E;
      --text: #43302E;
      --muted: rgba(67, 48, 46, .78);
      --accent: #43302E;
      --accent-2: #C1DBE8;
      --warn: #43302E;
      --ok: #C1DBE8;
      --shadow: rgba(67, 48, 46, .18);
    }
    * {
      box-sizing: border-box;
      font-family: "Lexend", "Segoe UI", "Noto Sans", sans-serif;
    }
    body {
      margin: 0;
      color: var(--text);
      background: var(--bg);
    }
    .wrap {
      max-width: 1240px;
      margin: 0 auto;
      padding: 18px 26px 16px;
    }
    .top {
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 14px;
      margin: 0 auto 14px;
      max-width: 1160px;
    }
    .card {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px 16px;
      box-shadow: 0 16px 40px -20px var(--shadow);
    }
    .page-title-row {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0;
      margin: 0 0 10px;
    }
    .page-title {
      margin: 0;
      font-size: 18px;
      font-weight: 800;
      letter-spacing: .25px;
      text-align: center;
    }
    .system-dot {
      width: 11px;
      height: 11px;
      border-radius: 999px;
      display: inline-block;
      border: 1px solid rgba(67, 48, 46, .35);
      box-shadow: 0 0 0 1px rgba(255, 255, 255, .55) inset;
    }
    .system-dot.online { background: #4bb86a; }
    .system-dot.offline { background: #d66767; }
    .system-state {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .2px;
      text-transform: lowercase;
    }
    .system-state.online { color: #2f7f47; }
    .system-state.offline { color: #9a3f3f; }
    .status-indicator-row {
      display: flex;
      justify-content: flex-end;
      margin-bottom: 2px;
    }
    .muted {
      color: var(--muted);
      font-size: 13px;
    }
    .status-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 9px;
      margin-top: 10px;
      align-items: stretch;
    }
    .status-note {
      position: relative;
      background: #fff;
      border: 1px solid rgba(67, 48, 46, .15);
      border-radius: 14px;
      min-height: 88px;
      box-shadow: 0 10px 18px -16px rgba(67, 48, 46, .35);
      padding: 6px;
      transform-origin: 50% 12%;
    }
    .status-note::before {
      content: "";
      position: absolute;
      top: -6px;
      left: 50%;
      width: 10px;
      height: 10px;
      border-radius: 999px;
      transform: translateX(-50%);
      box-shadow: 0 2px 8px rgba(67, 48, 46, .24);
      background: #846044;
    }
    .status-note:nth-child(1)::before { background: #f07f5a; }
    .status-note:nth-child(2)::before { background: #6f73b9; }
    .status-note:nth-child(3)::before { background: #7b5ac7; }
    .status-note:nth-child(4)::before { background: #d47f4a; }
    .status-note:nth-child(1) .status-inner { background: #f8efe7; }
    .status-note:nth-child(2) .status-inner { background: #eaf0ff; }
    .status-note:nth-child(3) .status-inner { background: #efe5ff; }
    .status-note:nth-child(4) .status-inner { background: #fff1e6; }
    .status-note:nth-child(1) .status-label { color: #7a604f; }
    .status-note:nth-child(2) .status-label { color: #566596; }
    .status-note:nth-child(3) .status-label { color: #654796; }
    .status-note:nth-child(4) .status-label { color: #8a5a3f; }

    .status-note:hover { box-shadow: 0 12px 20px -16px rgba(67, 48, 46, .38); }
    .status-inner {
      border-radius: 10px;
      min-height: 74px;
      padding: 8px 7px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      text-align: center;
      border: 1px solid rgba(67, 48, 46, .08);
      gap: 4px;
    }
    .status-inner { background: #f8efe7; }
    .status-label {
      font-size: 12px;
      color: var(--muted);
      font-weight: 700;
      letter-spacing: .2px;
      margin: 0;
    }
    .status-value {
      font-size: 11px;
      line-height: 1.3;
      color: var(--text);
      font-weight: 600;
      overflow-wrap: anywhere;
      word-break: break-word;
      margin: 0;
    }
    .iso-text {
      font-size: 11px;
      font-weight: 500;
      letter-spacing: 0;
      line-height: 1.25;
      opacity: .92;
      overflow-wrap: anywhere;
      word-break: break-word;
      font-family: inherit;
    }
    .status-note .iso-text {
      font-size: 11px;
      font-weight: 600;
      line-height: 1.3;
    }
    .clock-date, .clock-time {
      display: block;
      text-align: center;
    }
    .clock-time {
      margin-top: 1px;
    }
    .pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 11px;
      border: 1px solid transparent;
      margin-left: 8px;
    }
    .pill.err { color: #FFF1B5; background: rgba(67, 48, 46, .75); border-color: #43302E; }
    .pill.new { color: #43302E; background: rgba(193, 219, 232, .95); border-color: #C1DBE8; }
    .pill.ok  { color: #43302E; background: rgba(193, 219, 232, .95); border-color: #C1DBE8; }
    .pill.off { color: #FFF1B5; background: rgba(67, 48, 46, .75); border-color: #43302E; }
    .action { margin: 2px 0 10px; font-size: 13px; }
    .action.ok { color: var(--ok); }
    .action.err { color: var(--warn); }
    .trade-card {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .trade-head {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 10px;
    }
    .trade-card h3 {
      margin: 0;
      font-size: 16px;
    }
    .trade-hint {
      margin: 0;
      line-height: 1.45;
      color: var(--muted);
      font-size: 13px;
    }
    .form-grid {
      display: grid;
      grid-template-columns: 1fr 1.15fr .95fr;
      gap: 10px;
      align-items: end;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 0;
    }
    .field label {
      color: var(--muted);
      font-size: 13px;
      font-weight: 700;
      line-height: 1.2;
    }
    select, input, button {
      border: 1px solid var(--line);
      background: #FFF1B5;
      color: #43302E;
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 14px;
      outline: none;
      min-height: 50px;
      box-sizing: border-box;
      width: 100%;
    }
    button {
      cursor: pointer;
      background: var(--accent);
      border-color: var(--accent);
      font-weight: 600;
      color: #FFF1B5;
    }
    .link-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 1px solid #43302E;
      background: rgba(193, 219, 232, .75);
      color: #43302E;
      border-radius: 10px;
      padding: 8px 12px;
      font-size: 13px;
      font-weight: 700;
      text-decoration: none;
    }
    .link-btn:hover {
      background: rgba(193, 219, 232, .95);
      text-decoration: none;
    }
    .link-btn.icon-only {
      width: 44px;
      min-width: 44px;
      min-height: 44px;
      padding: 0;
      border-radius: 999px;
      font-size: 22px;
      line-height: 1;
      font-family: "Segoe UI Symbol", "Apple Symbols", "Noto Sans Symbols 2", "Noto Sans Symbols", sans-serif;
    }
    button.disarm {
      background: #C1DBE8;
      border-color: #43302E;
      color: #43302E;
    }
    .action-row {
      margin-top: 2px;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
      width: 100%;
    }
    .action-row form { margin: 0; }
    .action-row button {
      min-width: 200px;
      max-width: 260px;
    }
    .action-row .buy-btn {
      background: #8fc9a1;
      color: #2f3d31;
      border-color: #7fb891;
    }
    .action-row .buy-btn:hover {
      background: #7dbb90;
      border-color: #6ea885;
    }
    .action-row .stop-btn {
      background: #e6a3a3;
      color: #4a2f2f;
      border-color: #d59292;
    }
    .action-row .stop-btn:hover {
      background: #d98f8f;
      border-color: #c97d7d;
    }
    .countdown-box {
      margin-top: 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(255, 241, 181, .72);
      text-align: center;
    }
    .countdown-value {
      font-size: 24px;
      font-weight: 800;
      letter-spacing: .8px;
      margin: 4px 0;
    }
    .help-line {
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
    }
    .metric-box {
      margin: 8px 0 10px;
      border: 1px solid rgba(67, 48, 46, .3);
      border-radius: 10px;
      background: rgba(255, 241, 181, .7);
      padding: 8px 10px;
    }
    .metric-title {
      font-size: 12px;
      font-weight: 700;
      color: var(--muted);
      margin: 0 0 4px;
    }
    .metric-line {
      font-size: 13px;
      font-weight: 600;
      line-height: 1.45;
      margin: 0;
    }
    .mini-form {
      display: flex;
      gap: 8px;
      align-items: center;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }
    .mini-form select {
      min-width: 190px;
    }
    .latency-single {
      margin-top: 8px;
    }
    .latency-controls {
      display: flex;
      align-items: stretch;
      gap: 8px;
      margin-bottom: 8px;
      flex-wrap: nowrap;
      overflow-x: auto;
      padding-bottom: 2px;
    }
    .latency-chip {
      min-height: 34px;
      padding: 6px 12px;
      border-radius: 999px;
      border: 1px solid rgba(67, 48, 46, .35);
      background: #f2e6d8;
      color: var(--text);
      font-size: 13px;
      font-weight: 700;
      width: auto;
      min-width: auto;
      white-space: nowrap;
      cursor: pointer;
    }
    .latency-chip.active {
      box-shadow: inset 0 0 0 2px rgba(67, 48, 46, .35);
    }
    .latency-chip.ex-gate { background: #f2e6d8; }
    .latency-chip.ex-mexc { background: #e2ecf4; }
    .latency-chip.ex-kucoin { background: #ebe3f2; }
    .latency-chip.ex-bitget { background: #efe3d7; }
    .latency-chip.ex-binance { background: #ddebe7; }
    .latency-line {
      font-size: 12px;
      line-height: 1.35;
      color: var(--text);
      margin: 2px 0;
    }
    .latency-detail {
      border: 1px solid rgba(67, 48, 46, .2);
      border-radius: 10px;
      background: #f2e6d8;
      padding: 9px 10px;
      display: block;
    }
    .latency-detail.ex-gate { background: #f2e6d8; }
    .latency-detail.ex-mexc { background: #e2ecf4; }
    .latency-detail.ex-kucoin { background: #ebe3f2; }
    .latency-detail.ex-bitget { background: #efe3d7; }
    .latency-detail.ex-binance { background: #ddebe7; }
    .ok-t { color: #2f6a2f; font-weight: 700; }
    .tabs {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin: 0 auto 10px;
      max-width: 1160px;
    }
    .tab-btn {
      border: 1px solid #43302E;
      background: rgba(193, 219, 232, .45);
      color: #43302E;
      border-radius: 999px;
      padding: 6px 12px;
      cursor: pointer;
      font-size: 13px;
      width: auto;
      min-width: auto;
      min-height: 34px;
      line-height: 1;
      display: inline-flex;
      align-items: center;
      white-space: nowrap;
    }
    .tab-btn.active {
      border-color: #43302E;
      background: rgba(67, 48, 46, .16);
    }
    .panel {
      display: none;
      max-width: 1160px;
      margin: 0 auto;
    }
    .panel.active { display: block; }
    .check-line {
      margin: 2px 0 8px;
      font-size: 13px;
      font-weight: 700;
    }
    .check-ok { color: #2f6a2f; }
    .check-bad { color: #7f2f2f; }
    table {
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 14px;
      border: 1px solid #43302E;
      background: rgba(255, 241, 181, .58);
    }
    th, td {
      padding: 10px;
      border-bottom: 1px solid rgba(67, 48, 46, .18);
      text-align: left;
      vertical-align: top;
      font-size: 13px;
    }
    th {
      color: #43302E;
      font-size: 12px;
      letter-spacing: .2px;
      background: rgba(193, 219, 232, .65);
    }
    a {
      color: #43302E;
      text-decoration: none;
      word-break: break-all;
    }
    a:hover { text-decoration: underline; }
    .pair-btn {
      border: 1px solid var(--line);
      background: rgba(255, 241, 181, .9);
      color: var(--text);
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      cursor: pointer;
      font-weight: 700;
      min-width: 92px;
    }
    .pair-btn:hover {
      background: #fff6d4;
    }
    .empty {
      padding: 12px;
      border: 1px dashed #43302E;
      border-radius: 12px;
      color: var(--muted);
      background: rgba(193, 219, 232, .35);
      font-size: 13px;
    }
    @media (max-width: 1200px) {
      .top { grid-template-columns: 1fr; }
    }
    @media (max-width: 1020px) {
      .top { grid-template-columns: 1fr; }
      .status-grid { grid-template-columns: 1fr; }
      .form-grid { grid-template-columns: 1fr; }
      .status-label { font-size: 12px; }
      .status-value { font-size: 11px; }
      .status-note { transform: none !important; }
    }
    @media (max-width: 760px) {
      .latency-controls { gap: 6px; }
      .trade-head { flex-direction: column; align-items: stretch; }
      .action-row { flex-direction: column; }
      .action-row button { max-width: none; min-width: 0; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="page-title-row">
      <h1 class="page-title">Yeni Listeleme İşlem Botu</h1>
    </div>
    <div class="top">
      <section class="card">
        <div class="status-indicator-row">
          <span class="system-state {{ 'online' if exec_state['online'] else 'offline' }}">
            <span
              class="system-dot {{ 'online' if exec_state['online'] else 'offline' }}"
              title="{{ 'Çevrimiçi' if exec_state['online'] else 'Çevrimdışı' }}"
            ></span>
            <span>{{ 'live' if exec_state['online'] else 'off' }}</span>
          </span>
        </div>
        <div class="status-grid">
          <div class="status-note">
            <div class="status-inner">
              <div class="status-label">Son Kontrol:</div>
              <div class="status-value">
                {% if last_poll_parts %}
                  <span class="iso-text clock-date">{{last_poll_parts["date"]}}</span>
                  <span class="iso-text clock-time">{{last_poll_parts["time"]}} (TSI)</span>
                {% else %}
                  <span class="iso-text">henüz yok</span>
                {% endif %}
              </div>
            </div>
          </div>
          <div class="status-note">
            <div class="status-inner">
              <div class="status-label">Bekleyen İşlem:</div>
              <div class="status-value">{{ "var" if exec_state["gate"].get("armed") else "yok" }}</div>
            </div>
          </div>
        </div>
        <div class="latency-single">
          {% set latency_selected = last_order.get("exchange") if last_order.get("exchange") in exchanges else exchanges[0] %}
          {% set latency_selected_row = order_latency.get(latency_selected) or {} %}
          <div class="latency-controls">
            {% for ex in exchanges %}
              {% set row = order_latency.get(ex) or {} %}
              <button
                type="button"
                class="latency-chip ex-{{ex}} {% if ex == latency_selected %}active{% endif %}"
                data-latency-ex="{{ex}}"
                data-ms="{{row.get('engine_latency_text') or '-'}}"
              >{{ex}}</button>
            {% endfor %}
          </div>
          <div class="latency-detail ex-{{latency_selected}}" id="latency-detail">
            <div class="latency-line"><b>Süre:</b> <span id="latency-ms-view">{{latency_selected_row.get("engine_latency_text") or "-"}}</span></div>
          </div>
          <form id="probe-form" method="POST" action="/probe-latency" style="display:none">
            <input type="hidden" name="probe_exchange" id="probe-exchange" value="{{latency_selected}}">
            <input type="hidden" name="probe_symbol" id="probe-symbol" value="">
            <input type="hidden" name="probe_spend_usdt" id="probe-spend" value="{{last_order.get('spend_usdt','5')}}">
          </form>
        </div>
      </section>

      <section class="card trade-card">
        <div class="trade-head">
          <a class="link-btn icon-only" href="/admin" aria-label="Admin Ayarları" title="Admin Ayarları">&#9881;</a>
        </div>
        <form method="POST" action="/arm" class="form-grid" id="arm-form">
          <div class="field">
            <label>Borsa</label>
            <select name="exchange" id="exchange-select">
              {% for ex in exchanges %}
                <option value="{{ex}}" {% if last_order.get("exchange") == ex %}selected{% endif %}>{{ex}}</option>
              {% endfor %}
            </select>
          </div>
          <div class="field">
            <label>Parite</label>
            <input id="symbol-input" name="symbol" placeholder="ABC_USDT / ABCUSDT / ABC-USDT" value="{{last_order.get('symbol','')}}" required>
          </div>
          <div class="field">
            <label>USDT Tutarı</label>
            <input id="spend-input" name="spend_usdt" type="number" min="0.01" step="0.01" value="{{last_order.get('spend_usdt','5')}}" required>
          </div>
        </form>
        <div class="action-row">
          <button type="submit" class="buy-btn" form="arm-form">Alış</button>
          <form method="POST" action="/stop-buy">
            <button type="submit" class="stop-btn">Satış</button>
          </form>
        </div>
        {% if countdown %}
          <div class="countdown-box" data-target="{{countdown['target_iso']}}" id="countdown-box">
            <div><b>{{countdown["exchange"]}} / {{countdown["symbol"]}}</b> için kalan süre</div>
            <div class="countdown-value" id="countdown-value">--:--:--</div>
            <div class="muted">{{countdown["target_iso"]}}</div>
          </div>
        {% endif %}
      </section>
    </div>

    <div class="tabs">
      {% for ex in exchanges %}
        <button class="tab-btn {% if loop.first %}active{% endif %}" data-tab="{{ex}}">
          {{ex}}
          {% if errors.get(ex) %}<span class="pill err" title="{{errors.get(ex)[:120]}}">ERR</span>{% endif %}
        </button>
      {% endfor %}
    </div>

    {% for ex in exchanges %}
      <section class="panel {% if loop.first %}active{% endif %}" id="panel-{{ex}}">
        <div class="check-line">
          {% if checks.get(ex) is sameas true %}
            <span class="check-ok">✓ Liste kontrol edildi</span>
          {% elif checks.get(ex) is sameas false %}
            <span class="check-bad">✕ Liste kontrol edilemedi</span>
          {% else %}
            <span class="muted">Kontrol bekleniyor...</span>
          {% endif %}
        </div>
        {% if listings[ex] %}
          <table>
            <thead>
              <tr>
                <th>Başlık</th>
                <th>Detected</th>
                <th>Trade Başlangıç</th>
                <th>Parite</th>
                <th>Link</th>
              </tr>
            </thead>
            <tbody>
              {% for item in listings[ex] %}
                <tr>
                  <td>
                    <b>{{item["title"]}}</b>
                    {% if item["is_new"] %}<span class="pill new">NEW</span>{% endif %}
                  </td>
                  <td>
                    {% if item.get("detected_parts") %}
                      <span class="iso-text clock-date">{{item["detected_parts"]["date"]}}</span>
                      <span class="iso-text clock-time">{{item["detected_parts"]["time"]}} (TSI)</span>
                    {% else %}
                      <span class="iso-text">-</span>
                    {% endif %}
                  </td>
                  <td>
                    <div><b>{{item["trade_start_display"]}}</b></div>
                    {% if not item.get("trade_start_ok") %}
                      <div class="muted">Duyuruda net trade başlangıç saati bulunamadı.</div>
                    {% endif %}
                  </td>
                  <td>
                    {% if item.get("pair_guess") %}
                      <button class="pair-btn" type="button" data-pair="{{item['pair_guess']}}" data-exchange="{{ex}}">{{item["pair_guess"]}}</button>
                    {% else %}
                      <span class="muted">-</span>
                    {% endif %}
                  </td>
                  <td><a href="{{item["url"]}}" target="_blank" rel="noopener">open</a></td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        {% else %}
          <div class="empty">
            {% if errors.get(ex) %}
              Bu borsa için veri alınamadı.
              {% if friendly_errors.get(ex) %} {{friendly_errors.get(ex)}}{% endif %}
            {% else %}
              Bu borsa için şu an yeni listing bulunamadı.
            {% endif %}
          </div>
        {% endif %}
      </section>
    {% endfor %}
  </div>

  <script>
    const tabs = document.querySelectorAll('.tab-btn');
    const panels = document.querySelectorAll('.panel');
    const exchangeSelect = document.getElementById('exchange-select');
    const symbolInput = document.getElementById('symbol-input');
    const spendInput = document.getElementById('spend-input');
    const probeForm = document.getElementById('probe-form');
    const probeExchangeInput = document.getElementById('probe-exchange');
    const probeSymbolInput = document.getElementById('probe-symbol');
    const probeSpendInput = document.getElementById('probe-spend');
    const latencyDetail = document.getElementById('latency-detail');
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

    const pairButtons = document.querySelectorAll('.pair-btn');
    pairButtons.forEach((btn) => {
      btn.addEventListener('click', () => {
        const pair = btn.getAttribute('data-pair') || '';
        const ex = btn.getAttribute('data-exchange') || '';
        if (exchangeSelect && ex) exchangeSelect.value = ex;
        if (symbolInput && pair) {
          symbolInput.value = pair;
          symbolInput.focus();
        }
        const topCard = document.querySelector('.trade-card');
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
      if (latencyDetail) {
        latencyDetail.classList.remove('ex-gate', 'ex-mexc', 'ex-kucoin', 'ex-bitget', 'ex-binance');
        if (ex) latencyDetail.classList.add('ex-' + ex);
      }
    };

    latencyChips.forEach((chip) => {
      chip.addEventListener('click', () => {
        setActiveLatencyChip(chip);
        if (probeSymbolInput) probeSymbolInput.value = '';
        if (probeSpendInput) {
          const spend = (spendInput && spendInput.value) ? spendInput.value : (probeSpendInput.value || '5');
          probeSpendInput.value = spend;
        }
        if (probeForm) {
          if (typeof probeForm.requestSubmit === 'function') probeForm.requestSubmit();
          else probeForm.submit();
        }
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
        countdownValue.textContent = days > 0 ? `${days}g ${hh}:${mm}:${ss}` : `${hh}:${mm}:${ss}`;
      };
      tick();
      setInterval(tick, 1000);
    }

  </script>
</body>
</html>"""

ADMIN_HTML = """<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin Ayarları</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Lexend:wght@300;400;500;600;700;800&display=swap');
    :root {
      --bg: #DDD7CE;
      --panel: #f5f1ea;
      --card: #ffffff;
      --line: #5b534a;
      --text: #2e2925;
      --muted: rgba(46, 41, 37, .72);
      --ok: #2f6a2f;
      --err: #8a2f2f;
      --btn: #43302E;
      --btn-text: #f7efe4;
      --chip: #e7ddd2;
    }
    * { box-sizing: border-box; font-family: "Lexend", sans-serif; }
    body { margin: 0; background: var(--bg); color: var(--text); }
    .wrap { max-width: 1260px; margin: 0 auto; padding: 18px; }
    .top-card {
      background: var(--panel);
      border: 1px solid rgba(91, 83, 74, .3);
      border-radius: 16px;
      padding: 14px 16px;
      margin-bottom: 14px;
      box-shadow: 0 14px 26px -22px rgba(67, 48, 46, .35);
    }
    h1 { margin: 0; font-size: 22px; }
    .sub { margin: 5px 0 0; color: var(--muted); font-size: 13px; }
    .field { display: flex; flex-direction: column; gap: 4px; }
    .field label { font-size: 12px; font-weight: 700; color: var(--muted); }
    input, select, button {
      border: 1px solid rgba(91, 83, 74, .45);
      border-radius: 10px;
      background: #fff;
      color: var(--text);
      padding: 8px 10px;
      font-size: 13px;
      outline: none;
    }
    input:focus, select:focus { border-color: #43302E; box-shadow: 0 0 0 2px rgba(67,48,46,.09); }
    .msg { padding: 10px 12px; border-radius: 10px; margin-top: 10px; font-size: 13px; }
    .msg.ok { background: rgba(47,106,47,.12); border: 1px solid rgba(47,106,47,.34); color: var(--ok); }
    .msg.err { background: rgba(127,47,47,.12); border: 1px solid rgba(127,47,47,.34); color: var(--err); }
    .grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .ex-card {
      background: var(--card);
      border: 1px solid rgba(91, 83, 74, .35);
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 14px 24px -20px rgba(67, 48, 46, .35);
    }
    .ex-card h3 {
      margin: 0 0 8px;
      font-size: 15px;
      letter-spacing: .1px;
    }
    .ex-fields {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    .full { grid-column: 1 / -1; }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      margin-left: 6px;
      background: var(--chip);
      border: 1px solid rgba(91, 83, 74, .22);
      color: #5f564d;
      border-radius: 999px;
      padding: 1px 7px;
      font-size: 11px;
      font-weight: 700;
      vertical-align: middle;
    }
    .sticky-actions {
      position: sticky;
      bottom: 10px;
      margin-top: 14px;
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      background: rgba(221, 215, 206, .86);
      backdrop-filter: blur(2px);
      border-radius: 12px;
      padding: 8px;
    }
    .link-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 1px solid rgba(91, 83, 74, .45);
      background: #f9f5ee;
      color: var(--text);
      border-radius: 10px;
      padding: 10px 13px;
      font-size: 14px;
      font-weight: 700;
      text-decoration: none;
    }
    button {
      cursor: pointer;
      font-weight: 700;
      background: var(--btn);
      color: var(--btn-text);
      border-color: var(--btn);
      min-width: 210px;
    }
    @media (max-width: 1100px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 760px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <form method="POST" action="/admin">
      <section class="top-card">
        <h1>Admin Ayarları</h1>
        <p class="sub">Borsa API bilgilerini kartlardan düzenle. Gizli alanı boş bırakırsan mevcut değer korunur.</p>
        {% if msg %}
          <div class="msg {{ 'ok' if ok else 'err' }}">{{msg}}</div>
        {% endif %}
      </section>

      <section class="grid">
        <article class="ex-card">
          <h3>Gate.io</h3>
          <div class="ex-fields">
            <div class="field">
              <label>API Key {% if flags["GATE_KEY"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="GATE_KEY" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>API Secret {% if flags["GATE_SECRET"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="GATE_SECRET" placeholder="değiştirmek için yaz">
            </div>
            <div class="field full">
              <label>Base URL</label>
              <input name="GATE_BASE" value="{{cfg['GATE_BASE']}}">
            </div>
          </div>
        </article>

        <article class="ex-card">
          <h3>Binance</h3>
          <div class="ex-fields">
            <div class="field">
              <label>API Key {% if flags["BINANCE_KEY"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="BINANCE_KEY" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>API Secret {% if flags["BINANCE_SECRET"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="BINANCE_SECRET" placeholder="değiştirmek için yaz">
            </div>
            <div class="field full">
              <label>Base URL</label>
              <input name="BINANCE_BASE" value="{{cfg['BINANCE_BASE']}}">
            </div>
          </div>
        </article>

        <article class="ex-card">
          <h3>MEXC</h3>
          <div class="ex-fields">
            <div class="field">
              <label>API Key {% if flags["MEXC_KEY"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="MEXC_KEY" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>API Secret {% if flags["MEXC_SECRET"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="MEXC_SECRET" placeholder="değiştirmek için yaz">
            </div>
            <div class="field full">
              <label>Base URL</label>
              <input name="MEXC_BASE" value="{{cfg['MEXC_BASE']}}">
            </div>
          </div>
        </article>

        <article class="ex-card">
          <h3>KuCoin</h3>
          <div class="ex-fields">
            <div class="field">
              <label>API Key {% if flags["KUCOIN_KEY"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="KUCOIN_KEY" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>API Secret {% if flags["KUCOIN_SECRET"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="KUCOIN_SECRET" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>Passphrase {% if flags["KUCOIN_PASSPHRASE"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="KUCOIN_PASSPHRASE" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>Base URL</label>
              <input name="KUCOIN_BASE" value="{{cfg['KUCOIN_BASE']}}">
            </div>
          </div>
        </article>

        <article class="ex-card">
          <h3>Bitget</h3>
          <div class="ex-fields">
            <div class="field">
              <label>API Key {% if flags["BITGET_KEY"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="BITGET_KEY" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>API Secret {% if flags["BITGET_SECRET"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="BITGET_SECRET" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>Passphrase {% if flags["BITGET_PASSPHRASE"] %}<span class="chip">kayıtlı</span>{% endif %}</label>
              <input type="password" name="BITGET_PASSPHRASE" placeholder="değiştirmek için yaz">
            </div>
            <div class="field">
              <label>Base URL</label>
              <input name="BITGET_BASE" value="{{cfg['BITGET_BASE']}}">
            </div>
          </div>
        </article>
      </section>

      <div class="sticky-actions">
        <a class="link-btn" href="/">Panele Dön</a>
        <button type="submit">Ayarları Kaydet ve Uygula</button>
      </div>
    </form>
  </div>
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
    return f"{ms} ms ({sec:.3f} sn)"


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
        return value or "henüz yok"
    dt_tr = dt.astimezone(TR_TZ) if TR_TZ else dt
    return dt_tr.strftime("%d.%m.%Y %H:%M:%S (TSI)")


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
        return "BELIRSIZ"
    dt_tr = dt.astimezone(TR_TZ) if TR_TZ else dt
    return dt_tr.strftime("%Y-%m-%d %H:%M:%S (TSI)")


def humanize_error(err: str):
    if not err:
        return ""
    m = re.search(r"HTTP\s+(\d{3})", err)
    if m:
        code = m.group(1)
        if code == "403":
            return "Erişim engellendi (HTTP 403)."
        if code == "404":
            return "Kaynak bulunamadı (HTTP 404)."
        if code == "429":
            return "İstek limiti aşıldı (HTTP 429)."
        if code.startswith("5"):
            return f"Borsa sunucu hatası (HTTP {code})."
        return f"HTTP hatası ({code})."
    if "Timeout" in err or "ReadTimeout" in err or "ConnectTimeout" in err:
        return "Zaman aşımı: borsa yanıt vermedi."
    if "ConnectionError" in err:
        return "Bağlantı hatası: borsaya ulaşılamadı."
    return "Veri alınırken hata oluştu."


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
            item["trade_start_ok"] = disp != "BELIRSIZ"
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
        set_action(False, "Geçersiz borsa seçimi.")
        return redirect(url_for("index"))
    if not symbol:
        set_action(False, "Symbol/pair boş bırakılamaz.")
        return redirect(url_for("index"))
    if not norm_symbol:
        set_action(False, "Symbol/pair formatı çözülemedi.")
        return redirect(url_for("index"))

    try:
        out = executor_post("/arm", {"exchange": ex, "symbol": norm_symbol, "spend_usdt": spend})
        mode = out.get("mode")
        countdown = find_countdown_target(ex, norm_symbol, app.config.get("LISTINGS") or {})
        STATE["countdown"] = countdown
        if ex == "gate" or mode == "ws_trigger":
            base_msg = f"{ex} için işlem hazırlandı: {norm_symbol}."
        else:
            base_msg = f"{ex} için alım isteği gönderildi ({norm_symbol})."
        if countdown:
            set_action(True, base_msg + " Geri sayım başlatıldı.")
        else:
            set_action(True, base_msg + " Bu parite için ileri tarihli listing saati bulunamadı.")
    except Exception as e:
        STATE["countdown"] = None
        set_action(False, f"Başlat işlemi başarısız: {e}")
    return redirect(url_for("index"))


@app.route("/stop-buy", methods=["POST"])
@app.route("/disarm", methods=["POST"])
def stop_buy():
    try:
        out = executor_post("/kill", {})
        STATE["countdown"] = None
        forced = out.get("forced_buy") if isinstance(out, dict) else None
        if isinstance(forced, dict) and forced.get("error"):
            set_action(False, f"Alım durduruldu, zorunlu alım başarısız: {forced['error']}")
        elif forced is not None:
            set_action(True, "Alım durduruldu. Aktif süreç için zorunlu market alım gönderildi.")
        else:
            set_action(True, "Alım durduruldu.")
    except Exception as e:
        set_action(False, f"Alımı durdurma işlemi başarısız: {e}")
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
        set_action(True, f"Borsa iletişim testi tamamlandı: {ok_count}/{total} başarılı.")
    except Exception as e:
        set_action(False, f"Borsa iletişim testi başarısız: {e}")
    return redirect(url_for("index"))


@app.route("/probe-latency", methods=["POST"])
def probe_latency():
    ex = request.form.get("probe_exchange", "").strip().lower()
    symbol = request.form.get("probe_symbol", "").strip()
    spend = request.form.get("probe_spend_usdt", "5").strip()
    if ex not in agg.exchanges:
        set_action(False, "Geçersiz borsa seçimi.")
        return redirect(url_for("index"))
    norm_symbol = normalize_symbol_for_exchange(ex, symbol) if symbol else ""
    if symbol and not norm_symbol:
        set_action(False, "Parite formatı çözülemedi.")
        return redirect(url_for("index"))
    try:
        out = executor_post("/order-latency", {"exchange": ex, "symbol": norm_symbol, "spend_usdt": spend})
        probe = out.get("probe") if isinstance(out, dict) else {}
        ms = probe.get("engine_latency_ms") if isinstance(probe, dict) else None
        txt = format_latency_text(ms)
        sent_symbol = probe.get("symbol_sent") if isinstance(probe, dict) else "-"
        auto_symbol = bool(probe.get("auto_symbol")) if isinstance(probe, dict) else False
        symbol_note = f"{sent_symbol} (otomatik)" if auto_symbol else sent_symbol
        if txt != "-":
            set_action(True, f"{ex} emir hattı ölçümü: {txt} ({symbol_note}).")
        else:
            set_action(True, f"{ex} emir hattı ölçümü tamamlandı ({symbol_note}).")
    except Exception as e:
        set_action(False, f"Emir hattı ölçümü başarısız: {e}")
    return redirect(url_for("index"))


@app.route("/set-dry-run", methods=["POST"])
def set_dry_run():
    raw = request.form.get("dry_run_mode", "1").strip()
    enabled = raw == "1"
    try:
        out = executor_post("/dry-run", {"enabled": enabled})
        if out.get("dry_run", enabled):
            set_action(True, "Test modu açıldı. Gerçek emir gönderilmez.")
        else:
            set_action(True, "Gerçek mod açıldı. Emirler borsaya gönderilir.")
    except Exception as e:
        set_action(False, f"Mod güncellenemedi: {e}")
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
                msg = "Ayarlar kaydedildi ve canlı olarak uygulandı."
            except Exception as e:
                ok = False
                msg = f"Ayarlar kaydedildi fakat executor'a uygulanamadı: {e}"
        except Exception as e:
            ok = False
            msg = f"Ayarlar kaydedilemedi: {e}"

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
