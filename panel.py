from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone

from flask import Flask, render_template_string

from exchanges import build_exchanges
from parsing import extract_time_info, html_to_text


PORT = 5177
POLL_INTERVAL_SECONDS = 30 * 60
NEW_WINDOW = timedelta(hours=24)
UNKNOWN = "UNKNOWN"

app = Flask(__name__)
exchanges = build_exchanges()
exchange_names = [ex.name for ex in exchanges]
exchange_labels = {
    "gate": "Gate",
    "mexc": "MEXC",
    "kucoin": "KuCoin",
    "bitget": "Bitget",
    "binance": "Binance",
}

state_lock = threading.Lock()
state = {
    "items_by_url": {},  # url -> {"item": ListingItem, "first_seen_at": datetime}
    "exchange_status": {
        name: {
            "error": None,
            "last_fetch_at": None,
        }
        for name in exchange_names
    },
}

poller_lock = threading.Lock()
poller_thread: threading.Thread | None = None


TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="120">
  <title>New Listings Aggregator</title>
  <style>
    :root {
      --bg: #0f1220;
      --panel: #171b2f;
      --muted: #a2a8c3;
      --text: #edf0ff;
      --border: #2a335a;
      --new: #1ea763;
      --err: #d64545;
      --accent: #4da1ff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(1200px 700px at 20% -20%, #1f2b55 0%, var(--bg) 45%);
      color: var(--text);
      font-family: "Segoe UI", -apple-system, BlinkMacSystemFont, sans-serif;
      padding: 20px;
    }
    .wrap {
      max-width: 1200px;
      margin: 0 auto;
    }
    .head {
      margin-bottom: 14px;
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 10px;
      flex-wrap: wrap;
    }
    h1 {
      margin: 0;
      font-size: 22px;
      letter-spacing: .2px;
    }
    .sub {
      color: var(--muted);
      font-size: 13px;
    }
    .tabs {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    .tab-btn {
      border: 1px solid var(--border);
      background: #0f1631;
      color: var(--text);
      padding: 8px 12px;
      border-radius: 999px;
      cursor: pointer;
      font-size: 14px;
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .tab-btn.active {
      border-color: var(--accent);
      background: #122044;
    }
    .badge {
      font-size: 11px;
      font-weight: 700;
      border-radius: 999px;
      padding: 2px 7px;
      line-height: 1.4;
      display: inline-block;
    }
    .badge.new {
      background: rgba(30, 167, 99, 0.2);
      color: #84ffbd;
      border: 1px solid rgba(30, 167, 99, 0.5);
    }
    .badge.err {
      background: rgba(214, 69, 69, 0.2);
      color: #ffb4b4;
      border: 1px solid rgba(214, 69, 69, 0.5);
    }
    .panel {
      display: none;
      background: rgba(23, 27, 47, 0.95);
      border: 1px solid var(--border);
      border-radius: 12px;
      overflow: hidden;
    }
    .panel.active {
      display: block;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      text-align: left;
      border-bottom: 1px solid rgba(255, 255, 255, 0.08);
      padding: 10px;
      vertical-align: top;
      font-size: 14px;
    }
    th {
      color: #b4bfeb;
      font-weight: 600;
      background: rgba(0, 0, 0, 0.15);
    }
    a {
      color: #9fcbff;
      text-decoration: none;
    }
    a:hover {
      text-decoration: underline;
    }
    .time-top {
      font-weight: 600;
      color: #eaf2ff;
    }
    .time-bottom {
      color: var(--muted);
      margin-top: 2px;
      font-size: 12px;
    }
    .empty {
      padding: 14px;
      color: var(--muted);
      font-size: 14px;
    }
    .status-cell {
      white-space: nowrap;
      min-width: 90px;
    }
    @media (max-width: 900px) {
      table, thead, tbody, th, td, tr { display: block; }
      thead { display: none; }
      tr { border-bottom: 1px solid rgba(255, 255, 255, 0.12); }
      td { border-bottom: none; padding: 8px 10px; }
      td::before {
        content: attr(data-label);
        display: block;
        color: #91a3df;
        font-size: 12px;
        margin-bottom: 2px;
      }
      .status-cell { min-width: 0; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <h1>New Listings Aggregator</h1>
      <div class="sub">Polling: every 30 minutes | Auto refresh: 120s | Port: 5177</div>
    </div>

    <div class="tabs" id="tabs">
      {% for name in exchange_names %}
      <button class="tab-btn {% if loop.first %}active{% endif %}" data-tab="{{ name }}">
        {{ exchange_labels[name] }}
        {% if view[name]["has_error"] %}
          <span class="badge err" title="{{ view[name]["error_short"] }}">ERR</span>
        {% endif %}
      </button>
      {% endfor %}
    </div>

    {% for name in exchange_names %}
    <section id="panel-{{ name }}" class="panel {% if loop.first %}active{% endif %}">
      {% if view[name]["items"] %}
      <table>
        <thead>
          <tr>
            <th>Title / URL</th>
            <th>Detected (UTC)</th>
            <th>Time</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {% for item in view[name]["items"] %}
          <tr>
            <td data-label="Title">
              <a href="{{ item.url }}" target="_blank" rel="noopener noreferrer">{{ item.title }}</a>
              <div class="time-bottom">{{ item.url }}</div>
            </td>
            <td data-label="Detected">{{ item.detected_at }}</td>
            <td data-label="Time">
              <div class="time-top">{{ item.normalized_tr_time }}</div>
              <div class="time-bottom">{{ item.raw_time_text }}</div>
            </td>
            <td data-label="Status" class="status-cell">
              {% if item.is_new %}<span class="badge new">NEW</span>{% endif %}
              {% if item.error %}<span class="badge err" title="{{ item.error[:120] }}">ERR</span>{% endif %}
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <div class="empty">No items yet for {{ exchange_labels[name] }}.</div>
      {% endif %}
    </section>
    {% endfor %}
  </div>

  <script>
    const buttons = document.querySelectorAll('.tab-btn');
    const panels = document.querySelectorAll('.panel');

    buttons.forEach((btn) => {
      btn.addEventListener('click', () => {
        const tab = btn.getAttribute('data-tab');

        buttons.forEach((b) => b.classList.remove('active'));
        panels.forEach((p) => p.classList.remove('active'));

        btn.classList.add('active');
        const panel = document.getElementById(`panel-${tab}`);
        if (panel) panel.classList.add('active');
      });
    });
  </script>
</body>
</html>
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: datetime | None = None) -> str:
    value = dt or utc_now()
    return value.isoformat(timespec="seconds")


def start_background_poller() -> None:
    global poller_thread
    with poller_lock:
        if poller_thread and poller_thread.is_alive():
            return
        poller_thread = threading.Thread(target=poll_loop, daemon=True, name="listing-poller")
        poller_thread.start()


def poll_loop() -> None:
    while True:
        for exchange in exchanges:
            poll_exchange(exchange)
        time.sleep(POLL_INTERVAL_SECONDS)


def poll_exchange(exchange) -> None:
    now = utc_now()
    try:
        listings = exchange.fetch_listings()
    except Exception as exc:  # noqa: BLE001
        with state_lock:
            status = state["exchange_status"][exchange.name]
            status["error"] = str(exc)
            status["last_fetch_at"] = utc_iso(now)
        return

    with state_lock:
        status = state["exchange_status"][exchange.name]
        status["error"] = None
        status["last_fetch_at"] = utc_iso(now)

    new_urls: list[str] = []
    with state_lock:
        items_by_url = state["items_by_url"]
        for item in listings:
            url = item.get("url")
            if not url:
                continue
            if url in items_by_url:
                continue
            new_item = dict(item)
            first_seen_at = utc_now()
            new_item["detected_at"] = utc_iso(first_seen_at)
            new_item["is_new"] = True
            new_item["raw_time_text"] = new_item.get("raw_time_text") or UNKNOWN
            new_item["normalized_tr_time"] = new_item.get("normalized_tr_time") or UNKNOWN
            new_item["error"] = None
            items_by_url[url] = {"item": new_item, "first_seen_at": first_seen_at}
            new_urls.append(url)

    for url in new_urls:
        raw_time_text = UNKNOWN
        normalized_tr_time = UNKNOWN
        err: str | None = None

        try:
            detail_html = exchange.fetch_detail_text(url)
            detail_text = html_to_text(detail_html)
            raw_time_text, normalized_tr_time = extract_time_info(exchange.name, detail_text)
            if raw_time_text == UNKNOWN:
                raw_time_text = _fallback_raw_time(detail_text)
        except Exception as exc:  # noqa: BLE001
            err = f"detail fetch failed: {exc}"

        with state_lock:
            stored = state["items_by_url"].get(url)
            if not stored:
                continue
            stored["item"]["raw_time_text"] = raw_time_text
            stored["item"]["normalized_tr_time"] = normalized_tr_time
            stored["item"]["error"] = err


def _fallback_raw_time(text: str) -> str:
    if not text:
        return UNKNOWN
    chunk = " ".join(text.split()[:20]).strip()
    return chunk[:120] if chunk else UNKNOWN


def build_view_model() -> dict:
    now = utc_now()
    view = {
        name: {
            "items": [],
            "error": None,
            "error_short": "",
            "has_error": False,
            "last_fetch_at": None,
        }
        for name in exchange_names
    }

    with state_lock:
        for name in exchange_names:
            status = state["exchange_status"].get(name, {})
            error = status.get("error")
            view[name]["error"] = error
            view[name]["error_short"] = (error or "")[:120]
            view[name]["has_error"] = bool(error)
            view[name]["last_fetch_at"] = status.get("last_fetch_at")

        grouped: dict[str, list[tuple[datetime, dict]]] = {name: [] for name in exchange_names}
        for record in state["items_by_url"].values():
            item = dict(record["item"])
            first_seen = record["first_seen_at"]
            item["is_new"] = (now - first_seen) < NEW_WINDOW
            grouped[item["exchange"]].append((first_seen, item))

    for name in exchange_names:
        ordered = sorted(grouped[name], key=lambda pair: pair[0], reverse=True)
        view[name]["items"] = [item for _, item in ordered]

    return view


@app.route("/")
def index():
    start_background_poller()
    view = build_view_model()
    return render_template_string(
        TEMPLATE,
        exchange_names=exchange_names,
        exchange_labels=exchange_labels,
        view=view,
    )


if __name__ == "__main__":
    start_background_poller()
    app.run(host="0.0.0.0", port=PORT, debug=False)
