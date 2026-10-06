"""
Weather Monitoring Modular System

A real-time IoT weather monitoring dashboard built with Dash. This application:

- Retrieves sensor readings from TimescaleDB (or other configured DB via config.get_db())
- Computes derived meteorological metrics (heat index, dew point, absolute humidity)
- Calculates an Air Quality Index (AQI) from pollutant readings
- Tracks node health/status and raises alerts based on configured thresholds
- Provides authentication (student logins) via auth.py backed by the same database
- Periodically refreshes and caches data to reduce database load

Environment variables:
- DB_CHOICE: Database backend to use (default: "timescale")
- LOOKBACK_HOURS: How far back to query historical readings (default: 24)
- OFFLINE_AFTER_S: Seconds without updates before a node is considered offline (default: 60)
- REFRESH_MS: Dashboard auto-refresh interval in milliseconds (default: 5000)
- CACHE_TTL_S: Cache time-to-live for DB queries in seconds (default: 2)
- PORT: Dash server port (default: 5000)
"""

import math
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime

import dash
import plotly.graph_objects as go
from dash import ALL, Input, Output, State, ctx, dcc, html

# Student logins live in the same database; auth.py manages the users table.
import auth

# Database access is provided by config.py (returns the configured DB adapter).
from config import get_db


APP_TITLE = "Weather Monitoring Modular System"
DB_CHOICE = os.environ.get("DB_CHOICE", "timescale")
LOOKBACK_HOURS = float(os.environ.get("LOOKBACK_HOURS", "24"))
OFFLINE_AFTER_S = float(os.environ.get("OFFLINE_AFTER_S", "60"))
REFRESH_MS = int(os.environ.get("REFRESH_MS", "5000"))
CACHE_TTL_S = float(os.environ.get("CACHE_TTL_S", "2"))
PORT = int(os.environ.get("PORT", "5000"))
SIGNED_OUT_TEXT = "Sign in to view the dashboard."

# Primary derived metrics displayed on the dashboard (labels, units, icons, descriptions).
PRIMARY_CALCULATIONS = {
    "heat_index": {
        "label": "Heat Index",
        "unit": "°C",
        "icon": "🔥",
        "description": "How hot the conditions feel.",
    },
    "dew_point": {
        "label": "Dew Point",
        "unit": "°C",
        "icon": "💧",
        "description": "Temperature at which moisture begins to condense.",
    },
    "absolute_humidity": {
        "label": "Absolute Humidity",
        "unit": "g/m³",
        "icon": "🌡",
        "description": "Water vapour present in the air.",
    },
    "true_aqi": {
        "label": "True AQI",
        "unit": "AQI",
        "icon": "AQI",
        "description": "Air Quality Index supplied by the backend.",
    },
}

# Thread lock to protect shared state (nodes, history buffers, cache timestamps).
lock = threading.RLock()
# Database adapter instance (obtained via config.get_db()).
DB = get_db()
# In-memory node registry: maps node_id -> node metadata/state.
nodes = {}
# Rolling history buffers per node (max 20000 points) for plotting/trends.
history_buf = defaultdict(lambda: deque(maxlen=20000))
# Tracks the most recent database error to display on the dashboard.
db_error = {"msg": None}
# Timestamp of last successful DB pull for caching/throttling.
_last_pull = {"t": 0.0}


# ---------- Data helpers ----------

def is_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def clean_value(value, decimals=1, empty="—"):
    if value is None or value == "":
        return empty
    if is_number(value):
        return f"{float(value):.{decimals}f}"
    return str(value)


def force_epoch(value):
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    if is_number(value):
        return float(value)

    text = str(value).strip()
    if not text:
        return 0.0

    try:
        return float(text)
    except ValueError:
        pass

    cleaned = text.replace("T", " ").replace("Z", "")
    cleaned = cleaned.split("+")[0].split(".")[0]

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return time.mktime(time.strptime(cleaned, fmt))
        except ValueError:
            continue

    return 0.0


def format_time(value):
    timestamp = force_epoch(value)
    if not timestamp:
        return "—"
    return time.strftime("%H:%M:%S", time.localtime(timestamp))


def format_date_time(value):
    timestamp = force_epoch(value)
    if not timestamp:
        return "—"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def safe_db_call(method_names, *args, **kwargs):
    if DB is None:
        return None

    for method_name in method_names:
        method = getattr(DB, method_name, None)
        if not callable(method):
            continue

        try:
            return method(*args, **kwargs)
        except TypeError:
            try:
                return method(*args)
            except Exception as exc:
                db_error["msg"] = f"{method_name}: {type(exc).__name__}: {exc}"
        except Exception as exc:
            db_error["msg"] = f"{method_name}: {type(exc).__name__}: {exc}"

    return None


def normalize_latest(record):
    if not isinstance(record, dict):
        return {}

    result = dict(record)
    result["sensors"] = (
        dict(result.get("sensors"))
        if isinstance(result.get("sensors"), dict)
        else {}
    )
    calc = result.get("calculations")
    if not isinstance(calc, dict):
        calc = result.get("derived")
    result["calculations"] = dict(calc) if isinstance(calc, dict) else {}

    aliases = {
        "heatIndex": "heat_index",
        "dewPoint": "dew_point",
        "absoluteHumidity": "absolute_humidity",
        "trueAqi": "true_aqi",
        "aqi": "true_aqi",
    }

    for source, target in aliases.items():
        if result.get(target) is None and result.get(source) is not None:
            result[target] = result[source]
        if (
            result.get(target) is None
            and result["calculations"].get(source) is not None
        ):
            result[target] = result["calculations"][source]

    if result.get("true_aqi") is None:
        for source in ("air_quality_index", "airQualityIndex"):
            value = result.get(source)
            if value is None:
                value = (result.get("extra") or {}).get(source)
            if value is not None:
                result["true_aqi"] = value
                break

    for key in PRIMARY_CALCULATIONS:
        if result.get(key) is None and result["calculations"].get(key) is not None:
            result[key] = result["calculations"][key]

    return result


def get_known_nodes():
    records = safe_db_call(
        ("get_known_nodes", "get_devices", "get_nodes"),
        hours=LOOKBACK_HOURS,
    )

    if records is None:
        return []

    if isinstance(records, dict):
        records = records.get("nodes") or records.get("devices") or []

    return sorted(str(item) for item in records if item is not None)


def get_latest(node_id):
    record = safe_db_call(
        ("get_latest_reading", "get_latest"),
        node_id,
        hours=LOOKBACK_HOURS,
    )
    return normalize_latest(record or {})


def get_history(node_id, hours):
    records = safe_db_call(
        ("get_history", "get_readings", "get_node_history", "get_recent_readings"),
        node_id,
        hours=hours,
    )

    if records is None:
        return []

    if isinstance(records, dict):
        records = records.get("readings") or records.get("data") or []

    if isinstance(records, dict):
        records = [records]

    return [
        normalize_latest(item)
        for item in records or []
        if isinstance(item, dict)
    ]


def get_locations():
    locations = safe_db_call(
        ("get_locations", "get_known_locations", "get_device_locations"),
        hours=LOOKBACK_HOURS,
    )

    if isinstance(locations, dict):
        locations = (
            locations.get("locations")
            or locations.get("devices")
            or [locations]
        )

    rows = []

    for item in locations or []:
        if not isinstance(item, dict):
            continue

        lat = item.get("lat", item.get("latitude"))
        lon = item.get(
            "lon",
            item.get("longitude", item.get("lng")),
        )

        if not (is_number(lat) and is_number(lon)):
            continue

        device_id = item.get(
            "deviceId",
            item.get("device_id", item.get("id")),
        )

        rows.append(
            {
                "device_id": str(device_id) if device_id is not None else "—",
                "name": item.get("name", item.get("station", device_id or "—")),
                "role": item.get("role", "Monitoring Node"),
                "latitude": float(lat),
                "longitude": float(lon),
                "status": item.get("status", "Unknown"),
                "last_seen": item.get(
                    "lastSeen",
                    item.get("last_seen", item.get("timestamp")),
                ),
            }
        )

    return rows


def get_alerts():
    records = safe_db_call(
        ("get_alerts", "get_attack_events", "get_events"),
        hours=LOOKBACK_HOURS,
    )

    if records is None:
        return []

    if isinstance(records, dict):
        records = (
            records.get("alerts")
            or records.get("events")
            or records.get("data")
            or [records]
        )

    return [dict(item) for item in records or [] if isinstance(item, dict)]


def device_name(node_id, latest=None, location_rows=None):
    latest = latest or {}

    for value in (
        latest.get("name"),
        latest.get("device"),
        latest.get("hostname"),
        latest.get("device_id"),
    ):
        if value:
            return str(value)

    if location_rows:
        for row in location_rows:
            if str(row["device_id"]) == str(node_id):
                if row.get("name"):
                    return str(row["name"])

    return str(node_id)


def latest_status(timestamp):
    timestamp = force_epoch(timestamp)
    if not timestamp:
        return "Unknown"
    return "Online" if time.time() - timestamp < OFFLINE_AFTER_S else "Offline"


def connection_value(latest, key):
    value = latest.get(key)
    if value is None:
        value = latest.get("sensors", {}).get(key)
    return value


def location_for_node(node_id, latest=None):
    latest = latest or {}
    locations = get_locations()

    for row in locations:
        if str(row["device_id"]) == str(node_id):
            return row

    lat = latest.get("lat", latest.get("latitude"))
    lon = latest.get("lon", latest.get("longitude", latest.get("lng")))

    if not (is_number(lat) and is_number(lon)):
        sensors = latest.get("sensors", {})
        lat = sensors.get("lat", sensors.get("latitude"))
        lon = sensors.get("lon", sensors.get("longitude", sensors.get("lng")))

    if is_number(lat) and is_number(lon):
        return {
            "device_id": node_id,
            "name": latest.get("name", node_id),
            "role": latest.get("role", "Monitoring Node"),
            "latitude": float(lat),
            "longitude": float(lon),
            "status": latest_status(latest.get("timestamp")),
            "last_seen": latest.get("timestamp"),
        }

    return None


def pull_data(force=False):
    with lock:
        now = time.time()

        if not auth.signed_in():
            return

        if not force and now - _last_pull["t"] < CACHE_TTL_S:
            return

        _last_pull["t"] = now
        ids = get_known_nodes()

        if db_error["msg"] is None:
            db_error["msg"] = None

        for node_id in ids:
            latest = get_latest(node_id)
            if not latest:
                continue

            ts = force_epoch(latest.get("timestamp"))

            nodes[node_id] = {
                "id": node_id,
                "latest": latest,
                "last_seen": ts,
            }

            if ts:
                buf = history_buf[node_id]
                if not buf or ts > buf[-1][0]:
                    buf.append((ts, latest))

        for node_id in list(nodes):
            if node_id not in ids:
                nodes.pop(node_id, None)
                history_buf.pop(node_id, None)


def node_ids():
    return sorted(nodes)


def node_snapshot(node_id):
    node = nodes[node_id]
    latest = node["latest"]
    timestamp = node["last_seen"]
    online = bool(timestamp) and time.time() - timestamp < OFFLINE_AFTER_S

    location = location_for_node(node_id, latest)

    wifi = connection_value(latest, "wifi")
    mqtt = connection_value(latest, "mqtt")
    rssi = connection_value(latest, "rssi")

    wifi_text = str(wifi).lower() if wifi is not None else ""
    mqtt_text = str(mqtt).lower() if mqtt is not None else ""

    if wifi_text in {"true", "1", "connected", "online", "up"}:
        wifi_state = "connected"
    elif wifi_text in {"false", "0", "disconnected", "offline", "down"}:
        wifi_state = "disconnected"
    elif is_number(rssi):
        wifi_state = "weak" if float(rssi) < -75 else "connected"
    else:
        wifi_state = "unknown"

    if mqtt_text in {"true", "1", "connected", "online", "up"}:
        mqtt_state = "connected"
    elif mqtt_text in {"false", "0", "disconnected", "offline", "down"}:
        mqtt_state = "disconnected"
    else:
        mqtt_state = "connected" if online else "disconnected"

    return {
        **node,
        "latest": latest,
        "online": online,
        "wifi": wifi_state,
        "mqtt": mqtt_state,
        "rssi": rssi if is_number(rssi) else None,
        "name": device_name(node_id, latest),
        "location": location,
    }


# ---------- UI helpers ----------

def empty(message):
    return html.P(message, className="empty-state")


def status_pill(online):
    return html.Span(
        [html.I(), " Online" if online else " Offline"],
        className="status online" if online else "status offline",
    )


def conn_pill(state):
    labels = {
        "connected": ("Connected", "good"),
        "weak": ("Weak signal", "warning"),
        "reconnecting": ("Reconnecting", "warning"),
        "disconnected": ("Disconnected", "bad"),
        "unknown": ("Unknown", "warning"),
    }
    text, tone = labels.get(state, (str(state).title(), "warning"))
    return html.Span([html.I(), text], className=f"pill pill-{tone}")


def page_header(eyebrow, title, subtitle, actions=None):
    return html.Header(
        className="page-header",
        children=[
            html.Div([
                html.P(eyebrow, className="eyebrow"),
                html.H1(title),
                html.P(subtitle, className="subtitle"),
            ]),
            html.Div(actions or [], className="header-actions"),
        ],
    )


def banner(eyebrow, heading, text, meta_children=None, bad=False):
    return html.Section(
        className="status-banner bad" if bad else "status-banner",
        children=[
            html.Div([
                html.P(eyebrow, className="eyebrow"),
                html.H2(heading),
                html.P(text),
            ]),
            html.Div(meta_children or [], className="status-meta"),
        ],
    )


def metric_card(label, icon, value, unit, note, accent=False):
    value_content = [value]
    if unit and value != "—":
        value_content.extend([" ", html.Small(unit)])

    return html.Article(
        className="metric-card accent-card" if accent else "metric-card",
        children=[
            html.Div(
                [
                    html.Span(label),
                    html.Span(icon, className="metric-icon"),
                ],
                className="metric-top",
            ),
            html.Strong(value_content),
            html.P(note),
        ],
    )


def panel_heading(title, subtitle, right=None):
    children = [html.Div([html.H2(title), html.P(subtitle)])]
    if right is not None:
        children.append(right)
    return html.Div(children, className="panel-heading")


def refresh_btn(id_):
    return html.Button("Refresh data", id=id_, n_clicks=0)


def dropdown(id_, options, value, width="240px"):
    return dcc.Dropdown(
        id=id_,
        options=options,
        value=value,
        clearable=False,
        style={"width": width, "minWidth": "180px"},
    )


def station_options(with_all=False):
    options = []
    if not auth.signed_in():
        return options

    for node_id in node_ids():
        sn = node_snapshot(node_id)
        label = sn["name"]
        if sn.get("location") and sn["location"].get("name"):
            label = sn["location"]["name"]
        options.append({"label": f"{label} ({node_id})", "value": node_id})

    if with_all:
        return [{"label": "All stations", "value": "all"}] + options
    return options


def pick_node(value):
    if not auth.signed_in():
        return None
    if value in node_ids():
        return value
    return node_ids()[0] if node_ids() else None


def updated_text():
    if db_error["msg"]:
        return f"Database error: {db_error['msg']}"

    latest = max((n["last_seen"] for n in nodes.values()), default=0)
    if not latest:
        return "No data yet"
    return "Latest database reading: " + format_time(latest)


def alert_timestamp(alert):
    for key in ("timestamp", "time", "created_at", "createdAt", "first_seen"):
        if alert.get(key) is not None:
            return alert.get(key)
    return None


def alert_station(alert):
    for key in ("station", "stationId", "deviceId", "device_id", "node", "node_id"):
        if alert.get(key) is not None:
            return str(alert[key])
    return "Unknown"


def alert_title(alert):
    for key in ("title", "name", "alert_type", "attack_type", "message"):
        if alert.get(key) is not None:
            return str(alert[key])
    return "Database event"


def alert_description(alert):
    for key in ("description", "text", "details", "message"):
        if alert.get(key) is not None:
            return str(alert[key])
    return ""


def alert_severity(alert):
    value = alert.get("severity", alert.get("level", "info"))
    return str(value).lower()


def alert_type(alert):
    value = alert.get("type", alert.get("category", alert.get("event_type", "event")))
    return str(value)


# ---------- Login ----------

def brand_block():
    return html.Div(
        className="brand",
        children=[
            html.Span("◉", className="brand-icon"),
            html.Div([
                html.Strong("Weather Monitoring"),
                html.Small("Modular System"),
            ]),
        ],
    )


def login_card():
    return html.Section(
        className="login-card",
        children=[
            brand_block(),
            html.P("Student sign in", className="eyebrow"),
            html.H1("Welcome back"),
            html.Div(
                className="login-fields",
                children=[
                    html.Label("Student number"),
                    dcc.Input(
                        id="login-user",
                        type="text",
                        placeholder="Student number",
                        debounce=True,
                        className="field",
                        autoFocus=True,
                    ),
                    html.Label("Password"),
                    dcc.Input(
                        id="login-pass",
                        type="password",
                        placeholder="Your password",
                        debounce=True,
                        className="field",
                    ),
                ],
            ),
            html.Button("Sign in", id="login-btn", n_clicks=0, className="login-btn"),
            html.Div(id="login-msg"),
        ],
    )


def account_skeleton():
    return [
        page_header(
            "Account",
            "Account & password",
            "Your student number is your username and cannot be changed. "
            "You can change the password you sign in with here.",
        ),
        html.Div(
            className="lower-grid",
            children=[
                html.Section(
                    className="panel",
                    children=[
                        panel_heading("Signed in as", "Details held in the users table"),
                        html.Dl(
                            className="details",
                            children=[
                                html.Div([html.Dt("Student number"), html.Dd(auth.current_student_number() or "—")]),
                                html.Div([html.Dt("Name"), html.Dd(auth.display_name() or "—")]),
                                html.Div([html.Dt("Password"), html.Dd(auth.password_state(), id="acc-pw-state")]),
                                html.Div([html.Dt("Username format"), html.Dd("Student number")]),
                            ],
                        ),
                    ],
                ),
                html.Section(
                    className="panel",
                    children=[
                        panel_heading("Change password", "Used the next time you sign in"),
                        html.Div(
                            className="login-fields",
                            children=[
                                html.Label("Current password"),
                                dcc.Input(id="acc-current", type="password", debounce=True, className="field"),
                                html.Label("New password"),
                                dcc.Input(id="acc-new", type="password", debounce=True, className="field"),
                                html.Label("Confirm new password"),
                                dcc.Input(id="acc-confirm", type="password", debounce=True, className="field"),
                            ],
                        ),
                        html.Button("Update password", id="acc-btn", n_clicks=0, className="account-btn"),
                        html.Div(id="acc-msg"),
                    ],
                ),
            ],
        ),
    ]


# ---------- CSS ----------
CSS = """
:root{
--navy:#123b2b;--teal:#168153;--teal-dark:#0d603d;--teal-pale:#e9f7ef;
--background:#f2f8f4;--surface:#fff;--border:#dbe5e7;--text:#1e343c;--muted:#687d84;
--warning:#b46a13;--danger:#bb4242;font-family:Inter,Arial,sans-serif
}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:var(--background);color:var(--text)}
.sidebar{position:fixed;inset:0 auto 0 0;width:245px;padding:28px 20px;background:linear-gradient(180deg,#0f4a32,#0a3022);color:#fff;display:flex;flex-direction:column;z-index:20}
.brand{display:flex;align-items:center;gap:12px;margin-bottom:40px}.brand-icon{display:grid;place-items:center;width:42px;height:42px;border-radius:12px;background:#bdebdc;color:var(--teal-dark);font-size:22px}.brand strong,.brand small{display:block}.brand small{margin-top:4px;color:#a9c2c6;font-size:11px}
nav{display:grid;gap:7px}nav a{display:flex;justify-content:space-between;color:#b7c9cc;text-decoration:none;padding:13px 15px;border-radius:10px;font-size:14px}nav a:hover,nav a.active{background:rgba(255,255,255,.13);color:#fff}.alert-count{min-width:22px;padding:2px 7px;border-radius:99px;background:var(--danger);text-align:center;font-size:11px}
.side-note{margin-top:auto;color:#a9c2c6;font-size:12px;line-height:1.6}
main{margin-left:245px;padding:34px 42px 60px;max-width:1500px}.page-header{display:flex;align-items:flex-start;justify-content:space-between;gap:30px;margin-bottom:25px}h1,h2,h3,p{margin-top:0}h1{margin-bottom:7px;font-size:clamp(27px,3vw,36px);letter-spacing:-.04em}h2{margin-bottom:5px;font-size:18px}.eyebrow{margin-bottom:6px;color:var(--teal);font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}.subtitle,.panel-heading p{margin-bottom:0;color:var(--muted);font-size:14px}.header-actions{display:flex;align-items:center;gap:9px;flex-wrap:wrap}.header-actions label{color:var(--muted);font-size:12px;font-weight:700}
button{min-height:42px;border:1px solid var(--teal);border-radius:10px;background:var(--teal);color:#fff;padding:0 13px;font:inherit;font-weight:700;cursor:pointer}button:hover{background:var(--teal-dark)}
.status-banner{display:flex;align-items:center;justify-content:space-between;gap:30px;margin-bottom:27px;padding:24px 26px;border:1px solid #b9dfca;border-left:6px solid var(--teal);border-radius:16px;background:linear-gradient(135deg,#e5f7ed,#f8fffb)}.status-banner.bad{border-left-color:var(--danger);border-color:#f0cccc;background:linear-gradient(135deg,#fdeeee,#fffafa)}.status-banner h2{font-size:20px}.status-banner p:last-child{margin-bottom:0;color:#56716d;font-size:13px}.status-meta{display:grid;justify-items:end;gap:10px;color:var(--muted);font-size:12px}.status{display:inline-flex;align-items:center;gap:7px;padding:8px 12px;border-radius:99px;background:#fff;color:var(--teal-dark);font-size:13px;font-weight:800}.status i{width:8px;height:8px;border-radius:50%;background:#29a36a}.status.offline{color:var(--danger)}.status.offline i{background:var(--danger)}
.metric-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:15px;margin:17px 0 20px}.metric-card,.panel{border:1px solid var(--border);border-radius:16px;background:var(--surface);box-shadow:0 5px 18px rgba(22,51,59,.05)}.metric-card{position:relative;min-height:150px;padding:21px;overflow:hidden;border-top:5px solid var(--teal)}.metric-card::after{content:"";position:absolute;width:95px;height:95px;right:-35px;bottom:-40px;border-radius:50%;background:var(--teal-pale)}.metric-top{display:flex;justify-content:space-between;gap:8px;color:#526c74;font-size:13px;font-weight:750}.metric-icon{display:grid;place-items:center;min-width:30px;height:30px;padding:0 6px;border-radius:9px;background:var(--teal-pale);color:var(--teal-dark);font-size:12px;font-weight:800}.metric-card>strong{position:relative;z-index:1;display:block;margin:23px 0 8px;color:#113d2d;font-size:33px;letter-spacing:-.04em}.metric-card strong small{font-size:14px}.metric-card p{position:relative;z-index:1;margin:0;color:var(--muted);font-size:12px}.accent-card{background:linear-gradient(145deg,#fff,#effbf4)}
.lower-grid{display:grid;grid-template-columns:1fr 1fr;gap:17px}.panel{padding:21px;border-top:4px solid #9fd5b6;margin-bottom:20px}.panel-heading{display:flex;justify-content:space-between;gap:15px;padding-bottom:16px;border-bottom:1px solid #edf1f2}.details{display:grid;grid-template-columns:1fr 1fr;margin:0}.details div{padding:16px 10px 7px 0}.details dt{margin-bottom:5px;color:var(--muted);font-size:11px}.details dd{margin:0;font-size:13px;font-weight:750}
.filter-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:17px;margin-top:16px}.filter-group{display:grid;gap:7px}.filter-group label{color:var(--muted);font-size:12px;font-weight:700}.table-scroll{overflow-x:auto;margin-top:16px}.data-table{width:100%;border-collapse:collapse;font-size:13px}.data-table th{padding:10px 14px;text-align:left;color:var(--muted);font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid var(--border);white-space:nowrap}.data-table td{padding:14px;border-bottom:1px solid #edf1f2;white-space:nowrap}.device-name{display:block;font-weight:800;color:#113d2d}.device-id{display:block;margin-top:2px;color:var(--muted);font-size:11px}
.pill{display:inline-flex;align-items:center;gap:6px;padding:6px 11px;border-radius:99px;font-size:12px;font-weight:750}.pill i{width:7px;height:7px;border-radius:50%}.pill-good{background:var(--teal-pale);color:var(--teal-dark)}.pill-good i{background:#29a36a}.pill-warning{background:#fff1d6;color:var(--warning)}.pill-warning i{background:var(--warning)}.pill-bad{background:#fbe7e7;color:var(--danger)}.pill-bad i{background:var(--danger)}
.alert-row{display:flex;align-items:flex-start;gap:14px;padding:16px 4px;border-bottom:1px solid #edf1f2}.alert-row:last-child{border:0}.alert-icon{display:grid;place-items:center;flex:0 0 36px;width:36px;height:36px;border-radius:10px;font-weight:850}.alert-icon.critical{background:#fbe7e7;color:var(--danger)}.alert-icon.warning{background:#fff1d6;color:var(--warning)}.alert-icon.info{background:#edf8f2;color:#406ea8}.alert-body{flex:1;min-width:0}.alert-title-row{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.alert-body p{margin:4px 0 0;color:var(--muted);font-size:12px}.type-badge,.severity-badge{padding:3px 9px;border-radius:99px;font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.04em}.type-badge{background:var(--teal-pale);color:var(--teal-dark)}.severity-badge.critical{background:#fbe7e7;color:var(--danger)}.severity-badge.warning{background:#fff1d6;color:var(--warning)}.severity-badge.info{background:#edf8f2;color:#406ea8}.alert-actions{display:flex;gap:8px;margin-top:10px}.ghost-btn{background:#fff;color:var(--teal-dark);border-color:var(--border);min-height:32px;font-size:12px}.danger-btn{background:#fff;color:var(--danger);border-color:#f0cccc;min-height:32px;font-size:12px}.empty-state{margin:22px 0 4px;color:var(--muted);font-size:13px;text-align:center}.map-layout{display:grid;grid-template-columns:minmax(0,1fr) 340px;gap:17px;align-items:start}.big-name{font-size:24px;font-weight:800;color:#113d2d;margin:10px 0 4px}.md p,.md li{color:var(--muted);font-size:13px;line-height:1.6}
@media(max-width:1050px){.page-header{flex-direction:column}.metric-grid{grid-template-columns:1fr 1fr}.map-layout{grid-template-columns:1fr}}@media(max-width:760px){.sidebar{position:static;width:100%;padding:16px}.brand{margin-bottom:14px}nav{display:flex;flex-wrap:wrap}.side-note{display:none}main{margin-left:0;padding:22px 16px 45px}.status-banner,.lower-grid{display:grid;grid-template-columns:1fr}.status-meta{justify-items:start}.filter-grid{grid-template-columns:1fr}}@media(max-width:520px){.metric-grid{grid-template-columns:1fr}.details{grid-template-columns:1fr}}
"""

AUTH_CSS = """
.login-wrap{display:grid;place-items:center;min-height:100vh;padding:34px 18px;background:linear-gradient(160deg,#0f4a32,#0a3022)}
.login-card{width:100%;max-width:420px;padding:32px;border:1px solid var(--border);border-radius:18px;background:var(--surface);box-shadow:0 22px 55px rgba(6,32,22,.34)}
.login-card .brand{margin-bottom:24px}.login-card .brand strong{font-size:16px}.login-card .brand small{margin-top:4px;color:var(--muted);font-size:11px}
.login-card h1{margin-bottom:6px;font-size:26px}
.login-fields{display:grid;gap:7px;margin:18px 0}.login-fields label{color:var(--muted);font-size:12px;font-weight:700}
.field{width:100%;min-height:44px;padding:0 13px;border:1px solid var(--border);border-radius:10px;background:#fff;color:var(--text);font:inherit;font-size:14px}
.field:focus{border-color:var(--teal);outline:2px solid var(--teal);outline-offset:1px}
.login-btn,.account-btn{width:100%}
.login-msg{margin-top:15px;padding:11px 13px;border-radius:10px;font-size:13px;font-weight:700}.login-msg.bad{background:#fbe7e7;color:var(--danger)}.login-msg.good{background:var(--teal-pale);color:var(--teal-dark)}
.who{padding-top:15px;margin-top:16px;border-top:1px solid rgba(255,255,255,.15)}.who-row{display:flex;align-items:center;gap:10px}
.who-avatar{display:grid;place-items:center;flex:0 0 36px;width:36px;height:36px;border-radius:10px;background:#bdebdc;color:var(--teal-dark);font-size:13px;font-weight:800}
.who-text{min-width:0}.who-text strong{display:block;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.who-text small{display:block;margin-top:2px;color:#a9c2c6;font-size:11px}
.who-actions{display:flex;align-items:center;gap:12px;margin-top:13px}.who-link{color:#b7c9cc;font-size:12px;font-weight:700;text-decoration:none}.who-link:hover{color:#fff;text-decoration:underline}
.who-actions button{min-height:32px;padding:0 11px;background:transparent;border-color:rgba(255,255,255,.3);color:#d7e5e6;font-size:12px}.who-actions button:hover{background:rgba(255,255,255,.14)}
"""

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    title=APP_TITLE,
)

# Signs the session cookie that holds the signed-in student number.
app.server.secret_key = auth.secret_key()

app.index_string = (
    "<!DOCTYPE html><html lang='en'><head>{%metas%}"
    "<title>{%title%}</title>{%favicon%}{%css%}<style>"
    + CSS
    + AUTH_CSS
    + "</style></head><body>{%app_entry%}<footer>"
    "{%config%}{%scripts%}{%renderer%}</footer></body></html>"
)


# ---------- Page shells ----------

def overview_skeleton():
    pull_data(force=True)
    return [
        page_header(
            "Overview",
            APP_TITLE,
            "Latest environmental information from the selected monitoring station.",
            [
                html.Label("Station"),
                dropdown("ov-station", station_options(), pick_node(None)),
                refresh_btn("ov-refresh"),
            ],
        ),
        html.Div(id="ov-body"),
    ]


def trends_skeleton():
    pull_data(force=True)
    return [
        page_header(
            "Trends",
            "Environmental Trends",
            "View database history for the four primary calculations.",
            [
                html.Label("Station"),
                dropdown("tr-station", station_options(), pick_node(None)),
                refresh_btn("tr-refresh"),
            ],
        ),
        html.Section(
            className="panel filter-panel",
            children=[
                panel_heading(
                    "Trend filters",
                    "Select a station, calculation and time period.",
                ),
                html.Div(
                    className="filter-grid",
                    children=[
                        html.Div(
                            [
                                html.Label("Calculation"),
                                dropdown(
                                    "tr-measure",
                                    [
                                        {"label": item["label"], "value": key}
                                        for key, item in PRIMARY_CALCULATIONS.items()
                                    ],
                                    "heat_index",
                                    "100%",
                                ),
                            ],
                            className="filter-group",
                        ),
                        html.Div(
                            [
                                html.Label("Time range"),
                                dropdown(
                                    "tr-range",
                                    [
                                        {"label": "Last 24 hours", "value": 24},
                                        {"label": "Last 7 days", "value": 168},
                                        {"label": "Last 30 days", "value": 720},
                                    ],
                                    24,
                                    "100%",
                                ),
                            ],
                            className="filter-group",
                        ),
                    ],
                ),
            ],
        ),
        html.Div(id="tr-body"),
    ]


def map_skeleton():
    pull_data(force=True)
    return [
        page_header(
            "Map",
            "Device Locations",
            "Locations are shown only when valid coordinates are supplied by the database.",
        ),
        html.Div(
            className="map-layout",
            children=[
                html.Section(
                    className="panel",
                    children=[
                        panel_heading(
                            "Live monitoring locations",
                            "Select a marker to view the latest database values.",
                        ),
                        dcc.Graph(
                            id="map-graph",
                            config={"displaylogo": False, "scrollZoom": True},
                            style={"height": "540px"},
                        ),
                    ],
                ),
                html.Aside(id="map-detail", className="panel"),
            ],
        ),
    ]


def devices_skeleton():
    pull_data(force=True)
    return [
        page_header(
            "Devices",
            "Connected Devices",
            "Connection status for devices returned by the database.",
            [
                html.Label("Status"),
                dropdown(
                    "dev-filter",
                    [
                        {"label": "All devices", "value": "all"},
                        {"label": "Online only", "value": "online"},
                        {"label": "Offline only", "value": "offline"},
                    ],
                    "all",
                    "180px",
                ),
                refresh_btn("dev-refresh"),
            ],
        ),
        html.Div(id="dev-body"),
    ]


def alerts_skeleton():
    pull_data(force=True)
    return [
        page_header(
            "Alerts",
            "Alerts",
            "Alerts and events returned by the backend database.",
            [refresh_btn("al-refresh")],
        ),
        html.Div(id="al-banner"),
        html.Section(
            className="panel filter-panel",
            children=[
                panel_heading("Filter alerts", "Filter database events by station, severity and time."),
                html.Div(
                    className="filter-grid",
                    children=[
                        html.Div(
                            [
                                html.Label("Station"),
                                dropdown("al-station", station_options(True), "all", "100%"),
                            ],
                            className="filter-group",
                        ),
                        html.Div(
                            [
                                html.Label("Severity"),
                                dropdown(
                                    "al-severity",
                                    [
                                        {"label": "All severities", "value": "all"},
                                        {"label": "Critical", "value": "critical"},
                                        {"label": "Warning", "value": "warning"},
                                        {"label": "Info", "value": "info"},
                                    ],
                                    "all",
                                    "100%",
                                ),
                            ],
                            className="filter-group",
                        ),
                        html.Div(
                            [
                                html.Label("Time period"),
                                dropdown(
                                    "al-time",
                                    [
                                        {"label": "All time", "value": "all"},
                                        {"label": "Last 24 hours", "value": 24},
                                        {"label": "Last 7 days", "value": 168},
                                    ],
                                    "all",
                                    "100%",
                                ),
                            ],
                            className="filter-group",
                        ),
                    ],
                ),
            ],
        ),
        html.Div(id="al-body"),
    ]


PAGES = {
    "/": overview_skeleton,
    "/map": map_skeleton,
    "/trends": trends_skeleton,
    "/devices": devices_skeleton,
    "/alerts": alerts_skeleton,
    "/account": account_skeleton,
}

NAV = [
    ("/", "Overview"),
    ("/map", "Stations / Map"),
    ("/trends", "Trends"),
    ("/devices", "Devices"),
    ("/alerts", "Alerts"),
    ("/account", "Account"),
]


app.layout = html.Div(
    [
        dcc.Location(id="url"),
        dcc.Store(id="auth"),
        dcc.Interval(id="tick", interval=REFRESH_MS, n_intervals=0),
        html.Div(
            className="login-wrap",
            id="login-wrap",
            children=[login_card()],
        ),
        html.Div(
            id="app-shell",
            children=[
                html.Aside(
                    className="sidebar",
                    children=[
                        brand_block(),
                        html.Nav(id="nav"),
                        html.Div(id="who", className="who"),
                        html.Div(
                            [
                                "Live from database",
                                html.Br(),
                                DB_CHOICE.upper(),
                            ],
                            className="side-note",
                        ),
                    ],
                ),
                html.Main(id="page"),
            ],
        ),
    ]
)


# ---------- Routing ----------
@app.callback(
    Output("page", "children"),
    Input("url", "pathname"),
    Input("auth", "data"),
)
def route(path, _auth):
    if (path or "/").rstrip("/") == "/logout":
        # Signed out here as well as in handle_logout, so bookmarking
        # /logout still ends the session.
        auth.sign_out()
        return []

    build = PAGES.get((path or "/").rstrip("/") or "/")
    if build:
        return build()
    return [
        html.H1("Page not found"),
        dcc.Link("Back to Overview", href="/"),
    ]


# ---------- Sign in ----------
@app.callback(
    Output("app-shell", "style"),
    Output("login-wrap", "style"),
    Input("auth", "data"),
)
def shell_visibility(_auth):
    """Shows the login card or the dashboard, never both.

    The display values are spelled out rather than left as an empty style:
    Dash patches styles by diffing, so an empty dict would keep the old
    display:none in place."""
    if auth.signed_in():
        return {"display": "block"}, {"display": "none"}
    return {"display": "none"}, {"display": "grid"}


@app.callback(
    Output("auth", "data", allow_duplicate=True),
    Output("login-msg", "children"),
    Output("login-msg", "className"),
    Input("login-btn", "n_clicks"),
    State("login-user", "value"),
    State("login-pass", "value"),
    prevent_initial_call=True,
)
def sign_in(_clicks, username, password):
    # The button sits in the fixed layout, but check the click anyway: Dash can
    # invoke a callback for a component it built after the page loaded.
    if not _clicks:
        return dash.no_update, dash.no_update, dash.no_update

    student_number, error = auth.sign_in(username, password)

    if not student_number:
        return dash.no_update, error, "login-msg bad"

    return (
        {"student_number": student_number, "name": auth.display_name()},
        None,
        "login-msg",
    )


@app.callback(
    Output("who", "children"),
    Input("auth", "data"),
)
def who(_auth):
    student_number = auth.current_student_number()
    if not student_number:
        return None

    name = auth.display_name() or student_number
    initials = "".join(word[0] for word in name.split()[:2]).upper()

    return [
        html.Div(
            [
                html.Span(initials or student_number, className="who-avatar"),
                html.Div(
                    [
                        html.Strong(name),
                        html.Small(f"Student {student_number}"),
                    ],
                    className="who-text",
                ),
            ],
            className="who-row",
        ),
        html.Div(
            [
                dcc.Link("Account & password", href="/account", className="who-link"),
                dcc.Link("Log out", href="/logout", className="who-link"),
            ],
            className="who-actions",
        ),
    ]


@app.callback(
    Output("url", "href", allow_duplicate=True),
    Output("auth", "data", allow_duplicate=True),
    Input("url", "pathname"),
    prevent_initial_call=True,
)
def handle_logout(path):
    """'Log out' is a plain link to /logout, resolved here rather than by a
    Flask route: Dash registers a catch-all that serves the app for every
    unmatched path, so a Flask route on the same path is never reached.

    Going through the URL (instead of a button) also stops Dash from firing
    the callback when it rebuilds the sidebar and creates a fresh button."""
    if (path or "").rstrip("/") != "/logout":
        return dash.no_update, dash.no_update

    auth.sign_out()
    return "/", None


# ---------- Change password ----------
@app.callback(
    Output("acc-msg", "children"),
    Output("acc-msg", "className"),
    Output("acc-pw-state", "children"),
    Input("acc-btn", "n_clicks"),
    State("acc-current", "value"),
    State("acc-new", "value"),
    State("acc-confirm", "value"),
    prevent_initial_call=True,
)
def update_password(_clicks, current, new, confirm):
    # /account is rebuilt every time the route runs, so this callback can be
    # invoked with n_clicks still 0. Do nothing then.
    if not _clicks:
        return dash.no_update, dash.no_update, dash.no_update

    changed, error = auth.change_password(current, new, confirm)

    if not changed:
        return error, "login-msg bad", dash.no_update

    return (
        "Password updated. Use it the next time you sign in.",
        "login-msg good",
        auth.password_state(),
    )


@app.callback(
    Output("nav", "children"),
    Input("url", "pathname"),
    Input("tick", "n_intervals"),
)
def nav(path, _):
    if not auth.signed_in():
        return []

    current_path = (path or "/").rstrip("/") or "/"
    alert_count = len(get_alerts())

    links = []
    for href, label in NAV:
        children = [label]
        if href == "/alerts" and alert_count:
            children.append(html.Span(alert_count, className="alert-count"))
        links.append(
            dcc.Link(
                children,
                href=href,
                className="active" if href == current_path else "",
            )
        )
    return links


# ---------- Overview ----------
@app.callback(
    Output("ov-body", "children"),
    Input("tick", "n_intervals"),
    Input("ov-station", "value"),
    Input("ov-refresh", "n_clicks"),
    Input("auth", "data"),
)
def overview(_, station, __, ___):
    if not auth.signed_in():
        return empty(SIGNED_OUT_TEXT)

    pull_data(force=ctx.triggered_id == "ov-refresh")

    node_id = pick_node(station)
    if not node_id:
        return empty("No monitoring stations are currently available from the database.")

    sn = node_snapshot(node_id)
    latest = sn["latest"]
    location = sn.get("location") or {}
    last_seen = sn["last_seen"]

    if not last_seen:
        heading = "Waiting for the first database reading"
        message = "The station exists in the database, but no timestamp is available yet."
        bad = False
    elif not sn["online"]:
        heading = "This station is currently offline"
        message = "The latest stored reading is older than the configured offline threshold."
        bad = True
    else:
        heading = "Latest database reading received"
        message = "The values below are the current calculations returned by the backend."
        bad = False

    cards = []
    for key, item in PRIMARY_CALCULATIONS.items():
        decimals = 0 if key == "true_aqi" else 1
        cards.append(
            metric_card(
                item["label"],
                item["icon"],
                clean_value(latest.get(key), decimals),
                item["unit"],
                item["description"],
                key == "true_aqi",
            )
        )

    site_name = location.get("name") or latest.get("location") or latest.get("site") or "Location not supplied"

    return [
        banner(
            "Current summary",
            heading,
            message,
            [status_pill(sn["online"]), html.Span(f"Last message: {format_time(last_seen)}")],
            bad=bad,
        ),
        html.Div(className="metric-grid", children=cards),
        html.Div(
            className="lower-grid",
            children=[
                html.Section(
                    className="panel",
                    children=[
                        panel_heading(
                            "Station information",
                            "Details returned by the database",
                            dcc.Link("View devices →", href="/devices"),
                        ),
                        html.Dl(
                            className="details",
                            children=[
                                html.Div([html.Dt("Device"), html.Dd(sn["name"])]),
                                html.Div([html.Dt("Device ID"), html.Dd(node_id)]),
                                html.Div([html.Dt("Location"), html.Dd(site_name)]),
                                html.Div([html.Dt("Status"), html.Dd("Online" if sn["online"] else "Offline")]),
                                html.Div([html.Dt("Last message"), html.Dd(format_date_time(last_seen))]),
                                html.Div([html.Dt("IP address"), html.Dd(str(latest.get("ip", latest.get("ip_address", "—"))))]),
                            ],
                        ),
                    ],
                ),
                html.Section(
                    className="panel",
                    children=[
                        panel_heading(
                            "Database source",
                            "Frontend data handoff",
                        ),
                        dcc.Markdown(
                            "The dashboard reads stored readings and calculations through `config.get_db()`. "
                            "The frontend does not generate sensor values or write MQTT data.",
                            className="md",
                        ),
                        html.P(updated_text()),
                    ],
                ),
            ],
        ),
    ]


# ---------- Trends ----------
@app.callback(
    Output("tr-body", "children"),
    Input("tick", "n_intervals"),
    Input("tr-station", "value"),
    Input("tr-measure", "value"),
    Input("tr-range", "value"),
    Input("tr-refresh", "n_clicks"),
    Input("auth", "data"),
)
def trends(_, station, measure, hours, __, ___):
    if not auth.signed_in():
        return empty(SIGNED_OUT_TEXT)

    pull_data(force=ctx.triggered_id == "tr-refresh")

    node_id = pick_node(station)
    if not node_id:
        return empty("No monitoring stations are currently available from the database.")

    measure = measure if measure in PRIMARY_CALCULATIONS else "heat_index"
    definition = PRIMARY_CALCULATIONS[measure]
    latest = node_snapshot(node_id)["latest"]

    records = get_history(node_id, int(hours or 24))
    points = []
    for row in records:
        timestamp = force_epoch(row.get("timestamp"))
        value = row.get(measure)
        if timestamp and is_number(value):
            points.append((timestamp, float(value)))

    points.sort(key=lambda item: item[0])

    if not points:
        return [
            banner(
                "Trend analysis",
                f"{definition['label']} trends",
                "No historical values were returned for this selection.",
                [html.Span(updated_text())],
            ),
            html.Section(
                className="panel",
                children=[
                    panel_heading(
                        definition["label"],
                        f"{node_id} · Last {int(hours or 24)} hours",
                    ),
                    empty("No history is available in the database for this window."),
                ],
            ),
        ]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=[datetime.fromtimestamp(ts) for ts, _ in points],
            y=[value for _, value in points],
            mode="lines+markers",
            name=definition["label"],
            line={"width": 3},
            marker={"size": 5},
        )
    )
    fig.update_layout(
        height=380,
        margin=dict(l=50, r=20, t=10, b=40),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False),
        yaxis=dict(gridcolor="#edf1f2", ticksuffix=f" {definition['unit']}"),
        showlegend=False,
        uirevision=f"{node_id}-{measure}-{hours}",
    )

    return [
        banner(
            "Trend analysis",
            f"{definition['label']} trends",
            "Historical values returned by the database.",
            [status_pill(node_snapshot(node_id)["online"]), html.Span(updated_text())],
        ),
        html.Section(
            className="panel",
            children=[
                panel_heading(
                    definition["label"],
                    f"{node_id} · Last {int(hours or 24)} hours",
                    html.Span(definition["unit"], className="type-badge"),
                ),
                dcc.Graph(figure=fig, config={"displaylogo": False}),
            ],
        ),
        html.Div(
            className="metric-grid",
            children=[
                metric_card(
                    "Current value",
                    definition["icon"],
                    clean_value(latest.get(measure), 0 if measure == "true_aqi" else 1),
                    definition["unit"],
                    "Latest value returned by the database.",
                ),
                metric_card(
                    "Data points",
                    "#",
                    str(len(points)),
                    None,
                    "Historical readings plotted.",
                ),
            ],
            style={"gridTemplateColumns": "repeat(2,minmax(0,1fr))"},
        ),
    ]


# ---------- Map ----------

def map_points():
    pull_data()
    rows = []
    for node_id in node_ids():
        sn = node_snapshot(node_id)
        location = sn.get("location")
        if not location:
            continue
        rows.append(
            {
                "id": node_id,
                "name": sn["name"],
                "site": location.get("name") or "Location",
                "lat": location["latitude"],
                "lon": location["longitude"],
                "online": sn["online"],
            }
        )
    return rows


@app.callback(
    Output("map-graph", "figure"),
    Input("tick", "n_intervals"),
    Input("auth", "data"),
)
def map_fig(_, __):
    points = map_points()

    if not points:
        fig = go.Figure()
        fig.update_layout(
            height=540,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis={"visible": False},
            yaxis={"visible": False},
            annotations=[
                {
                    "text": "No valid station locations are currently available from the database.",
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "showarrow": False,
                }
            ],
        )
        return fig

    trace = getattr(go, "Scattermap", None) or getattr(go, "Scattermapbox", None)
    use_maplibre = hasattr(go, "Scattermap")

    marker_colors = ["#168153" if p["online"] else "#b7bcc3" for p in points]
    marker_sizes = [20 if p["online"] else 10 for p in points]

    fig = go.Figure(
        trace(
            lat=[p["lat"] for p in points],
            lon=[p["lon"] for p in points],
            mode="markers+text",
            marker={
                "size": marker_sizes,
                "color": marker_colors,
            },
            text=[p["name"] for p in points],
            textposition="top center",
            customdata=[p["id"] for p in points],
            hovertemplate="%{text}<extra></extra>",
        )
    )

    center_lat = sum(p["lat"] for p in points) / len(points)
    center_lon = sum(p["lon"] for p in points) / len(points)

    if use_maplibre:
        fig.update_layout(
            map={
                "zoom": 15,
                "center": {"lat": center_lat, "lon": center_lon},
            }
        )
    else:
        fig.update_layout(
            mapbox={
                "zoom": 15,
                "center": {"lat": center_lat, "lon": center_lon},
            }
        )

    fig.update_layout(
        height=540,
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=False,
        uirevision="database-locations",
    )
    return fig


@app.callback(
    Output("map-detail", "children"),
    Input("tick", "n_intervals"),
    Input("map-graph", "clickData"),
    Input("auth", "data"),
)
def map_detail(_, click, __):
    points = map_points()
    if not points:
        return empty("No station locations are currently available.")

    selected = None
    if click:
        selected = click.get("points", [{}])[0].get("customdata")

    selected = selected if selected in {p["id"] for p in points} else points[0]["id"]
    sn = node_snapshot(selected)
    latest = sn["latest"]
    location = sn.get("location") or {}

    cards = [
        metric_card(
            item["label"],
            item["icon"],
            clean_value(latest.get(key), 0 if key == "true_aqi" else 1),
            item["unit"],
            "",
            key == "true_aqi",
        )
        for key, item in PRIMARY_CALCULATIONS.items()
    ]

    return [
        panel_heading("Selected Location", "Latest stored station information"),
        html.Div(sn["name"], className="big-name"),
        html.Dl(
            className="details",
            children=[
                html.Div([html.Dt("Device ID"), html.Dd(selected)]),
                html.Div([html.Dt("Location"), html.Dd(location.get("name", "—"))]),
                html.Div([html.Dt("Latitude"), html.Dd(clean_value(location.get("latitude"), 6))]),
                html.Div([html.Dt("Longitude"), html.Dd(clean_value(location.get("longitude"), 6))]),
                html.Div([html.Dt("Status"), html.Dd("ONLINE" if sn["online"] else "OFFLINE")]),
                html.Div([html.Dt("Last message"), html.Dd(format_date_time(sn["last_seen"]))]),
            ],
        ),
        html.H3("Current calculations", style={"marginTop": "18px"}),
        html.Div(
            className="metric-grid",
            children=cards,
            style={"gridTemplateColumns": "1fr 1fr"},
        ),
    ]


# ---------- Devices ----------
@app.callback(
    Output("dev-body", "children"),
    Input("tick", "n_intervals"),
    Input("dev-filter", "value"),
    Input("dev-refresh", "n_clicks"),
    Input("auth", "data"),
)
def devices(_, flt, __, ___):
    if not auth.signed_in():
        return empty(SIGNED_OUT_TEXT)

    pull_data(force=ctx.triggered_id == "dev-refresh")

    snapshots = [node_snapshot(node_id) for node_id in node_ids()]
    online_count = sum(sn["online"] for sn in snapshots)
    offline_count = len(snapshots) - online_count
    rows = [
        sn
        for sn in snapshots
        if flt == "all" or (flt == "online" and sn["online"]) or (flt == "offline" and not sn["online"])
    ]

    if not snapshots:
        return empty("No devices are currently available from the database.")

    table_rows = []
    for sn in rows:
        table_rows.append(
            html.Tr(
                [
                    html.Td([
                        html.Span(sn["name"], className="device-name"),
                        html.Span(sn["id"], className="device-id"),
                    ]),
                    html.Td((sn.get("location") or {}).get("name", "—")),
                    html.Td(status_pill(sn["online"])),
                    html.Td(conn_pill(sn["wifi"])),
                    html.Td(conn_pill(sn["mqtt"])),
                    html.Td(format_date_time(sn["last_seen"])),
                ]
            )
        )

    return [
        banner(
            "Fleet summary",
            f"{online_count} of {len(snapshots)} devices are online",
            "Status is based on the latest timestamp returned by the database.",
            [html.Span(updated_text())],
        ),
        html.Div(
            className="metric-grid",
            children=[
                metric_card("Total devices", "#", str(len(snapshots)), None, "Devices returned by the database."),
                metric_card("Online", "◉", str(online_count), None, "Latest reading is within the online threshold."),
                metric_card("Offline", "×", str(offline_count), None, "Latest reading is older than the offline threshold."),
                metric_card("Database", "DB", DB_CHOICE.upper(), None, "Configured database adapter.", True),
            ],
        ),
        html.Section(
            className="panel",
            children=[
                panel_heading("Device list", "Device, location and connection status"),
                html.Div(
                    className="table-scroll",
                    children=html.Table(
                        className="data-table",
                        children=[
                            html.Thead(html.Tr([
                                html.Th("Device"), html.Th("Location"), html.Th("Status"),
                                html.Th("WiFi"), html.Th("MQTT"), html.Th("Last message"),
                            ])),
                            html.Tbody(table_rows),
                        ],
                    ),
                ),
                None if rows else empty("No devices match the selected filter."),
            ],
        ),
    ]


# ---------- Alerts ----------
def alert_row(alert, with_actions=True):
    severity = alert_severity(alert)
    title = alert_title(alert)
    station = alert_station(alert)
    description = alert_description(alert)
    timestamp = alert_timestamp(alert)
    alert_id = str(alert.get("id", alert.get("event_id", f"{station}:{timestamp}:{title}")))
    alert["_ui_id"] = alert_id

    badges = [
        html.Strong(title),
        html.Span(alert_type(alert).title(), className="type-badge"),
        html.Span(severity.title(), className=f"severity-badge {severity}"),
    ]

    body = [
        html.Div(badges, className="alert-title-row"),
        html.P(f"{station} · {format_date_time(timestamp)}"),
    ]
    if description:
        body.append(html.P(description))

    if with_actions:
        body.append(
            html.Div(
                [
                    html.Button(
                        "View",
                        id={"type": "alert-view", "id": alert_id},
                        className="ghost-btn",
                    ),
                ],
                className="alert-actions",
            )
        )

    return html.Div(
        className="alert-row",
        children=[
            html.Span("!", className=f"alert-icon {severity}"),
            html.Div(body, className="alert-body"),
        ],
    )


@app.callback(
    Output("al-banner", "children"),
    Output("al-body", "children"),
    Input("tick", "n_intervals"),
    Input("al-station", "value"),
    Input("al-severity", "value"),
    Input("al-time", "value"),
    Input("al-refresh", "n_clicks"),
    Input({"type": "alert-view", "id": ALL}, "n_clicks"),
    Input("auth", "data"),
)
def alerts_page(_, station, severity, span, __, ___, ____):
    if not auth.signed_in():
        return empty(SIGNED_OUT_TEXT), empty(SIGNED_OUT_TEXT)

    pull_data(force=ctx.triggered_id == "al-refresh")
    alerts = get_alerts()

    filtered = []
    cutoff = 0
    if span == 24:
        cutoff = time.time() - 86400
    elif span == 168:
        cutoff = time.time() - 7 * 86400

    for alert in alerts:
        alert_station_id = alert_station(alert)
        alert_level = alert_severity(alert)
        timestamp = force_epoch(alert_timestamp(alert))

        if station != "all" and alert_station_id != str(station):
            continue
        if severity != "all" and alert_level != severity:
            continue
        if cutoff and timestamp and timestamp < cutoff:
            continue
        filtered.append(alert)

    critical = sum(alert_severity(a) == "critical" for a in alerts)
    warning = sum(alert_severity(a) == "warning" for a in alerts)
    info = sum(alert_severity(a) == "info" for a in alerts)

    banner_el = banner(
        "Database alerts",
        f"{len(alerts)} alert/event(s) returned",
        f"{critical} critical · {warning} warning · {info} info",
        [html.Span(updated_text())],
    )

    rows = [alert_row(dict(alert)) for alert in filtered]

    return banner_el, [
        html.Section(
            className="panel",
            children=[
                panel_heading("Alert list", f"Showing {len(filtered)} alert/event(s)"),
                rows if rows else empty("No alerts match the selected filters."),
            ],
        )
    ]


if __name__ == "__main__":
    try:
        app.run(
            host="0.0.0.0",
            port=PORT,
            debug=False,
            threaded=True,
        )
    finally:
        if DB is not None and hasattr(DB, "close"):
            DB.close()
