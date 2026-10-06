# Backend

The backend is the database side of the IoT monitoring system: it receives
finished readings over MQTT, stores them in TimescaleDB, and serves them (plus
the student login) to the dashboard.

Six files:

| File | One line |
|---|---|
| [`config.py`](#configpy) | Connection settings + the `get_db()` factory |
| [`timescale_adapter.py`](#timescale_adapterpy) | Schema, idempotent inserts, every read query |
| [`live_capture.py`](#live_capturepy) | Subscribes to the pipeline and writes rows |
| [`auth.py`](#authpy) | Student accounts, password hashing, sessions |
| [`seed_users.py`](#seed_userspy) | Loads the class list into `users` |
| [`time_queries.py`](#time_queriespy) | 12-line query latency probe |

**Not part of this directory:** `data_pipeline.py` is the upstream stage that
cleans readings and publishes them to `processed/+/readings`. This backend only
consumes that output — it never talks to the raw `nodes/#` topics.

---

## Data flow

```
  data_pipeline.py  ──▶  MQTT processed/<id>/readings
                                   │
                                   ▼
                          live_capture.py
                                   │  insert_processed_reading()
                                   ▼
                          timescale_adapter.py
                                   │
                                   ▼
                            TimescaleDB :5432
                            ┌──────────────┐
                            │  readings    │◀──── time_queries.py (probe)
                            │  users       │◀──── auth.py
                            └──────┬───────┘
                                   │ get_* queries
                                   ▼
                             weather_app.py
```

`config.py` supplies credentials to every box on the right.

---

## `config.py`

The single place connection settings are defined. Nothing else in the backend
hardcodes a host, port or credential.

```python
from config import get_db, TIMESCALEDB, required_env
```

| Name | Kind | What it is |
|---|---|---|
| `TIMESCALEDB` | dict | psycopg2 kwargs: `host`, `port`, `database`, `user`, `password` |
| `get_db()` | function | Returns a ready `TimescaleDBAdapter` (imports it lazily) |
| `required_env(name)` | function | Reads a required env var or raises `RuntimeError` |

### Why `required_env` exists

Non-sensitive defaults are inlined (`localhost`, `5432`, `iot_trust`,
`iot_user`), but **the password has no default**. `required_env` fails loudly
instead of falling back to a literal, so a credential can never be committed by
accident:

```
RuntimeError: TS_PASSWORD is not set. Set it in the environment before running,
e.g.  export TS_PASSWORD=...  (and add that to your shell profile or systemd
unit so it survives a restart).
```

Because `TIMESCALEDB` is built at import time, `import config` itself fails if
`TS_PASSWORD` is missing. That is deliberate — fail closed, at startup, with a
message you can act on.

### Environment

| Var | Default | Required |
|---|---|---|
| `TS_HOST` | `localhost` | no |
| `TS_PORT` | `5432` | no |
| `TS_DB` | `iot_trust` | no |
| `TS_USER` | `iot_user` | no |
| `TS_PASSWORD` | — | **yes** |

---

## `timescale_adapter.py`

`class TimescaleDBAdapter` — the only code in the backend that speaks SQL.

```python
from config import get_db
db = get_db()
```

### Schema

`_init_tables()` runs on construction: enables the `timescaledb` extension, then
`CREATE TABLE IF NOT EXISTS readings (...)` and `create_hypertable('readings',
'time')`.

One row per reading, partitioned by `time` (TIMESTAMPTZ). Two renames from the
pipeline's JSON: `ts` → `time`, `id` → `node_id`.

| Group | Columns |
|---|---|
| Identity | `time`, `node_id`, `UNIQUE (node_id, time)` |
| Sensors (11) | `temperature` `humidity` `pressure` `pm25` `tvoc` `eco2` `co2` `battery_v` `lat` `lon` `altitude` |
| Derived (4) | `heat_index` `dew_point` `absolute_humidity` `aqi` |
| Category | `aqi_category` |
| Lineage | `flags TEXT[]`, `received_at`, `source_topic`, `extra JSONB` |

**NULL semantics** — three distinct meanings, not one:

- a **sensor** column is NULL → the field was missing *or* failed range validation
- a **derived** column is NULL → one of its inputs was NULL (never imputed)
- `extra` is NULL → nothing extra; a fully-mapped reading

Any pipeline key that is not a column is preserved in `extra` JSONB, so new
sensors need no `ALTER TABLE`.

### Writes

| Method | Returns | Notes |
|---|---|---|
| `insert_processed_reading(r)` | `True` stored / `False` duplicate | accepts dict, JSON str, or MQTT bytes |
| `insert_many(readings)` | count of new rows | `execute_values`, `page_size=500` |
| `insert_from_jsonl(path)` | `{inserted, duplicates, bad_lines}` | backfill / replay |

All three use `ON CONFLICT (node_id, time) DO NOTHING`. That is what makes the
pipeline safe under MQTT **QoS 1** (at-least-once) — duplicate deliveries are
skipped, not errors, so every write path is replayable.

### Reads

| Method | Returns | Used for |
|---|---|---|
| `get_known_nodes(hours)` | sorted node ids | the node registry |
| `get_latest_reading(node, hours)` | `{node_id, timestamp, sensors, derived, flags, received_at, extra}` | latest snapshot |
| `get_history(node, hours)` | list of the same shape, oldest first | trend charts |
| `get_locations(hours)` | `[{device_id, lat, lon}]` | **stub** — lat/lon always `None` |
| `get_alerts(hours)` | `[]` | **stub** — no alert source yet |
| `query_history(..., bucket=...)` | time series, optional `time_bucket()` averages | analysis |
| `query_metrics_summary(node, hours)` | `{count, <field>: {avg, min, max}}` | summary stats |
| `query_aqi_categories(...)` | `{category: count}` | AQI distribution |
| `query_flagged_readings(...)` | `[{time, node_id, flags}]` | readings the pipeline annotated |

### Resilience

- **`_run(fn)`** executes a statement and, if the connection drops, reconnects
  **once** and retries. Safe even for inserts because duplicates are ignored.
- **`autocommit = True`** — the adapter is shared across Flask's threads.
- **Graceful degradation**: if the `timescaledb` extension is unavailable it
  warns, sets `timescale_enabled = False`, and `query_history` falls back from
  `time_bucket()` to `date_bin()`. Everything still works on plain PostgreSQL.
- **`_check_fields()`** whitelists column names before they are interpolated
  into SQL — field names can never be parameterised.

---

## `live_capture.py`

The ingest process. One class, one job: turn each MQTT message into a row.

```bash
python3 live_capture.py
```

```python
from live_capture import LiveIngestor
```

`class LiveIngestor`

| Piece | Behaviour |
|---|---|
| `_on_connect` | subscribes `MQTT_TOPIC` at **QoS 1** |
| `_on_message` | `json.loads` → `_ingest_reading()`; any exception is caught and counted |
| `_ingest_reading` | falls back to the topic's device id if `id` is missing, then `insert_processed_reading()` |
| `_extract_node` | `processed/<device-id>/readings` → `<device-id>` |
| `run()` | connect, `loop_start()`, sleep until Ctrl+C, then disconnect and close the DB |
| `stats` | `{"stored", "duplicates", "dropped_bad_json", "failed"}` |

### Environment

Uses the **same variable names as `data_pipeline.py`**, so one export
configures both ends of the handoff:

| Var | Default |
|---|---|
| `MQTT_HOST` | `127.0.0.1` |
| `MQTT_PORT` | `1883` |
| `MQTT_USERNAME` | unset → connect anonymously |
| `MQTT_PASSWORD` | unset |
| `MQTT_TOPIC` | `processed/+/readings` |

Credentials are deliberately never hardcoded — set them in the environment (or
a systemd unit / `.env`).

### Two things to know before you rely on it

1. **Readings published while this process is stopped are lost.** It uses a
   client id derived from the topic + PID with a clean session, so the broker
   queues nothing between runs. This is intentional (no stale-session
   confusion), and it is recoverable:
   `get_db().insert_from_jsonl("final_readings.jsonl")` replays the gap
   idempotently.
2. **QoS 1 means duplicates are normal.** `stored` and `duplicates` are counted
   separately for exactly this reason.

---

## `auth.py`

Student login for the dashboard. One row per student in the **same**
TimescaleDB instance as `readings` — no separate session store, no extra
service.

```python
import auth
```

### Module constants

| Constant | Source | Notes |
|---|---|---|
| `PASSWORD_PREFIX`, `PASSWORD_SUFFIX` | `required_env("PW_PREFIX")`, `required_env("PW_SUFFIX")` | the default-password scheme |
| `MIN_PASSWORD_LENGTH` | `8` | |
| `SESSION_STUDENT_NUMBER` | `"student_number"` | Flask session key |
| `SESSION_FULL_NAME` | `"full_name"` | Flask session key |
| `SECRET_KEY_ENV` | `"SECRET_KEY"` | env var for the session signing key |
| `SECRET_KEY_FILE` | `".dashboard_secret_key"` | on-disk fallback (gitignore this) |

The prefix/suffix are read from the environment **because usernames are bare
student numbers** — anyone who knows the pattern can derive any account's first
password, so it must not live in source.

### `secret_key()`

Resolution order, used to sign the Flask session cookie:

1. `SECRET_KEY` environment variable ← set this in production
2. contents of `.dashboard_secret_key` beside the module
3. `secrets.token_hex(32)`, written to that file with `chmod 0600`

> If step 3 has to fire on every start (file unwritable), every process signs a
> different key and users are logged out on restart.

### `class UsersAdapter`

Constructs with an optional `db_config` (defaults to `TIMESCALEDB`), connects
with `autocommit=True`, and creates the table on first use:

| Column | Type | Notes |
|---|---|---|
| `student_number` | `TEXT PRIMARY KEY` | this **is** the username |
| `password_hash` | `TEXT NOT NULL` | PBKDF2, never plaintext |
| `first_names`, `surname`, `initials` | `TEXT` | from the class list |
| `using_default_pw` | `BOOLEAN NOT NULL DEFAULT TRUE` | records *whether*, never *what* |
| `created_at`, `updated_at`, `last_login_at` | `TIMESTAMPTZ` | |

| Method | Behaviour |
|---|---|
| `get(number)` | record dict, or `None` |
| `verify(number, password)` | record on match, else `None` — **identical result for wrong password and unknown user** |
| `set_password(number, pw, still_default=False)` | `UPDATE … RETURNING student_number` |
| `record_login(number)` | sets `last_login_at = NOW()` |
| `count()` | account total |
| `seed(students, reset_passwords=False)` | `ON CONFLICT DO NOTHING` by default; with `reset_passwords=True` it overwrites |

`get_users()` returns a module-level shared adapter (or `None` on failure, so
the login page still renders when the database is down).

### Module functions

```python
default_password(student_number)   # PW_PREFIX + number + PW_SUFFIX
clean_student_number(value)        # trims and validates digits-only; None otherwise
secret_key()                       # see above
```

**Session helpers** — thin wrappers over Flask's `session`:

```python
current_student_number()   # session.get("student_number"), None if signed out
signed_in()                # bool(current_student_number())  ← the guard
display_name()             # full name, falling back to the student number
account_count()
password_state()           # "Still the default one" / "Changed from the default"
sign_in(username, pw)      # (student_number, None) or (None, error_message)
sign_out()                 # pops both session keys
change_password(cur, new, confirm)   # (True, None) or (False, message)
```

### Security properties worth knowing before you touch this file

| Property | How |
|---|---|
| Hashing | `werkzeug.security.generate_password_hash` → PBKDF2-SHA256. Only the hash is stored |
| No user enumeration | `verify()` returns the same thing for a wrong password and an unknown number; the UI shows one message for both |
| No browser sign-up | `seed_users.py` reads the class list only — there is no path to create an account from the web |
| Seed is idempotent | `ON CONFLICT DO NOTHING`, so re-running never resets a changed password |
| Session transport | server-signed cookie; no token in the URL, so a shared link grants nothing |
| Password policy | min 8 chars, must match confirm, must differ from current, current must verify. No lockout, no rate limiting |
| Telemetry writes | none — this module only ever touches `users` |

---

## `seed_users.py`

CLI that loads the class list into `users`.

```bash
python3 seed_users.py                     # add missing accounts
python3 seed_users.py --show              # list what is in the table
python3 seed_users.py --reset-passwords   # everybody back to the default
```

| Flag | Default | Effect |
|---|---|---|
| `--class-list` | `class_list.csv` beside the script | CSV path |
| `--show` | off | print accounts and exit, no writes |
| `--reset-passwords` | off | overwrite every password with the default |

| Function | What it does |
|---|---|
| `read_class_list(path)` | `csv.DictReader` with `utf-8-sig` (BOM-tolerant); skips rows whose `student_number` fails `clean_student_number()`; returns `[{student_number, first_names, surname, initials}]` |
| `main()` | argparse → `auth.get_users()` → either `--show` or `users.seed(...)`; returns `0`/`1` |

Exit code `1` if the database is unreachable or no student numbers parse.

`class_list.csv` exists as CSV rather than the original `.xlsx` specifically so
the Pi does not need `openpyxl`.

**Safe to re-run.** Seeding only inserts rows that do not exist; a password a
student has already changed is never overwritten unless you pass
`--reset-passwords` explicitly.

---

## `time_queries.py`

A throwaway latency probe — twelve lines, no functions, no CLI. Prints wall
clock for three read paths:

```bash
python3 time_queries.py
```

```
known_nodes       12  ms  -> 2 results
history           31  ms  -> 148 results
latest node1       4  ms  -> 1 results
```

Calls `get_known_nodes(hours=1)`, `query_history(hours=1)` and
`get_latest_reading("node1", hours=1)`, then `db.close()`.

Useful as a smoke test after a schema or index change, and as a sanity check
that `TS_PASSWORD` and the container are actually reachable.

---

## Running the backend

Order matters only for the first run.

```bash
# 1. environment (once per shell / profile)
export TS_PASSWORD='...'
export PW_PREFIX='...'
export PW_SUFFIX='...'

# 2. database up
docker start iot_timescaledb          # or docker compose up -d
docker exec -it iot_timescaledb pg_isready -U iot_user -d iot_trust

# 3. accounts (first time only)
python3 seed_users.py                 # idempotent — safe to re-run

# 4. ingest + dashboard
python3 live_capture.py               # terminal 1
python3 weather_app.py                # terminal 2, :5000

# 5. verify
python3 time_queries.py               # should print three latencies
```

## Dependencies

From `requirements.txt`:

| Package | Used by |
|---|---|
| `paho-mqtt>=2.0` | `live_capture.py` |
| `psycopg2-binary>=2.9` | `timescale_adapter.py`, `auth.py` |
| `dash>=2.14`, `plotly>=5.18` | `weather_app.py` (consumer of this backend) |
| `werkzeug>=3.0` | `auth.py` — password hashing and PBKDF2 |

Standard library elsewhere: `argparse`, `csv`, `json`, `logging`, `os`, `time`.

## Module graph

```
config.py  (required_env, TIMESCALEDB, get_db)
   │
   ├── timescale_adapter.py   TimescaleDBAdapter  ──┐
   │                                                 │
   ├── live_capture.py         LiveIngestor ─────────┤
   │                                                 ├──▶ TimescaleDB :5432
   ├── auth.py                 UsersAdapter ─────────┤
   │                                                 │
   └── seed_users.py          CLI ──▶ auth ──────────┤
                                                     │
      time_queries.py         probe ──▶ config ──────┘
```

No cycles. `config.py` imports nothing from this directory at module level
(`get_db()` imports `timescale_adapter` lazily, inside the function).

