"""
timescale_adapter.py

Stores the finished readings from the data team's pipeline (data_pipeline.py)
in TimescaleDB and provides the queries the API/dashboard needs.

One row per reading in a single hypertable, `readings`. Column names match the
pipeline's JSON fields, except:
    pipeline "ts" -> column "time"      (TimescaleDB convention)
    pipeline "id" -> column "node_id"
Any field the pipeline passes through that isn't a column (e.g. a new sensor the
ESP starts sending, or a sensor sent under a different name such as "temp") is
kept in the JSONB column `extra`, so nothing is lost.

A NULL sensor column means the sensor was missing from the message OR the
pipeline rejected its value. A NULL calculated column (heat_index, aqi, ...)
means an input it needs was NULL. `flags` only holds notes the pipeline added
itself (currently: the message had no timestamp, so the receive time was used).

Duplicates are ignored: MQTT QoS 1 can deliver the same message twice, so
(node_id, time) is unique and repeat inserts are skipped, not errors.
"""
import json
import logging

import psycopg2
from psycopg2.extras import Json, execute_values

from config import TIMESCALEDB

log = logging.getLogger("timescale")

SENSOR_FIELDS = [
    "temperature", "humidity", "pressure", "pm25", "tvoc", "eco2", "co2",
    "battery_v", "lat", "lon", "altitude",
]
DERIVED_FIELDS = ["heat_index", "dew_point", "absolute_humidity", "aqi"]
NUMERIC_FIELDS = SENSOR_FIELDS + DERIVED_FIELDS

# pipeline keys that have their own column; everything else goes into `extra`
_COLUMN_KEYS = {"id", "ts", "aqi_category", "flags", "received_at", "source_topic", *NUMERIC_FIELDS}

_INSERT_COLUMNS = (["time", "node_id"] + NUMERIC_FIELDS
                   + ["aqi_category", "flags", "received_at", "source_topic", "extra"])

_INSERT_SQL = (f"INSERT INTO readings ({', '.join(_INSERT_COLUMNS)}) VALUES %s "
               f"ON CONFLICT (node_id, time) DO NOTHING RETURNING 1")

_CONNECTION_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)


def _number(value):
    """Numbers pass through; anything else (text, bools, dicts) becomes NULL."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def _check_fields(fields):
    """Column names can't be SQL parameters, so only allow known ones."""
    fields = list(fields) if fields else list(NUMERIC_FIELDS)
    unknown = [f for f in fields if f not in NUMERIC_FIELDS]
    if unknown:
        raise ValueError(f"Unknown field(s) {unknown}. Allowed: {NUMERIC_FIELDS}")
    return fields


class TimescaleDBAdapter:
    def __init__(self, db_config=None):
        self._db_config = db_config or TIMESCALEDB
        self._connect()
        self._init_tables()

    def _connect(self):
        self.conn = psycopg2.connect(**self._db_config)
        self.conn.autocommit = True

    def _run(self, fn):
        """Runs fn(cursor). If the database connection drops, reconnects once and retries.
        Safe for inserts too, because duplicate rows are ignored on conflict."""
        for attempt in (1, 2):
            try:
                with self.conn.cursor() as cur:
                    return fn(cur)
            except _CONNECTION_ERRORS:
                if attempt == 2:
                    raise
                log.warning("Database connection lost, reconnecting...")
                try:
                    self.conn.close()
                except Exception:
                    pass
                self._connect()

    def _init_tables(self):
        def init(cur):
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            except psycopg2.Error as e:
                log.warning("Could not enable the timescaledb extension: %s", e)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS readings (
                    time               TIMESTAMPTZ NOT NULL,
                    node_id            TEXT NOT NULL,
                    temperature        DOUBLE PRECISION,   -- degC
                    humidity           DOUBLE PRECISION,   -- %
                    pressure           DOUBLE PRECISION,   -- hPa
                    pm25               DOUBLE PRECISION,   -- ug/m3
                    tvoc               DOUBLE PRECISION,   -- ppb
                    eco2               DOUBLE PRECISION,   -- ppm (estimated)
                    co2                DOUBLE PRECISION,   -- ppm (measured)
                    battery_v          DOUBLE PRECISION,   -- V
                    lat                DOUBLE PRECISION,
                    lon                DOUBLE PRECISION,
                    altitude           DOUBLE PRECISION,   -- m
                    heat_index         DOUBLE PRECISION,   -- degC
                    dew_point          DOUBLE PRECISION,   -- degC
                    absolute_humidity  DOUBLE PRECISION,   -- g/m3
                    aqi                INTEGER,            -- 0-500, US EPA
                    aqi_category       TEXT,
                    flags              TEXT[] NOT NULL DEFAULT '{}',
                    received_at        TIMESTAMPTZ,
                    source_topic       TEXT,
                    extra              JSONB,
                    UNIQUE (node_id, time)
                );
            """)

            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';")
            self.timescale_enabled = cur.fetchone() is not None
            if self.timescale_enabled:
                cur.execute("SELECT create_hypertable('readings', 'time', if_not_exists => TRUE);")
            else:
                log.warning("TimescaleDB is not installed: 'readings' is a normal Postgres table. "
                            "Everything still works, just without hypertable performance.")
        self._run(init)

    def close(self):
        self.conn.close()

    # ----------------------------------------------------------
    # WRITE
    # ----------------------------------------------------------
    @staticmethod
    def _to_row(reading):
        if isinstance(reading, (bytes, str)):
            reading = json.loads(reading)
        if not reading.get("id") or not reading.get("ts"):
            raise ValueError("reading has no 'id' or 'ts'")
        extra = {k: v for k, v in reading.items() if k not in _COLUMN_KEYS}
        return (
            reading["ts"],
            str(reading["id"]),
            *[_number(reading.get(f)) for f in NUMERIC_FIELDS],
            reading.get("aqi_category"),
            [str(f) for f in (reading.get("flags") or [])],
            reading.get("received_at"),
            reading.get("source_topic"),
            Json(extra) if extra else None,
        )

    def insert_processed_reading(self, reading):
        """Insert one processed reading (a dict, or the raw JSON text/bytes from MQTT).
        Returns True if stored, False if it was a duplicate."""
        row = self._to_row(reading)
        inserted = self._run(lambda cur: execute_values(cur, _INSERT_SQL, [row], fetch=True))
        return len(inserted) == 1

    def insert_many(self, readings):
        """Insert a list of processed readings in one go. Returns how many were new."""
        rows = [self._to_row(r) for r in readings]
        if not rows:
            return 0
        inserted = self._run(lambda cur: execute_values(cur, _INSERT_SQL, rows, fetch=True, page_size=500))
        return len(inserted)

    def insert_from_jsonl(self, path, batch_size=500):
        """Backfill from the pipeline's final_readings.jsonl (one finished reading per line).
        Use it to fill a gap after the live ingest was stopped.
        Safe to run more than once: rows already in the database are skipped.
        Returns {"inserted", "duplicates", "bad_lines"}."""
        stats = {"inserted": 0, "duplicates": 0, "bad_lines": 0}
        batch = []

        def flush():
            if batch:
                new = self.insert_many(batch)
                stats["inserted"] += new
                stats["duplicates"] += len(batch) - new
                batch.clear()

        with open(path, encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    reading = json.loads(line)
                    self._to_row(reading)  # validate before batching
                except (json.JSONDecodeError, ValueError, AttributeError) as e:
                    stats["bad_lines"] += 1
                    log.warning("Skipping line %s of %s: %s", line_no, path, e)
                    continue
                batch.append(reading)
                if len(batch) >= batch_size:
                    flush()
        flush()
        return stats

    # ----------------------------------------------------------
    # QUERY
    # ----------------------------------------------------------
    def get_known_nodes(self, hours=24):
        """Node ids that sent at least one reading in the last `hours`."""
        def q(cur):
            cur.execute("""
                SELECT DISTINCT node_id FROM readings
                WHERE time > NOW() - %s * INTERVAL '1 hour'
            """, (hours,))
            return sorted(r[0] for r in cur.fetchall())
        return self._run(q)

    def get_latest_reading(self, node_id, hours=24):
        """Latest reading for a node within the last `hours`:
        {'node_id', 'timestamp' (unix seconds, 0 if none), 'time' (ISO),
         'sensors': {...}, 'derived': {...}, 'flags': [...], 'received_at', 'extra'}
        This is the newest row. Any value the pipeline rejected is None in it."""
        reading = {"node_id": node_id, "timestamp": 0, "sensors": {}, "derived": {}, "flags": []}
        columns = ["time"] + NUMERIC_FIELDS + ["aqi_category", "flags", "received_at", "extra"]

        def q(cur):
            cur.execute(f"""
                SELECT {', '.join(columns)} FROM readings
                WHERE node_id = %s AND time > NOW() - %s * INTERVAL '1 hour'
                ORDER BY time DESC LIMIT 1
            """, (node_id, hours))
            return cur.fetchone()
        row = self._run(q)
        if not row:
            return reading

        values = dict(zip(columns, row))
        reading.update({
            "timestamp": values["time"].timestamp(),
            "time": values["time"].isoformat(),
            "sensors": {f: values[f] for f in SENSOR_FIELDS},
            "derived": {**{f: values[f] for f in DERIVED_FIELDS}, "aqi_category": values["aqi_category"]},
            "flags": values["flags"] or [],
            "received_at": values["received_at"].isoformat() if values["received_at"] else None,
            "extra": values["extra"] or {},
        })
        return reading

    def get_history(self, node_id, hours=24):
        """Readings for one node in the last `hours`, oldest first. Each item has
        the same shape as get_latest_reading's return value."""
        columns = ["time"] + NUMERIC_FIELDS + ["aqi_category", "flags", "received_at", "extra"]

        def q(cur):
            cur.execute(f"""
                SELECT {', '.join(columns)} FROM readings
                WHERE node_id = %s AND time > NOW() - %s * INTERVAL '1 hour'
                ORDER BY time ASC
            """, (node_id, hours))
            return cur.fetchall()

        rows = self._run(q)
        out = []
        for row in rows:
            value = dict(zip(columns, row))
            out.append({
                "node_id": node_id,
                "timestamp": value["time"].timestamp(),
                "time": value["time"].isoformat(),
                "sensors": {f: value[f] for f in SENSOR_FIELDS},
                "derived": {**{f: value[f] for f in DERIVED_FIELDS}, "aqi_category": value["aqi_category"]},
                "flags": value["flags"] or [],
                "received_at": value["received_at"].isoformat() if value["received_at"] else None,
                "extra": value["extra"] or {},
            })
        return out

    def get_locations(self, hours=24):
        """Device locations. The readings topic carries no lat/lon (GPS is status
        only), so there is nothing to return yet; the Map page shows an empty
        state instead of erroring."""
        def q(cur):
            cur.execute("""
                SELECT DISTINCT node_id FROM readings
                WHERE time > NOW() - %s * INTERVAL '1 hour'
            """, (hours,))
            return [{"device_id": r[0], "lat": None, "lon": None} for r in cur.fetchall()]
        return self._run(q)

    def get_alerts(self, hours=24):
        """Alerts/events have no source yet — the team's ESP only publishes
        readings and diagnostics. Return [] so the Alerts page renders cleanly."""
        return []

    def query_history(self, node_id=None, hours=24, fields=None, bucket=None):
        """Time series for charts. Replaces the old query_bti_history.

        node_id  one node, or None for all nodes
        fields   list of numeric fields, default all of them
        bucket   None for every raw reading, or an interval like '5 minutes' / '1 hour'
                 to get averages per time bucket (much less data for long ranges)

        Returns [{'time': ISO, 'node_id': ..., '<field>': value, ...}, ...] oldest first.
        """
        fields = _check_fields(fields)
        where = "time > NOW() - %s * INTERVAL '1 hour'" + (" AND node_id = %s" if node_id else "")
        where_params = (hours, node_id) if node_id else (hours,)

        if bucket:
            bucket_fn = ("time_bucket(%s::interval, time)" if self.timescale_enabled
                         else "date_bin(%s::interval, time, TIMESTAMPTZ '2000-01-01')")
            selects = ", ".join(f"AVG({f})::double precision AS {f}" for f in fields)
            sql = (f"SELECT {bucket_fn} AS t, node_id, {selects} FROM readings "
                   f"WHERE {where} GROUP BY t, node_id ORDER BY t, node_id")
            params = (bucket, *where_params)
        else:
            sql = (f"SELECT time AS t, node_id, {', '.join(fields)} FROM readings "
                   f"WHERE {where} ORDER BY time, node_id")
            params = where_params

        def q(cur):
            cur.execute(sql, params)
            return [{"time": r[0].isoformat(), "node_id": r[1], **dict(zip(fields, r[2:]))}
                    for r in cur.fetchall()]
        return self._run(q)

    def query_metrics_summary(self, node_id, hours=1):
        """Average, min and max of every numeric field for one node over the last `hours`.
        Returns {} if there's no data, else {'count': n, '<field>': {'avg','min','max'}, ...}."""
        selects = ", ".join(
            f"AVG({f})::double precision, MIN({f})::double precision, MAX({f})::double precision"
            for f in NUMERIC_FIELDS)

        def q(cur):
            cur.execute(f"""
                SELECT COUNT(*), {selects} FROM readings
                WHERE node_id = %s AND time > NOW() - %s * INTERVAL '1 hour'
            """, (node_id, hours))
            return cur.fetchone()
        row = self._run(q)
        if not row or row[0] == 0:
            return {}
        summary = {"count": row[0]}
        for i, f in enumerate(NUMERIC_FIELDS):
            avg, lo, hi = row[1 + i * 3: 4 + i * 3]
            summary[f] = {"avg": avg, "min": lo, "max": hi}
        return summary

    def query_aqi_categories(self, node_id=None, hours=24):
        """How many readings fell in each AQI category, e.g. {'Good': 40, 'Moderate': 12}."""
        where = "time > NOW() - %s * INTERVAL '1 hour' AND aqi_category IS NOT NULL"
        params = (hours,)
        if node_id:
            where += " AND node_id = %s"
            params = (hours, node_id)

        def q(cur):
            cur.execute(f"SELECT aqi_category, COUNT(*) FROM readings WHERE {where} "
                        f"GROUP BY aqi_category ORDER BY COUNT(*) DESC", params)
            return {cat: n for cat, n in cur.fetchall()}
        return self._run(q)

    def query_flagged_readings(self, hours=24, limit=50):
        """Most recent readings that carry a note from the pipeline in `flags`.
        With data_pipeline.py that is currently only "ts: missing from sender, filled
        with receive time". It does not list readings where a value was rejected:
        those are stored as NULL without a note."""
        def q(cur):
            cur.execute("""
                SELECT time, node_id, flags FROM readings
                WHERE time > NOW() - %s * INTERVAL '1 hour' AND cardinality(flags) > 0
                ORDER BY time DESC LIMIT %s
            """, (hours, limit))
            return [{"time": r[0].isoformat(), "node_id": r[1], "flags": r[2]} for r in cur.fetchall()]
        return self._run(q)
