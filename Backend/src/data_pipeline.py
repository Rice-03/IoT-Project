"""
data_pipeline.py

The data team's whole pipeline in one file:

    MQTT in -> Stage 1 (receive) -> Stage 2a (structure) -> Stage 2b (values)
            -> Stage 3 (calculations) -> Stage 4 (publish to backend)

Each stage below is the team member's original code, copied as written, and
marked with a banner showing where it starts and ends. Stage 4 at the bottom
is the only new code: it connects the stages together.

HOW TO RUN
    Live, against the network team's broker:
        python data_pipeline.py --host <broker address>
        python data_pipeline.py --host <address> --username <u> --password <p>
        (or set MQTT_HOST / MQTT_USERNAME / MQTT_PASSWORD environment variables)

    Offline, on a saved file (runs 2a -> 2b -> 3 using each stage's own
    process_file function):
        python data_pipeline.py --offline raw_readings.jsonl

    Stage 3's own self-tests:
        python data_pipeline.py --self-test

OUTPUT
    Finished readings are published to  processed/<device-id>/readings
    and saved to                         final_readings.jsonl
    Rejected readings are saved to       rejected_readings.jsonl (with reason)
    Everything received is saved to      raw_readings.jsonl (by Stage 1)

WHAT WAS CHANGED IN THE ORIGINAL CODE (and why)
    Only what was needed for four files to live in one file. No logic changed.
    1. process_file() existed in 2a, 2b and 3. In one file, the last would
       overwrite the other two, so they are renamed process_file_2a,
       process_file_2b and process_file_3.
    2. The "if __name__ == '__main__':" blocks that ran code (Stage 1, 2a,
       2b's second one, 3) are removed, otherwise all of them would run at
       once. One entry point at the bottom replaces them. Stage 2b's first
       block is kept as written: it only holds test notes and does nothing.
    3. Line endings normalised from Windows (CRLF) to LF. No visible change.
"""


##############################################################################
#
#   STAGE 1: MQTT CONNECTION
#   Original file: mqtt_consumer.py
#   Owner: ______________________
#
##############################################################################

"""
mqtt_consumer.py  (Stage 3.1: MQTT connection)

What this stage does:
  1. Connects to the broker and subscribes to sensors/+/readings (every node)
  2. Turns each message from raw bytes into a Python dict
  3. Drops messages that aren't valid JSON (logged, never crashes)
  4. Adds two fields: received_at (when WE got it) and source_topic
  5. Hands the dict to handle_reading(), and saves it to raw_readings.jsonl

What this stage does NOT do:
  No checking of fields or values. A reading with a missing id or a
  temperature of 250 is passed through untouched. That is the cleaning
  stage's job (2a/2b), not this one.

HANDOFF FOR THE CLEANING TEAM:
  Replace the body of handle_reading() below with a call to your function.
  Or, if you'd rather work offline, just use raw_readings.jsonl: one
  reading per line, exactly what this stage produced.

Run (with fake_broker.py already running in another terminal):
    python mqtt_consumer.py
"""
import json
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

BROKER_HOST = "127.0.0.1"
BROKER_PORT = 1883
TOPIC = "nodes/+/readings"
OUTPUT_FILE = "raw_readings.jsonl"


def handle_reading(reading):
    """
    >>> THE HANDOFF POINT <<<
    Receives one parsed reading (a dict). Right now it just prints it.
    Cleaning team: replace this with a call to your cleaning function, e.g.
        cleaned = clean_reading(reading)
    """
    print(f"[stage 1] -> {reading}")


class MQTTConsumer:
    def __init__(self, handler=handle_reading, host=BROKER_HOST, port=BROKER_PORT,
                 topic=TOPIC, output_file=OUTPUT_FILE, client_id="data-team-stage1"):
        self.handler = handler
        self.host = host
        self.port = port
        self.topic = topic
        self.output_file = output_file
        self.stats = {"received": 0, "dropped_bad_json": 0, "handler_errors": 0}

        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            print(f"[stage 1] connected to {self.host}:{self.port}, listening on {self.topic}")
            client.subscribe(self.topic, qos=1)
        else:
            print(f"[stage 1] connection refused: {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            print(f"[stage 1] lost connection ({reason_code}), reconnecting automatically...")

    def _on_message(self, client, userdata, msg):
        """Handle an incoming MQTT message: decode, validate as JSON dict,
        annotate with receive metadata, persist to raw file, and pass to handler."""
        try:
            reading = json.loads(msg.payload.decode("utf-8"))
            if not isinstance(reading, dict):
                raise ValueError("payload is JSON but not an object")
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
            self.stats["dropped_bad_json"] += 1
            print(f"[stage 1] DROPPED unparseable message: {e}")
            return

        reading["received_at"] = datetime.now(timezone.utc).isoformat()
        reading["source_topic"] = msg.topic
        self.stats["received"] += 1

        if self.output_file:
            with open(self.output_file, "a") as f:
                f.write(json.dumps(reading) + "\n")

        try:
            self.handler(reading)
        except Exception as e:
            # A bug further down the pipeline must never kill the connection.
            self.stats["handler_errors"] += 1
            print(f"[stage 1] next stage raised an error, skipping this reading: {e}")

    def start(self, retries=10):
        for attempt in range(1, retries + 1):
            try:
                self.client.connect(self.host, self.port, keepalive=60)
                self.client.loop_start()
                return True
            except (ConnectionRefusedError, OSError):
                print(f"[stage 1] broker not reachable (attempt {attempt}/{retries}). "
                      f"Is fake_broker.py running?")
                time.sleep(2)
        return False

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

# -------------------- END OF STAGE 1 --------------------


##############################################################################
#
#   STAGE 2a: STRUCTURAL VALIDATION
#   Original file: clean_structure.py
#   Owner: ______________________
#
##############################################################################

"""
clean_structure.py

Stage 2a: Structural Validation

This stage receives one reading from Stage 1 and checks its structure.

Checks performed:
    1. The reading must be a dictionary.
    2. "id" must exist and must be a string.
    3. "ts" must exist and must be a valid ISO timestamp.
    4. Every sensor field that is present must contain either:
       - a number, or
       - null

If a sensor has the wrong type, that sensor value is changed to None.
The rest of the reading is kept.

Physical/range validation is NOT done here.
That belongs to Stage 2b: Value Validation.

Input:
    raw_readings.jsonl

Output:
    structure_ok.jsonl

Rejected readings:
    rejected_readings.jsonl
"""

import json
from datetime import datetime


# ---------------------------------------------------------
# Sensor fields defined by the project.
# ---------------------------------------------------------
SENSOR_FIELDS = {
    "temperature",
    "humidity",
    "pressure",
    "pm25",
    "tvoc",
    "eco2",
    "co2",
    "battery_v",
    "lat",
    "lon",
    "altitude",
}


def clean_structure(reading):
    """
    Validate the structure of one sensor reading.

    Parameters:
        reading (dict):
            One reading received from Stage 1.

    Returns:
        dict:
            Structurally valid reading.

        None:
            If a required structural field is missing
            or invalid.
    """

    # -----------------------------------------------------
    # 1. Reading must be a dictionary.
    # -----------------------------------------------------
    if not isinstance(reading, dict):
        print("[2a] REJECTED: reading is not a dictionary")
        return None

    # -----------------------------------------------------
    # 2. ID must exist.
    # -----------------------------------------------------
    if "id" not in reading:
        print("[2a] REJECTED: missing 'id'")
        return None

    # ID must be a string.
    if not isinstance(reading["id"], str):
        print("[2a] REJECTED: 'id' must be a string")
        return None

    # Empty ID is also structurally invalid.
    if not reading["id"].strip():
        print("[2a] REJECTED: 'id' cannot be empty")
        return None

    # -----------------------------------------------------
    # 3. Timestamp must exist.
    # -----------------------------------------------------
    if "ts" not in reading:
        print("[2a] REJECTED: missing 'ts'")
        return None

    # Timestamp must be a string.
    if not isinstance(reading["ts"], str):
        print("[2a] REJECTED: 'ts' must be a string")
        return None

    # -----------------------------------------------------
    # 4. Timestamp must be valid.
    # -----------------------------------------------------
    try:
        timestamp = reading["ts"].replace("Z", "+00:00")
        datetime.fromisoformat(timestamp)

    except (ValueError, TypeError):
        print("[2a] REJECTED: invalid 'ts' timestamp")
        return None

    # -----------------------------------------------------
    # Make a copy so that the original reading is not
    # modified.
    # -----------------------------------------------------
    cleaned_reading = reading.copy()

    # -----------------------------------------------------
    # 5. Validate sensor field types.
    #
    # Missing sensor fields are allowed.
    #
    # Existing sensor fields must contain:
    #   - a number
    #   - or None
    # -----------------------------------------------------
    for field in SENSOR_FIELDS:

        if field not in cleaned_reading:
            continue

        value = cleaned_reading[field]

        # None represents a missing sensor reading.
        if value is None:
            continue

        # Numbers are valid.
        #
        # bool is explicitly excluded because Python treats
        # True and False as integers.
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            continue

        # Any other type is structurally invalid for that
        # sensor field.
        print(
            f"[2a] INVALID TYPE: '{field}' "
            f"was {type(value).__name__}; changed to null"
        )

        cleaned_reading[field] = None

    # -----------------------------------------------------
    # Reading passed structural validation.
    # -----------------------------------------------------
    return cleaned_reading


def get_rejection_reason(reading):
    """
    Determine why a reading should be rejected.

    This function is used only for producing useful
    information in rejected_readings.jsonl.
    """

    if not isinstance(reading, dict):
        return "reading is not a dictionary"

    if "id" not in reading:
        return "missing 'id'"

    if not isinstance(reading["id"], str):
        return "'id' must be a string"

    if not reading["id"].strip():
        return "'id' cannot be empty"

    if "ts" not in reading:
        return "missing 'ts'"

    if not isinstance(reading["ts"], str):
        return "'ts' must be a string"

    try:
        datetime.fromisoformat(
            reading["ts"].replace("Z", "+00:00")
        )
    except (ValueError, TypeError):
        return "invalid 'ts' timestamp"

    return "unknown structural error"


def process_file_2a(
    input_file="raw_readings.jsonl",
    output_file="structure_ok.jsonl",
    rejected_file="rejected_readings.jsonl",
):
    """
    Process every reading in the input JSONL file.

    Valid readings are written to structure_ok.jsonl.

    Structurally rejected readings are written to
    rejected_readings.jsonl with their rejection reason.
    """

    kept = 0
    rejected = 0
    invalid_sensor_values = 0

    # -----------------------------------------------------
    # Open input and output files.
    # -----------------------------------------------------
    with open(input_file, "r", encoding="utf-8") as infile, \
         open(output_file, "w", encoding="utf-8") as outfile, \
         open(rejected_file, "w", encoding="utf-8") as rejectfile:

        # -------------------------------------------------
        # Process one JSON object per line.
        # -------------------------------------------------
        for line_number, line in enumerate(infile, start=1):

            # Ignore blank lines.
            if not line.strip():
                continue

            # -------------------------------------------------
            # Parse JSON.
            # -------------------------------------------------
            try:
                reading = json.loads(line)

            except json.JSONDecodeError as error:
                rejected += 1

                rejectfile.write(
                    json.dumps({
                        "line": line_number,
                        "reason": f"invalid JSON: {error.msg}",
                        "raw": line.strip(),
                    }) + "\n"
                )

                continue

            # -------------------------------------------------
            # Check required structural fields first.
            # -------------------------------------------------
            reason = get_rejection_reason(reading)

            if reason != "unknown structural error":

                # If get_rejection_reason returned a real
                # structural problem, reject the reading.
                rejected += 1

                rejectfile.write(
                    json.dumps({
                        "line": line_number,
                        "reason": reason,
                        "reading": reading,
                    }) + "\n"
                )

                continue

            # -------------------------------------------------
            # Run the actual structural cleaning.
            # -------------------------------------------------
            cleaned = clean_structure(reading)

            if cleaned is None:
                rejected += 1

                rejectfile.write(
                    json.dumps({
                        "line": line_number,
                        "reason": "structural validation failed",
                        "reading": reading,
                    }) + "\n"
                )

                continue

            # -------------------------------------------------
            # Count sensor values that were changed to null.
            # -------------------------------------------------
            for field in SENSOR_FIELDS:

                if field in reading and field in cleaned:

                    if (
                        reading[field] is not None
                        and cleaned[field] is None
                    ):
                        invalid_sensor_values += 1

            # -------------------------------------------------
            # Write cleaned reading.
            # -------------------------------------------------
            kept += 1

            outfile.write(
                json.dumps(cleaned) + "\n"
            )

    # ---------------------------------------------------------
    # Print summary.
    # ---------------------------------------------------------
    print()
    print("[2a] Structural validation complete")
    print("-----------------------------------")
    print(f"[2a] Readings kept:              {kept}")
    print(f"[2a] Readings rejected:          {rejected}")
    print(f"[2a] Invalid sensor values nulled: {invalid_sensor_values}")
    print(f"[2a] Output:                     {output_file}")
    print(f"[2a] Rejected:                   {rejected_file}")

# -------------------- END OF STAGE 2a --------------------


##############################################################################
#
#   STAGE 2b: VALUE VALIDATION
#   Original file: clean_values.py
#   Owner: ______________________
#
##############################################################################

import json

VALUE_RANGES = {
    "temperature": (-40, 60),
    "humidity": (0, 100),
    "pressure": (870, 1085),
    "pm25": (0, 1000),
    "tvoc": (0, 65000),
    "eco2": (400, 65000),
    "co2": (300, 40000),
    "battery_v": (2.5, 4.5),
    "lat": (-90, 90),
    "lon": (-180, 180),
}

# Check sensor values against their allowed ranges.
def clean_values(reading):
    """
    Values outside their allowed range are changed to None.
    Other values in the reading are kept.
    """

    if not isinstance(reading, dict):
        return None

    cleaned_reading = reading.copy()

    for field, (minimum, maximum) in VALUE_RANGES.items():

        if field not in cleaned_reading:
            continue

        value = cleaned_reading[field]

        if value is None:
            continue

        if value < minimum or value > maximum:
            cleaned_reading[field] = None

    return cleaned_reading


if __name__ == "__main__":
    # 1ST TEST 
    """
    test_reading = {
        "id": "device-01",
        "ts": "2026-09-26T10:00:00Z",
        "temperature": 250,
        "humidity": 55,
        "pm25": -10,
        "pressure": 1013
    }

    """
#PASSED 

#2ND TEST
    """
    test_reading = {
    "id": "device-01",
    "ts": "2026-09-26T10:00:00Z",
    "temperature": -40,
    "humidity": 100,
    "pressure": 870,
    "pm25": 1000
    }
    """
    #PASSED 

    #both manual tests passed 


def process_file_2b(
    input_file="structure_ok.jsonl",
    output_file="clean_readings.jsonl"
):
    kept = 0
    invalid_values = 0  
    with open(input_file, "r", encoding="utf-8") as infile, \
        open(output_file, "w", encoding="utf-8") as outfile:  
        for line_number, line in enumerate(infile, start=1):

            if not line.strip():
                continue

            try:
                reading = json.loads(line)

            except json.JSONDecodeError:
                print(f"[2b] Skipping invalid JSON on line {line_number}")
                continue

            cleaned = clean_values(reading)

            if cleaned is None:
                print(f"[2b] Skipping invalid reading on line {line_number}")
                continue

            for field in VALUE_RANGES:

                if (
                    field in reading
                    and field in cleaned
                    and reading[field] is not None
                    and cleaned[field] is None
                ):
                    invalid_values += 1

            outfile.write(
            json.dumps(cleaned) + "\n"
            )

            kept += 1               

    print()
    print("[2b] Value validation complete")
    print(" ")
    print(f"[2b] Readings processed:       {kept}")
    print(f"[2b] Invalid values nulled:    {invalid_values}")
    print(f"[2b] Output:                    {output_file}")

# -------------------- END OF STAGE 2b --------------------


##############################################################################
#
#   STAGE 3: CALCULATIONS
#   Original file: calculations.py
#   Owner: ______________________
#
##############################################################################

"""
Reading from Stage 2b:
head_index (needs temperature, humidity) -> degrees C
dew_point (needs temperature, humidity) -> degrees C
absolute_humidity (needs temperature, humidity) -> g/m3
aqi (needs pm25) -> 0-500 index
aqi_category (derived from aqi) -> label (bonus field)
"""

import json
import math

def _heat_index_fahrenheit(t_f, rh):
    hi_simple = 0.5 * (t_f + 61.0 + ((t_f - 68.0)) * 1.2 + (rh * 0.094))

    average = (hi_simple + t_f) / 2.0
    if average < 80.0:
        return hi_simple

    hi = (
        -42.379
        + 2.04901523 * t_f
        + 10.14333127 * rh
        - 0.22475541 * t_f * rh
        - 0.00683783 * t_f * t_f
        - 0.05481717 * rh * rh
        + 0.00122874 * t_f * t_f * rh
        + 0.00085282 * t_f * rh * rh
        - 0.00000199 * t_f * t_f * rh * rh
    )

    if rh < 13 and 80 <= t_f <= 112:
        adjustment = ((13 - rh) / 4.0) * math.sqrt((17 - abs(t_f - 95.0)) / 17.0)
        hi -= adjustment
    elif rh > 85 and 80 <= t_f <= 87:
        adjustment = ((rh - 85) / 10.0) * ((87 - t_f) / 5.0)
        hi += adjustment

    return hi

def heat_index(temperature, humidity):
    if temperature is None or humidity is None:
        return None

    t_f = temperature * 9.0 / 5.0 + 32.0
    hi_f = _heat_index_fahrenheit(t_f, humidity)
    hi_c = (hi_f - 32.0) * 5.0 / 9.0
    return round(hi_c, 2)

def dew_point(temperature, humidity):
    if temperature is None or humidity is None:
        return None

    if humidity <= 0:
        return None

    a, b = 17.27, 237.7
    alpha = (a * temperature) / (b + temperature) + math.log(humidity / 100.0)
    dp = (b * alpha) / (a - alpha)
    return round(dp, 2)

def absolute_humidity(temperature, humidity):
    if temperature is None or humidity is None:
        return None

    saturation_vapor_pressure = 6.112 * math.exp(
        (17.67 * temperature) / (temperature + 243.5)
    )
    ah = (saturation_vapor_pressure * humidity * 2.1674) / (273.15 + temperature)
    return round(ah, 2)

PM25_BREAKPOINTS = [
    (0.0, 9.0, 0, 50),        # Good
    (9.1, 35.4, 51, 100),     # Moderate
    (35.5, 55.4, 101, 150),   # Unhealthy for Sensitive Groups
    (55.5, 125.4, 151, 200),  # Unhealthy
    (125.5, 225.4, 201, 300),  # Very Unhealthy
    (225.5, 325.4, 301, 500),  # Hazardous
]

def aqi_pm25(pm25):
    if pm25 is None:
        return None

    if pm25 < 0:
        return None

    cp = math.floor(pm25 * 10) / 10.0

    if cp > PM25_BREAKPOINTS[-1][1]:
        return 500

    for bp_lo, bp_hi, aqi_lo, aqi_hi in PM25_BREAKPOINTS:
        if bp_lo <= cp <= bp_hi:
            aqi = ((aqi_hi - aqi_lo) / (bp_hi - bp_lo)) * (cp - bp_lo) + aqi_lo
            return round(aqi)

    return None

def aqi_category(aqi):
    if aqi is None:
        return None
    if aqi <= 50:
        return "Good"
    if aqi <= 100:
        return "Moderate"
    if aqi <= 150:
        return "Unhealthy for Sensitive Groups"
    if aqi <= 200:
        return "Unhealthy"
    if aqi <= 300:
        return "Very Unhealthy"
    return "Hazardous"

def add_calculations(reading):
    if not isinstance(reading, dict):
        return None

    result = reading.copy()

    temperature = result.get("temperature")
    humidity = result.get("humidity")
    pm25 = result.get("pm25")

    result["heat_index"] = heat_index(temperature, humidity)
    result["dew_point"] = dew_point(temperature, humidity)
    result["absolute_humidity"] = absolute_humidity(temperature, humidity)

    aqi = aqi_pm25(pm25)
    result["aqi"] = aqi
    result["aqi_category"] = aqi_category(aqi)

    return result

def process_file_3(
        input_file="clean_readings.jsonl",
        output_file="final_readings.jsonl",
):
    processed = 0

    with open(input_file, "r", encoding="utf-8") as infile, \
        open(output_file, "w", encoding="utf-8") as outfile:

        for line_number, line in enumerate(infile, start=1):
            if not line.strip():
                continue
            try:
                reading = json.loads(line)
            except json.JSONDecodeError:
                print(f"[3] Skipping invalid JSON on line {line_number}")
                continue

            enriched = add_calculations(reading)

            if enriched is None:
                print(f"[3] Skipping invalid reading on line {line_number}")
                continue

            outfile.write(json.dumps(enriched) + "\n")
            processed += 1

    print()
    print("[3] Calculations complete")
    print(f"[3] Readings processed: {processed}")
    print(f"[3] Output: {output_file}")

def _run_self_tests():
    hi = heat_index(32.222, 50)
    hi_f = hi * 9.0 / 5.0 + 32.0
    assert abs(hi_f - 94.6) < 1.0, f"heat_index off: got {hi_f}F, expected ~94.6F"

    hi_cool = heat_index(10, 50)
    assert abs(hi_cool - 10) < 3, f"cool heat_index should stay near air temp, got {hi_cool}"

    dp = dew_point(25, 50)
    assert abs(dp - 13.9) < 0.2, f"dew_point off: got {dp}, expected ~13.9"

    ah = absolute_humidity(25, 50)
    assert abs(ah - 11.5) < 0.3, f"absolute_humidity off: got {ah}, expected ~11.5"

    assert aqi_pm25(9.0) == 50, f"aqi_pm25(9.0) should be 50, got {aqi_pm25(9.0)}"
    assert aqi_pm25(35.4) == 100, f"aqi_pm25(35.4) should be 100, got {aqi_pm25(35.4)}"
    assert aqi_pm25(55.4) == 150, f"aqi_pm25(55.4) should be 150, got {aqi_pm25(55.4)}"
    assert aqi_pm25(125.4) == 200, f"aqi_pm25(125.4) should be 200, got {aqi_pm25(125.4)}"
    assert aqi_pm25(225.4) == 300, f"aqi_pm25(225.4) should be 300, got {aqi_pm25(225.4)}"
    assert aqi_category(50) == "Good"
    assert aqi_category(51) == "Moderate"
    assert aqi_category(301) == "Hazardous"

    assert add_calculations({"pm25": None, "temperature": None, "humidity": None}) == {
        "pm25": None,
        "temperature": None,
        "humidity": None,
        "heat_index": None,
        "dew_point": None,
        "absolute_humidity": None,
        "aqi": None,
        "aqi_category": None,
    }

    print("[3] All self-tests passed.")

# -------------------- END OF STAGE 3 --------------------


##############################################################################
#
#   STAGE 4: WIRING + HANDOFF TO BACKEND  (ADDED, not from any teammate)
#   Original file: new
#   Owner: ______________________
#
##############################################################################

import argparse
import os
import signal
import threading

OUTPUT_TOPIC = "processed/{device_id}/readings"
FINAL_FILE = "final_readings.jsonl"
REJECTED_FILE = "rejected_readings.jsonl"

# ============================================================
# Field mapping from the ESP32's payload names to the project's
# canonical names. The connectivity team's nodes publish
#     co2_temperature_c, humidity_percent, pm2_5_ug_m3, co2_ppm,
#     pm1_ug_m3, pm10_ug_m3, uv_index, co2_status, timestamp
# but the pipeline and the DB schema expect
#     temperature, humidity, pm25, co2
# Keys in this dict are renamed before cleaning; anything not
# listed (e.g. pm1_ug_m3, pm10_ug_m3, uv_index) is passed through
# untouched and lands in the readings table's "extra" JSONB column.
# ============================================================
ESP_FIELD_MAP = {
    "co2_temperature_c": "temperature",
    "humidity_percent": "humidity",
    "pm2_5_ug_m3": "pm25",
    "co2_ppm": "co2",
    "battery_voltage_v": "battery_v",
}

# The network team's message format is {"id": ..., "<sensor>": value}, with no
# timestamp. Stage 2a correctly rejects readings without "ts", so without this
# every real reading would be rejected. When True, a missing ts is filled with
# the time Stage 1 received the message, and a note is added to "flags".
# Set to False (or run with --strict-ts) once the ESP sends its own timestamp.
FILL_MISSING_TS = True

_TOPIC_UNSAFE = str.maketrans({"/": "_", "+": "_", "#": "_"})


class Pipeline:
    """Runs one reading through 2a -> 2b -> 3, then saves and publishes it."""

    def __init__(self, publish=None, fill_missing_ts=FILL_MISSING_TS, save_files=True):
        self.publish = publish
        self.fill_missing_ts = fill_missing_ts
        self.save_files = save_files
        self.stats = {"processed": 0, "rejected": 0}
        self._lock = threading.Lock()

    def _save(self, path, record):
        if self.save_files:
            with self._lock, open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

    def _reject(self, reading, reason):
        with self._lock:
            self.stats["rejected"] += 1
        self._save(REJECTED_FILE, {"reason": reason, "reading": reading})
        return None

    def process_reading(self, reading):
        if isinstance(reading, dict):
            reading = {ESP_FIELD_MAP.get(k, k): v for k, v in reading.items()}

        if (self.fill_missing_ts and isinstance(reading, dict)
                and "ts" not in reading and "received_at" in reading):
            reading = dict(reading)
            reading["ts"] = reading["received_at"]
            reading["flags"] = list(reading.get("flags", [])) + [
                "ts: missing from sender, filled with receive time"]

        cleaned = clean_structure(reading)                      # Stage 2a
        if cleaned is None:
            return self._reject(reading, get_rejection_reason(reading))

        cleaned = clean_values(cleaned)                         # Stage 2b
        if cleaned is None:
            return self._reject(reading, "value validation failed")

        final = add_calculations(cleaned)                       # Stage 3
        if final is None:
            return self._reject(reading, "calculations failed")

        with self._lock:
            self.stats["processed"] += 1
        self._save(FINAL_FILE, final)
        if self.publish:
            topic = OUTPUT_TOPIC.format(device_id=final["id"].translate(_TOPIC_UNSAFE))
            self.publish(topic, json.dumps(final))

        print(f"[4] OK {final['id']} | temp {final.get('temperature')} "
              f"hum {final.get('humidity')} pm25 {final.get('pm25')} | "
              f"heat_index {final.get('heat_index')} dew_point {final.get('dew_point')} "
              f"abs_hum {final.get('absolute_humidity')} AQI {final.get('aqi')} "
              f"({final.get('aqi_category')})")
        return final


def run_live(args):
    pipeline = Pipeline(fill_missing_ts=not args.strict_ts, save_files=not args.no_files)
    consumer = MQTTConsumer(handler=pipeline.process_reading, host=args.host, port=args.port,
                            output_file=None if args.no_files else OUTPUT_FILE,
                            client_id=args.client_id)
    if args.username:
        consumer.client.username_pw_set(args.username, args.password)
    if not args.no_publish:
        pipeline.publish = lambda topic, payload: consumer.client.publish(topic, payload, qos=1)

    if not consumer.start():
        raise SystemExit(f"Could not connect to the broker at {args.host}:{args.port}.")

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    print("[4] Pipeline running"
          + ("" if args.no_publish else ", publishing to processed/<device-id>/readings")
          + ". Ctrl+C to stop.")
    while not stop.wait(timeout=1):  # short waits so Ctrl+C works quickly on Windows
        pass

    consumer.stop()
    print(f"\n[4] Stopped. Stage 1: {consumer.stats}  Pipeline: {pipeline.stats}")


def run_offline(input_file):
    process_file_2a(input_file=input_file)   # -> structure_ok.jsonl, rejected_readings.jsonl
    process_file_2b()                          # -> clean_readings.jsonl
    process_file_3()                           # -> final_readings.jsonl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data team pipeline: stages 1, 2a, 2b, 3 and handoff")
    parser.add_argument("--host", default=os.environ.get("MQTT_HOST", BROKER_HOST))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", BROKER_PORT)))
    parser.add_argument("--username", default=os.environ.get("MQTT_USERNAME"))
    parser.add_argument("--password", default=os.environ.get("MQTT_PASSWORD"))
    parser.add_argument("--client-id", default="data-team-pipeline")
    parser.add_argument("--strict-ts", action="store_true",
                        help="reject readings with no ts instead of filling it in")
    parser.add_argument("--no-publish", action="store_true", help="don't publish results for backend")
    parser.add_argument("--no-files", action="store_true", help="don't write the .jsonl files")
    parser.add_argument("--offline", metavar="FILE", help="process a saved .jsonl file instead of MQTT")
    parser.add_argument("--self-test", action="store_true", help="run Stage 3's own self-tests")
    args = parser.parse_args()

    if args.self_test:
        _run_self_tests()
    elif args.offline:
        run_offline(args.offline)
    else:
        run_live(args)