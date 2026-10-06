"""
=================================================================
live_capture.py: Live MQTT ingestion into TimescaleDB
=================================================================
Subscribes to the data team's pipeline output (data_pipeline.py)
and stores each finished reading in the database.

The pipeline publishes one JSON message per reading to
    processed/<device-id>/readings
after cleaning it and calculating heat_index, dew_point,
absolute_humidity, aqi and aqi_category. This script's only job is
to get each finished reading into the database as it arrives.
There is no BTI/F-T-D-P computation here any more: that belonged
to a different system, not this project.

What a stored reading looks like:
  - a sensor value the pipeline rejected (impossible, or the wrong
    type) is stored as NULL, and so is anything calculated from it
  - "flags" is only filled when the pipeline added a note itself
    (currently: the ESP sent no timestamp, so the receive time was used)
  - a field this project doesn't know about (or a sensor sent under a
    different name, e.g. "temp") is kept in the "extra" column

Run on the Pi alongside the dashboard/API:

    python3 live_capture.py

MQTT settings come from the environment, using the same variable names as
data_pipeline.py so one export configures both ends of the handoff:

    MQTT_HOST MQTT_PORT MQTT_USERNAME MQTT_PASSWORD

Defaults to the Mosquitto broker running on this machine. There is no
built-in username or password: if MQTT_USERNAME is unset, connect
anonymously. Credentials are deliberately not hardcoded - set them in
the environment (or a systemd unit / .env) instead.
================================================================
"""

import json
import os
import re
import time

import paho.mqtt.client as mqtt

from config import get_db

# =========================
# MQTT CONFIG
# =========================
MQTT_BROKER = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USERNAME")
MQTT_PASS = os.environ.get("MQTT_PASSWORD")
# The data pipeline publishes one finished reading per message here,
# one topic per node: processed/<device-id>/readings.
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "processed/+/readings")


# =========================
# INGESTOR
# =========================
class LiveIngestor:
    def __init__(self):
        self.db = get_db()
        self.db_choice = "timescale"
        self.stats = {"stored": 0, "duplicates": 0, "dropped_bad_json": 0, "failed": 0}

        cid = "db_" + re.sub(r"[^A-Za-z0-9]", "_", MQTT_TOPIC) + "_" + str(os.getpid())
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=cid)
        except AttributeError:
            self.client = mqtt.Client(client_id=cid)
        if MQTT_USER:
            self.client.username_pw_set(MQTT_USER, MQTT_PASS)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT {MQTT_BROKER}:{MQTT_PORT} "
                  f"-> writing to DB '{self.db_choice}', listening on {MQTT_TOPIC}")
            # QoS 1 = at-least-once delivery while this script is connected.
            # It does NOT queue readings while the script is stopped: each run
            # uses a new client id and a clean session, so anything published
            # during a gap is not delivered. To fill a gap afterwards, load the
            # pipeline's final_readings.jsonl (safe to repeat; duplicates are
            # skipped): get_db().insert_from_jsonl("final_readings.jsonl")
            client.subscribe(MQTT_TOPIC, qos=1)
        else:
            print(f"MQTT connect failed rc={rc}")

    def _on_disconnect(self, client, userdata, *args):
        print("Disconnected from broker; reconnecting automatically...")

    def _on_message(self, client, userdata, msg):
        try:
            self._ingest_reading(msg.topic, msg.payload)
        except Exception as e:
            self.stats["failed"] += 1
            print(f"ingest error: {e}")

    def _ingest_reading(self, topic, payload):
        try:
            reading = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.stats["dropped_bad_json"] += 1
            print(f"dropped unreadable message on {topic}: {e}")
            return

        if not reading.get("id"):
            # Stage 2a of the pipeline rejects any reading without an id, so
            # everything published to this topic has one. Safety net only.
            reading["id"] = self._extract_node(topic) or "node-unknown"

        stored = self.db.insert_processed_reading(reading)
        self.stats["stored" if stored else "duplicates"] += 1

        note = "" if stored else "  [duplicate, skipped]"
        print(f"[{reading['id']}] temp={reading.get('temperature')} "
              f"hum={reading.get('humidity')} pm25={reading.get('pm25')} "
              f"AQI={reading.get('aqi')} ({reading.get('aqi_category')}){note}")

    @staticmethod
    def _extract_node(topic):
        """processed/<device-id>/readings -> <device-id>"""
        try:
            parts = topic.split("/")
            if len(parts) == 3 and parts[0] == "processed":
                return parts[1]
        except Exception:
            pass
        return None

    def run(self):
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        except Exception as e:
            print(f"Could not connect to broker: {e}")
            return
        self.client.loop_start()
        print("Live capture running - Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping live capture...")
        self.client.loop_stop()
        self.client.disconnect()
        self.db.close()
        print(f"Stopped. {self.stats}")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    ingestor = LiveIngestor()
    ingestor.run()
