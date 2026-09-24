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
TOPIC = "sensors/+/readings"
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
            # a bug further down the pipeline must never kill the connection
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


if __name__ == "__main__":
    consumer = MQTTConsumer()
    if not consumer.start():
        raise SystemExit("Could not connect to the broker. Start fake_broker.py first.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        consumer.stop()
        print(f"\n[stage 1] stopped. Stats: {consumer.stats}")
        print(f"[stage 1] readings saved to {OUTPUT_FILE}")