"""
fake_broker.py

Runs a local MQTT broker AND publishes fake sensor readings through it,
so the data team can build and test without the real ESP32 or network.

Nothing to install except Python packages:
    pip install amqtt paho-mqtt

Run:
    python fake_broker.py                 # one reading every 5 seconds
    python fake_broker.py --interval 1    # faster
    python fake_broker.py --clean-only    # no deliberately bad messages

Broker address for everyone else's code: localhost, port 1883
Readings are published to topic: sensors/<device-id>/readings

About every 4th message is deliberately broken (bad JSON, missing fields,
impossible values, wrong types) so the cleaning stage has real problems
to catch. Each broken message is labelled in this script's output so you
know what was sent.
"""
import argparse
import asyncio
import json
import math
import random
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from amqtt.broker import Broker

HOST = "127.0.0.1"
PORT = 1883
NODE_ID = "device-01"
LAT, LON, ALT = -33.9249, 18.4241, 15.0

# broken message types, cycled through in order so they're predictable
BAD_CASES = [
    "malformed_json",       # not valid JSON at all (stage 1 drops this)
    "missing_id",           # no device id
    "temp_out_of_range",    # temperature 250 C
    "pm25_null",            # sensor returned nothing
    "humidity_as_string",   # wrong type
    "negative_pm25",        # impossible value
    "missing_timestamp",    # no ts field
]


def diurnal(base, amplitude, hour, peak_hour=14, noise=0.5):
    """Daily sine-wave cycle plus a little random noise."""
    radians = 2 * math.pi * (hour - peak_hour) / 24
    return base + amplitude * math.cos(radians) + random.uniform(-noise, noise)


def good_reading():
    now = datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60
    return {
        "id": NODE_ID,
        "ts": now.isoformat(),
        "temperature": round(diurnal(18, 6, hour), 2),
        "humidity": round(max(0, min(100, diurnal(60, -15, hour, peak_hour=4, noise=2))), 1),
        "pressure": round(1013 + random.uniform(-3, 3), 1),
        "pm25": round(max(0, diurnal(12, 8, hour, peak_hour=8, noise=3)), 1),
        "tvoc": round(max(0, diurnal(150, 80, hour, peak_hour=9, noise=20))),
        "eco2": round(max(400, diurnal(500, 100, hour, peak_hour=9, noise=30))),
        "co2": round(max(400, diurnal(480, 60, hour, peak_hour=9, noise=20))),
        "battery_v": round(max(3.3, 4.1 - hour / 48), 2),
        "lat": LAT,
        "lon": LON,
        "altitude": ALT,
    }


def bad_payload(case):
    """Returns the raw string to publish for a given broken case."""
    r = good_reading()
    if case == "malformed_json":
        return '{"id": "device-01", "temperature": 21.5,'  # cut off mid-message
    if case == "missing_id":
        del r["id"]
    elif case == "temp_out_of_range":
        r["temperature"] = 250.0
    elif case == "pm25_null":
        r["pm25"] = None
    elif case == "humidity_as_string":
        r["humidity"] = "sixty"
    elif case == "negative_pm25":
        r["pm25"] = -5.0
    elif case == "missing_timestamp":
        del r["ts"]
    return json.dumps(r)


def publisher_loop(interval, clean_only, delay, stop_event):
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id="fake-sensor-node")
    client.connect(HOST, PORT)
    client.loop_start()
    topic = f"sensors/{NODE_ID}/readings"

    # messages sent before anyone subscribes are lost, so give consumers time to connect
    print(f"[fake sensor] first reading in {delay}s (start your consumer now)")
    if stop_event.wait(delay):
        return

    count = 0
    bad_index = 0
    while not stop_event.is_set():
        count += 1
        if not clean_only and count % 4 == 0:
            case = BAD_CASES[bad_index % len(BAD_CASES)]
            bad_index += 1
            payload = bad_payload(case)
            label = f"BAD ({case})"
        else:
            payload = json.dumps(good_reading())
            label = "good"
        client.publish(topic, payload, qos=1)
        print(f"[fake sensor] #{count} {label}: {payload}")
        stop_event.wait(interval)

    client.loop_stop()
    client.disconnect()


async def main(interval, clean_only, delay):
    broker = Broker({"listeners": {"default": {"type": "tcp", "bind": f"{HOST}:{PORT}"}}})
    try:
        await broker.start()
    except OSError:
        print(f"Port {PORT} is already in use. Is another broker (e.g. Mosquitto) running?")
        return
    print(f"[fake broker] running on {HOST}:{PORT}")

    stop_event = threading.Event()
    pub_thread = threading.Thread(target=publisher_loop,
                                  args=(interval, clean_only, delay, stop_event), daemon=True)
    pub_thread.start()
    print(f"[fake sensor] will publish every {interval}s. Ctrl+C to stop.")

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        stop_event.set()
        pub_thread.join(timeout=3)
        await broker.shutdown()
        print("\n[fake broker] stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fake MQTT broker with fake sensor readings")
    parser.add_argument("--interval", type=float, default=5, help="seconds between readings")
    parser.add_argument("--clean-only", action="store_true", help="never send broken messages")
    parser.add_argument("--delay", type=float, default=3,
                        help="seconds to wait before the first reading, so consumers can connect")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.interval, args.clean_only, args.delay))
    except KeyboardInterrupt:
        pass