"""
test_stage1.py

One command to prove stage 3.1 works. It starts fake_broker.py by itself,
runs the consumer against it, and checks everything. You do NOT need the
broker running already.

    python test_stage1.py

Checks:
  1. Consumer connects and receives live fake readings
  2. Every reading gets received_at and source_topic added
  3. Invalid JSON is dropped, not crashed on
  4. Readings with bad VALUES are passed through untouched (cleaning's job)
  5. Readings are saved to the output file, one per line
  6. If the next stage crashes on a reading, the consumer keeps going
"""
import json
import os
import subprocess
import sys
import tempfile
import time

import paho.mqtt.client as mqtt

from mqtt_consumer import MQTTConsumer

HERE = os.path.dirname(os.path.abspath(__file__))
results = []


def check(name, condition, detail=""):
    results.append(condition)
    status = "PASS" if condition else "FAIL"
    print(f"  {status}: {name}" + (f"  ({detail})" if detail else ""))


def publish(payloads, topic="sensors/test-node/readings"):
    pub = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="test-pub")
    pub.connect("127.0.0.1", 1883)
    pub.loop_start()
    for p in payloads:
        pub.publish(topic, p, qos=1).wait_for_publish()
    time.sleep(0.5)
    pub.loop_stop()
    pub.disconnect()


def main():
    print("Starting fake_broker.py (fast mode, 0.3s between readings)...")
    broker = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "fake_broker.py"), "--interval", "0.3"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(1.5)  # broker is up; it waits 3s before its first reading

    out_file = os.path.join(tempfile.mkdtemp(), "raw_readings.jsonl")
    received = []

    def handler(reading):
        received.append(reading)
        if reading.get("crash_me"):
            raise RuntimeError("simulated bug in the next stage")

    consumer = MQTTConsumer(handler=handler, output_file=out_file, client_id="test-stage1")
    try:
        connected = consumer.start(retries=5)

        print("\nPart 1: live fake readings (running for 5 seconds)")
        time.sleep(5)
        live = [r for r in received if r["source_topic"] == "sensors/device-01/readings"]
        check("connected to broker", connected)
        check("received live readings", len(live) >= 5, f"{len(live)} readings")
        check("every reading has received_at and source_topic",
              all("received_at" in r and "source_topic" in r for r in live))
        check("invalid JSON was dropped without crashing",
              consumer.stats["dropped_bad_json"] >= 1,
              f"{consumer.stats['dropped_bad_json']} dropped")
        check("bad values passed through untouched for cleaning",
              any("id" not in r for r in live) or any(r.get("temperature") == 250.0 for r in live))

        print("\nPart 2: known messages with exact expected outcomes")
        before_dropped = consumer.stats["dropped_bad_json"]
        publish([
            json.dumps({"id": "test-node", "temperature": 21.4, "marker": "A"}),
            "this is not json",
            json.dumps({"id": "test-node", "crash_me": True, "marker": "B"}),
            json.dumps({"id": "test-node", "temperature": 19.0, "marker": "C"}),
        ])
        time.sleep(1)
        test_msgs = [r for r in received if r["source_topic"] == "sensors/test-node/readings"]
        markers = [r.get("marker") for r in test_msgs]
        check("good message parsed with correct values",
              any(r.get("marker") == "A" and r.get("temperature") == 21.4 for r in test_msgs))
        check("'this is not json' was dropped",
              consumer.stats["dropped_bad_json"] == before_dropped + 1)
        check("next stage crashing did not stop the consumer",
              "B" in markers and "C" in markers and consumer.stats["handler_errors"] >= 1,
              f"markers seen: {markers}")

        with open(out_file) as f:
            lines = [json.loads(line) for line in f]
        check("readings saved to output file, one per line",
              len(lines) == consumer.stats["received"], f"{len(lines)} lines")
    finally:
        consumer.stop()
        broker.terminate()
        broker.wait(timeout=5)

    print(f"\n{sum(results)}/{len(results)} checks passed")
    example = next((r for r in received if r.get("id") == "device-01"
                    and r.get("temperature") != 250.0), None)
    if example:
        print(f"\nExample of what stage 1 hands to cleaning:\n{json.dumps(example, indent=2)}")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()