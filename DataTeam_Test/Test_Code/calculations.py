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

def process_file(
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

if __name__ == "__main__":
    _run_self_tests()
    process_file()