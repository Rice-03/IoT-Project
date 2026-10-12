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

# HEAT INDEX: how hot it "feels" when you combine temperature and humidity. 
def _heat_index_fahrenheit(t_f, rh):
    hi_simple = 0.5 * (t_f + 61.0 + ((t_f - 68.0)) * 1.2 + (rh * 0.094))

    average = (hi_simple + t_f) / 2.0
    if average < 80.0:
        return hi_simple
    # Full formula (NOAA calls this the Rothfusz regression)
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

    # NOAA has two extra corrections for unusual conditions:
    # very dry heat, and very humid heat. Apply whichever one fits. 
    if rh < 13 and 80 <= t_f <= 112:
        #Hot and very dry: teh formula overestimates, so subtract a bit.
        adjustment = ((13 - rh) / 4.0) * math.sqrt((17 - abs(t_f - 95.0)) / 17.0)
        hi -= adjustment
    elif rh > 85 and 80 <= t_f <= 87:
        # Hot and very humid: the formula underestimates, so add a bit. 
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

# DEW POINT: teh temperature sir needs to cool before it starts forming dew (i.e. the air becomes 100% humid)
def dew_point(temperature, humidity):
    if temperature is None or humidity is None:
        return None

    if humidity <= 0:
        return None
        
    # fixed constants from the Magnus formula
    a, b = 17.27, 237.7
    alpha = (a * temperature) / (b + temperature) + math.log(humidity / 100.0)
    dp = (b * alpha) / (a - alpha)
    return round(dp, 2)

# ABSOLUTE HUMIDITY: hwo many grams of water are in each cubic metre of air
def absolute_humidity(temperature, humidity):
    if temperature is None or humidity is None:
        return None

    # Step 1: how much water vapor the air COULD hold at this temperature
    # if it were 100% humid (this is called the saturation vapor pressure).
    saturation_vapor_pressure = 6.112 * math.exp(
        (17.67 * temperature) / (temperature + 243.5)
    )

    # Step 2: scale that down by the actual humidity percentage, and
    # convert the units into grams per cubic metre. 
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

#AQI (Air Quality Index) from PM2.5: each range of PM2.5 pollution maps a range of AQI numbers, and
# you work out exactly where a reading falls using a straight line between the two ends of its row. 
def aqi_pm25(pm25):
    if pm25 is None:
        return None

    if pm25 < 0:
        # Not a real/possible value, so there's nothing sensible to return. 
        return None

    # EPA rounds PM2.5 DOWN to one decimal place before looking it up. 
    cp = math.floor(pm25 * 10) / 10.0

    # Nothing above this is officially defined, so just cap it at 500
    # instead of guessing with the formula past where it's meant to work.
    if cp > PM25_BREAKPOINTS[-1][1]:
        return 500

    # Find which row of the table this reading falls into, then draw a 
    # straight line between that row's two ends to get the exact AQI. 
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

# Put it all together for one reading
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

# Run this over a whole file of readings 
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

# Quick checks against known-correct answers.
# Run this file directly and these run automatically before anything else.
def _run_self_tests():
    # 90F, 50% humidity should a heat of about 94.6F.
    # (90F is about 32.222C)
    hi = heat_index(32.222, 50)
    hi_f = hi * 9.0 / 5.0 + 32.0
    assert abs(hi_f - 94.6) < 1.0, f"heat_index off: got {hi_f}F, expected ~94.6F"

    # In cool weather, heat index should stay close to the real temperature.
    hi_cool = heat_index(10, 50)
    assert abs(hi_cool - 10) < 3, f"cool heat_index should stay near air temp, got {hi_cool}"

    # 25C, 50% humidity should give a dew point of about 13.9C.
    dp = dew_point(25, 50)
    assert abs(dp - 13.9) < 0.2, f"dew_point off: got {dp}, expected ~13.9"

    # 25C, 50% humidity should give absolute humidity of about 11.5 g/m3.
    ah = absolute_humidity(25, 50)
    assert abs(ah - 11.5) < 0.3, f"absolute_humidity off: got {ah}, expected ~11.5"

    # Each breakpoint's top edge should land exactly on its published AQi value. 
    assert aqi_pm25(9.0) == 50, f"aqi_pm25(9.0) should be 50, got {aqi_pm25(9.0)}"
    assert aqi_pm25(35.4) == 100, f"aqi_pm25(35.4) should be 100, got {aqi_pm25(35.4)}"
    assert aqi_pm25(55.4) == 150, f"aqi_pm25(55.4) should be 150, got {aqi_pm25(55.4)}"
    assert aqi_pm25(125.4) == 200, f"aqi_pm25(125.4) should be 200, got {aqi_pm25(125.4)}"
    assert aqi_pm25(225.4) == 300, f"aqi_pm25(225.4) should be 300, got {aqi_pm25(225.4)}"
    assert aqi_category(50) == "Good"
    assert aqi_category(51) == "Moderate"
    assert aqi_category(301) == "Hazardous"

    # If every input is missing, every output should be None too. 
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
