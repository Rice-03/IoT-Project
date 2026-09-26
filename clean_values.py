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


def process_file(
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

if __name__ == "__main__":
    process_file()    