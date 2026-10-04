# Stage 2b: Sensor Value Cleaning
#
# This module validates numerical sensor readings against
# predefined acceptable ranges. Values outside their allowed
# ranges are changed to None, while valid and existing None values are preserved.
# Input:  structure_ok.jsonl
# Output: clean_readings.jsonl

import json

# Allowed minimum and maximum values for each sensor field.
# These limits are used to identify physically invalid
# numerical measurements during Stage 2b.

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



# Validate the sensor values in a single reading.
# Each supported sensor is checked against its allowed range.
# Out-of-range values are replaced with None. Other fields in
# the reading are retained.

def clean_values(reading):
    
    # Stage 2b expects each input reading to be a dictionary.
    # Invalid input objects cannot be processed safely.
    if not isinstance(reading, dict):
        return None

    # Work on a copy so that the original reading is not modified.
    # This allows the input data to remain unchanged while the cleaned version is produced.
    cleaned_reading = reading.copy()

    # Check each sensor field that has a defined validation range.
    # The same validation logic is reused for all supported fields.
    for field, (minimum, maximum) in VALUE_RANGES.items():

        # Some readings may not contain every possible sensor field.
        # Missing fields are left unchanged rather than being added or assigned an assumed value.
        if field not in cleaned_reading:
            continue

        value = cleaned_reading[field]

        # A None value represents a missing measurement rather than
        # an invalid numerical value, so it is preserved as-is.
        if value is None:
            continue

        # Values outside the inclusive minimum/maximum range are
        # considered invalid and are replaced with None.
        if value < minimum or value > maximum:

            # Replace only the invalid sensor field so that other valid
            # measurements in the same reading are retained.
            cleaned_reading[field] = None

    # Return the cleaned reading for writing to the output JSONL file.
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



# Process the Stage 2a output file.
# Each structurally valid reading is passed through
# clean_values() and the cleaned result is written to the Stage 2b output file.
def process_file(

    # Stage 2b receives structurally valid readings from Stage 2a
    # and produces the cleaned readings used by the next stage.
    input_file="structure_ok.jsonl",
    output_file="clean_readings.jsonl"
):
    # Track processing statistics so that the cleaning operation can report how many readings were processed
    #  and how many sensor values were changed to None.
    kept = 0
    invalid_values = 0  

    # Read the Stage 2a output one JSON object per line.
    with open(input_file, "r", encoding="utf-8") as infile, \
        open(output_file, "w", encoding="utf-8") as outfile:  
        # Write each cleaned reading as a separate JSON line.
        # JSONL keeps individual sensor readings independently readable
        # and suitable for the next pipeline stage.
        for line_number, line in enumerate(infile, start=1):

            if not line.strip():
                continue

            try:
                # Convert the JSONL line into a Python dictionary so that
                # the sensor fields can be validated.
                reading = json.loads(line)

            except json.JSONDecodeError:
                print(f"[2b] Skipping invalid JSON on line {line_number}")
                continue

            # Apply Stage 2b value validation to the structurally valid
            # reading produced by Stage 2a.
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

            # Convert the cleaned dictionary back to JSON and write it as
            # one line in the cleaned readings file.
            outfile.write(
            json.dumps(cleaned) + "\n"
            )

            kept += 1               

    print()
    print("[2b] Value validation complete")
    print(" ")

    # Report the results of the Stage 2b cleaning process.
    print(f"[2b] Readings processed:       {kept}")
    print(f"[2b] Invalid values nulled:    {invalid_values}")
    print(f"[2b] Output:                    {output_file}")

# Run the Stage 2b file-processing pipeline when this script is executed directly.
if __name__ == "__main__":
    process_file()    