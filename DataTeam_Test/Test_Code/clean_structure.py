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


def process_file(
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


# -------------------------------------------------------------
# Run Stage 2a when the file is executed directly.
# -------------------------------------------------------------
if __name__ == "__main__":
    process_file()