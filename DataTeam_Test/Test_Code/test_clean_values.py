# Tests for Stage 2b: Sensor Value Cleaning

# These tests verify that clean_values() correctly preserves
# valid measurements, rejects out-of-range values, handles
# missing values, and accepts valid boundary values.

from clean_values import clean_values


# Verify that a reading containing valid sensor values is
# returned without changing the measurements.
def test_valid_reading():
    reading = {
        "id": "device-01",
        "ts": "2026-09-26T10:00:00Z",
        "temperature": 25,
        "humidity": 55,
        "pressure": 1013,
        "pm25": 20,
        "tvoc": 500,
        "eco2": 800,
        "co2": 600,
        "battery_v": 3.7,
        "lat": -33.9,
        "lon": 18.4
    }

    result = clean_values(reading)

    assert result["temperature"] == 25
    assert result["humidity"] == 55
    assert result["pressure"] == 1013
    assert result["pm25"] == 20

# Verify that a temperature above the permitted maximum is replaced with None.
def test_temperature_out_of_range():
    reading = {
        "id": "device-01",
        "temperature": 250
    }

    result = clean_values(reading)

    assert result["temperature"] is None


# Verify that a temperature below the permitted minimum is replaced with None.
def test_temperature_below_range():
    reading = {
        "id": "device-01",
        "temperature": -50
    }

    result = clean_values(reading)

    assert result["temperature"] is None

# Verify that humidity above the allowed 0-100% range is replaced with None.
def test_humidity_out_of_range():
    reading = {
        "id": "device-01",
        "humidity": 150
    }

    result = clean_values(reading)

    assert result["humidity"] is None

# Verify that a negative PM2.5 measurement is identified
# as invalid and replaced with None.
def test_pm25_out_of_range():
    reading = {
        "id": "device-01",
        "pm25": -10
    }

    result = clean_values(reading)

    assert result["pm25"] is None


# Verify that an existing None value is preserved and is
# not replaced with zero or another assumed measurement.
def test_null_value_remains_null():
    reading = {
        "id": "device-01",
        "temperature": None,
        "pm25": None
    }

    result = clean_values(reading)

    assert result["temperature"] is None
    assert result["pm25"] is None

# Verify that values exactly equal to the configured minimum
# or maximum limits are considered valid.
def test_boundary_values_are_valid():
    reading = {
        "id": "device-01",
        "temperature": -40,
        "humidity": 100,
        "pressure": 870,
        "pm25": 1000
    }

    result = clean_values(reading)

    assert result["temperature"] == -40
    assert result["humidity"] == 100
    assert result["pressure"] == 870
    assert result["pm25"] == 1000


# Verify that multiple invalid sensor values in the same
# reading are independently replaced with None.
def test_multiple_invalid_values():
    reading = {
        "id": "device-01",
        "temperature": 250,
        "humidity": -10,
        "pm25": -20,
        "pressure": 2000
    }

    result = clean_values(reading)

    assert result["temperature"] is None
    assert result["humidity"] is None
    assert result["pm25"] is None
    assert result["pressure"] is None


# Verify that valid measurements remain unchanged after
# the cleaning function is applied.
def test_valid_values_are_not_changed():
    reading = {
        "id": "device-01",
        "temperature": 25,
        "humidity": 50,
        "pressure": 1000,
        "pm25": 15
    }

    result = clean_values(reading)

    assert result["temperature"] == 25
    assert result["humidity"] == 50
    assert result["pressure"] == 1000
    assert result["pm25"] == 15