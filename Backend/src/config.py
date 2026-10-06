"""
Database connection configuration.

Provides the TimescaleDB/PostgreSQL connection parameters used by
adapters and modules (e.g. timescale_adapter, auth, weather_app).
"""

import os


def required_env(name):
    """Read a required environment variable.

    Fails loudly instead of falling back to a default, so no credential can
    end up hard-coded (and therefore committed) in this file.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. Set it in the environment before running, "
            f"e.g.  export {name}=...  (and add that to your shell profile "
            f"or systemd unit so it survives a restart)."
        )
    return value


def get_db():
    """Return an adapter instance for the selected database (TimescaleDB)."""
    from timescale_adapter import TimescaleDBAdapter

    return TimescaleDBAdapter()


# ============================================================
# TimescaleDB (PostgreSQL)
#
# Nothing secret lives in this file. Non-sensitive defaults are inlined;
# the password is required from the environment.
#
#   export TS_HOST=localhost
#   export TS_PORT=5432
#   export TS_DB=iot_trust
#   export TS_USER=iot_user
#   export TS_PASSWORD=...
# ============================================================
TIMESCALEDB = {
    "host": os.environ.get("TS_HOST", "localhost"),
    "port": int(os.environ.get("TS_PORT", 5432)),
    "database": os.environ.get("TS_DB", "iot_trust"),
    "user": os.environ.get("TS_USER", "iot_user"),
    "password": required_env("TS_PASSWORD"),
}
