# config.py — all connection settings in one place
import os
from dotenv import load_dotenv
load_dotenv()

# ── Source DB (Read-Only — your existing ClearSight PostgreSQL) ──
SOURCE = {
    "host":     os.getenv("SOURCE_HOST",     "your-source-pg-host"),
    "port":     int(os.getenv("SOURCE_PORT", "5432")),
    "dbname":   os.getenv("SOURCE_DB",       "clearsight_db"),
    "user":     os.getenv("SOURCE_USER",     "readonly_user"),
    "password": os.getenv("SOURCE_PASS",     "readonly_pass"),
    "connect_timeout": 10,
    "options":  "-c statement_timeout=15000"  # 15s query timeout — be a good citizen
}

# ── Target DB (Writable — your new reporting DB) ──────────────
TARGET = {
    "host":     os.getenv("TARGET_HOST",     "your-target-pg-host"),
    "port":     int(os.getenv("TARGET_PORT", "5432")),
    "dbname":   os.getenv("TARGET_DB",       "clearsight_reporting"),
    "user":     os.getenv("TARGET_USER",     "etl_user"),
    "password": os.getenv("TARGET_PASS",     "etl_pass"),
    "connect_timeout": 10,
}

# ── ETL Settings ──────────────────────────────────────────────
POLL_INTERVAL_SEC   = 30    # How often the ETL runs (seconds)
OVERLAP_SEC         = 90    # Safety overlap — re-process last 90s to catch slow writes
MAX_BATCH_ROWS      = 5000  # Max rows per table per cycle (prevents memory spikes)

# Keys to extract from input_data JSONB
# Run discovery query first: SELECT DISTINCT jsonb_object_keys(input_data) FROM public.fc_scenario_node_data;
INPUT_DATA_KEYS = [
    "value", "unit", "start_year", "end_year", "input_type",
    "timeframe", "dosing_type", "actuals_flag", "curve_type",
    "selected_output", "pfs_flag", "ppc_flag"
]

# Keys to extract from event_data JSONB
# Run: SELECT DISTINCT jsonb_object_keys(event_data) FROM public.fc_scenario_event_data;
EVENT_DATA_KEYS = [
    "year", "share_value", "entry_quarter", "erosion_rate",
    "launch_date", "steady_state", "sob_value"
]
