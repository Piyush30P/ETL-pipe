# transform.py — all data transformation logic
# psycopg2 automatically deserializes PostgreSQL JSONB → Python dict
# So input_data arrives as a real dict — no json.loads() needed

import json
import logging
from config import INPUT_DATA_KEYS, EVENT_DATA_KEYS

logger = logging.getLogger(__name__)


def safe_get(data, key):
    """Safely extract a key from a dict (handles None and non-dict values)."""
    if isinstance(data, dict):
        return data.get(key)
    return None


def safe_bool(val):
    """Convert various truthy representations to bool or None."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


def safe_numeric(val):
    """Convert to float or None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val):
    """Convert to int or None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def flatten_input_data(input_data):
    """
    Extracts known keys from input_data JSONB dict into typed values.
    Returns a dict with one entry per INPUT_DATA_KEYS item.
    Unknown keys are not lost — they stay in input_data_full_text.
    """
    if not isinstance(input_data, dict):
        # Sometimes JSONB arrives as a string if not auto-deserialized
        if isinstance(input_data, str):
            try:
                input_data = json.loads(input_data)
            except Exception:
                input_data = {}
        else:
            input_data = {}

    return {
        "inp_value":          safe_numeric(safe_get(input_data, "value")),
        "inp_unit":           safe_get(input_data, "unit"),
        "inp_start_year":     safe_int(safe_get(input_data, "start_year")),
        "inp_end_year":       safe_int(safe_get(input_data, "end_year")),
        "inp_input_type":     safe_get(input_data, "input_type"),
        "inp_timeframe":      safe_get(input_data, "timeframe"),
        "inp_dosing_type":    safe_get(input_data, "dosing_type"),
        "inp_actuals_flag":   safe_bool(safe_get(input_data, "actuals_flag")),
        "inp_curve_type":     safe_get(input_data, "curve_type"),
        "inp_selected_output":safe_get(input_data, "selected_output"),
        "inp_pfs_flag":       safe_bool(safe_get(input_data, "pfs_flag")),
        "inp_ppc_flag":       safe_bool(safe_get(input_data, "ppc_flag")),
        # Full JSON text for reference/tooltip in Power BI
        "input_data_full_text": json.dumps(input_data, default=str) if input_data else None
    }


def flatten_event_data(event_data):
    """Same pattern for event_data JSONB."""
    if not isinstance(event_data, dict):
        if isinstance(event_data, str):
            try:
                event_data = json.loads(event_data)
            except Exception:
                event_data = {}
        else:
            event_data = {}

    return {
        "evt_year":          safe_int(safe_get(event_data, "year")),
        "evt_share_value":   safe_numeric(safe_get(event_data, "share_value")),
        "evt_entry_quarter": safe_get(event_data, "entry_quarter"),
        "evt_erosion_rate":  safe_numeric(safe_get(event_data, "erosion_rate")),
        "evt_launch_date":   safe_get(event_data, "launch_date"),
        "evt_steady_state":  safe_numeric(safe_get(event_data, "steady_state")),
        "evt_sob_value":     safe_numeric(safe_get(event_data, "sob_value")),
        "event_data_full_text": json.dumps(event_data, default=str) if event_data else None
    }


def transform_scenarios(rows):
    """
    Scenarios need minimal transformation — mostly just renaming columns
    to match the target schema and handling None values.
    Returns list of tuples for bulk upsert.
    """
    result = []
    for r in rows:
        result.append((
            r["id"],                        # scenario_id
            r["scenario_display_name"],
            r["status"],
            r["is_starter"],
            r["currency"],
            r["currency_code"],
            r["scenario_start_year"],
            r["scenario_end_year"],
            r.get("scenario_region_name"),
            r.get("scenario_country_name"),
            r["created_at"],
            r["created_by"],
            r.get("submitted_at"),
            r.get("submitted_by"),
            r.get("locked_at"),
            r.get("locked_by"),
            r.get("updated_at"),
            r.get("updated_by"),
            r.get("withdraw_at"),
            r.get("withdraw_by"),
            r.get("delete_at"),
            r["model_id"],
            r["model_display_name"],
            r.get("model_type"),
            r.get("model_publish_level"),
            r["therapeutic_area_name"],
            r["disease_area_name"],
            r.get("loe_enabled"),
            r.get("model_region_name"),
            r.get("model_country_name"),
            r.get("forecast_cycle_name"),
            r.get("forecast_cycle_start"),
            r.get("forecast_cycle_end"),
            r.get("horizon_start_limit"),
            r.get("horizon_end_limit"),
            r.get("starter_created"),
        ))
    logger.debug(f"  Transformed {len(result)} scenarios")
    return result


def transform_node_data(rows):
    """
    The most important transformation:
    1. Flatten input_data JSONB into typed columns
    2. Set is_current_version based on whether end_at is NULL
    3. Return tuples ready for upsert
    """
    result = []
    for r in rows:
        flat = flatten_input_data(r["input_data"])
        is_current = r["version_ended_at"] is None

        result.append((
            r["id"],                        # source_id (for dedup)
            r["scenario_id"],
            r["model_node_id"],
            r.get("node_display_name"),
            r.get("node_type"),
            r.get("tab_name"),
            r.get("tab_level"),
            r.get("group_name"),
            r.get("group_type"),
            r.get("node_seq"),
            r.get("flow"),
            r["version_started_at"],
            r.get("version_ended_at"),
            is_current,
            r.get("edited_by"),
            r.get("input_hash"),
            r.get("input_validated"),
            str(r["validation_message"]) if r.get("input_validation_message") else None,
            r.get("source"),
            # Flattened JSONB
            flat["inp_value"],
            flat["inp_unit"],
            flat["inp_start_year"],
            flat["inp_end_year"],
            flat["inp_input_type"],
            flat["inp_timeframe"],
            flat["inp_dosing_type"],
            flat["inp_actuals_flag"],
            flat["inp_curve_type"],
            flat["inp_selected_output"],
            flat["inp_pfs_flag"],
            flat["inp_ppc_flag"],
            flat["input_data_full_text"],
        ))
    logger.debug(f"  Transformed {len(result)} node data rows")
    return result


def transform_runs(rows):
    result = []
    for r in rows:
        result.append((
            r["run_id"],
            r["scenario_id"],
            r.get("run_status"),
            r.get("run_at"),
            r.get("run_by"),
            r.get("run_complete_at"),
            r.get("run_duration_minutes"),
            r.get("fail_reason"),
            r.get("branch_count") or 0,
            r.get("total_nodes_processed") or 0,
            r.get("nodes_success") or 0,
            r.get("nodes_failed") or 0,
            r.get("nodes_timeout") or 0,
        ))
    logger.debug(f"  Transformed {len(result)} runs")
    return result


def transform_node_calc(rows):
    result = []
    for r in rows:
        result.append((
            r["id"],                        # source_id (dedup)
            r["run_id"],
            r["scenario_id"],
            r.get("branch_id"),
            r.get("event_tag"),
            r.get("model_node_id"),
            r.get("node_display_name"),
            r.get("node_type"),
            r.get("calc_status"),
            r.get("fail_reason"),
            r.get("processing_start_at"),
            r.get("processing_end_at"),
            r.get("processing_duration_s"),
            r.get("output_data_text"),
        ))
    logger.debug(f"  Transformed {len(result)} node calc rows")
    return result


def transform_event_data(rows):
    result = []
    for r in rows:
        flat = flatten_event_data(r["event_data"])
        is_current = r["version_ended_at"] is None

        result.append((
            r["id"],                        # source_id (dedup)
            r["scenario_id"],
            r.get("event_type_name"),
            r.get("is_inherent"),
            r.get("population_node_name"),
            r.get("parent_product_name"),
            r["version_started_at"],
            r.get("version_ended_at"),
            is_current,
            r.get("edited_by"),
            r.get("event_data_hash"),
            r.get("is_overridden"),
            r.get("override_data_text"),
            r.get("is_validated"),
            r.get("validation_message"),
            flat["evt_year"],
            flat["evt_share_value"],
            flat["evt_entry_quarter"],
            flat["evt_erosion_rate"],
            flat["evt_launch_date"],
            flat["evt_steady_state"],
            flat["evt_sob_value"],
            flat["event_data_full_text"],
        ))
    logger.debug(f"  Transformed {len(result)} event data rows")
    return result


def transform_timeline(rows):
    result = []
    for r in rows:
        result.append((
            r["scenario_id"],
            r["event_time"],
            r["event_type"],
            r.get("event_category"),
            r.get("actor"),
            r.get("description"),
            r.get("run_id"),
            r.get("node_name"),
            r.get("event_type_name"),
            r.get("source_key"),        # UNIQUE key for dedup
        ))
    logger.debug(f"  Transformed {len(result)} timeline events")
    return result
