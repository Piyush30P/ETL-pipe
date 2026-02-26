# load.py — writes transformed data to target DB
# Each function explains exactly WHY it uses INSERT vs UPSERT vs UPDATE

import logging
from db import executemany_target, execute_target

logger = logging.getLogger(__name__)


def load_scenarios(rows):
    """
    UPSERT — scenario already exists? Update its mutable fields.
    Why: A scenario is created once, but status, submitted_at, locked_at etc
         change over time. We want ONE row per scenario, always up to date.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO dim_scenario (
            scenario_id, scenario_display_name, scenario_status, is_starter,
            currency, currency_code, scenario_start_year, scenario_end_year,
            scenario_region_name, scenario_country_name,
            created_at, created_by, submitted_at, submitted_by,
            locked_at, locked_by, updated_at, updated_by,
            withdraw_at, withdraw_by, delete_at,
            model_id, model_display_name, model_type, model_publish_level,
            therapeutic_area_name, disease_area_name, loe_enabled,
            model_region_name, model_country_name,
            forecast_cycle_name, forecast_cycle_start, forecast_cycle_end,
            horizon_start_limit, horizon_end_limit, starter_created
        ) VALUES %s
        ON CONFLICT (scenario_id) DO UPDATE SET
            scenario_status   = EXCLUDED.scenario_status,
            submitted_at      = EXCLUDED.submitted_at,
            submitted_by      = EXCLUDED.submitted_by,
            locked_at         = EXCLUDED.locked_at,
            locked_by         = EXCLUDED.locked_by,
            updated_at        = EXCLUDED.updated_at,
            updated_by        = EXCLUDED.updated_by,
            withdraw_at       = EXCLUDED.withdraw_at,
            withdraw_by       = EXCLUDED.withdraw_by,
            delete_at         = EXCLUDED.delete_at,
            etl_updated_at    = NOW()
    """
    n = executemany_target(sql, rows)
    logger.info(f"  dim_scenario: {n} rows upserted")
    return n


def load_node_data(rows):
    """
    INSERT with ON CONFLICT DO UPDATE for is_current_version.
    Why: Append-only source — new version = INSERT. Old version ending = 
         UPDATE its is_current_version to FALSE and set version_ended_at.
    source_id (= public.fc_scenario_node_data.id) is the dedup key.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO fact_node_input_history (
            source_id, scenario_id, model_node_id,
            node_display_name, node_type, tab_name, tab_level,
            group_name, group_type, node_seq, flow,
            version_started_at, version_ended_at, is_current_version,
            edited_by, input_hash, input_validated, validation_message,
            data_source,
            inp_value, inp_unit, inp_start_year, inp_end_year,
            inp_input_type, inp_timeframe, inp_dosing_type, inp_actuals_flag,
            inp_curve_type, inp_selected_output, inp_pfs_flag, inp_ppc_flag,
            input_data_full_text
        ) VALUES %s
        ON CONFLICT (source_id) DO UPDATE SET
            -- Only mutable field: when end_at gets set on the source row,
            -- we need to flip is_current_version to FALSE here
            version_ended_at    = EXCLUDED.version_ended_at,
            is_current_version  = EXCLUDED.is_current_version,
            input_validated     = EXCLUDED.input_validated,
            validation_message  = EXCLUDED.validation_message,
            etl_loaded_at       = NOW()
    """
    n = executemany_target(sql, rows)
    logger.info(f"  fact_node_input_history: {n} rows upserted")
    return n


def load_runs(rows):
    """
    UPSERT — a run starts as IN_PROGRESS and later becomes SUCCESS/FAILED.
    Same run_id, status changes → UPDATE the existing row.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO fact_run_summary (
            run_id, scenario_id, run_status, run_at, run_by,
            run_complete_at, run_duration_minutes, fail_reason,
            branch_count, total_nodes_processed,
            nodes_success, nodes_failed, nodes_timeout
        ) VALUES %s
        ON CONFLICT (run_id) DO UPDATE SET
            run_status              = EXCLUDED.run_status,
            run_complete_at         = EXCLUDED.run_complete_at,
            run_duration_minutes    = EXCLUDED.run_duration_minutes,
            fail_reason             = EXCLUDED.fail_reason,
            branch_count            = EXCLUDED.branch_count,
            total_nodes_processed   = EXCLUDED.total_nodes_processed,
            nodes_success           = EXCLUDED.nodes_success,
            nodes_failed            = EXCLUDED.nodes_failed,
            nodes_timeout           = EXCLUDED.nodes_timeout,
            etl_updated_at          = NOW()
    """
    n = executemany_target(sql, rows)
    logger.info(f"  fact_run_summary: {n} rows upserted")
    return n


def load_node_calc(rows):
    """
    INSERT ONLY (with dedup) — calc results never change once written.
    ON CONFLICT DO NOTHING = if already inserted, skip silently.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO fact_node_calc_results (
            source_id, run_id, scenario_id, branch_id, event_tag,
            model_node_id, node_display_name, node_type,
            calc_status, fail_reason,
            processing_start_at, processing_end_at, processing_duration_s,
            output_data_text
        ) VALUES %s
        ON CONFLICT (source_id) DO NOTHING
    """
    n = executemany_target(sql, rows)
    logger.info(f"  fact_node_calc_results: {n} rows inserted")
    return n


def load_event_data(rows):
    """
    Same pattern as node data — append-only source.
    INSERT new versions, UPDATE ended versions.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO fact_event_input_history (
            source_id, scenario_id, event_type_name, is_inherent,
            population_node_name, parent_product_name,
            version_started_at, version_ended_at, is_current_version,
            edited_by, event_data_hash, is_overridden, override_data_text,
            is_validated, validation_message,
            evt_year, evt_share_value, evt_entry_quarter, evt_erosion_rate,
            evt_launch_date, evt_steady_state, evt_sob_value,
            event_data_full_text
        ) VALUES %s
        ON CONFLICT (source_id) DO UPDATE SET
            version_ended_at    = EXCLUDED.version_ended_at,
            is_current_version  = EXCLUDED.is_current_version,
            is_validated        = EXCLUDED.is_validated,
            validation_message  = EXCLUDED.validation_message,
            etl_loaded_at       = NOW()
    """
    n = executemany_target(sql, rows)
    logger.info(f"  fact_event_input_history: {n} rows upserted")
    return n


def load_timeline(rows):
    """
    INSERT ONLY with dedup via source_key.
    Why: Timeline is an event log — events never change, only new ones arrive.
    source_key (e.g. 'NE_<uuid>') ensures the same event isn't inserted twice
    even if the ETL re-processes due to overlap.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO fact_scenario_timeline (
            scenario_id, event_time, event_type, event_category,
            actor, description, run_id, node_name, event_type_name, source_key
        ) VALUES %s
        ON CONFLICT (source_key) DO NOTHING
    """
    n = executemany_target(sql, rows)
    logger.info(f"  fact_scenario_timeline: {n} events inserted")
    return n
