# extract.py — reads ONLY from source PostgreSQL (read-only)
# Every function takes a `since` timestamp and returns only NEW/CHANGED rows.
# This is incremental extraction — we never re-read the whole table.

import logging
from db import query_source

logger = logging.getLogger(__name__)

def get_watermark(table_name):
    """Read the last processed timestamp for a given table from TARGET DB."""
    from db import execute_target
    from datetime import datetime, timedelta
    rows = query_source.__module__  # just importing
    # Read from TARGET watermark table
    import psycopg2, psycopg2.extras
    from config import TARGET
    conn = psycopg2.connect(**TARGET)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT last_fetched_at FROM etl_watermark WHERE table_name = %s",
            [table_name]
        )
        row = cur.fetchone()
    conn.close()
    if row:
        # Safety overlap: go back 90 seconds to catch rows written slightly late
        from config import OVERLAP_SEC
        from datetime import timedelta
        return row["last_fetched_at"] - timedelta(seconds=OVERLAP_SEC)
    return datetime(2020, 1, 1)

def update_watermark(table_name, rows_fetched):
    """Update the watermark to NOW after a successful ETL cycle."""
    import psycopg2
    from config import TARGET
    from datetime import datetime
    conn = psycopg2.connect(**TARGET)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE etl_watermark
            SET last_fetched_at = NOW(),
                rows_last_run   = %s,
                last_run_at     = NOW(),
                total_rows_ever = total_rows_ever + %s
            WHERE table_name = %s
        """, [rows_fetched, rows_fetched, table_name])
    conn.commit()
    conn.close()


def extract_scenarios(since):
    """
    Fetch scenarios that were created OR updated since the watermark.
    Pre-joins public.fc_model and public.fc_forecast_init so target needs no joins.
    """
    logger.debug(f"Extracting scenarios since {since}")
    sql = """
        SELECT
            s.id,
            s.scenario_display_name,
            s.status,
            s.is_starter,
            s.currency,
            s.currency_code,
            s.scenario_start_year,
            s.scenario_end_year,
            s.scenario_region_name,
            s.scenario_country_name,
            s.created_at,
            s.created_by,
            s.submitted_at,
            s.submitted_by,
            s.locked_at,
            s.locked_by,
            s.updated_at,
            s.updated_by,
            s.withdraw_at,
            s.withdraw_by,
            s.delete_at,
            -- Model info pre-joined
            m.id                        AS model_id,
            m.model_display_name,
            m.model_type,
            m.model_publish_level,
            m.therapeutic_area_name,
            m.model_disease_area_name   AS disease_area_name,
            m.has_inherent_event        AS loe_enabled,
            m.model_region_display_name AS model_region_name,
            m.model_country_display_name AS model_country_name,
            -- Forecast cycle pre-joined
            fi.forecast_cycle_display_name AS forecast_cycle_name,
            fi.forecast_cycle_start_dt     AS forecast_cycle_start,
            fi.forecast_cycle_end_dt       AS forecast_cycle_end,
            fi.horizon_start_limit,
            fi.horizon_end_limit,
            fi.starter_created
        FROM public.fc_scenario s
        JOIN public.fc_model m           ON s.model_id = m.id
        JOIN public.fc_forecast_init fi  ON s.forecast_init_id = fi.id
        WHERE s.created_at >= %(since)s
           OR s.updated_at >= %(since)s
           OR s.submitted_at >= %(since)s
           OR s.locked_at >= %(since)s
           OR s.withdraw_at >= %(since)s
        LIMIT 5000
    """
    rows = query_source(sql, {"since": since})
    logger.info(f"  Scenarios extracted: {len(rows)}")
    return rows


def extract_node_data(since):
    """
    Fetch node input rows created OR ended since watermark.
    'ended' means end_at was set (previous version closed out).
    Pre-joins node hierarchy (tab, group, node) so Power BI needs no joins.
    """
    logger.debug(f"Extracting node data since {since}")
    sql = """
        SELECT
            nd.id,
            nd.scenario_id,
            nd.model_node_id,
            nd.input_data,          -- JSONB — will be flattened in transform
            nd.input_hash,
            nd.input_validated,
            nd.input_validation_message,
            nd.source,
            nd.created_at           AS version_started_at,
            nd.end_at               AS version_ended_at,
            nd.created_by           AS edited_by,
            -- Node hierarchy pre-joined
            mn.node_display_name,
            mn.node_type,
            mn.node_seq,
            mn.flow,
            mg.group_display_name   AS group_name,
            mg.group_type,
            mg.group_seq,
            mt.tab_display_name     AS tab_name,
            mt.tab_level,
            mt.tab_seq
        FROM public.fc_scenario_node_data nd
        JOIN public.fc_model_node mn        ON nd.model_node_id = mn.id
        JOIN public.fc_model_node_groups mg ON mn.model_node_group_id = mg.id
        JOIN public.fc_model_node_tab mt    ON mg.model_node_tab_id = mt.id
        WHERE nd.created_at >= %(since)s
           OR (nd.end_at IS NOT NULL AND nd.end_at >= %(since)s)
        ORDER BY nd.created_at
        LIMIT 5000
    """
    rows = query_source(sql, {"since": since})
    logger.info(f"  Node data rows extracted: {len(rows)}")
    return rows


def extract_runs(since):
    """
    Fetch runs started OR completed since watermark.
    Pre-aggregates branch + node calc counts so Power BI sees flat numbers.
    """
    logger.debug(f"Extracting runs since {since}")
    sql = """
        SELECT
            sr.id                                                       AS run_id,
            sr.scenario_id,
            sr.run_status,
            sr.run_at,
            sr.run_by,
            sr.run_complete_at,
            ROUND(
                EXTRACT(EPOCH FROM (sr.run_complete_at - sr.run_at)) / 60.0,
                2
            )                                                           AS run_duration_minutes,
            sr.fail_reason,
            COUNT(DISTINCT rb.id)                                       AS branch_count,
            COUNT(nc.id)                                                AS total_nodes_processed,
            SUM(CASE WHEN nc.status = 'success' THEN 1 ELSE 0 END)     AS nodes_success,
            SUM(CASE WHEN nc.status = 'failed'  THEN 1 ELSE 0 END)     AS nodes_failed,
            SUM(CASE WHEN nc.status = 'timeout' THEN 1 ELSE 0 END)     AS nodes_timeout
        FROM public.fc_scenario_run sr
        LEFT JOIN public.fc_scenario_run_branch rb ON rb.scenario_run_id = sr.id
        LEFT JOIN public.fc_scenario_node_calc  nc ON nc.scenario_run_branch_id = rb.id
        WHERE sr.run_at >= %(since)s
           OR (sr.run_complete_at IS NOT NULL AND sr.run_complete_at >= %(since)s)
        GROUP BY sr.id, sr.scenario_id, sr.run_status, sr.run_at,
                 sr.run_by, sr.run_complete_at, sr.fail_reason
        LIMIT 1000
    """
    rows = query_source(sql, {"since": since})
    logger.info(f"  Runs extracted: {len(rows)}")
    return rows


def extract_node_calc(since):
    """
    Fetch node calculation results (outputs) since watermark.
    Pre-joins node name and run branch event tag.
    """
    logger.debug(f"Extracting node calc results since {since}")
    sql = """
        SELECT
            nc.id,
            sr.id                                                       AS run_id,
            sr.scenario_id,
            rb.id                                                       AS branch_id,
            rb.event_tag,
            nc.model_node_id,
            mn.node_display_name,
            mn.node_type,
            nc.status                                                   AS calc_status,
            nc.fail_reason,
            nc.processing_start_at,
            nc.processing_end_at,
            ROUND(
                EXTRACT(EPOCH FROM (nc.processing_end_at - nc.processing_start_at)),
                3
            )                                                           AS processing_duration_s,
            nc.output_data::text                                        AS output_data_text
        FROM public.fc_scenario_node_calc nc
        JOIN public.fc_scenario_run_branch rb ON nc.scenario_run_branch_id = rb.id
        JOIN public.fc_scenario_run sr        ON rb.scenario_run_id = sr.id
        JOIN public.fc_model_node mn          ON nc.model_node_id = mn.id
        WHERE nc.created_at >= %(since)s
        LIMIT 5000
    """
    rows = query_source(sql, {"since": since})
    logger.info(f"  Node calc results extracted: {len(rows)}")
    return rows


def extract_event_data(since):
    """
    Fetch event data rows created OR ended since watermark.
    Pre-joins event type name and segment node names.
    """
    logger.debug(f"Extracting event data since {since}")
    sql = """
        SELECT
            ed.id,
            st.scenario_id,
            et.display_name                 AS event_type_name,
            et.inherent                     AS is_inherent,
            pn.node_display_name            AS population_node_name,
            ppn.node_display_name           AS parent_product_name,
            ed.created_at                   AS version_started_at,
            ed.end_at                       AS version_ended_at,
            ed.created_by                   AS edited_by,
            ed.event_data,                  -- JSONB — flattened in transform
            ed.event_data_hash,
            ed.is_overridden,
            ed.event_shares_overridden::text AS override_data_text,
            ed.is_validated,
            ed.input_validation_message     AS validation_message
        FROM public.fc_scenario_event_data ed
        JOIN public.fc_scenario_event_type st ON ed.scenario_event_type_id = st.id
        JOIN public.fc_event_type et           ON st.event_type_id = et.id
        LEFT JOIN public.fc_model_node pn      ON ed.population_node_id = pn.id
        LEFT JOIN public.fc_model_node ppn     ON ed.parent_product_node_id = ppn.id
        WHERE ed.created_at >= %(since)s
           OR (ed.end_at IS NOT NULL AND ed.end_at >= %(since)s)
        LIMIT 5000
    """
    rows = query_source(sql, {"since": since})
    logger.info(f"  Event data rows extracted: {len(rows)}")
    return rows


def extract_timeline_events(since):
    """
    The UNION ALL timeline query — runs in source DB.
    Returns every type of user action since watermark as flat rows.
    Power BI never needs to do this join — it just reads the pre-built table.
    """
    logger.debug(f"Extracting timeline events since {since}")
    sql = """
        SELECT
            event_time,
            event_type,
            event_category,
            actor,
            description,
            run_id,
            node_name,
            event_type_name,
            scenario_id,
            source_key
        FROM (

            -- 1. Scenario created
            SELECT
                created_at              AS event_time,
                'SCENARIO_CREATED'      AS event_type,
                'LIFECYCLE'             AS event_category,
                created_by              AS actor,
                'Scenario created'      AS description,
                NULL::uuid              AS run_id,
                NULL                    AS node_name,
                NULL                    AS event_type_name,
                id                      AS scenario_id,
                'SC_' || id::text       AS source_key
            FROM public.fc_scenario
            WHERE created_at >= %(since)s

            UNION ALL

            -- 2. Scenario submitted
            SELECT
                submitted_at, 'SUBMITTED', 'LIFECYCLE', submitted_by,
                'Scenario submitted', NULL, NULL, NULL, id,
                'SUBM_' || id::text
            FROM public.fc_scenario
            WHERE submitted_at >= %(since)s AND submitted_at IS NOT NULL

            UNION ALL

            -- 3. Scenario locked
            SELECT
                locked_at, 'LOCKED', 'LIFECYCLE', locked_by,
                'Scenario locked', NULL, NULL, NULL, id,
                'LOCK_' || id::text
            FROM public.fc_scenario
            WHERE locked_at >= %(since)s AND locked_at IS NOT NULL

            UNION ALL

            -- 4. Scenario withdrawn
            SELECT
                withdraw_at, 'WITHDRAWN', 'LIFECYCLE', withdraw_by,
                'Scenario withdrawn', NULL, NULL, NULL, id,
                'WITH_' || id::text
            FROM public.fc_scenario
            WHERE withdraw_at >= %(since)s AND withdraw_at IS NOT NULL

            UNION ALL

            -- 5. Node input edits (every row in append-only table = one edit event)
            SELECT
                nd.created_at,
                'NODE_EDITED',
                'INPUT_CHANGE',
                nd.created_by,
                'Node edited: ' || mn.node_display_name
                    || ' | Validated: ' || nd.input_validated::text,
                NULL,
                mn.node_display_name,
                NULL,
                nd.scenario_id,
                'NE_' || nd.id::text
            FROM public.fc_scenario_node_data nd
            JOIN public.fc_model_node mn ON nd.model_node_id = mn.id
            WHERE nd.created_at >= %(since)s

            UNION ALL

            -- 6. Event data edits
            SELECT
                ed.created_at,
                'EVENT_EDITED',
                'EVENT_CHANGE',
                ed.created_by,
                'Event edited: ' || et.display_name
                    || COALESCE(' | Segment: ' || pn.node_display_name, ''),
                NULL,
                NULL,
                et.display_name,
                st.scenario_id,
                'EVT_' || ed.id::text
            FROM public.fc_scenario_event_data ed
            JOIN public.fc_scenario_event_type st ON ed.scenario_event_type_id = st.id
            JOIN public.fc_event_type et           ON st.event_type_id = et.id
            LEFT JOIN public.fc_model_node pn      ON ed.population_node_id = pn.id
            WHERE ed.created_at >= %(since)s

            UNION ALL

            -- 7. Forecast run triggered
            SELECT
                run_at,
                'RUN_TRIGGERED',
                'RUN',
                run_by,
                'Run started',
                id,
                NULL,
                NULL,
                scenario_id,
                'RT_' || id::text
            FROM public.fc_scenario_run
            WHERE run_at >= %(since)s

            UNION ALL

            -- 8. Forecast run completed (success or failure)
            SELECT
                run_complete_at,
                'RUN_COMPLETED',
                'RUN',
                run_by,
                'Run completed: ' || run_status
                    || COALESCE(' | Error: ' || fail_reason, ''),
                id,
                NULL,
                NULL,
                scenario_id,
                'RC_' || id::text
            FROM public.fc_scenario_run
            WHERE run_complete_at >= %(since)s
              AND run_complete_at IS NOT NULL

        ) timeline
        WHERE event_time IS NOT NULL
        ORDER BY event_time
        LIMIT 10000
    """
    rows = query_source(sql, {"since": since})
    logger.info(f"  Timeline events extracted: {len(rows)}")
    return rows
