# pipeline.py — ties extract → transform → load together for one ETL cycle
# This runs every 30 seconds from scheduler.py

import logging
import time
from datetime import datetime

from extract import (
    get_watermark, update_watermark,
    extract_scenarios, extract_node_data, extract_runs,
    extract_node_calc, extract_event_data, extract_timeline_events
)
from transform import (
    transform_scenarios, transform_node_data, transform_runs,
    transform_node_calc, transform_event_data, transform_timeline
)
from load import (
    load_scenarios, load_node_data, load_runs,
    load_node_calc, load_event_data, load_timeline
)

logger = logging.getLogger(__name__)


def run_cycle():
    """
    One full ETL cycle:
    1. Read watermark (what time did we last process?)
    2. Extract only NEW/CHANGED rows from source since that time
    3. Transform (flatten JSONB, resolve append-only logic)
    4. Load into target DB (upsert / insert with dedup)
    5. Update watermark to now
    Returns total rows processed.
    """
    cycle_start = time.time()
    total_rows  = 0
    logger.info("─" * 60)
    logger.info(f"ETL cycle starting at {datetime.utcnow().isoformat()}")

    # ── Step 1: Scenarios ──────────────────────────────────────────
    try:
        since = get_watermark("public.fc_scenario")
        rows  = extract_scenarios(since)
        if rows:
            transformed = transform_scenarios(rows)
            n = load_scenarios(transformed)
            total_rows += n
        update_watermark("public.fc_scenario", len(rows))
    except Exception as e:
        logger.error(f"  ❌ Scenarios ETL failed: {e}", exc_info=True)

    # ── Step 2: Node input data (append-only — JSONB flattening) ──
    try:
        since = get_watermark("public.fc_scenario_node_data")
        rows  = extract_node_data(since)
        if rows:
            transformed = transform_node_data(rows)
            n = load_node_data(transformed)
            total_rows += n
        update_watermark("public.fc_scenario_node_data", len(rows))
    except Exception as e:
        logger.error(f"  ❌ Node data ETL failed: {e}", exc_info=True)

    # ── Step 3: Runs ───────────────────────────────────────────────
    try:
        since = get_watermark("public.fc_scenario_run")
        rows  = extract_runs(since)
        if rows:
            transformed = transform_runs(rows)
            n = load_runs(transformed)
            total_rows += n
        update_watermark("public.fc_scenario_run", len(rows))
    except Exception as e:
        logger.error(f"  ❌ Runs ETL failed: {e}", exc_info=True)

    # ── Step 4: Node calculation results (outputs) ─────────────────
    try:
        since = get_watermark("public.fc_scenario_node_calc")
        rows  = extract_node_calc(since)
        if rows:
            transformed = transform_node_calc(rows)
            n = load_node_calc(transformed)
            total_rows += n
        update_watermark("public.fc_scenario_node_calc", len(rows))
    except Exception as e:
        logger.error(f"  ❌ Node calc ETL failed: {e}", exc_info=True)

    # ── Step 5: Event data (append-only — JSONB flattening) ────────
    try:
        since = get_watermark("public.fc_scenario_event_data")
        rows  = extract_event_data(since)
        if rows:
            transformed = transform_event_data(rows)
            n = load_event_data(transformed)
            total_rows += n
        update_watermark("public.fc_scenario_event_data", len(rows))
    except Exception as e:
        logger.error(f"  ❌ Event data ETL failed: {e}", exc_info=True)

    # ── Step 6: Timeline (UNION ALL — pre-built event log) ─────────
    try:
        since = get_watermark("timeline")
        rows  = extract_timeline_events(since)
        if rows:
            transformed = transform_timeline(rows)
            n = load_timeline(transformed)
            total_rows += n
        update_watermark("timeline", len(rows))
    except Exception as e:
        logger.error(f"  ❌ Timeline ETL failed: {e}", exc_info=True)

    elapsed = round(time.time() - cycle_start, 2)
    logger.info(f"ETL cycle complete — {total_rows} rows written in {elapsed}s")
    logger.info("─" * 60)
    return total_rows
