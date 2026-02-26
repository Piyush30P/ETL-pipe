# setup_target.py
# Run ONCE to create all tables in your target (writable) DB.
# After this, the ETL fills them continuously.

import psycopg2
from config import TARGET
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_SQL = """

-- ── Watermark table: tracks last processed timestamp per source table ──────
-- This is the BRAIN of the ETL. Every cycle reads from here, every cycle updates it.
CREATE TABLE IF NOT EXISTS etl_watermark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_fetched_at TIMESTAMP    NOT NULL DEFAULT '2020-01-01 00:00:00',
    rows_last_run   INT          DEFAULT 0,
    last_run_at     TIMESTAMP,
    total_rows_ever BIGINT       DEFAULT 0
);

-- Seed with all source tables (safe to re-run — ON CONFLICT DO NOTHING)
INSERT INTO etl_watermark (table_name) VALUES
    ('public.fc_scenario'),
    ('public.fc_scenario_node_data'),
    ('public.fc_scenario_run'),
    ('public.fc_scenario_node_calc'),
    ('public.fc_scenario_event_data'),
    ('public.fc_scenario_run_branch'),
    ('timeline')
ON CONFLICT (table_name) DO NOTHING;


-- ── dim_scenario: one row per scenario, full lifecycle state ─────────────
-- When scenario status changes, this row is UPDATED (not duplicated).
CREATE TABLE IF NOT EXISTS dim_scenario (
    scenario_id              UUID         PRIMARY KEY,
    scenario_display_name    VARCHAR(255),
    scenario_status          VARCHAR(50),
    is_starter               BOOLEAN,
    currency                 VARCHAR(10),
    currency_code            VARCHAR(3),
    scenario_start_year      INT,
    scenario_end_year        INT,
    scenario_region_name     VARCHAR(255),
    scenario_country_name    VARCHAR(255),
    -- Lifecycle timestamps
    created_at               TIMESTAMP,
    created_by               VARCHAR(255),
    submitted_at             TIMESTAMP,
    submitted_by             VARCHAR(255),
    locked_at                TIMESTAMP,
    locked_by                VARCHAR(255),
    updated_at               TIMESTAMP,
    updated_by               VARCHAR(255),
    withdraw_at              TIMESTAMP,
    withdraw_by              VARCHAR(255),
    delete_at                TIMESTAMP,
    -- Model info (pre-joined — no join needed in Power BI)
    model_id                 UUID,
    model_display_name       VARCHAR(255),
    model_type               VARCHAR(100),
    model_publish_level      VARCHAR(50),
    therapeutic_area_name    VARCHAR(255),
    disease_area_name        VARCHAR(255),
    loe_enabled              BOOLEAN,
    model_region_name        VARCHAR(255),
    model_country_name       VARCHAR(255),
    -- Forecast cycle info (pre-joined)
    forecast_cycle_name      VARCHAR(255),
    forecast_cycle_start     TIMESTAMP,
    forecast_cycle_end       TIMESTAMP,
    horizon_start_limit      INT,
    horizon_end_limit        INT,
    starter_created          BOOLEAN,
    -- ETL metadata
    etl_loaded_at            TIMESTAMP    DEFAULT NOW(),
    etl_updated_at           TIMESTAMP    DEFAULT NOW()
);


-- ── fact_scenario_timeline: every user action, chronologically ──────────
-- INSERT ONLY — new events always appended, never overwritten.
-- This is the table that powers the Journey Timeline visual in Power BI.
CREATE TABLE IF NOT EXISTS fact_scenario_timeline (
    id                BIGSERIAL    PRIMARY KEY,
    scenario_id       UUID         NOT NULL,
    event_time        TIMESTAMP    NOT NULL,
    event_type        VARCHAR(50)  NOT NULL,
    -- SCENARIO_CREATED | NODE_EDITED | EVENT_EDITED |
    -- RUN_TRIGGERED | RUN_COMPLETED | SUBMITTED | LOCKED | WITHDRAWN
    event_category    VARCHAR(30),
    -- LIFECYCLE | INPUT_CHANGE | RUN | EVENT_CHANGE
    actor             VARCHAR(255),
    description       TEXT,
    run_id            UUID,
    node_name         VARCHAR(255),
    event_type_name   VARCHAR(100),
    -- Dedup key: prevents same event being inserted twice if ETL overlaps
    source_key        VARCHAR(255) UNIQUE,
    etl_loaded_at     TIMESTAMP    DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_timeline_scenario_time
    ON fact_scenario_timeline(scenario_id, event_time);
CREATE INDEX IF NOT EXISTS idx_timeline_event_type
    ON fact_scenario_timeline(event_type, event_category);


-- ── fact_node_input_history: every version of every node input ──────────
-- INSERT ONLY for new versions. UPDATE when end_at gets set (version ended).
-- is_current_version=TRUE means this is the live value right now.
CREATE TABLE IF NOT EXISTS fact_node_input_history (
    id                   BIGSERIAL    PRIMARY KEY,
    source_id            UUID         UNIQUE,  -- public.fc_scenario_node_data.id
    scenario_id          UUID         NOT NULL,
    model_node_id        UUID         NOT NULL,
    -- Node context (pre-joined from public.fc_model_node + groups + tabs)
    node_display_name    VARCHAR(255),
    node_type            VARCHAR(100),
    tab_name             VARCHAR(255),
    tab_level            INT,
    group_name           VARCHAR(255),
    group_type           VARCHAR(100),
    node_seq             INT,
    flow                 VARCHAR(100),
    -- Version tracking
    version_started_at   TIMESTAMP,
    version_ended_at     TIMESTAMP,
    is_current_version   BOOLEAN      DEFAULT TRUE,
    edited_by            VARCHAR(255),
    input_hash           VARCHAR(255),
    -- Validation state
    input_validated      BOOLEAN,
    validation_message   TEXT,
    data_source          VARCHAR(255),
    -- JSONB keys flattened into typed columns (Power BI can filter/join on these)
    inp_value            NUMERIC,
    inp_unit             VARCHAR(100),
    inp_start_year       INT,
    inp_end_year         INT,
    inp_input_type       VARCHAR(100),
    inp_timeframe        VARCHAR(100),
    inp_dosing_type      VARCHAR(100),
    inp_actuals_flag     BOOLEAN,
    inp_curve_type       VARCHAR(50),
    inp_selected_output  VARCHAR(50),
    inp_pfs_flag         BOOLEAN,
    inp_ppc_flag         BOOLEAN,
    -- Full JSON as text for Power BI tooltip / reference
    input_data_full_text TEXT,
    etl_loaded_at        TIMESTAMP    DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_node_hist_scenario
    ON fact_node_input_history(scenario_id, model_node_id, is_current_version);
CREATE INDEX IF NOT EXISTS idx_node_hist_current
    ON fact_node_input_history(is_current_version) WHERE is_current_version = TRUE;


-- ── fact_run_summary: one row per forecast run ──────────────────────────
-- UPSERT — run starts as IN_PROGRESS, later becomes SUCCESS or FAILED.
-- Same run_id, status changes → UPDATE existing row.
CREATE TABLE IF NOT EXISTS fact_run_summary (
    run_id                   UUID         PRIMARY KEY,
    scenario_id              UUID         NOT NULL,
    run_status               VARCHAR(30),
    run_at                   TIMESTAMP,
    run_by                   VARCHAR(255),
    run_complete_at          TIMESTAMP,
    run_duration_minutes     NUMERIC(10,2),
    fail_reason              TEXT,
    branch_count             INT          DEFAULT 0,
    total_nodes_processed    INT          DEFAULT 0,
    nodes_success            INT          DEFAULT 0,
    nodes_failed             INT          DEFAULT 0,
    nodes_timeout            INT          DEFAULT 0,
    node_edits_since_prev_run INT         DEFAULT 0,
    event_edits_since_prev_run INT        DEFAULT 0,
    etl_loaded_at            TIMESTAMP    DEFAULT NOW(),
    etl_updated_at           TIMESTAMP    DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_run_scenario
    ON fact_run_summary(scenario_id, run_at);


-- ── fact_node_calc_results: outputs per node per run branch ─────────────
-- INSERT ONLY — results don't change once written.
CREATE TABLE IF NOT EXISTS fact_node_calc_results (
    id                    BIGSERIAL    PRIMARY KEY,
    source_id             UUID         UNIQUE,  -- public.fc_scenario_node_calc.id
    run_id                UUID         NOT NULL,
    scenario_id           UUID         NOT NULL,
    branch_id             UUID,
    event_tag             VARCHAR(50),
    model_node_id         UUID,
    node_display_name     VARCHAR(255),
    node_type             VARCHAR(100),
    calc_status           VARCHAR(30),
    fail_reason           TEXT,
    processing_start_at   TIMESTAMP,
    processing_end_at     TIMESTAMP,
    processing_duration_s NUMERIC(10,3),
    output_data_text      TEXT,
    etl_loaded_at         TIMESTAMP    DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_calc_run
    ON fact_node_calc_results(run_id, scenario_id, calc_status);


-- ── fact_event_input_history: every version of event data ───────────────
CREATE TABLE IF NOT EXISTS fact_event_input_history (
    id                     BIGSERIAL    PRIMARY KEY,
    source_id              UUID         UNIQUE,
    scenario_id            UUID         NOT NULL,
    event_type_name        VARCHAR(100),
    is_inherent            BOOLEAN,
    population_node_name   VARCHAR(255),
    parent_product_name    VARCHAR(255),
    version_started_at     TIMESTAMP,
    version_ended_at       TIMESTAMP,
    is_current_version     BOOLEAN      DEFAULT TRUE,
    edited_by              VARCHAR(255),
    event_data_hash        VARCHAR(255),
    is_overridden          BOOLEAN,
    override_data_text     TEXT,
    is_validated           BOOLEAN,
    validation_message     TEXT,
    -- JSONB keys flattened
    evt_year               INT,
    evt_share_value        NUMERIC,
    evt_entry_quarter      VARCHAR(10),
    evt_erosion_rate       NUMERIC,
    evt_launch_date        VARCHAR(50),
    evt_steady_state       NUMERIC,
    evt_sob_value          NUMERIC,
    event_data_full_text   TEXT,
    etl_loaded_at          TIMESTAMP    DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_evt_scenario
    ON fact_event_input_history(scenario_id, is_current_version);

"""

def setup():
    conn = psycopg2.connect(**TARGET)
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        logger.info("✅ Target schema created successfully")
        logger.info("   Tables created:")
        for t in ["etl_watermark","dim_scenario","fact_scenario_timeline",
                  "fact_node_input_history","fact_run_summary",
                  "fact_node_calc_results","fact_event_input_history"]:
            logger.info(f"   → {t}")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Schema setup failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    setup()
