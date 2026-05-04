-- =============================================================================
-- Création des tables dans le dataset canids
-- =============================================================================

-- Table Bronze : trames CAN valides
CREATE TABLE IF NOT EXISTS canids.messages_raw (
  session_id STRING,
  timestamp_us INT64 NOT NULL,
  arbitration_id INT64 NOT NULL,
  dlc INT64 NOT NULL,
  data BYTES NOT NULL,
  label INT64 NOT NULL,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table : trames rejetées
CREATE TABLE IF NOT EXISTS canids.messages_rejected (
  session_id STRING,
  timestamp_us INT64,
  arbitration_id INT64,
  dlc INT64,
  data BYTES,
  label INT64,
  reject_reason STRING,
  rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table audit
CREATE TABLE IF NOT EXISTS canids.pipeline_runs (
  step_name STRING NOT NULL,
  status STRING NOT NULL,
  rows_valid INT64,
  rows_rejected INT64,
  executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
