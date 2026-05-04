-- =============================================================================
-- Ingestion complète
-- =============================================================================

-- Table externe
CREATE OR REPLACE EXTERNAL TABLE canids.staging_external
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://can-ids-data/raw/*.parquet']
);

-- Trames valides → Bronze
INSERT INTO canids.messages_raw (session_id, timestamp_us, arbitration_id, dlc, data, label)
SELECT
  REGEXP_EXTRACT(_FILE_NAME, r'([^/]+)\.parquet$') AS session_id,
  timestamp AS timestamp_us,
  arbitration_id,
  dlc,
  data,
  label
FROM canids.staging_external
WHERE dlc BETWEEN 0 AND 8
  AND arbitration_id BETWEEN 0 AND 2047
  AND timestamp IS NOT NULL;

-- Trames rejetées
INSERT INTO canids.messages_rejected (session_id, timestamp_us, arbitration_id, dlc, data, label, reject_reason)
SELECT
  REGEXP_EXTRACT(_FILE_NAME, r'([^/]+)\.parquet$'),
  timestamp, arbitration_id, dlc, data, label,
  CASE
    WHEN dlc NOT BETWEEN 0 AND 8 THEN 'DLC hors bornes [0..8]'
    WHEN arbitration_id NOT BETWEEN 0 AND 2047 THEN 'AID hors bornes 11 bits'
    WHEN timestamp IS NULL THEN 'Timestamp NULL'
  END
FROM canids.staging_external
WHERE dlc NOT BETWEEN 0 AND 8
   OR arbitration_id NOT BETWEEN 0 AND 2047
   OR timestamp IS NULL;

-- Audit
INSERT INTO canids.pipeline_runs (step_name, status, rows_valid, rows_rejected)
SELECT 'ingest_gcs', 'SUCCESS',
  (SELECT COUNT(*) FROM canids.messages_raw),
  (SELECT COUNT(*) FROM canids.messages_rejected);

-- Vérification
SELECT
  (SELECT COUNT(*) FROM canids.messages_raw) AS total_valides,
  (SELECT COUNT(*) FROM canids.messages_rejected) AS total_rejetees;

-- Cleanup
DROP TABLE canids.staging_external;