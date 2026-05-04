-- =============================================================================
-- Création du dataset (data warehouse)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS canids
OPTIONS(location = '${BQ_LOCATION}');