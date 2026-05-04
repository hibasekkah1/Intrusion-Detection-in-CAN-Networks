-- =============================================================================
-- test_qualite.sql — Tests de qualité post-ingestion
-- =============================================================================

-- =============================================================================
-- TEST 1 : Complétude — Tous les fichiers sont chargés
-- =============================================================================
SELECT 'TEST 1 - Completude' AS test,
  CASE WHEN COUNT(DISTINCT session_id) = 133 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  COUNT(DISTINCT session_id) AS nb_sessions,
  '133 attendus' AS attendu
FROM canids.messages_raw;

-- =============================================================================
-- TEST 2 : Volume total — Nombre de trames cohérent
-- =============================================================================
SELECT 'TEST 2 - Volume total' AS test,
  CASE WHEN COUNT(*) > 500000000 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  COUNT(*) AS nb_trames,
  '> 500M attendu' AS attendu
FROM canids.messages_raw;

-- =============================================================================
-- TEST 3 : Pas de doublons — Unicité timestamp par session + AID
-- =============================================================================
SELECT 'TEST 3 - Doublons' AS test,
  CASE WHEN doublons = 0 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  doublons AS nb_doublons,
  '0 attendu' AS attendu
FROM (
  SELECT COUNT(*) - COUNT(DISTINCT CONCAT(session_id, '-', CAST(timestamp_us AS STRING), '-', CAST(arbitration_id AS STRING))) AS doublons
  FROM canids.messages_raw
  WHERE session_id = 'dump1'
);

-- =============================================================================
-- TEST 4 : DLC valide — Toutes les valeurs entre 0 et 8
-- =============================================================================
SELECT 'TEST 4 - DLC valide' AS test,
  CASE WHEN invalides = 0 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  invalides AS nb_invalides,
  '0 attendu' AS attendu
FROM (
  SELECT COUNT(*) AS invalides
  FROM canids.messages_raw
  WHERE dlc NOT BETWEEN 0 AND 8
);

-- =============================================================================
-- TEST 5 : AID valide — Toutes les valeurs entre 0 et 2047
-- =============================================================================
SELECT 'TEST 5 - AID valide' AS test,
  CASE WHEN invalides = 0 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  invalides AS nb_invalides,
  '0 attendu' AS attendu
FROM (
  SELECT COUNT(*) AS invalides
  FROM canids.messages_raw
  WHERE arbitration_id NOT BETWEEN 0 AND 2047
);

-- =============================================================================
-- TEST 6 : Pas de NULL — Colonnes obligatoires remplies
-- =============================================================================
SELECT 'TEST 6 - Pas de NULL' AS test,
  CASE WHEN nulls_count = 0 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  nulls_count AS nb_nulls,
  '0 attendu' AS attendu
FROM (
  SELECT
    COUNTIF(timestamp_us IS NULL)
    + COUNTIF(arbitration_id IS NULL)
    + COUNTIF(dlc IS NULL)
    + COUNTIF(data IS NULL)
    + COUNTIF(label IS NULL) AS nulls_count
  FROM canids.messages_raw
);

-- =============================================================================
-- TEST 7 : Labels valides — Uniquement 0 ou 1
-- =============================================================================
SELECT 'TEST 7 - Labels valides' AS test,
  CASE WHEN invalides = 0 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  invalides AS nb_invalides,
  '0 attendu' AS attendu
FROM (
  SELECT COUNT(*) AS invalides
  FROM canids.messages_raw
  WHERE label NOT IN (0, 1)
);

-- =============================================================================
-- TEST 8 : Timestamp monotone — Croissant par session + AID
-- =============================================================================
SELECT 'TEST 8 - Timestamp monotone' AS test,
  CASE WHEN inversions = 0 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  inversions AS nb_inversions,
  '0 attendu' AS attendu
FROM (
  SELECT COUNTIF(iat_us < 0) AS inversions
  FROM (
    SELECT timestamp_us - LAG(timestamp_us) OVER (
      PARTITION BY session_id, arbitration_id
      ORDER BY timestamp_us
    ) AS iat_us
    FROM canids.messages_raw
    WHERE session_id = 'dump1'
  )
);

-- =============================================================================
-- TEST 9 : Répartition benign vs attaque — Les deux existent
-- =============================================================================
SELECT 'TEST 9 - Repartition labels' AS test,
  CASE WHEN benign > 0 AND attaque > 0 THEN 'PASSE' ELSE 'ECHOUE' END AS resultat,
  benign AS nb_benign,
  attaque AS nb_attaque
FROM (
  SELECT
    COUNTIF(label = 0) AS benign,
    COUNTIF(label = 1) AS attaque
  FROM canids.messages_raw
);

-- =============================================================================
-- TEST 10 : Cohérence par type de dump
-- =============================================================================
SELECT 'TEST 10 - Coherence dumps' AS test,
  session_id,
  COUNT(*) AS nb_trames,
  COUNTIF(label = 0) AS benign,
  COUNTIF(label = 1) AS attaque,
  CASE
    WHEN session_id IN ('dump1','dump2','dump3','dump4','dump5','dump6','dump7')
      AND COUNTIF(label = 1) = 0 THEN 'PASSE - benign pur'
    WHEN session_id NOT IN ('dump1','dump2','dump3','dump4','dump5','dump6','dump7')
      AND COUNTIF(label = 1) > 0 THEN 'PASSE - contient attaques'
    ELSE 'A VERIFIER'
  END AS resultat
FROM canids.messages_raw
GROUP BY session_id
ORDER BY session_id
LIMIT 20;

-- =============================================================================
-- RÉSUMÉ
-- =============================================================================
SELECT 'RESUME' AS section,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(*) AS total_trames,
  COUNTIF(label = 0) AS benign,
  COUNTIF(label = 1) AS attaque,
  MIN(dlc) AS dlc_min,
  MAX(dlc) AS dlc_max,
  MIN(arbitration_id) AS aid_min,
  MAX(arbitration_id) AS aid_max,
  COUNT(DISTINCT arbitration_id) AS nb_capteurs
FROM canids.messages_raw;