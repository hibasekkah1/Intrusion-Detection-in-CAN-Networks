-- =============================================================================
-- ingest_batch6_suspension.sql
-- Batch 6/6 : Suspension (35 fichiers)
-- Idempotent : les fichiers deja charges sont ignores
-- =============================================================================

USE CAN_IDS;
GO

PRINT '========================================';
PRINT '  Batch 6/6 — Suspension (35 fichiers)';
PRINT '  ' + CONVERT(VARCHAR(30), SYSUTCDATETIME(), 120);
PRINT '========================================';

CREATE TABLE #files (filename VARCHAR(200));
INSERT INTO #files VALUES
('dump6-susp-044h'),
('dump6-susp-080h'),
('dump6-susp-081h'),
('dump6-susp-111h'),
('dump6-susp-112h'),
('dump6-susp-113h'),
('dump6-susp-162h'),
('dump6-susp-18Fh'),
('dump6-susp-200h'),
('dump6-susp-220h'),
('dump6-susp-251h'),
('dump6-susp-260h'),
('dump6-susp-2B0h'),
('dump6-susp-316h'),
('dump6-susp-329h'),
('dump6-susp-381h'),
('dump6-susp-383h'),
('dump6-susp-386h'),
('dump6-susp-387h'),
('dump6-susp-47Fh'),
('dump6-susp-4F1h'),
('dump6-susp-50Ch'),
('dump6-susp-52Ah'),
('dump6-susp-541h'),
('dump6-susp-545h'),
('dump6-susp-547h'),
('dump6-susp-549h'),
('dump6-susp-553h'),
('dump6-susp-555h'),
('dump6-susp-556h'),
('dump6-susp-557h'),
('dump6-susp-58Bh'),
('dump6-susp-593h'),
('dump6-susp-5A0h'),
('dump6-susp-5B0h');

DECLARE @f VARCHAR(200), @sql NVARCHAR(MAX), @n INT = 0, @total INT;
SELECT @total = COUNT(*) FROM #files;
DECLARE cur CURSOR FOR SELECT filename FROM #files ORDER BY filename;
OPEN cur;
FETCH NEXT FROM cur INTO @f;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @n = @n + 1;

    -- Verifier si ce fichier est deja charge
    IF EXISTS (SELECT 1 FROM bronze.messages_raw WHERE session_id = @f)
    BEGIN
        PRINT '  [' + CAST(@n AS VARCHAR) + '/' + CAST(@total AS VARCHAR) + '] ' + @f + ' — deja charge, skip';
    END
    ELSE
    BEGIN
        PRINT '  [' + CAST(@n AS VARCHAR) + '/' + CAST(@total AS VARCHAR) + '] ' + @f + ' — chargement...';

        -- Trames valides
        SET @sql = '
        INSERT INTO bronze.messages_raw (session_id, timestamp_us, arbitration_id, dlc, data, label)
        SELECT ''' + @f + ''', timestamp, arbitration_id, dlc, data, label
        FROM OPENROWSET(BULK ''/raw/' + @f + '.parquet'', DATA_SOURCE = ''AWSSource'', FORMAT = ''PARQUET'') AS [src]
        WHERE dlc BETWEEN 0 AND 8 AND arbitration_id BETWEEN 0 AND 2047 AND timestamp IS NOT NULL;';
        EXEC sp_executesql @sql;

        -- Trames rejetees
        SET @sql = '
        INSERT INTO bronze.messages_rejected (session_id, timestamp_us, arbitration_id, dlc, data, label, reject_reason)
        SELECT ''' + @f + ''', timestamp, arbitration_id, dlc, data, label,
            CASE
                WHEN dlc NOT BETWEEN 0 AND 8 THEN ''DLC hors bornes [0..8]''
                WHEN arbitration_id NOT BETWEEN 0 AND 2047 THEN ''AID hors bornes 11 bits [0..2047]''
                WHEN timestamp IS NULL THEN ''Timestamp NULL''
            END
        FROM OPENROWSET(BULK ''/raw/' + @f + '.parquet'', DATA_SOURCE = ''AWSSource'', FORMAT = ''PARQUET'') AS [src]
        WHERE dlc NOT BETWEEN 0 AND 8 OR arbitration_id NOT BETWEEN 0 AND 2047 OR timestamp IS NULL;';
        EXEC sp_executesql @sql;
    END

    FETCH NEXT FROM cur INTO @f;
END
CLOSE cur;
DEALLOCATE cur;
DROP TABLE #files;

-- Resume
PRINT '';
PRINT '  Resume batch 6:';
SELECT
    (SELECT COUNT(*) FROM bronze.messages_raw) AS total_valides,
    (SELECT COUNT(*) FROM bronze.messages_rejected) AS total_rejetees;
PRINT 'Batch 6 termine.';
GO
