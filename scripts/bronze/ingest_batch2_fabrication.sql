-- =============================================================================
-- ingest_batch2_fabrication.sql
-- Batch 2/6 : Fabrication (35 fichiers)
-- Idempotent : les fichiers deja charges sont ignores
-- =============================================================================

USE CANIDS;
GO

PRINT '========================================';
PRINT '  Batch 2/6 — Fabrication (35 fichiers)';
PRINT '  ' + CONVERT(VARCHAR(30), SYSUTCDATETIME(), 120);
PRINT '========================================';

CREATE TABLE #files (filename VARCHAR(200));
INSERT INTO #files VALUES
('dump6-fabr-044h'),
('dump6-fabr-080h'),
('dump6-fabr-081h'),
('dump6-fabr-111h'),
('dump6-fabr-112h'),
('dump6-fabr-113h'),
('dump6-fabr-162h'),
('dump6-fabr-18Fh'),
('dump6-fabr-200h'),
('dump6-fabr-220h'),
('dump6-fabr-251h'),
('dump6-fabr-260h'),
('dump6-fabr-2B0h'),
('dump6-fabr-316h'),
('dump6-fabr-329h'),
('dump6-fabr-381h'),
('dump6-fabr-383h'),
('dump6-fabr-386h'),
('dump6-fabr-387h'),
('dump6-fabr-47Fh'),
('dump6-fabr-4F1h'),
('dump6-fabr-50Ch'),
('dump6-fabr-52Ah'),
('dump6-fabr-541h'),
('dump6-fabr-545h'),
('dump6-fabr-547h'),
('dump6-fabr-549h'),
('dump6-fabr-553h'),
('dump6-fabr-555h'),
('dump6-fabr-556h'),
('dump6-fabr-557h'),
('dump6-fabr-58Bh'),
('dump6-fabr-593h'),
('dump6-fabr-5A0h'),
('dump6-fabr-5B0h');

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
PRINT '  Resume batch 2:';
SELECT
    (SELECT COUNT(*) FROM bronze.messages_raw) AS total_valides,
    (SELECT COUNT(*) FROM bronze.messages_rejected) AS total_rejetees;
PRINT 'Batch 2 termine.';
GO
