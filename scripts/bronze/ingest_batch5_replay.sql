-- =============================================================================
-- ingest_batch5_replay.sql
-- Batch 5/6 : Replay (4 fichiers)
-- Idempotent : les fichiers deja charges sont ignores
-- =============================================================================

USE CAN_IDS;
GO

PRINT '========================================';
PRINT '  Batch 5/6 — Replay (4 fichiers)';
PRINT '  ' + CONVERT(VARCHAR(30), SYSUTCDATETIME(), 120);
PRINT '========================================';

CREATE TABLE #files (filename VARCHAR(200));
INSERT INTO #files VALUES
('dump6-repl-0-120'),
('dump6-repl-120-240'),
('dump6-repl-240-360'),
('dump6-repl-360-479.99999');

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
PRINT '  Resume batch 5:';
SELECT
    (SELECT COUNT(*) FROM bronze.messages_raw) AS total_valides,
    (SELECT COUNT(*) FROM bronze.messages_rejected) AS total_rejetees;
PRINT 'Batch 5 termine.';
GO
