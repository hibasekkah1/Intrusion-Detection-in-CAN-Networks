-- =============================================================================
-- ddl_bronze.sql
-- CAN IDS Pipeline — Tables Bronze
-- =============================================================================

USE CAN_IDS;
GO

-- Table principale : messages CAN bruts validés
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'messages_raw' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.messages_raw (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        session_id      VARCHAR(100)  NOT NULL,
        timestamp_us    BIGINT        NOT NULL,
        arbitration_id  INT           NOT NULL,
        dlc             TINYINT       NOT NULL,
        data            VARBINARY(8)  NOT NULL,
        label           TINYINT       NOT NULL DEFAULT 0,
        ingested_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    -- Index pour les requêtes Silver et Gold
    CREATE INDEX IX_bronze_session ON bronze.messages_raw (session_id, arbitration_id, timestamp_us);

    PRINT 'Table bronze.messages_raw créée';
END
ELSE
    PRINT 'bronze.messages_raw existe déjà';
GO

-- Table des trames rejetées
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'messages_rejected' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.messages_rejected (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        session_id      VARCHAR(100)  NOT NULL,
        timestamp_us    BIGINT        NULL,
        arbitration_id  INT           NULL,
        dlc             TINYINT       NULL,
        data            VARBINARY(8)  NULL,
        label           TINYINT       NULL,
        reject_reason   VARCHAR(200)  NOT NULL,
        rejected_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    PRINT 'Table bronze.messages_rejected créée';
END
ELSE
    PRINT 'bronze.messages_rejected existe déjà';
GO

PRINT '';
PRINT '========================================';
PRINT '  ddl_bronze.sql terminé';
PRINT '  Tables : messages_raw, messages_rejected';
PRINT '========================================';
GO
