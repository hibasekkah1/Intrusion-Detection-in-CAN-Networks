/*
=============================================================
Create Database and Schemas
=============================================================
*/

-- Créer la base si elle n'existe pas
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'CAN_IDS')
    CREATE DATABASE CAN_IDS;
GO

USE CAN_IDS;
GO

-- Schémas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'audit')
    EXEC('CREATE SCHEMA audit');
GO

PRINT 'Schémas créés : bronze, silver, gold, audit';

-- Table d'audit pipeline_runs
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pipeline_runs' AND schema_id = SCHEMA_ID('audit'))
BEGIN
    CREATE TABLE audit.pipeline_runs (
        run_id          INT IDENTITY(1,1) PRIMARY KEY,
        step_name       VARCHAR(100) NOT NULL,
        status          VARCHAR(20)  NOT NULL DEFAULT 'STARTED',
        rows_valid      BIGINT       NULL,
        rows_rejected   BIGINT       NULL,
        started_at      DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
        finished_at     DATETIME2    NULL,
        duration_ms     AS DATEDIFF(MILLISECOND, started_at, finished_at)
    );
    PRINT 'Table audit.pipeline_runs créée';
END
ELSE
    PRINT 'audit.pipeline_runs existe déjà';
GO

PRINT '';
PRINT '========================================';
PRINT '  init_database.sql terminé';
PRINT '========================================';
GO




