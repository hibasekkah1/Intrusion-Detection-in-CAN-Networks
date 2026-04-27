-- =============================================================================
-- ingest_setup_aws.sql
-- Configuration PolyBase + AWS S3 (executer une seule fois avant les batches)
-- =============================================================================

USE CAN_IDS;
GO

-- Master Key
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'CanIds2024!SecureKey';
GO

-- Supprimer dans le bon ordre
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'AWSSource')
    DROP EXTERNAL DATA SOURCE AWSSource;
GO
IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'AWSCredential')
    DROP DATABASE SCOPED CREDENTIAL AWSCredential;
GO

-- Credentials AWS
CREATE DATABASE SCOPED CREDENTIAL AWSCredential
WITH IDENTITY = 'S3 Access Key',
     SECRET = 'VOTRE_ACCESS_KEY_ID:VOTRE_SECRET_ACCESS_KEY';
GO

-- Source externe
CREATE EXTERNAL DATA SOURCE AWSSource
WITH (
    LOCATION = 's3://can-ids.s3.eu-north-1.amazonaws.com',
    CREDENTIAL = AWSCredential
);
GO

-- Format Parquet
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFormat')
    CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH (FORMAT_TYPE = PARQUET);
GO

-- Test de connexion
PRINT 'Test de connexion AWS S3...';
SELECT TOP 5 * FROM OPENROWSET(
    BULK '/raw/dump1.parquet',
    DATA_SOURCE = 'AWSSource',
    FORMAT = 'PARQUET'
) AS [test];
PRINT 'Connexion AWS S3 OK';
GO
