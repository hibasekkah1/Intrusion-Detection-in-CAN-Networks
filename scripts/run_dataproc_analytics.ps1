param(
  [string]$ProjectId = "project-e6de9b55-41d5-4f13-ae0",
  [string]$Region = "europe-southwest1",
  [string]$DepsBucket = "can-ids-data",
  [string]$ConfigFile = "gs://can-ids-data/config/config.yml",
  [string]$RuntimeVersion = "2.1"
)
$ErrorActionPreference = "Stop"
function New-BatchId([string]$Prefix) {
  $timestamp = Get-Date -Format "yyyyMMddHHmmss"
  return (($Prefix -replace "[^a-zA-Z0-9-]", "-").ToLower() + "-" + $timestamp)
}
function Run-Batch([string]$Name, [string]$Script) {
  $batchId = New-BatchId $Name
  Write-Host "Running $Name ($batchId)"
  gcloud dataproc batches submit pyspark $Script `
    --project=$ProjectId `
    --region=$Region `
    --batch=$batchId `
    --deps-bucket=$DepsBucket `
    --version=$RuntimeVersion `
    -- `
    --config_path $ConfigFile
  if ($LASTEXITCODE -ne 0) { throw "$Name failed" }
}
Write-Host "Creating BigQuery datasets/audit tables"
python .\spark_jobs\00_create_bigquery_native_schema.py --config_path $ConfigFile

Run-Batch "bronze-analytics" "spark_jobs/01_bronze_analytics_ingestion.py"
Run-Batch "silver-analytics" "spark_jobs/02_silver_analytics_clean.py"
Run-Batch "gold-analytics" "spark_jobs/03_gold_analytics_signal_window_5min.py"
Run-Batch "quality-checks" "spark_jobs/05_quality_checks_datamesh.py"
Write-Host "Analytics pipeline completed"
