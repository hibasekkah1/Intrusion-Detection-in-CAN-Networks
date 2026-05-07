$PROJECT = "project-e6de9b55-41d5-4f13-ae0"
$REGION = "europe-southwest1"
$JOB_NAME = "can-ids-orchestrator-job"

$SA = "can-ids-orchestrator-sa@project-e6de9b55-41d5-4f13-ae0.iam.gserviceaccount.com"

$IMAGE = "europe-southwest1-docker.pkg.dev/project-e6de9b55-41d5-4f13-ae0/can-ids-containers/can-ids-orchestrator:latest"

$ENV_VARS = "PROJECT_ID=$PROJECT,REGION=$REGION,ZONE=europe-southwest1-a,CLUSTER_NAME=can-ids-spark-cluster,BUCKET_NAME=can-ids-data,RAW_PREFIX=raw/,CONFIG_URI=gs://can-ids-data/config/config.yml,DBC_URI=gs://can-ids-data/dbc/hyundai_2015_ccan.dbc,TEMP_BUCKET=can-ids-data,DELETE_CLUSTER_AT_END=true,INIT_ACTION_URI=gs://can-ids-data/init-actions/install_dependencies.sh"

Write-Host "Vérification existence du Cloud Run Job : $JOB_NAME"

$jobExists = $false

try {
    gcloud run jobs describe $JOB_NAME `
        --region=$REGION `
        --project=$PROJECT `
        --format="value(name)" | Out-Null

    if ($LASTEXITCODE -eq 0) {
        $jobExists = $true
    }
}
catch {
    $jobExists = $false
}

if ($jobExists) {
    Write-Host "Le job existe déjà. Mise à jour du Cloud Run Job..."

    gcloud run jobs update $JOB_NAME `
        --image=$IMAGE `
        --region=$REGION `
        --service-account=$SA `
        --cpu=1 `
        --memory=512Mi `
        --task-timeout=3600 `
        --max-retries=0 `
        --set-env-vars=$ENV_VARS `
        --project=$PROJECT
}
else {
    Write-Host "Le job n'existe pas. Création du Cloud Run Job..."

    gcloud run jobs create $JOB_NAME `
        --image=$IMAGE `
        --region=$REGION `
        --service-account=$SA `
        --cpu=1 `
        --memory=512Mi `
        --task-timeout=3600 `
        --max-retries=0 `
        --set-env-vars=$ENV_VARS `
        --project=$PROJECT
}

Write-Host "Terminé."