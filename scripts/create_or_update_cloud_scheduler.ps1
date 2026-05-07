$PROJECT = "project-e6de9b55-41d5-4f13-ae0"
$REGION = "europe-southwest1"
$JOB_NAME = "can-ids-pipeline-hourly"

$SA = "can-ids-orchestrator-sa@project-e6de9b55-41d5-4f13-ae0.iam.gserviceaccount.com"

$SCHEDULE = "0 * * * *"
$TIME_ZONE = "Africa/Casablanca"

$URI = "https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/can-ids-orchestrator-job:run"

Write-Host "Verification existence du Cloud Scheduler Job : $JOB_NAME"

$jobExists = $false

gcloud scheduler jobs describe $JOB_NAME `
    --location=$REGION `
    --project=$PROJECT `
    --format="value(name)" 1>$null 2>$null

if ($LASTEXITCODE -eq 0) {
    $jobExists = $true
}

if ($jobExists) {
    Write-Host "Le Scheduler existe deja. Mise a jour..."

    gcloud scheduler jobs update http $JOB_NAME `
        --location=$REGION `
        --schedule=$SCHEDULE `
        --time-zone=$TIME_ZONE `
        --uri=$URI `
        --http-method=POST `
        --oauth-service-account-email=$SA `
        --project=$PROJECT
}
else {
    Write-Host "Le Scheduler n'existe pas. Creation..."

    gcloud scheduler jobs create http $JOB_NAME `
        --location=$REGION `
        --schedule=$SCHEDULE `
        --time-zone=$TIME_ZONE `
        --uri=$URI `
        --http-method=POST `
        --oauth-service-account-email=$SA `
        --project=$PROJECT
}

Write-Host "Termine."
Write-Host ""
Write-Host "Scheduler : $JOB_NAME"
Write-Host "Schedule  : $SCHEDULE"
Write-Host "Timezone  : $TIME_ZONE"
Write-Host "Target    : $URI"