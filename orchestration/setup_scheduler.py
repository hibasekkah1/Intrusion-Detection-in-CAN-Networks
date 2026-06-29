import argparse
import logging
import os
import sys

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud.scheduler_v1 import (
    CloudSchedulerClient,
    CreateJobRequest,
    DeleteJobRequest,
    GetJobRequest,
    HttpMethod,
    HttpTarget,
    Job,
    OAuthToken,
    RunJobRequest,
    UpdateJobRequest,
)
from google.protobuf import duration_pb2, field_mask_pb2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("setup_scheduler")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

PROJECT_ID          = os.getenv("PROJECT_ID",          "project-e6de9b55-41d5-4f13-ae0")
REGION              = os.getenv("REGION",              "europe-southwest1")
# Cloud Scheduler supporte uniquement certaines régions
# europe-southwest1 n'est pas supporté → utiliser europe-west1
SCHEDULER_LOCATION  = os.getenv("SCHEDULER_LOCATION",  "europe-west1")
CLOUD_RUN_JOB       = os.getenv("CLOUD_RUN_JOB",       "can-ids-pipeline")
SERVICE_ACCOUNT     = os.getenv("SERVICE_ACCOUNT",     f"can-ids-sa@{PROJECT_ID}.iam.gserviceaccount.com")

# Cron : 02h00 UTC tous les jours
# Modifier via variable d'environnement SCHEDULER_CRON
SCHEDULER_CRON      = os.getenv("SCHEDULER_CRON",      "0 2 * * *")
SCHEDULER_JOB_NAME  = os.getenv("SCHEDULER_JOB_NAME",  "can-ids-daily-trigger")
SCHEDULER_TIMEZONE  = os.getenv("SCHEDULER_TIMEZONE",  "UTC")


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _client() -> CloudSchedulerClient:
    return CloudSchedulerClient()


def _parent() -> str:
    return f"projects/{PROJECT_ID}/locations/{SCHEDULER_LOCATION}"


def _job_name() -> str:
    return f"{_parent()}/jobs/{SCHEDULER_JOB_NAME}"


def _cloud_run_url() -> str:
    """
    URL REST pour déclencher un Cloud Run Job via Cloud Scheduler.
    Cloud Scheduler envoie un POST HTTP à cette URL.
    Cloud Run reçoit la requête et lance une nouvelle exécution du job.
    """
    return (
        f"https://{REGION}-run.googleapis.com/apis/run.googleapis.com/v1/"
        f"namespaces/{PROJECT_ID}/jobs/{CLOUD_RUN_JOB}:run"
    )


def _build_job() -> Job:
    """Construit l'objet Cloud Scheduler Job."""
    return Job(
        name=_job_name(),
        description=f"CAN IDS pipeline daily trigger. Cron: {SCHEDULER_CRON} {SCHEDULER_TIMEZONE}. Job: {CLOUD_RUN_JOB}. Region: {REGION}",
        schedule=SCHEDULER_CRON,
        time_zone=SCHEDULER_TIMEZONE,

        # HTTP target → déclenche le Cloud Run Job via l'API REST
        http_target=HttpTarget(
            uri=_cloud_run_url(),
            http_method=HttpMethod.POST,
            oauth_token=OAuthToken(
                service_account_email=SERVICE_ACCOUNT,
                scope="https://www.googleapis.com/auth/cloud-platform",
            ),
        ),

        # Retry en cas d'échec du trigger (pas du pipeline lui-même)
        # retry_count=3, max 1h, backoff 60s→600s
        retry_config={
            "retry_count":          3,
            "max_retry_duration":   {"seconds": 3600},
            "min_backoff_duration": {"seconds": 60},
            "max_backoff_duration": {"seconds": 600},
            "max_doublings":        3,
        },

        # Timeout du trigger HTTP : 30 min max
        attempt_deadline=duration_pb2.Duration(seconds=1800),
    )


# ═══════════════════════════════════════════════════════════════
# ACTIONS
# ═══════════════════════════════════════════════════════════════

def deploy() -> None:
    """Crée ou met à jour le Cloud Scheduler Job."""
    client = _client()
    job    = _build_job()

    try:
        client.create_job(
            request=CreateJobRequest(parent=_parent(), job=job)
        )
        logger.info("Cloud Scheduler Job créé : %s", _job_name())
    except AlreadyExists:
        # Mise à jour si le job existe déjà
        client.update_job(
            request=UpdateJobRequest(
                job=job,
                update_mask=field_mask_pb2.FieldMask(
                    paths=["schedule", "time_zone", "http_target",
                           "retry_config", "attempt_deadline", "description"]
                ),
            )
        )
        logger.info("Cloud Scheduler Job mis à jour : %s", _job_name())

    logger.info("Cron           : %s %s", SCHEDULER_CRON, SCHEDULER_TIMEZONE)
    logger.info("Cloud Run Job  : %s", CLOUD_RUN_JOB)
    logger.info("Trigger URL    : %s", _cloud_run_url())
    logger.info("Service Account: %s", SERVICE_ACCOUNT)


def status() -> None:
    """Affiche le statut du Cloud Scheduler Job."""
    client = _client()
    try:
        job = client.get_job(request=GetJobRequest(name=_job_name()))
        logger.info("=== Cloud Scheduler Job Status ===")
        logger.info("Nom           : %s", job.name)
        logger.info("Cron          : %s", job.schedule)
        logger.info("Timezone      : %s", job.time_zone)
        logger.info("Statut        : %s", Job.State(job.state).name)
        logger.info("Dernier run   : %s", job.last_attempt_time)
        logger.info("Prochain run  : %s", job.schedule_time)
        logger.info("URL cible     : %s", job.http_target.uri)
    except NotFound:
        logger.warning("Cloud Scheduler Job introuvable : %s", _job_name())
        logger.info("Lancer : python setup_scheduler.py --deploy")


def trigger_now() -> None:
    """
    Déclenche immédiatement le Cloud Scheduler Job sans attendre le cron.
    Utile pour tester sans modifier le schedule.
    """
    client = _client()
    try:
        client.run_job(request=RunJobRequest(name=_job_name()))
        logger.info("Cloud Scheduler Job déclenché manuellement : %s", _job_name())
        logger.info("Le Cloud Run Job '%s' va démarrer dans quelques secondes.", CLOUD_RUN_JOB)
        logger.info(
            "Suivre l'exécution :\n"
            "  gcloud run jobs executions list --job=%s --region=%s --project=%s",
            CLOUD_RUN_JOB, REGION, PROJECT_ID
        )
    except NotFound:
        logger.error("Cloud Scheduler Job introuvable : %s", _job_name())
        logger.info("Lancer d'abord : python setup_scheduler.py --deploy")
        sys.exit(1)


def delete() -> None:
    """Supprime le Cloud Scheduler Job."""
    client = _client()
    try:
        client.delete_job(request=DeleteJobRequest(name=_job_name()))
        logger.info("Cloud Scheduler Job supprimé : %s", _job_name())
    except NotFound:
        logger.warning("Cloud Scheduler Job introuvable (déjà supprimé ?) : %s", _job_name())


# ═══════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Setup Cloud Scheduler pour le pipeline CAN IDS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--deploy",      action="store_true", help="Créer ou mettre à jour le scheduler")
    parser.add_argument("--status",      action="store_true", help="Voir le statut du scheduler")
    parser.add_argument("--trigger-now", action="store_true", help="Déclencher maintenant (sans attendre le cron)")
    parser.add_argument("--delete",      action="store_true", help="Supprimer le scheduler")
    args = parser.parse_args()

    if args.deploy:
        deploy()
    elif args.status:
        status()
    elif args.trigger_now:
        trigger_now()
    elif args.delete:
        delete()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()