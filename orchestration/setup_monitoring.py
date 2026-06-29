import argparse
import logging
import os
import sys
import time
from typing import List, Optional

from google.api_core.exceptions import AlreadyExists
from google.cloud import monitoring_v3
from google.protobuf import duration_pb2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("setup_monitoring")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

PROJECT_ID          = os.getenv("PROJECT_ID",          "project-e6de9b55-41d5-4f13-ae0")
REGION              = os.getenv("REGION",              "europe-southwest1")
CLOUD_RUN_JOB       = os.getenv("CLOUD_RUN_JOB",       "can-ids-pipeline")
NOTIFICATION_EMAIL  = os.getenv("NOTIFICATION_EMAIL",  "hiba@example.com")
ALERT_PREFIX        = os.getenv("ALERT_PREFIX",        "CAN IDS")

# Métriques custom — préfixe commun
METRIC_PREFIX = "custom.googleapis.com/can_ids"

# Toutes les métriques utilisées par orchestrate_pipeline.py
METRICS = {
    "pipeline_success":          f"{METRIC_PREFIX}/pipeline_success",
    "pipeline_failure":          f"{METRIC_PREFIX}/pipeline_failure",
    "pipeline_duration_seconds": f"{METRIC_PREFIX}/pipeline_duration_seconds",
    "step_success":              f"{METRIC_PREFIX}/step_success",
    "step_failure":              f"{METRIC_PREFIX}/step_failure",
    "step_duration_seconds":     f"{METRIC_PREFIX}/step_duration_seconds",
}


# ═══════════════════════════════════════════════════════════════
# MÉTRIQUES CUSTOM — push depuis l'orchestrateur
# ═══════════════════════════════════════════════════════════════

def push_metric(metric_type: str, value: float, labels: dict) -> None:
    """
    Pousse une métrique custom à Cloud Monitoring.
    Appelé depuis orchestrate_pipeline.py après chaque step.
    """
    try:
        client  = monitoring_v3.MetricServiceClient()
        project = f"projects/{PROJECT_ID}"

        series = monitoring_v3.TimeSeries()
        series.metric.type = metric_type
        series.metric.labels.update(labels)
        series.resource.type = "global"
        series.resource.labels["project_id"] = PROJECT_ID

        now      = time.time()
        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": int(now), "nanos": 0}}
        )
        point = monitoring_v3.Point({
            "interval": interval,
            "value":    {"double_value": float(value)},
        })
        series.points = [point]
        client.create_time_series(name=project, time_series=[series])
        logger.debug("Metric pushed: %s = %s labels=%s", metric_type, value, labels)
    except Exception as exc:
        # Ne jamais bloquer le pipeline pour une métrique
        logger.warning("Failed to push metric %s: %s", metric_type, exc)


def record_step_success(step: str, duration_s: float) -> None:
    labels = {"step": step, "project_id": PROJECT_ID}
    push_metric(METRICS["step_success"],          1.0,        labels)
    push_metric(METRICS["step_duration_seconds"], duration_s, labels)


def record_step_failure(step: str, duration_s: float) -> None:
    labels = {"step": step, "project_id": PROJECT_ID}
    push_metric(METRICS["step_failure"],          1.0,        labels)
    push_metric(METRICS["step_duration_seconds"], duration_s, labels)


def record_pipeline_success(duration_s: float) -> None:
    push_metric(METRICS["pipeline_success"],          1.0,        {"project_id": PROJECT_ID})
    push_metric(METRICS["pipeline_duration_seconds"], duration_s, {"project_id": PROJECT_ID})


def record_pipeline_failure(duration_s: float) -> None:
    push_metric(METRICS["pipeline_failure"],          1.0,        {"project_id": PROJECT_ID})
    push_metric(METRICS["pipeline_duration_seconds"], duration_s, {"project_id": PROJECT_ID})


# ═══════════════════════════════════════════════════════════════
# CANAL DE NOTIFICATION
# ═══════════════════════════════════════════════════════════════

def create_notification_channel() -> Optional[str]:
    """
    Crée un canal de notification email.
    Retourne le nom du canal pour l'utiliser dans les alertes.
    """
    client  = monitoring_v3.NotificationChannelServiceClient()
    project = f"projects/{PROJECT_ID}"

    channel = monitoring_v3.NotificationChannel(
        type_="email",
        display_name=f"{ALERT_PREFIX} — Alertes Pipeline",
        labels={"email_address": NOTIFICATION_EMAIL},
        enabled=True,
    )
    try:
        created = client.create_notification_channel(
            name=project, notification_channel=channel
        )
        logger.info("Canal email créé : %s → %s", created.name, NOTIFICATION_EMAIL)
        return created.name
    except Exception as exc:
        logger.warning("Canal email non créé (existe peut-être déjà) : %s", exc)
        # Récupérer le canal existant
        for ch in client.list_notification_channels(name=project):
            if ch.labels.get("email_address") == NOTIFICATION_EMAIL:
                logger.info("Canal email existant récupéré : %s", ch.name)
                return ch.name
        return None


# ═══════════════════════════════════════════════════════════════
# ALERTES
# ═══════════════════════════════════════════════════════════════

def _create_alert(
    client: monitoring_v3.AlertPolicyServiceClient,
    project: str,
    name: str,
    filter_str: str,
    threshold: float,
    comparison: str,
    duration_s: int,
    channels: List[str],
    doc: str,
) -> None:
    """Crée une politique d'alerte Cloud Monitoring."""
    condition = monitoring_v3.AlertPolicy.Condition(
        display_name=name,
        condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
            filter=filter_str,
            comparison=monitoring_v3.ComparisonType[comparison],
            threshold_value=threshold,
            duration=duration_pb2.Duration(seconds=duration_s),
            aggregations=[
                monitoring_v3.Aggregation(
                    alignment_period=duration_pb2.Duration(seconds=300),
                    per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
                )
            ],
        ),
    )

    policy = monitoring_v3.AlertPolicy(
        display_name=f"{ALERT_PREFIX} — {name}",
        conditions=[condition],
        combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.OR,
        notification_channels=channels,
        alert_strategy=monitoring_v3.AlertPolicy.AlertStrategy(
            auto_close=duration_pb2.Duration(seconds=86400),
        ),
        documentation=monitoring_v3.AlertPolicy.Documentation(
            content=doc,
            mime_type="text/markdown",
        ),
        enabled=True,
    )

    try:
        client.create_alert_policy(name=project, alert_policy=policy)
        logger.info("Alerte créée : %s", name)
    except AlreadyExists:
        logger.info("Alerte déjà existante : %s", name)
    except Exception as exc:
        logger.warning("Alerte non créée [%s] : %s", name, exc)


def _ensure_metric_descriptors() -> None:
    """
    Crée les descripteurs de métriques custom avant de créer les alertes.
    """
    from google.cloud.monitoring_v3 import MetricServiceClient
    from google.api import metric_pb2

    client  = MetricServiceClient()
    project = f"projects/{PROJECT_ID}"

    for metric_name, metric_type in METRICS.items():
        descriptor = metric_pb2.MetricDescriptor(
            type=metric_type,
            metric_kind=metric_pb2.MetricDescriptor.MetricKind.GAUGE,
            value_type=metric_pb2.MetricDescriptor.ValueType.DOUBLE,
            display_name=metric_name.replace("_", " ").title(),
            description=f"CAN IDS pipeline metric: {metric_name}",
        )
        try:
            client.create_metric_descriptor(name=project, metric_descriptor=descriptor)
            logger.info("Descripteur cree : %s", metric_type)
        except Exception:
            logger.debug("Descripteur deja existant : %s", metric_type)


def deploy_alerts(channels: List[str]) -> None:
    """Déploie les alertes du pipeline."""
    # Créer les descripteurs de métriques d'abord
    _ensure_metric_descriptors()

    client  = monitoring_v3.AlertPolicyServiceClient()
    project = f"projects/{PROJECT_ID}"

    # ── 1. Pipeline failure
    _create_alert(
        client=client,
        project=project,
        name="Pipeline Failure",
        filter_str=f'metric.type="{METRICS["pipeline_failure"]}" AND resource.type="global"',
        threshold=0.5,
        comparison="COMPARISON_GT",
        duration_s=0,
        channels=channels,
        doc=(
            "## Pipeline CAN IDS — ECHEC\n\n"
            "Le pipeline complet a echoue.\n\n"
            "Actions :\n"
            "1. Cloud Run Jobs -> Logs -> chercher STEP FAILED\n"
            "2. Dataproc -> Jobs -> verifier le job en erreur\n"
            "3. BigQuery -> audit.file_processing_status -> status = FAILED\n\n"
            f"Relancer : gcloud run jobs execute {CLOUD_RUN_JOB} --region={REGION} --project={PROJECT_ID}"
        ),
    )

    # ── 2. Step failure
    _create_alert(
        client=client,
        project=project,
        name="Step Failure",
        filter_str=f'metric.type="{METRICS["step_failure"]}" AND resource.type="global"',
        threshold=0.5,
        comparison="COMPARISON_GT",
        duration_s=0,
        channels=channels,
        doc=(
            "## Pipeline CAN IDS — Etape en echec\n\n"
            "Un step Dataproc a echoue.\n\n"
            f"Verifier : gcloud dataproc jobs list --region={REGION} --project={PROJECT_ID}"
        ),
    )

    # ── 3. Durée pipeline > 2h
    _create_alert(
        client=client,
        project=project,
        name="Pipeline Duration > 2h",
        filter_str=f'metric.type="{METRICS["pipeline_duration_seconds"]}" AND resource.type="global"',
        threshold=7200.0,
        comparison="COMPARISON_GT",
        duration_s=0,
        channels=channels,
        doc=(
            "## Pipeline CAN IDS — Duree anormale (> 2h)\n\n"
            "Le pipeline a depasse 2 heures d execution.\n\n"
            "Causes possibles : job Dataproc bloque, cluster sous-dimensionne.\n\n"
            f"Verifier : gcloud dataproc jobs list --region={REGION} --project={PROJECT_ID}"
        ),
    )

    # ── 4. Cloud Run Job error — basé sur métrique native Cloud Run
    # Utiliser la métrique Cloud Run request_count avec filtre sur error
    # au lieu d'un filtre sur les logs (non supporté dans MetricThreshold)
    _create_alert(
        client=client,
        project=project,
        name="Cloud Run Job Task Failed",
        filter_str=(
            f'metric.type="run.googleapis.com/job/completed_task_attempt_count" '
            f'AND resource.type="cloud_run_job" '
            f'AND metric.labels.result="failed"'
        ),
        threshold=0.5,
        comparison="COMPARISON_GT",
        duration_s=0,
        channels=channels,
        doc=(
            f"## Cloud Run Job {CLOUD_RUN_JOB} — Task Failed\n\n"
            "Une tache du Cloud Run Job a echoue.\n\n"
            f"Voir les logs : gcloud logging read resource.type=cloud_run_job --project={PROJECT_ID} --limit=100"
        ),
    )


# ═══════════════════════════════════════════════════════════════
# STATUS
# ═══════════════════════════════════════════════════════════════

def status() -> None:
    """Affiche toutes les alertes Cloud Monitoring du projet."""
    client  = monitoring_v3.AlertPolicyServiceClient()
    project = f"projects/{PROJECT_ID}"

    logger.info("=== Cloud Monitoring Alert Policies ===")
    found = False
    for policy in client.list_alert_policies(name=project):
        if ALERT_PREFIX in policy.display_name:
            found = True
            enabled = " ACTIVE" if policy.enabled else "⏸  DISABLED"
            logger.info("  %s | %s", enabled, policy.display_name)

    if not found:
        logger.info("  Aucune alerte trouvée avec le préfixe '%s'", ALERT_PREFIX)
        logger.info("  Lancer : python setup_monitoring.py --deploy")

    logger.info("")
    logger.info("=== Métriques custom disponibles ===")
    for name, metric_type in METRICS.items():
        logger.info("  %s", metric_type)
    logger.info("")
    logger.info("Voir dans Cloud Console :")
    logger.info("  Monitoring > Metrics Explorer > Filtre : custom.googleapis.com/can_ids/*")


# ═══════════════════════════════════════════════════════════════
# DELETE
# ═══════════════════════════════════════════════════════════════

def delete_alerts() -> None:
    """Supprime toutes les alertes du pipeline."""
    client  = monitoring_v3.AlertPolicyServiceClient()
    project = f"projects/{PROJECT_ID}"

    deleted = 0
    for policy in client.list_alert_policies(name=project):
        if ALERT_PREFIX in policy.display_name:
            client.delete_alert_policy(name=policy.name)
            logger.info("Alerte supprimée : %s", policy.display_name)
            deleted += 1

    if deleted == 0:
        logger.info("Aucune alerte à supprimer.")
    else:
        logger.info("%d alerte(s) supprimée(s).", deleted)


# ═══════════════════════════════════════════════════════════════
# TEST METRIC
# ═══════════════════════════════════════════════════════════════

def test_metric() -> None:
    """
    Pousse une métrique de test pour vérifier que Cloud Monitoring
    reçoit bien les données depuis ce projet.
    """
    logger.info("Envoi d'une métrique de test...")
    push_metric(
        METRICS["pipeline_success"],
        1.0,
        {"project_id": PROJECT_ID}
    )
    push_metric(
        METRICS["step_success"],
        1.0,
        {"step": "test-metric", "project_id": PROJECT_ID}
    )
    logger.info("Métriques de test envoyées.")
    logger.info(
        "Vérifier dans Cloud Console :\n"
        "  Monitoring > Metrics Explorer\n"
        "  Filtre : custom.googleapis.com/can_ids/pipeline_success"
    )


# ═══════════════════════════════════════════════════════════════
# DEPLOY
# ═══════════════════════════════════════════════════════════════

def deploy() -> None:
    """Déploie le canal email et les 4 alertes."""
    logger.info("=== Déploiement Cloud Monitoring ===")
    logger.info("Project        : %s", PROJECT_ID)
    logger.info("Notification   : %s", NOTIFICATION_EMAIL)
    logger.info("Cloud Run Job  : %s", CLOUD_RUN_JOB)

    # Canal de notification
    channel = create_notification_channel()
    channels = [channel] if channel else []

    if not channels:
        logger.warning(
            "Aucun canal de notification configuré. "
            "Les alertes seront créées sans destinataire email."
        )

    # Alertes
    deploy_alerts(channels)

    logger.info("")
    logger.info("=== Monitoring déployé ===")
    logger.info("Alertes actives dans Cloud Console :")
    logger.info("  Monitoring > Alerting > Alert Policies")
    logger.info("Métriques custom :")
    logger.info("  Monitoring > Metrics Explorer > custom.googleapis.com/can_ids/*")


# ═══════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Setup Cloud Monitoring pour le pipeline CAN IDS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--deploy",      action="store_true", help="Créer le canal email et les alertes")
    parser.add_argument("--status",      action="store_true", help="Voir les alertes existantes")
    parser.add_argument("--delete",      action="store_true", help="Supprimer toutes les alertes du pipeline")
    parser.add_argument("--test-metric", action="store_true", help="Pousser une métrique de test")
    args = parser.parse_args()

    if args.deploy:
        deploy()
    elif args.status:
        status()
    elif args.delete:
        delete_alerts()
    elif args.test_metric:
        test_metric()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()