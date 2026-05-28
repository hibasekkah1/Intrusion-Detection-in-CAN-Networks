FROM python:3.11-slim

WORKDIR /app

# Sans versions fixes — pip résout les conflits automatiquement
RUN pip install --no-cache-dir \
    google-cloud-bigquery \
    google-cloud-dataproc \
    google-cloud-monitoring \
    google-cloud-scheduler \
    google-cloud-storage \
    pyyaml \
    protobuf

COPY orchestrate_pipeline.py .
COPY setup_scheduler.py .
COPY setup_monitoring.py .

# Cloud Run Job → lance le pipeline au démarrage
ENTRYPOINT ["python", "orchestrate_pipeline.py", "--run-pipeline"]