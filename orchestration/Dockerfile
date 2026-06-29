FROM python:3.11-slim

WORKDIR /app

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

ENTRYPOINT ["python", "orchestrate_pipeline.py", "--run-pipeline"]