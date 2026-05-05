#!/bin/bash

set -e

echo "Updating pip..."
python3 -m pip install --upgrade pip

echo "Installing Python packages for Dataproc Spark jobs..."
python3 -m pip install \
  PyYAML \
  google-cloud-storage \
  google-cloud-bigquery \
  cantools \
  great-expectations

echo "Python packages installed successfully."