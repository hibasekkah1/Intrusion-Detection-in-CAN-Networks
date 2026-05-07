#!/bin/bash

set -e

echo "Creating local wheelhouse..."
mkdir -p /tmp/wheelhouse

echo "Copying Python wheels from GCS..."
gsutil -m cp gs://can-ids-data/wheelhouse/* /tmp/wheelhouse/

echo "Installing cantools offline..."
python3 -m pip install --no-index --find-links=/tmp/wheelhouse cantools

echo "Testing imports..."
python3 -c "import cantools; print('cantools OK')"
python3 -c "import yaml; print('PyYAML OK')"
python3 -c "import google.cloud.storage; print('google-cloud-storage OK')"

echo "Python packages installed successfully."