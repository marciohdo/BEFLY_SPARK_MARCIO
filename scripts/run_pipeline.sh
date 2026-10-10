#!/usr/bin/env bash
# Executa o pipeline completo: Raw -> Bronze -> Silver -> Gold.
set -euo pipefail

cd "$(dirname "$0")/.."
source env.sh

for step in raw_to_bronze bronze_to_silver silver_to_gold; do
  echo "==> $step"
  spark-submit \
    --driver-java-options "-Dlog4j.configurationFile=utils/log4j2.properties" \
    scripts/$step.py "$@"
done
