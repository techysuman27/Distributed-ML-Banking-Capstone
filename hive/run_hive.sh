#!/usr/bin/env bash
set -euo pipefail
mkdir -p /workspace/reports
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000/default -n root \
  --outputformat=tsv2 --verbose=false -f /workspace/hive/queries.sql \
  > /workspace/reports/hive-results.txt 2> /workspace/reports/hive-execution.log
echo 'Hive question set completed successfully'
