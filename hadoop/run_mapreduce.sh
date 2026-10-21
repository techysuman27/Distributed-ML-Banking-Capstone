#!/usr/bin/env bash
set -euo pipefail
mkdir -p /workspace/reports/hadoop
exec > >(tee /workspace/reports/hadoop/execution.log) 2>&1
set -x
hdfs dfs -mkdir -p /banking/raw
hdfs dfs -put -f /workspace/data/bank.csv /banking/raw/bank.csv
hdfs dfs -ls /banking/raw
hdfs dfsadmin -report
run_id=$(date +%Y%m%d%H%M%S)
for mode in job_balance education_housing month_subscription outcome_duration age_balance; do
  reducer_mode=mean
  if [ "$mode" = education_housing ] || [ "$mode" = month_subscription ]; then reducer_mode=count; fi
  hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -D mapreduce.framework.name=local -D mapreduce.job.name="Banking_${mode}" \
    -D mapreduce.job.reduces=1 \
    -files /workspace/hadoop/mapper.py,/workspace/hadoop/reducer.py \
    -mapper "python3 mapper.py $mode" -reducer "python3 reducer.py $reducer_mode" \
    -input /banking/raw/bank.csv -output "/banking/mapreduce/$run_id/$mode"
  hdfs dfs -cat "/banking/mapreduce/$run_id/$mode/part-*" | tee "/workspace/reports/hadoop/$mode.tsv"
done
