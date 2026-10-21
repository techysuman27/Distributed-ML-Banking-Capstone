# Run the distributed banking capstone

Run these commands from the project folder with Docker Desktop running. The supplied Compose project keeps all project services under the banking-distributed-ml name and binds dashboards only to localhost.

## Hadoop and Hive

```powershell
docker compose --profile hadoop build hadoop-hive
docker compose --profile hadoop up -d hadoop-hive
```

Allow HiveServer2 to initialize its metastore. Check readiness without rerunning initialization:

```powershell
docker compose --profile hadoop exec -T hadoop-hive bash -c '(echo > /dev/tcp/localhost/10000) 2>/dev/null'
docker compose --profile hadoop exec -T hadoop-hive bash /workspace/hadoop/run_mapreduce.sh
docker compose --profile hadoop exec -T hadoop-hive bash /workspace/hive/run_hive.sh
docker compose --profile hadoop stop hadoop-hive
```

HDFS dashboard while that service is running: http://localhost:9870

## Spark notebooks

```powershell
docker compose build spark-driver
docker compose up -d spark-master spark-worker-1 spark-worker-2 spark-driver
docker compose exec -T spark-driver python3 scripts/execute_notebooks.py notebooks/02_spark_eda.ipynb notebooks/03_spark_ml.ipynb notebooks/04_spark_streaming.ipynb notebooks/05_data_parallelism.ipynb
```

The model-comparison notebook performs three-fold cross-validation and may take several minutes on a laptop. Do not rerun training during a recording; use the executed notebook and saved results unless you specifically want a fresh full run.

Spark master: http://localhost:8088  
Application UI while a notebook is running: http://localhost:4040

Use the project resource monitor in a separate PowerShell window if desired:

```powershell
./scripts/monitor_containers.ps1
```

## Validate and view evidence

```powershell
docker compose exec -T spark-driver python3 scripts/execute_host_notebook.py
docker compose exec -T spark-driver python3 scripts/verify_results.py
docker compose exec -T spark-driver python3 -m unittest discover -s tests -v
docker compose exec -T spark-driver python3 scripts/build_evidence_pages.py
```

The five executed notebooks contain full output. Reports contain compact JSON results, actual Hadoop/Hive transcripts, streaming query progress, per-batch outputs and resource measurements. Screenshots in docs/evidence show code and recorded outputs from the same files.

## Stop only this project's services

```powershell
docker compose --profile hadoop stop
```

Avoid deleting the HDFS volume if you want to retain the stored data and Hive metastore. Rerunning MapReduce uses a new HDFS output directory; streaming uses a new replay/checkpoint directory, so earlier evidence is not silently reused.
