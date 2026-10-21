# Distributed ML Banking Capstone

AlmaBetter Module 2 project **Financial Forecasting Frontier: Distributed ML**.

The project uses the supplied banking marketing dataset to demonstrate HDFS storage, Python Hadoop MapReduce, Hive SQL, Spark EDA, Spark ML, Structured Streaming and data parallelism. It contains five executed notebooks, source code, real execution transcripts, screenshots and question-by-question documentation.

## Start here

- [Architecture and build sequence](docs/architecture.md)
- [Run guide](docs/run_guide.md)
- [Reflective summary](docs/reflective_summary.md)
- [Recording guide](docs/recording_guide.md)
- [Verified presentation figures](docs/presentation_figures.md)
- [Verification record](docs/verification.md)
- [Final submission checklist](docs/submission_checklist.md)
- [Official requirements and submission checklist](requirements/project_brief.md)
- [Submission links](requirements/submission_links.json)

## The five parts

| Part | Main deliverable | What it demonstrates |
|---|---|---|
| 1 | [Hadoop and Hive notebook](notebooks/01_hadoop_hive.ipynb) | HDFS ingestion, five Python MapReduce jobs, typed Hive ORC table and banking queries |
| 2 | [Spark EDA notebook](notebooks/02_spark_eda.ipynb) | Schema and quality checks, transformations, UDF, aggregation, charts and Spark SQL |
| 3 | [Spark ML notebook](notebooks/03_spark_ml.ipynb) | Training-only preprocessing, logistic/tree comparison, cross-validation and held-out evaluation |
| 4 | [Spark Streaming notebook](notebooks/04_spark_streaming.ipynb) | Incremental predictions, job summaries, event-time windows and watermark behavior |
| 5 | [Data parallelism notebook](notebooks/05_data_parallelism.ipynb) | Partition strategies, parallel training, timing, resource monitoring and FAIR task scheduling |

The first notebook reads retained Hadoop/Hive terminal outputs. The corresponding programs were executed in the local Hadoop/Hive container, not inside Colab. All four Spark notebooks run through a real standalone master with two worker containers.

## Data and interpretation

The original AlmaBetter CSV has **4,521 records and 17 fields**. There are 521 subscriptions and 4,000 non-subscriptions, no duplicate records and no null cells. The source file is [data/bank.csv](data/bank.csv); its download URL and SHA256 checksum are recorded in [the brief](requirements/project_brief.md).

- No year or full date exists, so annual contact trends cannot be calculated.
- The loan field is yes/no, not a loan amount. The parallel analysis explicitly substitutes loan-holder counts and rates by age decade.
- Duration is a marketing-call duration. It is excluded from pre-call prediction to prevent leakage.
- Existing default flags are not future default labels. This project does not build a credit eligibility or fraud detection system.
- Streaming timestamps and arrival order are synthetic. Streaming replays held-out marketing records rather than actual payment transactions.

## Execution scope

The Hadoop demonstration uses one NameNode and one DataNode with HDFS replication 1. MapReduce and Hive use the local MapReduce engine over HDFS. It is not a YARN deployment. Spark uses separate networked executor processes on two worker containers sharing one physical laptop. This establishes real task distribution while making no claim of multi-machine availability or production scale.

The dataset is small. Partition benchmarks measure scheduling and shuffle overhead as well as computation, and repeated synthetic copies are never used to inflate model evaluation.

## Model results

Three-fold training cross-validation selects a depth-3 decision tree, with mean PR-AUC **0.3990**, over logistic regression at **0.3657**. Selection uses positive-class probabilities for both families. The stable split has 3,645 training rows and 876 held-out rows.

| Held-out metric | Result |
|---|---:|
| Accuracy | 0.8893 |
| Precision | 0.5000 |
| Recall | 0.1340 |
| F1 | 0.2114 |
| ROC-AUC | 0.5797 |
| PR-AUC | 0.3264 |

Confusion counts are TN 766, FP 13, FN 84 and TP 13. Accuracy equals the majority baseline; the no-skill PR baseline is 0.1107. The unchanged 0.5 threshold misses most subscribers. This is a limited educational model, and the test set was not used to tune the threshold. The strongest tree importance is previous campaign success; importance does not imply causation.

## Reproduce

Docker Desktop and Docker Compose are required. Follow the [run guide](docs/run_guide.md) to run Hadoop/Hive first and Spark second within a small laptop memory budget.

```powershell
docker compose up -d spark-master spark-worker-1 spark-worker-2 spark-driver
docker compose exec -T spark-driver python3 scripts/execute_notebooks.py notebooks/02_spark_eda.ipynb notebooks/03_spark_ml.ipynb notebooks/04_spark_streaming.ipynb notebooks/05_data_parallelism.ipynb
docker compose exec -T spark-driver python3 scripts/verify_results.py
```

During execution, the Spark master is available at `http://localhost:8088`, and the current application UI at `http://localhost:4040`. Host ports bind to localhost only. Do not expose these educational services publicly.

## Evidence and submission

[Open the complete Drive submission folder](https://drive.google.com/drive/folders/1bZle80G-EToAdZvMIGnYqyVfpfDt5S01). It contains the five answer documents, reflective summary, executed notebooks, original data, complete source archive, replay samples, trained pipeline and recording guide. Anyone with the link can view it.

Full outputs are stored in the notebooks and reports. [Screenshots](docs/evidence) show actual code and retained execution output. [Streaming samples](data/stream_samples) demonstrate the replay inputs. Runtime checkpoints, trained model files and event logs are kept outside version control; models can be regenerated by executing Part 3.

The official checklist requires a Drive folder containing all five parts, source files, data, streaming samples and a reflective summary, plus a separate presentation link. The video must last **at least 15 minutes**. Final recording and dashboard submission remain actions for the student after reviewing and understanding the implementation.
