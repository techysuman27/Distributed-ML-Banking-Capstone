# Reflection on the distributed banking ML capstone

This project connects storage, SQL analysis, distributed model training and streaming through one banking dataset. The strongest practical lesson is that a working pipeline needs consistent data definitions and verifiable outputs at every stage. A model score alone is not enough to demonstrate that the system is correct.

## Implementation decisions

I used the original AlmaBetter bank.csv and retained its checksum. HDFS stores the source file, Python MapReduce answers grouped questions, and Hive creates a typed ORC table for SQL analysis. Spark runs on a standalone master with two worker containers. The workers share one physical laptop, which demonstrates separate executor processes and networked scheduling without claiming a multi-machine production cluster.

The source has 4,521 records and a strongly imbalanced subscription target. I kept unknown categories rather than deleting them. The source has no complete contact date or year, so I did not invent an annual trend. The loan field is yes/no, so I reported personal-loan counts and rates instead of unsupported loan amounts.

## Challenges and resolutions

The laptop gives Docker about 4 GB of memory. I ran Hadoop/Hive and the Spark workloads in stages to stay within that limit. The initial Hive startup exposed an unsupported logging option and a stale process marker; both were corrected in the startup script. An older package mirror returned unavailable dependency files, so the Hadoop image now copies Python from an official compatible Python image.

I also found that the Hive load could include the CSV header even with a header-skipping table property. An explicit numeric-age and valid-label condition fixed the typed table, and its 4,521-record count now agrees with Spark. A failing local join optimization was replaced by a window-based anomaly calculation.

For modeling, I excluded call duration because it is unavailable before the current marketing call. Preprocessing is inside the Spark ML pipeline and is fitted on training folds. Ranking evaluation uses positive-class probabilities, which makes comparison across different classifiers meaningful. Accuracy is accompanied by precision, recall, F1, ROC-AUC, PR-AUC and a confusion matrix. In this imbalanced problem, high accuracy can coexist with poor subscriber recall.

Streaming uses held-out marketing records with synthetic event timestamps. I distinguished call duration from payment transaction duration, and did not claim a fraud detection system. The replay includes in-order, acceptable out-of-order and excessively late events. Separate query checkpoints and actual watermark-drop counters make these behaviors testable.

## Learning outcomes

HDFS, Hive and Spark have different responsibilities: distributed storage, SQL querying and computation. Spark transformations are lazy; actions trigger execution. Repartitioning changes task layout and is different from splitting data into training and test sets. A saved PipelineModel preserves preprocessing for future batches. Watermarks bound state at the cost of excluding events that arrive too late.

More partitions do not automatically make a small job faster. Scheduling, serialization and shuffling have costs, so performance claims need measurements and correctness checks. Driver process monitoring and worker-container statistics also measure different scopes and should be reported separately.

## Future work

Before any banking use, I would obtain a larger, properly governed dataset with customer identifiers and complete timestamps, validate across time and customer groups, calibrate probabilities and tune the operating threshold using independent validation data. The current educational model should not be used to decide credit eligibility or classify fraud.

A production architecture would add durable streaming sources and sinks, idempotent event identifiers, authentication, monitoring, higher HDFS replication and multiple physical hosts. A reviewed retraining and model-release process would be needed before replacing the model used for streaming predictions.
