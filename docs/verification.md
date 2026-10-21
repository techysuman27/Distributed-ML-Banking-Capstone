# Verification record

All five notebooks executed successfully. `reports/verification.json` records the verification timestamp and checks. Three independent MapReduce unit tests pass, and all Python source files compile.

Verification checks the original CSV SHA256 and 4,521 × 17 dimensions, missing/duplicate counts, independent CSV versus Hadoop aggregates, Hive's typed row count, selection by training cross-validation, and held-out metric bounds and confusion totals.

Streaming checks cover 17 unique predictions with valid probabilities, independent job-level counts and means, exactly one watermark rejection for each window query, and independent counts and mean balances for every finalized window. Closed windows contain 13 records; three later records remain open. The parallelism notebook asserts identical grouped answers across layouts and stores three timing repetitions for each of 1, 2 and 4 partitions.

The five native answer documents contain 40 answer groups and 45 code/output screenshots. Their exported PDFs were rendered and visually inspected, along with the two-page reflective summary. Original questions are retained. The final parallelism document's trailing blank page was removed.

The selected depth-3 tree has training CV PR-AUC 0.3990 and held-out PR-AUC 0.3264. Precision is 0.50 and recall 0.1340 at threshold 0.5; accuracy equals the majority baseline. These limits appear in the answers, README and presentation material.

Partition timing medians were 0.694 seconds (1 partition), 1.737 seconds (2) and 1.235 seconds (4). This local benchmark shows no speed benefit from extra partitions. Driver-process and separate container measurements have their scopes identified.

Hadoop uses LocalJobRunner over HDFS with replication 1. Spark uses two worker containers on one laptop. Streaming timestamps are synthetic. No multi-machine availability, production banking, fraud-detection, loan-amount or annual-trend results are claimed.

The student still needs to review the work, record a presentation of at least 15 minutes, upload its video link and complete the AlmaBetter submission. No video or final submission is represented as completed.
