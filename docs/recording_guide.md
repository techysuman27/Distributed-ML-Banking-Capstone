# Recording guide — Module 2 Distributed ML

Record your own explanation after reviewing the notebooks. The official minimum is 15 minutes; aim for 20–25 minutes. Use the time ranges below as pacing guidance and explain the displayed outputs in your own words. Do not read commands character by character or wait for the full training run during the recording.

## Before recording

Open the submission Drive folder, the five answer documents, the five executed notebooks and the repository README. Keep `reports/model_selection.json`, `reports/model_metrics.json`, `reports/streaming_results.json` and `reports/parallel_benchmark.json` available. Their saved values are the source of truth for your spoken numbers. Zoom until the code and results are readable. Hide personal tabs and notifications. Check your microphone with a short test recording.

The five notebooks are numbered by the project parts, while the original question files use a different download order. Use the descriptive titles when presenting.

## 0:00–2:00 — Introduction and problem

**SHOW:** README, five deliverables and data description.

**SAY:** “Hello, I am Suman. This is my individual Module 2 capstone, Financial Forecasting Frontier: Distributed ML. I use a banking marketing dataset to demonstrate distributed storage, batch analysis, predictive modeling, streaming and parallel processing. The prediction target is whether a client subscribes to a term deposit. My aim is to show a reproducible workflow and explain the evidence behind its results.”

“The original file contains 4,521 records and 17 columns. There are 521 subscriptions and 4,000 non-subscriptions. This imbalance matters because a model predicting no for everyone already has high accuracy. I therefore compare precision, recall, F1, ROC-AUC and PR-AUC instead of relying on accuracy alone.”

Explain three example fields from the schema. Point out that duration measures a marketing call. The dataset does not provide payment transaction records, loan amounts or a year field. Explain why these limits affect later questions.

## 2:00–4:00 — Architecture and data management

**SHOW:** `docs/architecture.md`, cluster screenshot and source checksum in the brief.

**SAY:** “The storage and batch section uses a real HDFS NameNode and DataNode, Python Hadoop Streaming jobs, and Hive SQL. The Spark section uses a standalone master and two worker containers. The workers execute tasks in separate processes over a Docker network, but all containers share one laptop. This demonstrates distribution of tasks; it does not demonstrate resilience across separate physical machines.”

“HDFS replication is one in this demonstration. Hadoop MapReduce uses LocalJobRunner over HDFS rather than a YARN cluster. I state that explicitly so the architecture description matches what actually ran. The container setup and commands are included for reproduction.”

Trace the flow: CSV → HDFS → MapReduce/Hive, then CSV → Spark EDA → training pipeline → saved model → streaming inference. Explain that Spark partitions are execution units and are different from the statistical training/test split. Show the two worker entries in the cluster evidence. Explain why the project runs Hadoop and Spark stages separately within the laptop's memory budget.

## 4:00–7:00 — Part 1: Hadoop and Hive

**SHOW:** 01 Hadoop and Hive Answers and its executed notebook.

**SAY:** “I first created the HDFS input directory and copied the unchanged CSV into it. The HDFS listing and DataNode report verify storage. The MapReduce mapper emits keys and numeric values, and the reducer combines sums and counts. The average is total balance divided by the number of records for each job category, rather than an unweighted average of partial averages.”

Show the five MapReduce outputs. Explain average balance by job, housing-loan counts by education, monthly subscription counts, call duration by previous campaign outcome, and balance by age decade. Point to an example job and its count and mean. Explain why the grouping key differs between tasks.

“Hive first reads raw CSV fields, then creates a typed ORC table. I explicitly filter out the header before converting numeric columns. The final row count is exactly 4,521. This check caught a data-ingestion issue during development.”

Show the table definition, count and first rows. Briefly demonstrate filtering married clients with a personal loan, sorting balances, aggregating age and defaults, and ranking job categories. Retired clients have the highest mean balance in this sample. May has the largest contact volume, 1,398 contacts, but its subscription rate is about 6.65 percent. Previous campaign success is associated with a higher present subscription rate; this is an association rather than evidence that a prior outcome causes a subscription.

“Age and balance have a weak positive linear correlation, about 0.084. The yearly-trend question cannot be answered because no year exists in the source. Average yearly balance describes a field, not a time series. For unusual balances, I report descriptive summaries and flag extreme records relative to their education group. These are statistical flags, not confirmed fraud.”

## 7:00–10:00 — Part 2: Spark exploration

**SHOW:** 02 Spark EDA, schema, quality checks, transformations and both charts.

**SAY:** “Spark loads the file with an explicit schema. I check record counts, nulls, duplicates and the target distribution before interpreting any result. The sample has no null cells and no duplicate rows. The string unknown is retained as a meaningful source category.”

Demonstrate filtering balances above 1,000, mapping month strings to month numbers and quarters, and grouping by job for average balance and median age. Show the subscribed-client counts by marital status. Explain that counts alone depend on the size of the group.

“The age UDF assigns less than 30, 30 through 60, and over 60. There are 482, 3,912 and 127 records respectively. Built-in Spark expressions are preferable for most transformations, but this question specifically asks for a UDF.”

Show education subscription rates and default rates. Tertiary education has about 14.30 percent subscription in this sample. The existing credit-default flag has 76 yes values, about 1.68 percent. It is not a label for a future default prediction model. Explain the string transformations for job/marital status and uppercase contact method.

Show the job-count chart. Explain that computations remain in Spark and only bounded aggregates are collected for plotting. Telephone has a subscription rate of about 14.62 percent, cellular about 14.36 percent and unknown about 4.61 percent; mention the different group sizes. Subscribers have longer average calls, about 553 versus 226 seconds. Explain that this retrospective observation motivates excluding duration from a pre-call prediction model. Finish by showing the temporary view and Spark SQL results.

## 10:00–14:00 — Part 3: Spark ML

**SHOW:** 03 Spark ML, pipeline stages, CV results, held-out metrics and feature interpretation.

**SAY:** “The model estimates subscription from information available before the current call. I exclude duration because the complete call length is only known afterward. The target y never enters the feature vector. A stable hash split creates 3,645 training rows and 876 held-out rows.”

“I split before fitting learned preprocessing. Numerical medians, categorical indexing, one-hot encoding, assembling and scaling remain inside the pipeline used by cross-validation. This prevents the test set from influencing preprocessing. Unknown categories are handled explicitly. The pdays value minus one means no previous contact, so it becomes a separate flag and nullable recency value.”

“I retain negative balances and reduce the influence of extreme values with a signed logarithm. Campaign and previous-contact counts use log-one-plus transforms. These are fixed transformations rather than thresholds learned from the complete dataset.”

Show the actual selected model in `model_selection.json`. Explain the four logistic configurations, the two tree depths, three-fold cross-validation, the fixed seed and selection by average validation PR-AUC. The evaluator uses positive-class probabilities for both model families. A resumed run may reuse the previously completed logistic CV; the notebook states this, while a normal reproduction refits all candidates.

Read the final accuracy, precision, recall, F1, ROC-AUC and PR-AUC from the displayed output. Explain all four confusion-matrix cells with actual counts. Compare accuracy with the majority baseline and PR-AUC with the test positive prevalence. State plainly if recall is low: a useful ranking score does not guarantee useful classifications at threshold 0.5. Explain that the test set was not used to select a threshold or model.

Show the top coefficients or tree importances from the final selected model. Describe their meaning and limitations. Conclude that the pipeline is reusable for streaming, while deployment would require independent validation, calibration, threshold selection and monitoring. Do not describe the model as production ready.

## 14:00–18:00 — Part 4: Structured Streaming

**SHOW:** 04 Spark Streaming, `banking/streaming.py`, sample input batches and retained query progress.

**SAY:** “I replay 17 held-out marketing records in six JSON files, one input file per microbatch. The timestamps and arrival order are synthetic. This is a reproducible streaming demonstration, not a claim to have live bank transactions.”

“The first query loads the saved Spark Pipeline and attaches a subscription prediction and probability to every arrival. It does not refit preprocessing or the classifier on streaming data. All 17 event identifiers receive predictions.”

Show cumulative job counts, average balances and average call durations from the complete-mode query. Explain that complete mode emits updated aggregate state, whereas append mode emits finalized window rows. The four queries replay identical input sequentially to fit the local memory budget. They are real Structured Streaming queries, but their sequential execution is not a concurrent production deployment.

Show the 10-second and one-minute windows. Explain why the one-minute view groups more events and smooths short-term variation. Discuss an actual mean balance from the output, without interpreting this tiny synthetic replay as a real financial trend.

“The watermark is maximum observed event time minus 20 seconds. Events at seconds seven and eight arrive after seconds twelve to fourteen and are still accepted. A deliberately late event at second four arrives only after the watermark has advanced far enough and an additional microbatch has propagated it. Query progress verifies the rejected row rather than relying only on its timestamp.”

Point to the actual `numRowsDroppedByWatermark` counters. Each window query drops one excessively late event and emits closed windows containing 13 records. Three recent records remain in open windows until event time advances; the late record is excluded from the window aggregates. The prediction query still scores all 17 arrivals because it has no watermark filter. Explain the trade-off between memory/state retention and completeness. Finishing the file replay does not itself advance event time to infinity.

## 18:00–21:00 — Part 5: Parallelism and resource management

**SHOW:** 05 Data Parallelism, partition sizes, loan groups, benchmark, resource report and concurrent summaries.

**SAY:** “Round-robin repartitioning spreads rows without depending on a potentially skewed job key. Four partitions run across two executor cores. A groupBy can require a shuffle to combine rows with matching keys. Partitioning changes where rows are processed, while the hash split determines which rows are used for training or testing.”

Show the parallel average balances. Explain that the loan field is yes/no and no monetary loan amount exists. The labelled substitute ranks age decades by the number of personal-loan holders and also reports group size and prevalence. These are counts and percentages, never currency amounts.

“Spark logistic regression trains one distributed model by combining statistics from partitions. It does not train an unrelated model for each partition. I use the same held-out split and keep model evaluation separate from performance benchmarking.”

Show all three timings per partition layout and their medians. The benchmark repeats the data 50 times to obtain 226,050 execution rows. These copies are only a workload and never new independent customers. Each layout is materialized before timing, results are checked for equality and performance is reported as observed. More partitions can add scheduling and shuffle cost, particularly for a small dataset. Do not claim speedup unless the actual medians support it.

Explain driver-process CPU and memory monitoring versus separate container measurements. Driver metrics exclude remote executors. Show the thread-based independent summaries, separate Spark job groups, FAIR pools and futures. Explain how futures surface task failures and why concurrent submission does not guarantee twice the speed.

## 21:00–23:00 — Reflection, validation and conclusion

**SHOW:** Reflective Summary, verification report, run guide and submission folder contents.

**SAY:** “The important lesson was to verify the system at its boundaries: source counts versus Hive counts, independent CSV calculations versus MapReduce means, training-only selection versus test evaluation, and event-time expectations versus actual streaming progress. I corrected a CSV-header ingestion issue and made the watermark replay wait for actual propagation before claiming that late data was rejected.”

“The project demonstrates HDFS, MapReduce, Hive, distributed Spark analysis, a reusable ML pipeline, streaming predictions, watermark behavior and partition scheduling. Its limits include the small source sample, missing dates and customer identifiers, synthetic streaming arrivals and a single physical host.”

Describe the next steps: larger event data with real timestamps, validation grouped by client and time, improved rare-positive recall using independent validation, and cluster deployment across machines. End by opening the folder and identifying each of the five answer documents, five notebooks, source archive, source CSV and reflection. State that the recording accompanies this folder as a separate video link.

## Five likely review questions

1. **Why exclude duration?** A pre-call prediction cannot know the finished call length. Including it would leak information unavailable at the intended decision time.
2. **Why PR-AUC?** Subscriptions are uncommon. PR-AUC emphasizes ranking quality for the positive class. It still needs precision, recall and threshold-specific evaluation alongside it.
3. **How do you know late data was dropped?** The saved Structured Streaming progress reports state-operator watermark drop counters. Timestamp arithmetic alone is insufficient because propagation and state eviction occur across microbatches.
4. **Did four partitions make the job faster?** Refer to the measured medians. The workload and local hardware determine the result; extra scheduling and shuffling may outweigh the benefit.
5. **Is this a real multi-machine banking system?** It uses real distributed Spark processes and real HDFS locally, with all containers on one laptop. It is an educational demonstration with explicit limits, not a production banking service.

## Final submission

Review the recording, confirm audible narration and a duration of at least 15 minutes, upload it to your preferred video location and enable reviewer access. Paste the Drive folder URL into the Project link field and the recording URL into the Video link field on AlmaBetter. Check both links from a signed-out browser before final submission. Do not submit an empty or placeholder video link.
