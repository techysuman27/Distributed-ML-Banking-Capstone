"""Derive final answers and presentation figures from verified execution reports."""
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
def report(name):return json.loads((ROOT/'reports'/name).read_text())
selection=report('model_selection.json');metrics=report('model_metrics.json')
importance=report('model_importance.json');m=metrics['test'];winner=selection['winner']

def answer(index,text,*images):return {'index':index,'answer':text,'images':list(images)}
scores='; '.join(f"{c['model']}: mean CV PR-AUC {c['cv_pr_auc']:.4f}, best parameters {c['best_params']}" for c in selection['candidates'])
evaluation=', '.join(f'{key} {m[key]:.4f}' for key in ['accuracy','precision','recall','f1','roc_auc','pr_auc'])
feature_text='; '.join(f"{r['feature']} = {r['value']:.4f}" for r in importance[:5])
part3=[
answer(450,'I loaded the original 4,521-row, 17-column CSV using an explicit Spark schema. There are 521 yes and 4,000 no labels, no null cells and no duplicate source records. The schema, example rows and label counts are retained in the executed notebook. Unknown string categories are retained. The objective is term-deposit subscription, not future credit default or fraud.','03_q1'),
answer(627,'I split the data before fitting learned transformations. A stable full-row hash gives 3,645 training and 876 held-out rows. Numerical median imputation, StringIndexer with invalid-category handling and OneHotEncoder are fitted inside each training-fold pipeline. pdays=-1 is represented by a prior-contact flag and nullable recency. A fixed signed-log balance transform preserves negative balances while reducing outlier influence; log1p reduces long campaign/previous-contact tails. I do not delete extreme balances without evidence of errors. Duration is excluded because the completed call length is unavailable before a call.','03_q2'),
answer(752,'The pipeline applies fixed SQL transformations, median imputation, categorical indexing and one-hot encoding. VectorAssembler combines the imputed numeric and encoded categorical columns into raw_features; StandardScaler creates features for the classifier. The target and duration are excluded. Saving the complete PipelineModel preserves exactly the same preprocessing for later streaming inference.','03_q3'),
answer(1002,f'I compared logistic regression, an interpretable linear classifier, with a nonlinear decision tree. Both run as Spark ML pipelines across four partitions on two executor cores. The full source-row hash split is fixed before any learned preprocessing. Selection uses three-fold training cross-validation PR-AUC, not held-out scores. {scores}. The selected family is {winner["model"]}. It is refitted on the training data and saved with preprocessing.','03_q4'),
answer(1086,f'The untouched 876-row test set gives {evaluation}. Confusion counts: {m["confusion"]}. The majority baseline accuracy is {metrics["baseline"]["accuracy"]:.4f}; the no-skill PR baseline is the positive prevalence, {metrics["baseline"]["pr_auc_no_skill"]:.4f}. High accuracy alone is misleading. At the unchanged 0.5 threshold, subscriber recall is {m["recall"]:.2%}; the operating threshold would need independent validation before practical use. The test set was not used to tune thresholds. Ranking scores do not establish deployment readiness.','03_q5'),
answer(1184,f'ParamGridBuilder tests logistic regParam 0.01 and 0.1 crossed with elasticNetParam 0 and 1, plus tree maxDepth 3 and 5 with minInstancesPerNode 10. CrossValidator uses three folds, seed 42 and parallelism 1 to fit the laptop budget. Its evaluator ranks positive-class probabilities for both model families using areaUnderPR. Learned preprocessing is inside the CV pipeline. {scores}. The resumed execution explicitly reuses previously completed logistic CV; raw logistic scores and probabilities have equivalent rankings. A fresh reproduction without the resume flag trains all candidates.','03_q6'),
answer(1367,f'The largest absolute coefficients or importances of the selected {winner["model"]} include {feature_text}. For logistic regression the coefficient signs refer to encoded, standardized features; they describe conditional association, not causal effects. Tree importance is impurity based and can favor features with many candidate splits. The complete ranked feature list is retained in model_importance.json. Results are limited by the small sample, no client identifiers and no chronological validation.','03_q7')]

if '--part3' in sys.argv:
    (ROOT/'docs/answers_3.json').write_text(json.dumps({'part3':part3},indent=2),encoding='utf-8')
    print('Part 3 answers generated.')
    raise SystemExit(0)
stream=report('streaming_results.json')
assert all(stream['dropped_by_watermark'][n]>=1 for n in ['bank_window_10s','bank_window_1m'])
windows={n:stream['queries'][n]['rows'] for n in ['bank_window_10s','bank_window_1m']}
window_text='; '.join(f"{name}: "+', '.join(f"count {r['records']}, mean balance {r['avg_balance']:.2f}" for r in rr) for name,rr in windows.items())
part4=[
answer(618,'I replay 17 held-out marketing records as six JSON chunks, with maxFilesPerTrigger=1 and an explicit schema including age, job, balance, duration, y, event ID and synthetic event time. Structured Streaming runs real incremental microbatches. The job aggregation uses count, average balance and average duration in complete output mode, displaying and persisting each update through foreachBatch. Its final counts sum to 17. These are marketing contacts; duration is call duration rather than payment transaction duration. The four queries replay identical input sequentially within the laptop memory budget.','04_q1'),
answer(1122,f'The historical-data model is the {winner["model"]} pipeline selected in Part 3 using training cross-validation. PipelineModel.load restores preprocessing and the classifier without refitting. transform(features(raw)) produces a prediction and a positive-class subscription probability for every arrival. The saved output contains 17 distinct event IDs, all with valid probabilities. Scoring uses held-out records and excludes duration. Prediction events are not dropped by watermark rules because that query has no event-time aggregation.','04_q2'),
answer(1593,f'I use tumbling 10-second and one-minute event-time windows to aggregate counts and average balances. The retained finalized outputs are {window_text}. The first minute groups the eight accepted early events, while the 90-second region groups five contacts in a later minute. The one-minute view smooths the shorter windows by combining their observations. These small synthetic arrivals illustrate mechanics rather than a credible real banking trend. Each query finalizes 13 records; three recent events remain in open windows until the watermark passes their ends. One excessively late event is excluded. availableNow completing the replay does not automatically close every final event-time window.','04_q3'),
answer(1809,f'I set withWatermark(event_time, 20 seconds). The watermark follows the maximum observed event time minus 20 seconds and bounds retained state. Events at seconds 7 and 8 arrive after seconds 12–14 but remain within the accepted lateness range. The deliberately late second-4 event arrives in the sixth microbatch after 90–96-second events and an extra batch for watermark propagation. Actual state-operator progress confirms drops: {stream["dropped_by_watermark"]}. Both window queries drop one row, while all 17 still receive model predictions. The replay was corrected after checking actual counters; timestamp arithmetic alone is not proof of rejection. Watermarking balances bounded state against completeness, rather than guaranteeing that every arbitrarily late event is included.','04_q4')]

if '--part4' in sys.argv:
    (ROOT/'docs/answers_3_4.json').write_text(json.dumps({'part3':part3,'part4':part4},indent=2),encoding='utf-8')
    print('Parts 3 and 4 answers generated.')
    raise SystemExit(0)
bench=report('parallel_benchmark.json')
resources={k:v for k,v in report('parallel_resources.json').items() if k!='series'}
loans=report('parallel_loan_groups.json')['results']
loan_text='; '.join(f"ages {r['age_decade']}–{r['age_decade']+9}: {r['loan_holders']} of {r['clients']} ({r['loan_pct']:.2f}%)" for r in loans)
benchmark_text='; '.join(f"{r['partitions']} partitions: median {r['median_seconds']:.3f} seconds" for r in bench)
part5=[
answer(280,'I load the original CSV into a typed Spark DataFrame and inspect rows and schema. Round-robin repartition(4) distributes records across four partitions without relying on skewed job keys. Two executor cores process these partitions over multiple tasks; groupBy operations shuffle keys as needed. Partitioning determines execution layout and is distinct from the training/test split. The notebook records actual row counts per partition.','05_q1'),
answer(672,f'Distributed groupBy(job).agg(avg(balance)) computes the occupation means, matching the Hadoop/Hive results; retired has the largest sample mean, about 2,319.19. The source contains only a yes/no personal-loan field, so the requested ranking of loan amounts cannot be calculated. My explicitly labelled substitute groups ages into decades and ranks personal-loan-holder counts, retaining group sizes and prevalence: {loan_text}. These values are counts and rates, not currency.','05_q2','05_q2_loans'),
answer(1061,'I train a regularized logistic classifier because it provides an interpretable baseline and supports distributed Spark ML optimization. The same stable hash creates training and held-out sets. Training uses four row partitions, fitted preprocessing and a single global model whose statistics are combined across partitions. Independent models per partition would solve a different problem. The notebook records held-out metrics. Memory pressure and scheduling overhead are controlled through caching, unpersisting completed tables, two executor slots and staged execution.','05_q3'),
answer(1233,f'I sample driver Python/JVM process CPU and resident memory during training and processing, and capture Docker container statistics separately. Driver measurements exclude remote executors. The full sampled summary is {resources}. A controlled benchmark uses 226,050 execution rows created as 50 repeated copies, never added to statistical model evaluation. Three repetitions compare pre-materialized cached layouts: {benchmark_text}. All layouts must return identical rounded grouped results. These timings describe this local run and include scheduling/shuffle overhead; more partitions do not inherently imply faster computation or more independent data.','05_q4','05_q4_resources'),
answer(1396,'Two independent summaries run through ThreadPoolExecutor with two futures. Each thread sets its own Spark job group and FAIR scheduling pool and clears those local properties afterward. Spark schedules tasks across the available executor slots. Retrieving every future propagates failures instead of silently discarding task exceptions. Cached input avoids repeated reading, and results and resource samples are saved before unpersisting and stopping Spark. Concurrent submission demonstrates task management; it does not prove a twofold speed improvement.','05_q5')]

(ROOT/'docs/answers_3_5.json').write_text(json.dumps({'part3':part3,'part4':part4,'part5':part5},indent=2),encoding='utf-8')
summary=f'''# Verified presentation figures

Read these alongside the saved notebook output. Generated from actual reports after validation.

- Source: 4,521 rows; training 3,645; held-out 876.
- Selected model: {winner['model']}. Mean training CV PR-AUC: {winner['cv_pr_auc']:.4f}.
- Held-out evaluation: {evaluation}.
- Confusion matrix: {m['confusion']}.
- Majority accuracy: {metrics['baseline']['accuracy']:.4f}; no-skill PR baseline: {metrics['baseline']['pr_auc_no_skill']:.4f}.
- Most influential features: {feature_text}.
- Streaming: 17 predictions; each window drops one excessively late event; finalized counts total 13 per window query.
- Partition benchmark: {benchmark_text}.

The operating threshold remains 0.5. Discuss recall honestly. The data contains marketing calls, no loan amounts and no year field. Two Spark workers share one laptop. Hadoop uses LocalJobRunner over real HDFS.
'''
(ROOT/'docs/presentation_figures.md').write_text(summary,encoding='utf-8')
print('Final answer plans and presentation figures generated from execution reports.')
