# Verified presentation figures

Read these alongside the saved notebook output. Generated from actual reports after validation.

- Source: 4,521 rows; training 3,645; held-out 876.
- Selected model: decision_tree. Mean training CV PR-AUC: 0.3990.
- Held-out evaluation: accuracy 0.8893, precision 0.5000, recall 0.1340, f1 0.2114, roc_auc 0.5797, pr_auc 0.3264.
- Confusion matrix: {'tn': 766, 'fp': 13, 'fn': 84, 'tp': 13}.
- Majority accuracy: 0.8893; no-skill PR baseline: 0.1107.
- Most influential features: poutcome_vec_success = 0.6977; month_vec_oct = 0.1539; day_imputed = 0.0844; housing_vec_yes = 0.0412; balance_log_imputed = 0.0228.
- Streaming: 17 predictions; each window drops one excessively late event; finalized counts total 13 per window query.
- Partition benchmark: 1 partitions: median 0.694 seconds; 2 partitions: median 1.737 seconds; 4 partitions: median 1.235 seconds.

The operating threshold remains 0.5. Discuss recall honestly. The data contains marketing calls, no loan amounts and no year field. Two Spark workers share one laptop. Hadoop uses LocalJobRunner over real HDFS.
