from pathlib import Path
import sys
import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT/'notebooks'
OUT.mkdir(exist_ok=True)

def write(name, cells):
    if len(sys.argv)>1 and name not in sys.argv[1:]:return
    nb = nbf.v4.new_notebook(cells=[nbf.v4.new_markdown_cell(s) if t=='m' else nbf.v4.new_code_cell(s) for t,s in cells])
    nb.metadata.kernelspec = {'name':'python3','display_name':'Python 3','language':'python'}
    nb.metadata.language_info = {'name':'python','version':'3.10'}
    nbf.write(nb, OUT/name)

intro = '''**Project:** Financial Forecasting Frontier: Distributed ML  
**Contribution:** Individual — Suman  
**Domain:** Banking marketing analytics  
**Data:** Official AlmaBetter bank.csv, 4,521 client/contact records, 17 columns.  
**Objective:** Explain distributed data processing and estimate term-deposit subscription. This is an educational sample, not a credit eligibility or fraud system.  
The supplied EDA/ML templates guide the summary → business problem → data → transformations → analysis/model → conclusion structure. Part-specific questions below follow the official question documents.
'''
setup = '''import sys
sys.path.insert(0, '/workspace')
from banking.common import *
prepare_dirs()
spark = session(APP_NAME)
df = load_bank(spark).repartition(4).cache()
print('Spark version:', spark.version)
print('Master:', spark.sparkContext.master)
print('Data partitions:', df.rdd.getNumPartitions())
'''
eda = [
('m','# Part 2 — Exploratory Data Analysis with Spark\n'+intro),
('m','## Project summary and business problem\nExplore which client groups and contact strategies are associated with subscription. Use Spark DataFrames and SQL for computation, collecting only small outputs for charts. Rates describe associations in this sample; they do not establish causes. The sample is small enough for local analysis, but the same distributed transformations run on two worker containers here.'),
('c',"APP_NAME='Banking-Spark-EDA'\n"+setup),
('m','## Q1. Know your data: loading, schema, summary, missing values and duplicates'),
('c',"""df.show(5, truncate=False)
df.printSchema()
df.select('age','balance','duration','campaign','pdays','previous').summary().show()
n=df.count()
missing={c:df.filter(F.col(c).isNull()).count() for c in df.columns}
print('Shape:', (n,len(df.columns)), 'Duplicates:', n-df.dropDuplicates().count())
print('Nulls:',missing)
print('Unknown categories:')
df.select([F.sum((F.col(c)=='unknown').cast('int')).alias(c) for c in CAT]).show()
df.groupBy('y').count().show()
assert n==4521 and set(df.columns)==set(SCHEMA.fieldNames())
save_json('data_quality.json',{'rows':n,'columns':len(df.columns),'duplicates':n-df.dropDuplicates().count(),'nulls':missing,'target':rows(df.groupBy('y').count())})
"""),
('m','### Variables and data limitations\nAge is in years; balance is average yearly account balance; housing/loan/default are yes/no flags; duration is call duration in seconds; pdays=-1 means no previous contact. Unknown categories are retained. No customer ID, loan amount, complete date, or year is provided. All y values must be yes/no.'),
('m','## Q2. Filtering and quarter\nMap month abbreviations to month numbers; do not invent a date or year.'),
('c',"""months=['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
month_map=F.create_map(*[v for i,m in enumerate(months,1) for v in (F.lit(m),F.lit(i))])
df=(df.withColumn('month_num',month_map[F.lower(F.col('month'))])
      .withColumn('quarter',F.ceil(F.col('month_num')/3).cast('int')))
df.filter(F.col('balance')>1000).select('age','job','balance','month','quarter').show(10)
print('Balance > 1000:',df.filter(F.col('balance')>1000).count())
df.groupBy('month','month_num','quarter').count().orderBy('month_num').show(12)
"""),
('m','## Q3. GroupBy and aggregation'),
('c',"""job_stats=df.groupBy('job').agg(F.count('*').alias('clients'),F.avg('balance').alias('avg_balance'),F.percentile_approx('age',0.5).alias('median_age')).orderBy(F.desc('avg_balance'))
job_stats.show(truncate=False)
df.filter(F.col('y')=='yes').groupBy('marital').count().show()
save_json('eda_job_stats.json',rows(job_stats))
"""),
('m','## Q4. Age-group UDF\nThe required Python UDF uses <30, 30–60 inclusive, and >60. A built-in CASE expression is preferable in production because it avoids Python serialization overhead.'),
('c',"""@F.udf(T.StringType())
def age_group(age):
    if age is None: return 'unknown'
    return '<30' if age<30 else ('30-60' if age<=60 else '>60')
df=df.withColumn('age_group',age_group('age'))
df.groupBy('age_group').count().show()
"""),
('m','## Q5. Education subscription rate and default prevalence\nThe default field describes existing credit default. Calling this a future loan-default probability would overstate what the data supports. Counts accompany rates to expose small samples.'),
('c',"""education=df.groupBy('education').agg(F.count('*').alias('clients'),F.sum((F.col('y')=='yes').cast('int')).alias('subscribers')).withColumn('subscription_pct',100*F.col('subscribers')/F.col('clients')).orderBy(F.desc('subscription_pct'))
education.show()
defaults=df.groupBy('job').agg(F.count('*').alias('clients'),F.sum((F.col('default')=='yes').cast('int')).alias('default_count')).withColumn('default_pct',100*F.col('default_count')/F.col('clients')).orderBy(F.desc('default_pct'),F.desc('clients'))
defaults.show(3)
save_json('eda_education.json',rows(education))
save_json('eda_default_rates.json',rows(defaults))
"""),
('m','## Q6. String transformations'),
('c',"""df=df.withColumn('job_marital',F.concat_ws('_','job','marital')).withColumn('contact_upper',F.upper('contact'))
df.select('job_marital','contact_upper').show(8,truncate=False)
"""),
('m','## Q7. Visualization\nThe question asks for a Pandas conversion. This sample has only 4,521 rows; the explicit row limit prevents accidental driver exhaustion on a larger dataset. For production, aggregate in Spark first, as the chart below does.'),
('c',"""import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from IPython.display import display,Image
assert n<=10000, 'Collect aggregated results instead of the full dataset at scale.'
sample_pdf=df.toPandas()
print('Pandas sample shape:',sample_pdf.shape)
plot=df.groupBy('job').count().orderBy(F.desc('count')).toPandas()
ax=plot.plot.barh(x='job',y='count',legend=False,color='#197b9c',figsize=(9,5))
ax.set(title='Clients by occupation',xlabel='Client records',ylabel='Occupation')
plt.tight_layout();plt.savefig('/workspace/reports/job_counts.png',dpi=150);display(Image('/workspace/reports/job_counts.png'));plt.close()
"""),
('m','## Q8. Contact volumes, campaign success and duration'),
('c',"""monthly=df.groupBy('month','month_num').agg(F.count('*').alias('contacts'),F.avg((F.col('y')=='yes').cast('double')).alias('subscription_rate')).orderBy(F.desc('contacts'))
monthly.show(12)
duration=df.groupBy('y').agg(F.count('*').alias('clients'),F.avg('duration').alias('avg_call_seconds'))
duration.show()
save_json('eda_monthly.json',rows(monthly))
save_json('eda_duration.json',rows(duration))
"""),
('m','## Q9. Age/balance correlation\nPearson correlation measures linear association and is sensitive to outliers. It does not imply causation.'),
('c',"""correlation=df.stat.corr('age','balance')
print('Age vs balance Pearson r:',correlation)
save_json('eda_correlation.json',{'age_balance_pearson':correlation})
"""),
('m','## Q10. Credit default proportions'),
('c',"""default_counts=df.groupBy('default').count().withColumn('pct',100*F.col('count')/n)
default_counts.show()
plot=default_counts.toPandas()
plot.plot.bar(x='default',y='count',legend=False,color='#197b9c',figsize=(6,4))
plt.title('Existing credit default indicator');plt.ylabel('Client records');plt.tight_layout()
plt.savefig('/workspace/reports/default_counts.png',dpi=150);display(Image('/workspace/reports/default_counts.png'));plt.close()
"""),
('m','## Q11. Contact-method subscription rate'),
('c',"""contact=df.groupBy('contact').agg(F.count('*').alias('contacts'),F.avg((F.col('y')=='yes').cast('double')).alias('subscription_rate')).orderBy(F.desc('subscription_rate'))
contact.show()
save_json('eda_contact.json',rows(contact))
"""),
('m','## Q12. Spark SQL and business interpretation'),
('c',"""df.createOrReplaceTempView('bank_clients')
spark.sql('SELECT age_group,COUNT(*) AS clients,AVG(balance) AS avg_balance FROM bank_clients GROUP BY age_group ORDER BY age_group').show()
spark.sql('SELECT job,COUNT(*) AS clients FROM bank_clients GROUP BY job ORDER BY clients DESC LIMIT 5').show()
print('Busiest month:',monthly.first().asDict())
print('Highest education subscription rate:',education.first().asDict())
print('Most successful recorded contact category:',contact.first().asDict())
print('Interpretation: compare counts and rates, not rates alone. These are sample associations, not causal campaign recommendations.')
df.unpersist()
spark.stop()
"""),
('m','## Conclusion\nSpark performs filtering, grouping and SQL across partitions. Only bounded data is collected for plots. Larger call durations among subscribers can be useful retrospectively, but duration must not enter a pre-call model. The next part fits preprocessing only on training data and evaluates an untouched test split.')]
write('02_spark_eda.ipynb',eda)

ml=[
('m','# Part 3 — Predictive Modeling with Spark ML\n'+intro),
('m','## Summary and business objective\nPredict term-deposit subscription from information available before the current call. Compare a logistic model and a decision tree using training cross-validation, then report held-out performance. This is imbalanced classification: accuracy alone is misleading because predicting no for everyone is already about 88% accurate. Use ROC-AUC, PR-AUC, precision, recall, F1 and a confusion matrix. No financial decision is automated.'),
('c',"APP_NAME='Banking-Spark-ML'\n"+setup),
('m','## Q1. Loading and initial exploration'),
('c',"""df.printSchema();df.show(5,truncate=False)
df.groupBy('y').count().show()
print('Null cells:',sum(df.select([F.sum(F.col(c).isNull().cast('int')).alias(c) for c in df.columns]).first()))
print('Unknown values are valid source categories, not silently deleted.')
"""),
('m','## Q2. Preprocessing and outliers\nSplit before fitting imputers, indexers, encoders or models. Duration is excluded because it becomes known after a call. A fixed signed-log transform reduces the influence of extreme account balances while preserving negative balances; log1p reduces long campaign/previous-contact tails. These are deterministic transforms, so they learn no test statistics. Numerical medians are fitted within each cross-validation pipeline. Unknown categories remain explicit. pdays=-1 becomes a separate previous-contact flag plus nullable recency.'),
('c',"""from banking.modeling import make_pipeline, evaluate, stable_split, transform_statement
df=features(df).filter(F.col('label').isNotNull())
train,test=stable_split(df)
train=train.repartition(4).cache();test=test.repartition(4).cache()
print('Training records:',train.count(),'Held-out records:',test.count())
print('Train labels:');train.groupBy('label').count().show()
print('Test labels:');test.groupBy('label').count().show()
print('Fixed transformations:',transform_statement())
assert 'duration' not in NUM and 'duration' not in CAT and 'y' not in CAT+NUM
"""),
('m','## Q3. Feature engineering and feature vector\nStringIndexer and OneHotEncoder represent categorical features. Imputer fills numeric medians; VectorAssembler combines them; StandardScaler scales for logistic optimization. Every fitted preprocessing stage stays inside Spark ML Pipeline and CrossValidator. Preprocessing can therefore be saved and applied identically to streaming records.'),
('c',"""from pyspark.ml.classification import LogisticRegression,DecisionTreeClassifier
lr=LogisticRegression(featuresCol='features',labelCol='label',maxIter=60)
dt=DecisionTreeClassifier(featuresCol='features',labelCol='label',seed=42)
lr_pipeline=make_pipeline(lr)
print('Pipeline stages:',[type(s).__name__ for s in lr_pipeline.getStages()])
"""),
('m','## Q4 and Q6. Classification, model selection and hyperparameter tuning\nThree-fold CrossValidator uses a fixed seed and PR-AUC to focus on the rare positive class. The tree is an interpretable nonlinear comparison. Cross-validation scores select the winning family; test results never select a model or threshold. Default threshold 0.5 is retained and its limitations are reported.'),
('c',"""from pyspark.ml.tuning import CrossValidator,ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
metric=BinaryClassificationEvaluator(labelCol='label',rawPredictionCol='probability',metricName='areaUnderPR')
candidates=[]
for name,model,grid in [
    ('logistic_regression',lr,ParamGridBuilder().addGrid(lr.regParam,[0.01,0.1]).addGrid(lr.elasticNetParam,[0.0,1.0]).build()),
    ('decision_tree',dt,ParamGridBuilder().addGrid(dt.maxDepth,[3,5]).addGrid(dt.minInstancesPerNode,[10]).build())]:
    cv=CrossValidator(estimator=make_pipeline(model),estimatorParamMaps=grid,evaluator=metric,numFolds=3,seed=42,parallelism=1)
    # Optional resume reuses only the already verified logistic CV result. Logistic raw-score and
    # probability rankings are monotonic equivalents. Fresh reproduction trains every candidate.
    if name=='logistic_regression' and os.environ.get('REUSE_LOGISTIC_CV')=='1':
        from pyspark.ml import PipelineModel
        cached=json.loads((ROOT/'artifacts/resume_lr_cv.json').read_text())
        print('Resuming verified logistic CV; positive-class probability ranking is equivalent:',cached)
        candidates.append((cached,PipelineModel.load(str(ROOT/'artifacts/resume_lr_model'))))
        continue
    fitted=cv.fit(train)
    scores=[float(x) for x in fitted.avgMetrics]
    best=max(range(len(scores)),key=scores.__getitem__)
    summary={'model':name,'cv_pr_auc':scores[best],'grid_scores':scores,'best_params':{p.name:v for p,v in grid[best].items()}}
    print(summary)
    candidates.append((summary,fitted.bestModel))
winner_summary,winner=max(candidates,key=lambda item:item[0]['cv_pr_auc'])
print('Selected using training CV only:',winner_summary)
winner.write().overwrite().save('/workspace/models/subscription_pipeline')
save_json('model_selection.json',{'candidates':[x[0] for x in candidates],'winner':winner_summary,'train_rows':train.count(),'test_rows':test.count(),'seed':42,'threshold':0.5,'excluded':['duration','y'],'split':'xxhash64 full source row, modulo 5; bucket 0 held out'})
"""),
('m','## Q5. Held-out evaluation and baseline\nPR-AUC is an area under the precision–recall curve, not accuracy. The no-skill PR baseline is test prevalence. A majority classifier has zero positive recall, despite high accuracy. Confidence is limited by the small test sample, random source sampling and absent customer identifiers; repeat clients cannot be grouped reliably. No results are interpreted as future default prediction.'),
('c',"""metrics=evaluate(winner.transform(test))
positives=test.filter(F.col('label')==1).count();total=test.count()
baseline={'accuracy':1-positives/total,'positive_recall':0.0,'pr_auc_no_skill':positives/total}
print('Held-out metrics:',metrics)
print('Majority/no-skill baseline:',baseline)
save_json('model_metrics.json',{'selected_model':winner_summary['model'],'test':metrics,'baseline':baseline})
pred=winner.transform(test)
pred.select('label','prediction','probability').show(12,truncate=False)
test.select(SCHEMA.fieldNames()).coalesce(1).write.mode('overwrite').option('header',True).csv('/workspace/artifacts/heldout_source')
"""),
('m','## Q7. Model interpretation\nCoefficients refer to standardized features. They describe association within this model and cannot establish a causal marketing effect. For the tree, importances are impurity based and can favor features with many possible splits.'),
('c',"""from banking.modeling import feature_importance
importance=feature_importance(winner,train)
for row in importance[:15]: print(row)
save_json('model_importance.json',importance)
train.unpersist();test.unpersist();df.unpersist();spark.stop()
"""),
('m','## Conclusion and practical limits\nThe saved Spark Pipeline includes preprocessing and can score structured streaming batches without refitting. Business use would require chronological/customer-group validation, probability calibration, fairness review, threshold selection on independent validation data and outcome monitoring. This educational model has not been approved for real banking use.')]
write('03_spark_ml.ipynb',ml)

parallel=[
('m','# Part 5 — Efficient Data Handling through Data Parallelism\n'+intro),
('m','## Q1. Data loading and partition strategy\nRound-robin repartitioning balances rows across workers without using highly skewed job categories as partition keys. Aggregations still shuffle by key. Four partitions use two available executor cores over multiple tasks. Partitioning changes execution layout, not the statistical train/test split.'),
('c',"APP_NAME='Banking-Parallelism'\n"+setup+"\ndf.show(5)\nprint('Rows per partition:',df.rdd.mapPartitions(lambda it:[sum(1 for _ in it)]).collect())"),
('m','## Q2. Parallel analysis: average balance and the unavailable loan amount\nCompute average balance with a distributed groupBy. The dataset contains only a yes/no loan flag, so ranking loan amounts is impossible. The explicit substitute ranks the five age decades with the most personal-loan holders and also reports prevalence and group size. These counts must never be presented as currency.'),
('c',"""df.groupBy('job').agg(F.avg('balance').alias('avg_balance')).orderBy('job').show()
loan_groups=(df.withColumn('age_decade',(F.floor(F.col('age')/10)*10).cast('int')).groupBy('age_decade')
 .agg(F.count('*').alias('clients'),F.sum((F.col('loan')=='yes').cast('int')).alias('loan_holders'))
 .withColumn('loan_pct',100*F.col('loan_holders')/F.col('clients')).orderBy(F.desc('loan_holders'),'age_decade'))
loan_groups.show(5)
save_json('parallel_loan_groups.json',{'limitation':'No loan amount is present; this ranks counts only.','results':rows(loan_groups.limit(5))})
"""),
('m','## Q3. Train on partitioned data\nA Spark logistic model distributes computation over row partitions and combines gradient statistics. It is not a separate independent model per partition. Use the same stable held-out split as Part 3. Resource monitoring below covers this training and the parallel processing tasks.'),
('c',"""from banking.monitor import ResourceMonitor
monitor=ResourceMonitor().start()
from banking.modeling import make_pipeline,stable_split,evaluate
from pyspark.ml.classification import LogisticRegression
train,test=stable_split(features(df))
trained=make_pipeline(LogisticRegression(maxIter=40,regParam=0.1)).fit(train.repartition(4))
parallel_metrics=evaluate(trained.transform(test))
print('Held-out metrics for partitioned logistic model:',parallel_metrics)
save_json('parallel_model.json',parallel_metrics)
"""),
('m','## Q4. Partition benchmark and resource monitoring\nCompare 1, 2 and 4 partitions over three repetitions on a cached synthetic expansion of the original data (50 repeated copies, 226,050 rows). Repetitions are for demonstrating execution only; they do not create new clients and never enter model evaluation. Materialize each layout before timing to separate repartition cost. Compare results for correctness. Tiny jobs can be slower with more partitions because scheduling and shuffle overhead dominate.'),
('c',"""import time,statistics
expanded=df.crossJoin(spark.range(50).withColumnRenamed('id','replica')).cache()
print('Synthetic benchmark rows:',expanded.count())
results=[];reference=None
for partitions in [1,2,4]:
    table=expanded.repartition(partitions).cache();table.count()
    samples=[]
    for repeat in range(3):
        start=time.perf_counter()
        out=table.groupBy('job').agg(F.round(F.avg('balance'),6).alias('avg_balance')).orderBy('job').collect()
        samples.append(time.perf_counter()-start)
        current=[r.asDict() for r in out]
        if reference is None: reference=current
        assert current==reference, 'Partitioning changed the answer'
    results.append({'partitions':partitions,'seconds':samples,'median_seconds':statistics.median(samples)})
    table.unpersist()
expanded.unpersist()
for item in results:print(item)
save_json('parallel_benchmark.json',results)
"""),
('m','## Q5. Task management and scheduling\nTwo independent summaries run concurrently using Python threads. Each thread sets its own Spark job group and FAIR scheduling pool. Spark schedules tasks across executor slots. Futures propagate failures; both results must complete. This demonstrates concurrency without claiming twice the speed.'),
('c',"""from concurrent.futures import ThreadPoolExecutor
def summary_task(name,column):
    sc=spark.sparkContext
    sc.setLocalProperty('spark.scheduler.pool',name)
    sc.setJobGroup(name,'Parallel '+name)
    try:return name,rows(df.groupBy(column).count().orderBy(column))
    finally:
        sc.setLocalProperty('spark.scheduler.pool',None)
        sc.setLocalProperty('spark.jobGroup.id',None)
with ThreadPoolExecutor(max_workers=2) as executor:
    futures=[executor.submit(summary_task,'education_summary','education'),executor.submit(summary_task,'contact_summary','contact')]
    concurrent=dict(f.result() for f in futures)
print(concurrent)
resource=monitor.stop()
print('Driver process monitor:',resource)
save_json('parallel_resources.json',resource)
save_json('parallel_tasks.json',concurrent)
df.unpersist();spark.stop()
"""),
('m','## Conclusion\nPartitioning, caching, built-in SQL transformations and explicit scheduling make work manageable. Driver CPU/memory samples are not executor measurements; Docker container statistics are collected separately for cluster resource evidence. All worker containers share one physical laptop, so this verifies distributed processes and networking, not multi-machine fault tolerance or production speedup.')]
write('05_data_parallelism.ipynb',parallel)
print('Built EDA, ML, and parallelism notebooks.')

