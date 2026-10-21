import json
import os
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(os.environ.get('PROJECT_ROOT', '/workspace'))
SCHEMA = T.StructType([
    T.StructField(name, T.IntegerType() if name in {
        'age', 'balance', 'day', 'duration', 'campaign', 'pdays', 'previous'
    } else T.StringType(), True)
    for name in 'age job marital education default balance housing loan contact day month duration campaign pdays previous poutcome y'.split()
])
CAT = 'job marital education default housing loan contact month poutcome'.split()
NUM = 'age balance day campaign previous previously_contacted pdays_clean'.split()

def session(name):
    spark = (SparkSession.builder.appName(name)
             .master(os.environ.get('SPARK_MASTER', 'local[2]'))
             .config('spark.driver.host', os.environ.get('SPARK_LOCAL_IP', 'localhost'))
             .config('spark.driver.memory', '512m')
             .config('spark.executor.memory', '512m')
             .config('spark.executor.cores', '1')
             .config('spark.cores.max', '2')
             .config('spark.sql.shuffle.partitions', '4')
             .config('spark.default.parallelism', '4')
             .config('spark.scheduler.mode', 'FAIR')
             .config('spark.sql.adaptive.enabled', 'true')
             .config('spark.eventLog.enabled', 'true')
             .config('spark.eventLog.dir', 'file:///workspace/artifacts/spark-events')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark

def load_bank(spark):
    return spark.read.option('header', True).schema(SCHEMA).csv(str(ROOT/'data/bank.csv'))

def features(df):
    # Unknown is a meaningful source category; keep it. Numerical medians are learned in the pipeline.
    df = df.fillna('unknown', subset=CAT)
    return (df.withColumn('previously_contacted', (F.col('pdays') >= 0).cast('double'))
            .withColumn('pdays_clean', F.when(F.col('pdays') >= 0, F.col('pdays')))
            .withColumn('label', F.when(F.col('y') == 'yes', 1.0).when(F.col('y') == 'no', 0.0)))

def save_json(name, value):
    p = ROOT/'reports'/name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(value, indent=2, default=str), encoding='utf-8')

def rows(df):
    return [r.asDict(recursive=True) for r in df.collect()]

def prepare_dirs():
    for p in ['artifacts/spark-events', 'reports', 'models', 'runtime', 'docs/evidence']:
        (ROOT/p).mkdir(parents=True, exist_ok=True)
