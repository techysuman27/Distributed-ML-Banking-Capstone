"""Build the local streaming notebook; execute after training Part 3."""
import nbformat as nbf
from pathlib import Path
cells=[]
def md(s):cells.append(nbf.v4.new_markdown_cell(s))
def code(s):cells.append(nbf.v4.new_code_cell(s))
md('''# Part 4 — Real-Time Analysis with Spark Structured Streaming
**Project:** Financial Forecasting Frontier: Distributed ML  
**Contribution:** Individual — Suman

## Summary and scope
Replay small chunks of the held-out banking marketing dataset through actual incremental Spark queries. Every event receives a prediction from Part 3's saved pre-call model. Track balances and call durations by job and over 10-second and one-minute windows. Event timestamps are synthetic; source duration describes a marketing call, not transaction latency. No actual fraud labels or payment events are supplied.

## Q1. Stream processing and aggregation
JSON chunk files contain age, job, balance, duration, y and all other source fields. The file source reads one file per microbatch in modification-time order. A complete-mode stateful job-category aggregation updates count, average balance and average call duration after each batch. foreachBatch displays and saves the actual outputs, and temporary views make the retained results inspectable; durable production sinks would be required in production.
''')
code('''import sys
sys.path.insert(0,'/workspace')
from banking.common import *
from banking.streaming import run
prepare_dirs()
spark=session('Banking-Structured-Streaming')
result=run(spark)
''')
md('''## Q2. Real-time model predictions
The saved PipelineModel applies identical imputation, indexing, encoding, scaling and classification to each arrival. It never refits on streamed data. Replay uses only held-out records; label y is included for inspection, never as a feature. Prediction output retains event ID, arrival batch and positive-class probability.
''')
code("spark.table('bank_predictions').orderBy('arrival_batch','event_id').show(25,truncate=False)")
md('''## Q3. Window operations and trend analysis
Compute event-time windows of 10 seconds and one minute with a 20-second watermark. Append output contains finalized windows only. A later event advances the watermark; the last windows remain open rather than being treated as complete prematurely. These chosen time gaps demonstrate changing counts and averages; they do not reveal real historical banking trends.
''')
code("""for name in ['bank_job_live','bank_window_10s','bank_window_1m']:
    print(name)
    spark.table(name).orderBy('job' if name=='bank_job_live' else 'window').show(truncate=False)
print('First three source batches, including in-order and out-of-order arrivals:')
for event in result['events'][:8]:print(event['event_id'],event['event_time'],event['arrival_batch'])
""")
md('''## Q4. Late and out-of-order data
Batch 3 contains seconds 7 and 8 after batch 2 reached second 14: these events are inside the 20-second tolerance and update their earlier window. Batch 6 contains second 4 after batches 4 and 5 advanced event time and finalized that old window. Both window queries must now reject the record. The record at second 4 still receives a prediction because the prediction query does not discard arrivals by event time. Watermark propagation and eviction can lag a microbatch, so being beyond the configured delay does not guarantee immediate rejection. The progress log measures actual drops rather than assuming the demonstration worked.
''')
code("""print('Actual late-row counts:',result['dropped_by_watermark'])
for name in ['bank_window_10s','bank_window_1m']:
    print(name)
    for progress in result['queries'][name]['progress']:
        print('batch',progress['batchId'],'input',progress['numInputRows'],'event time',progress.get('eventTime'),'state',progress.get('stateOperators'))
spark.stop()
""")
md('''## Conclusion and production extensions
All arrivals receive predictions; job summaries update incrementally; window results show the distinct treatment of acceptable and excessive lateness. Replay directories and checkpoints are unique per run to avoid accidentally reusing old state. A production system would use Kafka or an equivalent durable source, authenticated infrastructure, a durable output sink, idempotent event IDs, monitoring, schema validation and a reviewed model release process. Watermark tolerance trades completeness for bounded state.
''')
nb=nbf.v4.new_notebook(cells=cells)
nb.metadata.kernelspec={'name':'python3','display_name':'Python 3','language':'python'}
nbf.write(nb,Path(__file__).resolve().parents[1]/'notebooks/04_spark_streaming.ipynb')
