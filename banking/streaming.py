"""Replay held-out marketing records through real Structured Streaming queries."""
import csv
import json
import os
import time
from datetime import datetime,timedelta,timezone
from pathlib import Path
from pyspark.sql import functions as F,types as T
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from banking.common import ROOT,SCHEMA,features,rows,save_json

def prepare_replay():
    source=next((ROOT/'artifacts/heldout_source').glob('part-*.csv'))
    records=list(csv.DictReader(source.open()))
    base=datetime(2026,1,1,tzinfo=timezone.utc)
    # Allow a full subsequent microbatch for watermark propagation and state eviction
    # before sending second 4. Lateness alone does not guarantee immediate rejection.
    offsets=[[0,1,2],[12,13,14],[7,8],[90,91,92],[95,96],[4,180,181,182]]
    stream_dir=ROOT/'runtime/stream_input'
    # Unique run directory preserves older checkpoints; restart never reuses input with different data.
    run_id=str(time.time_ns())
    stream_dir=stream_dir/run_id
    stream_dir.mkdir(parents=True)
    samples=ROOT/'data/stream_samples'/run_id
    samples.mkdir(parents=True)
    events=[]
    for batch,times in enumerate(offsets,1):
        batch_records=[]
        for seconds in times:
            i=len(events)
            event={k:(int(records[i][k]) if isinstance(f.dataType,T.IntegerType) else records[i][k]) for k,f in [(f.name,f) for f in SCHEMA.fields]}
            event.update(event_id=f'event_{i:03d}',event_time=(base+timedelta(seconds=seconds)).isoformat(),arrival_batch=batch,synthetic_event_time=True)
            events.append(event);batch_records.append(event)
        content=''.join(json.dumps(x)+'\n' for x in batch_records)
        file=stream_dir/f'batch_{batch:02d}.json'
        file.write_text(content,encoding='utf-8')
        # Spark's file source orders by modification time, with one input file per trigger.
        stamp=base.timestamp()+batch
        os.utime(file,(stamp,stamp))
        (samples/file.name).write_text(content,encoding='utf-8')
    return stream_dir,run_id,events

def run(spark):
    input_dir,run_id,events=prepare_replay()
    schema=T.StructType(SCHEMA.fields+[
        T.StructField('event_id',T.StringType()),T.StructField('event_time',T.TimestampType()),
        T.StructField('arrival_batch',T.IntegerType()),T.StructField('synthetic_event_time',T.BooleanType())])
    raw=spark.readStream.schema(schema).option('maxFilesPerTrigger',1).json(str(input_dir))
    pipeline=PipelineModel.load(str(ROOT/'models/subscription_pipeline'))
    predictions=(pipeline.transform(features(raw)).withColumn('subscription_probability',vector_to_array('probability')[1])
                 .select('event_id','event_time','arrival_batch','y','prediction','subscription_probability'))
    checkpoint=ROOT/'runtime/checkpoints'/run_id
    names={}
    # Run queries sequentially against identical replay to fit a 4 GB local Docker allocation.
    # foreachBatch prints and persists each microbatch's real output, including complete-mode updates.
    def execute(frame,name,mode='append'):
        output=[];snapshots=[]
        def consume(batch,batch_id):
            batch.persist()
            try:
                collected=rows(batch)
                snapshots.append({'batch_id':batch_id,'rows':collected})
                if mode=='complete':output[:]=collected
                else:output.extend(collected)
                print(name,'batch',batch_id,'output rows',len(collected))
                batch.show(25,truncate=False)
                save_json(f'stream_batches/{run_id}/{name}_{batch_id}.json',collected)
            finally:batch.unpersist()
        query=(frame.writeStream.foreachBatch(consume).queryName(name).outputMode(mode)
               .option('checkpointLocation',str(checkpoint/name)).trigger(availableNow=True).start())
        try:
            if not query.awaitTermination(600):
                raise TimeoutError('Streaming replay exceeded 600 seconds')
            progress=[json.loads(json.dumps(p,default=str)) for p in query.recentProgress]
            names[name]={'rows':output,'progress':progress,'batch_outputs':snapshots}
            spark.createDataFrame(output,frame.schema).createOrReplaceTempView(name)
            print(name,':',len(output),'output rows')
            spark.table(name).show(25,truncate=False)
        finally:
            if query.isActive:query.stop()
    execute(predictions,'bank_predictions')
    execute(raw.groupBy('job').agg(F.count('*').alias('records'),F.avg('balance').alias('avg_balance'),F.avg('duration').alias('avg_call_seconds')),'bank_job_live','complete')
    timed=raw.withWatermark('event_time','20 seconds')
    for interval,name in [('10 seconds','bank_window_10s'),('1 minute','bank_window_1m')]:
        aggregate=timed.groupBy(F.window('event_time',interval)).agg(F.count('*').alias('records'),F.avg('balance').alias('avg_balance'))
        execute(aggregate,name)
    drops={name:sum(op.get('numRowsDroppedByWatermark',0) for p in result['progress'] for op in p.get('stateOperators',[])) for name,result in names.items()}
    result={'run_id':run_id,'input_count':len(events),'events':events,'queries':names,'dropped_by_watermark':drops,
            'semantics':'Simulated marketing-record arrivals, not payment transactions. Prediction query scores every arrival. Window queries discard excessively late events. Final event-time windows remain open until watermark passes window end.'}
    save_json('streaming_results.json',result)
    assert len(names['bank_predictions']['rows'])==len(events)
    assert all(x['subscription_probability'] is not None for x in names['bank_predictions']['rows'])
    assert sum(x['records'] for x in names['bank_job_live']['rows'])==len(events)
    assert drops['bank_window_10s']>=1 and drops['bank_window_1m']>=1,'Expected deliberately late event to be dropped'
    print('Late rows dropped:',drops)
    return result

if __name__=='__main__':
    from banking.common import session,prepare_dirs
    prepare_dirs();spark=session('Banking-Structured-Streaming')
    try:run(spark)
    finally:spark.stop()
