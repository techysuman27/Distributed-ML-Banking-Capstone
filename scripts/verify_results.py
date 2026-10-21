"""Validate cross-tool consistency and key statistical/streaming claims."""
import csv
import json
import math
from pathlib import Path
import statistics
import hashlib
from collections import defaultdict
from datetime import datetime,timezone

ROOT=Path(__file__).resolve().parents[1]
def read(name):return json.loads((ROOT/'reports'/name).read_text())
source=list(csv.DictReader((ROOT/'data/bank.csv').read_text().splitlines()))
assert len(source)==4521
assert len(source[0])==17
assert hashlib.sha256((ROOT/'data/bank.csv').read_bytes()).hexdigest()=='c7764aef15407fa186de79c2628f42ab28845b10a9390493f60e984ca4eff4ca'
quality=read('data_quality.json')
assert quality['rows']==len(source) and quality['duplicates']==0
assert sum(quality['nulls'].values())==0
assert 'Q2_count\t4521' in (ROOT/'reports/hive-results.txt').read_text()
for job,count,mean in csv.reader((ROOT/'reports/hadoop/job_balance.tsv').read_text().splitlines(),delimiter='\t'):
    values=[int(r['balance']) for r in source if r['job']==job]
    assert int(count)==len(values) and abs(float(mean)-statistics.mean(values))<1e-5
for mode in ['education_housing','month_subscription']:
    assert sum(int(row[1]) for row in csv.reader((ROOT/f'reports/hadoop/{mode}.tsv').read_text().splitlines(),delimiter='\t'))==4521
selection=read('model_selection.json')
assert selection['train_rows']+selection['test_rows']==4521
assert selection['winner']['cv_pr_auc']==max(x['cv_pr_auc'] for x in selection['candidates'])
metrics=read('model_metrics.json')['test']
assert sum(metrics['confusion'].values())==selection['test_rows']
for key in ['accuracy','precision','recall','f1','roc_auc','pr_auc']:
    assert math.isfinite(metrics[key]) and 0<=metrics[key]<=1
stream=read('streaming_results.json')
assert stream['input_count']==17
predictions=stream['queries']['bank_predictions']['rows']
assert len(predictions)==len({p['event_id'] for p in predictions})==17
assert all(0<=p['subscription_probability']<=1 for p in predictions)
assert {p['event_id'] for p in predictions}=={e['event_id'] for e in stream['events']}
assert sum(row['records'] for row in stream['queries']['bank_job_live']['rows'])==17
for row in stream['queries']['bank_job_live']['rows']:
    events=[event for event in stream['events'] if event['job']==row['job']]
    assert row['records']==len(events)
    assert abs(row['avg_balance']-statistics.mean(event['balance'] for event in events))<1e-9
    assert abs(row['avg_call_seconds']-statistics.mean(event['duration'] for event in events))<1e-9
for interval,name in [(10,'bank_window_10s'),(60,'bank_window_1m')]:
    assert stream['dropped_by_watermark'][name]==1
    assert any(batch['numInputRows']>0 for batch in stream['queries'][name]['progress'])
    assert sum(row['records'] for row in stream['queries'][name]['rows'])==13
    # Independently derive finalized groups: first 13 are accepted and closed;
    # event 13 is too late, and events 14-16 remain in open final windows.
    expected=defaultdict(list)
    for event in stream['events'][:13]:
        stamp=int(datetime.fromisoformat(event['event_time']).timestamp())
        expected[stamp//interval*interval].append(event['balance'])
    actual=stream['queries'][name]['rows']
    assert len(actual)==len(expected)
    for row in actual:
        start=datetime.fromisoformat(row['window']['start']).replace(tzinfo=timezone.utc)
        values=expected[int(start.timestamp())]
        assert row['records']==len(values)
        assert abs(row['avg_balance']-statistics.mean(values))<1e-9
benchmark=read('parallel_benchmark.json')
assert [b['partitions'] for b in benchmark]==[1,2,4]
assert all(len(b['seconds'])==3 and min(b['seconds'])>0 for b in benchmark)
notebooks=list((ROOT/'notebooks').glob('*.ipynb'))
assert len(notebooks)==5
for file in notebooks:
    nb=json.loads(file.read_text())
    for cell in nb['cells']:
        if cell['cell_type']=='code':
            assert cell['execution_count'] is not None,(file,'unexecuted cell')
            assert not any(x['output_type']=='error' for x in cell.get('outputs',[])),file
print('PASS: dataset counts, Hadoop parity, Hive count, model selection/evaluation, streaming predictions, watermark drops, partition results, and all five executed notebooks.')
(ROOT/'reports/verification.json').write_text(json.dumps({
    'status':'PASS','verified_at_utc':datetime.now(timezone.utc).isoformat(),
    'checks':['source SHA256 and 4521x17 dimensions','no duplicate or null source records',
      'independent Hadoop CSV aggregation parity','Hive typed count 4521',
      'training CV selects maximum PR-AUC','held-out metric bounds and confusion count',
      '17 distinct streaming predictions','independent streaming job means and counts',
      'one watermark rejection per window query','independent closed-window means and counts',
      'three timing repetitions for 1/2/4 partitions','five executed error-free notebooks']},indent=2))
