"""Create readable HTML views of real code and retained execution output for screenshots."""
import html
import json
import re
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/evidence/pages'
OUT.mkdir(parents=True,exist_ok=True)
manifest=[]

def page(key,title,code,output,note='',image=None):
    manifest[:]=[item for item in manifest if item['key']!=key]
    # Excerpts are labelled and link to the full source/output through the project submission.
    source_lines=code.strip().splitlines()
    result_lines=output.strip().splitlines()
    code_excerpt='\n'.join(source_lines[:14])
    result_excerpt='\n'.join(result_lines[:17])
    if len(source_lines)>14:code_excerpt+='\n# ... full code is included in the source notebook or script.'
    if len(result_lines)>17:result_excerpt+='\n... additional output is retained in the notebook or transcript.'
    figures=f'<img src="data:image/png;base64,{image}" alt="Executed chart">' if image else ''
    body=f'''<!doctype html><html><head><meta charset="utf-8"><title>{html.escape(title)}</title>
<style>*{{box-sizing:border-box}}body{{margin:0;background:white;color:#192732;font:20px Arial,sans-serif}}main{{width:1180px;padding:32px 42px}}h1{{font-size:28px;line-height:1.2;margin:10px 0 14px}}h2{{font-size:21px;margin:18px 0 8px}}p{{font-size:18px;line-height:1.4;margin:8px 0}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#f3f6f8;border:1px solid #d7e0e6;padding:15px;font:18px/1.35 Consolas,monospace;margin:0}}small{{font-size:16px;color:#485e6d}}img{{display:block;max-width:100%;max-height:380px;object-fit:contain}}.tag{{color:#11647d;font-weight:bold;letter-spacing:.05em;font-size:16px}}</style></head>
<body><main><div class="tag">DISTRIBUTED BANKING ML EXECUTION EVIDENCE</div><h1>{html.escape(title)}</h1>
<p>{html.escape(note)}</p><h2>Code excerpt</h2><pre>{html.escape(code_excerpt)}</pre>
<h2>Recorded output excerpt</h2><pre>{html.escape(result_excerpt or '(See the accompanying executed chart.)')}</pre>{figures}
<p><small>Source data: official AlmaBetter bank.csv. The underlying notebook and full execution transcripts accompany this screenshot.</small></p></main></body></html>'''
    (OUT/f'{key}.html').write_text(body,encoding='utf-8')
    manifest.append({'key':key,'title':title,'url':f'http://localhost:8012/{key}.html','screenshot':f'docs/evidence/{key}.jpg'})

def hadoop_pages():
    log=(ROOT/'reports/hadoop/execution.log').read_text()
    page('01_hadoop_q1','Hadoop Q1 Data ingestion',"hdfs dfs -mkdir -p /banking/raw\nhdfs dfs -put -f /workspace/data/bank.csv /banking/raw/bank.csv\nhdfs dfs -ls /banking/raw\nhdfs dfsadmin -report",log.split('++ date')[0],'Real HDFS NameNode and DataNode; replication factor 1.')
    mapper=(ROOT/'hadoop/mapper.py').read_text()
    reducer=(ROOT/'hadoop/reducer.py').read_text()
    for mode,title in [('job_balance','Average balance by job'),('education_housing','Housing loan counts by education'),('month_subscription','Monthly contacts and subscriptions'),('outcome_duration','Call duration by previous outcome'),('age_balance','Average balance by age decade')]:
        relevant='\n'.join(line for line in mapper.splitlines() if mode in line)
        code=f"# Mapper branch for {mode}\n{relevant}\n\n# Shared reducer\n"+reducer[reducer.index('mode=sys.argv[1]'):]
        page('01_mr_'+mode,'Hadoop MapReduce '+title,code,(ROOT/f'reports/hadoop/{mode}.tsv').read_text(),'Hadoop Streaming with Python and LocalJobRunner over HDFS; no YARN cluster is claimed.')
    known_headers=[
        'col_name\tdata_type\tcomment','question\tclients',
        'age\tjob\tmarital\teducation\tdefault\tbalance\thousing\tloan\tcontact\tday\tmonth\tduration\tcampaign\tpdays\tprevious\tpoutcome\ty',
        'job\tmarital\tbalance','job\tclients\tavg_age','education\tdefault_count',
        'job\tclients\tavg_balance\tsubscription_pct','month\tcontacts\tsubscription_pct',
        'age_balance_pearson','q7_limitation','education\tclients\tavg_yearly_balance\tbalance_sd\tminimum_balance\tmaximum_balance',
        'education\tunusual_records','poutcome\tclients\tsubscription_pct','y\tclients\tavg_call_seconds']
    sections=[]
    for line in (ROOT/'reports/hive-results.txt').read_text().splitlines():
        line=line.rstrip().replace('client_info.','')
        if line in known_headers:sections.append([line])
        elif sections and line:sections[-1].append(line)
    (ROOT/'reports/hive_sections.json').write_text(json.dumps(sections,indent=2))
    assert len(sections)==15,len(sections)
    sql=(ROOT/'hive/queries.sql').read_text()
    blocks=re.split(r'-- Q\d[^\n]*\n',sql)
    outputs={1:sections[0],2:sections[1]+sections[2],3:sections[3][:6]+sections[4],4:sections[5]+sections[6],5:sections[7]+sections[8],6:sections[9],7:sections[10],8:sections[11]+sections[12],9:sections[13]+sections[14]}
    for i in range(1,10):
        code=(blocks[1] if i==1 else blocks[i]) if len(blocks)>i else sql
        page(f'01_hive_q{i}',f'Hive Q{i} '+['Typed table creation','Count and first rows','Filtering and sorting','Aggregation by occupation and education','Balances and campaign success','Age balance correlation','Yearly trend limitation','Unusual balance patterns','Previous outcomes and call duration'][i-1],code,'\n'.join(outputs[i]),'Executed in Apache HiveServer2. Header rows are explicitly excluded from the typed table.')

def notebook_pages(number,pattern):
    path=next((ROOT/'notebooks').glob(pattern))
    nb=json.loads(path.read_text(encoding='utf-8'))
    groups=[];active=None
    for cell in nb['cells']:
        text=''.join(cell['source'])
        match=re.search(r'^## Q\d[^\n]*',text,re.M) if cell['cell_type']=='markdown' else None
        if match:
            active={'title':match[0].lstrip('# '),'code':[],'output':[],'images':[],'note':text[match.end():].strip()}
            groups.append(active)
        elif cell['cell_type']=='code' and active:
            active['code'].append(text)
            for output in cell.get('outputs',[]):
                if output['output_type']=='stream' and output.get('name')=='stdout':active['output'].append(''.join(output['text']))
                elif output['output_type'] in ['display_data','execute_result']:
                    data=output.get('data',{})
                    if 'image/png' in data:active['images'].append(''.join(data['image/png']))
                    elif 'text/plain' in data:active['output'].append(''.join(data['text/plain']))
    for group in groups:
        q=int(re.match(r'Q(\d+)',group['title'])[1])
        page(f'{number:02d}_q{q}',group['title'],'\n\n'.join(group['code']),'\n'.join(group['output']),group['note'],group['images'][0] if group['images'] else None)
    return groups

hadoop_pages()
for number,pattern in [(2,'02*.ipynb'),(3,'03*.ipynb'),(4,'04*.ipynb'),(5,'05*.ipynb')]:
    notebook_pages(number,pattern)

# Show the reusable implementation alongside its actual saved results.
modeling=(ROOT/'banking/modeling.py').read_text()
page('03_q3','Q3. Reusable feature pipeline',modeling[modeling.index('def make_pipeline'):modeling.index('def evaluate')],
     'SQLTransformer → Imputer → StringIndexer → OneHotEncoder → VectorAssembler → StandardScaler → classifier',
     'Implementation source; the executed notebook prints these stage names. All learned preprocessing remains within each training-fold pipeline.')
if (ROOT/'reports/model_selection.json').exists():
    selection=json.loads((ROOT/'reports/model_selection.json').read_text())
    cv_code="""metric = BinaryClassificationEvaluator(rawPredictionCol='probability', metricName='areaUnderPR')
lr_grid = ParamGridBuilder().addGrid(lr.regParam,[0.01,0.1]).addGrid(lr.elasticNetParam,[0.0,1.0]).build()
tree_grid = ParamGridBuilder().addGrid(dt.maxDepth,[3,5]).addGrid(dt.minInstancesPerNode,[10]).build()
cv = CrossValidator(estimator=make_pipeline(model), estimatorParamMaps=grid,
                    evaluator=metric, numFolds=3, seed=42, parallelism=1)
fitted = cv.fit(train)
# Rank by training-fold mean PR-AUC; evaluate the winner on held-out data only afterward."""
    page('03_q6','Q6. Hyperparameter tuning',cv_code,json.dumps(selection['candidates'],indent=2),'Recorded training cross-validation results; the logistic resume cache is disclosed in the executed notebook.')
if (ROOT/'reports/streaming_results.json').exists():
    stream=json.loads((ROOT/'reports/streaming_results.json').read_text())
    for q,name,code in [
      (1,'bank_job_live',"raw=spark.readStream.schema(schema).option('maxFilesPerTrigger',1).json(str(input_dir))\nsummary=raw.groupBy('job').agg(F.count('*').alias('records'), F.avg('balance').alias('avg_balance'), F.avg('duration').alias('avg_call_seconds'))\nexecute(summary,'bank_job_live','complete')"),
      (2,'bank_predictions',"pipeline=PipelineModel.load('/workspace/models/subscription_pipeline')\npredictions=pipeline.transform(features(raw))\npredictions=predictions.withColumn('subscription_probability',vector_to_array('probability')[1])\nexecute(predictions.select('event_id','prediction','subscription_probability'),'bank_predictions')"),
      (3,'bank_window_10s',"timed=raw.withWatermark('event_time','20 seconds')\nfor interval,name in [('10 seconds','bank_window_10s'),('1 minute','bank_window_1m')]:\n    aggregate=timed.groupBy(F.window('event_time',interval)).agg(F.count('*').alias('records'),F.avg('balance').alias('avg_balance'))\n    execute(aggregate,name)"),
      (4,'bank_window_1m',"offsets=[[0,1,2],[12,13,14],[7,8],[90,91,92],[95,96],[4,180,181,182]]\n# One file per trigger; extra microbatch permits watermark propagation.\ntimed=raw.withWatermark('event_time','20 seconds')\ndrops=sum(op.get('numRowsDroppedByWatermark',0)\n          for p in query.recentProgress for op in p.get('stateOperators',[]))")]:
        query=stream['queries'][name]
        if q==1:result=query['rows']
        elif q==2:result=[{k:r[k] for k in ['event_id','prediction','subscription_probability']} for r in query['rows'][:6]]
        elif q==3:result={n:stream['queries'][n]['rows'] for n in ['bank_window_10s','bank_window_1m']}
        else:result={'dropped_by_watermark':stream['dropped_by_watermark'],'input_events':stream['input_count'],'finalized_counts':{n:sum(r['records'] for r in stream['queries'][n]['rows']) for n in ['bank_window_10s','bank_window_1m']}}
        lines=[json.dumps(r,default=str) for r in result] if isinstance(result,list) else [f'{k}: {json.dumps(v,default=str)}' for k,v in result.items()]
        page(f'04_q{q}',f'Streaming Q{q}: '+['Updated job aggregates','Saved-pipeline predictions','Event-time windows','Verified watermark behavior'][q-1],code,'\n'.join(lines),'Actual Structured Streaming outputs; marketing records with synthetic timestamps. Full per-batch outputs and query progress accompany the submission.')
if (ROOT/'reports/parallel_loan_groups.json').exists():
    loans=json.loads((ROOT/'reports/parallel_loan_groups.json').read_text())
    page('05_q2_loans','Q2. Personal-loan counts by age decade',
         "groups=df.withColumn('age_decade',(F.floor(F.col('age')/10)*10).cast('int')).groupBy('age_decade')\ngroups=groups.agg(F.count('*').alias('clients'),F.sum((F.col('loan')=='yes').cast('int')).alias('loan_holders'))\ngroups=groups.withColumn('loan_pct',100*F.col('loan_holders')/F.col('clients')).orderBy(F.desc('loan_holders'),'age_decade')\ngroups.show(5)",
         '\n'.join(json.dumps(r) for r in loans['results']),loans['limitation'])
if (ROOT/'reports/parallel_resources.json').exists():
    resource=json.loads((ROOT/'reports/parallel_resources.json').read_text())
    page('05_q4_resources','Q4. Resource monitoring',
         "monitor=ResourceMonitor().start()\n# Training, partition benchmark and concurrent summaries execute here.\nresource=monitor.stop()\nsave_json('parallel_resources.json',resource)\n# Separate Docker statistics are captured in reports/docker-stats.jsonl.",
         json.dumps({k:v for k,v in resource.items() if k!='series'},indent=2),'Driver Python and child JVM samples exclude remote executors. CPU percentages can exceed 100% when processes use more than one core.')
if (ROOT/'reports/parallel_tasks.json').exists():
    tasks=json.loads((ROOT/'reports/parallel_tasks.json').read_text())
    page('05_q5','Q5. Concurrent summaries and FAIR task scheduling',
         "def summary_task(name,column):\n    sc=spark.sparkContext\n    sc.setLocalProperty('spark.scheduler.pool',name)\n    sc.setJobGroup(name,'Parallel '+name)\n    try: return name,rows(df.groupBy(column).count().orderBy(column))\n    finally:\n        sc.setLocalProperty('spark.scheduler.pool',None)\n        sc.setLocalProperty('spark.jobGroup.id',None)\nwith ThreadPoolExecutor(max_workers=2) as executor:\n    futures=[executor.submit(summary_task,'education_summary','education'),\n             executor.submit(summary_task,'contact_summary','contact')]\n    concurrent=dict(f.result() for f in futures)\nprint(concurrent)",
         '\n'.join(f'{name}: {json.dumps(result)}' for name,result in tasks.items()),
         'Both futures completed. Each thread has a separate job group and scheduling pool; full resource samples are stored separately.')
(OUT/'manifest.json').write_text(json.dumps(manifest,indent=2))
(OUT/'index.html').write_text('<h1>Distributed banking ML execution evidence</h1>'+''.join(f'<p><a href="{x["key"]}.html">{html.escape(x["title"])}</a></p>' for x in manifest),encoding='utf-8')
print('Built',len(manifest),'evidence pages.')
