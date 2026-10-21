"""Record confirmed cloud deliverables without authentication data."""
import json
from pathlib import Path
root=Path(__file__).resolve().parents[1]
path=root/'requirements/submission_links.json'
links=json.loads(path.read_text())
links['repository']='https://github.com/techysuman27/Distributed-ML-Banking-Capstone'
links['reflection']={'id':'12RFcsnB-XxxOh78uxafrhlebcKvzE6ltuedJbCD0mYU','url':'https://docs.google.com/document/d/12RFcsnB-XxxOh78uxafrhlebcKvzE6ltuedJbCD0mYU/edit'}
links['uploads']=links.get('uploads',[])
confirmed=[
    ('01_hadoop_hive.ipynb','1GoLGEwQ5HxJG0KzxTYr4yHrigjw3gJan'),
    ('02_spark_eda.ipynb','18csSHFPKEZUs-4FWkZXd3b1IhuXiL7ZQ'),
    ('recording_guide.md','1RrlY3PvzRtnYx17zQu12ggA0UjsaX1Uo'),
    ('bank.csv','1CR9MoJmnuOLDvdN0-x7hdFGY1FK0Qvzn'),
    ('03_spark_ml.ipynb','1AW0fLGUGi4Tb_vBR0Cc6PxIG5Ct74lJm'),
    ('04_spark_streaming.ipynb','1sO4ELNgNR7-xa8sBJHritHgVVnpifGVJ'),
    ('05_data_parallelism.ipynb','1XzoHG57rzwAiU-LaiLQHVMZ9OiksI3ys'),
    ('presentation_figures.md','1niMCz2CGjZjte_ymVwukqS4IdGybNikQ'),
    ('submission_checklist.md','1xEEqCGPCRAISs0s61QlxP7GsbCwW2Eec'),
    ('Distributed_ML_Banking_Capstone.zip','1rh1GnshIWUr0tvgdkz41nJMo8HTjg3O2'),
    ('Streaming_Sample_Batches.zip','1EFq1HKEYXcfjNJfg5PwxyxFCdNiVdvvb'),
    ('Trained_Subscription_Pipeline.zip','16olUHABWTdPCMphNlucPuxrFXIM6DO6B')]
for name,id in confirmed:
    if not any(r['id']==id for r in links['uploads']):links['uploads'].append({'name':name,'id':id,'url':f'https://drive.google.com/file/d/{id}/view'})
links['video_status']='Student recording required; minimum 15 minutes. No placeholder link submitted.'
links['folder_access']='Anyone with the link, reader; verified from Drive permission metadata.'
path.write_text(json.dumps(links,indent=2),encoding='utf-8')
