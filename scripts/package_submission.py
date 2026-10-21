"""Package reviewable deliverables, excluding runtime state and obsolete replay runs."""
import hashlib
import json
from pathlib import Path
import zipfile

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'submission';OUT.mkdir(exist_ok=True)
stream=json.loads((ROOT/'reports/streaming_results.json').read_text())
run_id=stream['run_id']
assert stream['dropped_by_watermark']['bank_window_10s']>=1
assert stream['dropped_by_watermark']['bank_window_1m']>=1

files=[]
for directory in ['banking','data','docker','docs','hadoop','hive','notebooks','reports','requirements','scripts','tests']:
    for file in (ROOT/directory).rglob('*'):
        if not file.is_file() or '__pycache__' in file.parts:continue
        rel=file.relative_to(ROOT)
        if file.name in ['publish_github.py','drive_reviewer_access.jpg']:continue
        if len(rel.parts)>2 and rel.parts[:2] in [('data','stream_samples'),('reports','stream_batches')] and rel.parts[2]!=run_id:continue
        files.append(file)
files += [ROOT/'README.md',ROOT/'compose.yaml',ROOT/'.gitignore',ROOT/'.gitattributes']
manifest=[{'path':f.relative_to(ROOT).as_posix(),'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in sorted(files)]
(OUT/'file_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
with zipfile.ZipFile(OUT/'Distributed_ML_Banking_Capstone.zip','w',zipfile.ZIP_DEFLATED) as archive:
    for file in files:archive.write(file,file.relative_to(ROOT).as_posix())
    archive.write(OUT/'file_manifest.json','file_manifest.json')
with zipfile.ZipFile(OUT/'Streaming_Sample_Batches.zip','w',zipfile.ZIP_DEFLATED) as archive:
    for file in (ROOT/'data/stream_samples'/run_id).glob('*.json'):archive.write(file,file.name)
with zipfile.ZipFile(OUT/'Trained_Subscription_Pipeline.zip','w',zipfile.ZIP_DEFLATED) as archive:
    for file in (ROOT/'models/subscription_pipeline').rglob('*'):
        if file.is_file():archive.write(file,file.relative_to(ROOT/'models').as_posix())
for file in OUT.glob('*.zip'):
    with zipfile.ZipFile(file) as archive:assert archive.testzip() is None
    print(file.name,file.stat().st_size,'bytes')
