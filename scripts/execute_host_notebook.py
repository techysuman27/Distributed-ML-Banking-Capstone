"""Execute the evidence notebook locally with the interpreter running this script."""
import json
import os
import sys
from pathlib import Path
import nbformat
from nbclient import NotebookClient

root=Path(__file__).resolve().parents[1]
kernel=root/'runtime/kernels/banking-host'
kernel.mkdir(parents=True,exist_ok=True)
(kernel/'kernel.json').write_text(json.dumps({'argv':[sys.executable,'-m','ipykernel_launcher','-f','{connection_file}'],'display_name':'Banking Host','language':'python'}))
os.environ['JUPYTER_PATH']=str(root/'runtime')
path=root/'notebooks/01_hadoop_hive.ipynb'
nb=nbformat.read(path,as_version=4)
NotebookClient(nb,timeout=120,kernel_name='banking-host',resources={'metadata':{'path':str(root)}}).execute()
nbformat.write(nb,path)
print('Executed Hadoop/Hive evidence notebook from captured terminal results.')
