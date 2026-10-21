"""Execute notebooks and retain real outputs; stop on the first error."""
import argparse
import os
from pathlib import Path
import nbformat
from nbclient import NotebookClient

parser = argparse.ArgumentParser()
parser.add_argument('notebooks', nargs='+')
args = parser.parse_args()
os.chdir('/workspace')
for directory in ['reports', 'artifacts/spark-events', 'models', 'runtime', 'docs/evidence']:
    Path(directory).mkdir(parents=True, exist_ok=True)
for file in args.notebooks:
    path = Path(file)
    nb = nbformat.read(path, as_version=4)
    print('EXECUTING', path, flush=True)
    try:
        NotebookClient(nb, timeout=1200, kernel_name='python3', resources={'metadata': {'path': '/workspace'}}).execute()
    finally:
        nbformat.write(nb, path)
    print('COMPLETED', path, flush=True)
