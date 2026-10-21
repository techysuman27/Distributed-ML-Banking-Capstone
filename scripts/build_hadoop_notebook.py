"""Assemble reproducible Hadoop/Hive commands and the saved terminal outputs."""
import json
from pathlib import Path
import nbformat as nbf

ROOT=Path(__file__).resolve().parents[1]
cells=[]
def md(s):cells.append(nbf.v4.new_markdown_cell(s))
def code(s):cells.append(nbf.v4.new_code_cell(s))

md('''# Part 1 — Data Analysis and Management with Hadoop and Hive
**Contribution:** Individual — Suman  
**Dataset:** Official bank.csv, 4,521 rows, 17 columns.

## Execution scope
HDFS runs a real NameNode and DataNode with replication factor 1. Python mapper/reducer programs run through the Hadoop Streaming JAR using LocalJobRunner over HDFS. HiveServer2 executes HiveQL against a typed ORC table in HDFS, using the local MapReduce engine. This laptop demonstration does not claim a YARN cluster or multi-machine fault tolerance.

The commands below reproduce the local run from the project root. Outputs are read from the saved execution transcripts, rather than being invented or presented as newly executed Hadoop jobs inside Colab. The accompanying Google Doc includes code/output screenshots under the official questions.
''')
md('''## HDFS ingestion and MapReduce execution
```powershell
docker compose --profile hadoop up -d hadoop-hive
docker compose --profile hadoop exec -T hadoop-hive bash /workspace/hadoop/run_mapreduce.sh
docker compose --profile hadoop exec -T hadoop-hive bash /workspace/hive/run_hive.sh
```
''')
code("""from pathlib import Path
root=Path.cwd()
if not (root/'data/bank.csv').exists(): root=root.parent
print('Dataset bytes:',(root/'data/bank.csv').stat().st_size)
print('Execution file:',root/'reports/hadoop/execution.log')
""")
md('## Hadoop Q1 Data ingestion\nThe HDFS report and file listing confirm that the original 375,542-byte CSV was transferred. The CSV header is ignored by the Python mapper. A single DataNode provides no replica failover.')
code("print((root/'reports/hadoop/execution.log').read_text().split('++ date')[0])")
md('## Hadoop Q2 Python programs and results\nMappers emit keyed sums and counts; the reducer combines all values for each shuffled key. Mean balances use total balance divided by record count, avoiding an incorrect average of partition averages.')
code("print((root/'hadoop/mapper.py').read_text());print((root/'hadoop/reducer.py').read_text())")
for mode,title in [('job_balance','Average balance by occupation'),('education_housing','Housing-loan counts by education'),('month_subscription','Monthly contact counts by subscription status'),('outcome_duration','Hadoop Q3 Average call duration by previous outcome'),('age_balance','Hadoop Q3 Age decade and average balance')]:
    md('### '+title)
    code(f"print((root/'reports/hadoop/{mode}.tsv').read_text())")
md('''## Hive Q1 to Q9 SQL and execution evidence
The original CSV is staged using OpenCSVSerde, then explicitly cast into a typed ORC table named banking_data.client_info. A defensive age/label filter excludes the CSV header even if header-skipping is bypassed by the execution path. The verified table contains 4,521 rows.

Q7 cannot be calculated because no year or full date exists. Q8 analyzes the supplied average yearly balance field across education groups; it is a cross-sectional comparison, not a year-over-year time series. A three-standard-deviation heuristic flags unusual within-group balances for inspection and is not a fraud label.
''')
code("print((root/'hive/queries.sql').read_text())")
code("print((root/'reports/hive-results.txt').read_text())")
md('''## Findings and interpretation
May has 1,398 recorded contacts and a subscription rate of 6.65%. Retired clients have the highest mean balance by occupation at about 2,319.19, while group sizes vary substantially. Pearson age/balance correlation is approximately 0.0838, indicating weak linear association. Previously successful campaigns are associated with higher current subscription rates, but this does not establish a causal effect.

The unusual balance heuristic flags 53 secondary, 35 tertiary, 6 primary and 4 unknown-education records. Negative balances are possible and retained. These flags identify records for data review, not proof of fraud. Source data lacks currency metadata, so no currency symbol is invented.
''')
nb=nbf.v4.new_notebook(cells=cells)
nb.metadata.kernelspec={'name':'python3','display_name':'Python 3','language':'python'}
nbf.write(nb,ROOT/'notebooks/01_hadoop_hive.ipynb')
