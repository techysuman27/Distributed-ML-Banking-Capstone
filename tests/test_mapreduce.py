import collections
import csv
import io
from pathlib import Path
import subprocess
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]

class MapReduceTests(unittest.TestCase):
    def run_job(self,mode,reducer):
        source=(ROOT/'data/bank.csv').read_text()
        mapped=subprocess.run([sys.executable,str(ROOT/'hadoop/mapper.py'),mode],input=source,text=True,capture_output=True,check=True).stdout
        shuffled='\n'.join(sorted(mapped.splitlines()))+'\n'
        reduced=subprocess.run([sys.executable,str(ROOT/'hadoop/reducer.py'),reducer],input=shuffled,text=True,capture_output=True,check=True).stdout
        return [line.split('\t') for line in reduced.splitlines()]
    def test_job_means_match_independent_csv_aggregation(self):
        groups=collections.defaultdict(list)
        for r in csv.DictReader(io.StringIO((ROOT/'data/bank.csv').read_text())):groups[r['job']].append(int(r['balance']))
        output=self.run_job('job_balance','mean')
        self.assertEqual(len(output),len(groups))
        for job,count,mean in output:
            self.assertEqual(int(count),len(groups[job]))
            self.assertAlmostEqual(float(mean),sum(groups[job])/len(groups[job]),places=5)
    def test_counts_preserve_all_source_rows(self):
        n=sum(1 for _ in csv.DictReader(io.StringIO((ROOT/'data/bank.csv').read_text())))
        for mode in ['education_housing','month_subscription']:
            self.assertEqual(sum(int(r[1]) for r in self.run_job(mode,'count')),n)
    def test_age_and_outcome_groups_preserve_record_count(self):
        for mode in ['age_balance','outcome_duration']:
            self.assertEqual(sum(int(r[1]) for r in self.run_job(mode,'mean')),4521)

if __name__=='__main__':unittest.main()
