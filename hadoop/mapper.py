#!/usr/bin/env python3
"""Hadoop Streaming mapper. Input is parsed as CSV, not split by commas manually."""
import csv
import sys
mode=sys.argv[1]
fields='age job marital education default balance housing loan contact day month duration campaign pdays previous poutcome y'.split()
for values in csv.reader(sys.stdin):
    if not values or values[0]=='age':continue
    if len(values)!=len(fields):
        raise ValueError('Invalid bank.csv record length')
    r=dict(zip(fields,values))
    if mode=='job_balance':key,value=r['job'],int(r['balance'])
    elif mode=='education_housing':key,value=r['education']+'|'+r['housing'],1
    elif mode=='month_subscription':key,value=r['month']+'|'+r['y'],1
    elif mode=='outcome_duration':key,value=r['poutcome'],int(r['duration'])
    elif mode=='age_balance':key,value=str(int(r['age'])//10*10)+'s',int(r['balance'])
    else:raise ValueError('Unknown mapper mode '+mode)
    print(f'{key}\t{value}\t1')
