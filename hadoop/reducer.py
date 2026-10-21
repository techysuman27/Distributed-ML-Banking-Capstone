#!/usr/bin/env python3
import itertools
import sys

def records():
    for line in sys.stdin:
        key,value,count=line.rstrip('\n').split('\t')
        yield key,float(value),int(count)

mode=sys.argv[1]
for key,group in itertools.groupby(records(),lambda r:r[0]):
    total=count=0
    for _,value,n in group:total+=value;count+=n
    if mode=='mean':print(f'{key}\t{count}\t{total/count:.6f}')
    elif mode=='count':print(f'{key}\t{int(total)}')
    else:raise ValueError('Unknown reducer mode '+mode)
