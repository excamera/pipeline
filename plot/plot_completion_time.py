from __future__ import print_function
import sys
from sys import stdin
import numpy as np
import matplotlib.pyplot as plt

from util import read_records, preprocess, get_intervals

def get_completion_time(filename):
    records = read_records(filename)
    lineages = preprocess(records, cmd_of_interest='', send_only=False)
    lineage_time = [l[-1]['ts'] for l in lineages.values()]
    return np.percentile(lineage_time, (95,99,100))


if __name__=='__main__':
    jobs = [l.split() for l in stdin.readlines()]
    x = np.arange(len(jobs))
    tasks = [get_completion_time("logs/"+job[1]+"/log_pb") for job in jobs]
    p95 = plt.bar(x, [t[0] for t in tasks])
    p99 = plt.bar(x, [t[1]-t[0] for t in tasks], bottom=[t[0] for t in tasks])
    p100 = plt.bar(x, [t[2]-t[1] for t in tasks], bottom=[t[1] for t in tasks])

    plt.ylabel('Completion Time (s)')
    plt.xlabel('Number of Concurrent Workers')
    plt.title('Pipeline Completion Time')
    plt.xticks(x, [j[0] for j in jobs])
    plt.legend((p95[0], p99[0], p100[0]), ('95 Percentile', '99 Percentile', 'Last'))
    
    plt.show()

