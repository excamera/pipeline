from __future__ import print_function
import sys
import pdb

from util import read_records, preprocess, get_intervals

def running_time(filename):
    records = read_records(filename)
    total_time = 0.0
    running = 0
    last_ts = records[0]['ts']
    for r in records:
        if r['op'] == 'recv' and r['msg'] == 'lambda_start_ts':
            total_time += (r['ts']-last_ts) * running
            running += 1
            last_ts = r['ts']
        elif r['op'] == 'send' and r['msg'] == 'quit:':
            total_time += (r['ts']-last_ts) * running
            running -= 1
            last_ts = r['ts']
    assert(running==0)
    return total_time




if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: %s log_file' % sys.argv[0], file=sys.stderr)
        sys.exit(1 )
    print("total lambda running time: %f" % running_time(sys.argv[1]))
