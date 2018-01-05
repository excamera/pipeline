from __future__ import print_function
import sys
import os
import pdb
import re

import matplotlib.pyplot as plt

from util import preprocess
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__))+'/../../external/mu/src/lambdaize/libmu/')

import joblog_pb2

fields = ('ts', 'lineage', 'op', 'msg', 'stage', 'worker_called', 'num_frames')
cmds = ('sleep', 'seti', 'invocation', 'request', 'run:./ffmpeg', 'emit', 'quit', 'collect', 'run:./youtube-dl', 'run:tar', 'lambda')

def plot_stack(lines, chunk_length=None, ystart=None):
    data = preprocess(lines, cmd_of_interest="", send_only=True) # empty string cmd_of_interest means all cmds
    values = data.values()
    for j in reversed(xrange(0, len(values[0]))):
        ts = [r[j]['ts'] for r in values]
        label = values[0][j-1]['msg'][:10]
        if chunk_length:
            xscale = [chunk_length * x for x in xrange(1, len(data)+1)]
        else:
            xscale = xrange(1, len(data)+1)
        if j == 0 or 'quit:' in label:
            drawn_line = plt.stackplot(xscale, ts, color='0.9')
            drawn_line[0].set_label('wait')
        else:
            drawn_line = plt.stackplot(xscale, ts)
            drawn_line[0].set_label(label)

    plt.legend(loc='best')
    plt.grid(b=True, axis='y')
    plt.ylabel('process time (s)')
    if chunk_length:
        plt.xlabel('video time (s)')
    else:
        plt.xlabel('lineage number')
    new_axis = plt.axis()
    plt.axis((new_axis[0], new_axis[1]*1.1, new_axis[2], new_axis[3]))
    if ystart is not None:
        length = 180
        xstart = 1
        plt.plot([xstart, xstart+length], [ystart, ystart+length], color='k', linestyle='-', linewidth=2)

    plt.show()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: %s log_file [chunk_length [start_playing_time]]' % sys.argv[0], file=sys.stderr)
        sys.exit(1)
    
    if sys.argv[1].endswith('.csv'):
        with open(sys.argv[1], 'r') as f:

            def define_fields(line):
                fields = line.split(', ')
                if len(fields) == 2:
                    return {'ts': float(fields[0]), 'msg': fields[1]}
                elif len(fields) >= 4:
                    return {'ts': float(fields[0]), 'lineage': fields[1], 'op': fields[2], 'msg': ', '.join(fields[3:])}
                else:
                    raise Exception('undefined fields: %s' % line)

            lines = [define_fields(l.strip()) for l in f.readlines()]
    else:
        with open(sys.argv[1], 'rb') as f:
            jl = joblog_pb2.JobLog()
            jl.ParseFromString(f.read())

            def define_fields(line):
                ret = {}
                for a in fields:
                    if hasattr(line, a):
                        ret[a] = getattr(line, a)
                return ret

            lines = [define_fields(l) for l in jl.record]
            
    chunk_length = None
    ystart = None
    if len(sys.argv) >= 3:
        chunk_length = float(sys.argv[2])
    if len(sys.argv) >= 4:
        ystart = float(sys.argv[3])

    plot_stack(lines, chunk_length, ystart)

