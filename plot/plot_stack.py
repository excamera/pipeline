from __future__ import print_function
import sys
import os
import pdb
import re
import argparse

import matplotlib.pyplot as plt

from util import preprocess, read_records

cmds = ('sleep', 'seti', 'invocation', 'request', 'run:./ffmpeg', 'run:time ./ffmpeg', 'emit', 'quit', 'collect', 'run:./youtube-dl', 'run:tar', 'lambda')

color_map = {
        'invocation': 'r',
        'emit': 'y',
        'collect': 'c',
        'ffmpeg': 'g'
        }

def plot_stack(lines, chunk_length=None, ystart=None, verbose=False, sort_by_completion_time=False):
    data = preprocess(lines, cmd_of_interest="" if verbose else cmds, send_only=False) # empty string cmd_of_interest means all cmds
    values = data.values()
    lengths = [len(r) for r in values]
    common_len = max(set(lengths), key=lengths.count)
    valid_values = [v for v in values if len(v)==common_len]
    invalid_values = [v for v in values if len(v)!=common_len]
    if len(invalid_values) > 0:
        print('%d lineages have uncommon # of messages: %s...' % (len(invalid_values), invalid_values[0]), file=sys.stderr)

    padded_values = [v if len(v) == common_len else [{'ts':0.0}]*common_len for v in values]
    if sort_by_completion_time:
        padded_values.sort(key=lambda v: v[common_len-1]['ts'])
    for j in reversed(xrange(0, common_len)):
        ts = [r[j]['ts'] for r in padded_values]
        label = valid_values[0][j-1]['stage'][:6]+':'+valid_values[0][j-1]['msg'][:15]
        if chunk_length:
            xscale = [chunk_length * x for x in xrange(1, len(data)+1)]
        else:
            xscale = xrange(1, len(data)+1)
        if j == 0 or 'quit:' in label:
            drawn_line = plt.stackplot(xscale, ts, color='0.9')
            drawn_line[0].set_label('wait')
        else:
            keywords = filter(lambda x: x in valid_values[0][j-1]['msg'][:25], color_map.keys())
            if len(keywords) > 0:
                drawn_line = plt.stackplot(xscale, ts, color=color_map[keywords[0]])
            else:
                drawn_line = plt.stackplot(xscale, ts)
            drawn_line[0].set_label(label)

    plt.legend(loc='best', ncol=2, prop={'size': 6})
    plt.grid(b=True, axis='y')
    plt.ylabel('process time (s)')
    if sort_by_completion_time:
        plt.xlabel('sorted workers')
    else:
        if chunk_length:
            plt.xlabel('video time (s)')
        else:
            plt.xlabel('lineage number')
    new_axis = plt.axis()
    plt.axis((new_axis[0], new_axis[1]*1.15, new_axis[2], new_axis[3]))
    if ystart is not None:
        length = 180
        xstart = 1
        plt.plot([xstart, xstart+length], [ystart, ystart+length], color='k', linestyle='-', linewidth=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="plot stack graphs")
    parser.add_argument("logfile", help="pipeline log file", type=str)
    parser.add_argument("-v", "--verbose", help="show ts of all commands", action="store_true")
    parser.add_argument("-s", "--sort", help="sort by completion time", action="store_true")
    parser.add_argument("--chunklen", help="chunk length", type=float)
    parser.add_argument("--playtime", help="virtual playback start time", type=float)
    args = parser.parse_args()
    
    lines = read_records(sys.argv[1])

    plot_stack(lines, chunk_length=args.chunklen, ystart=args.playtime, verbose=args.verbose, sort_by_completion_time=args.sort)
    plt.show()

