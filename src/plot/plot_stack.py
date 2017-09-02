import sys
import pdb
import re
from collections import OrderedDict

import matplotlib.pyplot as plt


cmd_of_interest = ('seti', 'request', 'run:./ffmpeg', 'emit', 'quit', 'collect', 'run:./youtube-dl')

# def plot_lines(lines):
#     start = float(lines[0].split(',')[0])
#     data = OrderedDict()
#
#     for i in xrange(1, len(lines)-1):
#         fields = lines[i].split(',')
#         k = fields[1].strip()
#         if not data.has_key(k):
#             data[k] = []
#         if fields[2].strip().startswith('seti') or fields[2].strip().startswith('run:./ffmpeg') or \
#                 fields[2].strip().startswith('emit') or fields[2].strip().startswith('quit') or \
#                 fields[2].strip().startswith('collect'):
#             data[k].append((float(fields[0])-start, fields[2].strip()))
#
#     # for i in xrange(len(data)):
#     #     k = data.keys()[i]
#     #     ts = [data[k][0][0], data[k][-1][0]]
#     #     plt.plot(ts, [i+1]*2, 'c-')
#
#     values = data.values()
#     j = len(values[0])-1
#     while j >= 0:
#         ts = map(lambda r: r[j][0], values)
#         drawn_line = plt.plot(range(1, len(data)+1), ts, '-')
#         drawn_line[0].set_label(values[0][j][1].split(':')[0])
#         j -= 1
#
#     plt.legend(loc='best')
#     plt.show()


def plot_stack(lines):
    assert lines[0].split(',')[1].strip() == 'starting pipeline'
    assert lines[-1].split(',')[1].strip() == 'pipeline finished'

    start_ts = float(lines[0].split(',')[0])
    data = OrderedDict()
    for i in xrange(1, len(lines)-1):  # first, last lines excluded
        fields = lines[i].split(',', 4)
        ts = float(fields[0].strip())
        lineage = fields[1].strip()
        op = fields[2].strip()
        msg = fields[3].strip()
        if lineage == '0' or op != 'send':
            continue
        if not data.has_key(lineage):
            data[lineage] = []
        if msg.startswith(cmd_of_interest):
        #if True:
            data[lineage].append((ts-start_ts, msg[:10]))

    values = data.values()
    j = len(values[0])-1
    while j >= 0:
        ts = [r[j][0] for r in values]
        drawn_line = plt.stackplot(range(1, len(data)+1), ts)
        if j > 0:
            drawn_line[0].set_label(values[0][j-1][1] if values[0][j-1][1] != 'quit' else 'wait')
        else:
            drawn_line[0].set_label('wait')
        j -= 1

    plt.legend(loc='best')
    plt.grid(b=True, axis='y')
    plt.ylabel('time (s)')
    plt.xlabel('lineage number')
    new_axis = plt.axis()
    plt.axis((new_axis[0], new_axis[1]*1.1, new_axis[2], new_axis[3]))
    plt.show()


if __name__ == '__main__':
    with open(sys.argv[1], 'r') as f:
        lines = f.readlines()

    plot_stack(lines)
