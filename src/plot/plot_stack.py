import sys
import pdb
import re

import matplotlib.pyplot as plt

from util import preprocess

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


def plot_stack(lines, chunk_length=None):
    data = preprocess(lines, cmd_of_interest)
    values = data.values()
    for j in reversed(xrange(0, len(values[0]))):
        ts = [r[j][0] for r in values]
        label = values[0][j-1][1]
        if chunk_length:
            xscale = [chunk_length * x for x in xrange(1, len(data)+1)]
        else:
            xscale = xrange(1, len(data)+1)
        if j == 0 or label == 'quit:':
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
    plt.show()


if __name__ == '__main__':
    with open(sys.argv[1], 'r') as f:
        lines = f.readlines()

    if len(sys.argv) == 3:
        chunk_length = float(sys.argv[2])
    else:
        chunk_length = None

    plot_stack(lines, chunk_length)
