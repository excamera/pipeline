import sys
import pdb
import re

import matplotlib.pyplot as plt

from util import preprocess

cmd_of_interest = ('seti', 'request', 'run:./ffmpeg', 'emit', 'quit', 'collect', 'run:./youtube-dl', 'run:tar')

def plot_stack(lines, chunk_length=None, ystart=None):
    data = preprocess(lines) # kwargs: tuple cmd_of_interest, bool send_only
    values = data.values()
    for j in reversed(xrange(0, len(values[0]))):
        ts = [r[j][0] for r in values]
        label = values[0][j-1][1]
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
    with open(sys.argv[1], 'r') as f:
        lines = f.readlines()

    chunk_length = None
    ystart = None
    if len(sys.argv) >= 3:
        chunk_length = float(sys.argv[2])
    if len(sys.argv) >= 4:
        ystart = float(sys.argv[3])

    plot_stack(lines, chunk_length, ystart)

