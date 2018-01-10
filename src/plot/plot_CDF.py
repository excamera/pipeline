from __future__ import print_function
import sys
import numpy as np
import matplotlib.pyplot as plt

from util import read_records, preprocess, get_intervals


def plot_CDF(filename, start_selector, end_selector):
    records = read_records(filename)
    lineages = preprocess(records)
    intervals = get_intervals(lineages, start_selector, end_selector)

    sortedtime = np.sort(intervals.values())
    p = 1. * np.arange(len(intervals.values())) / (len(intervals.values()) - 1)
    plt.plot(sortedtime, p)
    plt.show()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('usage: %s log_file start_keyword end_keyword' % sys.argv[0], file=sys.stderr)
        sys.exit(1)
    plot_CDF(sys.argv, lambda _, r: sys.argv[1] in r['msg'], lambda _, r: sys.argv[2] in r['msg'])
