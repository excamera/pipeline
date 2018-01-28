from sys import stdin
import argparse
import matplotlib.pyplot as plt

from plot_CDF import plot_CDF

def total(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'invocation' in r['msg'] and r['op']=='send', lambda _,r: 'quit' in r['msg'] and r['op']=='send', start_index=0, end_index=-1, label=job[0])

def stage1(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'invocation' in r['msg'] and r['op']=='send', lambda _,r: 'quit' in r['msg'] and r['op']=='send', start_index=0, end_index=0, label=job[0])

def stage2(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'invocation' in r['msg'] and r['op']=='send', lambda _,r: 'quit' in r['msg'] and r['op']=='send', start_index=1, end_index=1, label=job[0])

def stage3(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'invocation' in r['msg'] and r['op']=='send', lambda _,r: 'quit' in r['msg'] and r['op']=='send', start_index=2, end_index=2, label=job[0])

def stage4(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'invocation' in r['msg'] and r['op']=='send', lambda _,r: 'quit' in r['msg'] and r['op']=='send', start_index=3, end_index=3, label=job[0])

def emit1(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'emit' in r['msg'] and r['op']=='send', lambda _,r: 'EMIT' in r['msg'] and r['op']=='recv', start_index=0, end_index=0, label=job[0])

def emit2(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'emit' in r['msg'] and r['op']=='send', lambda _,r: 'EMIT' in r['msg'] and r['op']=='recv', start_index=1, end_index=1, label=job[0])

def emit3(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'emit' in r['msg'] and r['op']=='send', lambda _,r: 'EMIT' in r['msg'] and r['op']=='recv', start_index=2, end_index=2, label=job[0])

def collect1(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'collect' in r['msg'] and r['op']=='send', lambda _,r: 'COLLECT' in r['msg'] and r['op']=='recv', start_index=0, end_index=0, label=job[0])

def collect2(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'collect' in r['msg'] and r['op']=='send', lambda _,r: 'COLLECT' in r['msg'] and r['op']=='recv', start_index=1, end_index=1, label=job[0])

def collect3(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'collect' in r['msg'] and r['op']=='send', lambda _,r: 'COLLECT' in r['msg'] and r['op']=='recv', start_index=2, end_index=2, label=job[0])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="plot CDF graphs")
    parser.add_argument("range", help="range on which CDF to be plotted", type=str)
    parser.add_argument("--chunklen", help="chunk length", type=float)
    parser.add_argument("--playtime", help="virtual playback start time", type=float)
    args = parser.parse_args()

    jobs = [l.split() for l in stdin.readlines()]

    for j in jobs:
        globals()[args.range](j)

    plt.xlabel(args.range + ' time (s)')
    plt.grid(which='both')
    plt.legend()
    plt.xlim(xmin=0.0)
    plt.show()

