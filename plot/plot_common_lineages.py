from sys import stdin
import argparse
import pdb
import matplotlib.pyplot as plt

from plot_lineages import plot_lineages as plot_CDF

def test_straggler_mitigation(job):
    return plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'invocation' in r['msg'] and r['op']=='send' and int(r['lineage']) <=33, lambda _,r: 'quit' in r['msg'] and r['op']=='send' and int(r['lineage']) <=33, start_index=0, end_index=-1, label=job[0], color='#3a75ae' if job[0]=='wo' else '#ef8635')

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

def rek(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'OK:HELLO' in r['msg'] and r['op']=='recv' and r['stage']=='rek', lambda _,r: 'quit' in r['msg'] and r['op']=='send' and r['stage']=='rek', start_index=0, end_index=0, label=job[0])

def draw(job):
    plot_CDF("logs/"+job[1]+"/log_pb", lambda _,r: 'OK:HELLO' in r['msg'] and r['op']=='recv' and r['stage']=='draw', lambda _,r: 'quit' in r['msg'] and r['op']=='send' and r['stage']=='draw', start_index=0, end_index=0, label=job[0])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="plot CDF graphs")
    parser.add_argument("range", help="range on which CDF to be plotted", type=str)
    parser.add_argument("--chunklen", help="chunk length", type=float)
    parser.add_argument("--playtime", help="virtual playback start time", type=float)
    args = parser.parse_args()

    jobs = [l.split() for l in stdin.readlines()]

    handles = [globals()[args.range](j) for j in jobs]

    plt.xlabel('chunk #')
    plt.ylabel('time (s)')
    #plt.grid(which='both')
    plt.legend((handles[0][0], handles[-1][0]),('w/o straggler mitigation', 'w/ straggler mitigation'))
    plt.xlim(xmin=0.0)
    #plt.show()
    plt.savefig('/tmp/straggler.pdf')

