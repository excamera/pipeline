from sys import stdin
from plot_CDF import plot_CDF
import matplotlib.pyplot as plt

if __name__ == '__main__':
    jobs = [l.split() for l in stdin.readlines()]

    for j in jobs:
        plot_CDF("logs/"+j[0]+"/log_pb", lambda _,r: 'invocation' in r['msg'] and r['op']=='send', lambda _,r: 'quit' in r['msg'] and r['op']=='send', start_index=0, end_index=-1, label=j[1])

    plt.xlabel('task completion time (s)')
    plt.grid(which='both')
    plt.legend()
    plt.xlim(xmin=0.0)
    plt.show()
