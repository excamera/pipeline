import logging


def print_task_states(tasks):
    out_msg = '\n' + str(len(tasks))+' tasks running:\n'
    statecount = {}
    for t in tasks:
        s = str(t)
        statecount[s] = statecount.get(s, 0) + 1
    tuples = sorted(statecount.iteritems(), key=lambda tup: tup[0])
    logging.info(out_msg + '\n'.join(map(str, tuples)) + '\n')
