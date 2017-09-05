import logging

__all__ = ['simple_scheduler', 'priority_scheduler', 'breadth_first_scheduler']


def print_task_states(tasks):
    out_msg = str(len(tasks))+' tasks running:\n'
    for i in range(0, len(tasks), 4):
        out_msg += str([str(t) for t in tasks[i:i+4]])+'\n'
    logging.info(out_msg)
