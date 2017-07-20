#!/usr/bin/python
import Queue

import logging
import time
import pdb

from libmu import tracker
from libmu.machine_state import ErrorState, TerminalState
from stages.util import default_deliver_func


def print_task_states(tasks):
    out_msg = str(len(tasks))+' tasks running:\n'
    for i in range(0, len(tasks), 4):
        out_msg += str([str(t) for t in tasks[i:i+4]])+'\n'
    logging.info(out_msg)


class SimpleScheduler(object):
    """A simple scheduler that scans every stage for events and submit any available tasks"""
    @classmethod
    def schedule(cls, pipeline):
        logging.info('start scheduling pipeline')
        last_print = 0
        tasks = []
        while True:
            buffer_empty = True
            deliver_empty = True
            for key, stage in pipeline.stages.iteritems():
                stage.deliver_func = default_deliver_func if stage.deliver_func is None else stage.deliver_func
                if not stage.buffer_queue.empty():
                    buffer_empty = False
                    stage.deliver_func(stage.buffer_queue, stage.deliver_queue, stale=len(tasks) == 0 and stage.deliver_queue.empty())

            for key, stage in pipeline.stages.iteritems():
                while not stage.deliver_queue.empty():
                    deliver_empty = False
                    t = tracker.Task(stage.lambda_function, stage.init_state, stage.deliver_queue.get(), stage.emit, stage.event)
                    tasks.append(t)
                    tracker.Tracker.submit(t)
                    logging.debug('submitted a task: '+str(t))

            error_tasks = [t for t in tasks if isinstance(t.current_state, ErrorState)]
            if len(error_tasks) > 0:
                logging.error(str(len(error_tasks))+" tasks failed: ")
                for et in error_tasks:
                    logging.error(et.current_state.str_extra())
                raise Exception(str(len(error_tasks))+" tasks failed")
            tasks = [t for t in tasks if not isinstance(t.current_state, TerminalState)]

            if buffer_empty and deliver_empty and len(tasks) == 0:
                break

            if time.time() > last_print+1:
                print_task_states(tasks)
                logging.debug("buffer empty: "+str(buffer_empty)+', deliver empty: '+str(deliver_empty))
                last_print = time.time()
            time.sleep(0.001)
            # sleep to avoid spinning, we can use notification instead, but so far, this works.
            # it may increase overall latency by at most n*0.001 second, where n is length of pipeline

        logging.info('finish scheduling pipeline')


class BarrierScheduler(object):
    """Imagine an invisible barrier between stages"""
    @classmethod
    def schedule(cls, pipeline):
        logging.info('start scheduling pipeline')
        last_print = 0
        tasks = []
        count = 0
        while True:
            buffer_empty = True
            deliver_empty = True
            for key, stage in pipeline.stages.iteritems():
                stage.deliver_func = default_deliver_func if stage.deliver_func is None else stage.deliver_func
                if not stage.buffer_queue.empty():
                    if count == 0:
                        count = stage.buffer_queue.qsize()
                    buffer_empty = False
                    stage.deliver_func(stage.buffer_queue, stage.deliver_queue)

            for key, stage in pipeline.stages.iteritems():
                if stage.deliver_queue.qsize() == count:
                    while not stage.deliver_queue.empty():
                        deliver_empty = False
                        t = tracker.Task(stage.lambda_function, stage.init_state, stage.deliver_queue.get(), stage.emit, stage.event)
                        tasks.append(t)
                        tracker.Tracker.submit(t)
                        logging.debug('submitted a task: '+str(t))

            error_tasks = [t for t in tasks if isinstance(t.current_state, ErrorState)]
            if len(error_tasks) > 0:
                logging.error(str(len(error_tasks))+" tasks failed: ")
                for et in error_tasks:
                    logging.error(et.current_state.str_extra())
                raise Exception(str(len(error_tasks))+" tasks failed")
            tasks = [t for t in tasks if not isinstance(t.current_state, TerminalState)]

            if buffer_empty and deliver_empty and len(tasks) == 0:
                break

            if time.time() > last_print+1:
                print_task_states(tasks)
                last_print = time.time()
            time.sleep(0.001)
            # sleep to avoid spinning, we can use notification instead, but so far, this works.
            # it may increase overall latency by at most n*0.001 second, where n is length of pipeline

        logging.info('finish scheduling pipeline')


class LadderScheduler(object):
    """A scheduler that scans every stage for events and submits at most N_TASKS each second"""
    N_TASKS = 50

    @classmethod
    def schedule(cls, pipeline):
        logging.info('start scheduling pipeline')
        last_fill = 0
        quota = 0
        last_print = 0
        tasks = []
        gen = None
        while True:
            buffer_empty = True
            deliver_empty = True
            for key, stage in pipeline.stages.iteritems():
                stage.deliver_func = default_deliver_func if stage.deliver_func is None else stage.deliver_func
                if not stage.buffer_queue.empty():
                    buffer_empty = False
                    stage.deliver_func(stage.buffer_queue, stage.deliver_queue)

            for key, stage in pipeline.stages.iteritems():
                if not stage.deliver_queue.empty():
                    deliver_empty = False
                    break

            if buffer_empty and deliver_empty and len(tasks) == 0:
                break

            if time.time() > last_fill + 1:
                quota = cls.N_TASKS
                last_fill = time.time()

            def task_gen():
                for key, stage in pipeline.stages.iteritems():
                    while not stage.deliver_queue.empty():
                        t = tracker.Task(stage.lambda_function, stage.init_state, stage.deliver_queue.get(), stage.emit, stage.event)
                        yield t

            while quota > 0:
                try:
                    if gen is None:
                        gen = task_gen()
                    t = gen.next()
                    tasks.append(t)
                    tracker.Tracker.submit(t)
                    logging.debug('submitted a task: '+str(t))
                    quota -= 1
                except StopIteration:
                    gen = task_gen()
                    break

            error_tasks = [t for t in tasks if isinstance(t.current_state, ErrorState)]
            if len(error_tasks) > 0:
                logging.error(str(len(error_tasks))+" tasks failed: ")
                for et in error_tasks:
                    logging.error(et.current_state.str_extra())
                raise Exception(str(len(error_tasks))+" tasks failed")
            tasks = [t for t in tasks if not isinstance(t.current_state, TerminalState)]


            if time.time() > last_print+1:
                print_task_states(tasks)
                last_print = time.time()
            time.sleep(0.001)
            # sleep to avoid spinning, we can use notification instead, but so far, this works.
            # it may increase overall latency by at most n*0.001 second, where n is length of pipeline

        logging.info('finish scheduling pipeline')


class PriorityLadderScheduler(object):
    """A scheduler that scans every stage for events and submits at most N_TASKS with highest priority each second"""
    N_TASKS = 50

    @classmethod
    def schedule(cls, pipeline):
        logging.info('start scheduling pipeline')
        last_fill = 0
        quota = 0
        last_print = 0
        tasks = []
        gen = None
        while True:
            buffer_empty = True
            deliver_empty = True
            for key, stage in pipeline.stages.iteritems():
                stage.deliver_func = default_deliver_func if stage.deliver_func is None else stage.deliver_func
                if not stage.buffer_queue.empty():
                    buffer_empty = False
                    stage.deliver_func(stage.buffer_queue, stage.deliver_queue)

            for key, stage in pipeline.stages.iteritems():
                if not stage.deliver_queue.empty():
                    deliver_empty = False
                    break

            if buffer_empty and deliver_empty and len(tasks) == 0:
                break

            if time.time() > last_fill + 1:
                quota = cls.N_TASKS
                last_fill = time.time()

            def task_gen(quota):
                all_items = []
                for key, stage in pipeline.stages.iteritems():
                    while not stage.deliver_queue.empty():
                        item = stage.deliver_queue.get()
                        all_items.append((item, stage))

                sorted_items = sorted(all_items, key=lambda item: int(item[0]['metadata']['lineage']))
                ret = []
                for i in xrange(min(quota, len(sorted_items))):
                    t = tracker.Task(sorted_items[i][1].lambda_function, sorted_items[i][1].init_state,
                                     sorted_items[i][0], sorted_items[i][1].emit, sorted_items[i][1].event)
                    ret.append(t)

                for item in sorted_items[quota:]:
                    item[1].deliver_queue.put(item[0])  # send back the items

                return ret

            task_list = task_gen(quota)
            for t in task_list:
                tasks.append(t)
                tracker.Tracker.submit(t)
                logging.debug('submitted a task: '+str(t))
                quota -= 1

            error_tasks = [t for t in tasks if isinstance(t.current_state, ErrorState)]
            if len(error_tasks) > 0:
                logging.error(str(len(error_tasks))+" tasks failed: ")
                for et in error_tasks:
                    logging.error(et.current_state.str_extra())
                raise Exception(str(len(error_tasks))+" tasks failed")
            tasks = [t for t in tasks if not isinstance(t.current_state, TerminalState)]


            if time.time() > last_print+1:
                print_task_states(tasks)
                last_print = time.time()
            time.sleep(0.001)
            # sleep to avoid spinning, we can use notification instead, but so far, this works.
            # it may increase overall latency by at most n*0.001 second, where n is length of pipeline

        logging.info('finish scheduling pipeline')
