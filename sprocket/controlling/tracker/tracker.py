#!/usr/bin/python
import Queue
import errno
import importlib
import json
import logging
import os
import select
import socket
import threading
import time
import traceback

import sys
import multiprocessing

import sprocket.controlling.common.defs
import sprocket.controlling.tracker.machine_state
import sprocket.controlling.tracker.util
import sprocket.platform
from sprocket.platform.launcher import LaunchEvent
from sprocket.config import settings
from sprocket.controlling.common.network import listen_socket
from sprocket.controlling.common.socket_nb import SocketNB
from sprocket.controlling.tracker.task import TaskStarter, OrphanedTask
from sprocket.util import lightlog
from sprocket.util.misc import read_pem


class Tracker(object):
    started = False
    started_lock = threading.Lock()
    should_stop = False

    launcher_pid = None
    submitted_queue = Queue.Queue()
    waiting_queues_lock = threading.Lock()
    waiting_queues = {}

    with open(settings['aws_access_key_id_file'], 'r') as f:
        akid = f.read().strip()
    with open(settings['aws_secret_access_key_file'], 'r') as f:
        secret = f.read().strip()

    cacert = read_pem(settings['cacert_file']) if 'cacert_file' in settings else None
    srvcrt = read_pem(settings['srvcrt_file']) if 'srvcrt_file' in settings else None
    srvkey = read_pem(settings['srvkey_file']) if 'srvkey_file' in settings else None

    @classmethod
    def _handle_server_sock(cls, ls, tasks, fd_task_map):
        batched_accept = True
        if batched_accept:
            while True:
                try:
                    (ns, addr) = ls.accept()
                    logging.debug("new conn from addr: %s:%s", addr[0], addr[1])
                except socket.error, e:
                    err = e.args[0]
                    if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
                        logging.error("error in accept: %s", e)
                    break
                ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                ns.setblocking(False)

                socknb = SocketNB(ns)
                socknb.do_handshake()

                task_starter = TaskStarter(socknb)
                tasks.append(task_starter)
                fd_task_map[task_starter.current_state.fileno()] = task_starter
        else:
            (ns, addr) = ls.accept()
            logging.debug("new conn from addr: %s:%s", addr[0], addr[1])
            ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            ns.setblocking(False)

            socknb = SocketNB(ns)
            socknb.do_handshake()
            # try:
            #     new_task = Tracker.waiting_queue.get(block=False)  # assume all tasks are the same
            # except Queue.Empty as e:
            #     logging.warning("get response from lambda, but no one's waiting?")
            #     return

            # new_task.start(ns)
            task_starter = TaskStarter(socknb)
            tasks.append(task_starter)
            fd_task_map[task_starter.current_state.fileno()] = task_starter

    @classmethod
    def _main_loop(cls):
        logging.info("tracker listening to port: %d" % settings['tracker_port'])
        lsock = listen_socket('0.0.0.0', settings['tracker_port'], cls.cacert, cls.srvcrt,
                              cls.srvkey, settings['tracker_backlog'])
        lsock_fd = lsock.fileno()

        tasks = []
        fd_task_map = {}
        poll_obj = select.poll()
        poll_obj.register(lsock_fd, select.POLLIN)
        npasses_out = 0

        while True:
            if cls.should_stop:
                if lsock is not None:
                    try:
                        lsock.shutdown(0)
                        lsock.close()
                    except:
                        logging.warning("failure shutting down the lsock")
                        pass
                    lsock = None
                    # os.kill(cls.pylaunch_pid, signal.SIGKILL)

            dflags = []
            for (tsk, idx) in zip(tasks, range(0, len(tasks))):
                st = tsk.current_state
                val = 0
                if st.sock is not None:
                    if not isinstance(st, sprocket.controlling.tracker.machine_state.TerminalState):  # always listening
                        val = val | select.POLLIN

                    if st.ssl_write or st.want_write:
                        val = val | select.POLLOUT

                    if val != tsk.rwflag:
                        tsk.rwflag = val
                        dflags.append(idx)

                else:
                    tsk.rwflag = 0
                    dflags.append(idx)
                    if not isinstance(st, sprocket.controlling.tracker.machine_state.TerminalState):
                        tsk.current_state = sprocket.controlling.tracker.machine_state.ErrorState(tsk.current_state,
                                                                                                  "sock closed in %s" % str(
                                                                                                      tsk))
                        logging.warning("socket closed abnormally: %s" % str(tsk))

            for idx in dflags:
                if tasks[idx].rwflag != 0:
                    poll_obj.register(tasks[idx].current_state, tasks[idx].rwflag)
                else:
                    try:
                        poll_obj.unregister(tasks[idx].current_state)
                    except Exception as e:
                        logging.error("unregister: " + str(e.message))
                        pass

            pfds = poll_obj.poll(2000)
            npasses_out += 1

            if len(pfds) == 0:
                if cls.should_stop:
                    break
                continue

            # look for readable FDs
            for (fd, ev) in pfds:
                if (ev & select.POLLIN) != 0:
                    if lsock is not None and fd == lsock_fd:
                        logging.debug("listening sock got conn in")
                        cls._handle_server_sock(lsock, tasks, fd_task_map)

                    else:
                        logging.debug("conn sock %d got buffer readable", fd)
                        task = fd_task_map[fd]
                        task.do_read()

            for (fd, ev) in pfds:
                if (ev & select.POLLOUT) != 0:
                    logging.debug("conn sock %d got buffer writable", fd)
                    task = fd_task_map[fd]
                    task.do_write()

            for tsk in [t for t in tasks if
                        isinstance(t.current_state, sprocket.controlling.tracker.machine_state.TerminalState)]:
                try:
                    poll_obj.unregister(tsk.current_state)
                except Exception as e:
                    logging.warning(e.message)
                try:
                    tsk.current_state.close()
                except Exception as e:
                    logging.warning(e.message)
                del fd_task_map[tsk.current_state.fileno()]

            should_append = []
            removable = []
            tasks = [t for t in tasks if
                     not isinstance(t.current_state, sprocket.controlling.tracker.machine_state.TerminalState)]
            for tsk in tasks:
                if tsk.current_state.want_handle:
                    if isinstance(tsk, TaskStarter):
                        # init msg lets us know which lambda function it's from
                        init_msg = tsk.current_state.recv_queue.popleft()
                        init_data = json.loads(init_msg)
                        with Tracker.waiting_queues_lock:
                            # so that we can get Task from the corresponding list
                            try:
                                func_queue = Tracker.waiting_queues.get(init_data['lambda_function'], [])
                                real_task = func_queue.pop(0)
                                if len(func_queue) == 0:
                                    del Tracker.waiting_queues[init_data['lambda_function']]  # GC
                                if 'lambda_start_ts' in init_data:
                                    logger = lightlog.getLogger(
                                        real_task.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                                    logger.debug(ts=init_data['lambda_start_ts'],
                                                 lineage=real_task.kwargs['in_events'].values()[0]['metadata'][
                                                     'lineage'],
                                                 op='recv', msg='lambda_start_ts')
                            except IndexError:
                                real_task = OrphanedTask()  # task doesn't exist
                                logging.info("get an orphaned lambda function, sending quit")

                        real_task.rewire(tsk.current_state)  # transition to a Task
                        fd_task_map[tsk.current_state.fileno()] = real_task
                        tsk.current_state.update_flags()
                        should_append.append(real_task)
                        removable.append(tsk)
            tasks.extend(should_append)
            for r in removable:
                tasks.remove(r)

            for tsk in tasks:
                if tsk.current_state.want_handle:
                    if not isinstance(tsk, TaskStarter):
                        tsk.do_handle()

    @classmethod
    def _invocation_loop(cls):
        testsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        testsock.connect(("lambda.us-east-1.amazonaws.com", 443))  # incorrect when running on EC2
        addr = testsock.getsockname()[0]
        testsock.close()
        platform_name = settings.get('platform', 'aws_lambda')
        launcher_module = importlib.import_module('sprocket.platform.' + platform_name + '.launcher')
        launcher_cls = launcher_module.Launcher
        launch_queue = multiprocessing.Queue()

        pid = os.fork()
        if pid == 0:
            launcher_cls.initialize(launch_queue)
            sys.exit(0)

        cls.launcher_pid = pid

        # cls._invoc_pr = cProfile.Profile()
        # cls._invoc_pr.enable()

        while not cls.should_stop:
            pending = {}  # function name -> tasklist

            t = cls.submitted_queue.get(block=True)
            lst = pending.get(t.lambda_func, [])
            lst.append(t)
            pending[t.lambda_func] = lst

            while True:
                try:
                    t = cls.submitted_queue.get(block=False)
                    lst = pending.get(t.lambda_func, [])
                    lst.append(t)
                    pending[t.lambda_func] = lst
                except Queue.Empty:
                    break

            for k, v in pending.iteritems():
                with cls.waiting_queues_lock:
                    wq = cls.waiting_queues.get(k, [])
                    wq.extend(v)
                    cls.waiting_queues[k] = wq

            for func, lst in pending.iteritems():
                lst[0].event['addr'] = settings['daemon_addr']
                start = time.time()
                payload = json.dumps(lst[0].event)
                launch_queue.put(
                    LaunchEvent(nlaunch=len(lst), fn_name=func, akid=cls.akid, secret=cls.secret, payload=payload,
                                regions=lst[0].regions))

                for p in lst:
                    # logger = logging.getLogger(p.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                    # logger.debug('%s, %s', p.kwargs['in_events'].values()[0]['metadata']['lineage'], 'send, request')
                    logger = lightlog.getLogger(p.kwargs['in_events'].values()[0]['metadata']['pipe_id'])
                    logger.debug(ts=time.time(), lineage=p.kwargs['in_events'].values()[0]['metadata']['lineage'],
                                 op='send', msg='invocation')

                logging.debug("invoking " + str(len(lst)) + ' workers takes ' + str(
                    (time.time() - start) * 1000) + ' ms')

            time.sleep(0.001)

    @classmethod
    def _start(cls):
        with cls.started_lock:
            if cls.started:
                return

            # pr = cProfile.Profile()
            def profile_main():
                # pr.enable()
                cls._main_loop()
                # pr.disable()
                # pr.dump_stats('tracker_prof_output.cprof')
                # logging.info("tracker_prof_output written")
                logging.info("finish main loop")

            mt = threading.Thread(target=profile_main)
            mt.setDaemon(False)
            mt.start()
            it = threading.Thread(target=cls._invocation_loop)
            it.setDaemon(True)
            it.start()
            cls.started = True

    @classmethod
    def stop(cls):
        cls.should_stop = True

    @classmethod
    def submit(cls, task):
        if not cls.started:
            cls._start()
        cls.submitted_queue.put(task)

    @classmethod
    def kill(cls, task):
        raise NotImplementedError('kill')
