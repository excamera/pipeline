#!/usr/bin/python
# coding=utf-8
import logging
import pdb

from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, OnePassState, IfElseState, ErrorState
from sprocket.stages import InitStateTemplate, util, FinalStateTemplate
from sprocket.stages.util import default_trace_func, get_output_from_message


class FinalState(FinalStateTemplate):
    pass

class StartEmitSteal(OnePassState):
    command = 'emit:##TMPDIR##/out_1/ {out_key}'
    def __init__(self, prevState):
        super(StartEmitSteal, self).__init__(prevState)
        params = {'out_key': self.local['out_key']}
        self.command = self.command.format(**params) if self.command is not None else None


class WaitForEmitAndStealWork(OnePassState):
    expect = 'OK:'

    def __init__(self, prevState):
        super(WaitForEmitAndStealWork, self).__init__(prevState)

    def post_transition(self):
        def peer_finished(_):
            self.local['peer_finished'] = True
            if self.local.get('own_finished'):
                return FinalState
            return WaitForEmitAndStealWork
        def emit_finished(_):
            self.emit_event('chunks', {'metadata': self.in_events['chunks']['metadata'], 'key': self.local['out_key']})
            if self.local.get('steal_work'):
                return WaitForEmitAndStealWork
            return FinalState
        def emit_steal_finished(_):
            self.emit_event('chunks', {'metadata': self.in_events['chunks']['metadata'], 'steal_work': True, 'key': self.local['out_key']})
            peer_lineage = self.local['lineage'] + 1 if self.local['relative_pos'] == 0 else self.local['lineage'] - 1
            peer = self.pipe['tasks'].get(peer_lineage)
            if peer:
                peer.send_async_msg('OK:PEER_FINISH_2')
            return FinalState
        message_types = {'OK:RETVAL(0)': lambda _: StartEmitSteal,
                         'OK:EMIT(##TMPDIR##/out_0': emit_finished,
                         'OK:EMIT(##TMPDIR##/out_1': emit_steal_finished,
                         'OK:EMITTING': lambda _: WaitForEmitAndStealWork,
                         'OK:RUNNING': lambda _: WaitForEmitAndStealWork,
                         'OK:PEER_FINISH_1': peer_finished,
                         'OK:PEER_FINISH_2': lambda _: FinalState
        }
        for mtype in message_types:
            if self.messages[-1][:len(mtype)] == mtype:
                self.nextState = message_types[mtype](self.messages[-1])
        return self.nextState(self)

StartEmitSteal.nextState = WaitForEmitAndStealWork

class StartEmitAndStealwork(CommandListState):
    nextState = WaitForEmitAndStealWork
    commandlist = [(None, 'emit:##TMPDIR##/out_0/ {out_key}'),
                   (None, 'run:./ffmpeg -y -ss {starttime} -t 1 -i $(find ##TMPDIR##/in_0/ -name "*mp4"|head -n1) {transform} ##TMPDIR##/out_1/peer_output.mp4')]

    def __init__(self, prevState):
        super(StartEmitAndStealwork, self).__init__(prevState)
        params = {'out_key': self.local['out_key'], 'transform': self.config.get('transform', '-vf null'), 'starttime': 1-self.local['relative_pos']}
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]

class StartEmitOnly(OnePassState):
    expect = None
    nextState = WaitForEmitAndStealWork
    command = 'emit:##TMPDIR##/out_0/ {out_key}'

    def __init__(self, prevState):
        super(StartEmitOnly, self).__init__(prevState)
        params = {'out_key': self.local['out_key']}
        self.command = self.command.format(**params) if self.command is not None else None


class WaitForRun(OnePassState):
    expect = 'OK:'

    def __init__(self, prevState):
        super(WaitForRun, self).__init__(prevState)

    def post_transition(self):
        def finishrun(_):
            self.local['own_finished'] = True
            peer_lineage = self.local['lineage'] + 1 if self.local['relative_pos'] == 0 else self.local['lineage'] - 1
            peer = self.pipe['tasks'].get(peer_lineage)
            if peer:
                peer.send_async_msg('OK:PEER_FINISH_1')
            if self.local.get('peer_finished'):
                return StartEmitOnly
            self.local['steal_work'] = True
            return StartEmitAndStealwork
        def peer_finished(_):
            self.local['peer_finished'] = True
            return WaitForRun

        message_types = {'OK:RETVAL(0)': finishrun,
                         'OK:RETVAL(': lambda _: ErrorState,
                         'OK:RUNNING': lambda _: WaitForRun,
                         'OK:PEER_FINISH_1': peer_finished,
                         'OK:PEER_FINISH_2': lambda _: FinalState
        }
        for mtype in message_types:
            if self.messages[-1][:len(mtype)] == mtype:
                self.nextState = message_types[mtype](self.messages[-1])
                break
        return self.nextState(self)


class StartRun(OnePassState):
    expect = None
    command = 'run:sleep {delay}; ./ffmpeg -y -ss {starttime} -t 1 -i $(find ##TMPDIR##/in_0/ -name "*mp4"|head -n1) {transform} ##TMPDIR##/out_0/own_output.mp4'
    nextState = WaitForRun

    def __init__(self, prevState):
        super(StartRun, self).__init__(prevState)
        self.local['out_key'] = util.get_output_key()

        params = {'delay': self.local['relative_pos']*0, 'key': self.in_events['chunks']['key'], 'out_key': self.local['out_key'],
                  'transform': self.config.get('transform', '-vf null'), 'starttime': self.local['relative_pos']}
        self.command = self.command.format(**params) if self.command is not None else None


class WaitForCollect(OnePassState):
    expect = 'OK:'

    def __init__(self, prevState):
        super(WaitForCollect, self).__init__(prevState)

    def post_transition(self):
        def peer_finished(_):
            self.local['peer_finished'] = True
            return WaitForCollect

        message_types = {'OK:COLLECT(': lambda _: StartRun,
                         'OK:COLLECTING': lambda _: WaitForCollect,
                         'OK:PEER_FINISH_1': peer_finished,
                         'OK:PEER_FINISH_2': lambda _: FinalState
        }
        for mtype in message_types:
            if self.messages[-1][:len(mtype)] == mtype:
                self.nextState = message_types[mtype](self.messages[-1])
        return self.nextState(self)


class SetupState(CommandListState):
    extra = "(run)"
    nextState = WaitForCollect
    commandlist = [(None, 'run:ps -eTf|grep "python\|ffmpeg"; mkdir -p ##TMPDIR##/in_0/ ##TMPDIR##/out_0/ ##TMPDIR##/out_1/')
        , ('OK:RETVAL(0)', 'seti:nonblock:1')
        , ('OK:SETI', 'collect:{key} ##TMPDIR##/in_0')]


    def __init__(self, prevState):
        super(SetupState, self).__init__(prevState)
        self.local['out_key'] = util.get_output_key()
        params = {'key': self.in_events['chunks']['key'], 'out_key': self.local['out_key'],
                  'transform': self.config.get('transform', '-vf null'), 'own_starttime': self.local['relative_pos']}
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(InitStateTemplate):
    nextState = SetupState

    def __init__(self, prevState, **kwargs):
        super(InitState, self).__init__(prevState, **kwargs)
        self.trace_func = lambda ev, msg, op: default_trace_func(ev, msg, op, stage='C_C_stealwork_transform')
        self.local['peer_finished'] = not self.config.get('stealwork')
        self.local['lineage'] = int(self.in_events['chunks']['metadata']['lineage'])
        tasks = self.pipe.setdefault('tasks', {})
        tasks[self.local['lineage']] = self.task
        self.local['relative_pos'] = (self.local['lineage'] - 1) % 2  # 0 or 1 depending on preceding or succeeding its peer

