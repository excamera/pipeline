import os
import json
import logging
import traceback
import grpc
import pdb
import time
from concurrent import futures

import cProfile, pstats, StringIO

import libmu.lightlog as lightlog
import pipeline
from pipeline.schedule import *
from pipeline.service import pipeline_pb2
from pipeline.service import pipeline_pb2_grpc
from pipeline.config import settings
from pipeline.util.amend_mpd import amend_mpd
from pipeline.util.media_probe import get_signed_URI

_server = None


class PipelineServer(pipeline_pb2_grpc.PipelineServicer):
    def Submit(self, request, context):
        logging.info('PipelineServer handling submit request')
        try:
            pipe = pipeline.create_from_spec(json.loads(request.pipeline_spec))

            for index in range(len(request.inputs)):
                in_event = {'key': request.inputs[index].value,
                            'metadata': {'pipe_id': pipe.pipe_id, 'lineage': '0'}}
                pipe.inputs['input_' + str(index)][1].put({request.inputs[index].type: in_event})
                # put events to the buffer queue of all input stages

            pipe_dir = 'logs/' + pipe.pipe_id
            os.system('mkdir -p ' + pipe_dir)

            # handler = logging.FileHandler(pipe_dir + '/log.csv')
            # handler.setLevel(logging.DEBUG)
            # handler.setFormatter(logging.Formatter('%(created)f, %(message)s'))
            #memhandler = logging.handlers.MemoryHandler(1024**2*10, target=handler)
            #memhandler.shouldflush = lambda _: False

            logger = lightlog.getLogger(pipe.pipe_id)
            # logger = logging.getLogger(pipe.pipe_id)
            # logger.propagate = False
            # logger.setLevel(logging.DEBUG)
            # logger.addHandler(memhandler)
            # logger.addHandler(handler)

            conf_sched = settings.get('scheduler', 'SimpleScheduler')
            candidates = [s for s in dir(pipeline.schedule) if hasattr(vars(pipeline.schedule)[s], conf_sched)]
            if len(candidates) == 0:
                logging.error("scheduler %s not found", conf_sched)
                raise ValueError("scheduler %s not found" % conf_sched)
            sched = getattr(vars(pipeline.schedule)[candidates[0]], conf_sched)  # only consider the first match

            logger.info(str(time.time())+', starting pipeline')
            sched.schedule(pipe)
            logger.info(str(time.time())+', pipeline finished')

            logging.info("pipeline: %s finished", pipe.pipe_id)
            with open(pipe_dir + '/log.csv', 'w') as f:
                for l in logger.cached:
                    f.write(l+'\n')

            #memhandler.flush()
            result_queue = pipe.outputs.values()[0][1]  # there should be only one output queue

            num_m4s = 0
            out_key = None
            logging.debug("length of output queue: %s", result_queue.qsize())

            duration = 0.0
            while not result_queue.empty():
                chunk = result_queue.get(block=False)['chunks']  # TODO: should named chunks or m4schunks
                num_m4s += 1
                duration += chunk['duration']
                if int(chunk['metadata']['lineage']) == 1:
                    out_key = chunk['key']

            logging.info("number of m4s chunks: %d", num_m4s)
            logging.info("total duration: %f", duration)
            if out_key is not None:
                os.system('aws s3 cp ' + out_key + '00000001_dash.mpd ' + pipe_dir + '/')
                logging.info('mpd downloaded')
                with open(pipe_dir + '/00000001_dash.mpd', 'r') as fin:
                    init_mpd = fin.read()

                final_mpd = amend_mpd(init_mpd, duration, out_key, num_m4s)

                logging.info('mpd amended')
                with open(pipe_dir + '/output.xml', 'wb') as fout:
                    fout.write(final_mpd)

                os.system('aws s3 cp ' + pipe_dir + '/output.xml ' + out_key)
                logging.info('mpd uploaded')
                signed_mpd = get_signed_URI(out_key + 'output.xml')
                logging.info('mpd signed, returning')

                return pipeline_pb2.SubmitReply(success=True, mpd_url=signed_mpd)

            else:
                return pipeline_pb2.SubmitReply(success=False, error_msg='no output is found')

        except Exception as e:
            if 'pipe_dir' in vars():
                with open(pipe_dir + '/log.csv', 'w') as f:
                    for l in logger.cached:
                        f.write(l+'\n')
            return pipeline_pb2.SubmitReply(success=False, error_msg=traceback.format_exc())


def serve():
    global _server
    _server = grpc.server(futures.ThreadPoolExecutor(max_workers=1000))
    pipeline_pb2_grpc.add_PipelineServicer_to_server(PipelineServer(), _server)
    _server.add_insecure_port('0.0.0.0:%d' % settings['daemon_port'])
    _server.start()


def stop(val):
    global _server
    _server.stop(val)
