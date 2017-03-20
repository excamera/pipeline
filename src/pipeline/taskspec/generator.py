#!/usr/bin/python

import logging
import random
import string

import simplejson as json
from time import localtime, strftime


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Generator(object):

    base_s3_dir = 's3://lixiang-lambda-test/'

    @staticmethod
    def parse(pipeline):
        try:
            ppl = json.loads(pipeline)
        except TypeError as e:
            logger.error(e.message)
            logger.error('Invalid pipeline')
            return None
        return ppl

    @staticmethod
    def generate(input, output=None, commands=[]):
        pipeline = {}

        pipeline['pipeid'] = strftime("%Y%m%d%H%M%S", localtime()) + ''.join(random.choice(string.ascii_uppercase) for _ in range(6))

        # create channels
        pipeline['channels'] = []

        for i in range(len(commands)+3):  # input, decoded, filtered[noprs], encoded
            pipeline['channels'].append({'channel': i,
                                         'URI': Generator.base_s3_dir+'temp/'+pipeline['pipeid']+'/'+str(i)+'/%08d.png',
                                         'type': 'png',
                                         'nchunks': None
                                         })

        pipeline['channels'][0]['URI'] = input
        extension = input[input.rfind('.')+1:]
        if extension in ['mp4', 'mkv', 'avi', 'mov', 'png', 'jpg', 'bmp']:
            pipeline['channels'][0]['type'] = extension
        else:
            pipeline['channels'][0]['type'] = None  # implicit type

        pipeline['channels'][0]['src'] = 'src'

        if output == None:
            pipeline['channels'][-1]['URI'] = Generator.base_s3_dir+'output/'+pipeline['pipeid']+'/%08d.mp4'
            pipeline['channels'][-1]['type'] = 'mp4'

        # create nodes
        pipeline['nodes'] = []

        pipeline['nodes'].append({'node': 0,
                                  'upstream': [0],
                                  'downstream': [1],
                                  'operator':'decode',
                                  'command':[]
                                  })

        for i in range(len(commands)):
            pipeline['nodes'].append({'node': i+1,
                                      'upstream': [i+1],
                                      'downstream': [i+2],
                                      'operator': commands[i][0],
                                      'command': commands[i][1]
                                      })

        pipeline['nodes'].append({'node': len(commands)+1,
                                  'upstream': [len(commands)+1],
                                  'downstream': [len(commands)+2],
                                  'operator':'encode_dash',
                                  'command':[]
                                  })

        # connect the channels

        for n in pipeline['nodes']:
            for ups in n['upstream']:
                pipeline['channels'][ups]['dest'] = n['node']
            for dns in n['downstream']:
                pipeline['channels'][dns]['src'] = n['node']

        for c in pipeline['channels']:
            if c['src'] == 'src':
                c['ready'] = True
            else:
                c['ready'] = False

            c['probed'] = False

            #    c['nchunks'] = ppl['nodes'][c['dest']]['nworkers']  # for source nodes, determine nchunk by nworker
            #    ppl['nodes'][c['dest']]['nchunks'] = c['nchunks']

        return pipeline

        # then decide the nchunk of each channel and node


        #remain = True
        #while remain:
        #    remain = False
        #    for c in ppl['channels']:
        #        if c['nchunks'] == None:
        #            if not ppl['nodes'][c['src']]['nchunks'] == None:
        #                c['nchunks'] = ppl['nodes'][c['src']]['nchunks'] * ppl['nodes'][c['src']]['amplification']
        #                if not c['dest'] == 'dest':
        #                    ppl['nodes'][c['dest']]['nchunks'] = c['nchunks']
        #            else:
        #                remain = True

        # define workers, assign the chunks

        # for n in ppl['nodes']:
        #     spec = {}
        #     spec['pipeid'] = ppl['pipeid']
        #     spec['workers'] = []

        #     spec['upstreams'] = [ppl['channels'][ch] for ch in n['upstream']]
        #     spec['downstreams'] = [ppl['channels'][ch] for ch in n['downstream']]

        #     spec['operator'] = n['operator']
        #     spec['engine'] = n['engine']

        #     spec['nchunks'] = n['nchunks']

        #     for src in spec['upstreams']:
        #         if src['src'] == 'src':
        #             src['type'], src['chunks'] = Generator.get_inputchunks(src, n['nchunks'])
        #             spec['nframes'] = src['chunks'][0][1]-src['chunks'][0][0]+1
        #         else:
        #             src['type'] = 'directory'
        #             src['chunks'] = [src['URI']%i for i in range(src['nchunks'])]

        #     for dest in spec['downstreams']:
        #         dest['type'] = 'frames'
        #         dest['chunks'] = src['chunks']

        #     for i in range(n['nworkers']):  # each worker get one or more chunks
        #         worker = {}
        #         worker['id'] = i
        #         worker['input-paths'] = []
        #         worker['output-paths'] = []

        #         for src in spec['upstreams']:
        #             cid = src['channel']
        #             curi = src['URI']
        #             ctype = src['type']
        #             chunks = [src['chunks'][j] for j in range(i, len(src['chunks']), n['nworkers'])]  # round-robin
        #             worker['input-paths'].append({'channel': cid, 'URI':curi, 'type':ctype, 'chunks': chunks})

        #         for dest in spec['downstreams']:
        #             cid = dest['channel']
        #             curi = dest['URI']
        #             ctype = dest['type']
        #             chunks = [dest['chunks'][idx] for idx in reduce(lambda x,y:x+y, [range(j*n['amplification'],
        #                             (j+1)*n['amplification']) for j in range(i, n['nchunks'], n['nworkers'])])]
        #             worker['output-paths'].append({'channel': cid, 'URI':curi, 'type':ctype, 'chunks': chunks})

        #         spec['workers'].append(worker)

        #     jobspecs.append(spec)

        # return jobspecs



