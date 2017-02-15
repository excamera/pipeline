#!/usr/bin/python

import logging
import simplejson as json


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Generator(object):

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
    def generate(ppl):
        jobspecs = []

        # connect the channels

        for n in ppl['nodes']:
            for ups in n['upstream']:
                ppl['channels'][ups]['dest'] = n['node']
            for dns in n['downstream']:
                ppl['channels'][dns]['src'] = n['node']

        for c in ppl['channels']:
            if c['URI'].startswith('channel'):  # materialize temp channels
                c['URI'] = 's3://lixiang-lambda-test/temp/' + ppl['pipeid'] + '/' + c['URI'][c['URI'].index('://') + 3:]
                c['URI'] = c['URI'] % (str(c['channel']), '%s')

            if c['src'] == 'src':
                c['ready'] = True
            else:
                c['ready'] = False

            c['probed'] = False

            #    c['nchunks'] = ppl['nodes'][c['dest']]['nworkers']  # for source nodes, determine nchunk by nworker
            #    ppl['nodes'][c['dest']]['nchunks'] = c['nchunks']

        return ppl

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


if __name__ == '__main__':

    pipe = '''{
            "pipeid" : "sjf2JY1f",
            "channels" :
                [
                    {"channel":0, "URI":"s3://lixiang-lambda-test/input/%08d.png", "nchunks":null, "type":"png", "src":"src", "dest":null},
                    {"channel":1, "URI":"s3://lixiang-lambda-test/output/%08d.png", "nchunks":null, "type":"mp4", "src":null, "dest":"dest"}
                ],
            "nodes" :
                [
                    {"node":0, "upstream":[0], "downstream":[1], "nworkers":10, "nchunks":null, "amplification":1, "engine": "aws-lambda", "operator": "crawlingtext", "command":[]}
                ]
        }'''

    gen = Generator()
    ppl = gen.parse(pipe)
    jobspecs = gen.generate(ppl)
    print json.dumps(jobspecs, indent=True)

