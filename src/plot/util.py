from __future__ import print_function
import os
import sys
import pdb
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__))+'/../../external/mu/src/lambdaize/libmu/')

import joblog_pb2

fields = ('ts', 'lineage', 'op', 'msg', 'stage', 'worker_called', 'num_frames')


def read_records(input_file):
    if input_file.endswith('.csv'):
        with open(input_file, 'r') as f:

            def define_fields(line):
                fields = line.split(', ')
                if len(fields) == 2:
                    return {'ts': float(fields[0]), 'msg': fields[1]}
                elif len(fields) >= 4:
                    return {'ts': float(fields[0]), 'lineage': fields[1], 'op': fields[2], 'msg': ', '.join(fields[3:])}
                else:
                    raise Exception('undefined fields: %s' % line)

            records = [define_fields(l.strip()) for l in f.readlines()]
    else:
        with open(input_file, 'rb') as f:
            jl = joblog_pb2.JobLog()
            jl.ParseFromString(f.read())

            def define_fields(line):
                ret = {}
                for a in fields:
                    if hasattr(line, a):
                        ret[a] = getattr(line, a)
                return ret

            records = [define_fields(l) for l in jl.record]
    return records


def preprocess(records, cmd_of_interest="", send_only=False):
    assert records[0]['msg'] == 'starting pipeline' or records[0]['msg'] == 'start pipeline'
    assert records[-1]['msg'] == 'pipeline finished' or records[-1]['msg'] == 'finish pipeline'

    start_ts = records[0]['ts']
    lineages = OrderedDict()
    for i in xrange(1, len(records)-1):  # first, last records excluded
        l = records[i]
        if l['lineage'] == '0' or (send_only and l['op'] != 'send'):
            continue
        if l['lineage'] not in lineages:
            lineages[l['lineage']] = []
        if l['msg'].startswith(cmd_of_interest):
            l['ts'] = l['ts']-start_ts # relative timestamp
            lineages[l['lineage']].append(l)

    return lineages


def get_intervals(lineages, start_selector, end_selector):
    intervals = {}
    for lineage, recs in lineages.iteritems():
        start_points = [r for r in recs if start_selector(lineage, r)]
        end_points = [r for r in recs if end_selector(lineage, r)]
        if len(start_points) != 1:
            print('found %d start points: %s' % (len(start_points), start_points), file=sys.stderr)
        elif len(end_points) != 1:
            pass
            print('found %d end points: %s' % (len(end_points), end_points), file=sys.stderr)
        elif start_points[0]['ts'] > end_points[0]['ts']:
            raise Exception('start point later than end point')
        else:
            intervals[lineage] = end_points[0]['ts'] - start_points[0]['ts']

    return intervals
