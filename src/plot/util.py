import os
import sys
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


def preprocess(lines, cmd_of_interest="", send_only=False):
    assert lines[0]['msg'] == 'starting pipeline' or lines[0]['msg'] == 'start pipeline' 
    assert lines[-1]['msg'] == 'pipeline finished' or lines[-1]['msg'] == 'finish pipeline' 

    start_ts = lines[0]['ts']
    data = OrderedDict()
    for i in xrange(1, len(lines)-1):  # first, last lines excluded
        l = lines[i]
        if l['lineage'] == '0' or (send_only and l['op'] != 'send'):
            continue
        if l['lineage'] not in data:
            data[l['lineage']] = []
        if l['msg'].startswith(cmd_of_interest):
            l['ts'] = l['ts']-start_ts # relative timestamp
            data[l['lineage']].append(l)

    return data


#def precise_time(lines, cmd_of_interest):
#    """
#        assumption: recv msg is right after the corresponding send msg (for the same lineage)
#    """
#    assert lines[0].split(',')[1].strip() == 'starting pipeline'
#    assert lines[-1].split(',')[1].strip() == 'pipeline finished'
#
#    data = dict()
#    current = dict()
#    for i in xrange(1, len(lines)-1):  # first, last lines excluded
#        fields = lines[i].split(',', 4)
#        ts = float(fields[0].strip())
#        lineage = fields[1].strip()
#        op = fields[2].strip()
#        msg = fields[3].strip()
#        if lineage == '0':
#            continue
#        if lineage in current:
#            start_record = current.pop(lineage)
#            lineage_records = data.get(lineage, [])
#            lineage_records.append({'msg': start_record['msg'][:10], 'duration': ts - start_record['ts']})
#            data[lineage] = lineage_records
#            continue
#        if op == 'send' and msg.startswith(cmd_of_interest):
#            current[lineage] = {'ts': ts, 'msg': msg}
#
#    return data  # not ordered
