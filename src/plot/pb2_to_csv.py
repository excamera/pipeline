from __future__ import print_function
import sys
import os
import pdb
import re

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__))+'/../../external/mu/src/lambdaize/libmu/')

import joblog_pb2

fields = ('ts', 'lineage', 'op', 'msg', 'stage', 'worker_called', 'num_frames')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: %s pb2_log_file' % sys.argv[0], file=sys.stderr)
        sys.exit(1)
    
    with open(sys.argv[1], 'rb') as f:
        with open(sys.argv[1]+'.csv', 'w+') as fout:
            jl = joblog_pb2.JobLog()
            jl.ParseFromString(f.read())

            def define_fields(line):
                ret = []
                for a in fields:
                    if hasattr(line, a):
                        ret.append(getattr(line, a))
                return ret

            for l in jl.record:
                fout.write(', '.join([str(f) for f in define_fields(l)])+'\n')
            
            

