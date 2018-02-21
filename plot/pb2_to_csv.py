from __future__ import print_function
import sys
import os
import pdb
import re

import sprocket.util.joblog_pb2 as joblog_pb2

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
            with open(sys.argv[1]+'_meta', 'w+') as meta:
                meta.write(jl.metadata)
            for l in jl.record:
                fout.write(', '.join([f.encode('utf-8') if isinstance(f, unicode) else str(f) for f in define_fields(l)])+'\n')
