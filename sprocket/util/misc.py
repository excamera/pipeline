#!/usr/bin/python
import json
import os
import random

import sys
from multiprocessing import Process

import sprocket.controlling.common.socket_nb
import sprocket.controlling.common.defs
import pdb

###
#  Random string (lowercase letters and numbers).
###
def rand_str(slen):
    import random
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in xrange(slen))

###
#  random green
###
def rand_green(string):
    greens = [ -2, -10, -22, -28, -29, -34, -35, -40, -41, -46, -47, -48, -70, -71, -76, -77, -82, -83, -112, -118, -148, -154, 32 ]
    ngreens = len(greens)

    ostr = ''
    for i in range(0, len(string)):
        ostr += '\033['
        tgrn = greens[random.randint(0, ngreens-1)]
        rstblink = False
        rstinvert = False
        if random.randint(0, 3):
            ostr += '1;'
        if not random.randint(0, 39):
            ostr += '5;'
            rstblink = True
        if not rstblink and not random.randint(0, 14):
            ostr += '7;'
            rstinvert = True
        if tgrn < 0:
            ostr += '38;5;' + str(-1 * tgrn) + 'm'
        else:
            ostr += str(tgrn) + 'm'
        ostr += string[i]
        if rstblink or rstinvert:
            bstr = '25' if rstblink else ''
            istr = '27' if rstinvert else ''
            sc = ';' if rstblink and rstinvert else ''
            ostr += '\033[' + bstr + sc + istr + 'm'

    ostr += '\033[0m'
    return ostr


###
#  load cert or pkey from file
###
def read_pem(fname):
    ret = ""
    with open(fname) as f:
        started = False
        for line in f:
            if line[:11] == "-----BEGIN ":
                started = True
                continue
            elif line[:9] == "-----END ":
                break

            if started:
                ret += line.rstrip()

    return ret


class ForkedPdb(pdb.Pdb):
    """A Pdb subclass that may be used
    from a forked multiprocessing child
    Borrowed from <http://stackoverflow.com/a/23654936> for debugging.
    """
    def interaction(self, *args, **kwargs):
        _stdin = sys.stdin
        try:
            sys.stdin = open('/dev/stdin')
            pdb.Pdb.interaction(self, *args, **kwargs)
        finally:
            sys.stdin = _stdin


def mock_launch(n, func, akid, secret, event, regions):
    for i in xrange(n):
        p = Process(target=lambda_setup, args=(json.loads(event),))
        p.start()


def lambda_setup(event):
    os.chdir(os.path.dirname(os.path.realpath(__file__)) + '/../mock_lambda/')
    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../mock_lambda/')
    event['rm_tmpdir'] = 0
    import lambda_function_template
    lambda_function_template.lambda_handler(event, None)

def escape_for_csv(msg):
    # see https://stackoverflow.com/a/769675/2144939
    if ',' in msg:
        msg.replace('"', '""')
        msg = '"' + msg + '"'
    return msg.replace('\n', '\\n')
