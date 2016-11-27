# -*- coding: utf-8 -*-

# Full Imports
import simplejson as json
import collections
import itertools
import six
import sys
import logging
from job_spec import JobSpecWriter
from util import *

logger    = logging.getLogger(__name__)
nh        = logging.NullHandler()
formatter = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
nh.setFormatter(formatter)
logger.addHandler(nh)
logger.setLevel(logging.DEBUG)

class JobSpecParser(object):
  def __init__(self, job_spec):
    self.filename = job_spec
    self.parse_job_spec_file()
    self.parse_common_args()
    self.parse_lambda_entries()

  def parse_job_spec_file(self):
    with open(self.filename) as fd:
      self.json_dump = json.load(fd)

  def parse_common_args(self):
    self.job_spec  = {
      'command'        : self.json_dump['command'],
      'args'           : self.json_dump['args'],
      'input_bucket'   : self.json_dump['input_bucket'],
      'input_prefix'   : self.json_dump['input_prefix'],
      'output_bucket'  : self.json_dump['output_bucket'],
      'output_prefix'  : self.json_dump['output_prefix'],
    }

  def parse_lambda_entries(self):
    self.lambdas = int(self.json_dump['args']['args']['-n'])
    self.frames  = int(self.json_dump['args']['args']['-f'])
    for lambda_num in range(0, int(self.lambdas)):
      self.job_spec[str(lambda_num)] = self.json_dump[str(lambda_num)]

  @staticmethod
  def spawn_parser():
    parser          = cmd_line_parser.CmdLineParser()
    (options, args) = parser.get_options_and_args()
    parser          = JobSpecParser(options.filename)
    logger.debug(parser.json_dump)
    return parser

if __name__ == "__main__":
  JobSpecParser.spawn_parser()
