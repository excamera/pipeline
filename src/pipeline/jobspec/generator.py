# -*- coding: utf-8 -*-

# Full Imports
import json
import simplejson
import collections
import itertools
import six
import sys
import logging
import job_spec
from util import *

# Global Variables declaration
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Generator(object):
  def __init__(self, options):
    self.options       = options
    self.input_bucket  = options.input_bucket
    self.input_prefix  = options.input_prefix
    self.output_bucket = options.output_bucket
    self.output_prefix = options.output_prefix
    self.operator      = options.operator
    self.lambdas       = options.concurrent_lambdas
    self.command       = mulib.MuLib.get_cmd_string(self.operator)
    self.args          = mulib.MuLib.get_coordinator_args(self.input_prefix, self.lambdas)
  
  def create_job_spec(self):
    if self.options is None:
      logger.error("[JOBSPEC] options hash is empty")
      raise Exception ("options hash is empty")
    try:
      logger.debug("[JOBSPEC] Creating Job Spec")
      job_spec = JobSpec(input_bucket=input_bucket_value
                        , input_prefix=input_prefix_value
                        , output_bucket=output_bucket_value
                        , output_prefix=output_prefix_value
                        , command=command_value
                        , args=args_value                    
                        )
      if job_spec is None:
        raise Exception ("[JOBSPEC] Job Spec creation failed for : " 
                          + str(self.options))
      job_spec.writeJsonToFile(ofd)
    except Exception as inst:
      logger.error(type(inst))
      logger.error(inst.args)
      logger.error (inst)

  @staticmethod
  def spawn_generator():
    logger.debug("[JOBSPEC] Parsing the cmd line options")
    parser = cmd_line_parser.CmdLineParser()
    (options, args) = parser.get_options_and_args()
    logger.debug("[JOBSPEC] Generating job_spec")
    gen = Generator(options)
    gen.create_job_spec()

if __name__ == "__main__":
  Generator.spawn_generator()
