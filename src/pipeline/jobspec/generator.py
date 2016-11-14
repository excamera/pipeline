# -*- coding: utf-8 -*-

# Full Imports
import json
import simplejson
import collections
import itertools
import six
import sys
import logging
from job_spec import JobSpec
from util import *

logging.basicConfig()
logger = logging.getLogger('JOBSPEC')
logger.setLevel(logging.DEBUG)

class Generator(object):
  def __init__(self, options):
    self.options             = options
    self.input_bucket_value  = options.input_bucket
    self.input_prefix_value  = options.input_prefix
    self.output_bucket_value = options.output_bucket
    self.output_prefix_value = options.output_prefix
    self.operator            = options.operator
    self.lambdas             = options.concurrent_lambdas
    self.command_value       = mulib.MuLib.get_cmd_string(self.operator)
    self.args_value          = mulib.MuLib.get_coordinator_args(self.input_prefix_value, 
                                                                self.lambdas)
    self.user_spec           = options.user_spec
    self.filename            = options.filename
  
  def create_job_spec(self):
    if self.options is None:
      logger.error("[JOBSPEC] options hash is empty")
      raise Exception ("options hash is empty")
    try:
      logger.debug("[JOBSPEC] Creating Job Spec")
      job_spec = JobSpec(self.filename
                        , input_bucket=self.input_bucket_value
                        , input_prefix=self.input_prefix_value
                        , output_bucket=self.output_bucket_value
                        , output_prefix=self.output_prefix_value
                        , command=self.command_value
                        , args=self.args_value
                        )
      if job_spec is None:
        raise Exception ("[JOBSPEC] Job Spec creation failed for : " 
                          + str(self.options))
      job_spec.writeJsonToFile()
    except Exception as inst:
      logger.error(type(inst))
      logger.error(inst.args)
      logger.error (inst)

  @staticmethod
  def spawn_generator():
    logger.info("[JOBSPEC] Parsing the cmd line options")
    parser          = cmd_line_parser.CmdLineParser()
    (options, args) = parser.get_options_and_args()
    gen             = Generator(options)
    logger.info("[JOBSPEC] Generating job_spec")
    gen.create_job_spec()

if __name__ == "__main__":
  Generator.spawn_generator()
