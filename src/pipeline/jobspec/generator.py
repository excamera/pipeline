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
 
  def create_job_spec(self, lambdaNum):
    if self.options is None:
      logger.error("[JOBSPEC] options hash is empty")
      raise Exception ("options hash is empty")
    try:
      logger.debug("[JOBSPEC] Creating Job Spec")
      self.args_value = prepare_args()
      job_spec_writer = JobSpecWriter(self.filename
                        , input_bucket=self.input_bucket_value
                        , input_prefix=self.input_prefix_value
                        , output_bucket=self.output_bucket_value
                        , output_prefix=self.output_prefix_value
                        , command=self.command_value
                        , args=self.args_value
                        )
      if job_spec_writer is None:
        raise Exception ("[JOBSPEC] Job Spec creation failed for : " 
                          + str(self.options))
      ofd = open(self.filename, "w")
      job_spec_writer.writeJsonToFile(ofd, job_spec_writer.params_hash)
      ofd.close()
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

class JobSpecGenerator(Generator):
  def initialize(self, options):
    self.job_spec            = {}
    self.options             = options
    self.input_bucket_value  = options.input_bucket
    self.input_prefix_value  = options.input_prefix
    self.output_bucket_value = options.output_bucket
    self.output_prefix_value = options.output_prefix
    self.operator            = options.operator
    self.lambdas             = options.concurrent_lambdas
    self.command_value       = mulib.MuLib.get_cmd_string(self.operator)
    self.args_value          = mulib.MuLib.get_coordinator_args(self.input_bucket_value,
                                                                self.input_prefix_value,
                                                                self.lambdas)
    self.user_spec           = options.user_spec
    self.filename            = options.filename
    self.job_spec            = {
      'command'        : self.command_value,
      'args'           : self.args_value,
      'input_bucket'   : self.input_bucket_value,
      'input_prefix'   : self.input_prefix_value,
      'output_bucket'  : self.output_bucket_value,
      'output_prefix'  : self.output_prefix_value,
    }
    self.counter             = 0

  def __init__(self, options):
    logger.debug("[JOBSPEC] JobSpecGenerator")
    self.initialize(options)

  def generate_entry(self, lambdaNum):
   chunks = s3lib.get_input_chunks(
                       self.input_bucket_value,
                       self.input_prefix_value,
                       mulib.MuLib.get_frames(),
                       lambdaNum)
   self.job_spec[lambdaNum] = {  
     'input_chunk' : chunks
   }
   return len(chunks)

  def generate_entries(self):
    for lambdaNum in range(0, int(self.lambdas)):
      if self.counter < self.lambdas:
        self.counter = self.counter + self.generate_entry(lambdaNum)
    self.write_job_spec_to_file()
    return self.filename
 
  def write_job_spec_to_file(self):
    try:
      logger.debug("[JOBSPEC] Creating Job Spec")
      logger.debug(self.job_spec)
      ofd = open(self.filename, "w")
      logger.debug(self.filename)
      json_map = json.dumps(self.job_spec,
                            indent='    ')
      print (json_map)
      ofd.write(json_map)
      ofd.close()
    except Exception as inst:
      logger.error(type(inst))
      logger.error(inst.args)
      logger.error (inst)

  @staticmethod
  def spawn_generator():
    logger.info("[JOBSPEC] Parsing the cmd line options")
    parser          = cmd_line_parser.CmdLineParser()
    (options, args) = parser.get_options_and_args()
    job_spec_gen    = JobSpecGenerator(options)
    logger.info("[JOBSPEC] Generating job_spec")
    return job_spec_gen.generate_entries()

if __name__ == "__main__":
  logger.debug("[JOBSPEC] Job Spec File : " + JobSpecGenerator.spawn_generator())
