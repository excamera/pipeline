# -*- coding: utf-8 -*-

# Full Imports
import simplejson as json
import collections
import itertools
import six
import sys
import logging

# Specific imports
from util import *
from parser import *
from validator import *
from job_spec import *
from generator import *

# Logger
logger    = logging.getLogger(__name__)
nh        = logging.NullHandler()
formatter = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
nh.setFormatter(formatter)
logger.addHandler(nh)
logger.setLevel(logging.DEBUG)

class JobSpecWorkflow(object):
  def __init__(self):
    logger.debug("[JOBSPEC] Initializing the workflow...")

  @staticmethod
  def spawn_workflow():
    job_spec_file = JobSpecGenerator.spawn_generator()
    logger.debug("[JOBSPEC] Job Spec Generated at : " + job_spec_file)
    job_spec_parser = JobSpecParser.spawn_parser()
    logger.debug("[JOBSPEC] Job Spec parsed successfully")
    job_spec        = job_spec_parser.job_spec
    validator       = JobSpecValidator(job_spec)
    if validator.do_validate():
      logger.debug("[JOBSPEC] Invoking mu to run the job...")
      mulib.MuLib.invoke_mu_coordinator(job_spec)

if __name__ == "__main__":
  JobSpecWorkflow.spawn_workflow()
