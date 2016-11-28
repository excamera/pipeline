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
from parser import JobSpecParser

logger    = logging.getLogger(__name__)
nh        = logging.NullHandler()
formatter = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
nh.setFormatter(formatter)
logger.addHandler(nh)
logger.setLevel(logging.DEBUG)

class JobSpecValidator(object):
  def __init__(self, job_spec):
    self.job_spec = job_spec

  def do_validate(self):
    logger.debug(self.job_spec)
    return s3lib.exists(self.job_spec['input_bucket'],
                        self.job_spec['input_prefix'])

  @staticmethod
  def spawn_validator():
    job_spec_parser = JobSpecParser.spawn_parser()
    job_spec   = job_spec_parser.job_spec
    validator  = JobSpecValidator(job_spec)
    return validator.do_validate()

if __name__ == "__main__":
  if JobSpecValidator.spawn_validator():
    logger.debug("Job Spec validation successful")
  else:
    logger.debug("Job Spec validation failed")
