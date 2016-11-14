# -*- coding: utf-8 -*-

# Full Imports
import simplejson as json
import collections
import itertools
import six
import sys
import logging

# Specific Imports
from enum import Enum

# Global Variables declaration
logger = logging.getLogger("JOBSPEC")
logging.basicConfig()
logger = logging.getLogger('JOBSPEC')
logger.setLevel(logging.DEBUG)

# JobType class
class JobType(Enum):
  ONE_TO_ONE  = 1
  ONE_TO_MANY = 2
  MANY_TO_ONE = 3

# JobSpec class
class JobSpec(object):
  def __init__(self, filename, **kwargs):
    self.filename    = filename
    self.params_hash = kwargs

  def get_params_hash(self, key):
    if key in self.params_hash:
      return self.params_hash[key]
    else:
      logger.debug("[JobSpec] Key not found")

  def writeJsonToFile(self):
    """ Function to write JSON object to 
        a file represented by ofd
  
      Args:
        ofd : Output File Descriptor
        json_obj : JSON to be dumped

      Returns:
        No return value

      Raises:
        Exception: If file write fails
    """
    try:
      ofd = open(self.filename, 'w')
      ofd.write(json.dumps(self.params_hash,
                           sort_keys=True,
                           indent='    '))
      logger.debug("[JobSpec] Write complete")
      ofd.close()
    except Exception as inst:
      logger.error(type(inst)) 
      logger.error(inst.args)
      logger.error (inst)
