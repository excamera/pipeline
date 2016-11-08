# -*- coding: utf-8 -*-

import json
import simplejson

import collections
import itertools
import six
import sys
import logging

# Global Variables declaration
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Functions 
def writeJsonToFile(ofd, json_obj):
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
    ofd.write(json.dumps(json_obj))
    logger.debug("Write complete")
  except Exception as inst:
    logger.error(type(inst)) 
    logger.error(inst.args)
    logger.error (inst)

