# -*- coding: utf-8 -*-

import boto
import os
import logging
from boto.s3.connection import S3Connection

AWS_KEY    = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def find_num_objects(bucketname):
  try:
    aws_connection = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = aws_connection.get_bucket(bucketname)
    logger.debug("[S3LIB] Getting number of objects" + bucketname)
    return len(bucket.list())
  except Exception as inst:
    logger.error("[S3LIB] " + str(type(inst)))
    logger.error("[S3LIB] " + str(inst.args))
    logger.error("[S3LIB] " + str(inst))
