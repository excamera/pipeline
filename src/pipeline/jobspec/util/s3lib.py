# -*- coding: utf-8 -*-

import boto
import os
import logging
import boto3
from boto.s3.connection import S3Connection

AWS_KEY    = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
s3_client  = boto3.client('s3')
s3         = boto3.resource('s3')

logger    = logging.getLogger(__name__)
nh        = logging.NullHandler()
formatter = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
nh.setFormatter(formatter)
logger.addHandler(nh)
logger.setLevel(logging.DEBUG)

def get_num_objects(bucketname,
                    prefix):
  try:
    AWS_KEY        = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET     = os.environ['AWS_SECRET_ACCESS_KEY']
    aws_connection = S3Connection(AWS_KEY, AWS_SECRET)
    bucket         = aws_connection.get_bucket(bucketname)
    logger.debug("[S3LIB] Getting number of objects in bucket : " + bucketname)

    bucket = s3.Bucket(bucketname)
    objects = list(bucket.objects.filter(Prefix=prefix))
    #for i in objects:
    #  print i.key
    num = len(objects)

    logger.debug("Number of Objects in %s/%s is : %d" %(bucketname, prefix, num))
    return (num, objects)
    #return (len(bucket.list(), bucket,  bucket.get_all_keys())
  except Exception as inst:
    logger.error("[S3LIB] " + str(type(inst)))
    logger.error("[S3LIB] " + str(inst.args))
    logger.error("[S3LIB] " + str(inst))

def get_input_chunks(bucketname,
                     prefix,
                     frames,
                     lambdaNum):
  try:
    (num_objects, keys) = get_num_objects(bucketname, prefix)
    start               = lambdaNum * frames
    end                 = start + frames
    chunks              = []
    logger.debug("[S3LIB] lambdaNum=%d, start=%d, end=%d" %(lambdaNum, start, end))
    for i in range(start, end):
      if i < num_objects:
        chunks.append(keys[i].key)
    return chunks
  except Exception as inst:
    logger.error("[S3LIB] " + str(type(inst)))
    logger.error("[S3LIB] " + str(inst.args))
    logger.error("[S3LIB] " + str(inst))

def exists(bucketname, 
           prefix):
  try:
    AWS_KEY        = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET     = os.environ['AWS_SECRET_ACCESS_KEY']
    aws_connection = S3Connection(AWS_KEY, AWS_SECRET)
    bucket         = aws_connection.lookup(bucketname)
    if bucket is None:
      return False
    bucket = s3.Bucket(bucketname)
    objects = list(bucket.objects.filter(Prefix=prefix))
    if len(objects) > 0:
      return True
    else:
      return False
  except Exception as inst:
    logger.error("[S3LIB] " + str(type(inst)))
    logger.error("[S3LIB] " + str(inst.args))
    logger.error("[S3LIB] " + str(inst))
