#!/usr/bin/env python

import time
import boto3
import botocore
import uuid
import csv
import subprocess
import re
import math
import json
import os
from collections import defaultdict
from optparse import OptionParser



def cleanup():
    os.system("rm -rf /tmp/ffmpeg_merge/")

def setup():
    os.system("rm -rf /tmp/ffmpeg_merge/")
    os.system("mkdir -p /tmp/ffmpeg_merge/")

def get_default():
    return {
        'inputbucket' : 'lixiang-lambda-test',
        'outputbucket' : 'lixiang-lambda-test',
        'inputdir' : 'sintel-1k-png16',
        'outputdir' : 'output',
        'framerate' : 24,
        'start_number' : 1,
        'inputfile' : '%08d.png',
        'frames' : 300,
        'vcodec' : 'libx264',
        'pix_fmt' : 'yuv420p',
        'outputfile' : 'out.mp4'
    }

def lambda_handler(event, context):
    event = defaultdict(lambda:None, event)

    setup()
    start = time.time()

    s3_client = boto3.client('s3')

    inputbucket = event['inputbucket']
    outputbucket = event['outputbucket']
    inputdir = event['inputdir']
    outputdir = event['outputdir']
    inputfile = event['inputfile'] 
    outputfile = event['outputfile']

    vcodec = event['vcodec']
    pix_fmt = event['pix_fmt']

    frames = event['frames'] or 100000000
    framerate = event['framerate'] or 24
    start_number = event['start_number'] or 1

    download_path = '/tmp/ffmpeg_merge/'
    
    for i in xrange(frames):
        s3path = inputdir+'/'+inputfile % (start_number+i)
        localpath = download_path+inputfile%(start_number+i)
        print inputbucket, s3path, localpath
        try:
            s3_client.download_file(inputbucket, s3path, localpath)
        except botocore.exceptions.ClientError as e:
            print e
            print 'finish downloading', i, 'frames downloaded'
            break
    ffmpeg_cmd = "ffmpeg -framerate %d  -start_number %d -i %s -c:v %s -pix_fmt %s %s" % (framerate, start_number, download_path+inputfile, vcodec, pix_fmt, download_path+outputfile)
   
    os.system(ffmpeg_cmd)
    
    s3_client.upload_file(download_path+outputfile, outputbucket, outputdir+'/'+outputfile)
    end = time.time()
    print "Elapsed time : " + str(end - start)

    cleanup()
    
if __name__ == '__main__':
    lambda_handler(get_default(), "")
