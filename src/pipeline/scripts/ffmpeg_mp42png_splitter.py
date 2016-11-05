#!/usr/bin/env python

import time
import boto3
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
    os.system("rm -rf /tmp/ffmpeg_split/")

def setup():
    os.system("rm -rf /tmp/ffmpeg_split/")
    os.system("mkdir -p /tmp/ffmpeg_split/")

def get_default():
    return {                   
        'inputbucket' : 'lixiang-lambda-test',
        'outputbucket' : 'lixiang-lambda-test',
        'outputdir' : 'sintel-1k-png16',
        'inputdir' : 'input',
        'framerate' : 24,
        'start_number' : 1,
        'outputfile' : '%08d.png',
        'frames' : 30,
        'vcodec' : 'libx264',
        'pix_fmt' : 'yuv420p',
        'inputfile' : 'in.mp4'
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

    framerate = event['framerate'] or 24
    start_number = event['start_number'] or 1
    frames = event['frames'] or None
    vcodec = event['vcodec'] or None
    pix_fmt = event['pix_fmt'] or None

    download_path = '/tmp/ffmpeg_split/'

    s3path = inputdir+'/'+inputfile
    localpath = download_path+inputfile
    s3_client.download_file(inputbucket, s3path, localpath)

    ffmpeg_cmd = "ffmpeg -i %s -r %d %s" % (localpath, framerate, download_path+outputfile)

    os.system(ffmpeg_cmd)

    for i in xrange(100000000):
        localpath = download_path+outputfile % (i+1)
        if not os.path.isfile(localpath):
            break
        s3path = outputdir+'/'+outputfile % (i+1)
        print localpath, outputbucket, s3path
        s3_client.upload_file(localpath, outputbucket, s3path)

    end = time.time()
    print "Elapsed time : " + str(end - start)

    cleanup()

if __name__ == '__main__':
    lambda_handler(get_default(), "")
