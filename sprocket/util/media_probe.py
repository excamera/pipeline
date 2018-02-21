#!/usr/bin/python

import os
import subprocess

import boto3

from sprocket.util.s3signurl import sign

def get_signed_URI(URI):
    if URI.startswith('s3://'):
        return sign(URI.split('/')[2], '/'.join(URI.split('/')[3:]), os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], https=True, expiry=86400)
    else:
        return URI


def get_all_info(URI):
    results = subprocess.Popen(["ffprobe", URI], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    return results.stdout.readlines()


def get_duration_from_output_lines(output):
    iso_time = [v for v in output if "Duration" in v][0].split(',')[0].strip().split(' ')[1]
    return sum(x * int(t) for x, t in zip([3600, 60, 1], iso_time.split('.')[0].split(":"))) + float(
        '.' + iso_time.split('.')[1])


def get_duration(URI):
    return get_duration_from_output_lines(get_all_info(URI))


def get_fps(URI):
    output = get_all_info(URI)
    line = [v for v in output if "fps" in v][0]
    fps = [v for v in line.split(',') if "fps" in v][0].split()[0]
    return float(fps)


def get_nframes(URI, suffix=''): # a wildcard suffix
    if URI.startswith('s3://'):
        client = boto3.client('s3')
        count = 0
        marker = ''
        while True:
            objs = client.list_objects_v2(Bucket=URI.split('/')[2], Prefix='/'.join(URI.split('/')[3:]), StartAfter=marker)
            count += len(filter(lambda x: x['Key'].endswith(suffix) and not x['Key'].endswith('/'), objs['Contents']))
            marker = objs['Contents'][len(objs['Contents'])-1]['Key']
            if not objs['IsTruncated']:
                break
        return count
    return None

