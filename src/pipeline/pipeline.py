import sys
from time import gmtime, strftime
sys.path.append('../../external/mu/src/lambdaize/')

from taskspec.generator import Generator
from taskspec.job_manager import JobManager
import simplejson as json


if __name__=='__main__':
    pipeline = Generator.generate('s3://lixiang-lambda-test/5s.mp4', commands=[('grayscale', [])])
    JobManager.submit(pipeline)
