import sys
import s3lib
import logging

logger    = logging.getLogger(__name__)
nh        = logging.NullHandler()
formatter = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
nh.setFormatter(formatter)
logger.addHandler(nh)
logger.setLevel(logging.DEBUG)

class Operator(object):
  operators = {
    'grayscale' : { 
      "./ffmpeg" : " -i ##INFILE## -vf hue=s=0 -c:a copy -safe 0 ##OUTFILE##"
    },
    'png2y4m'   : { 
      "./png2y4m" : " -i -d -o ##OUTFILE## ##INFILE##"
    },
    'y4m2png'   : { 
      "./y4m2png" : " -o ##OUTFILE## ##INFILE##"
    }
  }

  @staticmethod
  def get_cmd_string(operator):
    return { operator : Operator.operators[operator] }

class CoordinatorArgs(object):
  ssl_dir = "/tmp/ssl"
  defaultFrames = 6
  args = {
      '-n' : 100
    , '-l' : 'ffmpeg'
    , '-b' : 'bucket'
    , '-f' : 6
    , '-c' : ssl_dir + "/ca_cert.pem.pem"
    , '-s' : ssl_dir + "/server_cert.pem"
    , '-k' : ssl_dir + "/server_key.pem"
  }

  @staticmethod
  def get_work_distribution(bucket, prefix, lambdas):
    logger.debug("bucket=%s, prefix=%s lambdas=%s" %(bucket, prefix, lambdas))
    (num_obj, keys) = s3lib.get_num_objects(bucket, prefix)
    logger.debug("[MULIB] No of objects : " + str(num_obj))
    if num_obj is None:
      return CoordinatorArgs.defaultFrames
    else:
      return int(num_obj) / min(int(num_obj), int(lambdas))

  @staticmethod
  def get_coordinator_args(bucket, prefix, lambdas):
    CoordinatorArgs.args['-n'] = lambdas
    CoordinatorArgs.args['-f'] =  CoordinatorArgs.get_work_distribution(bucket, prefix, lambdas)
    return { 'args' : CoordinatorArgs.args }

  @staticmethod
  def get_frames():
    return CoordinatorArgs.args['-f']

class MuLib(object):
  @staticmethod
  def get_cmd_string(operator):
    return Operator.get_cmd_string(operator)
   
  @staticmethod
  def get_coordinator_args(bucket, prefix, lambdas):
    return CoordinatorArgs.get_coordinator_args(bucket, prefix, lambdas)

  @staticmethod
  def get_frames():
    return CoordinatorArgs.get_frames()
