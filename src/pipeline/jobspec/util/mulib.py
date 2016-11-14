import sys
import s3lib

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
  def get_work_distribution(lambdas, prefix):
    num_obj    = s3lib.find_num_objects(prefix)
    if num_obj is None:
      return CoordinatorArgs.defaultFrames
    else:
      return num_obj / min(num_obj, lambdas)

  @staticmethod
  def get_coordinator_args(lambdas, prefix):
    CoordinatorArgs.args['-n'] = lambdas
    CoordinatorArgs.args['-f'] =  CoordinatorArgs.get_work_distribution(lambdas, prefix)
    return { 'args' : CoordinatorArgs.args }

class MuLib(object):
  @staticmethod
  def get_cmd_string(operator):
    return Operator.get_cmd_string(operator)
   
  @staticmethod
  def get_coordinator_args(lambdas, prefix):
    return CoordinatorArgs.get_coordinator_args(lambdas, prefix)
