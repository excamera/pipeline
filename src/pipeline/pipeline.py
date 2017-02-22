import sys
from time import gmtime, strftime
sys.path.append('../../external/mu/src/lambdaize/')

from taskspec.generator import Generator
from taskspec.job_manager import JobManager
import simplejson as json


if __name__=='__main__':
    pipe = '''{
            "pipeid" : "2S1H8nx5",
            "channels" :
                [
                    {"channel":0, "URI":"s3://lixiang-lambda-test/60s.mp4", "nchunks":null, "type":"mp4", "src":"src", "dest":null},
                    {"channel":1, "URI":"s3://lixiang-lambda-test/temp/2S1H8nx5/1/%08d.png", "nchunks":null, "type":"png", "src":null, "dest":null},
                    {"channel":2, "URI":"s3://lixiang-lambda-test/temp/2S1H8nx5/2/%08d.png", "nchunks":null, "type":"png", "src":null, "dest":null},
                    {"channel":3, "URI":"s3://lixiang-lambda-test/output/2S1H8nx5/%08d.mp4", "nchunks":null, "type":"mp4", "src":null, "dest":"dest"}
                ],
            "nodes" :
                [
                    {"node":0, "upstream":[0], "downstream":[1], "nworkers":10, "nchunks":null, "amplification":1, "engine": "aws-lambda", "operator": "decode", "command":[]},
                    {"node":1, "upstream":[1], "downstream":[2], "nworkers":10, "nchunks":null, "amplification":1, "engine": "aws-lambda", "operator": "grayscale", "command":[]},
                    {"node":2, "upstream":[2], "downstream":[3], "nworkers":10, "nchunks":null, "amplification":1, "engine": "aws-lambda", "operator": "encode", "command":[]}
                ]
                }'''
#     pipe = '''{
#             "pipeid" : "2S1H8nx5",
#             "channels" :
#                 [
#                     {"channel":0, "URI":"s3://lixiang-lambda-test/temp/2S1H8nx5/2/%08d.png", "nchunks":null, "type":"png", "src":"src", "dest":null},
#                     {"channel":1, "URI":"s3://lixiang-lambda-test/output/2S1H8nx5/%08d.mp4", "nchunks":null, "type":"mp4", "src":null, "dest":"dest"}
#                 ],
#             "nodes" :
#                 [
#                     {"node":0, "upstream":[0], "downstream":[1], "nworkers":10, "nchunks":null, "amplification":1, "engine": "aws-lambda", "operator": "encode", "command":[]}
#                 ]
#         }'''


    parsed = Generator.parse(pipe)
    job_spec = Generator.generate(parsed)
   # print json.dumps(job_spec, indent=True)
    JobManager.submit(job_spec)
