import jobspec
import simplejson as json


if __name__=='__main__':
    pipe = '''{
            "pipeid" : "sjf2JY1f",
            "channels" :
                [
                    {"channel":0, "URI":"s3://lixiang-lambda-test/input/%08d.png", "nchunks":null, "type":"png", "src":"src", "dest":null},
                    {"channel":1, "URI":"s3://lixiang-lambda-test/output/%08d.png", "nchunks":null, "type":"mp4", "src":null, "dest":"dest"}
                ],
            "nodes" :
                [
                    {"node":0, "upstream":[0], "downstream":[1], "nworkers":10, "nchunks":null, "amplification":1, "engine": "aws-lambda", "operator": "crawlingtext", "command":[]}
                ]
        }'''
    gen = jobspec.Generator2()
    js = gen.generate(gen.parse(pipe))
    job = json.dumps(js[0])
    jobspec.submit(job)
