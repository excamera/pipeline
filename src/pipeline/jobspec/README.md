$ cd src/pipeline/jobspec
$ python workflow.py

Initializing pylaunch... done.
INFO:generator:[JOBSPEC] Parsing the cmd line options
DEBUG:generator:[JOBSPEC] JobSpecGenerator
DEBUG:util.mulib:bucket=sintel-1k-png16, prefix=sintel-1k-png16/ lambdas=10
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.mulib:[MULIB] No of objects : 1498
INFO:generator:[JOBSPEC] Generating job_spec
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=0, start=0, end=149
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=1, start=149, end=298
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=2, start=298, end=447
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=3, start=447, end=596
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=4, start=596, end=745
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=5, start=745, end=894
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=6, start=894, end=1043
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=7, start=1043, end=1192
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=8, start=1192, end=1341
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498
DEBUG:util.s3lib:[S3LIB] lambdaNum=9, start=1341, end=1490
DEBUG:generator:[JOBSPEC] Creating Job Spec
DEBUG:__main__:[JOBSPEC] Job Spec Generated at : job1.json
DEBUG:__main__:[JOBSPEC] Job Spec parsed successfully
DEBUG:__main__:[JOBSPEC] Invoking mu to run the job...
DEBUG:util.mulib:[MULIB] Invoking mu...
DEBUG:job_spec_server:Running the JobSpec
