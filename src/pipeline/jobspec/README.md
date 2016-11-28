$ cd src/pipeline/jobspec <br />
$ python workflow.py <br />
<br />
Initializing pylaunch... done.<br />
INFO:generator:[JOBSPEC] Parsing the cmd line options<br />
DEBUG:generator:[JOBSPEC] JobSpecGenerator<br />
DEBUG:util.mulib:bucket=sintel-1k-png16, prefix=sintel-1k-png16/ lambdas=10<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.mulib:[MULIB] No of objects : 1498<br />
INFO:generator:[JOBSPEC] Generating job_spec<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=0, start=0, end=149<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=1, start=149, end=298<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=2, start=298, end=447<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=3, start=447, end=596<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=4, start=596, end=745<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=5, start=745, end=894<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=6, start=894, end=1043<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=7, start=1043, end=1192<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=8, start=1192, end=1341<br />
DEBUG:util.s3lib:[S3LIB] Getting number of objects in bucket : sintel-1k-png16<br />
DEBUG:util.s3lib:Number of Objects in sintel-1k-png16/sintel-1k-png16/ is : 1498<br />
DEBUG:util.s3lib:[S3LIB] lambdaNum=9, start=1341, end=1490<br />
DEBUG:generator:[JOBSPEC] Creating Job Spec<br />
DEBUG:__main__:[JOBSPEC] Job Spec Generated at : job1.json<br />
DEBUG:__main__:[JOBSPEC] Job Spec parsed successfully<br />
DEBUG:__main__:[JOBSPEC] Invoking mu to run the job...<br />
DEBUG:util.mulib:[MULIB] Invoking mu...<br />
DEBUG:job_spec_server:Running the JobSpec<br />
