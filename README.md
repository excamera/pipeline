[![Build Status](https://travis-ci.org/excamera/pipeline.svg?branch=master)](https://travis-ci.org/excamera/pipeline)

# Sprocket Video Processing Pipeline
This is a Top-Level Project implementing pipelines. This project internally
uses a modified version of https://github.com/excamera/mu for interacting with Lambda.

## How to build the code
```
git clone --recursive https://github.com/excamera/pipeline.git
cd pipeline
./autogen.sh
./configure
make -j$(nproc)

cd sprocket/platform/aws_lambda
./autogen.sh
./configure
make -j$(nproc)
```
