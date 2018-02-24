[![Build Status](https://travis-ci.org/excamera/pipeline.svg?branch=master)](https://travis-ci.org/excamera/pipeline)

# Video Processing Pipeline (WIP)
This is a Top-Level Project implementing pipelines. This project internally
uses https://github.com/excamera/mu for interacting with mu.

## How to build the code
```
git clone --recursive https://github.com/excamera/pipeline.git
cd pipeline
./autogen.sh
./configure
make -j$(nproce)

cd sprocket/platform/aws_lambda
./autogen.sh
./configure
make -j$(nproce)
```
