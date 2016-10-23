# Video Processing Pipeline (WIP)
This is a Top-Level Project implementing pipelines. This project internally
uses https://github.com/excamera/mu for interacting with mu.

## How to build the code
```
git clone --recursive git@github.com:excamera/pipeline.git
cd pipeline
./autogen.sh
./configure
make -j$(nproce)
make test
```
