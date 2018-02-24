#!/bin/sh

./autogen.sh
./configure
make

cd sprocket/platform/aws_lambda/
./autogen.sh
./configure
make
cd -
