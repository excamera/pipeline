#!/bin/sh

./autogen.sh
./configure
make
make test
