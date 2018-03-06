#!/bin/bash

set -eou pipefail

DEFAULT_MEM_SIZE=3008
DEFAULT_TIMEOUT=120
DEFAULT_REGION=us-east-1

if [ ${#@} -lt 1 ]; then
  echo "Usage: $0 <appdir>"
  echo "Environment variables:"
  echo "    AWS_ROLE (required, no default) role to use when executing function"
  echo "    FUN_NAME (optional, default ${USER}_<appdir>, lambda function name"
  echo "    MEM_SIZE (optional, default ${DEFAULT_MEM_SIZE}, range 128-3008) size of lambda's memory"
  echo "    TIMEOUT (optional, default ${DEFAULT_TIMEOUT}) execution timeout in seconds"
  echo "    REGION (optional, default ${DEFAULT_REGION}) install lambda in given region"
  exit 1
fi

if [ -z $AWS_ROLE ]; then
  echo "Please specify an AWS role in the AWS_ROLE envvar. Giving up."
  exit 1
fi

if [ -z ${FUN_NAME:-} ]; then
  FUN_NAME="$USER"_$(basename $1)
fi
if [ -z ${MEM_SIZE:-} ]; then
  MEM_SIZE="$DEFAULT_MEM_SIZE"
fi
if [ -z ${TIMEOUT:-} ]; then
  TIMEOUT="$DEFAULT_TIMEOUT"
fi
if [ -z ${REGION:-} ]; then
  REGION="$DEFAULT_REGION"
fi

INITDIR=$(pwd -P)
TMPDIR=$(mktemp -d)
ZIPFILE="$TMPDIR"/lambda_code.zip
AWS_DIR="$(dirname $(readlink -f "$0"))"/..
SPROCKET_ROOT_S_PARENT="$AWS_DIR"/../../..
SPROCKET_FILES="sprocket/__init__.py sprocket/controlling/__init__.py sprocket/controlling/worker/__init__.py sprocket/controlling/worker/fd_wrapper.py sprocket/controlling/worker/worker.py sprocket/controlling/common/__init__.py sprocket/controlling/common/network.py sprocket/controlling/common/handler.py sprocket/controlling/common/defs.py sprocket/controlling/common/socket_nb.py"
LAMBDA_FUNCTION="$AWS_DIR"/lambda_function.py
COMMON_DEPS="$AWS_DIR"/common_deps
APP=$(readlink -f "$1")

cd "$SPROCKET_ROOT_S_PARENT"
for F in $SPROCKET_FILES; do
  cp --parent "$F" "$TMPDIR"
done
cd "$INITDIR"
cp "$LAMBDA_FUNCTION" "$TMPDIR"
cp -r "$COMMON_DEPS/"* "$TMPDIR"
cp -r "$APP/"* "$TMPDIR"

cd "$TMPDIR"
zip -q -r "$ZIPFILE" .
cd "$INITDIR"


if aws lambda get-function --region "$REGION" --function "$FUN_NAME" &>/dev/null; then
  echo "Function of the same name exists, overwriting..."
  aws lambda delete-function --region "$REGION" --function "$FUN_NAME" &>/dev/null
fi

aws lambda create-function \
  --runtime python2.7 \
  --role "$AWS_ROLE" \
  --handler lambda_function.lambda_handler \
  --function-name "$FUN_NAME" \
  --timeout "$TIMEOUT" \
  --memory-size "$MEM_SIZE" \
  --publish \
  --region "$REGION" \
  --zip-file fileb://"$ZIPFILE"

rm -rf "$TMPDIR"
