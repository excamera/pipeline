#!/bin/bash

set -eou pipefail

DEFAULT_MEM_SIZE=2048
DEFAULT_TIMEOUT=120
DEFAULT_REGION=us-central1

if [ ${#@} -lt 1 ]; then
  echo "Usage: $0 <appdir>"
  echo "Environment variables:"
  echo "    FUN_NAME (optional, default ${USER}_<appdir>, gcp function name"
  echo "    MEM_SIZE (optional, default ${DEFAULT_MEM_SIZE}, range 128-2048) size of funcion's memory"
  echo "    TIMEOUT (optional, default ${DEFAULT_TIMEOUT}) execution timeout in seconds"
  echo "    REGION (optional, default ${DEFAULT_REGION}) install function in given region"
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
GCP_DIR="$(dirname $(readlink -f "$0"))"/..
SPROCKET_ROOT_S_PARENT="$GCP_DIR"/../../..
SPROCKET_FILES=(sprocket/__init__.py sprocket/controlling/__init__.py sprocket/controlling/worker/__init__.py sprocket/controlling/worker/fd_wrapper.py sprocket/controlling/worker/worker.py sprocket/controlling/common/__init__.py sprocket/controlling/common/network.py sprocket/controlling/common/handler.py sprocket/controlling/common/defs.py sprocket/controlling/common/socket_nb.py)
INITFILES=("$GCP_DIR"/package.json "$GCP_DIR"/index.js "$GCP_DIR"/entry.py)
COMMON_DEPS="$GCP_DIR"/common_deps
APP=$(readlink -f "$1")

cd "$SPROCKET_ROOT_S_PARENT"
for F in "${SPROCKET_FILES[@]}"; do
  cp --parent "$F" "$TMPDIR"
done
cd "$INITDIR"

for F in "${INITFILES[@]}"; do
  cp "$F" "$TMPDIR" 
done

cp -r "$COMMON_DEPS"/. "$TMPDIR"
cp -r "$APP"/. "$TMPDIR"

if gcloud functions describe "$FUN_NAME" --region "$REGION" &>/dev/null; then
  echo "Function exists, overwriting..."
fi

gcloud functions deploy \
  "$FUN_NAME" \
  --entry-point entry \
  --memory "$MEM_SIZE" \
  --region "$REGION" \
  --source "$TMPDIR" \
  --timeout "$TIMEOUT" \
  --trigger-http

echo Google Cloud Function: $FUN_NAME deployed.

#rm -rf "$TMPDIR"
