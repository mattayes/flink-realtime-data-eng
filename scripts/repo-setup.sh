#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DATA_DIR="$SCRIPT_DIR/../data"
if [ ! -d "$DATA_DIR" ]
then
  mkdir -m 777 "$DATA_DIR"
elif [[ $(ls -ld "$DATA_DIR" | cut -d ' ' -f 1) != 'drwxrwxrwx' ]]
then
  rm -rf "$DATA_DIR" && mkdir -m 777
fi

if ! command -v flink &> /dev/null
then
  echo "Flink is missing. Ensure it is installed and on PATH" >&2
  exit 1
fi

FLINK_VERSION=$(flink --version)
if ! grep -q 'Version: 1.19' <<< "$FLINK_VERSION"
then
  echo "Flink is not running 1.19 ($FLINK_VERSION), please install the latest version of 1.19." >&2
  exit 1
fi

if ! command -v docker &> /dev/null
then
  echo "Docker is missing. Ensure it is installed and on PATH" >&2
  exit 1
fi