#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR="$SCRIPT_DIR/../data"

mkdir_if_not_exist() {
  local DIR="$1"
  if [ ! -d "$DIR" ]
  then
    mkdir -m 777 "$DIR"
  fi
}

for DIR in \
  "$DATA_DIR" \
  "$DATA_DIR/raw_audit_trail" \
  "$DATA_DIR/five_sec_summary" \
  "$DATA_DIR/five_sec_summary" \
  "$DATA_DIR/raw_browser_events"
do
  mkdir_if_not_exist "$DIR"
done

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


if [ ! -f "kui_config.yml" ]
then
  (umask 001; touch "kui_config.yml")
fi