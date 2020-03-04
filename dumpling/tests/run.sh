#!/bin/sh

# To avoid permission denied error, please run `chmod +x tests/_utils/*`.

set -e

DUMPLING_TEST_DIR=${DUMPLING_TEST_DIR:-"/tmp/dumpling_test_result"}
DUMPLING_TEST_USER=${DUMPLING_TEST_USER:-"root"}
DUMPLING_TEST_HOST=${DUMPLING_TEST_HOST:-"127.0.0.1"}
DUMPLING_TEST_PORT=${DUMPLING_TEST_PORT:-"3306"}
DUMPLING_TEST_PASSWORD=${DUMPLING_TEST_PASSWORD:-""}

export DUMPLING_TEST_DIR
export DUMPLING_TEST_USER
export DUMPLING_TEST_HOST
export DUMPLING_TEST_PORT
export DUMPLING_TEST_PASSWORD

set -eu

mkdir -p "$DUMPLING_TEST_DIR"
PATH="tests/_utils:$PATH"

test_connection() {
  i=0
  while ! run_sql 'select 0 limit 0' > /dev/null; do
      i=$((i+1))
      if [ "$i" -gt 10 ]; then
          echo 'Failed to ping MySQL Server'
          exit 1
      fi
      sleep 3
  done
}

test_connection

for script in tests/*/run.sh; do
    echo "Running test $script..."
    DUMPLING_BASE_NAME="$(dirname "$script")"
    export DUMPLING_BASE_NAME
    TEST_NAME="$(basename "$(dirname "$script")")"
    DUMPLING_OUTPUT_DIR="$DUMPLING_TEST_DIR"/sql_res."$TEST_NAME"
    export DUMPLING_OUTPUT_DIR

    echo "Cleaning up test output dir: $DUMPLING_OUTPUT_DIR"
    rm "$DUMPLING_OUTPUT_DIR" -rf

    PATH="tests/_utils:$PATH" \
    sh "$script"
done

echo "Passed integration tests."
