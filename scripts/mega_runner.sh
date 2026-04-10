#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

set -euo pipefail

# mega_runner.sh - Build and run mega tests
#
# Usage:
#   mega_runner.sh                  # Run all mega tests
#   mega_runner.sh -run PATTERN     # Run tests matching regex pattern
#   mega_runner.sh -list            # List all tests
#   mega_runner.sh -run PATTERN -shard N -total M  # Run shard N of M

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MEGA_BIN="$PROJECT_ROOT/bazel-bin/pkg/testkit/mega/mega_test_/mega_test"

# Parse args
RUN_PATTERN=""
LIST_ONLY=false
SHARD_INDEX=0
SHARD_TOTAL=1
TIMEOUT="3600"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -run)
            RUN_PATTERN="$2"
            shift 2
            ;;
        -list)
            LIST_ONLY=true
            shift
            ;;
        -shard)
            SHARD_INDEX="$2"
            shift 2
            ;;
        -total)
            SHARD_TOTAL="$2"
            shift 2
            ;;
        -timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# Build mega test binary
echo "=== Building mega test binary ==="
cd "$PROJECT_ROOT"
bazel build //pkg/testkit/mega:mega_test --define gotags=deadlock,inject_failpoints_50

if [ ! -f "$MEGA_BIN" ]; then
    echo "ERROR: mega test binary not found at $MEGA_BIN"
    exit 1
fi
echo "=== Build complete ==="

# List mode
if [ "$LIST_ONLY" = true ]; then
    exec "$MEGA_BIN" -test.run "^TestMega$" -mega.list
fi

# Build test args
TEST_ARGS=(
    -test.run "^TestMega$"
    -test.timeout "${TIMEOUT}s"
)

if [ -n "$RUN_PATTERN" ]; then
    TEST_ARGS+=(-mega.run "$RUN_PATTERN")
fi

# Shard mode: compute which tests this shard should run
if [ "$SHARD_TOTAL" -gt 1 ]; then
    # Get the list of all tests and pick this shard's subset
    ALL_TESTS=$("$MEGA_BIN" -test.run "^TestMega$" -mega.list 2>/dev/null | grep "^  - " | sed 's/  - //' || true)
    TOTAL=$(echo "$ALL_TESTS" | wc -l | tr -d ' ')
    PER_SHARD=$(( (TOTAL + SHARD_TOTAL - 1) / SHARD_TOTAL ))
    START=$(( SHARD_INDEX * PER_SHARD + 1 ))
    
    MY_TESTS=$(echo "$ALL_TESTS" | sed -n "${START},$((START + PER_SHARD - 1))p")
    
    if [ -z "$MY_TESTS" ]; then
        echo "Shard $SHARD_INDEX/$SHARD_TOTAL: no tests assigned, exiting"
        exit 0
    fi
    
    COUNT=$(echo "$MY_TESTS" | wc -l | tr -d ' ')
    echo "=== Shard $SHARD_INDEX/$SHARD_TOTAL: running $COUNT tests ==="
    
    # Build a regex from the test names
    PATTERN=$(echo "$MY_TESTS" | tr '\n' '|' | sed 's/|$//')
    TEST_ARGS+=(-mega.run "$PATTERN")
fi

echo "=== Running mega tests ==="
echo "Args: ${TEST_ARGS[*]}"
echo ""

exec "$MEGA_BIN" "${TEST_ARGS[@]}" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}