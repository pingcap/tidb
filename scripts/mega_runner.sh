#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");

set -euo pipefail

# mega_runner.sh - Build mega binary, then run each test in its own process (like ut).
#
# Usage:
#   mega_runner.sh                       # Run all mega tests in parallel
#   mega_runner.sh -run PATTERN          # Run tests matching regex
#   mega_runner.sh -list                 # List all tests
#   mega_runner.sh -parallel N           # Max parallel processes (default: 8)
#   mega_runner.sh -timeout SECS         # Per-test timeout (default: 300)
#   mega_runner.sh -run PATTERN -retry 3 # Retry failed tests up to N times

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MEGA_BIN="$PROJECT_ROOT/bazel-bin/pkg/testkit/mega/mega_test_/mega_test"

# Defaults
RUN_PATTERN=""
LIST_ONLY=false
PARALLEL=8
TIMEOUT=300
RETRY=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        -run)       RUN_PATTERN="$2"; shift 2 ;;
        -list)      LIST_ONLY=true; shift ;;
        -parallel)  PARALLEL="$2"; shift 2 ;;
        -timeout)   TIMEOUT="$2"; shift 2 ;;
        -retry)     RETRY="$2"; shift 2 ;;
        *)          shift ;;
    esac
done

# --- Build ---
echo "=== Building mega test binary ==="
cd "$PROJECT_ROOT"
bazel build //pkg/testkit/mega:mega_test --define gotags=deadlock,inject_failpoints_50 2>&1

if [ ! -f "$MEGA_BIN" ]; then
    echo "ERROR: mega test binary not found at $MEGA_BIN"
    exit 1
fi
echo "=== Build complete ==="

# --- List mode ---
if [ "$LIST_ONLY" = true ]; then
    exec "$MEGA_BIN" -test.run "^TestMega$" -mega.list
fi

# --- Collect test names ---
echo "=== Collecting test names ==="
RAW=$("$MEGA_BIN" -test.run "^TestMega$" -mega.list 2>/dev/null || true)
# Parse lines like "  - pkg/name" or "pkg/name"
TESTS=()
while IFS= read -r line; do
    name="${line#  - }"  # strip leading "  - "
    name="${name#  }"    # strip leading spaces
    # Filter by pattern if given
    if [[ -n "$RUN_PATTERN" ]]; then
        if ! echo "$name" | grep -qE "$RUN_PATTERN"; then
            continue
        fi
    fi
    # Skip empty / header lines
    [[ -z "$name" ]] && continue
    [[ "$name" == "Total"* ]] && continue
    [[ "$name" == "Package"* ]] && continue
    [[ "$name" == "Registered"* ]] && continue
    TESTS+=("$name")
done <<< "$RAW"

TOTAL=${#TESTS[@]}
if [[ $TOTAL -eq 0 ]]; then
    echo "No tests matched."
    exit 0
fi
echo "=== Running $TOTAL tests ($PARALLEL parallel, ${TIMEOUT}s timeout each) ==="

# --- Run one test in an isolated process ---
run_one() {
    local test_name="$1"
    local attempt=1

    while [[ $attempt -le $RETRY ]]; do
        local output
        output=$(timeout "$TIMEOUT" "$MEGA_BIN" \
            -test.run "^TestMega$" \
            -mega.run "^${test_name}$" \
            -test.timeout "${TIMEOUT}s" \
            2>&1)
        local exit_code=$?

        if [[ $exit_code -eq 0 ]]; then
            echo "  [PASS] $test_name"
            return 0
        fi

        if [[ $attempt -lt $RETRY ]]; then
            echo "  [RETRY $attempt/$RETRY] $test_name (exit=$exit_code)"
            attempt=$((attempt + 1))
            continue
        fi

        echo "  [FAIL] $test_name (exit=$exit_code)"
        echo "$output" | tail -20 | sed 's/^/    /'
        return 1
    done
}

export -f run_one
export MEGA_BIN TIMEOUT RETRY

# --- Parallel execution ---
PASS=0
FAIL=0
FAIL_LIST=()

SEMAPHORE="/tmp/mega_sem_$$"
mkfifo "$SEMAPHORE"
exec 3<>"$SEMAPHORE"
rm -f "$SEMAPHORE"
# Initialize semaphore with $PARALLEL tokens
for ((i = 0; i < PARALLEL; i++)); do
    echo >&3
done

START_TIME=$SECONDS

for test_name in "${TESTS[@]}"; do
    # Acquire semaphore slot
    read -r -u 3

    (
        if run_one "$test_name"; then
            echo "PASS" > "/tmp/mega_result_${test_name//\//_}_$$"
        else
            echo "FAIL" > "/tmp/mega_result_${test_name//\//_}_$$"
        fi
        # Release semaphore
        echo >&3
    ) &
done <&3

wait
exec 3>&-

# Collect results
for test_name in "${TESTS[@]}"; do
    result_file="/tmp/mega_result_${test_name//\//_}_$$"
    if [[ -f "$result_file" ]] && [[ "$(cat "$result_file")" == "PASS" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        FAIL_LIST+=("$test_name")
    fi
    rm -f "$result_file"
done

ELAPSED=$(( SECONDS - START_TIME ))
echo ""
echo "=== Results: $PASS passed, $FAIL failed, $TOTAL total in ${ELAPSED}s ==="

if [[ ${#FAIL_LIST[@]} -gt 0 ]]; then
    echo "Failed tests:"
    for f in "${FAIL_LIST[@]}"; do
        echo "  - $f"
    done
    exit 1
fi

exit 0