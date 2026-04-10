#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");

set -euo pipefail

# mega_runner.sh - Build mega binary, run each test in its own process.
#
# Usage:
#   mega_runner.sh                       # Run all mega tests in parallel
#   mega_runner.sh -run PATTERN          # Run tests matching regex
#   mega_runner.sh -list                 # List all tests
#   mega_runner.sh -parallel N           # Max parallel processes (default: 8)
#   mega_runner.sh -timeout SECS         # Per-test timeout (default: 180)
#   mega_runner.sh -retry 3              # Retry failed tests up to N times
#
# Output (stdout): one line per test — [PASS]/[FAIL]/[TIMEOUT] name  time
# Output (stderr): build info + failure details (for debugging)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MEGA_BIN="$PROJECT_ROOT/bazel-bin/pkg/testkit/mega/mega_test_/mega_test"

# Defaults
RUN_PATTERN=""
LIST_ONLY=false
PARALLEL=8
TIMEOUT=180
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

fmt_time() {
    local secs=$1
    if [[ $secs -lt 10 ]]; then
        printf "0.%ds" "$secs"
    else
        local m=$((secs / 60))
        local s=$((secs % 60))
        if [[ $m -gt 0 ]]; then
            printf "%dm%ds" "$m" "$s"
        else
            printf "%ds" "$s"
        fi
    fi
}

GOTAGS="${UNIT_TEST_TAGS:-deadlock,intest}"

# --- Build ---
echo "=== Building mega test binary ===" >&2
cd "$PROJECT_ROOT"
bazel build //pkg/testkit/mega:mega_test --define gotags=$GOTAGS 2>&1
if [ ! -f "$MEGA_BIN" ]; then
    echo "ERROR: mega test binary not found at $MEGA_BIN" >&2
    exit 1
fi
echo "=== Build complete ===" >&2

# --- List mode ---
if [ "$LIST_ONLY" = true ]; then
    exec "$MEGA_BIN" -test.run "^TestMega$" -mega.list
fi

# --- Collect test names ---
RAW=$("$MEGA_BIN" -test.run "^TestMega$" -mega.list 2>/dev/null || true)
TESTS=()
while IFS= read -r line; do
    name="${line#  - }"
    name="${name#  }"
    [[ -z "$name" ]] && continue
    [[ "$name" == "Total"* ]] && continue
    [[ "$name" == "Package"* ]] && continue
    [[ "$name" == "Registered"* ]] && continue
    [[ "$name" == "=== "* ]] && continue
    if [[ -n "$RUN_PATTERN" ]]; then
        echo "$name" | grep -qE "$RUN_PATTERN" || continue
    fi
    TESTS+=("$name")
done <<< "$RAW"

TOTAL=${#TESTS[@]}
if [[ $TOTAL -eq 0 ]]; then
    echo "No tests matched." >&2
    exit 0
fi
echo "=== Running $TOTAL tests ($PARALLEL parallel, ${TIMEOUT}s timeout) ===" >&2

# --- Temp dir ---
RESULT_DIR=$(mktemp -d)
trap 'rm -rf "$RESULT_DIR"' EXIT

# --- Semaphore for parallelism ---
mkfifo "$RESULT_DIR/sem"
exec 3<>"$RESULT_DIR/sem"
rm -f "$RESULT_DIR/sem"
for ((i = 0; i < PARALLEL; i++)); do
    echo >&3
done

START_TIME=$SECONDS

for idx in "${!TESTS[@]}"; do
    test_name="${TESTS[$idx]}"
    read -r -u 3  # acquire slot

    (
        result_file="$RESULT_DIR/$idx"
        log_file="$RESULT_DIR/$idx.log"
        attempt=1
        final_status="FAIL"
        final_elapsed=0

        while [[ $attempt -le $RETRY ]]; do
            t0=$SECONDS

            # Run with hard timeout via `timeout` command
            timeout "$TIMEOUT" "$MEGA_BIN" \
                -test.run "^TestMega$" \
                -mega.run "^${test_name}$" \
                -test.timeout "${TIMEOUT}s" \
                > "$log_file" 2>&1
            ec=$?

            final_elapsed=$(( SECONDS - t0 ))

            # exit code 124 = timeout killed it; 137 = SIGKILL
            if [[ $ec -eq 124 || $ec -eq 137 ]]; then
                final_status="TIMEOUT"
                break
            fi

            if [[ $ec -eq 0 ]]; then
                final_status="PASS"
                break
            fi

            # Non-zero, non-timeout => test failure
            final_status="FAIL"
            attempt=$((attempt + 1))
        done

        echo "${final_status} ${final_elapsed}" > "$result_file"
        # Keep log only for failures
        if [[ "$final_status" == "PASS" ]]; then
            rm -f "$log_file"
        fi

        echo >&3  # release slot
    ) &
done

wait

# --- Report results ---
PASS=0
FAIL=0
TIMEOUT_COUNT=0
FAIL_LIST=()

for idx in "${!TESTS[@]}"; do
    test_name="${TESTS[$idx]}"
    result_file="$RESULT_DIR/$idx"
    log_file="$RESULT_DIR/$idx.log"

    if [[ -f "$result_file" ]]; then
        status=$(cut -d' ' -f1 "$result_file")
        elapsed=$(cut -d' ' -f2 "$result_file")
        tstr=$(fmt_time "${elapsed:-0}")

        case "$status" in
            PASS)
                PASS=$((PASS + 1))
                printf "[PASS]   %-60s %s\n" "$test_name" "$tstr"
                ;;
            TIMEOUT)
                TIMEOUT_COUNT=$((TIMEOUT_COUNT + 1))
                FAIL_LIST+=("$test_name")
                printf "[TIMEOUT] %-60s %s\n" "$test_name" "$tstr"
                ;;
            *)
                FAIL=$((FAIL + 1))
                FAIL_LIST+=("$test_name")
                printf "[FAIL]   %-60s %s\n" "$test_name" "$tstr"
                # Print failure details to stderr
                if [[ -f "$log_file" ]]; then
                    {
                        echo "--- FAILED: $test_name ---"
                        grep -v "Registered tests:" "$log_file" \
                            | grep -E "Error|FAIL|panic|fatal|Trace|test\.go" \
                            | head -20
                        echo "--- END ---"
                    } >&2
                fi
                ;;
        esac
    else
        FAIL=$((FAIL + 1))
        FAIL_LIST+=("$test_name")
        printf "[FAIL]   %-60s ?\n" "$test_name"
    fi
done

TOTAL_TIME=$(( SECONDS - START_TIME ))
echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed, ${TIMEOUT_COUNT} timeout, ${TOTAL} total in $(fmt_time $TOTAL_TIME) ==="

if [[ ${#FAIL_LIST[@]} -gt 0 ]]; then
    echo ""
    echo "Failed/Timed out tests:"
    for f in "${FAIL_LIST[@]}"; do
        echo "  - $f"
    done
    exit 1
fi

exit 0