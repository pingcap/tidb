#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");

set -euo pipefail

# bazel_unit_test_targets.sh - Generate bazel test targets for Phase 1 (real unit tests only).
#
# Excludes:
#   - pkg/*/test/...      (registered in mega via init())
#   - pkg/*/tests/...     (independent integration tests, slow, sharded)
#   - pkg/testkit/mega    (test framework itself)
#   - integrationtest     (heavy integration tests)
#
# Keeps:
#   - executor/join/test/{indexjoin,mergejoin}  (not in mega)
#   - table/tables/test/partition                (not in mega)

EXCEPTIONS_RE="^//pkg/(executor/join/test/(indexjoin|mergejoin)|table/tables/test/partition):"

TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT

bazel query 'kind("go_test", //pkg/...)' --output=label 2>/dev/null | while IFS= read -r target; do
    # Skip mega framework
    if [[ "$target" == *"/pkg/testkit/mega"* ]]; then
        continue
    fi
    # Skip integrationtest
    if [[ "$target" == *"/integrationtest/"* ]] || [[ "$target" == *"/integrationtest:"* ]]; then
        continue
    fi
    # Skip pkg/*/tests/* (independent integration tests with their own TestMain)
    if [[ "$target" == *"/tests/"* ]] || [[ "$target" == *"/tests:"* ]]; then
        continue
    fi
    # Skip pkg/*/test targets (covered by mega) unless they match exceptions
    if [[ "$target" == *"/test/"* ]] || [[ "$target" == *"/test:"* ]]; then
        if echo "$target" | grep -qE "$EXCEPTIONS_RE"; then
            echo "$target" >> "$TMPFILE"
        fi
        continue
    fi
    echo "$target" >> "$TMPFILE"
done

sort -u "$TMPFILE"