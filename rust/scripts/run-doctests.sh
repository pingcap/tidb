#!/usr/bin/env bash
#
# The doctest gate.
#
# `cargo nextest` DOES NOT RUN DOCTESTS AT ALL -- it has no way to, the
# harness is rustdoc's, not libtest's. The workspace gate is a nextest run,
# so for the whole life of this project every `assert!` inside a `///`
# comment has been unverified, and `cargo test -p tidb-executor --doc` was
# failing without anyone seeing it. This script is the missing step.
#
# It guards TWO failure modes, because a doctest gate that only checks
# "did the tests that ran pass?" is blind to the quieter one:
#
#   1. A doctest that RAN AND FAILED.       -> cargo's own exit status.
#   2. A doctest that STOPPED EXISTING.     -> the EXPECTED_DOCTESTS floor.
#
# (2) is not hypothetical. Retagging a ```rust fence to ```text, or wrapping
# an example in ```ignore, deletes the check while leaving the example
# looking exactly as verified as before -- a green run reporting
# "0 passed; 0 failed" is indistinguishable from a green run that proved
# something. Pinning the count turns "the example quietly stopped being
# checked" into a red gate that names the number it expected.
#
# Raising or lowering EXPECTED_DOCTESTS is allowed, in the same commit that
# changes the examples, which is the point: it makes the change reviewed
# rather than silent.

set -euo pipefail

EXPECTED_DOCTESTS=2

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# A DEDICATED BUILD DIRECTORY, DELIBERATELY OVERRIDING THE CALLER'S.
#
# `cargo test --doc` and `cargo nextest run` do not share artifacts: pointed
# at one directory they invalidate each other every run and then serialize on
# its build lock. Measured on this workspace, checking the same two doctests:
#
#     shared with the nextest gate    203-330s wall, 19% CPU  (waiting)
#     dedicated, cold                  24s wall, 437% CPU
#     dedicated, warm                   2.8s wall
#
# So the isolation is not a tidiness preference -- it is the difference
# between a gate that costs seconds and one that dominates the whole run.
# It is set here rather than left to callers because the failure is silent:
# a caller that forgets simply gets a slow gate and no explanation.
export CARGO_TARGET_DIR="${PWD}/target/doctest-gate"

log="$(mktemp -t tidb-doctests)"
trap 'rm -f "$log"' EXIT

echo "== cargo test --workspace --doc --no-fail-fast"
set +e
cargo test --workspace --doc --no-fail-fast 2>&1 | tee "$log"
status="${PIPESTATUS[0]}"
set -e

# Sum the "N passed" across every per-crate `test result:` line. rustdoc
# prints one such line per crate, including the many crates with zero.
passed="$(awk '/^test result:/ { total += $4 } END { print total + 0 }' "$log")"
failed="$(awk '/^test result:/ { total += $6 } END { print total + 0 }' "$log")"
ignored="$(awk '/^test result:/ { total += $8 } END { print total + 0 }' "$log")"
found=$((passed + failed + ignored))

echo
echo "== doctests: ${found} found (${passed} passed, ${failed} failed, ${ignored} ignored)"

if [ "$status" -ne 0 ]; then
  echo "FAIL: cargo test --doc exited ${status}" >&2
  exit "$status"
fi

if [ "$found" -ne "$EXPECTED_DOCTESTS" ]; then
  echo "FAIL: expected ${EXPECTED_DOCTESTS} doctests, found ${found}." >&2
  echo "      A doctest appearing or disappearing is a reviewed change:" >&2
  echo "      update EXPECTED_DOCTESTS in $0 in the same commit." >&2
  exit 1
fi

if [ "$ignored" -ne 0 ]; then
  echo "FAIL: ${ignored} doctest(s) marked \`ignore\` -- an ignored example" >&2
  echo "      is documentation that claims to be checked and is not." >&2
  exit 1
fi

echo "OK"
