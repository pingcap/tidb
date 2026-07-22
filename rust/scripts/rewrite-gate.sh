#!/bin/sh
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUST_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
cd "$RUST_ROOT"

export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-12}
if [ -z "${CARGO_TARGET_DIR:-}" ]; then
    # Several evidence binaries embed CARGO_MANIFEST_DIR. Reusing their target
    # across Git worktrees can execute a fresh binary against the checkout that
    # compiled it, so the default cache must be checkout-specific.
    checkout_key=$(printf '%s' "$RUST_ROOT" | cksum | awk '{print $1}')
    CARGO_TARGET_DIR=${XDG_CACHE_HOME:-$HOME/.cache}/tidb-rust-target-$checkout_key
    export CARGO_TARGET_DIR
fi

usage() {
    echo "usage: $0 status | leaf <package> <test-target> [filter] | prepare | static | integrate" >&2
    exit 2
}

status() {
    awk -F '\t' '
        !/^#/ { count[$5]++ }
        END {
            printf "source\tUNTRIAGED\t%d\n", count["UNTRIAGED"]
            printf "source\tPARTIAL\t%d\n", count["PARTIAL"]
            printf "source\tCOVERED\t%d\n", count["COVERED"]
            printf "source\tBLOCKED\t%d\n", count["BLOCKED"]
        }
    ' difftests/corpus/coverage/go_source_inventory.tsv
    awk -F '\t' '
        !/^#/ { count[$6]++ }
        END {
            printf "test\tUNTRIAGED\t%d\n", count["UNTRIAGED"]
            printf "test\tPARTIAL\t%d\n", count["PARTIAL"]
            printf "test\tCOVERED\t%d\n", count["COVERED"]
            printf "test\tBLOCKED\t%d\n", count["BLOCKED"]
        }
    ' difftests/corpus/coverage/go_test_inventory.tsv
}

build_evidence_tools() {
    cargo build --offline --locked -j12 -p difftest --bins -q
}

run_static_check() {
    "$@" &
    static_pids="$static_pids $!"
}

wait_static_checks() {
    failed=0
    for pid in $static_pids; do
        if ! wait "$pid"; then
            failed=1
        fi
    done
    [ "$failed" -eq 0 ]
}

static_gates() {
    build_evidence_tools
    # These checks are read-only and share no output files. Run all of them at
    # once after the evidence binaries are built; the slow test-ledger scan no
    # longer serializes every other inventory and golden check.
    static_pids=""
    run_static_check scripts/work-unit-queue.py check
    run_static_check python3 scripts/status-dashboard.py --check
    run_static_check "$CARGO_TARGET_DIR/debug/go_source_ledger" --check
    run_static_check "$CARGO_TARGET_DIR/debug/go_test_ledger" --check
    run_static_check "$CARGO_TARGET_DIR/debug/external_go_ledger" --check
    run_static_check "$CARGO_TARGET_DIR/debug/domain_queue" --check
    run_static_check "$CARGO_TARGET_DIR/debug/parser_translation_manifest" --check
    run_static_check "$CARGO_TARGET_DIR/debug/integration_parser_inventory" --check
    run_static_check "$CARGO_TARGET_DIR/debug/integration_parser_golden" --check
    run_static_check "$CARGO_TARGET_DIR/debug/integration_parser_queue" --check
    run_static_check "$CARGO_TARGET_DIR/debug/integration_plan_inventory" --check
    run_static_check "$CARGO_TARGET_DIR/debug/parser_translation_manifest" --check-fragments
    wait_static_checks
}

prepare_generated() {
    build_evidence_tools
    "$CARGO_TARGET_DIR/debug/go_source_ledger" --write
    "$CARGO_TARGET_DIR/debug/go_test_ledger" --write-evidence
    python3 scripts/status-dashboard.py --write
}

case ${1:-} in
    status)
        [ "$#" -eq 1 ] || usage
        status
        ;;
    leaf)
        [ "$#" -ge 3 ] && [ "$#" -le 4 ] || usage
        package=$2
        test_target=$3
        test_filter=${4:-}
        cargo fmt --all -- --check
        if [ -n "$test_filter" ]; then
            cargo test --offline --locked -j12 -p "$package" --test "$test_target" "$test_filter"
        else
            cargo test --offline --locked -j12 -p "$package" --test "$test_target"
        fi
        cargo clippy --offline --locked -j12 -p "$package" --test "$test_target" -- -D warnings
        git -C .. diff --check -- rust
        ;;
    prepare)
        [ "$#" -eq 1 ] || usage
        prepare_generated
        ;;
    static)
        [ "$#" -eq 1 ] || usage
        cargo fmt --all -- --check
        static_gates
        git -C .. diff --check -- rust
        ;;
    integrate)
        [ "$#" -eq 1 ] || usage
        scripts/work-unit-queue.py gate-begin
        trap 'scripts/work-unit-queue.py gate-abort >/dev/null 2>&1 || true' EXIT HUP INT TERM
        cargo fmt --all -- --check
        # Fail on cheap ownership, dashboard, and generated-inventory drift
        # before paying for workspace Clippy and every test binary.
        static_gates
        cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings
        cargo test --offline --locked -j12 --workspace -q
        python3 -m unittest \
            scripts/test_campaign_close.py \
            scripts/test_work_unit_queue.py \
            scripts/test_status_dashboard.py
        # gate-finish compares the complete checked-input snapshot with
        # gate-begin. If tests mutate any checked file it rejects the run, so a
        # second 50k-query static pass would repeat the same proof.
        # POSIX grep, not rg: this script runs under /bin/sh, where a caller's
        # interactive-shell `rg` may not exist. A missing matcher made the
        # pipeline print nothing, so the isolation check passed unconditionally.
        parser_test_tree=$(cargo tree --offline --locked -p difftest-parser-tests)
        test -z "$(printf '%s\n' "$parser_test_tree" | grep -E 'tidb-(expr|exec)' || true)"
        git -C .. diff --check -- rust
        scripts/work-unit-queue.py gate-finish
        trap - EXIT HUP INT TERM
        ;;
    *)
        usage
        ;;
esac
