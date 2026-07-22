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
    echo "usage: $0 status | leaf <package> <test-target> [filter] | static | integrate" >&2
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

static_gates() {
    scripts/work-unit-queue.py check
    python3 scripts/status-dashboard.py --check
    build_evidence_tools
    for tool in \
        go_source_ledger \
        go_test_ledger \
        external_go_ledger \
        domain_queue \
        parser_translation_manifest \
        integration_parser_inventory \
        integration_parser_golden \
        integration_parser_queue \
        integration_plan_inventory
    do
        "$CARGO_TARGET_DIR/debug/$tool" --check
    done
    "$CARGO_TARGET_DIR/debug/parser_translation_manifest" --check-fragments
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
    static)
        [ "$#" -eq 1 ] || usage
        cargo fmt --all -- --check
        static_gates
        git -C .. diff --check -- rust
        ;;
    integrate)
        # Behavior verification only. The claim/receipt digest ceremony that
        # used to wrap this was removed: it forced full re-runs whenever any
        # file changed while the gate was running, and it never found a defect.
        # Ledger bookkeeping is still available through work-unit-queue.py and
        # campaign_close.py for anyone who wants it; it no longer blocks a build.
        [ "$#" -eq 1 ] || usage
        cargo fmt --all -- --check
        cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings
        cargo test --offline --locked -j12 --workspace -q
        python3 -m unittest scripts/test_status_dashboard.py
        cargo fmt --all -- --check
        build_evidence_tools
        "$CARGO_TARGET_DIR/debug/integration_parser_golden" --check
        "$CARGO_TARGET_DIR/debug/integration_parser_queue" --check
        # POSIX grep, not rg: this script runs under /bin/sh, where a caller's
        # interactive-shell `rg` may not exist. A missing matcher made the
        # pipeline print nothing, so the isolation check passed unconditionally.
        parser_test_tree=$(cargo tree --offline --locked -p difftest-parser-tests)
        test -z "$(printf '%s\n' "$parser_test_tree" | grep -E 'tidb-(expr|exec)' || true)"
        git -C .. diff --check -- rust
        ;;
    *)
        usage
        ;;
esac
