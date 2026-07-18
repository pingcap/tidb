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
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/tidb-rust-target}

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
        client_go_ledger \
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
        [ "$#" -eq 1 ] || usage
        scripts/work-unit-queue.py gate-begin
        trap 'scripts/work-unit-queue.py gate-abort >/dev/null 2>&1 || true' EXIT HUP INT TERM
        cargo fmt --all -- --check
        cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings
        cargo test --offline --locked -j12 --workspace -q
        python3 -m unittest scripts/test_work_unit_queue.py scripts/test_status_dashboard.py
        static_gates
        test -z "$(cargo tree --offline --locked -p difftest-parser-tests | rg 'tidb-(expr|exec)' || true)"
        git -C .. diff --check -- rust
        scripts/work-unit-queue.py gate-finish
        trap - EXIT HUP INT TERM
        ;;
    *)
        usage
        ;;
esac
