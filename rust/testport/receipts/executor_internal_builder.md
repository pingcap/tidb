# `pkg/executor/internal/builder` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 123 lines. Every production
source file and Bazel target was read line by line. There are no package tests,
fixtures, generated files, benchmarks, fuzz targets, or platform variants.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 18 | `ca7a82c43ad3e5d7885352e03c1b98e5b4efc123` | `948431ecd444f00c2c6d6c816b269892bfbdbd41532fb22188ea58a85a83e556` | internal builder library target and dependency closure |
| `builder_utils.go` | 105 | `7757c0fe03fd857cf7d6647dd56883092cd175b6` | `b1c23d185a2ed749f1f77067e7a57026d0f0ff569a5a1fbbddef2de85e619e54` | DAG executor-list/tree construction and request metadata |

`builder_utils.go` defines five helpers. The tree branch calls a physical
plan's TiFlash `ToPB`; the list branch lowers each plan to TiKV and stops on
the first error; the non-natural-order variant stamps each executor's
`ParentIdx`; and `ConstructDAGReq` sets timezone name/offset, pushdown flags,
runtime-summary presence, non-default division precision, TiKV versus TiFlash
shape, and the session-selected result encoding. The final wrapper applies
the same parent indices to the request. The package itself has no test
harness; callers under `pkg/executor` provide integration coverage.

## Rust ownership and explicit boundary

The dependency-closed Rust owner is split across `tidb-exec::dag_request`,
`real_tikv_read`, and `cop_scan`. `dag_request` already implements the
bounded one-scan TiKV list form, including timezone/flags/summary/division
precision/encoding metadata, selections, limits, aggregates, and output
offset validation. Focused Rust source-derived DAG tests and the live TiKV
read path exercise that owner.

The Go package also owns a general physical-plan `ToPB` tree for TiFlash and
arbitrary list plans, plus the index-merge non-natural parent-index rewrite.
This Rust workspace has no TiFlash coprocessor/MPP DAG transport or
dependency-closed physical-plan-to-TiPB tree builder, and no Rust caller uses
the parent-index rewrite. Implementing either as an uncalled second planner
would invent behavior outside the current Rust execution path. The complete
Go package is therefore recorded as an explicit boundary; no Rust-only API or
behavior was changed in this batch.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No source was
changed, so `make bazel_prepare` and the Ready lint gate are not required for
this receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/executor/internal/builder
# passed; package has no test files and compiles cleanly
```

Not verified here: caller-level executor tests, Bazel execution, TiFlash
transport, and full workspace tests. Existing Rust warnings and unrelated
dirty `tidb-txnkv` files remain.

## Follow-up: the session's division scale reaches the DAG (2026-09-08)

`dag_request` implemented the `DivPrecisionIncrement` field and the lowering
omitted it at the default, but no production caller assigned it: `cop_scan`'s
`ConstructDAGReq` port built `DagRequestContext::new(...)` and left the field
at `DEFAULT_DIV_PRECISION_INCREMENT`, so `SET div_precision_increment = 5`
still sent the default to TiKV. `PushdownStatementContext` now carries
`div_precision_increment` from `StmtContext` (populated in `from_stmt`),
`cop_scan` assigns it to the DAG context, and `real_tikv_read` gained the
field plus `set_div_precision_increment` for its read-only tier.

Regression:
`cop_scan_string_selection_source::each_request_carries_the_statements_division_scale`
asserts the DAG omits the field at the default and carries `Some(5)` when the
statement sets 5. It failed with `left: None, right: Some(5)` before the
wiring and passes after. `cargo test -p tidb-exec --test all cop_scan` — 15
passed, 1 pre-existing failure (`a_limit_over_a_fully_lowered_builtin_
predicate_travels_with_it`, which fails on the clean baseline too).

## Follow-up: the planner resolver carries the statement's division scale (2026-09-09)

The executor's DAG request carried `div_precision_increment`, but the
PLANNER-side expression builder did not: `PlanScopeResolver` used the
`ColumnResolver` trait's default of 4, so a `/` built through
`PlanBuilder::rewrite_scalar` minted its decimal scale from 4 regardless of
the statement. `avg(a/b)` under `div_precision_increment = 10` therefore
answered `1.21428571428571` (division scale 4 + AVG increment 10) instead of
Go's `1.21428571428571428550` (10 + 10).

`PlanScopeResolver` now carries `div_precision_increment`, initialized from
`StmtContext::div_precision_increment()` at every `rewrite_scalar`, with the
same trait default of 4 for context-free resolvers.

Regression: `tests_executor_suite_statements_source::decimal_div_precision_increment`
failed at `avg_div(10)` and passes after; the `div(4/7/30)` and `avg(4)`
assertions are unchanged.

Ready validation from `rust/`:

```text
cargo test -p tidb-executor --lib decimal_div_precision_increment
# passed: 1
cargo test -p tidb-executor --lib -- --test-threads=1
# 81 failures against the rebased baseline's 82 (only the target fixed)
cargo fmt -p tidb-planner -- --check
# pre-existing joinorder.rs / logical/rewrite.rs drift only
git diff --check -- rust
# passed
```
