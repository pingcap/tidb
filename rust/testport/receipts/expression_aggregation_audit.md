# `pkg/expression/aggregation` — Go-master parity receipt

Status: audited as a complete Go package with a bounded Rust descriptor parity
slice. The Go `max_count`/`min_count` family is a cross-package feature: parser
names and grammar, expression descriptors, aggregate runtime, hash aggregation,
planner routing, protobuf projection, and KV pushdown all change together. This
batch closes the dependency-closed descriptor/type-inference/pushdown portion
in `tidb-expr`; this follow-up adds the dependency-closed pair accumulator in
`tidb-exec`, while live SQL wiring and aggregate protobuf projection remain
explicit boundaries in their owning crates.

Comparison source: Go `origin/master` at commit
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-04). The relevant Go
feature was introduced by `5d6fdbe6f5` (`max_count`/`min_count` aggregate
support); the package inventory below was read at the current master tree.

## Complete Go inventory

The package has exactly 25 tracked artifacts and 4,193 Go lines. It has no
`doc.go`, fixture/testdata tree, generated production source, or
platform-specific Go variant.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 93 | package library and test target metadata |
| `agg_to_pb.go` | 231 | aggregate descriptor ↔ tipb projection |
| `agg_to_pb_test.go` | 129 | protobuf mapping tests |
| `aggregation.go` | 334 | aggregate factory, mode and pushdown policy |
| `aggregation_test.go` | 789 | aggregate descriptor and policy tests |
| `avg.go` | 100 | AVG evaluator |
| `base_func.go` | 558 | shared aggregate evaluator/type-inference base |
| `base_func_test.go` | 246 | base evaluator tests |
| `bench_test.go` | 99 | aggregate benchmarks |
| `bit_and.go` | 70 | BIT_AND evaluator |
| `bit_or.go` | 68 | BIT_OR evaluator |
| `bit_xor.go` | 68 | BIT_XOR evaluator |
| `concat.go` | 135 | GROUP_CONCAT evaluator |
| `count.go` | 81 | COUNT evaluator |
| `descriptor.go` | 423 | aggregate descriptor, split and result metadata |
| `explain.go` | 80 | EXPLAIN formatting |
| `first_row.go` | 59 | FIRST_ROW evaluator |
| `main_test.go` | 34 | package test initialization |
| `max_min.go` | 62 | MAX/MIN evaluator |
| `max_min_count.go` | 103 | Go `max_count`/`min_count` pair evaluator |
| `sum.go` | 40 | SUM evaluator |
| `sum_int.go` | 88 | integer SUM evaluator |
| `util.go` | 97 | aggregate utility and distinct-checker helpers |
| `util_test.go` | 46 | utility tests |
| `window_func.go` | 160 | window aggregate wrappers |

The 18 production Go files contain 4,193 − 1,363 test/build lines and were
read function by function; the six test files contain the descriptor, evaluator,
benchmark, and package-initialization coverage listed above. There are no
additional generated or platform-specific inputs hidden from the package
build.

## Rust ownership and comparison

The expression-side Rust seed is split across
`rust/crates/tidb-expr/src/aggregation/{base_func,descriptor,explain,mod,names,tests,window_func,wrap_cast}.rs`.
Its module documentation explicitly scopes this crate to descriptor/type
inference and explains that aggregate evaluation lives elsewhere. The runtime
owners are `tidb-exec/src/aggregate/runtime/{mod,max_min}.rs` and
`tidb-executor/src/{hash_agg.rs,driver/agg_build.rs}`. Parser aggregate names
are in `tidb-parser/src/expr/window.rs`; protobuf aggregate expression types
are projected by `tidb-proto`; planner and KV pushdown consume the descriptor.

Before the descriptor batch, the Rust owner had no `MAX_COUNT`/`MIN_COUNT`
names or descriptor type-inference/pushdown arms. Those exact Go arms are now
present, and this follow-up adds the executor pair state described below,
including count-shaped return/default metadata, one-stage TiFlash-only
pushdown, original extreme-value typing across `Split`, outer-join count
defaults, and NOT NULL behavior. The pair state remains outside `tidb-expr`
in the aggregate runtime owners, and protobuf `ExprType_MaxCount` /
`ExprType_MinCount` remains absent from `tidb-proto`.

The existing source-parity tests therefore remain explicit, actionable gaps:

- `test_agg_func_max_min_count_to_pb` — ignored because the Rust protobuf
  projection lacks the aggregate ExprType members.
- `test_max_min_count` — the descriptor-side carrier remains ignored because
  its SQL evaluator harness is outside the `tidb-expr` seed. The dependency-
  closed pair-state semantics are now covered by
  `tidb-exec/tests/max_min_count_runtime_source.rs`; live SQL dispatch and
  row-based/window evaluation remain unported.
- `test_check_agg_push_down_max_min_count` and
  `test_base_func_infer_max_min_count_ret_type` are now active in the source
  carrier; the focused descriptor tests also live in `aggregation/tests.rs`.

No Rust-only behavior was removed. The descriptor and runtime changes are
deliberately bounded: they do not advertise a SQL or PB path that is not
present.

## Executor pair-state follow-up (`pkg/executor/aggfuncs`)

The complete Go owner inventory above includes `max_min_count.go` and its
coverage in `func_max_min_count_test.go`. The Go evaluator keeps one winning
extreme value plus the number of rows tied at that extreme: NULL rows are
ignored, an empty/all-NULL group returns zero, a strictly better value resets
the tie count, and a partial merge adds counts only when the winning values are
equal. The descriptor chooses MAX versus MIN ordering and the evaluator uses
the argument's native comparison domain.

Rust now mirrors that dependency-closed state in
`tidb-exec::aggregate::runtime::MaxMinCountState`. `fold_values` updates a
partial state, merges it into the destination, and returns the BIGINT tie
count. Focused source-derived regressions cover MAX/MIN ties, NULL/default
semantics, UInt/Decimal/case-insensitive string comparison, partial merges,
kind mismatch, and reset behavior.

The remaining boundaries are intentional and tracked: `tidb-executor`'s live
hash-aggregation dispatch still has no MaxCount/MinCount arm; tipb/protobuf
`ExprType_MaxCount` and `ExprType_MinCount` are absent; and Go's row-based
final mode, DISTINCT/window sliding state, memory tracker, and SQL integration
tests have no Rust owner yet. These must be implemented together before the
package can claim end-to-end parity.

## Validation

Profile: Ready for the bounded descriptor slice; package-complete parity is not
claimed while evaluator/PB owners remain unported.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/expression/aggregation -count=1` — package tests passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr max_min_count -- --nocapture` — 6 active descriptor regressions passed; the SQL evaluator and PB source carriers remain ignored for their documented owner gaps.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-exec --test all max_min_count_runtime_source -- --nocapture` — 3 focused pair-state regressions passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-exec` — passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-exec --test all -- --test-threads=1` — Ready suite reached the new 3 pair-state tests (all passed), then reported the pre-existing `analyze_added_column_source::a_column_added_after_the_rows_analyzes_as_its_origin_default` and `placement_delivery_source::a_bundle_delivery_is_gos_post_with_partial_true` failures; the next placement-delivery case hung awaiting its external fixture, so the run was interrupted after the baseline failure/hang. No max/min-count test failed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/executor/aggfuncs -run 'TestMaxMinCountAllMaxMinTypes|TestMaxMinCountDuplicateSemantics|TestMergePartialResult4MaxMinCount' -count=1` — blocked before test selection by the pre-existing `pkg/session/session.go` metrics mismatch (`CancelWaitAversePlan` and `CancelStandardModePlan` are absent).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check rust/crates/tidb-expr/src/aggregation/base_func.rs rust/crates/tidb-expr/src/aggregation/descriptor.rs rust/crates/tidb-expr/src/aggregation/mod.rs` — passed.
- `git diff --check` — passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `git diff --check` — passed.
- `make lint` with the pinned Go toolchain — passed with existing warnings.

## Risks and unverified surfaces

- Correctness risk is concentrated in the pair accumulator's complete/partial/
  final state contract and its interaction with DISTINCT and NULL extrema,
  which this descriptor slice intentionally does not implement.
- Compatibility risk spans parser canonicalization, planner one-store routing,
  tipb expression enums, and KV checker behavior; a leaf-only implementation
  could expose SQL that cannot be planned or executed.
- Runtime cost is one native comparison per input row plus constant-size state;
  no serialization path was added.
- The end-to-end max/min-count feature remains unverified until live hash-agg,
  protobuf, row-based/window, and SQL owners are implemented as one
  dependency-closed batch.
