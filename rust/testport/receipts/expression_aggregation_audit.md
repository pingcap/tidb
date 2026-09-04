# `pkg/expression/aggregation` — Go-master parity receipt

Status: audited as a complete Go package with a bounded Rust descriptor parity
slice. The Go `max_count`/`min_count` family is a cross-package feature: parser
names and grammar, expression descriptors, aggregate runtime, hash aggregation,
planner routing, protobuf projection, and KV pushdown all change together. This
batch closes the dependency-closed descriptor/type-inference/pushdown portion
in `tidb-expr`; evaluator runtime and aggregate protobuf projection remain
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

Before this batch, the Rust owner had no `MAX_COUNT`/`MIN_COUNT` names or
descriptor type-inference/pushdown arms. The batch adds those exact Go arms,
including count-shaped return/default metadata, one-stage TiFlash-only
pushdown, original extreme-value typing across `Split`, outer-join count
defaults, and NOT NULL behavior. The pair state remains outside `tidb-expr`
in the aggregate runtime owners, and protobuf `ExprType_MaxCount` /
`ExprType_MinCount` remains absent from `tidb-proto`.

The existing source-parity tests therefore remain explicit, actionable gaps:

- `test_agg_func_max_min_count_to_pb` — ignored because the Rust protobuf
  projection lacks the aggregate ExprType members.
- `test_max_min_count` — ignored because the evaluator pair state is outside
  the `tidb-expr` seed and is absent from the executor runtime owners.
- `test_check_agg_push_down_max_min_count` and
  `test_base_func_infer_max_min_count_ret_type` are now active in the source
  carrier; the focused descriptor tests also live in `aggregation/tests.rs`.

No Rust-only behavior was removed. The descriptor changes are deliberately
bounded: they do not advertise a runtime or PB path that is not present. The
next implementation unit is the dependency-closed evaluator/PB feature across
the parser, executor, planner, KV, and tipb owners, with focused tests at each
seam.

## Validation

Profile: Ready for the bounded descriptor slice; package-complete parity is not
claimed while evaluator/PB owners remain unported.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/expression/aggregation -count=1` — package tests passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr max_min_count -- --nocapture` — 6 active descriptor regressions passed; the evaluator and PB source carriers remain ignored for their documented owner gaps.
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
- Performance impact is limited to descriptor matching; no evaluator loop or
  serialization path was added.
- The end-to-end max/min-count feature remains unverified until all listed Rust
  owners are implemented as one dependency-closed batch.
