# `pkg/expression/aggregation` — Go-master parity receipt

Status: historical complete-package inventory with partial Rust acceptance.
Descriptor, protobuf and live executor owners remain; the disconnected
`tidb-exec` model follow-up has been retired. No complete package acceptance
is established by the model tests or their removal. Current ownership and
remaining obligations are recorded below.

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

## Current Rust owners and remaining obligations

The expression descriptor owner remains
`rust/crates/tidb-expr/src/aggregation`. Production partial states, updates,
merge and spill are owned by `tidb-executor/src/hash_agg.rs` and
`hash_agg/parallel.rs`; stream and window execution use their executor owners.
The disconnected `tidb-exec` aggregate runtime, count-aware deque and private
harnesses were retired in the
[aggregate-chain cleanup](../../docs/parity/current-audit/aggregate-chain-cleanup-validation.json).
The previous state-only test results are historical evidence; they never
established integration into the live SQL path.

Keep all original descriptor/evaluator, typed partial-state, DISTINCT, sliding,
memory, merge/spill, error-order, protobuf, platform and fixture obligations
from the Go packages. Existing live tests cover numerous cases, including
MAX_COUNT/MIN_COUNT ties and typed distinct merge/spill; this cleanup does not
claim those suites exhaust the original packages. Window/row-final-mode and
other previously incomplete boundaries remain open. Recover historical
implementation and validation notes from Git at fe8a0cbe32; no laptop-only
receipt is asserted to exist in Cloud.
