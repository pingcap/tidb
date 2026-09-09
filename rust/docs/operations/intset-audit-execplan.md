# Align `pkg/util/intset` with the pinned Go package

This ExecPlan follows `PLANS.md` and records the complete-package parity work
against Go commit `e2788410d8d696605e8cb002585877a063ccc909`.

## Goal

Treat the four Go artifacts (`BUILD.bazel`, `fast_int_set.go`,
`fast_int_set_test.go`, and `fast_int_set_bench_test.go`) as one unit. Rust must
provide the same set behavior, the six source tests, and the six benchmark
workloads, without a second constructor, a Rust-only iterator API,
duplicate contract tests, or a retired semantic-gate manifest.

## Progress

- [x] Read every pinned Go production, test, benchmark, and build artifact.
- [x] Mapped the Rust owner and all workspace consumers.
- [x] Removed the Rust-only `of`, `iter`, `FastIntSetIter`, borrowed
  `IntoIterator`, equality traits, sentinel exports, and `must_use` surface.
- [x] Generalized `new` only enough to represent Go's variadic constructor and
  updated consumers to use it.
- [x] Retained exactly the six source test translations and removed the three
  duplicate integration contracts plus ten supplemental owner tests.
- [x] Added executable translations of all six Go benchmarks, including the
  source benchmark's two insert-into-A loops.
- [x] Run focused Go/Rust tests, benchmark compilation/execution, consumer
  checks, formatting, and diff review.
- [x] Completed the package receipt and pushed commit `e03c6cb8a3` normally.

## Validation

Use the WIP profile because the broader package-by-package parity goal
continues after this package. Run `go test ./pkg/util/intset`, the six focused
Rust owner tests, the `intset` benchmark once, all-target checks for
`tidb-util` and `tidb-funcdep`, their focused/full tests as warranted, and
`cargo fmt --all --check`. Record exact outcomes in the receipt.

No Go or Bazel file changes are planned, so `make bazel_prepare` is not
required. If the final diff changes that premise, reevaluate the gate before
validation.

The WIP gate passed for the Go package, the six Rust owner tests, all 12
`tidb-funcdep` tests, the complete `tidb-util` crate, both changed crates'
all-target checks, the planner library check, the six-workload benchmark run,
scoped Clippy, formatting, and diff checks. The broader planner all-target
check is independently blocked by existing test drift: one test omits
`RuleContext.column_allocator`, and another initializes `ByItems` with a
`SortItem`.
