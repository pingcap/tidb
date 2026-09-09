# `pkg/planner/cascades/base` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly seven tracked artifacts and 563 lines. Every
production file, test file, and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 27 | library and flaky short-test target |
| `base.go` | 37 | hash, equality, and combined interface contracts |
| `base_test.go` | 79 | typed/dynamic equality benchmarks |
| `hash_equaler.go` | 210 | FNV-1a hasher, cache lifecycle, and nil markers |
| `hash_equaler_test.go` | 149 | string framing, cross-type equality, and primitive hash tests |
| `task_scheduler_base.go` | 25 | scheduler interface |
| `task_stack_base.go` | 36 | task and stack interfaces |

The production declaration inventory was checked function by function:
`Hasher`, `Equals`, and `HashEquals`; `NewHashEqualer`, `Reset`, `Cache`,
`SetCache`, `Sum64`, every primitive `Hash*` method, and the `NilFlag` /
`NotNilFlag` constants; plus the scheduler and stack interface methods. The
test inventory contains two benchmarks (`BenchmarkEqualsT`,
`BenchmarkEqualsAny`) and three tests (`TestStringLen`, `TestStructType`,
`TestHash64a`). There is no package `doc.go`, `TestMain`, fuzz target, example,
fixture/testdata tree, generated source, platform/build-tag variant, nested
package, or other support artifact. The BUILD file lists exactly the four
production and two test source files above.

## Rust ownership and parity

The dependency-closed owner is `rust/crates/tidb-planner`: `base_traits.rs`,
`hash_equaler.rs`, `scheduler_contract.rs`, `stack_contract.rs`, and the
package re-export/test surface in `cascades_base.rs`. The planner manifest and
module declarations, the source-derived
`tests/cascades_base_hash_equaler_source.rs` (the two remaining hash tests;
`TestStringLen` is covered by the existing inline owner test), and direct
logical-plan/parser consumers were re-read. The related concrete scheduler and
stack implementations remain owned by the separate `pkg/planner/cascades/task`
package; this batch changes no task implementation.

Go permits callers to discard `NewHashEqualer`'s returned `Hasher`. Rust
previously marked the source-shaped `new_hash_equaler` constructor
`#[must_use]`, while `Hash64a::new/raw` remain annotated as native raw-value
adapters with no Go constructor/method counterpart. The constructor annotation
was removed; FNV-1a update order, byte-length string framing, cache reset, nil
markers, dynamic equality, and scheduler/stack interface shapes are unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards the direct
constructor result. Before the edit it failed with exactly one diagnostic;
after the edit it passes.

## Validation (Ready profile)

- Current Go-master seven-artifact inventory and complete declaration/test
  mapping — passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib hash_equaler::return_contract_tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly one `unused return value` diagnostic.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib hash_equaler::return_contract_tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib cascades_base::tests -- --test-threads=1` — passed (8/8 owner tests).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/hash_equaler.rs rust/crates/tidb-planner/src/cascades_base.rs rust/crates/tidb-planner/src/scheduler_contract.rs rust/crates/tidb-planner/src/stack_contract.rs` and `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The source-derived hash test file is consumed by the generated planner `all`
target; no standalone Cargo test target exists for it. The broader aggregate
was not rerun here because the current unrelated
`core_logical_cte_topn_prune_source.rs:75` initializer still omits
`RuleContext.allow_agg_push_down`, as recorded by the adjacent planner
receipts. This is a Rust-only batch: no Go source, import section, Go test
function, Bazel file, or module dependency changed, so `make bazel_prepare` is
not required. The package has no failpoint use. The repository-level Ready
lint gate passed with the receipt and ExecPlan updates included.

## Risks and boundaries

- Correctness: the fail-before/pass-after constructor regression and all eight
  package owner tests pass; primitive hashing and contract behavior are
  unchanged.
- Compatibility: only Rust lint policy changed for a Go constructor whose
  result may be discarded; no Rust signature or runtime behavior changed.
- Performance: no hashing algorithm, allocation, scheduler, or stack path
  changed.
- Not verified locally: the generated aggregate test target, full workspace
  tests, Bazel execution, and cross-platform planner builds.
