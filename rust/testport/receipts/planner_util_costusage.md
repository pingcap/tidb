# `pkg/planner/util/costusage` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly two tracked artifacts and 210 lines. Every
production file and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 8 | public Go library target |
| `cost_misc.go` | 202 | cost flags, values, traces, arithmetic, and options |

The declaration inventory was checked function by function: the three cost
flags; `CostVer2.GetCost` and `GetTrace`; `CostTrace.GetFormula` and
`GetFactorCosts`; `NewZeroCostVer2`, `HasCostFlag`, `TraceCost`, `NewCostVer2`,
`SumCostVer2`, `DivCostVer2`, `MulCostVer2`, `AddCostWithoutTrace`,
`NewDefaultPlanCostOption`, and `PlanCostOption.WithCostFlag`; plus
`CostVer2Factor.String`. The package has no Go test file, `doc.go`, `TestMain`,
benchmark, fuzz target, example, fixture/testdata tree, generated source,
platform/build-tag variant, nested package, or other support artifact. The
BUILD file lists exactly the one production source file above.

## Rust ownership and parity

The dependency-closed owner is
`rust/crates/tidb-planner/src/cost_usage.rs`. Its flags, cost/tracing structs,
arithmetic helpers, option handling, formatting behavior, direct consumers,
and existing planner source-derived tests were re-read. Go permits callers to
discard all of the source-shaped query, predicate, constructor, and arithmetic
results; Rust had imposed `#[must_use]` on 13 such APIs. The annotations were
removed from `CostTrace::{formula,factor_costs}`, `CostVer2::{value,trace}`,
`PlanCostOption::new` (the owner of Go's `NewDefaultPlanCostOption`),
`has_cost_flag`, `trace_cost`, `new_zero_cost_ver2`, `new_cost_ver2`,
`sum_cost_ver2`, `div_cost_ver2`, `mul_cost_ver2`, and
`add_cost_without_trace`. The four annotations on Rust-native factor
construction/accessors and the option field accessor remain; they have no
discardable Go method counterpart. Cost arithmetic, trace aggregation,
formula formatting, flags, and ownership remain unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards every
source-shaped result. In a temporary clean worktree at the pre-fix revision it
failed with exactly 13 `unused return value` diagnostics; after the edit it
passes 1/1.

## Validation (Ready profile)

- Current Go-master two-artifact inventory and complete declaration mapping —
  passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib cost_usage::return_contract_tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly 13 diagnostics (captured in `/tmp/tidb-codex/costusage-prefix2.log`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib cost_usage::return_contract_tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/cost_usage.rs` and
  `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The package has no Go test target and no failpoint use. This is a Rust-only
batch: no Go source, import section, Go test function, Bazel file, or module
dependency changed, so `make bazel_prepare` is not required. The generated
planner aggregate, full workspace tests, Bazel execution, and cross-platform
planner builds were not run; the aggregate remains blocked by the unrelated
`tests/core_logical_cte_topn_prune_source.rs:75` missing
`RuleContext.allow_agg_push_down` initializer recorded by adjacent receipts.

## Risks and boundaries

- Correctness: the fail-before/pass-after regression and planner library check
  pass; cost arithmetic, tracing, flags, and formatting are unchanged.
- Compatibility: only Rust lint policy changed for APIs whose Go callers may
  discard results; Rust-native ownership adapters remain strict.
- Performance: no cost calculation, allocation, map, or formatting path
  changed.
- Not verified locally: the generated aggregate target, full workspace tests,
  Bazel execution, and cross-platform planner builds.
