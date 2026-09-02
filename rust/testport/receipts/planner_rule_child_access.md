# Rust `tidb-planner` rule-entry child-access boundary receipt

Status: bounded Rust-only alignment batch. This receipt covers the
child/argument access boundaries of three logical-optimization rule bodies;
it does not claim completion of the entire planner transcreation.

Comparison source: Go `origin/master` at `a85e0fd5df` (2026-09-02). The owning
Go sources for this batch are byte-identical to that revision for everything
compared here: `pkg/planner/core/rule/rule_max_min_eliminate.go`,
`pkg/planner/core/rule_eliminate_unionall_dual_item.go`, and
`pkg/planner/core/rule_derive_topn_from_window.go`; `logical_selection.go`
differs from master only by an unrelated inherited-method comment stub. The
comparison is limited to the shared access contracts below and does not claim
a completed transcreation of any of these Go packages.

## Rust owner inventory

The owning crate is `rust/crates/tidb-planner`, inventoried in
`planner_child_accessors.md` (344 tracked artifacts, 140,179 lines; no
fixture tree, benchmark, or platform-specific source variant). This batch
edits three production files — `src/logical/rule_eliminate_unionall_dual_item.rs`,
`src/logical/rule_derive_topn_from_window.rs`, and
`src/logical/rule_max_min_elimination.rs` — plus their shared regression home
`src/logical/rule_tail_tests.rs`, whose helpers (`base`, `with_children`,
`data_source`, `dual`, `projection`, `selection`, `row_number_window`,
`test_context`) were read before validation. No Go source, Bazel metadata,
fixture result, generated output, or platform variant was changed.

## Alignment

Each site below previously answered a malformed tree with a Rust-only
`None`/internal-error refusal where the Go rule body indexes directly and
panics with an index-out-of-range:

- `unionAllEliminateDualItem` case 2 reads `proj.Children()[0]`
  unconditionally; Rust's `is_projection_over_zero_row_dual` now indexes the
  projection's first child instead of skipping childless projections.
- `LogicalSelection.DeriveTopN` → `windowIsTopN` reads `p.Children()[0]`
  before any type assertion, and then `child.Children()[0]` before asserting
  the grandchild is a `DataSource`; Rust's `derive_topn` walk and
  `window_is_topn` now preserve both index boundaries while keeping Go's
  `return false` for a non-window child and a non-DataSource grandchild.
- `MaxMinEliminator.eliminateSingleMaxMin` reads `agg.AggFuncs[0]`,
  `f.Args[0]`, and `agg.Children()[0]` unconditionally, and
  `splitAggFuncAndCheckIndices` reads `agg.Children()[0]`, `f.Args[0]`, and
  `agg.Schema().Columns[i]` unconditionally; `cloneSubPlans` indexes
  `p.Children()[0]` for the selection arm. Rust's `eliminate_single`,
  `split_aggregations`, and `clone_subplan` now index the same slots; the
  non-selection `cloneSubPlans` default still maps Go's checked-subtree `nil`
  return to `None`.

The Go guards that produce early returns rather than panics (conditions
length, upper-bound shape, enum/set arguments, non-column aggregate
arguments) were already mirrored and are unchanged.

## Focused regressions

`src/logical/rule_tail_tests.rs` gains four panic-contract tests, each proven
to FAIL before the fix and pass after it on this branch:

- `a_projection_branch_without_children_panics_like_gos_direct_index`
- `a_childless_selection_panics_when_derive_topn_probes_it_like_go`
- `a_window_without_children_panics_at_the_grandchild_probe_like_go`
- `a_max_min_aggregation_without_a_child_panics_like_gos_direct_index`

## Validation

Profile: Ready for this bounded Rust package batch.

- Pre-fix baseline: the four new tests fail against the unmodified rule
  bodies (2 observed under the `panics_like_go` filter and 2 under the
  `a_childless_selection_panics_when a_window_without_children_panics`
  filter), then pass after the three production edits.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --lib --
  rule_tail_tests` — 27 passed, including the four new regressions and all
  pre-existing union-dual / derive-topn coverage.
- Full `tidb-planner --lib`, aggregate `--test all`, `cargo check
  --all-targets -p tidb-planner`, pinned `cargo fmt --all -- --check`,
  `make lint`, and `git diff --check` — recorded in
  `TESTPORT_EXECPLAN.md`; the crate keeps the five known baseline `--lib`
  failures that reproduce at clean HEAD.

## Risks

- Correctness: malformed subtrees now panic at the same index Go panics at;
  valid planning trees never hit these boundaries, and all non-index guard
  paths keep their early-return behavior.
- Compatibility: the public rule entry signatures are unchanged; the only
  behavior change on valid input is none — indexes stay in range.
- Performance: direct indexing replaces `Option`-producing `first()` probes
  on the hot rule-entry paths, removing a branch per access.
