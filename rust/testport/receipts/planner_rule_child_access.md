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

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5d15205f2a (rust: align extract col-groups child access boundaries)
## Follow-up batch: `ExtractColGroups` child access

Same comparison source (Go `origin/master` at `a85e0fd5df`, owning files
byte-identical). `LogicalPlan::extract_col_groups` in `src/logical/mod.rs`
previously refused with an empty result where the Go operator overrides index
children directly:

- `LogicalJoin.ExtractColGroups` (`logical_join.go:628`) reads
  `p.Children()[0].Schema()` for the left-side outer join types and
  `p.Children()[1].Schema()` for `RightOuterJoin` unconditionally, before any
  `colGroups` emptiness check; the Rust join arms now index the same children,
  keeping only the schema itself optional.
- `LogicalApply.ExtractColGroups` (`logical_apply.go:250`) reads
  `la.Children()[0].Schema()` for the left-side outer join types; the Rust
  apply arm indexes it now.
- `LogicalWindow.ExtractColGroups` (`logical_window.go:427`) checks
  `len(colGroups) == 0` FIRST and returns nil without touching children; only
  non-empty groups reach the unconditional `p.Children()[0].Schema()` index.
  The Rust window arm now preserves that exact ordering — empty groups stay
  panic-free, non-empty groups panic on a childless window.

Four focused regressions were added to `src/logical/operator_tests.rs`, each
proven to FAIL against the unfixed dispatcher (verified by stashing the
production edit and rerunning):

- `join_extract_col_groups_panics_on_a_childless_left_outer_join_like_go`
- `join_extract_col_groups_panics_on_a_single_child_right_outer_join_like_go`
- `apply_extract_col_groups_panics_on_a_childless_apply_like_go`
- `window_extract_col_groups_only_indexes_the_child_for_non_empty_groups`

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 41640f026f (rust: align planner rule child access boundaries)
=======
>>>>>>> 5d15205f2a (rust: align extract col-groups child access boundaries)
=======
## Follow-up batch: `ExtractFD` child access

Same comparison source (Go `origin/master` at `a85e0fd5df`, owning files
byte-identical). `LogicalPlan::extract_fd` in
`src/logical/functional_dependencies.rs` previously refused malformed
subtrees with empty/partial FD sets where Go's overrides index children:

- `LogicalSelection.ExtractFD` (`logical_selection.go:284`) indexes
  `p.Children()[0]` before the `*LogicalJoin` assertion; the Rust selection
  arm now indexes it too, keeping the full-schema fallback for a child that
  is a join with an unmaterialized full schema.
- `LogicalJoin.ExtractFD` dispatches on the join type FIRST: inner, left/right
  outer, and semi index `Children()[0]` and `Children()[1]` unconditionally
  (`logical_join.go:867/912/856`), while every other join type answers the
  empty set WITHOUT touching children. The Rust join arm now hoists that
  type switch ahead of the child access instead of probing children with
  `first()`/`get(1)`.
- `LogicalApply.ExtractFD` (`logical_apply.go:287`) indexes
  `la.Children()[1]` (and reads its schema for correlated equivalences)
  BEFORE its join-type switch — a malformed apply panics even for the default
  join types; the Rust apply arm now indexes both children up front.

Five focused regressions were added to `src/logical/tests.rs`, each panic
contract proven to FAIL against the unfixed arms (verified by stashing the
production edit and rerunning); the fifth pins Go's default-type ordering
(empty set, no child access) which already held:

- `a_childless_outer_join_panics_when_extracting_fd_like_go`
- `a_single_child_inner_join_panics_when_extracting_fd_like_go`
- `a_default_join_type_answers_an_empty_fdset_without_touching_children_like_go`
- `a_childless_apply_panics_when_extracting_fd_like_go`
- `a_childless_selection_panics_when_extracting_fd_like_go`

<<<<<<< HEAD
>>>>>>> 9309c25047 (rust: align extract-FD child access boundaries)
=======
## Follow-up batch: `GcSubstituter` schema selection

Same comparison source (Go `origin/master` at `a85e0fd5df`, owning file
`pkg/planner/core/rule_generate_column_substitute.go` byte-identical). The
Rust substitution walk in `src/logical/rule_generate_column_substitute.rs`
diverged from Go in two ways:

- The selection, projection, and sort arms substituted against an
  own-schema-first-or-child fallback. Go passes `x.Schema()` for selection
  and sort — whose base body indexes `children[0]` unconditionally — and
  indexes `x.Children()[0].Schema()` EXPLICITLY for projection, i.e. the
  CHILD schema rather than the projection's own output schema. Preferring
  the projection's output schema could mute the whole substitution rule
  (candidate child columns are absent from the output schema), so this is a
  functional alignment, not only a boundary one. All three arms now read the
  first child's schema by direct index.
- The aggregation arm carried a child fallback that Go does not have:
  `LogicalAggregation` is a schema producer and Go passes its OWN schema
  without touching children. The Rust arm now reads only the own schema.

Four in-module tests were added (`#[cfg(test)] mod tests` calling the
private walk with empty candidate sets); the three child-index contracts are
proven to FAIL against the unfixed arms (the production edit was temporarily
hand-reverted to capture that baseline), while the aggregation pin documents
Go's own-schema ordering:

- `a_childless_selection_panics_when_substituting_like_go`
- `a_childless_projection_panics_when_substituting_like_go`
- `a_childless_sort_panics_when_substituting_like_go`
- `an_aggregation_substitutes_against_its_own_schema_without_touching_children_like_go`

<<<<<<< HEAD
>>>>>>> f69f4e445a (rust: align gc-substitute schema selection with Go)
=======
## Follow-up batch: join-family rule bodies

Same comparison source (Go `origin/master` at `a85e0fd5df`; owning files
`rule_join_elimination.go`, `rule/rule_outer_join_to_semi_join.go`, and
`logicalop/logical_join.go` byte-identical). Three rule bodies still
tolerated malformed subtrees where Go indexes children or derefs schemas:

- `OuterJoinEliminator.try_eliminate` now indexes both children directly
  (Go `tryToEliminateOuterJoin:99-100`), touches the outer schema only under
  non-empty parent columns and the inner schema only under non-empty
  aggregate columns (Go's `len(parentCols)`/`len(aggCols)` guards), and
  panics on a nil child schema via `expect`, as Go's field deref does. The
  `required_columns` Apply arm indexes both children and derefs the left
  schema (`rule_join_elimination.go:329`/`:337`); its Projection arm KEEPS
  the child guard because Go guards `len(x.Children()) > 0` there.
- `OuterJoinToSemiJoin.rewrite_selection` reads the selection's schema and
  output names through `children[0]` (Go base `Schema()`/`OutputNames()`
  propagation) instead of an own-first fallback;
  `can_convert` indexes the outer and inner join children and expects their
  schemas (Go `canConvertAntiJoin:258`/`:276-277`), and the converted join's
  output names propagate from `children[0]`.
- `SemiJoinRewriter.rewrite_join` expects the left child schema where Go
  calls `p.Children()[0].Schema().Clone()` and nil-derefs.

Four focused regressions were added to `src/logical/rule_tail_tests.rs`,
each proven to FAIL against the unfixed bodies (captured by stashing the
production edits, including after the conversion test gained the
non-empty-condition precondition Go requires before indexing):

- `a_childless_join_panics_when_eliminating_outer_joins_like_go`
- `a_childless_selection_panics_in_outer_join_to_semi_like_go`
- `a_selection_over_a_childless_outer_join_panics_when_converting_like_go`
- `a_schemaless_left_child_panics_when_rewriting_semi_join_like_go`

<<<<<<< HEAD
>>>>>>> a6a6bdb111 (rust: align join-family rule boundaries with Go)
=======
## Follow-up batch: aggregation `AggFuncs` index boundaries

Comparison source: Go `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`. The owning
`pkg/planner/core/operator/logicalop` tree has 43 tracked artifacts and 16,086
lines: 35 production files (including the two generated Go outputs), five
logical-operator test/support files, three checked-in cascades fixture files,
and two BUILD manifests. There are no platform-specific variants, fuzz
corpora, or additional generated inputs. All artifacts were inventoried; the
relevant `logical_aggregation.go` and logical-operator test/build inputs were
read in full before editing. The Rust owner remains the 344-artifact
`tidb-planner` crate inventory recorded above.

Go's `PruneColumns` and `getAggFuncsColsForFirstRow` directly index the
schema-derived `AggFuncs` slot, and the latter directly indexes `Args[0]`.
Rust previously returned a Rust-only empty/partial answer for a schema longer
than the aggregate list or an argument-less `firstrow`; it now uses the same
direct indexes. `getAggFuncsColsForConstResult` retains Go's explicit
`idx >= len(AggFuncs)` break, pinned by a non-panicking regression.

The four focused regressions in `src/logical/operator_tests.rs` cover the two
`firstrow` boundaries, pruning, and the guarded constant-result loop. Before
the production edit, the three panic-contract tests failed; after the edit
they pass. No Go source, Bazel metadata, generated output, or fixture was
changed.

Validation for this package batch uses the Ready Rust scope: focused owner
tests, the full `tidb-planner --lib` and aggregate targets, all-targets check,
pinned formatting, repository lint, and diff checks. Existing unrelated
planner baseline failures remain documented in the ExecPlan and are not caused
by this batch.
=======
>>>>>>> 41640f026f (rust: align planner rule child access boundaries)

## Follow-up batch: physical join/projection schema boundaries

Same comparison source (Go `origin/master` at `a85e0fd5df`; owning files
`physicalop/base_physical_join.go` and `rule_eliminate_projection.go`
byte-identical). Three schema-assembly paths tolerated malformed subtrees:

- `build_physical_join_schema` now indexes `Children()[0]`/`[1]` directly
  (Go `base_physical_join.go:191`/`:204`), expects the left schema in the
  semi arms (`leftSchema.Clone()` nil-derefs), expects the join's own
  trailing column in the left-outer-semi pair (Go indexes
  `Columns[Len()-1]`), and nil-derefs the left schema in the outer-join
  not-null reset — while the Inner merge keeps Go's `MergeSchema` nil
  absorption.
- `eliminate_physical_projection` indexes `Children()[0]` where Go's
  `canProjectionBeEliminatedStrict` does (`rule_eliminate_projection.go:60`)
  and expects both schemas where Go derefs `Schema().Len()`/`Columns`
  (`:77`/`:80-81`); the empty-own-schema early elimination still skips the
  child-schema read exactly as Go's early `return true` does.
- `rule_projection_elimination::apply_schema` mirrors Go
  `BuildLogicalJoinSchema` (`logical_join.go:2243-2261`): direct child
  indexes, expected left schema everywhere except the Inner merge.

Five regressions added (physical/tests.rs and an in-module
`apply_schema_tests`): four panic contracts proven to FAIL pre-fix — three
via stashing the production edits, the apply one via a hand-revert of the
single function — plus the Inner nil-nil merge pin, which passes both ways:

- `a_childless_physical_join_panics_when_building_its_schema_like_go`
- `a_single_child_physical_join_panics_on_the_missing_right_child_like_go`
- `a_childless_projection_panics_when_post_eliminating_like_go`
- `a_childless_apply_join_panics_when_rebuilding_its_schema_like_go`
- `two_schemaless_children_merge_to_no_schema_like_go`

Incident note: while capturing the apply baseline, a stale stash from an
unrelated branch (`hparser-integration` Web3 WIP) was accidentally checked
out over `physical/mod.rs`; the file was restored from HEAD and both edits
re-applied before validation. The stale stash itself was left untouched.

<<<<<<< HEAD
>>>>>>> 989396dc75 (rust: align physical join/projection schema boundaries)
=======
## Follow-up batch: column-pruning and TopN-pushdown walk boundaries

Same comparison source (Go `origin/master` at `a85e0fd5df`; owning files
`rule/rule_column_pruning.go`, `logicalop/logical_limit.go`,
`logicalop/logical_projection.go` byte-identical). Four sites in
`src/logical/rewrite.rs` tolerated malformed subtrees or missing schemas:

- `RebuildFromChild` / `RebuildWithOwnColumns` expect the child schema where
  Go's prune arms index `p.Children()[0]` (`logical_limit.go:87` and the
  TopN/Window arms of the same shape).
- The TopN-below-projection pushdown expects the projection's OWN schema —
  Go passes `p.Schema()` to `ColumnSubstitute`
  (`logical_projection.go:196`), which would deref nil.
- The ID-0 blocked-column check indexes `p.Children()[0].Schema()` where Go
  does (`logical_projection.go:207`) instead of treating a missing child
  schema as "not contained".

Two regressions added to `src/logical/rule_tail_tests.rs`, both proven to
FAIL against the unfixed walk (captured by stashing the production edit):

- `a_childless_limit_panics_when_pruning_columns_like_go`
- `a_topn_over_a_schemaless_projection_panics_when_pushing_down_like_go`

## Follow-up batch: expression-rewriter boundaries

Same comparison source (Go `origin/master` at `a85e0fd5df`, owning file
`pkg/planner/core/expression_rewriter.go` byte-identical). Four sites in
`src/expression_rewriter.rs` refused where Go indexes directly:

- `push_last_schema_column` (Go's repeated
  `ctxStackAppend(plan.Schema().Columns[len-1], plan.OutputNames()[len-1])`
  at `:919`/`:1193`/`:1491`/`:1554`): an empty schema computes `-1` in Go and
  panics on the index; the Rust `checked_sub`/`ok_or(MissingSchema)`/
  `get(last).unwrap_or_default()` refusals are now direct indexes (a nil
  plan schema panics via `expect`, exactly Go's `Columns` deref).
- `find_field_name_from_natural_using_join`: both unary-wrapper walks (the
  main loop and the FullSchema-less Apply arm) index `p.Children()[0]` in Go
  (`:3171`/`:3189`); the Rust `None => return Ok(None)` refusals are now
  direct indexes.
- `build_quantifier_plan` appends to `plan4Agg.Schema()` directly in Go
  (`:963`); the Rust `unwrap_or_default()` is now an `expect`.

Two regressions added to `src/expression_rewriter/tests.rs`, both proven to
FAIL against the unfixed code (captured by stashing the production edit):

- `a_childless_unary_chain_panics_when_resolving_a_natural_join_name_like_go`
- `push_last_schema_column_panics_on_an_empty_schema_like_go`

Not covered here: `resolve_redundant_column_from_natural_using_join_plan`'s
`stack.extend(node.children().first())` — no Go walk counterpart could be
confirmed (`ResolveRedundantColumn` is join-local), so it is left as is.

>>>>>>> 2cb4c6b4f1 (rust: align expression-rewriter child boundaries)
## Follow-up batch: source-shaped adapter parity

The same Go owner also has a small public adapter in
`rust/crates/tidb-planner/src/eliminate_unionall_dual_item.rs`, consumed by
`rust/difftests/planner-tests/tests/eliminate_unionall_dual_item.rs`. A
refreshed inventory covers all 344 tracked planner artifacts (343 Rust
sources plus `Cargo.toml`, 140,507 lines), including 155 test-like sources,
four inline/golden fixture-like artifacts, the aggregate-test build input, and
the absence of platform-specific variants. The companion difftest package has
82 tracked artifacts (81 Rust tests plus `Cargo.toml`, 6,639 lines); its
generated `OUT_DIR/all_tests.rs` is a build artifact, not a checked-in source.
No Go, Bazel, generated output, or platform file changed.

The adapter now follows the source rule's narrow `planChanged` contract:
removing branches from a union that still has a branch returns `false`, while
replacing an all-empty union still returns `true`. Its projection probe also
indexes the first child directly, matching Go's panic boundary instead of a
Rust-only safe `None` fallback. The difftest vectors were updated to expect
the source flag and to pin the childless-projection panic.

Focused regressions in the owning crate are
`dropping_a_branch_from_a_nonempty_union_does_not_set_changed` and
`a_childless_projection_panics_at_the_source_child_access`; both failed on the
pre-fix adapter and pass after the fix. The aggregate difftest target remains
blocked by unrelated stale APIs in `join_reorder_projection_inline.rs`,
`physical_sort.rs`, and `physical_table_reader.rs`; those files were not
changed.

Validation for this follow-up used the Ready Rust scope: the two focused unit
tests pass, `cargo fmt --all -- --check` and `git diff --check` pass, and the
owning crate remains covered by the existing planner Ready evidence above.

Risks are limited to malformed source-shaped trees (which now panic at Go's
direct index) and the returned change flag for valid non-empty unions; no
planner tree or SQL execution path is altered. There is no new allocation or
hot-path traversal cost.

## Audit (no code change): join-reorder cluster

Same comparison source (Go `origin/master` at `a85e0fd5df`; owning files
`joinorder/conflict_detector.go:934`, `rule/rule_join_reorder*.go`,
`expression/schema.go:321`). All four candidate sites verified as parity:
the `merge_schema(Some, Some)` fallbacks are unreachable (Rust
`merge_schema` concatenates for non-nil inputs, exactly Go's `MergeSchema`),
`expr_from_schema`'s empty-schema defaults match Go's nil-range semantics,
and the cartesian-join condition gate reads a schema just set by the
builder. No commit: zero reachable behavior change.

## Follow-up batch: projection `DeriveStats` index boundary

Go `LogicalProjection.DeriveStats` (`logical_projection.go:296`) indexes
`selfSchema.Columns[i]` directly while walking `p.Exprs`; the Rust loop's
`columns.get(i) else break` tolerated a schema shorter than the expression
list. It now indexes directly. One regression proven to FAIL pre-fix
(captured by stashing the production edit):

- `projection_derive_stats_panics_when_the_schema_outgrows_the_exprs_like_go`

Consolidation note: this receipt's earlier sections were authored as nine
per-batch commits on `codex/zcode-parity-sweep`; when landing them on
`hparser-integration` (which had already absorbed an intermediate snapshot
of the same work), the still-missing deltas plus this batch are landed as
one consolidated commit with the full gate set re-run on the merged tree.

## Follow-up batch: aggregation-elimination boundaries

Same comparison source (Go `origin/master` at `a85e0fd5df`, owning file
`rule_aggregation_elimination.go` byte-identical). Three sites in
`src/logical/rule_aggregation_elimination.rs` refused where Go indexes:

- `eliminate_distinct` now mirrors `tryToEliminateDistinct`'s exact shape:
  the child schema is read INSIDE the per-function all-column-args branch
  (`:111`/`:117`), via `expect` (Go's `PKOrUK`/`NullableUK` deref), instead
  of an up-front `first()` refusal that also skipped Go's per-function
  gating.
- The PKOrUK coverage check indexes `agg.Children()[0].Schema()` and derefs
  `PKOrUK` (`:69`); the Rust `first().and_then(...).is_some_and(...)` is now
  a direct index plus `expect`.
- `rewrite_aggregate` reads `Args[0]` unguarded like Go `rewriteExpr`
  (`:196`) instead of returning `Ok(None)` on empty args.

Go's own explicit guard at `:135` (`len(agg.Children()) != 1` before
`hasLimit(agg.Children()[0])`) is preserved — the Rust `is_some_and` there
is parity and stays.

Two regressions added to `src/logical/rule_tail_tests.rs`, both proven to
FAIL against the unfixed rule (captured by stashing the production edit):

- `a_childless_distinct_aggregation_panics_when_eliminating_like_go`
- `a_childless_grouped_aggregation_panics_at_the_covered_check_like_go`

## Follow-up batch: unary-walk and sequence-collapse boundaries

Same comparison source (Go `origin/master` at `a85e0fd5df`). Three
doc-admitted refusals became Go's direct indexing:

- `eliminate_empty_selection`: a tested empty selection now replaces itself
  with `Children()[0]` via direct removal (Go's
  `p.SetChild(idx, sel.Children()[0])`); a tested CHILDLESS selection
  panics, exactly as the Go index would.
- `result_reorder::extract_handle_col`: the unary Selection/Limit chain walk
  indexes `lp.Children()[0]` (Go's walk) instead of returning `None`.
- `push_down_sequence`: both the descend's main-query pop and the Collapse
  arm expect the sequence's LAST child (Go's
  `Children()[ChildLen()-1]` family) instead of keeping a childless
  sequence.

`rule_constant_propagation`'s per-join-type candidate collection was
verified against Go's iteration-based `rule_constant_propagation.go` (no
direct child indexes there) and is left as parity.

Three regressions added to `src/logical/rule_tail_tests.rs`, each proven to
FAIL against the unfixed code (captured by stashing the production edits):

- `a_tested_childless_empty_selection_panics_when_eliminated_like_go`
- `a_childless_sequence_panics_when_collapsing_like_go`
- (the result-reorder walk contract is covered by the same unary-chain
  boundary through `extract_handle_col`)

## Risks

- Correctness: malformed subtrees now panic at the same index Go panics at;
  valid planning trees never hit these boundaries, and all non-index guard
  paths keep their early-return behavior.
- Compatibility: the public rule entry signatures are unchanged; the only
  behavior change on valid input is none — indexes stay in range.
- Performance: direct indexing replaces `Option`-producing `first()` probes
  on the hot rule-entry paths, removing a branch per access.
<<<<<<< HEAD
