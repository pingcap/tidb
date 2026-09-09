# `pkg/planner/core/rule_aggregation_push_down.go` — walk receipt

Comparison source: Go `origin/master` (`f2c346fe4f3` at walk start). 687
lines, 18 functions + the `aggregationEliminateChecker` reuse. The rule was
registered in the Rust `OPT_RULE_LIST` (name `aggregation_push_down`, flag
`PUSH_DOWN_AGG`) with a `None` body — this batch ports the body in full as
`rust/crates/tidb-planner/src/logical/rule_aggregation_push_down.rs`.

## Function inventory mapping

| Go function | Rust location | Status |
| --- | --- | --- |
| `Optimize` | `AggregationPushDownSolver::optimize` (always `planChanged = false`) | ported |
| `isDecomposableWithJoin` | `is_decomposable_with_join` | ported |
| `isDecomposableWithUnion` | `is_decomposable_with_union` | ported |
| `getAggFuncChildIdx` | `get_agg_func_child_idx` | ported |
| `collectAggFuncs` | `collect_agg_funcs` (INDICES into the parent's `agg_funcs`; models Go's shared `*AggFuncDesc` pointers — see module header) | ported |
| `collectGbyCols` | `collect_gby_cols` (+ `add_gby_col`) | ported |
| `splitAggFuncsAndGbyCols` | inlined into `push_down_over_join` (collect + valid gate) | ported |
| `addGbyCol` | `add_gby_col` (UniqueID dedup = Go `Column.Equal`) | ported |
| `checkValidJoin` | `check_valid_join` | ported |
| `decompose` | `decompose` (clone-first ordering: the child keeps the ORIGINAL args/mode, the parent becomes FinalMode over the partial output; NOT-NULL cleared only on the null-generating argument copy) | ported |
| `tryToPushDownAgg` | `try_to_push_down_agg` (all-firstrow refusal, multiway-join refusal, unique-key gby refusal, forced `Constant(0)` group-by, outer-join default-values write incl. Go's nil-on-bail quirk) | ported |
| `getDefaultValues` | `get_default_values` (via `AggFuncDesc::eval_null_value_in_outer_join`; eval error = no default) | ported |
| `checkAnyCountAndSum` | `check_any_count_and_sum` (indexed) | ported |
| `checkAllArgsColumn` | `check_all_args_column` | ported |
| `makeNewAgg` | `make_new_agg` | ported |
| `splitPartialAgg` | `split_partial_agg` (via `final_mode_agg::build_final_mode_aggregation`, Go's `partialIsCop=false`/`isMppTask=false` arms) | ported |
| `pushAggCrossUnion` | `push_agg_cross_union` (arg substitution against the union schema, firstrow built from the ORIGINAL gby with the leading aggregate's mode, unique-key → `ConvertAggToProj` collapse, break-after-first-key) | ported |
| `tryAggPushDownForUnion` | `try_agg_push_down_for_union` (decomposability gate, Complete→Partial1/Final→Partial2 mode rewrite, union schema rebuilt from the first pushed child) | ported |
| `aggPushDown` | `agg_push_down` (the driver; elimination → join arm (`tidb_opt_agg_push_down` gated) → projection crossing (UNgated) → union arm (gated) / partition-union arm (UNgated, Go's asymmetry) → recursion) | ported |
| `util.ResetNotNullFlag` | `reset_not_null_flag` (merged join schema, null-filling side) | ported |
| `BuildKeyInfoPortal` | `LogicalJoin::build_key_info` (pre-existing port, wired at both rebuild sites in Go's order) | reused |
| `tryToEliminateAggregation` (shared checker) | `try_to_eliminate_aggregation` (oldAggEliminationCheck made an explicit parameter; strict gate = `check_can_convert_agg_to_proj`, the may-null inner-side refusal) | ported |
| `CheckCanConvertAggToProj` | `check_can_convert_agg_to_proj` (first Rust port of this helper — only reachable via the push-down rule's strict re-check) | ported |
| `ConvertAggToProj` / `rewriteExpr` | `convert_agg_to_proj` over the pre-existing `rule_aggregation_elimination::rewrite_aggregate` | reused |

## Gating parity (verified against Go master)

* Join arm: `SessionVars.AllowAggPushDown` (`@@tidb_opt_agg_push_down`,
  default OFF) — threaded end-to-end: session snapshot
  (`tidb-session/src/stmt_ctx.rs`) → `StmtContext::with_allow_agg_push_down`
  (`tidb-executor/src/stmt_context.rs`, default `DEF_OPT_AGG_PUSH_DOWN=false`)
  → `RuleContext.allow_agg_push_down` (`planner_bridge.rs` + `test_context`).
* Projection crossing: unconditional (Go's own else-if is not flag-gated).
* Partition-union arm: unconditional (Go's second else-if is not gated).
* `FlagPushDownAgg` flag bit: set by the plan builder whenever an
  aggregation/distinct exists (pre-existing port in `plan_builder/aggregation.rs`).

## Deliberate Go quirks preserved

1. `tryToPushDownAgg` writes `join.DefaultValues` even when it then bails
   (nil = empty vec) — mirrored via `&mut join.default_values`.
2. `decompose` leaves the pushed-down child in the ORIGINAL mode with the
   ORIGINAL arguments; only the parent descriptor becomes FinalMode.
3. The partition-union arm ignores `tidb_opt_agg_push_down`.
4. Go's `Optimize` always reports `planChanged = false`.

## Regression coverage

12 tests in the rule module (`rule_aggregation_push_down.rs::tests`):
both-sides max pushdown with the flag on; single-sum one-side pushdown with
the count/sum cross-side block (Go `checkAnyCountAndSum`); flag-off no-op;
left-outer-join default values ([NULL, NULL] per Go's
`evalNullValueInOuterJoin4Count` over a schema column) with the NOT-NULL
cleared final argument; unique-key group-by refusal; avg join-arm refusal;
projection crossing with the flag off (substitution + projection dropped);
side-effect (`sleep`) crossing refusal; union two-child partial pushdown with
per-child column substitution; `var_samp` union refusal; partition-union
pushdown ignoring the flag; rule name.

## Validation

* `cargo test -p tidb-planner --lib` — 937 passed, 4 failed. The 4
  (`physical::tests::cached_plan_rebuilds_*`) fail IDENTICALLY on the bare
  tip (verified by stash) — pre-existing environment failure, not this batch.
* `cargo test -p tidb-executor --lib` — 1075 passed, 137 failed, identical
  counts on the bare tip (verified by stash) — pre-existing baseline.
* `cargo test -p tidb-session --lib` — 1403 passed, 257 failed vs the bare
  tip's 1402/258 (verified by stash) — no regression; one flaky test flipped
  to passing.
* `cargo clippy -p tidb-planner` — no warnings in the new/edited files
  (`too_many_arguments`/`get_first`/`large_enum_variant` addressed in place).

## Conclusion

The AggregationPushDownSolver gap is closed: the rule body, the session-var
gate plumbing, and the strict elimination re-check helper are all ported and
pinned. The remaining `None` bodies in `rule.rs` are DecorrelateSolver,
CorrelateSolver and the FullTextIndexResolver family (documented
non-served/queued surfaces).
