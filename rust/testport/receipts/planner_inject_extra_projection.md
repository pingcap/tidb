# `pkg/planner/core/rule_inject_extra_projection.go` — walk receipt

Comparison source: Go `origin/master` (`f2c346fe4f3` at walk start). 348
lines; a PHYSICAL post-optimization (`postOptimize`, `optimizer.go:462`),
not one of the 35 logical rules. Go's `postOptimize` order is
`eliminatePhysicalProjection` → `InjectExtraProjection` → the rest; the
narrow tier replicated only the first half. This batch ports the second.

## Function inventory mapping

| Go function | Rust location | Status |
| --- | --- | --- |
| `InjectExtraProjection` | `inject_extra_projection` (`physical/inject_extra_projection.rs`) | ported |
| `projInjector.inject` | `inject` (children-first recursion, per-operator switch) | ported |
| `injectProjBelowUnion` | `inject_proj_below_union` — the `mpp: false` early return is the only reachable arm on this tier | ported (guarded) |
| `InjectProjBelowAgg` | `inject_proj_below_agg` (hash) + `inject_proj_below_stream_agg` (stream rewrap; Go shares one body) | ported |
| `InjectProjBelowSort` | `inject_proj_below_sort` (bottom proj evaluates scalar order-by items, top proj prunes back to the sort's schema) | ported |
| `TurnNominalSortIntoProj` | `turn_nominal_sort_into_proj` — `only_column=true` drops the node; the non-column shape builds the two pass-through projections. The Rust NominalSort narrows by-items to column `SortItem`s, so Go's scalar-expression loop adds nothing here | ported |
| `coreusage.WrapCastForAggFuncs` | `core_usage::wrap_cast_for_agg_funcs` (pre-existing) | reused |
| `refine4NeighbourProj` (`resolve_indices.go:56`) | `refine_4_neighbour_proj_exprs` + a local `DisjointSet` (Go `disjointset.IntSet`) | ported |

## Gating / wiring parity

* Call site: `planner_bridge.rs::physical_plan_for_logical`, immediately
  after `eliminate_physical_projection`, mirroring Go's `postOptimize`
  order.
* `DisableProjectionPostOptimization` failpoint: test-only seam, not
  reproduced (failpoint harness is not ported on this tier).
* TiFlash `PhysicalTableReader` recursion arm: no counterpart — the tier is
  TiKV-only and its readers carry no TiFlash store type.
* Stats: injected projections take
  `child.StatsInfo().ScaleByExpectCnt(childReqProps[0].ExpectedCnt)`; the
  Rust `scale_by_expect_cnt` also threads the session skew ratio (1.0, the
  same constant the wired search context uses).

## Deliberate Go quirks preserved

1. Constants are skipped as projection inputs but still consumed by the
   aggregate; order-by/group-by dedup runs by expression `Equal` against the
   accumulated projection exprs (`slices.IndexFunc` order).
2. `turn_nominal_sort_into_proj` for `only_column=true` returns the CHILD —
   the nominal sort vanishes from the final plan (Go's EXPLAIN never shows
   it).
3. `inject_proj_below_union` answers unchanged for `mpp: false`.

## Regression coverage

10 tests in the module: scalar agg argument injection + argument rewrite,
column-only agg no-op, group-by expression dedup against accumulated
projection exprs, sort wrapped in two projections for a scalar order-by
item, column order-by no-op, column-only nominal sort disappearance,
expression nominal sort → two pass-through projections, non-MPP union
untouched, stream-agg identity preserved (`tp()` stays "StreamAgg"), and
the neighbour-refinement union-find over duplicated output positions.

## Validation

* `cargo test -p tidb-planner --lib` — 935 passed, 4 failed (the
  `physical::tests::cached_plan_rebuilds_*` set that fails identically on
  the bare tip; stash-verified).
* Clean-worktree comparison against the parent commit
  (`0f413fc16e0`, run from a detached worktree sharing the target dir):
  `tidb-executor` 1075/137 → 1076/136; `tidb-session` 1402/258 → 1406/255.
  No new failures; net flips positive.
* `cargo clippy -p tidb-planner` — no warnings in the new file.

## Conclusion

The `postOptimize` second half is closed for the served tier. The remaining
`postOptimize` steps (`mergeContinuousSelections`,
`eliminateUnionScanAndLock`, `avoidColumnEvaluatorForProjBelowUnion`,
`enableParallelApply`, `handleFineGrainedShuffle`, `propagateProbeParents`,
`countStarRewrite`, `disableReuseChunkIfNeeded`, `generateRuntimeFilter`)
are store/concurrency-tier surfaces outside the narrow tier's model.
