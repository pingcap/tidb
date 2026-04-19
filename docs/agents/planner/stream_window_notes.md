# Stream Window Notes

## 2026-04-10: First demo path for ordered-input window execution (issue #66906)

Background:
- The motivating query shape was `row_number() over (partition by waybill_no order by apply_time desc)` on top of an access path that can already provide `(waybill_no, apply_time)` order.
- The immediate goal was not full window/index integration. The goal was to prove that planner and executor can preserve and use an existing order instead of always falling back to `Sort + Window`.

Scope of the demo:
- Add a root-only `StreamWindow` physical operator.
- Only enumerate it when the child naturally provides the required `PARTITION BY + ORDER BY` property.
- Do not allow a sort enforcer to synthesize that property.
- Limit the first implementation to a single `row_number()` window function.
- Keep MPP unsupported in this first step.

Implementation choice:
- Add `PhysicalStreamWindow` in the same code path that currently enumerates `PhysicalWindow`.
- Reuse the existing child property derivation from window partition/order keys, but set `CanAddEnforcer=false` for the stream variant.
- Keep ordinary `PhysicalWindow` enumeration unchanged so unsupported cases still fall back cleanly.
- Add an explicit `StreamWindowExec` type in executor builder, but reuse the existing `PipelinedWindowExec` implementation underneath instead of introducing a second execution algorithm in the first step.

Why not build a brand new executor first:
- The first risk to reduce was planner/executor end-to-end viability, not executor specialization.
- `PipelinedWindowExec` already has the execution model closest to ordered-input window evaluation.
- Reusing it keeps the initial diff small and makes it easier to isolate planner-property issues from executor-correctness issues.

Testing choice:
- Put the demo directly in `pkg/planner/core/casetest/windows/testdata/window_push_down_suite_*`.
- Extend the window casetest helper so non-query setup statements can live in testdata instead of as ad hoc `MustExec` calls in the test body.
- Keep the demo table simple: `stream_window_t(a, b, c, key idx_ab(a, b))`.

Covered cases in the demo:
- `row_number() over(partition by a)` can use `StreamWindow`.
- `row_number() over(partition by a order by b)` can use `StreamWindow` with `idx_ab(a, b)`.
- `rank() over(partition by a order by b)` still falls back to ordinary `Window`.
- `row_number() over(partition by a order by c)` still falls back because the available order does not satisfy the requested `ORDER BY`.

Development takeaways:
- The first useful boundary is not "window supports index" in general. The useful boundary is "window can consume an already ordered child without paying an extra sort".
- Keeping `StreamWindow` root-only and `row_number()`-only makes failures easier to interpret. Once the plan shape and executor path are stable, support can expand incrementally.
- For this topic, casetest golden files are more valuable than one-off assertions in Go test bodies because the exact plan shape is part of the feature contract.

Likely next steps:
- Broaden function coverage beyond `row_number()` only after validating which functions can safely reuse the same ordered-input executor path.
- Decide whether `StreamWindowExec` should remain a thin wrapper over `PipelinedWindowExec` or eventually become an independent executor.
- Explore whether some `row_number() = 1` patterns can be rewritten further into a latest-row access path rather than only removing `Sort`.

## 2026-04-10: First working `IndexJoin + StreamWindow` inner path

Background:
- The next target after the root-only demo was to let `row_number()` participate in index nested loop join on the inner side.
- The initial motivating shape was `join (select ..., row_number() over(partition by k order by t) as rn from inner) x on outer.k = x.k where x.rn = 1`.

Scope of this step:
- Keep the planner admission narrow: only allow `LogicalWindow` on index-join inner side when it can become `StreamWindow`.
- Keep MPP unsupported.
- Treat this as the first executor integration step for window-on-index-join, not full general support for arbitrary window functions on arbitrary inner plans.

Implementation choice:
- Allow `LogicalWindow` in `admitIndexJoinInnerChildPattern` only when `EnableINLJoinInnerMultiPattern` is on and `CanUseStreamWindow(...)` is true.
- Reuse the existing `PhysicalStreamWindow` plan node instead of introducing a join-specific window operator.
- Add a `dataReaderBuilder` branch for `PhysicalStreamWindow` so index join inner workers can build it directly.

Executor pitfall discovered:
- `dataReaderBuilder` does not follow the normal recursive `Open()` contract.
- In the index-join inner path, child executors are built bottom-up and each layer is expected to perform a self-initialization step only; children are already prepared by the recursive builder.
- `SelectionExec`, `ProjectionExec`, `HashAggExec`, and `StreamAggExec` already follow this pattern via `open(...)` or `OpenSelf()`.
- The first `StreamWindow` attempt incorrectly used recursive `exec.Open(...)`, which reopened the already-built child chain and produced an empty inner result at runtime.
- The minimal fix was to add `PipelinedWindowExec.OpenSelf()` and use that from `buildStreamWindowForIndexJoin(...)`.

Development takeaways:
- For new operators under `dataReaderBuilder`, the key question is not just "can this executor run?" but also "does it need recursive open or self-open?"
- The builder-side contract matters as much as the planner admission rule. A correct plan shape can still fail silently if the executor is initialized with the wrong opening semantics.
- The fastest way to localize these failures was:
  - first prove standalone `StreamWindow` works,
  - then prove `Selection(StreamWindow(...))` works,
  - then inspect index-join inner execution separately.

Validation commands used in this step:
- `go test ./pkg/executor/join/test/indexjoin -run TestIndexJoinWithStreamWindowInner --tags=intest`
- `go test ./pkg/planner/core/casetest/windows -run TestWindowPushDownPlans --tags=intest`

Validation commands used during the demo:
- `go test ./pkg/planner/core/casetest/windows -run TestWindowPushDownPlans -record --tags=intest`
- `go test ./pkg/planner/core/casetest/windows -run TestWindowPushDownPlans --tags=intest`

## 2026-04-10: First `row_number() <= K` pruning on index-join inner side

Background:
- After `IndexJoin + StreamWindow` itself worked, the next tempting step was to replace `Selection(StreamWindow(...))` with a plain `Limit`.
- That direction is wrong on index-join inner execution because the inner reader is built for a batch of lookup contents, not one outer row at a time.

What failed and why:
- A plain `Limit 1` on the inner side only keeps the first row of the whole lookup batch.
- For a batched inner probe over keys like `[1, 2, 3]`, this returns only the first partition's first row instead of one row per partition.
- The bug is not in `Limit` itself. The mismatch is semantic: ordinary `Limit` is global, while `row_number() <= K` under `partition by join_key` needs a per-partition bound.

Implementation choice for this step:
- Do not force a planner-wide `PartitionTopN` rollout yet.
- Keep the physical plan shape unchanged and add a targeted executor-side rewrite in `dataReaderBuilder`.
- Detect `Selection -> StreamWindow(row_number)` with an upper-bound predicate on the window result column.
- Replace only the child executor path with a specialized stream executor that:
  - reads the already ordered child,
  - emits only the first `K` rows of each partition,
  - materializes the `row_number` result column for the emitted rows,
  - still leaves the original `SelectionExec` on top so equality predicates like `rn = 2` remain correct.

Why keep `SelectionExec`:
- `rn <= K` can be satisfied entirely by the specialized executor, but `rn = K` cannot.
- Leaving `SelectionExec` above the new child keeps the rewrite local and correct for both `rn <= K` and `rn = K`.
- The new executor only reduces the amount of data and window work; it does not try to change the logical predicate semantics.

Development takeaways:
- On index-join inner paths, "per-partition" almost always means "per lookup key group in one batched execution", not "per query".
- A local executor fusion is a safer first step than forcing a planner-wide partition-aware `TopN` contract before root/coprocessor semantics are aligned.
- Cross-chunk correctness matters here. Tests must force tiny chunk sizes; otherwise a bug that resets partition rank at chunk boundaries can pass accidentally.

Validation commands used in this step:
- `go test ./pkg/executor/join/test/indexjoin -run TestIndexJoinWithStreamWindowInner --tags=intest`
- `go test ./pkg/planner/core/casetest/windows -run TestWindowPushDownPlans --tags=intest`
- `go test ./pkg/planner/core/casetest/rule -run TestPushDerivedTopnFlash --tags=intest`

## TODO: Unify `StreamWindow` pruning with partition-aware `TopN/Limit`

Background:
- Commit `e8ae09d88e8b990e999f3474152c2d642de9bf61` added a more general partition-aware `TopN/Limit` path for rank-like semantics.
- The current `StreamWindow` work still uses a local executor-side fusion in `dataReaderBuilder` via `PartitionTopNWindowExec`.

What should be optimized later:
- Move the current `Selection -> StreamWindow(row_number)` upper-bound recognition into a planner-visible physical contract instead of keeping it as a builder-only rewrite.
- Reuse the partition-aware `TopN/Limit` machinery where possible so `rank()` / `dense_rank()` can share the same pruning primitive, especially for tie-preserving boundaries.
- Keep the distinction clear:
  - `row_number() <= K` can be pruned by a hard per-partition row bound,
  - `rank() <= K` needs tie-aware pruning and cannot reuse the same executor blindly.

Why this is not merged yet:
- `PartitionTopNWindowExec` currently materializes the window result column while pruning, which is necessary to preserve predicates such as `rn = K` above the executor.
- The existing rank-topn path is primarily a partition-aware `TopN/Limit` capability and does not by itself produce window result columns.
- A direct replacement would therefore lose logical information even if the pruning direction is similar.

Expected end state:
- Planner recognizes `Selection(rn/rank upper bound) -> StreamWindow -> ordered child`.
- Planner inserts an explicit partition-aware pruning operator rather than relying on a local `dataReaderBuilder` special case.
- Executor keeps window result materialization and pruning responsibilities separated cleanly enough that `row_number`, `rank`, and `dense_rank` can evolve on one shared contract.
