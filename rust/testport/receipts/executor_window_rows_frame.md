# `pkg/executor/windows` — WindowExec over a ROWS frame

Comparison source: Go `pkg/planner/core/operator/physicalop/physical_window.go`
(`ExhaustPhysicalPlans4LogicalWindow`, `ExplainInfo`), `pkg/planner/core/task.go`
(`attach2Task4PhysicalWindow`), and `pkg/executor/windows/`
(`builder.go::Build`, `window.go::WindowExec`, `aggfuncs/row_number.go`).
No Go source was edited.

## Go behavior (the oracle)

`ExhaustPhysicalPlans4LogicalWindow` builds one root `PhysicalWindow` whose
child property requires `PartitionBy ++ OrderBy` (with `CanAddEnforcer`), and
refuses unless the required property is a prefix of it. `Attach2Task` converts
the child to a root task. The executor buffers the child, splits it into
contiguous partitions by `PARTITION BY` key equality, and for every output row
evaluates each window function over its frame; `row_number` returns the row's
1-based position in its partition. `EXPLAIN` renders
`<funcs> over(partition by ... order by ... <frame>)`.

## The Rust gap

`LogicalPlan::Window` had no dispatcher arm at all:
`exhaustPhysicalPlans over Window is not ported to the dispatcher`. There was
no `PhysicalWindow`, no `attach2_task` arm, no explain arm and no executor, so
`select 1+1, row_number() over() num from t` and
`sum(v) over (partition by p order by o rows between 0 preceding and 0
following)` both failed before planning.

## Change

* `tidb-planner`: `PhysicalWindow` (`window_func_descs`, `partition_by`,
  `order_by`, `frame`), `exhaust_physical_plans_4_logical_window` (root only;
  the TiFlash/MPP arm is absent with that tier), the dispatcher arm, the
  `attach2_task` arm (convert-then-attach), the `clone_shallow` arm and the
  plan-cache expression binder.
* `tidb-executor`: `explain::window_info` renders Go's
  `<funcs> over(...)`; `hash_agg::AggFunc::window_frame_value` folds one frame
  into a fresh `AggState` and finishes it through the existing
  `finish_agg_value`; new `window.rs::WindowExec` buffers the child, computes
  contiguous partitions, evaluates the ROWS frame per row (`CurrentRow`,
  `Unbounded`, `Offset { num, preceding }`) and emits the child row followed by
  each window value; `physical_builder::build_window` builds it, refusing a
  RANGE/GROUPS frame by name.

Narrowings: RANGE/GROUPS frames, the pipelined/shuffle window variants, and
TiFlash/MPP windows are refused or absent by name.

## Regressions

* `tests_executor_suite_statements_source::column_name_resolution`
  (`select 1+1, row_number() over() num from t`) — passed after; failed before
  with `Unsupported("exhaustPhysicalPlans over Window is not ported to the
  dispatcher")`.
* `tests_executor_suite_statements_source::issue52984_named_window_self_frame_runs_repeatedly`
  (`sum(v) over w ... rows between 0 preceding and 0 following limit 10`) —
  passed after; same pre-fix failure.

```text
cargo test -p tidb-executor --lib -- --test-threads=1 column_name_resolution
cargo test -p tidb-executor --lib -- --test-threads=1 issue52984
# ok after; both failed before

cargo test -p tidb-executor --lib -- --test-threads=1
# 1258 passed; 4 failed; both window tests left the baseline set, no additions

cargo test -p tidb-planner
# 1003 + 268 + 6 + 3 passed; 0 failed

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
# clean
```
