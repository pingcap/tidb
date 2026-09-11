# Historical Gap: ROLLUP Physical Execution

## Reproduction (2026-09-06)

```sql
create table t (g int, v int);
insert into t values (1, 10), (1, 20), (2, 5);
select g, sum(v) from t group by g with rollup order by g;
-- Port: ERR "ROLLUP physical planning is not implemented"
-- Go:   per-group rows plus super-aggregate rows with NULL group keys
```

## Historical State

The logical half exists but is not yet verified end to end: `select.rollup` reaches
`PlanBuilder::build_expand` (plan_builder.rs:3304-3310, Go
`logical_plan_builder.go:4494`), producing `LogicalExpand`
(`plan_builder/expand.rs`; grouping sets, gid columns, GROUPING()
resolution). What is missing:

1. **Physical lowering** — no `PhysicalExpand` in the physical plan enum;
   `planner_bridge.rs:1185-1210` short-circuits with an internal error
   before the logical tree is even built.
2. **Executor binding** — no grouping executor; the expand.rs module doc
   points at `driver/grouping.rs:221 run_rollup_aggregate`, which does not
   exist yet.

## Resolution

The physical path now uses `PhysicalExpand` and serial `ExpandExec`: each input
chunk is projected once per grouping level, then the ordinary aggregation
computes detail/subtotal rows. GROUPING evaluates Expand's GID using
planner-installed metadata. The original ROLLUP and window-over-ROLLUP tests
pass in `/tmp/rollup-final-focused.log`.

The proposed `run_rollup_aggregate` AST shortcut was not Go's execution model
and was never implemented. See [the execution plan](rollup-physical-execplan.md)
for source references, regressions and validation status. These results do not
claim complete Go planner/executor package or TiFlash wire parity.
