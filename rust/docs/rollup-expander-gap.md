# Gap: GROUP BY ... WITH ROLLUP is refused at physical planning

## Reproduction (2026-09-06)

```sql
create table t (g int, v int);
insert into t values (1, 10), (1, 20), (2, 5);
select g, sum(v) from t group by g with rollup order by g;
-- Port: ERR "ROLLUP physical planning is not implemented"
-- Go:   per-group rows plus super-aggregate rows with NULL group keys
```

## State of the tree

The LOGICAL half exists and is complete: `select.rollup` reaches
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

## Plan (feature-sized, queued)

1. Add `PhysicalExpand` carrying the grouping sets and the gid column;
2. lower `LogicalExpand` in the physical builder;
3. implement the grouping executor (`run_rollup_aggregate`): emit one
   result row per grouping set per input group, NULLing the set's absent
   columns via gid;
4. bind GROUPING() to the gid column;
5. pin: `select g, sum(v) from t group by g with rollup` → (1,30), (2,5),
   (NULL,35); plus GROUPING() and ORDER BY interactions.
