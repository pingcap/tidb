# Yannakakis+ Phase 1 Notes

## Scope

- Entry point: `pkg/planner/core/rule_yannakakis_plus.go`.
- Current rewrite scope is intentionally narrower than the paper:
  - top-level `DISTINCT` represented as `LogicalAggregation(first_row(...))`;
  - top-level weighted aggregates represented as `count(constant)`,
    `count(root_col)`, and `sum(root_col)` plus optional
    `first_row(group_by_col)` outputs;
  - top-level duplicate-insensitive `MIN/MAX(column)` represented as one or
    more `min/max(column)` plus optional `first_row(group_by_col)` outputs;
  - all output attribute classes must be dominated by the same leaf relation;
  - child is an acyclic inner equi-join component;
  - no outer join, non-equality join predicate, or generic aggregation yet.

## Why the Scope Is Narrow

- TiDB already rewrites `DISTINCT` into a `LogicalAggregation` whose
  `AggFuncs` are `first_row` and whose `GroupByItems` are output columns.
- TiDB already has distributed partial/final aggregation for single-relation
  `COUNT`; the Yannakakis+ rule therefore only targets join-count cases where
  subtree reduction can shrink intermediate joins.
- `MIN/MAX` are added only for the duplicate-insensitive subset, so the current
  rewrite can reuse interface-level `DISTINCT` reductions without introducing a
  separate multiplicity summary.
- `COUNT(root_col)` is limited to direct column arguments so the rewrite only
  needs root-local null checks, not arbitrary expression remapping.
- `SUM(root_col)` is limited to numeric root columns so the weighted
  contribution stays within the same arithmetic subset already supported by
  TiDB's aggregate type inference.
- Phase 1 only permits remapping to equality-equivalent root columns with
  identical field types; it still avoids general expression remapping.

## Core Invariant

For a join tree node `v` and its parent `p`, let `I(v, p)` be the equality
attribute classes shared by the two sides.

`reduceDistinctSubtree(v, p)` returns a logical plan equivalent to:

- evaluate the original subtree rooted at `v`;
- project to `I(v, p)`;
- remove duplicates.

At the root, `reduceDistinctSubtree(root, nil)` returns the reduced join subtree
without the final root projection because the top-level `DISTINCT` aggregation
still sits above it.

For the weighted-aggregate rewrite, let `summarizeMultiplicitySubtree(v, p)`
denote the grouped summary produced for a non-root node. Its invariant is:

- the output schema contains `I(v, p)` and one multiplicity column `m(v, p)`;
- for every interface assignment `x`, `m(v, p, x)` equals the number of tuples
  in the original subtree rooted at `v` that project to `x`.

## Proof Sketch

1. DISTINCT base case:
   - If `v` is a leaf, `reduceDistinctSubtree(v, p)` is exactly
     `DISTINCT(project leaf to I(v, p))`.
2. DISTINCT inductive step:
   - Assume each child rewrite returns the distinct interface relation promised
     by the invariant.
   - Joining the current relation with each reduced child on the shared
     equality classes preserves exactly the tuples of the original subtree that
     can participate in a full join.
   - Projecting the joined result back to the interface with the parent and
     applying `DISTINCT` re-establishes the invariant for `v`.
3. DISTINCT root:
   - Because all output attribute classes are dominated by the chosen root
     relation, and the top-level `DISTINCT` is remapped only to
     equality-equivalent root columns with identical field types, the original
     result is preserved after child subtrees are reduced to their join
     interfaces.
4. Multiplicity summary base case:
   - If `v` is a leaf, grouping the leaf by `I(v, p)` and aggregating
     `count(1)` yields exactly the multiplicity of each interface assignment.
5. Multiplicity summary inductive step:
   - Assume each child summary already exposes the exact multiplicity for its
     interface with `v`.
   - Joining the current relation with all child summaries reconstructs the
     valid subtree tuples, while the product of child multiplicities gives the
     number of descendant combinations attached to each current row.
   - Grouping by `I(v, p)` and summing that product yields the exact subtree
     multiplicity for each interface assignment.
6. Weighted root aggregates:
   - `count(constant)` at the root becomes `sum(multiplicity)`; scalar count
     rewrites add `ifnull(..., 0)` so empty-input semantics stay unchanged.
   - `count(root_col)` becomes `sum(if(root_col is null, 0, multiplicity))`,
     which preserves SQL's null-insensitive count semantics.
   - `sum(root_col)` becomes `sum(root_col * multiplicity)`; `NULL` root values
     remain `NULL` contributions and are ignored by `SUM`, just as in the
     original join result.
7. MIN/MAX root:
   - Child subtree duplicates can be collapsed to interface-level `DISTINCT`
     because `MIN/MAX` depend only on which root tuples survive the join, not
     on how many descendant witnesses each tuple has.
   - After remapping to equality-equivalent root columns, evaluating the
     original `MIN/MAX` over the reduced root join returns the same result.

The only non-obvious step is synthetic edge selection: the implementation may
join two relations that were not direct neighbors in the original left-deep
join syntax, but only when they share the same equality attribute class after
union-find. This is sound for pure inner equi-joins because equality is
transitive inside the connected component.

## Code Checks That Back the Invariant

- `isDistinctLikeAgg` accepts only the exact `DISTINCT`-style aggregation
  shape.
- `extractWeightedAggInfo` accepts only `count(constant)`, `count(column)`,
  `sum(column)`, and optional `first_row(group_by_col)` outputs.
- `extractMinMaxAggInfo` accepts only `min/max(column)` plus optional
  `first_row(group_by_col)` outputs.
- `collectInnerJoinGraph` rejects anything outside inner equi-joins.
- `buildYannakakisGraph` rejects duplicate equality-class columns inside a
  single relation and rejects disconnected relation sets.
- `deriveAcyclicRelationTree` uses GYO-style elimination and rejects graphs
  that cannot be reduced to one relation.
- `isSupportedYannakakisSumArg` keeps `SUM` inside the numeric subset where the
  weighted arithmetic stays explicit and reviewable.

## What Phase 1 Does Not Prove

- Generic `GROUP BY` with decomposable aggregates.
- `COUNT(expr)` where `expr` is not a direct dominated root column.
- `SUM(expr)` where `expr` is not a direct numeric dominated root column.
- generic `AVG` and other duplicate-sensitive aggregates beyond the weighted
  root-column subset.
- Output remapping when the root representative would change field type.
- Cost-based selection among multiple legal join trees.
- Semijoin reduction with foreign-key or Bloom-filter style extensions.

## Suggested Validation

- Positive regression:
  - `select distinct t1.a from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select distinct t1.a, t3.b from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select count(*) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select t1.a, count(*) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b group by t1.a`
  - `select count(t1.c) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select sum(t1.c) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select t1.a, sum(t3.b) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b group by t1.a`
  - `select min(t1.c) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select t1.a, max(t3.b) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b group by t1.a`
- Negative regressions:
  - cyclic join graph;
  - single-table `count(*)`;
  - grouped count whose output attribute classes are not dominated by one relation;
  - weighted aggregates whose output and argument attribute classes are not dominated by one relation;
  - `min/max` whose arguments and outputs are not dominated by one relation;
  - `DISTINCT` whose output attribute classes are not dominated by one relation.
