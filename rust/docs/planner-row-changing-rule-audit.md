# Planner audit: rules where a divergence changes the ROW MULTISET

Scope: `rust/crates/tidb-planner` + the live rewrite rules in
`rust/crates/tidb-executor/src/driver` and `.../column_prune.rs`, against
`pkg/planner/core`.

Question asked of every rule: **is there a schema and a query for which our
rule fires when Go's does not (or vice versa), such that the rows returned
differ?** A rule we simply do not implement is a *performance* gap and is
recorded as such, not as a finding.

Nothing in this document was executed. `syspolicyd` on the audit machine
refuses freshly built binaries, so `cargo test`, `gorun` and `goeval` were all
unavailable. Every claim below is read from source on both sides. The
"expected rows" columns are derived from the two implementations' text, not
observed.

---

## 0. The structural fact that shapes the whole audit

`tidb-planner`'s `rule_*`-derived modules are **dependency-closed leaf
transcreations of the Go rule *wrappers*, not of the rules**, and **none of
them is on the live query path**.

* `outer_to_inner_join.rs` transcribed `rule_outer_to_inner_join.go:39` —
  including the fact that the Go wrapper does nothing but delegate to
  `LogicalPlan.ConvertOuterToInnerJoin`, whose null-rejection analysis has no
  Rust counterpart. It was **deleted** (batch52): it was never declared in
  `tidb-planner/src/lib.rs`, so no crate in the workspace ever compiled it —
  proved by appending `compile_error!` to it and building
  `--workspace --tests` clean. A delegation to a delegate nobody wrote, in a
  file nobody compiles, is not coverage.
* Three more never-compiled files went with it in batch52, on the same
  evidence (`compile_error!` appended, `cargo build --workspace --tests`
  clean): `logical_property.rs` (Go's memo `LogicalProperty` over opaque
  identity tokens — this tier has no memo group to hold one),
  `logical_mock.rs` (Go's test-only `MockDataSource.Init`, over an opaque
  `PlanContext` token — this tier has no `BaseLogicalPlan`), and
  `wrap_cast.rs`. `wrap_cast.rs` is the sharpest case: Go's
  `WrapCastForAggFuncs` is a five-line mode gate whose whole content is the
  delegate `baseFuncDesc.WrapCastForAggArgs`, and that delegate's rules —
  the `noNeedCastAggFuncs` set keyed BY FUNCTION NAME, the `RetTp.EvalType()`
  dispatch, the per-argument `TypeNull` skip, the `LEAD`/`LAG`/`NTH_VALUE`
  second-argument skip — could not be expressed in the file at all: its own
  constructor `new_agg_function` passes `""` for the name.
  `crates/tidb-planner/src/configured_catalog.rs` is NOT in this class and was
  kept: it is compiled, through `#[path = "configured_catalog.rs"]` in
  `read_only_scan.rs`, and `lib.rs`'s `pub use read_only_scan::configured_catalog`
  re-exports it rather than shadowing a rival.
* A workspace-wide sweep for `.rs` files named by no `mod` declaration and no
  `#[path]` attribute finds nothing else: the remaining hits are `include!`d
  generated tables (`tidb-datatype`'s `charset_data/*`, `encoding_labels.rs`),
  Cargo's own auto-discovered `src/bin/*` targets, and a fuzz target.
* `topn_push_down.rs:31` likewise wraps `rule_topn_push_down.go` and delegates.
* `max_min_elimination.rs`, `column_pruning.rs`, `condition_to_dual.rs`,
  `rule_set.rs` model the *legality classification* over caller-supplied
  normalised metadata; no caller supplies it.
* `predicate_partition.rs::partition_predicates` has **zero callers outside
  its own file** (grep across `rust/crates`).

The rules that actually decide rows for a live `SELECT` are, in the order the
driver runs them:

| live rule | file | Go counterpart |
| --- | --- | --- |
| column pruning | `tidb-executor/src/column_prune.rs:135`, `:325` | `rule/rule_column_pruning.go` |
| access-path / range choice | `tidb-executor/src/driver/access.rs` | `DetachCondAndBuildRangeForIndex`, `findBestTask` |
| scan filter push-down | `driver/access.rs:311` `negotiate_scan_filter` | `rule_predicate_push_down.go` (DataSource arm) |
| WHERE-equality push into a join | `driver/predicate_push_down.rs:102` | `LogicalJoin.PredicatePushDown` (inner arm) |
| LIMIT push into a scan | `driver/access.rs:1453` `scan_limit_cap` | cop-task `Limit` / `TopN` |
| equi-key split | `hash_join.rs:170` `split_equi` | `extractOnCondition` → `EqualConditions` |

So: **an audit "for wrong rows" over `tidb-planner` alone finds nothing,
because `tidb-planner` runs nothing.** Both surfaces are covered below.

---

## 1. Findings, ranked by consequence

### F1 — LATENT WRONG ROWS: `predicate_partition.rs::route_for` is join-type-blind and ON/WHERE-blind

* Rust: `rust/crates/tidb-planner/src/predicate_partition.rs:161-185`
* Go: `pkg/planner/core/operator/logicalop/logical_join.go:171-278`
  (`LogicalJoin.PredicatePushDown`), `:1586` (`extractOnCondition`)

`route_for` decides a predicate's destination from **column dependency
alone**:

```rust
match (left, right) {
    (true, true)  => PredicateRoute::JoinResidual,
    (true, false) => PredicateRoute::LeftPushdown,
    (false, true) => PredicateRoute::RightPushdown,
    ...
}
```

It takes no join type and no ON/WHERE flag. Go's answer to the same question
is a five-way switch on `p.JoinType`, and the two axes it uses are exactly the
two this function does not have:

* `LeftOuterJoin` (`logical_join.go:196-215`): WHERE predicates are extracted
  with `p.extractOnCondition(predicates, true, false)` — **derive left only**.
  A right-only WHERE predicate is *returned upward* (`ret = append(ret,
  rightPushCond...)`), never pushed into the right child. Conversely the ON
  condition is pushed **right only** (`DeriveOtherConditions(..., false,
  true)`), because a left ON condition would filter preserved rows.
* `RightOuterJoin` (`:216-232`): the mirror image.
* `AntiSemiJoin` (`:258-277`): `leftCond = leftPushCond` only, and Go
  explicitly refuses to derive `is not null` for the anti side, with three
  worked counter-examples in the comment.
* `AntiLeftOuterSemiJoin` / `LeftOuterSemiJoin` (`:173-179`): predicate
  simplification of `OtherConditions` is **disabled entirely**, citing
  pingcap/tidb#9051 — the `IN (subq)` equality lives in `OtherConditions` and
  simplifying it "would cause wrong results".

Concrete divergence, if any caller ever routes on `route_for`'s answer:

```sql
CREATE TABLE t1 (a INT, b INT);
CREATE TABLE t2 (a INT, b INT);
INSERT INTO t1 VALUES (1,1),(2,2);
INSERT INTO t2 VALUES (1,1);

SELECT t1.a, t2.a FROM t1 LEFT JOIN t2 ON t1.b = t2.b WHERE t2.a IS NULL;
```

`t2.a IS NULL` binds only to the right child, so `route_for` answers
`RightPushdown`. Pushed into `t2`, it selects no `t2` row, the left join
null-extends every `t1` row, and the query returns `(1,NULL),(2,NULL)`.
Go returns `(2,NULL)` — the classic anti-join idiom. **Two rows vs one, no
error.**

Status: **latent, not live.** `partition_predicates` is dead code today; the
live path (§2) never asks this question. This is filed at rank 1 because the
function's public API *invites* a caller to use its answer, and its doc
comment describes the route as a "conservative dependency route" — which is
true of the dependency analysis and false of the pushdown decision it is
named for.

Applied here: a type-level warning on `PredicateRoute` and on
`partition_predicates` naming the two missing inputs, the Go switch, and the
query above. Doc-only — the behaviour is unchanged.

Recommended real fix (**not applied** — it is an API change, not a one-liner):
give `PredicateRoute` a construction that cannot be built without a join type
and an ON/WHERE origin, so the join-type-blind route is unrepresentable rather
than merely documented. Applying a partial gate without the caller that
consumes it would be guessing at the contract.

### F2 — RANK 3: the null-rejection outer→inner conversion does not exist

* Rust: `rust/crates/tidb-planner/src/outer_to_inner_join.rs:50` — wrapper only.
* Go: `pkg/planner/core/operator/logicalop/logical_join.go:306`
  (`simplifyOuterJoin`) and `pkg/planner/util/null_misc.go:98`
  (`IsNullRejected`).

Does its absence change an answer? **No.** Not converting an outer join to an
inner one leaves the null-extended rows in the pipeline, and the `WHERE` above
still removes them. Same multiset, more work. Recorded here because scope item
1 asks for it specifically and because the *legality condition* is what a
future implementation must reproduce, and it is easy to get wrong:

* `simplifyOuterJoin` `logical_join.go:311-316`: the "inner" table is
  `children[1]` for a LEFT join and `children[0]` for a RIGHT join — the
  variables are named `innerTable`/`outerTable` and are **swapped** for
  LeftOuterJoin. A transcreation that reads them positionally inverts the rule.
* `:323-325`: a predicate that refers **only** to the outer table's schema is
  skipped (`ExprFromSchema(expr, outerTable.Schema())` → `continue`) *before*
  the null-rejection test. Dropping that guard converts an outer join on a
  predicate that says nothing about the inner side.
* Null-rejection is proved against the **inner** schema only
  (`null_misc.go:98`), after `PushDownNot`, and it proves `nonTrue` — "cannot
  be TRUE once every inner column is NULL" — not "is FALSE". `NULL` counts as
  rejecting; `UNKNOWN` does not become `TRUE`.
* `null_misc.go:103-140` folds null-hiding wrappers (`COALESCE(t2.a,2) > 2`
  nullifies to `2 > 2`, which is nonTrue) but disables that fold for a
  `Constant.DeferredExpr`, so plan-cache-deferred values are proved
  symbolically only.

### F3 — RANK 4 (cost): `order_is_index_order` refuses an equal-prefix skip Go allows

* Rust: `rust/crates/tidb-executor/src/driver/access.rs:1490-1512`
* Go: `matchIndicesProp` /
  `pkg/planner/core/operator/logicalop/logical_index_scan.go`

`order_is_index_order` zips the `ORDER BY` items against the index's column
list **from position 0**. For `KEY idx(a,b)` and
`SELECT ... WHERE a = 1 ORDER BY b LIMIT 3`, item `b` is paired with `a`'s
offset, the match fails, and no cap is pushed. Go discharges the property
because the equal-prefix column is constant across the single range.

Rows are identical — the `SortExec` at `driver.rs:784` is built
unconditionally and is never elided — so this is purely a lost push-down.

---

## 2. Verified-equal list (live path)

These were checked against the Go legality condition and found to agree, or to
be sound by a structural argument. This list is what makes the next audit
cheap: it says what does *not* need re-reading.

### 2.1 WHERE-equality push into a join — `driver/predicate_push_down.rs`

* Gate, `driver/from.rs:1112`: pushed **only** when
  `join.tp == JoinType::Cross && !coalescing`. Outer joins are refused at the
  node, never reasoned about.
* Eligibility, `predicate_push_down.rs:73-93`: only a bare `Expr::Binary(Eq,
  Column, Column)` survives `offered_conjuncts`. No subquery, no function, no
  cast — so the two hazards Go screens by name
  (`expression.IsMutableEffectsExpr`, `CheckNonDeterministic`) are
  unrepresentable rather than checked.
* Placement, `:102-125`: a conjunct is taken by the lowest node whose two
  children land its columns on opposite sides of `left_width`. An unresolvable
  column (an outer-query correlation) is left alone.
* Redundancy: the conjunct is **copied**, not moved — it still runs in the
  `WHERE` above. For an inner join a condition is a filter over the same pairs,
  so `WHERE(J_c(a,b)) = WHERE(J(a,b))` with `J_c ⊆ J`.
* **The case worth stating, because it looks unsound and is not**: `offered`
  *is* propagated into a `Cross` node nested under an outer join
  (`from.rs:962-968`, `from.rs:330`). `SELECT * FROM t1 LEFT JOIN (t2, t3) ON
  t1.a = t2.a WHERE t2.b = t3.b` does push `t2.b = t3.b` into the inner cross
  join. This is sound: a bare `col = col` in `WHERE` is null-rejecting on both
  its columns, so every row the push causes to be null-extended instead of
  dropped is then dropped by that same `WHERE` (`NULL = NULL` is NULL). It is
  the null-rejection argument arriving by a different road.
* `offered` does **not** cross a derived-table boundary: `from.rs:333-348`
  calls `build_derived_source` without it, and the subquery computes its own
  from its own `WHERE` (`driver.rs:319`). A push into a derived table with
  `GROUP BY`/`LIMIT`/`DISTINCT` is therefore unrepresentable.
* `left_width` is initialised from `left_scope.width()` (`from.rs:1006`) and
  reassigned only when the prune actually narrows (`:1078`), so the
  side-classification and the equi-key offsets cannot go stale.

### 2.2 Equi-key split — `hash_join.rs:97-109`, `:183-210`

A pushed conjunct becomes a hash key, which is the one way this push-down
could *drop* rows: a hash key must be injective for `=`. `KeyClass::of`
refuses whenever `left.eval_type() != right.eval_type()`, and refuses
`Datetime`/`Timestamp`/`Duration`/`Json`/`VectorFloat32` outright. So
`t1.int_col = t2.varchar_col` never becomes a key — it stays in `other cond:`
and is evaluated with full comparison semantics. Signed/unsigned Int share one
`i128` number line; `-0.0`/`0.0` share a key and `NaN` gets none; a string key
carries the `eq`'s **derived** collation. `key_part` returns `Err` rather than
guessing for an out-of-class datum, on the stated grounds that a guess drops
rows silently.

### 2.3 LIMIT push into a scan — `driver/access.rs:1453-1480`

Guard list: `residual_where.is_some() || select.distinct ||
select.having.is_some() || select_has_window(select)`, then `ORDER BY` must be
discharged by the access path.

* `GROUP BY` and select-list aggregates are **not** in that list, and do not
  need to be: `driver.rs:370-384` computes `is_aggregate` (Go's
  `detectSelectAgg`: GROUP BY, or any field/HAVING/ORDER BY expression
  *containing* an aggregate) and returns into `run_aggregate_select` **before**
  `offer_scan_limit` at `:439` is reached. `SELECT count(*) FROM t LIMIT 1`
  therefore cannot cap the scan at one row.
* A multi-table `FROM` yields a `JoinExec`, whose `table_access()` is `None`,
  so `accept_scan_limit` is never offered across a join.
* The `ORDER BY` discharge (`:1490`) requires a **single** range (several
  ranges are each in index order but their concatenation is not), all items
  ascending (the cursor is forward-only), and each item to be a bare column at
  the matching index offset.
* Prefix indexes are correctly excluded: `IndexAccessOrder.column_offsets` is
  `KvIndex::ordered_column_offsets()`
  (`kv_table/table_meta.rs:179-191`), which `take_while`s the leading key
  parts with `UNSPECIFIED_LENGTH`. A key part with a declared length is cut,
  so `ORDER BY a` over `KEY idx(a(3))` can never discharge — matching Go's
  `matchIndicesProp`, which rejects the property at the first sort item on a
  length-carrying key part.
* The `SortExec` at `driver.rs:784` is built whenever `order_by` is non-empty,
  unconditionally. The cap is therefore an optimisation over an
  order-preserving prefix, never a replacement for the sort.

### 2.4 Column pruning — `column_prune.rs:135`, `:325`

`collect_statement_columns` walks select fields, `WHERE`, `HAVING`,
`GROUP BY`, `ORDER BY`, and both `LIMIT` bounds. Nothing an aggregate or an
`ORDER BY` still needs can be pruned. Every refusal is a *shape* refusal taken
up front (`:139-160`): more than one table (with the two-base-table widening),
`WITH`, `ROLLUP`, window clause, `VALUES`, `INTO OUTFILE`, locking clause,
non-`Table` FROM node, any wildcard. `collect_expr_columns` matches
`tidb_ast::Expr` exhaustively with no wildcard arm, so a new AST variant is a
compile error rather than an unvisited subtree; an unresolvable column returns
`None` and the full-width path raises the proper MySQL error. The narrowing
happens **before** any expression is built, so there is no offset to remap.

### 2.5 Constant-condition → TableDual — `condition_to_dual.rs:51`

Matches `pkg/planner/core/operator/logicalop/expression_util.go:24-52`
statement for statement, including the two non-obvious parts:

* a `Null` constant **anywhere** in the list produces a dual regardless of
  list length (Go's loop at `:29-38` runs before the `len(conds) != 1` test at
  `:39`), and
* `IsConstFalse` (`:54-65`) treats a NULL constant as false and a
  conversion error as *not* false — which is what `ConditionTruth::Null` /
  `ConditionTruth::ConversionError` model.

### 2.6 Correlated subqueries: `NOT IN` with NULL, and the empty inner side

* Rust: `tidb-executor/src/driver/subquery.rs:399-437`, `:456-470`
* Go: `buildSemiApply` / `rule_decorrelate.go`

There is **no decorrelation** in this tier. Every correlated subquery is
evaluated per outer row (an Apply), which is the semantics-preserving route by
construction — an unsound decorrelation is not reachable. The two NULL cases
scope item 3 names both land right:

* `NOT IN` over a **non-empty** result keeps an ordinary `Expr::In { not:
  true }` and gets the three-valued answer: `x NOT IN (1, NULL)` with `x = 2`
  is NULL, so the row is filtered — MySQL's answer.
* `NOT IN` over an **empty** result folds to the constant `1`
  (`in_list_expr:459-471`), true for every `x` including NULL. That is the
  semi-join reading, and it is the case an `Expr::In` with an empty list could
  not express.
* `EXISTS` folds to `!rows.is_empty() != not`.
* `> ANY` over an empty list is FALSE, `> ALL` over an empty list is TRUE,
  both for a NULL left operand (`any_all_expr:477-497`).

Side note, out of this audit's scope but adjacent: the fold re-serialises each
inner `Datum` into an AST literal (`driver.rs:902`). Non-UTF-8 bytes become
`Expr::Hex` rather than a lossy string, and `Datum::UInt` above `i64::MAX`
becomes a plain `Expr::Int` decimal string — whether the rewriter re-reads
that as a u64 is an expression-crate question, not a planner one.

---

### 2.7 Loose projection elimination — `projection_elimination.rs:83-90`

`can_eliminate_loose` is `!proj4_expand && all exprs are Column`, which is
`canProjectionBeEliminatedLoose`
(`pkg/planner/core/rule_eliminate_projection.go:32-48`) statement for
statement, `Proj4Expand` guard included. Not live (no caller supplies a
`LogicalProjectionShape`), so it cannot prune a column an aggregate or
`ORDER BY` needs.

## 3. Rules not implemented at all — performance gaps, not correctness

Grep-verified absent from `rust/crates` (only the sysvar *names* exist):

| Go rule | absence changes rows? |
| --- | --- |
| `rule_aggregation_push_down.go` (incl. the COUNT→SUM rewrite) | No. The aggregate runs once, at the top. There is no pushed COUNT to mis-rewrite. |
| `rule_decorrelate.go` | No. Apply is always evaluated per outer row (§2.6). |
| `rule_semi_join_rewrite.go` | No. |
| `rule_join_elimination.go` | No. An un-eliminated join produces the same multiset; elimination is the risky direction. |
| `rule_join_reorder*.go` | No. |
| `rule_aggregation_elimination.go` | No. |
| `rule_max_min_eliminate.go` | No — `max_min_elimination.rs` classifies eligibility but nothing consumes it. |
| `expression/constant_propagation.go` | No. See §4. |

## 4. Settling the constant-propagation question

Scope item 6 asks whether we perform equality constant propagation, given that
`WHERE a = <string>` warns 4 times in TiDB and 2 here.

**We do not perform it, anywhere.** `grep -rn
"propagate_constant\|PropagateConstant\|constant_propagat" rust/crates
--include='*.rs'` returns **zero hits**. There is no partially-working
implementation to debug.

It is also not the cause of the 4-vs-2 warning gap, and the Go source rules it
out directly: `propConstSolver.propagateConstantEQ`
(`pkg/expression/constant_propagation.go:357-380`) substitutes a
`column = constant` into the **other** conditions of the same list. With a
single conjunct there is no other condition, so the loop's
`ColumnSubstitute` never runs on anything and no additional conversion — and
therefore no additional truncation warning — can be produced. The extra two
warnings must come from Go evaluating the same string→int conversion in more
places (range building and selectivity estimation both re-evaluate the
constant), not from constant propagation. Counting Go's warning-emission sites
for that shape is the follow-up; it was not done here.

Two legality conditions to carry forward if propagation is ever implemented,
both easy to miss:

* `propagateColumnEQ` (`constant_propagation.go:398-420`) requires the two
  columns to have the **same collation** and neither to be a **hybrid** type
  (ENUM/SET/BIT). Propagating `a < 3` across `a = b` when the collations
  differ changes rows.
* `propagateColumnEQ` also requires determinism and no side effects: Go's own
  comment gives `a = b AND a < rand()` as the case that must not propagate.

---

## 5. How far this got, and what is unverified

Covered in scope order: **1 (predicate push-down through joins) fully; 2
(aggregate push-down) settled as absent; 3 (decorrelation / semi-join NULLs)
fully on the live Apply path; 4 (outer-join elimination, reorder) settled as
absent; 5 (column pruning, projection elimination) pruning fully, projection
elimination not reached; 6 (constant propagation) settled; 7 (max/min, TopN,
LIMIT) LIMIT fully, max/min and TopN settled as non-live.**

Counts: **3 findings** (1 latent wrong-rows, 1 rank-3 absence, 1 cost),
**7 verified-equal rules**, **8 rules absent with a stated rows-unchanged
argument**.

Explicitly **not** verified:

* Nothing was executed. No query in this document was run on either side.
  Every "returns" is derived from reading the two implementations.
* The module docs of `eliminate_empty_selection.rs`,
  `eliminate_unionall_dual_item.rs`, `push_down_sequence.rs`,
  `resolve_grouping_expand.rs`, `derive_topn_from_window.rs` and
  `join_reorder_projection_inline.rs` were read and all six declare the same
  "caller-owned plan adapter, optimizer integration external" contract as §0,
  but their bodies were not diffed against Go line by line.
  `projection_elimination.rs` was (§2.7).
* `split_scan_predicates` (the single-table scan-filter split feeding
  `negotiate_scan_filter`) was not audited; it is `exec-u2-pushdown`'s
  surface. This audit only established that it cannot run across a join
  (`scope.tables.len() == 1` gate, `access.rs:321`).
* The `costing_limit_cap` used for access-path *costing* was not compared to
  `scan_limit_cap`. It can only choose a worse index; the actual push is
  re-decided by `scan_limit_cap`.
* The ENUM/SET-vs-VARCHAR equi-key case: `KeyClass::of` gives both
  `EvalType::String`, so a key is formed, and `key_part` then returns
  `Err(KeyError)` for a hybrid datum. Whether that error is reachable from
  SQL, and what it surfaces as, was not traced.

## 6. Where a later unit should resume

1. Decide `predicate_partition.rs`'s fate (F1): either delete it as dead code
   or give it a join-type-carrying API before anything wires it up. Deleting
   is the smaller diff and removes the hazard entirely.
2. Diff the six unexamined `tidb-planner` rule modules against their Go
   wrappers to confirm the §0 classification file by file.
3. Count Go's warning-emission sites for `WHERE <int col> = <string literal>`
   to close the 4-vs-2 gap; §4 rules out the constant propagator but does not
   name the replacement.
