// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Which side of a join, if either, can be READ ONCE PER OUTER KEY rather
//! than read whole: Go's `getIndexJoinBuildHelper` reduced to the shapes
//! this tier can both execute and print.
//!
//! # What Go decides here, and what this decides
//!
//! Go asks this question inside `findBestTask`: an `IndexJoinProp` travels
//! down to the inner `DataSource`, which answers with the access path its
//! ranger can build from the outer join keys
//! (`buildDataSource2IndexScanByIndexJoinProp` for an index,
//! `buildDataSource2TableScanByIndexJoinProp` for the clustered handle), and
//! the resulting task is COSTED against the hash and merge alternatives.
//!
//! This tier has no join cost model and no physical-plan IR to cost (see
//! `crate::driver::merge_decision` for the same seam on the merge side), so
//! the choice here is STRUCTURAL, and deliberately narrower than Go's:
//!
//! * the looked-up side is a single base table read whole -- not a derived
//!   table, not a nested join, not a partitioned table, and not a side column
//!   pruning has narrowed (its offsets would no longer be the table's);
//! * every probe column's type must be EXACTLY the indexed column's type, so
//!   the outer value IS the index probe. Go instead converts each outer value
//!   to the inner column's type and drops the row when the round trip changes
//!   it (`constructDatumLookupKey`'s `ConvertTo` + `Compare`); requiring the
//!   types to agree removes that whole branch rather than reimplementing it,
//!   at the cost of refusing the mixed-type joins Go accepts. NAMED RESIDUE.
//! * the looked-up side is never the outer-join-PRESERVED side, which is Go's
//!   rule too: an index join reads its inner side per outer row, and a
//!   preserved side must be read whole.
//!
//! # Why this is not an over-eager chooser
//!
//! The structural conditions above are necessary for Go to pick an index
//! join, but they are NOT sufficient: Go picks between the three strategies
//! by cost, and a join over an indexed key whose outer side is large is
//! usually a hash join in Go. So one more condition is imposed, and it is the
//! one the recordings support: the driving side must NOT itself be a single
//! base table. Every recorded plan this tier can currently reach an
//! `IndexJoin` for drives from a DERIVED table -- a projection over a join,
//! whose join key is a computed expression that only an index probe can use
//! -- and every recorded plan whose two sides are both base tables is a hash
//! or merge join in Go. See `docs` on [`index_join_decision`].

//! # THE MEASUREMENT HISTORY OF THIS DECISION
//!
//! Every rule tried here was falsified against the recordings, and the
//! sections below are those measurements in the order they were made. They
//! are kept because each one names a prerequisite the next one refuted.
//!
//! ## The structural rules, and what falsified them
//!
//! Every structural rule tried here was falsified against
//! `r/planner/core/join_reorder_through_projection.result`, which records the
//! same statements twice -- once under `tidb_opt_join_reorder_through_proj =
//! off` and once under `on`:
//!
//! * "the inner side is a base table with an index on the join key" fires on
//!   `jt_ab`/`jt_ch` and on `t5`, where TiDB reads the table WHOLE and merge-
//!   or hash-joins it. Replay: 13 -> 18 divergences.
//! * "...and the outer join key is a projected EXPRESSION (Go's bare
//!   `Column`), the one key no index can pre-sort" is the narrowest rule the
//!   recordings suggest, and it is still wrong: `select t1.*, dt.* from t1,
//!   (select t2.a, t2.b * 2 from t2 join t3 ...) dt where t1.b =
//!   dt.doubled_b` has exactly that shape and TiDB plans a `HashJoin` over a
//!   `TableFullScan` of `t1` under BOTH settings of the variable
//!   (result:1584 and result:1607). Replay: 13 -> 22 divergences.
//!
//! What separates the recorded index joins from the recorded hash joins over
//! the same shape is not a property this decision can read at all: it is
//! WHICH JOIN TREE the statement was reordered into before any strategy was
//! costed. The section below measures that. A structural chooser here can
//! only trade one recorded instance of a statement for the other, never
//! reduce the divergence count.
//!
//! Everything BELOW this switch is exercised by
//! `crate::tests_index_join`: the decision, the executor
//! ([`crate::join::IndexLookupPlan`]) and the plan text. It is not thereby
//! PROVEN -- running the replay with the switch on is what found the range
//! text naming an inner table by its alias instead of by its own name (Go's
//! `OrigName`, see [`JoinSide::origin`]), which no test below reached.
//!
//! # What the cost model is, and what it turns out not to decide
//!
//! Half of it exists and is validated:
//! [`tidb_planner::plan_cost_ver2`] is Go's `plan_cost_ver2.go` -- the
//! DEFAULT cost model -- reproducing every `estCost` in
//! `r/planner/core/plan_cost_ver2.result` to the printed digit, including
//! `getIndexJoinCostVer24PhysicalIndexJoin` and `compareTaskCost`.
//!
//! # `gorun` is NOT an oracle for these statements, and why
//!
//! Read the paragraph below with this correction in front of it. `gorun` runs
//! a plain session; mysql-tester, which RECORDED every `.result` this tier
//! replays, puts extra variables in the DSN of every connection it opens, and
//! one of them decides exactly this question:
//!
//! ```text
//! tidb_hash_join_concurrency = 1     (a plain session resolves to 5)
//! ```
//!
//! `getPlanCostVer24PhysicalHashJoin` divides the probe filter and the probe
//! hash by `p.Concurrency`, so at 1 a hash join is charged what five workers
//! would have shared. Measured against a `tidb-server` built from this tree
//! (`make server`), replaying `planner/core/join_reorder_through_projection`
//! reproduces 87 of its 94 recorded plans at concurrency 5 and 94 of 94 at
//! concurrency 1; the SEVEN it gets wrong at 5 are seven of the eight
//! `IndexHashJoin`/`IndexJoin` plans this switch exists to reach. On the
//! three-join shape of `result:1042` the same statement costs `3943972.48` at
//! concurrency 1 and `2969908.48` at 5, and at 5 the join even swaps which
//! side it builds. `gorun` and `goeval` therefore print the plan of a
//! DIFFERENT session than the one the recording was made in, and a per-node
//! cost table taken from them is a table for another plan. The replay
//! harness issues the variables (see the mysql-tester setup list in
//! `difftests/result-tests/tests/mysqltest_connections.rs`); anything else
//! asking Go what it costs must set them by hand.
//!
//! # A cost evaluator is NOT the missing piece. Join reorder is.
//!
//! The paragraph above named the missing piece as "a recursive plan-cost
//! evaluator over THIS tier's plan tree". That is now FALSIFIED, by asking a
//! `tidb-server` built from this tree the one question that separates the two
//! explanations: hold the statement, the data, the session variables and the
//! statistics fixed, and move only `tidb_opt_join_reorder_through_proj`.
//!
//! The subject is `result:1042`'s statement, on a COLD database (so pseudo
//! statistics answer 10000 rows, as they did when the file was recorded):
//!
//! ```text
//! OFF  HashJoin(Projection(MergeJoin(MergeJoin(t3,t2), t4)), t1)
//!        t1: TableFullScan                       <- read WHOLE
//! ON   MergeJoin(t4, MergeJoin(t3, IndexHashJoin(Projection(t2), t1)))
//!        t1: IndexRangeScan range: decided by [eq(<db>.t1.b, Column)]
//! ```
//!
//! The two plans are not two strategies over one join tree; they are two
//! JOIN TREES. Under `on`, Go's `join_reorder` looks through the projection,
//! pulls the `t1` join out of the derived table and lands it at the BOTTOM of
//! a left-deep chain -- and only in that position does the index join win.
//! Under `off` the tree is the one THIS TIER builds, and on that tree TiDB's
//! own cost model chooses the hash join over a whole-table read of `t1`,
//! which is exactly what this switch's `false` already produces.
//!
//! So a faithful cost evaluator over this tier's tree would agree with TiDB
//! about this tier's tree, and this tier's tree is the `off` one. The
//! evaluator cannot reach the `on` plan because the join it would have to
//! cost does not exist here.
//!
//! Measured end to end, at commit `250d117`: flipping this switch to `true`
//! takes `planner/core/join_reorder_through_projection` from 13 divergences
//! to 22 and fixes ZERO of the eight targets. The topic records every
//! statement under BOTH settings of the variable and this decision is blind
//! to it, so the gate fires on both instances -- right on the `on` one, wrong
//! on the `off` one -- and the `on` instances still diverge for their own
//! reasons (`t5`'s covering-index choice, and TiDB reading `t1` whole under a
//! shape this tier plans differently). "Trades one recorded instance of a
//! statement for the other" was the right prediction; the count is 13 -> 22
//! with a FIXED column of zero.
//!
//! The named prerequisite is therefore `tidb_opt_join_reorder_through_proj`
//! itself -- `pkg/planner/core/rule_join_reorder.go`'s projection inlining
//! and its `colExprMap` substitution -- not [`tidb_planner::plan_cost_ver2`].
//! A cost comparison becomes the deciding input only AFTER the reordered tree
//! exists to compare over.
//!
//! # That prerequisite has since LANDED, and the switch still cannot flip
//!
//! `driver::through_proj::inject_expressions` now materializes Go's
//! `injectExpr` column, so the `on` tree above is a tree this tier BUILDS:
//! for `result:1319`'s statement it reaches `t5` on top, `t3` next, and
//! `Projection(t2)` joined against `t1` at the bottom -- the recorded leaf
//! order, the one the paragraph above said the evaluator could not reach.
//!
//! Re-measured on that tree, flipping this switch to `true` is WORSE than it
//! was before the reorder existed:
//!
//! | | `false` | `true` |
//! | --- | --- | --- |
//! | topic divergences | `13` | `25` (was `22` at `250d117`) |
//! | corpus divergences | `24` | `36` |
//! | `join_shape` agreements | `105` | `91` |
//! | topic join plans | `68` of `82` | `54` of `82` |
//!
//! Those `false` figures are the ones that commit measured; the corpus has
//! moved under them since, so read them as a BEFORE/AFTER pair and not as a
//! current baseline. Re-measured with the switch still `false`, the replay
//! reports `planner/core/join_reorder_through_projection: 311 matched, 15
//! diverged, 20 skipped of 346` and `70 of 84 join plans agree`, under a
//! corpus-wide `join shape over 106 topics: 137 of 231 join plans agree on
//! BOTH`.
//!
//! So the tree was necessary and is not sufficient. What the numbers now
//! isolate is the thing the paragraph above deferred and nothing has supplied:
//! the COMPARISON. [`index_join_decision`] answers "could an index join be
//! built here", and with the switch on that structural answer becomes the
//! decision -- so the index join is taken wherever it is possible rather than
//! wherever it is cheaper, which is why the agreements fall on a tree that
//! otherwise got closer to TiDB's. The missing input is a recursive
//! [`tidb_planner::plan_cost_ver2`] evaluation of both candidate plans.
//!
//! # The evaluator EXISTS now, and "the only missing input" was wrong twice
//!
//! [`tidb_planner::candidate_cost`] is that recursion: `GetPlanCostVer2` over
//! a whole candidate plan, children first. It is validated node by node
//! against `EXPLAIN FORMAT='cost_trace'` from a `tidb-server` built from this
//! tree in a session carrying mysql-tester's DSN variables -- every node of
//! `result:1042`'s recorded `IndexHashJoin`, every node of `result:1169`'s
//! recorded `IndexJoin`, and every node of the HASH-JOIN ALTERNATIVE a
//! `hash_join()` hint makes the same server print at the same join. All three
//! reproduce to the printed digit.
//!
//! The paragraph above then called that evaluation the ONLY missing input.
//! Building it falsified the claim twice over.
//!
//! ## First: the evaluator needs a row count this tier cannot supply
//!
//! `getCardinality(p)` is read at EVERY node, and the dominant index-join
//! term is `buildRows*10*tidb_cpu_factor` -- `3,992,000` of `result:1042`'s
//! `4,606,578.48`. This tier derives a per-node row estimate in exactly one
//! place, [`crate::plan_trace::PlanTrace`], which the driver constructs only
//! for `EXPLAIN`. A comparison wired to it would make the STRATEGY depend on
//! whether the statement is being explained -- `EXPLAIN` printing an index
//! join over a pipeline that hash-joins. The prerequisite is an estimate
//! owner both the recorder and the chooser read, which `PlanTrace` is not.
//!
//! ## Second: a comparison AT THIS JOIN is not the comparison Go makes
//!
//! Measured on the same server, holding the statement, the data and the
//! session fixed, and moving only a `hash_join()` hint:
//!
//! | | `result:1042` (pseudo) | `result:1169` (ANALYZEd) |
//! | --- | --- | --- |
//! | Go's recorded join | `IndexHashJoin_51 10000.00 4606578.48` | `IndexJoin_31 2.00 4106.23` |
//! | the hash alternative | `HashJoin_94 10000.00 2373179.65` | `HashJoin_38 2.00 2423.24` |
//! | cheaper AT THE JOIN | hash, by `2233398.83` | hash, by `1682.99` |
//! | Go's whole tree | `Projection_23 15625.00 5540964.75` | `Projection_15 2.00 4578.26` |
//! | the alternative's tree | `Projection_23 15625.00 7196543.24` | `Projection_15 2.00 3007.87` |
//! | cheaper AS A TREE | index, by `1655578.49` | hash, by `1570.39` |
//! | Go's actual choice | INDEX | INDEX |
//!
//! Both columns refute a local comparison, and for different reasons.
//!
//! On `result:1042` the hash join is cheaper at the join and the index join
//! is cheaper as a tree, because the index-join task PRESERVES the outer
//! side's order: the two parent `MergeJoin`s stay merge joins. Drop the
//! order and they become hash joins over whole re-reads
//! (`HashJoin_78`, `HashJoin_24`), and even the CHILDREN change -- the
//! index-join build reads `t2` in order (`TableReader_55 8000.00 435671.93`)
//! while the hash-join build reads `t2`'s index unordered
//! (`IndexReader_67 8000.00 236517.33`). The two candidates are not two
//! strategies over one pair of children; Go's `findBestTask` re-plans each
//! side under the required property and compares whole TASKS.
//!
//! On `result:1169` the hash tree is cheaper at the join AND as a tree
//! (`2987.11` against `4557.50` at the parent `MergeJoin_18`, the extra
//! `Sort_57 2.00 2535.84` included), and Go still records the index join.
//! So on that statement not even a whole-task comparison reproduces Go's
//! choice: the cheaper plan is one the unhinted enumeration never generates.
//! Reproducing Go's recorded CHOICE and minimising Go's cost are not the
//! same objective.
//!
//! The named prerequisite is therefore `findBestTask`'s required-property
//! propagation -- the thing that decides which child plans each candidate is
//! even allowed to compare over -- and not a bigger evaluator. The evaluator
//! is done and is not the blocker; it is kept, validated, and unused by this
//! switch.
//!
//! Two further inputs stay true and are kept:
//!
//! * the leaf half of the model is NOT a second cost model. [`crate::access_cost`]
//!   is `plan_cost_ver2.go` too -- same `MinNumRows`/`MinRowSize`/
//!   `MaxPenaltyRowCount`, same `tikv_scan_factor` `40.70` and
//!   `tidb_kv_net_factor` `3.96`, same `getTableScanPenalty` -- reached
//!   through a private copy of the leaf formulas rather than through
//!   [`tidb_planner::plan_cost_ver2`]. A join chooser built on
//!   [`tidb_planner::plan_cost_ver2`] therefore extends the leaf choosers'
//!   model, it does not fork one.
//! * `gorun` is still not an oracle for these statements, but the SERVER is:
//!   `make server`, then a session issuing mysql-tester's seven setup
//!   variables, then `EXPLAIN FORMAT='cost_trace'`. Every number in the table
//!   above was read that way, and `format='verbose'` alone is not enough --
//!   an index join's inner subtree PRINTS total rows and is COSTED per outer
//!   row, so only the trace shows which one a formula used.
//!
//! # The comparison EXISTS now, and it is an ENUMERATION rule, not a cost one
//!
//! [`tidb_planner::find_best_task`] is the `(logical plan, required property)`
//! recursion the section above named as the prerequisite, ported for
//! `LogicalJoin`. It reproduces BOTH rows of the table above, and the reason
//! is not a cost comparison at all -- it is which candidates Go enumerates:
//!
//! * `getHashJoins` opens with "hash join doesn't promise any orders" and
//!   returns NOTHING under a non-empty `prop.SortItems`. On `result:1042` the
//!   join sits under a parent `MergeJoin`'s key order, so the cheaper
//!   `HashJoin_94 2373179.65` is not a candidate there; the search picks
//!   `IndexHashJoin_51 4606578.48`, and the SAME site under the empty property
//!   picks the hash join. The property, not the cost, is the difference.
//! * `constructIndexJoinStatic` hands the OUTER child `prop.SortItems`
//!   unchanged, which is what keeps the parent merge joins alive and what
//!   makes the outer side take its DEARER ordered read
//!   (`Projection_52 517108.73` rather than `Projection_61 317954.13`).
//! * `getEnforcedMergeJoin` is reached only under a `MERGE_JOIN` hint or with
//!   hash join disabled, so the `Sort`-enforced merge that beats Go's own plan
//!   on `result:1169` is a plan no unhinted enumeration generates.
//! * `findBestTask`'s enforcer branch runs only when `prop.CanAddEnforcer`,
//!   and `PhysicalMergeJoin.tryToGetChildReqProp` builds its children's
//!   properties with `enforced: false`. A join under a merge-join parent
//!   therefore never prices a `Sort` of its own -- so "the enforcer-Sort
//!   alternative is always in the comparison" was wrong.
//!
//! The switch still stays `false`, and for the ONE reason that survived:
//! nothing calls the search. [`tidb_planner::find_best_task`] takes its row
//! counts through a caller-supplied cost model, and this tier's only per-node
//! row estimate still lives in [`crate::plan_trace::PlanTrace`], which the
//! driver builds for `EXPLAIN` alone. Re-measured at the commit that landed
//! the search, flipping this switch -- which would put the STRUCTURAL gate
//! below back in charge, not the search -- moves every control the wrong way:
//!
//! | | `false` | `true` |
//! | --- | --- | --- |
//! | `join_reorder_through_projection` diverged | `15` of `346` | `27` of `346` |
//! | topic join plans agreeing | `70` of `84` | `56` of `84` |
//! | corpus `join shape` agreeing on BOTH | `137` of `231` | `123` of `231` |
//!
//! The named prerequisite is now exactly one thing: an estimate owner both
//! `EXPLAIN` and the chooser read, so the driver can hand
//! [`tidb_planner::find_best_task`] the rows its candidates need.
//!
//! # THE ESTIMATE OWNER EXISTS NOW, AND IT WAS NOT THE PREREQUISITE EITHER
//!
//! [`crate::driver::join_reorder::RowSource`] is that owner:
//! [`tidb_planner::cardinality::derive_stats`] over the models the DP solver
//! already builds, read off the statement, the catalog and the statistics and
//! NOTHING ELSE. It reproduces every row count TiDB records for
//! `result:1042`'s plan -- `9990.00` for the `t1` side, `8000.00` for the
//! `t2` side, `10000.00` at the `IndexHashJoin`, `12500.00` above it and
//! `15625.00` at the root -- and it answers identically whether or not the
//! statement is being explained. Both halves are pinned by
//! `crate::tests_join_search`.
//!
//! The switch is GONE with it. [`crate::driver::join_search`] now stands
//! between `build_join` and this decision, and it asks Go's own enumeration
//! (`exhaustPhysicalPlans4LogicalJoin`, through
//! [`tidb_planner::find_best_task::exhaust_join`]) which strategies the
//! property this site was asked for even admits. The structural gate that
//! used to sit inside [`index_join_decision`] -- "some outer key must be a
//! bare `Column`" -- is gone too: it was a proxy for the enumeration, and the
//! enumeration is here.
//!
//! ## What the wired search measures, and what it refuses
//!
//! Over the 106-topic replay the chooser answers at 1109 join sites:
//!
//! | answer | sites |
//! | --- | --- |
//! | `HashAlsoEnumerated` -- the required property is EMPTY | `543` |
//! | `NoRowSource` -- this `FROM` shape has no estimate owner | `385` |
//! | `NoEquiKeys` -- a cross join | `140` |
//! | `MergeAlsoEnumerated` -- ordered property, merge join also enumerated | `41` |
//! | `Index` -- the choice by elimination | `0` |
//!
//! Zero. Every control therefore holds EXACTLY where it was, and that is the
//! result: `planner/core/join_reorder_through_projection: 311 matched, 15
//! diverged, 20 skipped of 346` and `70 of 84 join plans agree`, under
//! `join shape over 106 topics: 137 of 229 join plans agree on BOTH` and
//! `integrationtest replay over 106 topics: 7885 of 10747 statements
//! compared`, per topic identical to the commit before this one. The switch
//! and the gate were removed without moving a single plan, which is what
//! "the search is now the only path" is worth on its own.
//!
//! ## The prerequisite this falsifies, and the one it names
//!
//! The row source was NOT what stood between this tier and Go's recorded
//! choice. It exists, it is exact, and the search still never chooses the
//! index join -- because at `result:1042`'s site, and at 542 others, the
//! property the site is asked for is EMPTY, so `getHashJoins` answers too.
//!
//! The property is empty for a reason this file can name precisely.
//! [`crate::driver::merge_decision::join_properties`] reports the order a
//! join's OWN CHOSEN PLAN produces -- a deliberate narrowing of Go's
//! `LogicalJoin.PreparePossibleProperties`, which reports the LOGICAL union
//! of both children's orders. On `result:1042` the bottom join's key is the
//! projected expression, no order covers it, so this tier plans a hash join
//! there, so the join reports NO order, so no parent merge join is formed, so
//! nothing ever requires an order of the site where TiDB puts the index join.
//! Go escapes the circle because a parent that wants one of those logical
//! orders RE-ASKS the child through `findBestTask(prop)`; there the index
//! join's `constructIndexJoinStatic` hands the outer child `prop.SortItems`
//! unchanged and the order is delivered.
//!
//! So the named prerequisite is now: Go's LOGICAL
//! `PreparePossibleProperties` union, plus a way for a parent to VERIFY that
//! the child delivered the order it promised. This tier builds executors
//! bottom-up and cannot re-plan a built child, so reporting the union today
//! would promise an order a hash join then fails to deliver and the merge
//! executor would silently drop rows -- which is exactly why the narrowing
//! was made. The increment is a second planning pass (or a promise/verify
//! protocol between `build_join` and its children), not another estimator and
//! not a bigger cost model.

use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::rewrite_expr_resolved;

use crate::driver::from::FromScope;
use crate::driver::{Catalog, TableEntry};
use crate::hash_join::EquiKey;
use crate::kv_table::KvTable;

/// What `EXPLAIN` prints for a column no name reaches -- a projected
/// expression. Go's `Column.StringWithCtx` falls back to it when the column
/// has no `OrigName`.
const UNNAMED_COLUMN: &str = "Column";

/// The looked-up side of a join and the object its probes read.
pub(crate) struct IndexJoinDecision {
    /// Whether the looked-up side is the join's left child.
    pub(crate) lookup_is_left: bool,
    /// Offsets into the join's equality keys that decide the probe range, in
    /// the object's own key-column order.
    pub(crate) probe_keys: Vec<usize>,
    /// The complete object key assembled from dynamic join keys and static
    /// access conditions.
    pub(crate) probe_parts: Vec<crate::access_path::LookupProbePart>,
    /// Number of equality join keys retained by the logical join. The access
    /// path may use only a subset of them to build its dynamic range.
    pub(crate) join_key_count: usize,
    /// The table the probes read.
    pub(crate) table: KvTable,
    /// The object the probes read: an index, or the clustered handle.
    pub(crate) object: crate::access_path::LookupObject,
    /// The looked-up table's visible columns, which are this side's whole
    /// physical row layout.
    pub(crate) columns: Vec<(String, FieldType)>,
    /// The physical database owning `table`. Inner filters use a table-local
    /// column layout and must not be qualified through the surrounding join.
    pub(crate) database: String,
    /// Physical table offsets emitted by a bare lookup, in the narrowed
    /// child's logical output order. A retained aggregation consumes its
    /// physical-width input instead and does not use this projection.
    pub(crate) output_offsets: Vec<usize>,
    /// The name this side is written under, for `EXPLAIN`'s `table:`.
    pub(crate) visible: String,
    /// The `EXPLAIN` text of what decided the range: Go's
    /// `indexJoinPathRangeInfo` / `indexJoinIntPKRangeInfo`.
    pub(crate) range_info: String,
    /// Every predicate local to the looked-up leaf, in statement form for
    /// EXPLAIN and rewritten form for execution.
    pub(crate) filters: Vec<tidb_ast::Expr>,
    pub(crate) filter_exprs: Vec<Expression>,
    /// Selectivity of the filter residue not already represented by a static
    /// object-key part.
    pub(crate) filter_selectivity: f64,
    /// Selectivity of every predicate on the base-table source, including
    /// predicates represented by static lookup-key parts. Go uses this when
    /// an IndexJoin runtime property crosses a retained aggregation: the
    /// aggregate output expectation must be scaled back to source rows.
    pub(crate) source_filter_selectivity: f64,
    /// A grouped derived table retained above the re-seeded base-table
    /// reader. Bare table lookup sides leave this absent.
    pub(crate) aggregation: Option<crate::join::IndexLookupAggregation>,
    /// Go's physical aggregation payload for that retained aggregation.
    pub(crate) aggregation_info: Option<String>,
    /// Go's final aggregation payload when a cop partial aggregation is
    /// retained below a double-read lookup.
    pub(crate) aggregation_final_info: Option<String>,
    /// Go's partial aggregation payload pushed onto the table side of a
    /// double-read lookup.
    pub(crate) aggregation_partial_info: Option<String>,
    /// The lookup target is nested below another join on this side. The
    /// executor rebuilds that subtree with a shared probe channel instead of
    /// replacing the whole side with a bare table reader.
    pub(crate) composite: bool,
    /// Whether a local equality constrains a dynamically probed object-key
    /// column. For a grouped lookup this means at most one distinct outer key
    /// can produce base rows; every other probe is empty.
    pub(crate) constant_constrained_probe: bool,
    /// Whether those leaf filters plus the join equalities cover the complete
    /// written WHERE.
    pub(crate) consumes_where: bool,
    /// Go's `rule_join_key_type_cast` probe: the outer key is
    /// `cast(str AS SIGNED)` computed behind the rule's guard rather than a
    /// bare outer column, and the equality it belongs to is not in the
    /// split keys. See [`crate::driver::join_key_cast`].
    pub(crate) probe_cast: Option<crate::join::IndexProbeCast>,
}

impl IndexJoinDecision {
    /// Records the evicted statistics Go touches while costing this physical
    /// lookup candidate. `ColumnStatsIsInvalid` owns clustered handles;
    /// `IndexStatsIsInvalid` owns common handles and secondary indexes.
    pub(crate) fn record_stats_access(&self, stats: Option<&crate::access_cost::TableStatistics>) {
        let Some(stats) = stats.filter(|stats| !stats.pseudo) else {
            return;
        };
        let mut columns = std::collections::BTreeSet::new();
        let mut indexes = std::collections::BTreeSet::new();
        match self.object {
            crate::access_path::LookupObject::Handle => {
                if let Some(column) = self
                    .table
                    .pk_handle_offset()
                    .and_then(|offset| self.table.columns.get(offset))
                {
                    columns.insert(column.id);
                }
            }
            crate::access_path::LookupObject::CommonHandle => {
                if let Some(index) = self
                    .table
                    .indexes()
                    .iter()
                    .find(|index| index.name.eq_ignore_ascii_case("PRIMARY"))
                {
                    indexes.insert(index.id);
                }
            }
            crate::access_path::LookupObject::Index(id) => {
                indexes.insert(id);
            }
        }
        stats.mark_accessed_statistics(columns, indexes);
    }

    /// Go's `maxOneRow`: only an integer handle or every column of a unique
    /// object key guarantees at most one row. A partial common-handle prefix is
    /// a table range and can return every suffix below it.
    pub(crate) fn max_one_row(&self) -> bool {
        match self.object {
            crate::access_path::LookupObject::Handle => true,
            crate::access_path::LookupObject::CommonHandle => {
                self.probe_parts.len() == self.table.common_handle_offsets().len()
            }
            crate::access_path::LookupObject::Index(id) => self
                .table
                .indexes()
                .iter()
                .find(|index| index.id == id)
                .is_some_and(|index| {
                    index.unique && self.probe_parts.len() == index.column_offsets.len()
                }),
        }
    }

    /// `TableStats.RowCount / NDV(usable join-key prefix)` -- Go's
    /// `rowCountUpperBound` (`exhaust_physical_plans.go:1123-1141`).
    ///
    /// CITATION CORRECTED, AND A DIVERGENCE RECORDED WITH IT. This carried
    /// the name `indexJoinProbeAccessRowsFloor` and a reference to Go
    /// "#70176"; no such function, and no `TestIndexJoinInnerRowCountUses
    /// UsableJoinKeys`, exists anywhere in the Go tree. What does exist is
    /// the quantity above, and Go uses it in two ways this does not:
    ///
    /// * as an UPPER bound -- `rowCount = math.Min(rowCount,
    ///   rowCountUpperBound)` (`:1144`), never a lower one;
    /// * on the inner INDEX-scan task only, and even there behind
    ///   `fixcontrol.Fix44855`, which DEFAULTS TO FALSE. Go's inner
    ///   TABLE-scan task (`constructDS2TableScanTask`, `:832-874`) bounds
    ///   `AvgInnerRowCnt / selectivity` in neither direction.
    ///
    /// The remaining callers use it as a `> 0.0` switch between estimate
    /// sources rather than as a bound. Removing it outright is gate-neutral
    /// except for `index_join_probe_rows_use_only_the_access_paths_join_keys`,
    /// whose expectation was written from the same missing Go test -- so the
    /// mechanism is left in place, and settling it needs a plan captured from
    /// a running Go TiDB rather than another reading of this tree.
    ///
    /// The one place it had inverted Go's direction outright -- `.max()` on
    /// the printed access estimate for a table-scan probe -- is fixed; see
    /// `driver::from`'s `estimated_access_rows`.
    pub(crate) fn probe_access_rows_floor(
        &self,
        stats: Option<&crate::access_cost::TableStatistics>,
    ) -> f64 {
        let used_join_keys = self
            .probe_keys
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if self.max_one_row() || used_join_keys >= self.join_key_count {
            return 0.0;
        }
        let Some(stats) = stats.filter(|stats| !stats.pseudo && stats.row_count > 0) else {
            return 0.0;
        };
        let key_offsets = match self.object {
            crate::access_path::LookupObject::Handle => self
                .table
                .pk_handle_offset()
                .map(|offset| vec![offset])
                .unwrap_or_default(),
            crate::access_path::LookupObject::CommonHandle => {
                self.table.common_handle_offsets().to_vec()
            }
            crate::access_path::LookupObject::Index(id) => self
                .table
                .indexes()
                .iter()
                .find(|index| index.id == id)
                .map(|index| index.column_offsets.clone())
                .unwrap_or_default(),
        };
        let used_column_ids = key_offsets
            .iter()
            .take(self.probe_parts.len())
            .map(|offset| self.table.columns.get(*offset).map(|column| column.id))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        if used_column_ids.is_empty() {
            return 0.0;
        }

        let full_loaded_columns = stats.columns.keys().copied().collect();
        let full_loaded_indexes = stats.indexes.keys().copied().collect();
        let column_ndvs = self
            .table
            .columns
            .iter()
            .filter_map(|column| {
                stats
                    .estimate_column_ndv(column.id, &full_loaded_columns, &full_loaded_indexes)
                    .map(|ndv| (column.id, ndv))
            })
            .collect::<Vec<_>>();
        // Go's one-column path estimate is `EstimateColumnNDV`, including its
        // analyzed/realtime increase factor. A GroupNDV is useful only for a
        // genuinely multi-column prefix; admitting a one-column index here
        // would replace that scaled NDV with the raw index histogram NDV.
        let group_ndvs = if used_column_ids.len() > 1 {
            self.table
                .indexes()
                .iter()
                .filter_map(|index| {
                    let index_stats = stats.indexes.get(&index.id)?;
                    let columns = index
                        .column_offsets
                        .iter()
                        .map(|offset| self.table.columns.get(*offset).map(|column| column.id))
                        .collect::<Option<Vec<_>>>()?;
                    Some(tidb_planner::cardinality::ndv::GroupNdv {
                        columns,
                        ndv: index_stats.histogram.ndv as f64,
                    })
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let (used_ndv, _) = tidb_planner::cardinality::ndv::estimate_cols_ndv_with_matched_len(
            &used_column_ids,
            &column_ndvs,
            stats.row_count as f64,
            &group_ndvs,
            0.0,
        );
        if used_ndv > 0.0 {
            stats.row_count as f64 / used_ndv
        } else {
            0.0
        }
    }

    /// The selected index's analyzed/realtime row-count ratio. Go's rebuilt
    /// partial-key task derives `CountAfterIndex` from that index's analyzed
    /// distribution, while the access floor starts from realtime rows.
    pub(crate) fn probe_analyzed_scale(
        &self,
        stats: Option<&crate::access_cost::TableStatistics>,
    ) -> f64 {
        let Some(stats) = stats.filter(|stats| !stats.pseudo && stats.row_count > 0) else {
            return 1.0;
        };
        let crate::access_path::LookupObject::Index(id) = self.object else {
            return 1.0;
        };
        let analyzed_rows = stats
            .indexes
            .get(&id)
            .map(|index| index.total_row_count())
            .unwrap_or(0.0);
        if analyzed_rows > 0.0 {
            analyzed_rows / stats.row_count as f64
        } else {
            1.0
        }
    }

    /// Whether the dynamic lookup path groups identical keys contiguously,
    /// allowing Go's physical StreamAgg candidate for a retained derived
    /// aggregation.
    pub(crate) fn aggregation_stream_ordered(&self) -> bool {
        let Some(aggregation) = &self.aggregation else {
            return false;
        };
        if self.max_one_row() {
            return true;
        }
        let key_offsets = match self.object {
            crate::access_path::LookupObject::Handle => self
                .table
                .pk_handle_offset()
                .map(|offset| vec![offset])
                .unwrap_or_default(),
            crate::access_path::LookupObject::CommonHandle => {
                self.table.common_handle_offsets().to_vec()
            }
            crate::access_path::LookupObject::Index(id) => self
                .table
                .indexes()
                .iter()
                .find(|index| index.id == id)
                .map(|index| index.column_offsets.clone())
                .unwrap_or_default(),
        };
        let fixed_len = self.probe_parts.len().min(key_offsets.len());
        let fixed = &key_offsets[..fixed_len];
        let remaining = aggregation
            .group_offsets
            .iter()
            .copied()
            .filter(|offset| !fixed.contains(offset))
            .collect::<std::collections::BTreeSet<_>>();
        let ordered = key_offsets[fixed_len..]
            .iter()
            .take(remaining.len())
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        remaining == ordered
    }
}

/// One join side reduced to what the decision reads about it.
pub(crate) struct JoinSide<'a> {
    /// The table this side reads, when it is a single base table read whole.
    pub(crate) table: Option<&'a KvTable>,
    /// The name it is written under.
    pub(crate) visible: String,
    /// Its output column types, in row order.
    pub(crate) types: Vec<FieldType>,
    /// Its output column names qualified as `EXPLAIN` prints them, in row
    /// order. A column no name reaches -- a projected expression -- is Go's
    /// bare `Column`.
    pub(crate) names: Vec<String>,
    /// `<database>.<table>` for the base table this side reads, written under
    /// the table's OWN name rather than the alias it is written under.
    ///
    /// Go's `Column.StringWithCtx` prints `OrigName`, which a table alias does
    /// NOT rename: the recorded plan for `from t1 outer_t, (...)` carries
    /// `table:outer_t` in the access object and
    /// `<db>.t1.b` in the same row's `range: decided by [...]`. Qualifying the
    /// range's inner column off the scope would print the alias in both.
    pub(crate) origin: Option<String>,
    /// The base table name printed by the lookup reader.  This differs from
    /// `visible` when the join side is a derived table alias.
    pub(crate) source_visible: String,
    /// For each side output, the base-table column it carries. Computed
    /// aggregate outputs have no source offset.
    pub(crate) output_to_source: Vec<Option<usize>>,
    /// Predicates written inside a grouped derived table, evaluated by the
    /// re-seeded base-table reader before aggregation.
    pub(crate) source_filters: Vec<tidb_ast::Expr>,
    /// The executable grouped derived-table transformation, if any.
    pub(crate) aggregation: Option<crate::join::IndexLookupAggregation>,
    pub(crate) aggregation_info: Option<String>,
    pub(crate) aggregation_final_info: Option<String>,
    pub(crate) aggregation_partial_info: Option<String>,
    /// Whether `table` is a target leaf discovered below a composite side.
    pub(crate) composite: bool,
}

/// The looked-up side of `join`, or `None` when neither side qualifies.
///
/// Reached ONLY through [`crate::driver::join_search`], which asks Go's own
/// enumeration whether the index strategy is this site's at all. This function
/// answers the second question: WHICH side, WHICH object, and WHICH ranges.
///
/// `keys` are the join's equality conjuncts as
/// [`crate::hash_join::split_equi`] produced them: `left` is an offset in the
/// LEFT child's row and `right` an offset in the RIGHT child's.
#[cfg(test)]
pub(crate) fn index_join_decision(
    kind: crate::join::JoinKind,
    keys: &[EquiKey],
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    merge_chosen: bool,
) -> Option<IndexJoinDecision> {
    index_join_decision_with_context(
        kind,
        keys,
        left,
        right,
        merge_chosen,
        None,
        None,
        &crate::StmtContext::for_query(),
    )
}

/// The production decision with the statement's derived leaf predicates and
/// evaluation context.
#[cfg(test)]
pub(crate) fn index_join_decision_with_context(
    kind: crate::join::JoinKind,
    keys: &[EquiKey],
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    merge_chosen: bool,
    rows: Option<&crate::driver::join_reorder::RowSource>,
    catalog: Option<&crate::driver::Catalog>,
    ctx: &crate::StmtContext,
) -> Option<IndexJoinDecision> {
    index_join_decisions_with_context(kind, keys, left, right, merge_chosen, rows, catalog, ctx)
        .into_iter()
        .next()
}

/// Every structurally valid looked-up side, in Go's outer-left then
/// outer-right enumeration order.
pub(crate) fn index_join_decisions_with_context(
    kind: crate::join::JoinKind,
    keys: &[EquiKey],
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    merge_chosen: bool,
    rows: Option<&crate::driver::join_reorder::RowSource>,
    catalog: Option<&crate::driver::Catalog>,
    ctx: &crate::StmtContext,
) -> Vec<IndexJoinDecision> {
    if keys.is_empty() || merge_chosen {
        return Vec::new();
    }
    // The looked-up side is never the preserved one.
    let sides: &[bool] = match kind {
        crate::join::JoinKind::Inner => &[false, true],
        crate::join::JoinKind::Left => &[false],
        crate::join::JoinKind::Right => &[true],
        crate::join::JoinKind::Semi | crate::join::JoinKind::AntiSemi => &[false],
    };
    let mut decisions = Vec::with_capacity(sides.len() * 2);
    for lookup_is_left in sides.iter().copied() {
        let (inner, outer) = if lookup_is_left {
            (left, right)
        } else {
            (right, left)
        };
        let Some(table) = inner.table else {
            continue;
        };
        let statistics =
            catalog.and_then(|catalog| catalog.table_statistics(table.stats_physical_id()));
        decisions.extend(decide_over(
            table,
            lookup_is_left,
            keys,
            inner,
            outer,
            rows,
            statistics.as_deref().map(AsRef::as_ref),
            ctx,
        ));
    }
    decisions
}

/// The decision for one candidate side, once it is known to be a base table.
fn decide_over(
    table: &KvTable,
    lookup_is_left: bool,
    keys: &[EquiKey],
    inner: &JoinSide<'_>,
    outer: &JoinSide<'_>,
    rows: Option<&crate::driver::join_reorder::RowSource>,
    statistics: Option<&crate::access_cost::TableStatistics>,
    ctx: &crate::StmtContext,
) -> Vec<IndexJoinDecision> {
    // A partitioned table's probe would have to name the partition the key
    // falls in; Go refuses `keepOrder` there and prunes per probe, neither of
    // which this reads. Refuse it whole.
    if table.partition().is_some() {
        return Vec::new();
    }
    let Some(database) = inner
        .origin
        .as_deref()
        .and_then(|origin| origin.rsplit_once('.'))
        .map(|(database, _)| database.to_owned())
    else {
        return Vec::new();
    };
    let columns: Vec<(String, FieldType)> = table
        .visible_columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    // Every bare output must map back to the physical table. A grouped
    // derived side may also contain computed aggregate outputs, which are
    // rebuilt after the lookup and therefore have no source offset.
    if inner.output_to_source.len() != inner.types.len() {
        return Vec::new();
    }
    let output_offsets = inner
        .output_to_source
        .iter()
        .copied()
        .collect::<Option<Vec<_>>>();
    if inner.aggregation.is_none() && output_offsets.is_none() && !inner.composite {
        return Vec::new();
    }
    let output_offsets = output_offsets.unwrap_or_default();
    let inner_at = |key: &EquiKey| if lookup_is_left { key.left } else { key.right };
    let outer_at = |key: &EquiKey| if lookup_is_left { key.right } else { key.left };
    // Which of this side's columns a key probes, and with which key.
    let key_of_column = |column: usize| -> Option<usize> {
        keys.iter().position(|key| {
            inner
                .output_to_source
                .get(inner_at(key))
                .is_some_and(|offset| *offset == Some(column))
                && probe_compatible(&inner.types[inner_at(key)], &outer.types[outer_at(key)])
        })
    };
    let outer_filters = rows
        .and_then(|rows| rows.filters_for(&inner.visible))
        .unwrap_or_default()
        .to_vec();
    let outer_filters = if inner.aggregation.is_some() {
        let Some(filters) = outer_filters
            .iter()
            .map(|filter| rewrite_derived_filter_to_source(filter, inner, &columns))
            .collect::<Option<Vec<_>>>()
        else {
            return Vec::new();
        };
        filters
    } else {
        outer_filters
    };
    let mut filters = inner
        .source_filters
        .iter()
        .cloned()
        .chain(outer_filters)
        .collect::<Vec<_>>();
    for filter in &mut filters {
        if normalize_source_filter(filter, &inner.source_visible, &columns).is_none() {
            return Vec::new();
        }
    }
    filters.dedup();
    let Some(filter_exprs) = rewrite_inner_filters(inner, &columns, &filters, ctx) else {
        return Vec::new();
    };
    let source_filter_selectivity = residual_filter_selectivity(
        &filters,
        &[],
        &columns,
        table,
        &inner.source_visible,
        statistics,
        &ctx.session_zone(),
    );
    let constants = static_equalities(&columns, &filters, ctx);
    let consumes_where = rows
        .is_some_and(crate::driver::join_reorder::RowSource::all_where_is_leaf_or_join_equality);

    // Builds the object's complete leading key from dynamic equality columns
    // and leaf-local constants. At least one part must be dynamic -- a wholly
    // static key is a point get, not an index join.
    let probe_for = |offsets: &[usize]| {
        let mut probe_keys = Vec::new();
        let mut probe_parts = Vec::new();
        let mut dynamic_info = Vec::new();
        let mut static_info = Vec::new();
        let mut static_columns = Vec::new();
        for offset in offsets {
            if let Some(key) = key_of_column(*offset) {
                let dynamic = probe_keys.len();
                probe_keys.push(key);
                probe_parts.push(crate::access_path::LookupProbePart::Dynamic(dynamic));
                dynamic_info.push(format!(
                    "eq({}, {})",
                    inner_column_name(inner, *offset),
                    if inner.composite {
                        outer.names[outer_at(&keys[key])].clone()
                    } else {
                        physical_outer_column_name(outer, outer_at(&keys[key]))
                    }
                ));
            } else if let Some(value) = constants.get(*offset).and_then(Clone::clone) {
                probe_parts.push(crate::access_path::LookupProbePart::Constant(value.clone()));
                static_columns.push(*offset);
                static_info.push(format!(
                    "eq({}, {})",
                    inner_column_name(inner, *offset),
                    datum_text(&value)
                ));
            } else {
                break;
            }
        }
        (!probe_keys.is_empty()).then_some((
            probe_keys,
            probe_parts,
            dynamic_info,
            static_info,
            static_columns,
        ))
    };

    let mut decisions = Vec::new();

    // The clustered integer handle is Go's table-range candidate. It is
    // enumerated beside, rather than instead of, the secondary-index candidate
    // built below.
    let int_handle = (0..columns.len()).find(|at| table.is_clustered_handle_column(*at));
    if let Some(pk) = int_handle {
        if let Some(key) = key_of_column(pk) {
            decisions.push(IndexJoinDecision {
                lookup_is_left,
                probe_keys: vec![key],
                probe_parts: vec![crate::access_path::LookupProbePart::Dynamic(0)],
                join_key_count: keys.len(),
                table: table.clone(),
                object: crate::access_path::LookupObject::Handle,
                filter_selectivity: residual_filter_selectivity(
                    &filters,
                    &[],
                    &columns,
                    table,
                    &inner.source_visible,
                    statistics,
                    &ctx.session_zone(),
                ),
                source_filter_selectivity,
                aggregation: inner.aggregation.clone(),
                aggregation_info: inner.aggregation_info.clone(),
                aggregation_final_info: inner.aggregation_final_info.clone(),
                aggregation_partial_info: inner.aggregation_partial_info.clone(),
                composite: inner.composite,
                constant_constrained_probe: constants[pk].is_some(),
                columns: columns.clone(),
                database: database.clone(),
                output_offsets: output_offsets.clone(),
                visible: inner.source_visible.clone(),
                // Go `indexJoinIntPKRangeInfo`: the OUTER keys, bare.
                range_info: format!(
                    "[{}]",
                    if inner.composite {
                        outer.names[outer_at(&keys[key])].clone()
                    } else {
                        physical_outer_column_name(outer, outer_at(&keys[key]))
                    }
                ),
                filters: filters.clone(),
                filter_exprs: filter_exprs.clone(),
                consumes_where,
                probe_cast: None,
            });
        }
    }

    // A clustered composite primary key is a table path, not a secondary
    // PRIMARY index. Constants and join keys may fix any non-empty leading
    // prefix; the resulting record-key range covers every remaining suffix.
    if !table.common_handle_offsets().is_empty() {
        if let Some((probe_keys, probe_parts, dynamic, static_parts, static_columns)) =
            probe_for(table.common_handle_offsets())
        {
            let range_info = format!(
                "[{}]",
                dynamic
                    .into_iter()
                    .chain(static_parts)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            let constant_constrained_probe = table
                .common_handle_offsets()
                .iter()
                .zip(&probe_parts)
                .any(|(offset, part)| {
                    matches!(part, crate::access_path::LookupProbePart::Dynamic(_))
                        && constants[*offset].is_some()
                });
            decisions.push(IndexJoinDecision {
                lookup_is_left,
                probe_keys,
                probe_parts,
                join_key_count: keys.len(),
                table: table.clone(),
                object: crate::access_path::LookupObject::CommonHandle,
                filter_selectivity: residual_filter_selectivity(
                    &filters,
                    &static_columns,
                    &columns,
                    table,
                    &inner.source_visible,
                    statistics,
                    &ctx.session_zone(),
                ),
                source_filter_selectivity,
                aggregation: inner.aggregation.clone(),
                aggregation_info: inner.aggregation_info.clone(),
                aggregation_final_info: inner.aggregation_final_info.clone(),
                aggregation_partial_info: inner.aggregation_partial_info.clone(),
                composite: inner.composite,
                constant_constrained_probe,
                columns: columns.clone(),
                database: database.clone(),
                output_offsets: output_offsets.clone(),
                visible: inner.source_visible.clone(),
                range_info,
                filters: filters.clone(),
                filter_exprs: filter_exprs.clone(),
                consumes_where,
                probe_cast: None,
            });
        }
    }

    // Otherwise the longest LEADING run of an index's columns that are all
    // join keys -- Go's `indexJoinPathTmpInit` walk, which stops at the first
    // index column no inner key covers.
    type IndexCandidate = (
        i64,
        Vec<usize>,
        Vec<crate::access_path::LookupProbePart>,
        Vec<String>,
        Vec<String>,
        Vec<usize>,
    );
    let mut best: Option<IndexCandidate> = None;
    for index in table.indexes() {
        if !index.visible
            || index.has_prefix()
            || (index.name.eq_ignore_ascii_case("PRIMARY")
                && (table.pk_handle_offset().is_some()
                    || !table.common_handle_offsets().is_empty()))
        {
            continue;
        }
        let Some((probe_keys, probe_parts, dynamic, static_parts, static_columns)) =
            probe_for(&index.column_offsets)
        else {
            continue;
        };
        if best
            .as_ref()
            .is_none_or(|(_, best, ..)| best.len() < probe_keys.len())
        {
            best = Some((
                index.id,
                probe_keys,
                probe_parts,
                dynamic,
                static_parts,
                static_columns,
            ));
        }
    }
    let Some((index_id, probe_keys, probe_parts, dynamic, static_parts, static_columns)) = best
    else {
        return decisions;
    };
    let Some(index) = table.indexes().iter().find(|index| index.id == index_id) else {
        return decisions;
    };
    let constant_constrained_probe =
        index
            .column_offsets
            .iter()
            .zip(&probe_parts)
            .any(|(offset, part)| {
                matches!(part, crate::access_path::LookupProbePart::Dynamic(_))
                    && constants[*offset].is_some()
            });
    // Go `indexJoinPathRangeInfo`: `eq(<index column>, <outer key>)` per
    // covered index column, in index order. The index column is Go's
    // `OrigName`, which an alias does not rename -- see [`JoinSide::origin`].
    let range_info = format!(
        "[{}]",
        dynamic
            .into_iter()
            .chain(static_parts)
            .collect::<Vec<_>>()
            .join(" ")
    );
    decisions.push(IndexJoinDecision {
        lookup_is_left,
        probe_keys,
        probe_parts,
        join_key_count: keys.len(),
        table: table.clone(),
        object: crate::access_path::LookupObject::Index(index_id),
        filter_selectivity: residual_filter_selectivity(
            &filters,
            &static_columns,
            &columns,
            table,
            &inner.source_visible,
            statistics,
            &ctx.session_zone(),
        ),
        source_filter_selectivity,
        aggregation: inner.aggregation.clone(),
        aggregation_info: inner.aggregation_info.clone(),
        aggregation_final_info: inner.aggregation_final_info.clone(),
        aggregation_partial_info: inner.aggregation_partial_info.clone(),
        composite: inner.composite,
        constant_constrained_probe,
        columns,
        database,
        output_offsets,
        visible: inner.source_visible.clone(),
        range_info,
        filters,
        filter_exprs,
        consumes_where,
        probe_cast: None,
    });
    decisions
}

/// One rewritten mismatched equality mapped onto a concrete lookup side --
/// [`crate::driver::join_key_cast`]'s product, in the child-local offsets the
/// executor reads.
pub(crate) struct CastLookupKey {
    /// Child-local offset of the INT column in the LOOKUP side's output.
    pub(crate) inner_offset: usize,
    /// Child-local offset of the STRING column in the OUTER side's output.
    pub(crate) outer_offset: usize,
    /// The computed key: `cast(str AS SIGNED)` and Go's guard.
    pub(crate) rewrite: crate::driver::join_key_cast::RewrittenEquality,
}

/// The lookup decision for Go's `rule_join_key_type_cast` shape: an INNER
/// join whose only usable equality pairs a signed INT column (the lookup
/// side's clustered handle) with a STRING column, made probeable by the
/// rule's `cast(str AS SIGNED)` key.
///
/// Deliberately NARROWER than `decide_over`, each refusal fail-closed:
///
/// * INNER joins only. Go's rule skips a preserved string side, and the
///   lookup refuses a preserved lookup side, so an outer join never
///   qualifies on both counts at once.
/// * the probed column must be the clustered INT handle -- Go's recorded
///   shape (`TableRangeScan range: decided by [Column#N]`); a secondary
///   index over the int column is NAMED RESIDUE.
/// * no leaf filters on the lookup side: the plain-column path threads them
///   into the probe with their selectivity, and this path would silently
///   drop them.
pub(crate) fn cast_lookup_decision(
    kind: crate::join::JoinKind,
    lookup_is_left: bool,
    key: CastLookupKey,
    inner: &JoinSide<'_>,
    rows: Option<&crate::driver::join_reorder::RowSource>,
) -> Option<IndexJoinDecision> {
    if kind != crate::join::JoinKind::Inner {
        return None;
    }
    let table = inner.table?;
    if table.partition().is_some()
        || inner.aggregation.is_some()
        || inner.composite
        || !inner.source_filters.is_empty()
    {
        return None;
    }
    if rows.is_some_and(|rows| {
        rows.filters_for(&inner.visible)
            .is_some_and(|filters| !filters.is_empty())
    }) {
        return None;
    }
    let database = inner
        .origin
        .as_deref()
        .and_then(|origin| origin.rsplit_once('.'))
        .map(|(database, _)| database.to_owned())?;
    if inner.output_to_source.len() != inner.types.len() {
        return None;
    }
    let output_offsets = inner
        .output_to_source
        .iter()
        .copied()
        .collect::<Option<Vec<_>>>()?;
    let columns: Vec<(String, FieldType)> = table
        .visible_columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    // The probed column must be the table's clustered INT handle.
    let source_offset = *output_offsets.get(key.inner_offset)?;
    if !table.is_clustered_handle_column(source_offset) {
        return None;
    }
    Some(IndexJoinDecision {
        lookup_is_left,
        probe_keys: Vec::new(),
        probe_parts: vec![crate::access_path::LookupProbePart::Dynamic(0)],
        join_key_count: 1,
        table: table.clone(),
        object: crate::access_path::LookupObject::Handle,
        filter_selectivity: 1.0,
        source_filter_selectivity: 1.0,
        aggregation: None,
        aggregation_info: None,
        aggregation_final_info: None,
        aggregation_partial_info: None,
        composite: false,
        constant_constrained_probe: false,
        columns,
        database,
        output_offsets,
        visible: inner.source_visible.clone(),
        // Go `indexJoinIntPKRangeInfo` prints the OUTER key column, which is
        // the rule's injected cast column and has no `OrigName`. The caller
        // patches in the numbered `Column#N` form when a plan trace carries
        // the statement's Go plan-column stream; this bare fallback is what
        // `format='plan_tree'` recordings show.
        range_info: format!("[{UNNAMED_COLUMN}]"),
        filters: Vec::new(),
        filter_exprs: Vec::new(),
        consumes_where: rows
            .is_some_and(crate::driver::join_reorder::RowSource::all_where_is_leaf_or_join_equality),
        probe_cast: Some(crate::join::IndexProbeCast {
            outer_offset: key.outer_offset,
            inner_offset: key.inner_offset,
            cast: key.rewrite.cast,
            guard: key.rewrite.guard,
            str_type: key.rewrite.str_type,
        }),
    })
}

/// Pushes a predicate on carried derived outputs down to the base row the
/// lookup source returns. A reference to a computed aggregate output refuses
/// the index strategy; applying it before aggregation would be wrong.
fn rewrite_derived_filter_to_source(
    filter: &tidb_ast::Expr,
    inner: &JoinSide<'_>,
    columns: &[(String, FieldType)],
) -> Option<tidb_ast::Expr> {
    struct Rewriter<'a, 'b> {
        inner: &'a JoinSide<'b>,
        columns: &'a [(String, FieldType)],
        failed: bool,
    }
    impl tidb_ast::Visitor for Rewriter<'_, '_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            match expr {
                tidb_ast::Expr::Subquery(_) | tidb_ast::Expr::Exists { .. } => {
                    self.failed = true;
                    true
                }
                tidb_ast::Expr::Column(path) => {
                    let Some(name) = path.last() else {
                        self.failed = true;
                        return true;
                    };
                    let output = self.inner.names.iter().position(|candidate| {
                        candidate
                            .rsplit('.')
                            .next()
                            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(name))
                    });
                    let Some(source) = output
                        .and_then(|output| self.inner.output_to_source.get(output))
                        .copied()
                        .flatten()
                    else {
                        self.failed = true;
                        return true;
                    };
                    *path = vec![
                        self.inner.source_visible.clone(),
                        self.columns[source].0.clone(),
                    ];
                    true
                }
                _ => false,
            }
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut rewritten = filter.clone();
    let mut rewriter = Rewriter {
        inner,
        columns,
        failed: false,
    };
    tidb_ast::Visitable::accept(&mut rewritten, &mut rewriter);
    (!rewriter.failed).then_some(rewritten)
}

/// Gives every base-column reference one spelling so a predicate copied out
/// of a derived table and the same predicate inferred above it deduplicate.
fn normalize_source_filter(
    filter: &mut tidb_ast::Expr,
    visible: &str,
    columns: &[(String, FieldType)],
) -> Option<()> {
    struct Rewriter<'a> {
        visible: &'a str,
        columns: &'a [(String, FieldType)],
        failed: bool,
    }
    impl tidb_ast::Visitor for Rewriter<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(tidb_ast::Expr::Column(path)) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            let Some(column) = path.last().and_then(|name| {
                self.columns
                    .iter()
                    .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            }) else {
                self.failed = true;
                return true;
            };
            *path = vec![self.visible.to_owned(), column.0.clone()];
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut rewriter = Rewriter {
        visible,
        columns,
        failed: false,
    };
    tidb_ast::Visitable::accept(filter, &mut rewriter);
    (!rewriter.failed).then_some(())
}

/// Rewrites leaf-local predicates against the full table row the lookup
/// source returns. `RowSource` has already classified them as belonging to
/// this leaf; a rewrite failure therefore means the physical and logical
/// scopes disagree, and the index strategy is refused.
fn rewrite_inner_filters(
    inner: &JoinSide<'_>,
    columns: &[(String, FieldType)],
    filters: &[tidb_ast::Expr],
    ctx: &crate::StmtContext,
) -> Option<Vec<Expression>> {
    let database = inner
        .origin
        .as_deref()
        .and_then(|origin| origin.rsplit_once('.'))
        .map(|(database, _)| database.to_owned());
    let mut scope = crate::plan_trace::PlanTrace::single_table_scope(
        &inner.source_visible,
        database,
        columns.to_vec(),
    );
    scope.zone = ctx.session_zone();
    let resolver = crate::driver::from::ScopeResolver { scope: &scope };
    filters
        .iter()
        .map(|filter| {
            let mut expression = rewrite_expr_resolved(filter, &resolver).ok()?;
            tidb_expr::builtin_compare::refine_comparisons(&mut expression, ctx).ok()?;
            Some(expression)
        })
        .collect()
}

/// Constant equalities available to the looked-up table, in table-column
/// order and already converted into each column's storage domain.
fn static_equalities(
    columns: &[(String, FieldType)],
    filters: &[tidb_ast::Expr],
    ctx: &crate::StmtContext,
) -> Vec<Option<Datum>> {
    let mut values = vec![None; columns.len()];
    for filter in filters {
        let mut pairs = Vec::new();
        if !crate::driver::access::name_value_pairs(filter, &mut pairs, &ctx.session_zone())
            || pairs.len() != 1
            || !crate::driver::access::convert_pairs_to_column_domain(&mut pairs, columns)
        {
            continue;
        }
        let pair = &pairs[0];
        if let Some(offset) = columns
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case(pair.column()))
        {
            values[offset] = Some(pair.value().clone());
        }
    }
    values
}

/// Pseudo selectivity of the inner filters not already represented by static
/// key parts. Go leaves the static equality visible in the cop Selection but
/// prices it in the range only once.
pub(crate) fn residual_filter_selectivity(
    filters: &[tidb_ast::Expr],
    static_columns: &[usize],
    columns: &[(String, FieldType)],
    table: &KvTable,
    visible: &str,
    statistics: Option<&crate::access_cost::TableStatistics>,
    zone: &tidb_datatype::SessionTimeZone,
) -> f64 {
    let residual: Vec<&tidb_ast::Expr> = filters
        .iter()
        .filter(|filter| {
            let mut pairs = Vec::new();
            if !crate::driver::access::name_value_pairs(filter, &mut pairs, zone)
                || pairs.len() != 1
            {
                return true;
            }
            !static_columns.iter().any(|offset| {
                columns
                    .get(*offset)
                    .is_some_and(|(name, _)| pairs[0].column().eq_ignore_ascii_case(name))
            })
        })
        .collect();
    if residual.is_empty() {
        1.0
    } else {
        let mut scope =
            crate::plan_trace::PlanTrace::single_table_scope(visible, None, columns.to_vec());
        scope.zone = zone.clone();
        let resolver = crate::driver::from::ScopeResolver { scope: &scope };
        let selectivity =
            crate::access_cost::selectivity_of_conjuncts(&residual, table, &resolver, statistics);
        // Go's cardinality.Selectivity applies the session SelectionFactor to a
        // residual predicate that has no usable statistics (for example the
        // q4 column-to-column date comparison). Keep the access-before-filter
        // estimate distinct from the logical rows that survive the Selection.
        if selectivity >= 1.0 {
            crate::plan_trace::SELECTIVITY_FACTOR
        } else {
            selectivity
        }
    }
}

fn datum_text(value: &Datum) -> String {
    match value {
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Bytes(value) => format!("\"{}\"", String::from_utf8_lossy(value)),
        other => other.sql_string().unwrap_or_else(|_| format!("{other:?}")),
    }
}

/// One looked-up column as `EXPLAIN` prints it INSIDE a range, which is Go's
/// `OrigName`: `<database>.<table>.<column>` written under the TABLE's own
/// name, never the alias the side is read under.
///
/// The scope-qualified name is the fallback for a side whose origin was not
/// resolved; a decision is only ever reached for a side that HAS one, so the
/// fallback is unreachable in practice rather than a second spelling.
fn inner_column_name(inner: &JoinSide<'_>, column: usize) -> String {
    match (&inner.origin, inner.table) {
        (Some(origin), Some(table)) => format!(
            "{}.{}",
            origin.to_lowercase(),
            table.columns[column].name.to_lowercase()
        ),
        _ => inner.names[column].clone(),
    }
}

/// Go's dynamic IndexJoin range text uses the outer column's `OrigName`, not
/// the relation alias through which SQL name resolution reached it.
fn physical_outer_column_name(outer: &JoinSide<'_>, output: usize) -> String {
    outer
        .origin
        .as_ref()
        .zip(outer.table)
        .zip(outer.output_to_source.get(output).copied().flatten())
        .and_then(|((origin, table), source)| {
            table
                .visible_columns()
                .get(source)
                .map(|column| format!("{}.{}", origin.to_lowercase(), column.name.to_lowercase()))
        })
        .unwrap_or_else(|| outer.names[output].clone())
}

/// Whether an outer value of type `outer` IS a probe of an indexed column of
/// type `inner`, with no conversion in between.
///
/// See the module doc: this replaces Go's `ConvertTo` + `Compare` per value
/// with a type check made once. `flen` and `decimal` are deliberately NOT
/// compared -- an `int` column and `t.b * 2` differ there and encode
/// identically -- while the type code, the signedness and, for strings, the
/// collation decide the bytes an index entry was written with.
fn probe_compatible(inner: &FieldType, outer: &FieldType) -> bool {
    if inner.is_unsigned() != outer.is_unsigned() {
        // An unsigned index column stores its entries under the unsigned
        // encoding; a signed probe would ask the wrong bytes for the same
        // number. Go converts and compares per value instead. NAMED RESIDUE.
        return false;
    }
    if inner.code().is_type_integer() && outer.code().is_type_integer() {
        // Every integer width shares ONE index encoding, so a `BIGINT`
        // expression probes an `INT` column's index with its own value and no
        // conversion. A value the narrower column cannot hold simply has no
        // entry -- which is the same answer Go reaches by dropping the row
        // when `ConvertTo` overflows, arrived at through the index rather
        // than through a check.
        return true;
    }
    inner.code() == outer.code()
        && inner
            .collation_name()
            .eq_ignore_ascii_case(outer.collation_name())
}

/// The two sides of a join as the decision reads them, built from the scope
/// the join produced and the executors its children became.
pub(crate) fn join_sides<'a>(
    join: &tidb_ast::Join,
    keys: &[EquiKey],
    scope: &FromScope,
    current_db: &str,
    left_width: usize,
    catalog: &'a Catalog,
    left_types: &[FieldType],
    right_types: &[FieldType],
) -> (JoinSide<'a>, JoinSide<'a>) {
    let left = side_of(
        &join.left, scope, current_db, catalog, left_types, 0, left_width,
    );
    let right = match &join.right {
        Some(node) => side_of(
            node,
            scope,
            current_db,
            catalog,
            right_types,
            left_width,
            scope.width(),
        ),
        None => JoinSide {
            table: None,
            visible: String::new(),
            types: Vec::new(),
            names: Vec::new(),
            origin: None,
            source_visible: String::new(),
            output_to_source: Vec::new(),
            source_filters: Vec::new(),
            aggregation: None,
            aggregation_info: None,
            aggregation_final_info: None,
            aggregation_partial_info: None,
            composite: false,
        },
    };
    (
        discover_composite_target(
            left,
            &join.left,
            &keys.iter().map(|key| key.left).collect::<Vec<_>>(),
            catalog,
            current_db,
        ),
        discover_composite_target(
            right,
            join.right.as_ref().expect("right side exists"),
            &keys.iter().map(|key| key.right).collect::<Vec<_>>(),
            catalog,
            current_db,
        ),
    )
}

/// Finds the physical table leaf that carries a join key through a composite
/// side. The side keeps its full output names, while `output_to_source` marks
/// only columns that can be mapped back to this target table.
fn discover_composite_target<'a>(
    mut side: JoinSide<'a>,
    node: &tidb_ast::JoinNode,
    key_outputs: &[usize],
    catalog: &'a Catalog,
    current_db: &str,
) -> JoinSide<'a> {
    if side.table.is_some() {
        return side;
    }
    let key_names = key_outputs
        .iter()
        .filter_map(|output| side.names.get(*output))
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut tables = Vec::new();
    collect_index_join_inner_tables(node, &key_names, &mut tables);
    for table_ref in tables {
        if !table_ref.partitions.is_empty()
            || table_ref.as_of.is_some()
            || !table_ref.hints.is_empty()
            || table_ref.sample.is_some()
        {
            continue;
        }
        let Ok((database, name)) =
            crate::driver::catalog::split_table_path(&table_ref.name, current_db)
        else {
            continue;
        };
        let Some(TableEntry::Kv(table)) = catalog.get_in(database, name) else {
            continue;
        };
        let origin = format!("{database}.{name}");
        let matches = side
            .names
            .iter()
            .enumerate()
            .filter_map(|(output, rendered)| {
                let suffix = rendered
                    .strip_prefix(&format!("{origin}."))
                    .or_else(|| rendered.strip_prefix(&format!("{}.", origin.to_lowercase())))?;
                let source = table
                    .visible_columns()
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(suffix))?;
                Some((output, source))
            })
            .collect::<Vec<_>>();
        if !matches
            .iter()
            .any(|(output, _)| key_outputs.contains(output))
        {
            continue;
        }
        side.table = Some(table);
        side.source_visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
        side.origin = Some(origin);
        side.output_to_source = vec![None; side.names.len()];
        for (output, source) in matches {
            side.output_to_source[output] = Some(source);
        }
        side.composite = true;
        return side;
    }
    side
}

/// Every table an index join may re-seed on this side: Go
/// `admitIndexJoinInnerChildPattern`.
///
/// Go walks DOWN from the join and asks of each operator whether it may sit
/// between the join and the `DataSource` it re-seeds. `DataSource`,
/// `Projection`, `Selection` and `UnionScan` may; a `LogicalJoin` may only if
/// it is an INNER join; a `LogicalAggregation` may only if
/// `checkIndexJoinInnerTaskWithAgg` holds. Everything else takes the
/// function's `default` arm and is refused, with the reason stated there:
/// "index join inner side couldn't allow join, sort, limit, because they are
/// Optimization Fence".
///
/// Walking to any table that merely CARRIES the key by name, as this used to,
/// re-seeds a leaf through operators that change what the leaf's rows mean.
/// `group by t2.a with rollup` was the visible one: the probe key reached
/// `t2` through the rollup's `Expand`, which Go's switch does not name at all.
fn collect_index_join_inner_tables<'a>(
    node: &'a tidb_ast::JoinNode,
    key_names: &[&str],
    out: &mut Vec<&'a tidb_ast::TableRef>,
) {
    match node {
        tidb_ast::JoinNode::Table(table) => out.push(table),
        // Go admits a `LogicalJoin` only when it is an INNER join; `Cross` is
        // this AST's spelling for `JOIN`/`INNER JOIN`/`CROSS JOIN`/the comma
        // join. An AST join node with no `right` is the single-relation
        // wrapper, which is no join at all.
        tidb_ast::JoinNode::Join(join) => {
            if join.right.is_some() && !matches!(join.tp, tidb_ast::JoinType::Cross) {
                return;
            }
            collect_index_join_inner_tables(&join.left, key_names, out);
            if let Some(right) = &join.right {
                collect_index_join_inner_tables(right, key_names, out);
            }
        }
        tidb_ast::JoinNode::Derived { subquery, .. } => {
            let tidb_ast::QueryStmt::Select(select) = &**subquery else {
                // A set operation is none of the admitted operators.
                return;
            };
            if !derived_admits_index_join_inner(select, key_names) {
                return;
            }
            if let Some(from) = &select.from {
                collect_index_join_inner_tables(&from.left, key_names, out);
                if let Some(right) = &from.right {
                    collect_index_join_inner_tables(right, key_names, out);
                }
            }
        }
    }
}

/// Whether the operators a derived table stands for are ones Go admits
/// between an index join and the leaf it re-seeds.
///
/// `WITH ROLLUP` is an `Expand`, a window spec is a `Window`, and `LIMIT` is a
/// `Limit`; none is in Go's switch. The remaining clauses build a
/// `Projection`, a `Selection` (`WHERE`/`HAVING`) or a `LogicalAggregation`,
/// which Go admits -- the aggregation only under
/// [`group_keys_cover`].
fn derived_admits_index_join_inner(select: &tidb_ast::SelectStmt, key_names: &[&str]) -> bool {
    if select.with.is_some()
        || !select.values.is_empty()
        || select.rollup
        || select.limit.is_some()
        || !select.windows.is_empty()
    {
        return false;
    }
    // `DISTINCT` groups by the whole output row, so any output column is a
    // group key and the cover below is trivially met.
    if select.group_by.is_empty() {
        return true;
    }
    group_keys_cover(&select.group_by, key_names)
}

/// Go `checkIndexJoinInnerTaskWithAgg`: every inner join key that comes from
/// the re-seeded `DataSource` must also be a GROUP BY key.
///
/// Otherwise the probe splits the aggregate's groups -- Go's own words,
/// "the aggregation group might be split into multiple groups by the join
/// keys, which generate incorrect result". Go compares `UniqueID`s and says
/// it is deliberately conservative for GROUP BY EXPRESSIONS, rejecting valid
/// plans rather than risking a wrong one; comparing written column names here
/// is the same trade at this tier, and a non-column group item refuses for
/// the same reason.
fn group_keys_cover(group_by: &[tidb_ast::GroupByItem], key_names: &[&str]) -> bool {
    let group_columns = group_by
        .iter()
        .map(|item| match &item.expr {
            tidb_ast::Expr::Column(path) => path.last().map(String::as_str),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    let Some(group_columns) = group_columns else {
        return false;
    };
    key_names.iter().all(|name| {
        let column = name.rsplit('.').next().unwrap_or(name);
        group_columns
            .iter()
            .any(|group| group.eq_ignore_ascii_case(column))
    })
}

fn side_of<'a>(
    node: &tidb_ast::JoinNode,
    scope: &FromScope,
    current_db: &str,
    catalog: &'a Catalog,
    types: &[FieldType],
    from: usize,
    to: usize,
) -> JoinSide<'a> {
    let computed = computed_columns(node, to - from);
    let mut names: Vec<String> = (from..to)
        .map(|offset| {
            if computed.get(offset - from).copied().unwrap_or(false) {
                UNNAMED_COLUMN.to_owned()
            } else {
                crate::driver::from::qualified_scope_column(scope, current_db, offset)
            }
        })
        .collect();
    let relation_visible = relation_visible_name(node);
    let derived = grouped_aggregate_of(node, catalog, current_db);
    let read = derived
        .as_ref()
        .map(|derived| {
            (
                derived.table,
                derived.source_visible.clone(),
                derived.origin.clone(),
            )
        })
        .or_else(|| single_table_of(node, catalog, current_db));
    let visible = relation_visible.or_else(|| {
        read.as_ref()
            .map(|(_, source_visible, _)| source_visible.clone())
    });
    let origin = read.as_ref().map(|(_, _, origin)| origin.clone());
    let source_visible = read
        .as_ref()
        .map_or_else(String::new, |(_, visible, _)| visible.clone());
    let output_to_source = derived.as_ref().map_or_else(
        || {
            read.as_ref().map_or_else(
                || vec![None; types.len()],
                |(table, ..)| {
                    (from..to)
                        .map(|output| {
                            let (name, _) = scope.column_at(output)?;
                            table
                                .visible_columns()
                                .iter()
                                .position(|column| column.name.eq_ignore_ascii_case(name))
                        })
                        .collect()
                },
            )
        },
        |derived| derived.output_to_source.clone(),
    );
    if let Some(derived) = &derived {
        for (output, source) in output_to_source.iter().copied().enumerate() {
            if let Some(source) = source {
                names[output] = format!(
                    "{}.{}",
                    derived.origin,
                    derived.table.visible_columns()[source].name
                );
            }
        }
    }
    for (output, name) in names.iter_mut().enumerate() {
        let Some(path) = scope.qualified_path(from + output) else {
            continue;
        };
        let [.., relation, column] = path.as_slice() else {
            continue;
        };
        if let Some(physical) = super::merge_decision::physical_column_trace_name(
            node,
            &super::merge_decision::RelColumn {
                relation: relation.clone(),
                column: column.clone(),
            },
            catalog,
            current_db,
        ) {
            *name = physical;
        }
    }
    JoinSide {
        table: read.map(|(kv, ..)| kv),
        visible: visible.unwrap_or_default(),
        types: types.to_vec(),
        names,
        origin,
        source_visible,
        output_to_source,
        source_filters: derived
            .as_ref()
            .map_or_else(Vec::new, |derived| derived.filters.clone()),
        aggregation: derived.as_ref().map(|derived| derived.aggregation.clone()),
        aggregation_info: derived
            .as_ref()
            .map(|derived| derived.aggregation_info.clone()),
        aggregation_final_info: derived
            .as_ref()
            .map(|derived| derived.aggregation_final_info.clone()),
        aggregation_partial_info: derived
            .as_ref()
            .map(|derived| derived.aggregation_partial_info.clone()),
        composite: false,
    }
}

fn relation_visible_name(node: &tidb_ast::JoinNode) -> Option<String> {
    match node {
        tidb_ast::JoinNode::Table(table) => {
            table.alias.clone().or_else(|| table.name.last().cloned())
        }
        tidb_ast::JoinNode::Derived { alias, .. } => alias.clone(),
        tidb_ast::JoinNode::Join(join)
            if join.right.is_none()
                && join.on.is_none()
                && join.using.is_empty()
                && !join.natural =>
        {
            relation_visible_name(&join.left)
        }
        tidb_ast::JoinNode::Join(_) => None,
    }
}

struct GroupedAggregateLookup<'a> {
    table: &'a KvTable,
    source_visible: String,
    origin: String,
    output_to_source: Vec<Option<usize>>,
    filters: Vec<tidb_ast::Expr>,
    aggregation: crate::join::IndexLookupAggregation,
    aggregation_info: String,
    aggregation_final_info: String,
    aggregation_partial_info: String,
}

/// A grouped derived side Go can rebuild over a re-seeded index reader.
///
/// This is intentionally the smallest truthful rule: one base table, plain
/// non-null integer group keys, carried group columns, and the COUNT, MAX, or
/// exact decimal SUM aggregates used by the Go TPCC plans. Unsupported clauses
/// or expressions keep the ordinary materialized join.
fn grouped_aggregate_of<'a>(
    node: &tidb_ast::JoinNode,
    catalog: &'a Catalog,
    current_db: &str,
) -> Option<GroupedAggregateLookup<'a>> {
    let tidb_ast::JoinNode::Derived {
        subquery,
        lateral: false,
        column_names,
        ..
    } = node
    else {
        return None;
    };
    if !column_names.is_empty() {
        return None;
    }
    let tidb_ast::QueryStmt::Select(select) = &**subquery else {
        return None;
    };
    if select.with.is_some()
        || !select.values.is_empty()
        || select.distinct
        || select.rollup
        || select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || !select.windows.is_empty()
    {
        return None;
    }
    let from = select.from.as_ref()?;
    if from.right.is_some() || from.on.is_some() || !from.using.is_empty() || from.natural {
        return None;
    }
    let (table, source_visible, origin) = single_table_of(&from.left, catalog, current_db)?;
    let column_offset = |path: &[String]| {
        let name = path.last()?;
        table
            .visible_columns()
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
    };
    let group_offsets = select
        .group_by
        .iter()
        .map(|item| match &item.expr {
            tidb_ast::Expr::Column(path) => column_offset(path),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    if group_offsets.iter().any(|offset| {
        let field_type = &table.visible_columns()[*offset].field_type;
        !field_type.code().is_type_integer()
            || !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
    }) {
        return None;
    }
    let group_set = group_offsets
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut input_offsets = std::collections::BTreeSet::new();
    let mut output_to_source = Vec::with_capacity(select.fields.fields().len());
    let mut outputs = Vec::with_capacity(select.fields.fields().len());
    let mut aggregate_functions = Vec::new();
    let mut final_aggregate_functions = Vec::new();
    let mut first_row_functions = Vec::new();
    let physical_name =
        |offset: usize| format!("{origin}.{}", table.visible_columns()[offset].name);
    for field in select.fields.fields() {
        match field {
            tidb_ast::SelectField::Expr {
                expr: tidb_ast::Expr::Column(path),
                ..
            } => {
                let offset = column_offset(path)?;
                if !group_set.contains(&offset) {
                    return None;
                }
                output_to_source.push(Some(offset));
                outputs.push(crate::join::IndexLookupAggregateOutput::Column(offset));
                let name = physical_name(offset);
                first_row_functions.push((offset, format!("funcs:firstrow({name})->{name}")));
            }
            tidb_ast::SelectField::Expr {
                expr:
                    tidb_ast::Expr::Aggregate {
                        name,
                        distinct: false,
                        args,
                    },
                ..
            } if name.eq_ignore_ascii_case("COUNT") => {
                let offset = match args.as_slice() {
                    [tidb_ast::Expr::Int(_)] => None,
                    [tidb_ast::Expr::Column(path)] => Some(column_offset(path)?),
                    _ => return None,
                };
                if let Some(offset) = offset {
                    input_offsets.insert(offset);
                }
                output_to_source.push(None);
                outputs.push(crate::join::IndexLookupAggregateOutput::Count(offset));
                let input = offset.map_or_else(|| "1".to_owned(), physical_name);
                aggregate_functions.push(format!(
                    "funcs:count({input})->Column#{}",
                    aggregate_functions.len()
                ));
                final_aggregate_functions.push(format!(
                    "funcs:count(Column#{0})->Column#{0}",
                    final_aggregate_functions.len()
                ));
            }
            tidb_ast::SelectField::Expr {
                expr:
                    tidb_ast::Expr::Aggregate {
                        name,
                        distinct: false,
                        args,
                    },
                ..
            } if name.eq_ignore_ascii_case("MAX") => {
                let [tidb_ast::Expr::Column(path)] = args.as_slice() else {
                    return None;
                };
                let offset = column_offset(path)?;
                input_offsets.insert(offset);
                output_to_source.push(None);
                outputs.push(crate::join::IndexLookupAggregateOutput::Max {
                    offset,
                    collation: table.visible_columns()[offset].field_type.collation(),
                });
                aggregate_functions.push(format!(
                    "funcs:max({})->Column#{}",
                    physical_name(offset),
                    aggregate_functions.len()
                ));
                final_aggregate_functions.push(format!(
                    "funcs:max(Column#{0})->Column#{0}",
                    final_aggregate_functions.len()
                ));
            }
            tidb_ast::SelectField::Expr {
                expr:
                    tidb_ast::Expr::Aggregate {
                        name,
                        distinct: false,
                        args,
                    },
                ..
            } if name.eq_ignore_ascii_case("SUM") => {
                let [tidb_ast::Expr::Column(path)] = args.as_slice() else {
                    return None;
                };
                let offset = column_offset(path)?;
                if table.visible_columns()[offset].field_type.code()
                    != tidb_datatype::FieldTypeCode::NewDecimal
                {
                    return None;
                }
                input_offsets.insert(offset);
                output_to_source.push(None);
                outputs.push(crate::join::IndexLookupAggregateOutput::DecimalSum(offset));
                aggregate_functions.push(format!(
                    "funcs:sum({})->Column#{}",
                    physical_name(offset),
                    aggregate_functions.len()
                ));
                final_aggregate_functions.push(format!(
                    "funcs:sum(Column#{0})->Column#{0}",
                    final_aggregate_functions.len()
                ));
            }
            _ => return None,
        }
    }
    let mut groups = group_offsets
        .iter()
        .map(|offset| physical_name(*offset))
        .collect::<Vec<_>>();
    groups.sort_unstable();
    let aggregate_precedes_carriers = select
        .fields
        .fields()
        .iter()
        .position(|field| {
            matches!(field, tidb_ast::SelectField::Expr { expr, .. } if expr.has_aggregate_flag())
        })
        .zip(select.fields.fields().iter().position(|field| {
            matches!(field,
                tidb_ast::SelectField::Expr { expr: tidb_ast::Expr::Column(path), .. }
                if select.group_by.iter().any(|item| {
                    matches!(&item.expr, tidb_ast::Expr::Column(group) if group == path)
                })
            )
        }))
        .is_some_and(|(aggregate, carrier)| aggregate < carrier);
    if !aggregate_precedes_carriers {
        first_row_functions.sort_by_key(|(offset, _)| *offset);
    }
    let first_row_functions = first_row_functions
        .into_iter()
        .map(|(_, function)| function)
        .collect::<Vec<_>>();
    let aggregation_partial_info = if aggregate_functions.is_empty() {
        format!("group by:{}", groups.join(", "))
    } else {
        format!(
            "group by:{}, {}",
            groups.join(", "),
            aggregate_functions.join(", ")
        )
    };
    let order_functions = |mut aggregates: Vec<String>, mut carriers: Vec<String>| {
        if super::derived_agg_pruning::has_pruned_row_count(select) {
            carriers.extend(aggregates);
            carriers
        } else {
            aggregates.extend(carriers);
            aggregates
        }
    };
    let functions = order_functions(aggregate_functions, first_row_functions.clone());
    let final_functions = order_functions(final_aggregate_functions, first_row_functions);
    let aggregation_info = format!("group by:{}, {}", groups.join(", "), functions.join(", "));
    let aggregation_final_info = format!(
        "group by:{}, {}",
        groups.join(", "),
        final_functions.join(", ")
    );
    let mut filters = Vec::new();
    if let Some(predicate) = &select.where_clause {
        ast_conjuncts(predicate, &mut filters);
    }
    Some(GroupedAggregateLookup {
        table,
        source_visible,
        origin,
        output_to_source,
        filters,
        aggregation: crate::join::IndexLookupAggregation {
            group_offsets,
            input_offsets: input_offsets.into_iter().collect(),
            outputs,
            pruned_row_count: super::derived_agg_pruning::has_pruned_row_count(select),
        },
        aggregation_info,
        aggregation_final_info,
        aggregation_partial_info,
    })
}

fn ast_conjuncts(expr: &tidb_ast::Expr, out: &mut Vec<tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, left, right) => {
            ast_conjuncts(left, out);
            ast_conjuncts(right, out);
        }
        expr => out.push(expr.clone()),
    }
}

/// The base table `node` reads whole, the name it is written under, and its
/// own `<database>.<table>` qualification -- or `None` for every other shape.
fn single_table_of<'a>(
    node: &tidb_ast::JoinNode,
    catalog: &'a Catalog,
    current_db: &str,
) -> Option<(&'a KvTable, String, String)> {
    // `FROM a, b` wraps its left relation in a single-child join node, the
    // same peeling `crate::column_prune` does.
    let mut node = node;
    while let tidb_ast::JoinNode::Join(inner) = node {
        if inner.right.is_some() || inner.on.is_some() || !inner.using.is_empty() || inner.natural {
            return None;
        }
        node = &inner.left;
    }
    let tidb_ast::JoinNode::Table(table_ref) = node else {
        return None;
    };
    // A named partition list, an `AS OF`, or an index hint all change what
    // the read is; none is read here, so none may be silently ignored.
    if !table_ref.partitions.is_empty()
        || table_ref.as_of.is_some()
        || !table_ref.hints.is_empty()
        || table_ref.sample.is_some()
    {
        return None;
    }
    let (database, name) =
        crate::driver::catalog::split_table_path(&table_ref.name, current_db).ok()?;
    let entry = catalog.get_in(database, name)?;
    let TableEntry::Kv(kv) = entry else {
        return None;
    };
    let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
    Some((kv, visible, format!("{database}.{name}")))
}

/// Which of a side's columns `EXPLAIN` prints as a bare `Column`, one flag
/// per column in row order.
///
/// Go carries `Column.OrigName` from the base column a projection merely
/// passes through, and leaves it EMPTY for a projected expression -- so
/// `t2.a AS key_a` still prints `<db>.t2.a` while `t2.b * 2 AS doubled_b`
/// prints `Column`. This tier's scope keeps only the name a column answers
/// to, so the distinction is read back off the derived table's own select
/// list, which is where it was made.
///
/// Everything that is not a derived table with an explicit, wildcard-free
/// select list of the expected width answers "no column is computed" -- a
/// refusal, since the index-join gate reads this as a REQUIREMENT.
fn computed_columns(node: &tidb_ast::JoinNode, width: usize) -> Vec<bool> {
    let none = vec![false; width];
    let tidb_ast::JoinNode::Derived { subquery, .. } = node else {
        return none;
    };
    let tidb_ast::QueryStmt::Select(select) = &**subquery else {
        return none;
    };
    let fields = select.fields.fields();
    if fields.len() != width {
        return none;
    }
    fields
        .iter()
        .map(|field| match field {
            // A wildcard expands to base columns, none of them computed.
            tidb_ast::SelectField::Wildcard(_) => false,
            // Go keeps `OrigName` through a plain column reference and
            // through nothing else -- not through a cast, not through an
            // arithmetic expression, not through a function call.
            tidb_ast::SelectField::Expr { expr, .. } => !matches!(expr, tidb_ast::Expr::Column(_)),
        })
        .collect()
}
