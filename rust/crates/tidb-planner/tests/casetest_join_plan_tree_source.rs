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

//! Port ledger for the head of `pkg/planner/core/casetest/join/
//! join_test.go` (`pkg/planner.part5`, items 295–300 of all `Test*`/
//! `Benchmark*` declarations under `pkg/planner/` on `origin/master`,
//! sorted by file path then line; item 301 is that package's TestMain
//! bootstrap and stays with the next batch).
//!
//! Family contract: join planning surfaces pinned through
//! `explain format = 'plan_tree'` trees plus result rows under the
//! cascades matrix: semi-join build-side hint honoring, NULL-safe-equal
//! (`nulleq`) join keys, big IN-list predicate placement,
//! tidb_opt_always_keep_join_key retention, and a long regression tail.
//!
//! All six items are honest gap ports: no SQL optimize/explain pipeline
//! exists in this crate, so these plan-tree goldens have no carrier;
//! related partial carriers that DO exist (e.g. `Join.has_null_eq` in
//! src/find_best_task.rs:209) are noted per-test without pretending they
//! can replay full plans.

/// GO PORT of `join_test.go:27 TestSemiJoinOrder`.
///
/// Re-derived contract: two-row-column tables seeded with null-heavy data;
/// `where exists` semi-join over ordered rows returns the same 7 sorted
/// rows for hash-join-version optimized AND legacy (:40-52/:85-88);
/// HASH_JOIN_BUILD hints keep semantics (only build side swaps, probe rows
/// identical :43/:45/:87) — optimized version renders the hinted side as
/// Build (:48-59) while legacy refuses the hints for semi joins emitting
/// TWO copies of `Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE
/// hints are not supported for semi join with hash join version 1...`
/// (:78-84).
#[test]
#[ignore = "go-parity-gap: needs semi-join planning + hash-join version vars + explain renderer"]
fn semi_join_order_hints_and_hash_join_version_matrix() {}

/// GO PORT of `join_test.go:96 TestJoinWithNullEQ`.
///
/// Re-derived contract: issue #57583 — INTERSECT is planned as a semi HashJoin
/// whose equal condition prints `nulleq(test.t1.id, test.t1.id)` over an
/// aggregated build side (verbatim tree :102-118); issue #60322 — LEFT JOIN
/// subquery joined to tt0 via `<=>` collapses to an inner HashJoin keyed by
/// nulleq(Column, test.tt0.c0) above cast-projections (:122-141) and the
/// query itself returns zero rows (NULL c0 never matches, :142-146).
/// The crate carries the consuming flag (`has_null_eq`,
/// src/find_best_task.rs:209/:461) but not the plan tree renderer.
#[test]
#[ignore = "go-parity-gap: needs intersect-to-semi-join rewrite + <=> key propagation + explain output"]
fn join_with_null_eq_plans_intersect_and_leftjoin_collapses() {}

/// GO PORT of `join_test.go:146 TestJoinSimplifyCondition`.
///
/// Re-derived contract: `t1.a=t2.a and t1.b=1 or 1=2` simplifies to the
/// IndexHashJoin tree pinned verbatim (:153-162) — `or 1=2` folds away via
/// constant truth-value, leaving eq conditions intact; separately a 10001-
/// element IN-list under INL_HASH_JOIN must stay in a ROOT Selection(Probe)
/// (`or(eq(test.t2.b, 1), in(test.t2.c...)`) rather than being pushed to
/// cop[tikv], with the plan remaining in the index-join family (:164-185,
/// largeInListThreshold=10000 const :165-166).
#[test]
#[ignore = "go-parity-gap: needs constant-folding join simplify + IN-list pushdown boundary + explain"]
fn join_simplify_condition_folds_or_true_false_and_limits_inlist_pushdown() {}

/// GO PORT of `join_test.go:188 TestKeepingJoinKeys`.
///
/// Re-derived contract: with @@tidb_opt_always_keep_join_key=true the join
/// key columns survive into child selections even when predicates already
/// implied them: left-outer join filtered on outer key keeps BOTH children's
/// `eq(col, 1)` selections (verbatim tree :194-204); filtering the inner
/// side flips the join to inner while keeping keys (:206-216); plain inner
/// join with WHERE keeps them too (:218-228).
#[test]
#[ignore = "go-parity-gap: needs always_keep_join_key sysvar + selection construction inside planning"]
fn keeping_join_keys_retains_key_predicates_under_sysvar() {}

/// GO PORT of `join_test.go:226 TestJoinRegression`.
///
/// Re-derived contract: long tail of regression queries each pinned:
/// #46556 natural join vs NULL-view LIKE plans to zero-row dual HashJoin
/// (verbatim :233-243); #65325 CASE DEFAULT() order-by runs clean :246;
/// #67731 cross-type equality `'9007199254740993' = 9007199254740992` is 1
/// and join matches one row :250-256; #63949 tidb_inlj + use_index abcd
/// honored :258-260; #61669 deep view aggregation join explains >0 rows
/// :262-333; #60076/#63314 leading-hint chains under
/// always_keep_join_key produce verbatim trees without warnings :335-370;
/// #67366 int/varchar PK join inserts an implicit bigint cast and warns
/// logicalop/logical_join.go:1924 text :372-377; #66859 expression-index
/// partition left join yields `-1 <nil>` :380-385.
#[test]
#[ignore = "go-parity-gap: eight executor+planner regressions need full SQL stack + explain"]
fn join_regression_batch_pins_issue_trees_and_rows() {}

/// GO PORT of `join_test.go:377 TestIndexJoinInnerRowCountUsesUsableJoinKeys`.
///
/// Re-derived contract: t1(k1,k2) probes t2 whose clustered PK (k1,id)
/// can only USE the k1 prefix (~1000 rows/probe) but secondary idx_k1_k2
/// uses both keys (1 row/probe); analyzed stats make the default plan pick
/// the idx_k1_k2 path (`explain ... CheckContain("idx_k1_k2")` :398);
/// setting fix-control '44855:OFF' restores the old post-join-cardinality
/// estimate so the PK range-scan wins again (CheckNotContain :400); clearing
/// the variable re-enables the fix. FIX_44855 exists as a constant here
/// (src/fix_control.rs:38) with no consumer yet.
#[test]
#[ignore = "go-parity-gap: needs index-join range sizing consumer of Fix44855 + analyze stats harness"]
fn index_join_inner_row_count_uses_usable_join_keys_fix_44855() {}
