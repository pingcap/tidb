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

//! The topics enrolled on the replay gate, shared by every comparator that
//! runs them.
//!
//! The list lived inside `integration_diff` while it had one reader. It moved
//! here when `join_shape` became the second: two comparators replaying two
//! different lists would report two different corpora, and the smaller of the
//! two numbers would silently be the one a claim rested on. One list, read by
//! both, is the only arrangement in which "across the enrolled topics" means
//! the same thing in both reports.
#![allow(dead_code)]

/// The onboarded topics, chosen from `survey_unonboarded_topics`'s own
/// ranking rather than by name: each replays far enough that its remaining
/// divergences are a countable list with named causes, so a regression
/// anywhere in these areas turns the gate red.
///
/// All but one had ZERO divergences when they were onboarded. The exception,
/// `explain_easy`, is deliberately on the list at a cost: it is the only topic
/// here dense enough in plan text to prove the access-property comparison
/// works at all (53 of its plans match), and it contributes the whole of the
/// carried access-path debt below.
///
/// The reason recorded with each is what the topic buys that the topics
/// already on the list do not.
pub const TOPICS: &[(&str, &str)] = &[
    (
        "planner/core/join_reorder_through_projection",
        "the largest zero-divergence topic in the suite: join reorder through a \
         projection, row results and access properties together",
    ),
    (
        "util/admin",
        "the best-covered topic by ratio -- ADMIN's own row results over real tables",
    ),
    (
        "naaj",
        "null-aware anti join: the NULL semantics of NOT IN / != ALL over a subquery",
    ),
    (
        "planner/funcdep/only_full_group_by",
        "ONLY_FULL_GROUP_BY: which GROUP BY queries are accepted and which are refused",
    ),
    (
        "explain_easy",
        "the suite's plainest EXPLAIN topic -- the access-property comparison's own \
         proving ground",
    ),
    (
        "planner/core/rule_outer2inner",
        "outer-join-to-inner conversion, where a WHERE on the null-extended side \
         changes the answer",
    ),
    (
        "subquery",
        "correlated and uncorrelated subqueries in every clause",
    ),
    (
        "session/user_variables",
        "every statement compares: user variables end to end, nothing skipped",
    ),
    (
        "globalindex/insert",
        "INSERT against a global index on a partitioned table. It was onboarded \
         with nothing skipped, which was an ILLUSION: its `CREATE TABLE ... \
         PARTITION BY` silently built an ordinary table, so 12 of its 14 \
         statements were compared against the wrong object. Now that the \
         create is refused those 12 are named OutOfDomain skips and 2 \
         statements are proved -- a smaller claim that is a true one, and the \
         topic's skip count is the size of the partitioning gap here",
    ),
    (
        "session/txn",
        "the first MULTI-CONNECTION topic on the gate: a second connection's \
         `BEGIN`/`COMMIT` against the same store as the first",
    ),
    (
        "executor/rowid",
        "`_tidb_rowid` written and read back across two connections",
    ),
    (
        "ddl/ddl_tiflash",
        "TiFlash replica DDL, refused on a peer connection exactly where TiDB \
         refuses it",
    ),
    (
        "executor/admin",
        "ADMIN CHECK/SHOW over real tables, and the topic that PROVES the recursive-CTE \
         fixpoint is not quadratic: its 100,000-row `WITH RECURSIVE` never terminated \
         while the fold re-deduplicated the whole accumulated result each round",
    ),
    (
        "executor/merge_join",
        "the largest zero-divergence topic left in the suite (246 of 259), and the one \
         that gates DERIVED TABLES: it compares row results and access properties for \
         merge joins whose sides are subqueries in `FROM`, which only became \
         describable once the plan recorder learned to descend into a derived table",
    ),
    (
        "ddl/db_rename",
        "the metadata-only ALTER actions' own gate: `RENAME INDEX`'s three \
         outcomes -- renamed, ignored as the same spelling, and 1061 naming the \
         EXISTING index -- decided by the case-sensitivity rule in Go's \
         `ValidateRenameIndex`, with `ADMIN CHECK INDEX` reading the renamed \
         key back",
    ),
    (
        "planner/core/join_reorder2",
        "join reorder over derived tables specifically -- 12 of its 30 matches are \
         access properties, so a regression in which side of a `FROM (SELECT ...)` is \
         read, or how, turns it red",
    ),
    (
        "session/variable",
        "the sysvar registry's own edge cases: which SET values clamp, which are \
         refused, and which switches name a feature that is now always on",
    ),
    (
        "executor/analyze",
        "ANALYZE's own statement surface: which forms are accepted, which are refused \
         as removed features, and the warnings a `SET` of a removed switch raises",
    ),
    // The four partition topics below reached zero divergences the moment
    // `CREATE TABLE ... PARTITION BY` stopped silently building an ordinary
    // table (they carried 32, 107, 90 and 10 divergences against the flat
    // object). They are onboarded for exactly that reason: their value is not
    // in what they prove about partitioning -- most of each is an honest
    // OutOfDomain skip -- but in being the tripwire that turns red the day a
    // partial partitioning implementation starts answering these statements
    // WRONGLY again rather than refusing them.
    (
        "table/partition",
        "the partition-refusal gate: 41 side effects proved and 35 statements \
         refused exactly where TiDB refuses them, with every partitioned \
         object's own query named as a skip rather than compared against a \
         flat table",
    ),
    (
        "planner/core/partition_pruner",
        "the largest partition topic at zero divergences (156 matched): every \
         query whose answer DEPENDS on pruning is a named skip, so a pruning \
         implementation that prunes wrongly cannot pass this quietly",
    ),
    (
        "executor/partition/partition_with_expression",
        "83 matched over expression-partitioned tables, the topic that carried \
         90 divergences while the partition expression was being discarded",
    ),
    (
        "executor/index_lookup_pushdown_partition",
        "index-lookup pushdown against a partitioned table -- the smallest of \
         the four and the only one that reaches a partitioned read path at all",
    ),
    (
        "table/cache",
        "reached zero divergences (82 matched, from 20) when a NOT NULL column \
         added by `ALTER TABLE` stopped backfilling NULL into the rows written \
         before it: the topic is dense in `ALTER TABLE ... ADD COLUMN` over \
         tables that already hold rows, so it is the read-back of an origin \
         default that it really gates",
    ),
    (
        "explain",
        "the only onboarded topic that compares `DESC <view>`, which is the one \
         place a view's column metadata is read back through the SHOW surface \
         rather than through `information_schema.columns` -- the two disagree \
         on purpose (see `view_column_description` in `tidb_session::show`) \
         and only a recording can hold both halves in place at once",
    ),
    (
        "executor/jointest/join",
        "the suite's join topic, and the largest single block of newly \
         MEASURABLE statements onboarded here: 793 compared where the topic \
         previously could not be replayed at all, because its \
         `tidb_mem_quota_query = 1 << 18` cross join ran forever instead of \
         raising 8175. It is the only onboarded topic that gates the memory \
         quota on the READ path",
    ),
    (
        "sessionctx/setvar",
        "the largest ZERO-divergence topic in the suite: 709 statements over the \
         system-variable surface -- what each variable accepts, what it refuses, \
         what it reads back, and what a SET_VAR hint does to a statement. Its \
         last two divergences were the non-prepared plan cache's own \
         `@@last_plan_from_cache`, which is why onboarding it belongs to that \
         unit: without this entry nothing gates the cache against regressing",
    ),
    (
        "agg_predicate_pushdown",
        "aggregate predicate pushdown, all 17 statements compared, nothing skipped",
    ),
    (
        "common_collation",
        "collation over the common built-ins, 14 of 25 compared -- the rest is an \
         explicit OutOfDomain minority, not the majority",
    ),
    (
        "ddl/ddl_error",
        "DDL error refusals: half the topic is both engines rejecting the same \
         statement, which is agreement, not a blind spot",
    ),
    (
        "executor/adapter",
        "the statement adapter's own row results, 5 of 6 compared",
    ),
    (
        "executor/executor_txn",
        "transactional executor behavior, 81 of 121 compared with the remainder a \
         named OutOfDomain minority",
    ),
    (
        "executor/partition/partition_boundaries",
        "the second-largest ZERO-divergence topic in the suite: 1,035 of 1,035 \
         statements compared, nothing skipped, over partition boundary reads",
    ),
    (
        "executor/foreign_key",
        "referential integrity end to end: 258 of 318 compared at ZERO \
         divergences, and the only onboarded topic that gates a CROSS-SCHEMA \
         constraint -- both the `REFERENCES `db`.`tbl`` spelling `SHOW CREATE \
         TABLE` prints and the `MODIFY`/`CHANGE COLUMN` rules a constrained \
         column lives under (3780/1832/1833, and the nullability-only change \
         Go lets through)",
    ),
    (
        "executor/revoke",
        "REVOKE's row and side-effect results, 43 of 65 compared",
    ),
    (
        "explain-non-select-stmt",
        "EXPLAIN of non-SELECT statements, 6 of 7 compared",
    ),
    (
        "explain_stats",
        "EXPLAIN over statistics-driven plans, 8 of 9 compared",
    ),
    (
        "expression/constant_fold",
        "constant folding, 9 of 16 compared with the remainder a named OutOfDomain \
         minority",
    ),
    (
        "expression/vitess_hash",
        "the VITESS_HASH builtin, 9 of 16 compared with the remainder a named \
         OutOfDomain minority",
    ),
    (
        "globalindex/mem_index_non_unique",
        "a non-unique GLOBAL index over a HASH-partitioned table, all 36 \
         statements compared including explicit `partition(p0)` / `partition(p0, \
         p1)` pruning reads -- checked against the `globalindex/insert` illusion \
         by hand: these queries require real partition filtering to even return \
         the right rows, so a silently-unpartitioned table could not have passed",
    ),
    (
        "parser/integration",
        "parser integration coverage, 6 of 8 compared",
    ),
    (
        "planner/cardinality/trace",
        "cardinality trace output, all 4 statements compared, nothing skipped",
    ),
    (
        "planner/core/casetest/expression_rewriter",
        "the expression rewriter's plan and row output, 13 of 21 compared",
    ),
    (
        "planner/core/cbo",
        "cost-based optimizer row and access-property output, 23 of 31 compared",
    ),
    (
        "planner/core/plan_cost_ver2",
        "plan cost v2's row and side-effect output, 36 of 74 compared -- the rest \
         is EXPLAIN FORMAT output this tier's comparator does not read as text, \
         not an OutOfDomain skip",
    ),
    (
        "planner/core/preprocess",
        "statement preprocessing refusals, 3 of 6 compared with the rest both \
         engines rejecting the same statement",
    ),
    (
        "statistics/handle",
        "the statistics handle's row and side-effect output, 31 of 41 compared",
    ),
    (
        "statistics/integration",
        "statistics integration coverage, 26 of 27 compared",
    ),
    (
        "statistics/lock_table_stats",
        "locked-table statistics, 26 of 47 compared with the remainder a named \
         OutOfDomain minority",
    ),
    (
        "table/tables",
        "the `table` package's row and side-effect output, 22 of 32 compared",
    ),
];
