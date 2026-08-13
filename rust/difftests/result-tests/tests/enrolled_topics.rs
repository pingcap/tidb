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
    // ------------------------------------------------------------------
    // THE ENROLLMENT CENSUS (batch46). Every unenrolled topic under
    // `tests/integrationtest/r/` was replayed through
    // `survey_unonboarded_topics` and classified. The 33 that replayed at ZERO
    // divergences are below, followed by the 24 whose divergences are a
    // countable list with a NAMED cause -- the same bar the list above was
    // built to. Together they raise the compared corpus from 5,639 of 6,882 to
    // 7,875 of 10,747.
    //
    // The topics deliberately LEFT OFF, and why, are in
    // `integration_diff::survey_unonboarded_topics`.
    // ------------------------------------------------------------------
    (
        "planner/core/lateral_join",
        "the largest topic onboarded by this census at ZERO divergences (66 of 75): a \
     lateral derived table's rows and access properties, which nothing else on \
     the gate reaches",
    ),
    (
        "executor/distsql",
        "61 of 61, nothing skipped -- the distsql reader's own row results, and the \
     topic that most directly gates the double read this batch reordered",
    ),
    (
        "planner/core/casetest/pushdown/push_down",
        "51 of 59 at zero divergences: which expressions may be evaluated below the \
     reader, read back as row results rather than as plan text",
    ),
    (
        "expression/noop_functions",
        "48 of 61: the no-op switch surface -- which statements `tidb_enable_noop_\
     functions` accepts and which it refuses",
    ),
    (
        "planner/core/casetest/partition/integration_partition",
        "36 of 132 at zero divergences, the fifth partition topic and the one whose \
     remainder is the honest OutOfDomain size of the partition-DDL gap",
    ),
    (
        "expression/multi_valued_index",
        "32 of 314: a multi-valued index is refused, not answered wrongly, and this \
     is the tripwire for the day it starts being answered",
    ),
    (
        "executor/stale_txn",
        "26 of 43 with 1 ROW divergence: complete variable traversal moves three \
     `CAST(@last_commit_ts AS UNSIGNED)` statements into comparison. The two \
     SET side effects match; the `@@tidb_current_ts` equality reads NULL where \
     TiDB reads 1 because this tier does not yet publish the last commit TSO \
     through `@@tidb_last_txn_info`",
    ),
    (
        "expression/uuid",
        "17 of 41: UUID and UUID_SHORT's shape and uniqueness, with the recorder \
     rewriting the values it cannot pin",
    ),
    (
        "planner/core/range_scan_for_like",
        "16 of 226: the LIKE-to-range rewrite, which decides whether a prefix pattern \
     reaches the index at all",
    ),
    (
        "ddl/attributes_sql",
        "12 of 30: `ALTER TABLE ... ATTRIBUTES` and the placement surface, refused \
     where TiDB refuses it",
    ),
    (
        "globalindex/update",
        "11 of 31: UPDATE through a global index on a partitioned table",
    ),
    (
        "tpch",
        "11 of 40 at zero divergences: the TPC-H schema and its queries, the only \
     analytic workload shape on the gate",
    ),
    (
        "executor/import_into",
        "10 of 106: `IMPORT INTO`'s statement surface, refused where TiDB refuses it",
    ),
    (
        "globalindex/misc",
        "10 of 91: the global-index odds and ends -- `ADMIN CHECK`, `SHOW CREATE \
     TABLE` and the index's own DDL",
    ),
    (
        "expression/format",
        "9 of 9, nothing skipped: the FORMAT builtin over every rounding and locale \
     case the topic names",
    ),
    (
        "globalindex/index_join",
        "9 of 31: an index join whose inner side is a global index",
    ),
    (
        "expression/enum_set",
        "8 of 13: ENUM and SET comparison, ordering and insertion",
    ),
    ("executor/kv", "7 of 16: the KV-level statement surface"),
    (
        "infoschema/cluster_tables",
        "7 of 17: the CLUSTER_* information_schema tables, which exist and are \
     readable rather than erroring",
    ),
    (
        "access_path_selection",
        "6 of 8: the access-path chooser's own topic, five of the six matches being \
     access PROPERTIES -- so a chooser regression turns it red directly",
    ),
    (
        "ddl/column_change",
        "6 of 9: concurrent column-change states read back through DML",
    ),
    (
        "globalindex/mem_index_lookup",
        "6 of 40: a global index reached through an index LOOKUP (the double read \
     this batch reordered), on a partitioned table",
    ),
    (
        "globalindex/mem_index_merge",
        "6 of 53: index merge over a global index",
    ),
    (
        "globalindex/mem_index_reader",
        "6 of 36: a COVERING read of a global index -- the reader whose row order \
     this batch had to keep in INDEX order while the lookup moved to handle \
     order",
    ),
    (
        "bindinfo/temptable",
        "4 of 38: SQL bindings against a temporary table",
    ),
    (
        "types/json_binary_functions",
        "3 of 6: the binary-JSON builtins' own topic",
    ),
    (
        "globalindex/point_get",
        "2 of 38: a point get routed through a global index",
    ),
    (
        "planner/core/topn_heavy_function_optimize",
        "2 of 6: the TopN rewrite that keeps an expensive projection off the \
     discarded rows",
    ),
    (
        "session/bootstrap_upgrade",
        "2 of 29: the bootstrap upgrade path's own statements",
    ),
    ("db_integration", "1 of 11 at zero divergences"),
    (
        "globalindex/aggregate",
        "1 of 16: aggregation over a global index",
    ),
    ("partition", "1 of 3"),
    ("show", "1 of 5"),
    (
        "black_list",
        "25 of 54 with 3 PLAN divergences, all one cause: the expression BLACK LIST \
     (`mysql.expr_pushdown_blacklist`) is not consulted, so a scan this tier \
     reads through `idx(b, a)` is a `TableFullScan` in the recording once `=` \
     or `<` is blacklisted, and the third is the ranger reading `b = 1 and a > \
     'a'` as one two-dimension range where TiDB, with `<` blacklisted, splits \
     it into two point ranges",
    ),
    (
        "ddl/db_change",
        "20 of 33 with 3 divergences of ONE cause: `unix_timestamp(<column>)` over a \
     row written in the same statement answers NULL where TiDB answers 0, so \
     `floor((unix_timestamp() - unix_timestamp(a)) / 2)` is NULL rather than 0",
    ),
    (
        "ddl/index_modify",
        "33 of 39 with 3 divergences in two causes: `CREATE INDEX c ON t(b, a, b)` \
     is accepted where TiDB raises 1060 for the repeated column, and a \
     `_bin`-collated index restores the PADDED key bytes (` A B C`) instead of \
     the stored value (`abc`) when the index answers the column",
    ),
    (
        "ddl/serial",
        "25 of 77 with 2 divergences: a partitioned table's rows survive a `TRUNCATE \
     PARTITION` this tier refused (11 rows against TiDB's 0), and \
     `auto_random(5, 31)` is accepted where TiDB raises 8216 for a range below \
     32 bits",
    ),
    (
        "ddl/table_modify",
        "17 of 47 with 2 divergences: a database-level `COLLATE utf8_general_ci` is \
     not inherited by a table created in it (so `SHOW CREATE TABLE` prints \
     `utf8mb4_bin`), and `ENGINE = MERGE UNION = (x, y)` is accepted where \
     TiDB raises 8232",
    ),
    (
        "executor/autoid",
        "the largest topic this census onboards: 411 of 458 compared with ONE \
     divergence -- `AUTO_ID_CACHE` is not modelled, so after the cache is \
     rebased the next id is 2 where TiDB's is 30001. Every other auto-increment \
     and auto-random statement in the topic agrees",
    ),
    (
        "executor/cte",
        "118 of 133 with 3 divergences: a recursive CTE's UNION (not UNION ALL) does \
     not deduplicate ACROSS iterations (1,1,1,2,2,2,3,4 for 1,2,3,4), and two \
     refusals this tier does not make -- 1221 for a `LIMIT` inside a recursive \
     term, and 3636 when `cte_max_recursion_depth` is exceeded",
    ),
    (
        "executor/dual_password",
        "39 of 92 with 2 divergences, both the same cause: `SHOW CREATE USER` prints \
     a `mysql_native_password` hash this tier computes differently from TiDB's \
     for the same password",
    ),
    (
        "executor/parallel_apply",
        "93 of 97 match with zero divergences; the remaining four statements are \
     rejected by both engines. Correlated scalar subqueries now drive SELECT \
     projections and DELETE/UPDATE/REPLACE source rows through the same \
     per-outer-row Apply semantics as TiDB",
    ),
    (
        "executor/window",
        "89 of 93 with ONE divergence: `LEAD(col, 1, NULL) OVER (ORDER BY col)` \
     answers in the wrong row order and labels the column `__window_0` instead \
     of the expression text. Every other window statement in the topic agrees, \
     which is what makes this the window surface's gate",
    ),
    (
        "explain_complex",
        "35 of 45 with 3 PLAN divergences of one cause: an index-join inner side and \
     two `BETWEEN`/`=` ranges are read as `TableFullScan` where TiDB narrows \
     them -- the index-join access-path increment, not a row difference",
    ),
    (
        "explain_foreign_key",
        "26 of 41 with ONE divergence: an `UPDATE`'s `Point_Get` does not name the \
     unique index it went through (`index:idx(id)`)",
    ),
    (
        "expression/plan_cache",
        "141 of 184 with ONE divergence: a single `@@last_plan_from_cache` reads 0 \
     where TiDB reads 1. This is the second-largest plan-cache surface on the \
     gate after `sessionctx/setvar`",
    ),
    (
        "index_join",
        "19 of 21 with 2 PLAN divergences, one cause: `TIDB_INLJ` builds a hash join \
     over a full scan instead of probing `idx(a)` per outer row. The topic is \
     named after the feature, so it is the right tripwire for the index-join \
     increment",
    ),
    (
        "infoschema/v2",
        "27 of 34 with 2 divergences of one cause: a query filtered on \
     `TIDB_TABLE_ID` for a memory-table id finds no row, because \
     `information_schema.tables` does not carry ids for the CLUSTER_* tables",
    ),
    (
        "planner/core/casetest/partition/partition_pruner",
        "258 of 294 with 2 divergences -- the largest partition topic on the gate \
     after `partition_boundaries`. One is a join's row ORDER over a partitioned \
     inner side, the other is `a IS NULL` reading all four partitions where \
     TiDB prunes to `p0` (the NULL partition rule)",
    ),
    (
        "planner/core/fulltext_search",
        "33 of 95 with ONE divergence, the same `@@last_plan_from_cache` as \
     `expression/plan_cache`. Half the topic is both engines rejecting the \
     full-text statements, which is agreement",
    ),
    (
        "planner/core/join_key_type_cast",
        "75 of 77 with 2 divergences: an `INL_JOIN` inner side is a `TableFullScan` \
     rather than a `TableRangeScan` over the cast key, and a five-way \
     straight-join's rows come back in a different (unordered) order",
    ),
    (
        "planner/core/physical_plan",
        "31 of 42 with 3 divergences in two causes: the deprecated INDEX MERGE JOIN \
     hint raises no 1815 warning, and `@@last_plan_from_binding` reads 0 where \
     TiDB reads 1",
    ),
    (
        "planner/core/rule_constant_propagation",
        "55 of 57 match; one nested UNION plan scans `s` twice where TiDB scans it \
     once, and one multi-table UPDATE EXPLAIN remains unsupported. The correlated \
     scalar assignment now writes the same value as TiDB",
    ),
    (
        "planner/core/rule_result_reorder",
        "28 of 29 with ONE divergence, again `@@last_plan_from_cache`. The rule that \
     makes a result order deterministic is exactly the kind this batch's \
     handle-order change could break, so it belongs on the gate",
    ),
    (
        "session/privileges",
        "59 of 66 with ONE divergence: `SHOW CREATE VIEW` prints an EMPTY definer \
     (``@``) where TiDB prints `root`@`%` -- the view's definer is not recorded \
     at creation",
    ),
    (
        "table/index",
        "37 of 44 with ONE divergence: a duplicate `CREATE INDEX ... IF NOT EXISTS` \
     raises the 1061 as an Error where TiDB raises it as a Note",
    ),
    (
        "topn_push_down",
        "15 of 19 with 3 PLAN divergences of one cause: every index-join inner side \
     is an `IndexFullScan` or `TableFullScan` where TiDB builds a per-probe \
     `IndexRangeScan`. Six of the 15 matches are access properties",
    ),
    (
        "window_function",
        "23 of 27 with 4 PLAN divergences of ONE cause, already carried elsewhere: \
     `select sum(a) over(...) from t` reads only the indexed column, so TiDB \
     scans the narrow index (`IndexFullScan index:idx(a)`) and this tier scans \
     the table. Twelve of the 23 matches are access properties, which is what \
     the topic buys: it is the only enrolled topic whose plans are WINDOW plans",
    ),
    (
        "executor/expand",
        "68 of 71 at zero divergences: the full ROLLUP row surface, including \
     aggregate arguments that are also grouping keys, derived grouping \
     expressions, GROUPING(), DISTINCT aggregates, HAVING, and windows over \
     super-aggregate rows",
    ),
    (
        "session/vars",
        "117 of 127 with 3 divergences in THREE causes, all of them about a \
     variable's own value rather than about a query: `@@time_zone` reads \
     `SYSTEM` where the recording session reads `Asia/Shanghai`; a \
     `SET_VAR(sql_auto_is_null=1)` hint takes effect here and is ignored by \
     TiDB; and `@@warning_count` reads 0 where TiDB counts 1. Explicit \
     SESSION/LOCAL reads of a GLOBAL-only variable now join TiDB at 1238, \
     while the ScopeNone `@@global.performance_schema_max_mutex_classes` \
     read joins TiDB at 200. It is the densest variable-behavior topic in the \
     suite, and 97 of its matches are ROW results",
    ),
    (
        "planner/core/integration_partition",
        "132 matched and 3 ROW divergences of one cause: `INSERT INTO tref SELECT * \
     FROM t` reads a partitioned source as an empty relation instead of refusing \
     it, so the ordinary reference table `tref` is empty in all three later \
     checks. The topic is otherwise a large named partition-refusal surface: 319 \
     statements are OutOfDomain and 38 are rejected by both engines",
    ),
];
