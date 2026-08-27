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

//! Port ledger for the index-advisor end-to-end suites:
//! `pkg/planner/indexadvisor/indexadvisor_test.go` (items 1213-1227),
//! `pkg/planner/indexadvisor/indexadvisor_sql_test.go` (items 1209-1212), and
//! `pkg/planner/indexadvisor/indexadvisor_tpch_test.go` (items 1257-1260)
//! (`pkg/planner.part21` on `origin/master`).
//!
//! Every one of these tests is a live-session integration test: they build a
//! mock TiDB store (`testkit.CreateMockStore`), create fixture tables/views,
//! and call `indexadvisor.AdviseIndexes(ctx, tk.Session(), sqls, options)`
//! (pkg/planner/indexadvisor/indexadvisor.go:49-59) or drive the
//! `recommend index run for ...` statement directly through the executor.
//! AdviseIndexes assembles a query set (from caller SQLs or an injected
//! context key), prepares/costs candidate indexes per query via
//! Optimizer.QueryPlanCost + EstIndexSize, prunes by benefit, enforces
//! max_num_index/max_index_columns caps, and persists results into
//! mysql.index_advisor_results (indexadvisor.go:61-294). The Rust crate owns
//! no session/executor/statistics stack yet, so each port records its pinned
//! contract as an `#[ignore]` gap.

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_sql_test.go:38
/// TestIndexAdvisorForSQL`.
///
/// Re-derived contract: via checkResult (sql_test.go:27-36) `recommend index
/// run for '<sql>'` renders recommendation rows "db.table.cols" joined by "|":
/// single equality on a/b/c gives exactly one covering single-column index;
/// two-predicate AND queries give composite indexes in selectivity order
/// test.t.a,b / test.t.a,c (:43-49).
#[test]
#[ignore = "go-parity-gap: needs the recommend-index executor plus per-query costing"]
fn index_advisor_for_sql_single_and_composite_recommendations() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_sql_test.go:49
/// TestIndexAdvisorForMultipleTables`.
///
/// Re-derived contract: workload spanning t1/t2 yields one index per table
/// ("test.t1.a|test.t2.b", then join predicate battery t1.b/t2.a for
/// `t1.a=t2.a and t1.b=1`) (:52-56); a 20-query workload under default
/// max_num_index=5 keeps only the five most beneficial table indexes
/// ("test.t1.a|test.t10.a|test.t11.a|test.t12.a|test.t13.a") even though half
/// of the tables were created lazily mid-workload (:57-66).
#[test]
#[ignore = "go-parity-gap: needs cross-table workload costing and lazy DDL visibility"]
fn index_advisor_for_multiple_tables_scores_per_table_benefits() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_sql_test.go:70
/// TestIndexAdvisorForVariousTypes`.
///
/// Re-derived contract: every indexable column type gets its own recommended
/// index when filtered alone — int, varchar, float, datetime (now()),
/// decimal (:76-81); pairwise workloads combine them into composites whose
/// column order follows value-selectivity rather than declaration order
/// (e.g. c before b for float vs varchar; d before b for datetime) (:83-88).
#[test]
#[ignore = "go-parity-gap: needs type-aware costing across int/varchar/float/datetime/decimal columns"]
fn index_advisor_for_various_types_costs_every_indexable_type() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_sql_test.go:87
/// TestIndexAdvisorEmptyResult`.
///
/// Re-derived contract: with existing `key(a, b, c)` fully covering the only
/// useful predicates, `recommend index run` returns ZERO rows (:92) and emits
/// warning 1105 with the exact advisory text "Considered 3 indexable
/// columns(test.t.a, test.t.b, test.t.c), 3 or more index candidates(test.t(a),
/// test.t(b), test.t(c)), no sufficiently beneficial indexes were found."
/// (:93).
#[test]
#[ignore = "go-parity-gap: needs the recommend-index executor plus the no-benefit warning emission path"]
fn index_advisor_empty_result_emits_exact_no_benefit_warning() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:60
/// TestIndexAdvisorInvalidQuery`.
///
/// Re-derived contract: AdviseIndexes over unparseable input errors (:63);
/// a batch mixing garbage with a valid statement also errors rather than
/// silently skipping (:64).
#[test]
#[ignore = "go-parity-gap: AdviseIndexes entry point has no Rust carrier"]
fn index_advisor_invalid_query_fails_batch() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:69
/// TestIndexAdvisorFrequency`.
///
/// Re-derived contract: with an injected query set
/// (`context.WithValue(..., indexadvisor.TestKey("query_set"), ...)`) and
/// max_num_index=1, the higher-frequency member wins the sole slot — freq(2)
/// a-beats-b, then b-beats-a inverted, and a freq-100 query beats both
/// (:72-94). Frequency multiplies each query's contribution during candidate
/// scoring (indexadvisor.go prepareQuerySet/preparation path :137-178).
#[test]
#[ignore = "go-parity-gap: needs the injected query-set channel and frequency-weighted scoring"]
fn index_advisor_frequency_weights_the_single_recommendation() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:96
/// TestIndexAdvisorBasic1`.
///
/// Re-derived contract: one useful SQL recommends its predicate column
/// (:101); comma-separated batch extends to per-column indexes sorted and
/// rendered "test.t.a,test.t.b" (:102); projections do not change it (:103).
#[test]
#[ignore = "go-parity-gap: AdviseIndexes pipeline unported"]
fn index_advisor_basic_one_recommends_predicate_columns() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:107
/// TestIndexAdvisorBasic2`.
///
/// Re-derived contract: among 100 useless full scans plus exactly one
/// selective filter, only the selective query contributes and the advisor
/// returns exactly test.t0.a (:112-117).
#[test]
#[ignore = "go-parity-gap: AdviseIndexes pipeline unported"]
fn index_advisor_basic_two_ignores_useless_scans() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:135
/// TestIndexAdvisorFixControl43817`.
///
/// Re-derived contract: scalar-subquery comparisons like
/// `a=(select max(a) from t2)` are rejected whole-batch (:140-145); through an
/// injected query set such entries are dropped during preparation so a batch
/// becomes empty -> error (:146-155), while adding one clean query lets the
/// run succeed recommending only its index (test.t1.a, :156-159).
#[test]
#[ignore = "go-parity-gap: needs the fix-control-43817 subquery validation in query-set preparation"]
fn index_advisor_fix_control_43817_rejects_subquery_comparisons() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:161
/// TestIndexAdvisorView`.
///
/// Re-created contract: queries against view v (definer root@127.0.0.1,
/// selecting from t where a=1) resolve to BASE-table indexes —
/// `select * from v where b=1` recommends test.t.b, and combining view query
/// with direct-table query yields both (:166-171).
#[test]
#[ignore = "go-parity-gap: needs view expansion inside advisor query preparation"]
fn index_advisor_view_resolves_to_base_table_indexes() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:173
/// TestIndexAdvisorMassive`.
///
/// Re-derived contract: over ten eight-column tables and ten random
/// three-predicate selections generated with math/rand (:179-188),
/// AdviseIndexes under max_num_index=3 must return EXACTLY three
/// recommendations without error (:189-191) — a stability/fuzz pin on the
/// search's cap enforcement.
#[test]
#[ignore = "go-parity-gap: needs randomized large-workload search over live costing"]
fn index_advisor_massive_returns_exactly_capped_recommendations() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:194
/// TestIndexAdvisorIncorrectCurrentDB`.
///
/// Re-derived contract: with current database switched to mysql but the SQL
/// fully qualified to test.t (:199-201), recommendations still land on
/// test.t.a — resolution uses the qualified schema, not the session default.
#[test]
#[ignore = "go-parity-gap: AdviseIndexes pipeline unported"]
fn index_advisor_incorrect_current_db_still_targets_explicit_schema() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:204
/// TestIndexAdvisorPrefix`.
///
/// Re-derived contract: prefix overlap collapses duplicates — {a} plus {a,b}
/// workloads recommend single test.t.a_b (:208-210); adding {a,b,c} keeps ONE
/// index because a_b_c covers a_b which covers a (:211-213).
#[test]
#[ignore = "go-parity-gap: prefix-dominance pruning is part of the unported search"]
fn index_advisor_prefix_collapses_dominated_candidates() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:216
/// TestIndexAdvisorCoveringIndex`.
///
/// Re-derived contract: select-list-only columns extend indexes as covering
/// suffixes after predicate prefixes — a+b, then a+b+c for `select b,c`,
/// then predicate-first ordering a,d,b where equality filters lead the list
/// order (:221-229).
#[test]
#[ignore = "go-parity-gap: covering-suffix synthesis lives in the unported candidate builder"]
fn index_advisor_covering_index_appends_select_list_suffixes() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:227
/// TestIndexAdvisorExistingIndex`.
///
/// Re-derived contract: an existing index ab(a,b) suppresses any new advice
/// for queries it covers (:233-236); once a third predicate appears the
/// advisor still only recommends what is missing — test.t.c (:237).
#[test]
#[ignore = "go-parity-gap: needs existing-index coverage checks in candidate pruning"]
fn index_advisor_existing_index_suppresses_covered_recommendations() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:239
/// TestIndexAdvisorTPCC`.
///
/// Re-derived contract: fifteen TPC-C-flavored workloads (point lookups,
/// IN-lists with FOR UPDATE, ORDER BY+LIMIT stock-level join with hint,
/// secondary-column sorts over customer/district/new_order/item/order_line
/// fixtures with all PKs removed, :243-371) form an injected query set at
/// frequency 1; with max_num_index=3 AdviseIndexes must return SOME non-empty
/// set without error (:372-375).
#[test]
#[ignore = "go-parity-gap: needs the full TPC-C-shaped workload costing search"]
fn index_advisor_tpcc_yields_nonempty_capped_set() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:378
/// TestIndexAdvisorWeb3Bench`.
///
/// Re-derived contract: nine web3-style SELECTs (hash/equality probes, big
/// IN lists, ORDER BY ... LIMIT, count distinct, NOT EXISTS derived tables,
/// UNION ALL self-join subquery) against blocks/transactions/token_transfers/
/// receipts/logs fixtures form an injected query set (:468-486); with
/// max_num_index=3 the advisor returns a non-empty recommendation list
/// without error (:488-490).
#[test]
#[ignore = "go-parity-gap: needs the web3-shaped workload costing search including derived tables"]
fn index_advisor_web3_bench_yields_nonempty_set() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:533
/// TestIndexAdvisorRunFor`.
///
/// Re-derived contract: the `recommend index run for "<stmts>"` statement
/// form splits on semicolons — two valid queries give two rows (:540-542);
/// empty-only, garbage, and mixed-garbage inputs all error (:543-546); blanks
/// between semicolons are skipped while real statements still run
/// (";;select * from t1 where a=1;; ;;  ;" -> one row, :547-548).
#[test]
#[ignore = "go-parity-gap: needs the recommend-index statement executor and splitter"]
fn index_advisor_run_for_splits_and_validates_statement_lists() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:552
/// TestIndexAdvisorStorage`.
///
/// Re-derived contract: every accepted run persists rows into
/// mysql.index_advisor_results carrying index_columns plus a JSON Reason at
/// index_details->'$.Reason' quoting the normalized source SQL — cumulative
/// across runs: "a" with reason for `select \`a\` from \`test\` . \`t\``
/// etc., reaching four rows (a / b / b,c / d) after the last battery
/// (:557-578).
#[test]
#[ignore = "go-parity-gap: needs persistence into mysql.index_advisor_results with JSON reasons"]
fn index_advisor_storage_persists_columns_and_json_reason() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_test.go:582
/// TestIndexAdvisorCreateIndexStmt`.
///
/// Re-derived contract: result row column 7 carries ready-to-run DDL text
/// equal to "CREATE INDEX idx_a ON t(a);" (:587-589) produced by the
/// graceful-index-name/DDL writer (indexadvisor.go:300-325 round-trip).
#[test]
#[ignore = "go-parity-gap: needs the DDL-text rendering of recommendation rows"]
fn index_advisor_create_index_stmt_emits_executable_ddl_text() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_tpch_test.go:758
/// TestIndexAdvisorTPCH1`.
///
/// Re-derived contract: TPC-H Q1..Q7 texts (declared at tpch_test.go:30-672)
/// against createTPCHTables fixtures (:673) form an injected query set at
/// frequency 1; AdviseIndexes runs with a per-call timeout option '2m'
/// (`[]ast.RecommendIndexOption{{Option: OptTimeout, Value: "2m"}}`) and must
/// return some non-empty recommendation list without error (:771-777).
#[test]
#[ignore = "go-parity-gap: needs TPC-H workload costing with per-run timeout option"]
fn index_advisor_tpch_q1_q7_nonempty_with_timeout_option() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_tpch_test.go:780
/// TestIndexAdvisorTPCH2`.
///
/// Re-derived contract: Q8..Q12 texts (subquery-heavy joins incl. correlated
/// min-supply-cost pruning) under identical injected-set + '2m' timeout
/// conditions return some non-empty list without error (:792-798).
#[test]
#[ignore = "go-parity-gap: needs TPC-H workload costing with per-run timeout option"]
fn index_advisor_tpch_q8_q12_nonempty_with_timeout_option() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_tpch_test.go:800
/// TestIndexAdvisorTPCH3`.
///
/// Re-derived contract: Q13/Q14/Q16..Q18 aggregation-heavy family under the
/// same harness returns some non-empty list without error (:812-818).
#[test]
#[ignore = "go-parity-gap: needs TPC-H workload costing with per-run timeout option"]
fn index_advisor_tpch_q13_q18_nonempty_with_timeout_option() {}

/// GO PORT of `pkg/planner/indexadvisor/indexadvisor_tpch_test.go:820
/// TestIndexAdvisorTPCH4`.
///
/// Re-derived contract: Q19..Q22 nested-join family under the same harness
/// returns some non-empty list without error (:832-838).
#[test]
#[ignore = "go-parity-gap: needs TPC-H workload costing with per-run timeout option"]
fn index_advisor_tpch_q19_q22_nonempty_with_timeout_option() {}
