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

//! Port ledger for `pkg/planner/indexadvisor/optimizer_test.go`
//! (`pkg/planner.part21` items 1225-1232 on `origin/master`), excluding item
//! 1226 `TestOptimizerPrefixContainIndex`, whose prefix-relation core is a real
//! port in `indexadvisor_prefix_relation_source.rs`.
//!
//! Every remaining test drives an
//! `indexadvisor.NewOptimizer(tk.Session())` (Go interface at
//! pkg/planner/indexadvisor/optimizer.go:39-61, impl at :63-66) against a live
//! testkit store: the implementation resolves tables/columns/indexes through
//! `infoschema` (:72-74 `is()`), reads statistics for sizes, and re-plans SQL
//! with hypothetical indexes to cost queries. The Rust crate carries only the
//! normalized identity model (`index_advisor_model.rs`, from model.go) — no
//! infoschema accessor, no stats handle, no plan-cost pipeline — so each port
//! below records the pinned contract as an `#[ignore]` gap; none of them is
//! approximated against strings or fixtures that are not backed by real
//! catalog/stats behavior.

/// GO PORT of `pkg/planner/indexadvisor/optimizer_test.go:29
/// TestOptimizerColumnType`.
///
/// Re-derived contract: over t1(a int, b float, c varchar(255)) and
/// t2(a int, b decimal(10,2), c varchar(1024)) (:31-34), `ColumnType` resolves
/// each (schema,table,column) to its FieldType — int -> mysql.TypeLong,
/// float -> mysql.TypeFloat, varchar(255/1024) -> mysql.TypeVarchar,
/// decimal -> mysql.TypeNewDecimal (:35-56); unknown column d and missing
/// table t3 both return errors (:57-62). Implementation:
/// optimizer.go:159-170 walks TableInfo → ColumnInfo by lowercased name.
#[test]
#[ignore = "go-parity-gap: Optimizer.ColumnType needs the live infoschema/table-metadata accessor and mysql FieldType carrier"]
fn optimizer_column_type_resolves_field_types_and_reports_misses() {}

/// GO PORT of `pkg/planner/indexadvisor/optimizer_test.go:99
/// TestOptimizerPossibleColumns`.
///
/// Re-derived contract: across t1/t2 (a..d) and t3 (c..f) (:103-107),
/// `PossibleColumns(schema, name)` returns every table's matching column as
/// `[Column]` sorted-by-table when rendered `table.column` — test.a -> {t1.a,
/// t2.a}, test.c/d include t3, e/f only t3, g -> empty (:109-129).
/// Implementation scans all tables of the schema, optimizer.go:108-133.
#[test]
#[ignore = "go-parity-gap: PossibleColumns needs cross-table infoschema enumeration"]
fn optimizer_possible_columns_spans_every_table_in_schema() {}

/// GO PORT of `pkg/planner/indexadvisor/optimizer_test.go:128
/// TestOptimizerTableColumns`.
///
/// Re-derived contract: `TableColumns(schema, table)` lists the table's columns
/// with SchemaName/TableName backfilled on each entry — t1/t2 -> [a b c d],
/// t3 -> [c d e f] (:131-153); optimizer.go:91-106 errors when the table is
/// absent.
#[test]
#[ignore = "go-parity-gap: TableColumns needs per-table infoschema lookup"]
fn optimizer_table_columns_lists_columns_with_identity_backfill() {}

/// GO PORT of `pkg/planner/indexadvisor/optimizer_test.go:155
/// TestOptimizerIndexNameExist`.
///
/// Re-derived contract: tables carrying `index ka(a), index kbc(b,c)`
/// (:159-161) answer IndexNameExist true for ka/kbc and false for kbc2 on both
/// tables (:163-175); optimizer.go:77-89 compares lowercased names per table.
#[test]
#[ignore = "go-parity-gap: IndexNameExist needs per-table index-metadata lookup"]
fn optimizer_index_name_exist_reports_known_and_unknown_names() {}

/// GO PORT of `pkg/planner/indexadvisor/optimizer_test.go:177
/// TestOptimizerEstIndexSize`.
///
/// Re-derived contract: before any rows land, EstIndexSize(test.t, ...) is 0
/// (:186-192); after inserting (1, space(32)), flushing stats deltas, updating
/// the stats handle and analyzing all columns (:193-196), single-column estimates
/// equal row-count times average encoded length — a=1, b=32+1 (:197-206) — and
/// multi-column sums to 34 (:207-210); after growing the row to space(64),
/// a=2 rows, b=99 (32+64+x), and (b,a)=99+2 (:211-225). Estimates come from the
/// domain StatsHandle via table stats row count and column info
/// (optimizer.go:247-260).
#[test]
#[ignore = "go-parity-gap: EstIndexSize needs the live statistics handle, analyze pipeline, and flush/DDL plumbing"]
fn optimizer_est_index_size_tracks_rows_and_encoded_lengths_after_analyze() {}

/// GO PORT of `pkg/planner/indexadvisor/optimizer_test.go:226
/// TestOptimizerQueryCost`.
///
/// Re-derived contract: this Go test currently only creates its fixture tables
/// and optimizer instance (:227-233) — it pins no cost assertions itself; it is
/// ported as scaffolding-only so the family stays addressable, asserting
/// nothing beyond construction succeeding.
#[test]
#[ignore = "go-parity-gap: NewOptimizer needs a session-bound optimizer constructor; test body itself has no assertions"]
fn optimizer_query_cost_fixture_scaffolding_only() {}

/// GO PORT of `pkg/planner/indexadvisor/optimizer_test.go:234
/// TestOptimizerQueryPlanCost`.
///
/// Re-derived contract: for `select a,b from t0 where a=1 and b=1`
/// (:239-241), adding hypothetical index idx_a(a) must lower the plan cost
/// (cost2 < cost1, :252), and widening it to idx_a_b(a,b) lowers it again
/// (cost3 < cost2, :265). QueryPlanCost builds hypo indexes into the schema,
/// plans the statement, and returns physicalOptimize cost (optimizer.go:219-245
/// + addHypoIndex :172-217).
#[test]
#[ignore = "go-parity-gap: QueryPlanCost needs hypothetical-index injection plus the physical optimize/cost pipeline"]
fn optimizer_query_plan_cost_drops_when_hypothetical_indexes_fit() {}
