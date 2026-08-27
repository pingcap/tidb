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

//! Port ledger for `pkg/planner/indexadvisor/utils_test.go`
//! (`pkg/planner.part21` items 1243-1251 on `origin/master`).
//!
//! Go's helpers walk the parsed AST of each query text:
//! `ParseOneSQL` (utils.go:40-44) plus a `nodeVisitor` collecting table names,
//! select/order-by/DNF columns, and indexable columns against the live catalog
//! through Optimizer.PossibleColumns/ColumnType (utils.go:75-105, :106-144,
//! :145-184, :187-237, :358-424). The Rust crate has no SQL-AST-walking port of
//! this surface and no optimizer-backed column resolution, so every item is an
//! `#[ignore]` gap below; none can be honestly exercised without the parse +
//! infoschema pipeline.

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:26
/// TestCollectTableFromQuery`.
///
/// Re-derived contract: CollectTableNamesFromQuery(defaultSchema, text)
/// returns qualified `schema.table` names in first-reference order — implicit
/// t -> test.t (:28-31), comma joins t1,t2 (:32-34), scalar subquery tables are
/// included in order (:35-38), and explicitly qualified db2.t2 keeps its own
/// schema while bare t1 uses the default (:39-43). Implementation walks every
/// TableName node (utils.go:77-104).
#[test]
#[ignore = "go-parity-gap: needs SQL parsing plus the AST TableName walker"]
fn collect_table_from_query_qualifies_and_orders_references() {}

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:47
/// TestCollectSelectColumnsFromQuery`.
///
/// Re-derived contract: select-list columns resolve to fully qualified
/// normalized identities rendered as `{test.t.a, test.t.b}` / 3-column variant
/// (:49-55). Implementation collects ColumnNameExpr under SelectField and maps
/// them through the default schema (utils.go:108-143).
#[test]
#[ignore = "go-parity-gap: needs SQL parsing plus select-field column resolution"]
fn collect_select_columns_from_query_qualified_identities() {}

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:57
/// TestCollectOrderByColumnsFromQuery`.
///
/// Re-derived contract: order-by columns keep their clause order — one key
/// test.t.a for single-column sort, test.t.a then test.t.b for two (:59-68);
/// implementation returns []Column in source order, utils.go:147-183.
#[test]
#[ignore = "go-parity-gap: needs SQL parsing plus OrderBy clause walking"]
fn collect_order_by_columns_from_query_preserves_clause_order() {}

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:70
/// TestCollectDNFColumnsFromQuery`.
///
/// Re-derived contract: top-level OR disjunctions contribute each branch's
/// column-equals predicate as set members — `{test.t.a, test.t.b}` then
/// extended with test.t.c for three branches (:72-78); flattenDNF/
/// flattenColEQConst do the flattening (utils.go:187-236, :259-287).
#[test]
#[ignore = "go-parity-gap: needs SQL parsing plus DNF expression flattening"]
fn collect_dnf_columns_from_query_gathers_or_branch_predicates() {}

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:80 TestRestoreSchemaName`.
///
/// Re-derived contract: RestoreSchemaName rewrites each query's schema into the
/// restored statement text and drops invalid SQL when ignoreErr — the sorted
/// result renders the three valid entries backquoted (`SELECT * FROM
/// `test2`.`t2``, `SELECT * FROM `test`.`t1``, `SELECT * FROM
/// `test`.`t3``) (:86-94; restore via utils.go:289-307 which restores the AST
/// then formats). With ignoreErr=false the unparsable `wrong` entry makes the
/// call error instead of being dropped (:96-98).
#[test]
#[ignore = "go-parity-gap: needs statement restore/format over the parser AST"]
fn restore_schema_name_rewrites_set_and_errors_without_ignore() {}

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:96
/// TestFilterSQLAccessingSystemTables`.
///
/// Re-derived contract: any reference to memdb/system schemas
/// (mysql/information_schema/metrics_schema/performance_schema via metadef
/// checks at utils.go:339-344) removes the query even when only the session
/// default schema named them; explicit qualification wins over SchemaName;
/// non-table statements (`select @@var`, `select sleep(1)`; zero collected
/// tables, utils.go:333-337) are dropped too; `select * from test.t1` with
/// default schema mysql survives because the qualified name excludes system
/// DBs (:99-114 assert exactly one survivor). ignoreErr=false surfaces the
/// parse error of the `wrong` entry instead (:116-118).
#[test]
#[ignore = "go-parity-gap: needs SQL parsing plus metadef schema classification"]
fn filter_sql_accessing_system_tables_keeps_only_user_table_queries() {}

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:117
/// TestFilterInvalidQueries`.
///
/// Re-derived contract: FilterInvalidQueries (utils.go:311-325) keeps only
/// queries that survive validation against the live store — references to a
/// missing table (t3) or missing column (d) go, fix-control-43817 subqueries
/// (`a < (select max(b) from t2)`) go, garbage goes; only
/// `select * from test.t1` remains when ignoring (:123-134), and errors are
/// raised when not ignoring (:136-138).
#[test]
#[ignore = "go-parity-gap: needs the Optimizer-backed validation pass over a live store"]
fn filter_invalid_queries_prunes_unknown_objects_and_43817_shapes() {}

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:140
/// TestCollectIndexableColumnsForQuerySet`.
///
/// Re-derived contract: union of per-query indexable columns across range
/// predicates (a/b/e like), IN + ORDER BY (c/d), GROUP BY (e) yields exactly
/// `{test.t.a, test.t.b, test.t.c, test.t.d, test.t.e}` (:147-156) — the
/// query-set driver delegates to CollectIndexableColumnsFromQuery and merges
/// sets (utils.go:356-370).
#[test]
#[ignore = "go-parity-gap: needs parse + possible-columns/type-filter resolution"]
fn collect_indexable_columns_for_query_set_merges_all_column_roles() {}

/// GO PORT of `pkg/planner/indexadvisor/utils_test.go:158
/// TestCollectIndexableColumnsFromQuery`.
///
/// Re-derived contract: per-query collection keeps range predicates {test.t.a,
/// test.t.b}, adds ORDER BY d to IN-predicate c, adds GROUP BY d (:163-181);
/// after dropping/recreating fixture tables it resolves an aliased join by
/// name across candidate columns — `select * from t2 tx where a<1` still
/// yields BOTH t1.a and t2.a because PossibleColumns matches by bare column
/// name across all schema tables (:183-194); the TPC-H Q5-style derived query
/// against tpch.nation reduces to {tpch.nation.n_name,
/// tpch.nation.n_nationkey} once PK filtering applies (:196-216; implementation
/// utils.go:372-430 plus isIndexableColumnType gating).
#[test]
#[ignore = "go-parity-gap: needs parse + possible-columns/type-filter resolution including dropped-table edge cases"]
fn collect_indexable_columns_from_query_resolves_names_across_tables() {}
