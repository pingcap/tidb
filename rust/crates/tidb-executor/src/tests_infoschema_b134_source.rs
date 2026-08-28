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

//! Ports of Go `pkg/executor/test/infoschema/infoschema_test.go` items
//! 945–960. The local executor does not own the infoschema retrievers,
//! privilege manager, DDL history store, statistics handle, or inspection
//! failpoints used by these tests; every source test is therefore recorded as
//! an explicit, compiled gap rather than approximated.

/// Go `TestInspectionTables` (:48): inspection-table cluster rows and cache.
#[test]
#[ignore = "go-parity-gap: inspection cluster retriever, failpoint, and cache are unported"]
fn inspection_tables() {}

/// Go `TestUserPrivileges` (:104): information_schema visibility under users,
/// roles, and table privileges.
#[test]
#[ignore = "go-parity-gap: users/roles and infoschema privilege filtering are unported"]
fn user_privileges() {}

/// Go `TestDataForTableStatsField` (:171): statistics deltas and ANALYZE
/// update information_schema.tables size fields.
#[test]
#[ignore = "go-parity-gap: statistics handle and information_schema table-size retriever are unported"]
fn data_for_table_stats_field() {}

/// Go `TestPartitionsTable` (:224): partition metadata, IDs, expressions,
/// and per-partition statistics.
#[test]
#[ignore = "go-parity-gap: partition infoschema retriever and statistics are unported"]
fn partitions_table() {}

/// Go `TestForAnalyzeStatus` (:308): ANALYZE_STATUS rows, privileges, and
/// SHOW ANALYZE STATUS equivalence.
#[test]
#[ignore = "go-parity-gap: analyze job history and infoschema retriever are unported"]
fn for_analyze_status() {}

/// Go `TestForServersInfo` (:378): local server metadata and configured
/// labels in TIDB_SERVERS_INFO.
#[test]
#[ignore = "go-parity-gap: TIDB_SERVERS_INFO retriever and global server config are unported"]
fn for_servers_info() {}

/// Go `TestTablesTable` (:424): information_schema.tables predicate
/// extraction, table IDs, and schema filtering.
#[test]
#[ignore = "go-parity-gap: information_schema.tables SQL retriever is owned by tidb-session and unported here"]
fn tables_table() {}

/// Go `TestColumnTable` (:503): information_schema.columns rows for tables and
/// views under column/table predicates.
#[test]
#[ignore = "go-parity-gap: information_schema.columns SQL retriever is unported here"]
fn column_table() {}

/// Go `TestIndexUsageTable` (:560): index usage rows and case-insensitive
/// schema/table/index predicates.
#[test]
#[ignore = "go-parity-gap: index usage information_schema retriever is unported here"]
fn index_usage_table() {}

/// Go `TestJoinSystemTableContainsView` (:633): nested metadata subqueries
/// over information_schema.tables/columns include a view correctly.
#[test]
#[ignore = "go-parity-gap: information_schema nested metadata query surface is unported here"]
fn join_system_table_contains_view() {}

/// Go `TestShowColumnsWithSubQueryView` (:690): SHOW COLUMNS on a view with a
/// scalar subquery must avoid storage coprocessor access.
#[test]
#[ignore = "go-parity-gap: SHOW COLUMNS/view metadata path and failpoints are unported"]
fn show_columns_with_subquery_view() {}

/// Go `TestReferencedTableSchemaWithForeignKey` (:715): key-column usage
/// reports the referenced schema for a cross-database foreign key.
#[test]
#[ignore = "go-parity-gap: information_schema.key_column_usage cross-schema retriever is unported here"]
fn referenced_table_schema_with_foreign_key() {}

/// Go `TestSameTableNameInTwoSchemas` (:731): table IDs disambiguate equal
/// table names in different schemas.
#[test]
#[ignore = "go-parity-gap: information_schema.tables table-ID filtering is unported here"]
fn same_table_name_in_two_schemas() {}

/// Go `TestInfoSchemaDDLJobs` (:761): DDL job history/running rows and plan
/// output for filtered ddl_jobs queries.
#[test]
#[ignore = "go-parity-gap: DDL job history and running-job inspection are unported here"]
fn info_schema_ddl_jobs() {}

/// Go `TestInfoSchemaConditionWorks` (:841): common metadata predicates
/// affect every populated information_schema table.
#[test]
#[ignore = "go-parity-gap: broad information_schema retriever/predicate matrix is unported here"]
fn info_schema_condition_works() {}

/// Go `TestInfoschemaTablesSpecialOptimizationCovered` (:960): the optimized
/// information_schema.tables path is selected for supported projections.
#[test]
#[ignore = "go-parity-gap: information_schema.tables special optimizer coverage hook is unported"]
fn infoschema_tables_special_optimization_covered() {}
