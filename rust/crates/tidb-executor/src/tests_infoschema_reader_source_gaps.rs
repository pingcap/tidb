// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for the `memtableRetriever` data sources in Go
//! `pkg/executor/infoschema_reader_internal_test.go` (CHECK_CONSTRAINTS and
//! KEYWORDS) and `infoschema_reader_test.go` (TiFlash system tables). The
//! Rust tier registers these information_schema table NAMES with metadata
//! (`tidb-session/src/infoschema.rs`) but does not implement their row
//! data sources (`setDataFromCheckConstraints` :924,
//! `setDataFromTiDBCheckConstraints` :969, `setDataFromKeywords` :4042,
//! `TiFlashSystemTableRetriever` :3528, all in
//! `pkg/executor/infoschema_reader.go`).

/// Go `pkg/executor/infoschema_reader_internal_test.go:30::TestSetDataFromCheckConstraints`:
/// `information_schema.CHECK_CONSTRAINTS` emits one row per PUBLIC table
/// check constraint over a MockInfoSchema: `CONSTRAINT_CATALOG`="def",
/// `CONSTRAINT_SCHEMA`="test", name "t2_c1", and the expression wrapped in
/// parentheses "(id<10)"; a `StateDeleteOnly` constraint (t3_c1) is hidden
/// (`setDataFromCheckConstraints`, `infoschema_reader.go:924`).
#[test]
#[ignore = "go-parity-gap: memtableRetriever.setDataFromCheckConstraints (infoschema_reader.go:924) is unported; no CHECK_CONSTRAINTS data source"]
fn check_constraints_lists_only_public_constraints_with_parenthesized_expr() {}

/// Go `pkg/executor/infoschema_reader_internal_test.go:92::TestSetDataFromTiDBCheckConstraints`:
/// `information_schema.TIDB_CHECK_CONSTRAINTS` adds TABLE_NAME and
/// TABLE_ID (6 columns): "def", "test", "t2_c1", "(id<10)", "t2", 2 -- again
/// skipping the DeleteOnly-state constraint
/// (`setDataFromTiDBCheckConstraints`, `infoschema_reader.go:969`).
#[test]
#[ignore = "go-parity-gap: memtableRetriever.setDataFromTiDBCheckConstraints (infoschema_reader.go:969) is unported"]
fn tidb_check_constraints_adds_table_name_and_table_id_columns() {}

/// Go `pkg/executor/infoschema_reader_internal_test.go:158::TestSetDataFromKeywords`:
/// `information_schema.KEYWORDS` starts at "ADD" with Reserved=1
/// (`setDataFromKeywords`, `infoschema_reader.go:4042`, driven by the
/// parser's keyword list).
#[test]
#[ignore = "go-parity-gap: memtableRetriever.setDataFromKeywords (infoschema_reader.go:4042) is unported; KEYWORDS has no row source"]
fn keywords_table_starts_with_add_reserved() {}

/// Go `pkg/executor/infoschema_reader_test.go:78::TestTiFlashSystemTableWithTiFlashV620`:
/// `information_schema.TIFLASH_SEGMENTS`/`TIFLASH_TABLES` proxy
/// `system.dt_segments`/`dt_tables` queries to TiFlash over the coprocessor
/// (`TiFlashSystemTableRetriever`, `infoschema_reader.go:3528`), parse the
/// JSON responses from `testdata/tiflash_v620_dt_*.json`, and fill missing
/// columns (older TiFlash) with NULL instead of warning.
#[test]
#[ignore = "go-parity-gap: TiFlashSystemTableRetriever (infoschema_reader.go:3528) needs the TiFlash coprocessor mock and its JSON testdata; unported"]
fn tiflash_system_tables_v620_fill_missing_columns_with_null() {}

/// Go `pkg/executor/infoschema_reader_test.go:138::TestTiFlashSystemTableWithTiFlashV630`:
/// same retriever against a v6.30 TiFlash layout
/// (`testdata/tiflash_v630_dt_segments.json`): segment rows carry keyspace
/// -less range keys and the row order follows the mocked response.
#[test]
#[ignore = "go-parity-gap: TiFlashSystemTableRetriever (infoschema_reader.go:3528) needs the TiFlash coprocessor mock and its JSON testdata; unported"]
fn tiflash_system_tables_v630_render_segment_rows_in_response_order() {}

/// Go `pkg/executor/infoschema_reader_test.go:183::TestTiFlashSystemTableWithTiFlashV640`:
/// same retriever against a v6.40 TiFlash layout
/// (`testdata/tiflash_v640_dt_tables.json`) for `TIFLASH_TABLES` rows.
#[test]
#[ignore = "go-parity-gap: TiFlashSystemTableRetriever (infoschema_reader.go:3528) needs the TiFlash coprocessor mock and its JSON testdata; unported"]
fn tiflash_system_tables_v640_render_table_rows() {}
