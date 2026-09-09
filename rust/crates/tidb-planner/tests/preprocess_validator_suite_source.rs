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

//! `pkg/planner.part14` DOCUMENTED GAP ports for
//! `pkg/planner/core/preprocess_test.go` (items :56, :359, :375, :401,
//! :422, :482).
//!
//! All six Go tests run `core.Preprocess` over a session
//! (`runSQL` helper :44-58) — the statement validation tier that checks
//! table DDL semantics against an infoschema, resolves CTE scopes, and
//! rewrites statements. This crate has no Preprocess pipeline, no
//! session/infoschema stack, and no AST `Restore` formatter; each item
//! below records its re-derived Go contract as an `#[ignore]` gap port.
//! Nothing is approximated.

/// GO PARITY GAP port of `pkg/planner/core/preprocess_test.go:56
/// TestValidator`.
///
/// go-parity-gap: needs `core.Preprocess` + the session type/DDL validators.
/// Go pins ~60 statement validations: `select ?` fails outside prepare and
/// passes inside it; illegal AUTO_INCREMENT defaults/placements
/// (types:1067, autoid:1075, "Incorrect column specifier"); column length
/// overflow for char/varchar per charset (types:1074, types:1439); duplicate
/// index column names (schema:1060); multiple primary keys (schema:1068);
/// and the legal float(53)/set-64/varchar-boundary `alter table` forms.
#[test]
#[ignore = "go-parity-gap: core.Preprocess session validation tier unported"]
fn validator_rejects_illegal_column_index_and_default_definitions() {}

/// GO PARITY GAP port of `pkg/planner/core/preprocess_test.go:359
/// TestForeignKey`.
///
/// go-parity-gap: needs `core.Preprocess` over a Domain infoschema with
/// cross-database tables. Go pins that ALTER TABLE ... ADD FOREIGN KEY
/// statements referencing tables in the same or another database pass
/// preprocess (three statements over test.t1/test.t2/test2.t).
#[test]
#[ignore = "go-parity-gap: Preprocess foreign-key resolution over a domain infoschema unported"]
fn foreign_key_alter_statements_pass_preprocess() {}

/// GO PARITY GAP port of `pkg/planner/core/preprocess_test.go:375
/// TestDropGlobalTempTable`.
///
/// go-parity-gap: needs `core.Preprocess` + temp-table metadata. Go pins
/// that `DROP GLOBAL TEMPORARY TABLE` errors
/// `ErrDropTableOnTemporaryTable` for a normal table, a LOCAL temporary
/// table (with or without db qualifier), and any multi-table list
/// containing one -- while global temporary tables (cross-database, in
/// lists) drop cleanly.
#[test]
#[ignore = "go-parity-gap: Preprocess + global/local temp-table classification unported"]
fn drop_global_temp_table_scope_errors() {}

/// GO PARITY GAP port of `pkg/planner/core/preprocess_test.go:401
/// TestLargeVarcharAutoConv` (issue #30328).
///
/// go-parity-gap: needs `core.Preprocess` + the session warning counter and
/// sql_mode handling. Go pins that `varbinary(70000)` errors types:1074
/// (max 65535) under strict mode, while under NO_ENGINE_SUBSTITUTION the
/// oversized varchar/binary columns auto-convert and each statement
/// accumulates exactly one `ErrAutoConvert` warning (three total).
#[test]
#[ignore = "go-parity-gap: Preprocess auto-convert warnings + sql_mode session state unported"]
fn large_varchar_auto_conv_records_err_auto_convert_warnings() {}

/// GO PARITY GAP port of `pkg/planner/core/preprocess_test.go:422
/// TestPreprocessCTE`.
///
/// go-parity-gap: needs `core.Preprocess` name resolution plus the AST
/// `Restore` formatter. Go pins eight WITH-clause restorations: nested and
/// RECURSIVE CTEs re-qualify table names with their database, shadowed CTE
/// names keep inner scope resolution (`t1` inside its own redefinition
/// stays unqualified), subquery-within-projection nesting is preserved,
/// and string literals restore with the `_UTF8MB4` introducer.
#[test]
#[ignore = "go-parity-gap: Preprocess CTE scope resolution + AST Restore formatter unported"]
fn preprocess_cte_scope_resolution_matches_restored_sql() {}

/// GO PARITY GAP port of `pkg/planner/core/preprocess_test.go:482
/// TestPreprocessDeleteFromWithAlias` (issue #56726).
///
/// go-parity-gap: needs `core.Preprocess` + the multi-table delete alias
/// binding and the CREATE GLOBAL BINDING pipeline. Go pins that
/// `delete tt1 from t1 tt1,(select max(id) id from t2)tt2 where ...`
/// executes and that the same statement parses as a global binding hint
/// source.
#[test]
#[ignore = "go-parity-gap: multi-table delete alias binding + binding pipeline unported"]
fn preprocess_delete_from_with_alias_binds_derived_table() {}
