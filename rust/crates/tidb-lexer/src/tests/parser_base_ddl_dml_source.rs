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

//! Batch b051 port of `pkg/parser` part-1 unit tests (Go tests sorted by
//! file path + line number on `origin/master`, items 1-60).
//!
//! The range covers AST node text conversion (`SetText` /
//! `convertBinaryStringLiterals`), DDL/DML Visitor traversal, and
//! parse-then-Restore of DDL/DML AST subtrees. Those APIs live in Go's
//! `pkg/parser/ast` package and in Rust's `tidb-ast`/`tidb-parser` crates.
//! They are not exposed by `tidb-lexer`; moreover, both owning Rust crates
//! depend on `tidb-lexer`, so this crate cannot depend back on them. Every
//! test is therefore an explicit parity gap rather than a lexer
//! approximation or a test-local reimplementation.

// ---------------------------------------------------------------------------
// pkg/parser/ast/base_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/base_test.go::TestNodeSetText`.
#[test]
// go-parity-gap: Node.SetText/Text/OriginalText and charset.Encoding.Transform live in tidb-ast.
#[ignore = "go-parity-gap: Node.SetText/Text/OriginalText and charset.Encoding.Transform live in tidb-ast"]
fn node_set_text() {}

/// Go: `pkg/parser/ast/base_test.go::TestBinaryStringLiteralConversion`.
#[test]
// go-parity-gap: convertBinaryStringLiterals hex-encodes non-printable quoted strings in tidb-ast.
#[ignore = "go-parity-gap: convertBinaryStringLiterals hex-encodes non-printable quoted strings in tidb-ast"]
fn binary_string_literal_conversion() {}

/// Go: `pkg/parser/ast/base_test.go::TestBinaryStringLiteralSkipsComments`.
#[test]
// go-parity-gap: skipComment inside convertBinaryStringLiterals belongs to tidb-ast.
#[ignore = "go-parity-gap: skipComment inside convertBinaryStringLiterals belongs to tidb-ast"]
fn binary_string_literal_skips_comments() {}

/// Go: `pkg/parser/ast/base_test.go::TestBinaryStringLiteralNoBackslashEscapes`.
#[test]
// go-parity-gap: Node.SetNoBackslashEscapes and convertBinaryStringLiterals live in tidb-ast.
#[ignore = "go-parity-gap: Node.SetNoBackslashEscapes and convertBinaryStringLiterals live in tidb-ast"]
fn binary_string_literal_no_backslash_escapes() {}

/// Go: `pkg/parser/ast/base_test.go::TestBinaryStringLiteralGBK`.
#[test]
// go-parity-gap: GBK Encoding.Transform plus convertBinaryStringLiterals live in tidb-ast.
#[ignore = "go-parity-gap: GBK Encoding.Transform plus convertBinaryStringLiterals live in tidb-ast"]
fn binary_string_literal_gbk() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/ddl_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLVisitorCover`.
#[test]
// go-parity-gap: DDL node Visitor traversal belongs to tidb-ast.
#[ignore = "go-parity-gap: DDL node Visitor traversal belongs to tidb-ast"]
fn ddl_visitor_cover() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLIndexColNameRestore`.
#[test]
// go-parity-gap: CREATE INDEX IndexPartSpecification Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE INDEX IndexPartSpecification Restore requires tidb-parser/tidb-ast"]
fn ddl_index_col_name_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLIndexExprRestore`.
#[test]
// go-parity-gap: CREATE INDEX column/prefix Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE INDEX column/prefix Restore requires tidb-parser/tidb-ast"]
fn ddl_index_expr_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLOnDeleteRestore`.
#[test]
// go-parity-gap: FOREIGN KEY OnDeleteOpt Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: FOREIGN KEY OnDeleteOpt Restore requires tidb-parser/tidb-ast"]
fn ddl_on_delete_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLOnUpdateRestore`.
#[test]
// go-parity-gap: FOREIGN KEY OnUpdateOpt Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: FOREIGN KEY OnUpdateOpt Restore requires tidb-parser/tidb-ast"]
fn ddl_on_update_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLIndexOption`.
#[test]
// go-parity-gap: CREATE INDEX IndexOption Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE INDEX IndexOption Restore requires tidb-parser/tidb-ast"]
fn ddl_index_option() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestTableToTableRestore`.
#[test]
// go-parity-gap: RENAME TABLE TableToTable Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: RENAME TABLE TableToTable Restore requires tidb-parser/tidb-ast"]
fn table_to_table_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLReferenceDefRestore`.
#[test]
// go-parity-gap: FOREIGN KEY ReferenceDef Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: FOREIGN KEY ReferenceDef Restore requires tidb-parser/tidb-ast"]
fn ddl_reference_def_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLConstraintRestore`.
#[test]
// go-parity-gap: CREATE TABLE Constraint Restore (incl. clustered special comments) requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE TABLE Constraint Restore (incl. clustered special comments) requires tidb-parser/tidb-ast"]
fn ddl_constraint_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLColumnOptionRestore`.
#[test]
// go-parity-gap: CREATE TABLE ColumnOption Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE TABLE ColumnOption Restore requires tidb-parser/tidb-ast"]
fn ddl_column_option_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestGeneratedRestore`.
#[test]
// go-parity-gap: generated-column Restore with schema/table name flags requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: generated-column Restore with schema/table name flags requires tidb-parser/tidb-ast"]
fn generated_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLColumnDefRestore`.
#[test]
// go-parity-gap: CREATE TABLE ColumnDef type/option Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE TABLE ColumnDef type/option Restore requires tidb-parser/tidb-ast"]
fn ddl_column_def_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLTruncateTableStmtRestore`.
#[test]
// go-parity-gap: TruncateTableStmt parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: TruncateTableStmt parsing and Restore require tidb-parser/tidb-ast"]
fn ddl_truncate_table_stmt_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDDLDropTableStmtRestore`.
#[test]
// go-parity-gap: DropTableStmt parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: DropTableStmt parsing and Restore require tidb-parser/tidb-ast"]
fn ddl_drop_table_stmt_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestColumnPositionRestore`.
#[test]
// go-parity-gap: ALTER TABLE ColumnPosition Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: ALTER TABLE ColumnPosition Restore requires tidb-parser/tidb-ast"]
fn column_position_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestAlterTableSpecRestore`.
#[test]
// go-parity-gap: AlterTableSpec parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: AlterTableSpec parsing and Restore require tidb-parser/tidb-ast"]
fn alter_table_spec_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestAlterTableWithSpecialCommentRestore`.
#[test]
// go-parity-gap: placement-policy Restore with RestoreTiDBSpecialComment lives in tidb-ast.
#[ignore = "go-parity-gap: placement-policy Restore with RestoreTiDBSpecialComment lives in tidb-ast"]
fn alter_table_with_special_comment_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestAlterTableOptionRestore`.
#[test]
// go-parity-gap: ALTER TABLE option-list Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: ALTER TABLE option-list Restore requires tidb-parser/tidb-ast"]
fn alter_table_option_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestAdminRepairTableRestore`.
#[test]
// go-parity-gap: ADMIN REPAIR TABLE parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: ADMIN REPAIR TABLE parsing and Restore require tidb-parser/tidb-ast"]
fn admin_repair_table_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestAdminOptimizeTableRestore`.
#[test]
// go-parity-gap: OPTIMIZE TABLE parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: OPTIMIZE TABLE parsing and Restore require tidb-parser/tidb-ast"]
fn admin_optimize_table_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestSequenceRestore`.
#[test]
// go-parity-gap: CREATE/DROP SEQUENCE parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE/DROP SEQUENCE parsing and Restore require tidb-parser/tidb-ast"]
fn sequence_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestIfExistsRestore`.
#[test]
// go-parity-gap: IF EXISTS/IF NOT EXISTS Restore (incl. /*T! */ wrapping) requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: IF EXISTS/IF NOT EXISTS Restore (incl. /*T! */ wrapping) requires tidb-parser/tidb-ast"]
fn if_exists_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestAlterDatabaseRestore`.
#[test]
// go-parity-gap: ALTER DATABASE charset/collate/placement Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: ALTER DATABASE charset/collate/placement Restore requires tidb-parser/tidb-ast"]
fn alter_database_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestCreatePlacementPolicyRestore`.
#[test]
// go-parity-gap: CREATE PLACEMENT POLICY parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE PLACEMENT POLICY parsing and Restore require tidb-parser/tidb-ast"]
fn create_placement_policy_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestAlterPlacementPolicyRestore`.
#[test]
// go-parity-gap: ALTER PLACEMENT POLICY parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: ALTER PLACEMENT POLICY parsing and Restore require tidb-parser/tidb-ast"]
fn alter_placement_policy_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestDropPlacementPolicyRestore`.
#[test]
// go-parity-gap: DROP PLACEMENT POLICY parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: DROP PLACEMENT POLICY parsing and Restore require tidb-parser/tidb-ast"]
fn drop_placement_policy_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestRemovePlacementRestore`.
#[test]
// go-parity-gap: SkipPlacementRuleForRestore stripping lives in tidb-ast Restore.
#[ignore = "go-parity-gap: SkipPlacementRuleForRestore stripping lives in tidb-ast Restore"]
fn remove_placement_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestFlashBackDatabaseRestore`.
#[test]
// go-parity-gap: FLASHBACK DATABASE parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: FLASHBACK DATABASE parsing and Restore require tidb-parser/tidb-ast"]
fn flash_back_database_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestTableOptionTTLRestore`.
#[test]
// go-parity-gap: TTL table-option Restore (incl. /*T![ttl] */) requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: TTL table-option Restore (incl. /*T![ttl] */) requires tidb-parser/tidb-ast"]
fn table_option_ttl_restore() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestTableOptionTTLRestoreWithTTLEnableOffFlag`.
#[test]
// go-parity-gap: RestoreWithTTLEnableOff rewriting lives in tidb-ast Restore.
#[ignore = "go-parity-gap: RestoreWithTTLEnableOff rewriting lives in tidb-ast Restore"]
fn table_option_ttl_restore_with_ttl_enable_off_flag() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestPresplitIndexSpecialComments`.
#[test]
// go-parity-gap: PRE_SPLIT_REGIONS special-comment Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: PRE_SPLIT_REGIONS special-comment Restore requires tidb-parser/tidb-ast"]
fn presplit_index_special_comments() {}

/// Go: `pkg/parser/ast/ddl_test.go::TestResourceGroupDDLStmtRestore`.
#[test]
// go-parity-gap: CREATE/ALTER RESOURCE GROUP Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CREATE/ALTER RESOURCE GROUP Restore requires tidb-parser/tidb-ast"]
fn resource_group_ddl_stmt_restore() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/dml_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/dml_test.go::TestDMLVisitorCover`.
#[test]
// go-parity-gap: DML node Visitor traversal belongs to tidb-ast.
#[ignore = "go-parity-gap: DML node Visitor traversal belongs to tidb-ast"]
fn dml_visitor_cover() {}

/// Go: `pkg/parser/ast/dml_test.go::TestTableNameRestore`.
#[test]
// go-parity-gap: TableName identifier quoting Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: TableName identifier quoting Restore requires tidb-parser/tidb-ast"]
fn table_name_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestTableNameIndexHintsRestore`.
#[test]
// go-parity-gap: table-name index-hint Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: table-name index-hint Restore requires tidb-parser/tidb-ast"]
fn table_name_index_hints_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestLimitRestore`.
#[test]
// go-parity-gap: SELECT Limit Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: SELECT Limit Restore requires tidb-parser/tidb-ast"]
fn limit_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestWildCardFieldRestore`.
#[test]
// go-parity-gap: WildCardField Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: WildCardField Restore requires tidb-parser/tidb-ast"]
fn wild_card_field_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestSelectFieldRestore`.
#[test]
// go-parity-gap: SelectField Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: SelectField Restore requires tidb-parser/tidb-ast"]
fn select_field_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestFieldListRestore`.
#[test]
// go-parity-gap: FieldList Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: FieldList Restore requires tidb-parser/tidb-ast"]
fn field_list_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestTableSourceRestore`.
#[test]
// go-parity-gap: TableSource alias/subquery Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: TableSource alias/subquery Restore requires tidb-parser/tidb-ast"]
fn table_source_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestOnConditionRestore`.
#[test]
// go-parity-gap: JOIN OnCondition Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: JOIN OnCondition Restore requires tidb-parser/tidb-ast"]
fn on_condition_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestJoinRestore`.
#[test]
// go-parity-gap: Join tree Restore (incl. associativity rewrite) requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: Join tree Restore (incl. associativity rewrite) requires tidb-parser/tidb-ast"]
fn join_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestTableRefsClauseRestore`.
#[test]
// go-parity-gap: TableRefsClause Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: TableRefsClause Restore requires tidb-parser/tidb-ast"]
fn table_refs_clause_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestDeleteTableListRestore`.
#[test]
// go-parity-gap: DELETE table-list Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: DELETE table-list Restore requires tidb-parser/tidb-ast"]
fn delete_table_list_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestDeleteTableIndexHintRestore`.
#[test]
// go-parity-gap: DELETE ... USE INDEX Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: DELETE ... USE INDEX Restore requires tidb-parser/tidb-ast"]
fn delete_table_index_hint_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestByItemRestore`.
#[test]
// go-parity-gap: ORDER BY ByItem Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: ORDER BY ByItem Restore requires tidb-parser/tidb-ast"]
fn by_item_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestGroupByClauseRestore`.
#[test]
// go-parity-gap: GROUP BY clause Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: GROUP BY clause Restore requires tidb-parser/tidb-ast"]
fn group_by_clause_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestOrderByClauseRestore`.
#[test]
// go-parity-gap: ORDER BY clause Restore (SELECT and UNION) requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: ORDER BY clause Restore (SELECT and UNION) requires tidb-parser/tidb-ast"]
fn order_by_clause_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestAssignmentRestore`.
#[test]
// go-parity-gap: UPDATE Assignment Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: UPDATE Assignment Restore requires tidb-parser/tidb-ast"]
fn assignment_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestHavingClauseRestore`.
#[test]
// go-parity-gap: HAVING clause Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: HAVING clause Restore requires tidb-parser/tidb-ast"]
fn having_clause_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestFrameBoundRestore`.
#[test]
// go-parity-gap: window FrameBound Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: window FrameBound Restore requires tidb-parser/tidb-ast"]
fn frame_bound_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestFrameClauseRestore`.
#[test]
// go-parity-gap: window FrameClause Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: window FrameClause Restore requires tidb-parser/tidb-ast"]
fn frame_clause_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestPartitionByClauseRestore`.
#[test]
// go-parity-gap: window PARTITION BY Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: window PARTITION BY Restore requires tidb-parser/tidb-ast"]
fn partition_by_clause_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestWindowSpecRestore`.
#[test]
// go-parity-gap: WINDOW spec Restore requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: WINDOW spec Restore requires tidb-parser/tidb-ast"]
fn window_spec_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestLoadDataRestore`.
#[test]
// go-parity-gap: LOAD DATA parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: LOAD DATA parsing and Restore require tidb-parser/tidb-ast"]
fn load_data_restore() {}
