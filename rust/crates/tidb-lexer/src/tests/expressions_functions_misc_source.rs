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

//! Batch b052 port of `pkg/parser` part-2 unit tests (Go tests sorted by
//! file path + line number on `origin/master`, items 61-120).
//!
//! The range exercises AST restore/formatting, visitor traversal, expression
//! flags, sensitive-text redaction, `CIStr`, stored procedures, and SEM command
//! classification. Those APIs live in the Go `pkg/parser/ast` package and in
//! Rust's `tidb-ast`/`tidb-parser` crates. They are not exposed by
//! `tidb-lexer`; moreover, both owning Rust crates depend on `tidb-lexer`, so
//! this crate cannot depend back on them. Every test is therefore an explicit
//! parity gap rather than a lexer approximation or a test-local reimplementation.

// ---------------------------------------------------------------------------
// pkg/parser/ast/dml_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/dml_test.go::TestImportActions`.
#[test]
// go-parity-gap: import-action parsing and AST Restore are not exposed by tidb-lexer.
#[ignore = "go-parity-gap: import-action parsing and AST Restore are not exposed by tidb-lexer"]
fn import_actions() {}

/// Go: `pkg/parser/ast/dml_test.go::TestImportIntoRestore`.
#[test]
// go-parity-gap: ImportIntoStmt parsing and Restore require tidb-parser and tidb-ast.
#[ignore = "go-parity-gap: ImportIntoStmt parsing and Restore require tidb-parser and tidb-ast"]
fn import_into_restore() {}

/// Go: `pkg/parser/ast/dml_test.go::TestFulltextSearchModifier`.
#[test]
// go-parity-gap: FulltextSearchModifier bitmask helpers live in tidb-ast, not tidb-lexer.
#[ignore = "go-parity-gap: FulltextSearchModifier bitmask helpers live in tidb-ast, not tidb-lexer"]
fn fulltext_search_modifier() {}

/// Go: `pkg/parser/ast/dml_test.go::TestImportIntoSecureText`.
#[test]
// go-parity-gap: ImportIntoStmt SecureText and URL redaction live in tidb-ast.
#[ignore = "go-parity-gap: ImportIntoStmt SecureText and URL redaction live in tidb-ast"]
fn import_into_secure_text() {}

/// Go: `pkg/parser/ast/dml_test.go::TestImportIntoFromSelectInvalidStmt`.
#[test]
// go-parity-gap: IMPORT INTO FROM SELECT semantic validation is a parser grammar action.
#[ignore = "go-parity-gap: IMPORT INTO FROM SELECT semantic validation is a parser grammar action"]
fn import_into_from_select_invalid_stmt() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/expressions_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/expressions_test.go::TestExpresionsVisitorCover`.
#[test]
// go-parity-gap: expression-node Visitor traversal belongs to tidb-ast.
#[ignore = "go-parity-gap: expression-node Visitor traversal belongs to tidb-ast"]
fn expresions_visitor_cover() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestUnaryOperationExprRestore`.
#[test]
// go-parity-gap: UnaryOperationExpr parsing and Restore belong to the AST/parser crates.
#[ignore = "go-parity-gap: UnaryOperationExpr parsing and Restore belong to the AST/parser crates"]
fn unary_operation_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestColumnNameExprRestore`.
#[test]
// go-parity-gap: ColumnNameExpr identifier quoting is an AST Restore behavior.
#[ignore = "go-parity-gap: ColumnNameExpr identifier quoting is an AST Restore behavior"]
fn column_name_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestIsNullExprRestore`.
#[test]
// go-parity-gap: IsNullExpr parsing and Restore are not exposed by tidb-lexer.
#[ignore = "go-parity-gap: IsNullExpr parsing and Restore are not exposed by tidb-lexer"]
fn is_null_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestIsTruthRestore`.
#[test]
// go-parity-gap: IsTruthExpr parsing and Restore are not exposed by tidb-lexer.
#[ignore = "go-parity-gap: IsTruthExpr parsing and Restore are not exposed by tidb-lexer"]
fn is_truth_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestBetweenExprRestore`.
#[test]
// go-parity-gap: BetweenExpr precedence-aware Restore belongs to tidb-ast.
#[ignore = "go-parity-gap: BetweenExpr precedence-aware Restore belongs to tidb-ast"]
fn between_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestCaseExpr`.
#[test]
// go-parity-gap: CaseExpr parsing and Restore require the AST/parser crates.
#[ignore = "go-parity-gap: CaseExpr parsing and Restore require the AST/parser crates"]
fn case_expr() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestBinaryOperationExpr`.
#[test]
// go-parity-gap: BinaryOperationExpr canonical operator Restore belongs to tidb-ast.
#[ignore = "go-parity-gap: BinaryOperationExpr canonical operator Restore belongs to tidb-ast"]
fn binary_operation_expr() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestBinaryOperationExprWithFlags`.
#[test]
// go-parity-gap: binary-operation Restore flags and AST formatting are unavailable here.
#[ignore = "go-parity-gap: binary-operation Restore flags and AST formatting are unavailable here"]
fn binary_operation_expr_with_flags() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestParenthesesExpr`.
#[test]
// go-parity-gap: precedence-aware ParenthesesExpr Restore belongs to tidb-ast.
#[ignore = "go-parity-gap: precedence-aware ParenthesesExpr Restore belongs to tidb-ast"]
fn parentheses_expr() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestWhenClause`.
#[test]
// go-parity-gap: WhenClause parsing and Restore require the AST/parser crates.
#[ignore = "go-parity-gap: WhenClause parsing and Restore require the AST/parser crates"]
fn when_clause() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestDefaultExpr`.
#[test]
// go-parity-gap: DefaultExpr parsing and Restore require the AST/parser crates.
#[ignore = "go-parity-gap: DefaultExpr parsing and Restore require the AST/parser crates"]
fn default_expr() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestPatternInExprRestore`.
#[test]
// go-parity-gap: PatternInExpr list/subquery Restore belongs to tidb-ast.
#[ignore = "go-parity-gap: PatternInExpr list/subquery Restore belongs to tidb-ast"]
fn pattern_in_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestPatternLikeExprRestore`.
#[test]
// go-parity-gap: PatternLikeOrIlikeExpr Restore belongs to tidb-ast.
#[ignore = "go-parity-gap: PatternLikeOrIlikeExpr Restore belongs to tidb-ast"]
fn pattern_like_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestValuesExpr`.
#[test]
// go-parity-gap: ValuesExpr AST construction and Restore are unavailable in tidb-lexer.
#[ignore = "go-parity-gap: ValuesExpr AST construction and Restore are unavailable in tidb-lexer"]
fn values_expr() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestPatternRegexpExprRestore`.
#[test]
// go-parity-gap: PatternRegexpExpr canonical REGEXP Restore belongs to tidb-ast.
#[ignore = "go-parity-gap: PatternRegexpExpr canonical REGEXP Restore belongs to tidb-ast"]
fn pattern_regexp_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestRowExprRestore`.
#[test]
// go-parity-gap: RowExpr parsing and Restore require the AST/parser crates.
#[ignore = "go-parity-gap: RowExpr parsing and Restore require the AST/parser crates"]
fn row_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestMaxValueExprRestore`.
#[test]
// go-parity-gap: partition AST extraction and MaxValueExpr Restore are unavailable here.
#[ignore = "go-parity-gap: partition AST extraction and MaxValueExpr Restore are unavailable here"]
fn max_value_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestPositionExprRestore`.
#[test]
// go-parity-gap: ORDER BY PositionExpr AST Restore belongs to tidb-ast.
#[ignore = "go-parity-gap: ORDER BY PositionExpr AST Restore belongs to tidb-ast"]
fn position_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestExistsSubqueryExprRestore`.
#[test]
// go-parity-gap: subquery parsing and ExistsSubqueryExpr Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: subquery parsing and ExistsSubqueryExpr Restore require tidb-parser/tidb-ast"]
fn exists_subquery_expr_restore() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestVariableExpr`.
#[test]
// go-parity-gap: VariableExpr scope normalization and Restore belong to tidb-ast.
#[ignore = "go-parity-gap: VariableExpr scope normalization and Restore belong to tidb-ast"]
fn variable_expr() {}

/// Go: `pkg/parser/ast/expressions_test.go::TestMatchAgainstExpr`.
#[test]
// go-parity-gap: MATCH AGAINST expression parsing and AST Restore are unavailable here.
#[ignore = "go-parity-gap: MATCH AGAINST expression parsing and AST Restore are unavailable here"]
fn match_against_expr() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/flag_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/flag_test.go::TestHasAggFlag`.
#[test]
// go-parity-gap: ExprNode flag storage and HasAggFlag live in tidb-ast.
#[ignore = "go-parity-gap: ExprNode flag storage and HasAggFlag live in tidb-ast"]
fn has_agg_flag() {}

/// Go: `pkg/parser/ast/flag_test.go::TestFlag`.
#[test]
// go-parity-gap: parser-built expression trees and the SetFlag visitor live outside tidb-lexer.
#[ignore = "go-parity-gap: parser-built expression trees and the SetFlag visitor live outside tidb-lexer"]
fn flag() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/format_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/format_test.go::TestAstFormat`.
#[test]
// go-parity-gap: AST Format output requires parsed expression nodes, not lexer tokens.
#[ignore = "go-parity-gap: AST Format output requires parsed expression nodes, not lexer tokens"]
fn ast_format() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/functions_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/functions_test.go::TestFunctionsVisitorCover`.
#[test]
// go-parity-gap: function-expression Visitor traversal belongs to tidb-ast.
#[ignore = "go-parity-gap: function-expression Visitor traversal belongs to tidb-ast"]
fn functions_visitor_cover() {}

/// Go: `pkg/parser/ast/functions_test.go::TestFuncCallExprRestore`.
#[test]
// go-parity-gap: special-form function parsing and FuncCallExpr Restore require AST/parser APIs.
#[ignore = "go-parity-gap: special-form function parsing and FuncCallExpr Restore require AST/parser APIs"]
fn func_call_expr_restore() {}

/// Go: `pkg/parser/ast/functions_test.go::TestFuncCastExprRestore`.
#[test]
// go-parity-gap: FuncCastExpr type/charset Restore belongs to tidb-ast.
#[ignore = "go-parity-gap: FuncCastExpr type/charset Restore belongs to tidb-ast"]
fn func_cast_expr_restore() {}

/// Go: `pkg/parser/ast/functions_test.go::TestAggregateFuncExprRestore`.
#[test]
// go-parity-gap: AggregateFuncExpr normalization and Restore belong to tidb-ast.
#[ignore = "go-parity-gap: AggregateFuncExpr normalization and Restore belong to tidb-ast"]
fn aggregate_func_expr_restore() {}

/// Go: `pkg/parser/ast/functions_test.go::TestConvert`.
#[test]
// go-parity-gap: CONVERT USING grammar validation and canonical charset AST values need tidb-parser.
#[ignore = "go-parity-gap: CONVERT USING grammar validation and canonical charset AST values need tidb-parser"]
fn convert() {}

/// Go: `pkg/parser/ast/functions_test.go::TestChar`.
#[test]
// go-parity-gap: CHAR USING grammar validation and canonical charset AST values need tidb-parser.
#[ignore = "go-parity-gap: CHAR USING grammar validation and canonical charset AST values need tidb-parser"]
fn r#char() {}

/// Go: `pkg/parser/ast/functions_test.go::TestWindowFuncExprRestore`.
#[test]
// go-parity-gap: WindowFuncExpr parsing and Restore require the AST/parser crates.
#[ignore = "go-parity-gap: WindowFuncExpr parsing and Restore require the AST/parser crates"]
fn window_func_expr_restore() {}

/// Go: `pkg/parser/ast/functions_test.go::TestGenericFuncRestore`.
#[test]
// go-parity-gap: generic-function schema/name AST Restore is unavailable in tidb-lexer.
#[ignore = "go-parity-gap: generic-function schema/name AST Restore is unavailable in tidb-lexer"]
fn generic_func_restore() {}

/// Go: `pkg/parser/ast/functions_test.go::TestRestoreWithError`.
#[test]
// go-parity-gap: json_memberof AST arity validation occurs in FuncCallExpr Restore.
#[ignore = "go-parity-gap: json_memberof AST arity validation occurs in FuncCallExpr Restore"]
fn restore_with_error() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/misc_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/misc_test.go::TestMiscVisitorCover`.
#[test]
// go-parity-gap: statement-node Visitor traversal belongs to tidb-ast.
#[ignore = "go-parity-gap: statement-node Visitor traversal belongs to tidb-ast"]
fn misc_visitor_cover() {}

/// Go: `pkg/parser/ast/misc_test.go::TestDDLVisitorCoverMisc`.
#[test]
// go-parity-gap: parsing DDL statements and traversing their AST nodes requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: parsing DDL statements and traversing their AST nodes requires tidb-parser/tidb-ast"]
fn ddl_visitor_cover_misc() {}

/// Go: `pkg/parser/ast/misc_test.go::TestDMLVistorCover`.
#[test]
// go-parity-gap: parsing DML statements and traversing their AST nodes requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: parsing DML statements and traversing their AST nodes requires tidb-parser/tidb-ast"]
fn dml_vistor_cover() {}

/// Go: `pkg/parser/ast/misc_test.go::TestSensitiveStatement`.
#[test]
// go-parity-gap: SensitiveStmtNode interface implementations are defined by tidb-ast.
#[ignore = "go-parity-gap: SensitiveStmtNode interface implementations are defined by tidb-ast"]
fn sensitive_statement() {}

/// Go: `pkg/parser/ast/misc_test.go::TestTableOptimizerHintRestore`.
#[test]
// go-parity-gap: optimizer-hint grammar and TableOptimizerHint Restore are outside the lexer API.
#[ignore = "go-parity-gap: optimizer-hint grammar and TableOptimizerHint Restore are outside the lexer API"]
fn table_optimizer_hint_restore() {}

/// Go: `pkg/parser/ast/misc_test.go::TestBRIESecureText`.
#[test]
// go-parity-gap: BRIE parsing, AST Restore, and SecureText redaction live outside tidb-lexer.
#[ignore = "go-parity-gap: BRIE parsing, AST Restore, and SecureText redaction live outside tidb-lexer"]
fn brie_secure_text() {}

/// Go: `pkg/parser/ast/misc_test.go::TestCompactTableStmtRestore`.
#[test]
// go-parity-gap: CompactTableStmt parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: CompactTableStmt parsing and Restore require tidb-parser/tidb-ast"]
fn compact_table_stmt_restore() {}

/// Go: `pkg/parser/ast/misc_test.go::TestPlanReplayerStmtRestore`.
#[test]
// go-parity-gap: PlanReplayerStmt parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: PlanReplayerStmt parsing and Restore require tidb-parser/tidb-ast"]
fn plan_replayer_stmt_restore() {}

/// Go: `pkg/parser/ast/misc_test.go::TestRedactURL`.
#[test]
// go-parity-gap: scheme-aware URL parsing, key normalization, and redaction live in tidb-ast.
#[ignore = "go-parity-gap: scheme-aware URL parsing, key normalization, and redaction live in tidb-ast"]
fn redact_url() {}

/// Go: `pkg/parser/ast/misc_test.go::TestAddQueryWatchStmtRestore`.
#[test]
// go-parity-gap: AddQueryWatchStmt parsing and Restore require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: AddQueryWatchStmt parsing and Restore require tidb-parser/tidb-ast"]
fn add_query_watch_stmt_restore() {}

/// Go: `pkg/parser/ast/misc_test.go::TestRedactTrafficStmt`.
#[test]
// go-parity-gap: TrafficStmt SecureText combines AST Restore, password masking, and URL redaction.
#[ignore = "go-parity-gap: TrafficStmt SecureText combines AST Restore, password masking, and URL redaction"]
fn redact_traffic_stmt() {}

/// Go: `pkg/parser/ast/misc_test.go::TestSetStmtSecureTextRedactsEmbeddingAPIKeys`.
#[test]
// go-parity-gap: SetStmt AST assignment classification and SecureText redaction live in tidb-ast.
#[ignore = "go-parity-gap: SetStmt AST assignment classification and SecureText redaction live in tidb-ast"]
fn set_stmt_secure_text_redacts_embedding_api_keys() {}

/// Go: `pkg/parser/ast/misc_test.go::TestSetPwdStmtSecureText`.
#[test]
// go-parity-gap: SetPwdStmt and user-identity SecureText formatting live in tidb-ast.
#[ignore = "go-parity-gap: SetPwdStmt and user-identity SecureText formatting live in tidb-ast"]
fn set_pwd_stmt_secure_text() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/model_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/model_test.go::TestT`.
#[test]
// go-parity-gap: CIStr original/lowercase representation lives in tidb-ast.
#[ignore = "go-parity-gap: CIStr original/lowercase representation lives in tidb-ast"]
fn t() {}

/// Go: `pkg/parser/ast/model_test.go::TestUnmarshalCIStr`.
#[test]
// go-parity-gap: CIStr backward-compatible JSON decoding and encoding live in tidb-ast.
#[ignore = "go-parity-gap: CIStr backward-compatible JSON decoding and encoding live in tidb-ast"]
fn unmarshal_ci_str() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/procedure_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/procedure_test.go::TestProcedureVisitorCover`.
#[test]
// go-parity-gap: stored-procedure node Visitor traversal belongs to tidb-ast.
#[ignore = "go-parity-gap: stored-procedure node Visitor traversal belongs to tidb-ast"]
fn procedure_visitor_cover() {}

/// Go: `pkg/parser/ast/procedure_test.go::TestProcedure`.
#[test]
// go-parity-gap: stored-procedure grammar acceptance requires tidb-parser.
#[ignore = "go-parity-gap: stored-procedure grammar acceptance requires tidb-parser"]
fn procedure() {}

/// Go: `pkg/parser/ast/procedure_test.go::TestShowCreateProcedure`.
#[test]
// go-parity-gap: SHOW CREATE/DROP PROCEDURE parsing and AST typing require tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: SHOW CREATE/DROP PROCEDURE parsing and AST typing require tidb-parser/tidb-ast"]
fn show_create_procedure() {}

/// Go: `pkg/parser/ast/procedure_test.go::TestProcedureVisitor`.
#[test]
// go-parity-gap: parsed stored-procedure tree traversal requires tidb-parser/tidb-ast.
#[ignore = "go-parity-gap: parsed stored-procedure tree traversal requires tidb-parser/tidb-ast"]
fn procedure_visitor() {}

/// Go: `pkg/parser/ast/procedure_test.go::TestProcedureRestore`.
#[test]
// go-parity-gap: stored-procedure AST Restore is not exposed by tidb-lexer.
#[ignore = "go-parity-gap: stored-procedure AST Restore is not exposed by tidb-lexer"]
fn procedure_restore() {}

// ---------------------------------------------------------------------------
// pkg/parser/ast/sem_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/ast/sem_test.go::TestShowCommand`.
#[test]
// go-parity-gap: ShowStmtType-to-SEM-command classification belongs to tidb-ast.
#[ignore = "go-parity-gap: ShowStmtType-to-SEM-command classification belongs to tidb-ast"]
fn show_command() {}
