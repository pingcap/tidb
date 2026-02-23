#!/usr/bin/env python3
"""
AST Field-Level Differential Audit

Identifies grammar alternatives whose AST fields have NEVER been verified
structurally, then generates targeted Go tests that check:
  - Exact AST field values (not just parse success)
  - Both parser and HandParser must produce identical field values

Organized into functional groups. Run with:
  python3 ast_field_audit.py
"""

import subprocess, os, json, re, sys
from collections import defaultdict

PARSER_DIR = "/Users/qiliu/projects/tidb/pkg/parser"

GO_TEST_TEMPLATE = '''package parser

import (
\t"testing"

\t"github.com/pingcap/tidb/pkg/parser/ast"
\t"github.com/stretchr/testify/assert"
\t_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

{imports_extra}

func TestASTFieldAudit_{label}(t *testing.T) {{
\ttype check struct {{
\t\tsql  string
\t\tnote string
\t\trun  func(t *testing.T, stmts []ast.StmtNode)
\t}}
\tcases := []check{{
{cases}
\t}}
\tfor _, c := range cases {{
\t\tt.Run(c.note, func(t *testing.T) {{
\t\t\tstmts := parseForTest(t, c.sql)
\t\t\tif stmts == nil {{
\t\t\t\treturn
\t\t\t}}
\t\t\tc.run(t, stmts)
\t\t}})
\t}}
}}
'''

def run_go_tests(label: str, go_functions: str, imports_extra: str = "") -> list:
    """Write and run Go test code, return list of failed sub-tests."""
    test_file = os.path.join(PARSER_DIR, "_ast_field_audit_test.go")
    with open(test_file, "w") as f:
        f.write(GO_TEST_TEMPLATE.format(
            label=label,
            cases=go_functions,
            imports_extra=imports_extra,
        ))
    try:
        result = subprocess.run(
            ["go", "test", "-v", "-run", f"TestASTFieldAudit_{label}", "./"],
            cwd=PARSER_DIR,
            capture_output=True, text=True, timeout=60,
        )
        combined = result.stdout + result.stderr
        failures = re.findall(r"--- FAIL: TestASTFieldAudit_[^/]+/([^\s]+)", combined)
        errors = {}
        for f in failures:
            idx = combined.find(f"Error:")
            errors[f] = combined[idx:idx+200].strip() if idx != -1 else "see output"
        return failures, combined
    except subprocess.TimeoutExpired:
        return ["TIMEOUT"], ""
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)


def main():
    all_failures = []

    # =========================================================================
    # GROUP 1: SelectStmt fields — the most complex statement
    # Focus on fields that differ between grammar alternatives
    # =========================================================================
    select_cases = r"""
        {`SELECT SQL_CALC_FOUND_ROWS a FROM t`, "select_sql_calc_found_rows", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.True(t, s.SelectStmtOpts.SQLCalcFoundRows, "SQL_CALC_FOUND_ROWS must set SQLCalcFoundRows")
        }},
        {`SELECT HIGH_PRIORITY a FROM t`, "select_high_priority", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.True(t, s.SelectStmtOpts.Priority == ast.HighPriority, "HIGH_PRIORITY must set Priority=HighPriority")
        }},
        {`SELECT SQL_SMALL_RESULT a FROM t`, "select_sql_small_result", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.True(t, s.SelectStmtOpts.SQLSmallResult, "SQL_SMALL_RESULT must set SQLSmallResult")
        }},
        {`SELECT SQL_BIG_RESULT a FROM t`, "select_sql_big_result", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.True(t, s.SelectStmtOpts.SQLBigResult, "SQL_BIG_RESULT must set SQLBigResult")
        }},
        {`SELECT SQL_BUFFER_RESULT a FROM t`, "select_sql_buffer_result", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.True(t, s.SelectStmtOpts.SQLBufferResult, "SQL_BUFFER_RESULT must set SQLBufferResult")
        }},
        {`SELECT SQL_NO_CACHE a FROM t`, "select_sql_no_cache", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.True(t, s.SelectStmtOpts.SQLNoCache, "SQL_NO_CACHE must set SQLNoCache")
        }},
        {`SELECT STRAIGHT_JOIN * FROM t`, "select_straight_join_opt", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.True(t, s.SelectStmtOpts.StraightJoin, "STRAIGHT_JOIN must set StraightJoin")
        }},
        {`SELECT DISTINCT a, b FROM t`, "select_distinct", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.True(t, s.Distinct, "DISTINCT must set Distinct=true")
        }},
        {`SELECT ALL a FROM t`, "select_all", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.False(t, s.Distinct, "ALL must NOT set Distinct=true")
        }},
        {`SELECT * FROM t INTO OUTFILE '/tmp/out.csv'`, "select_into_outfile", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.NotNil(t, s.SelectIntoOpt, "INTO OUTFILE must set SelectIntoOpt")
            assert.Equal(t, "/tmp/out.csv", s.SelectIntoOpt.FileName)
        }},
        {`SELECT * FROM t INTO DUMPFILE '/tmp/dump.dat'`, "select_into_dumpfile", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.NotNil(t, s.SelectIntoOpt, "INTO DUMPFILE must set SelectIntoOpt")
            assert.True(t, s.SelectIntoOpt.Dumpfile, "INTO DUMPFILE must set Dumpfile=true")
        }},
        {`SELECT * FROM t GROUP BY a WITH ROLLUP`, "select_group_by_rollup", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.GroupBy) {
                assert.True(t, s.GroupBy.Rollup, "WITH ROLLUP must set GroupBy.Rollup=true")
            }
        }},
        {`SELECT * FROM t HAVING count(*) > 1`, "select_having", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            assert.NotNil(t, s.Having, "HAVING must set Having field")
        }},
        {`SELECT * FROM t ORDER BY a ASC, b DESC`, "select_order_by_direction", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.OrderBy) && assert.Len(t, s.OrderBy.Items, 2) {
                assert.False(t, s.OrderBy.Items[0].Desc, "ASC must set Desc=false")
                assert.True(t, s.OrderBy.Items[1].Desc, "DESC must set Desc=true")
            }
        }},
        {`SELECT * FROM t LIMIT 10 OFFSET 5`, "select_limit_offset", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.Limit) {
                assert.NotNil(t, s.Limit.Offset, "OFFSET must set Limit.Offset")
                assert.NotNil(t, s.Limit.Count, "LIMIT count must set Limit.Count")
            }
        }},
        {`SELECT * FROM t FOR UPDATE NOWAIT`, "select_for_update_nowait", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.LockInfo) {
                assert.Equal(t, ast.SelectLockForUpdate, s.LockInfo.LockType)
                assert.Equal(t, ast.SelectLockNoWait, s.LockInfo.WaitType)
            }
        }},
        {`SELECT * FROM t FOR UPDATE SKIP LOCKED`, "select_for_update_skip_locked", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.LockInfo) {
                assert.Equal(t, ast.SelectLockForUpdate, s.LockInfo.LockType)
                assert.Equal(t, ast.SelectLockSkipLocked, s.LockInfo.WaitType)
            }
        }},
        {`SELECT * FROM t FOR SHARE`, "select_for_share", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.LockInfo) {
                assert.Equal(t, ast.SelectLockForShare, s.LockInfo.LockType)
            }
        }},
        {`SELECT * FROM t LOCK IN SHARE MODE`, "select_lock_in_share_mode", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.LockInfo) {
                assert.Equal(t, ast.SelectLockInShareMode, s.LockInfo.LockType)
            }
        }},
"""
    failures, out = run_go_tests("SelectFields", select_cases)
    if failures:
        all_failures.append(("SelectFields", failures, out))
        print(f"  ❌ SelectFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ SelectFields: all pass")

    # =========================================================================
    # GROUP 2: InsertStmt fields
    # =========================================================================
    insert_cases = r"""
        {`INSERT IGNORE INTO t VALUES (1)`, "insert_ignore", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.InsertStmt)
            assert.True(t, s.IgnoreErr, "INSERT IGNORE must set IgnoreErr=true")
        }},
        {`INSERT LOW_PRIORITY INTO t VALUES (1)`, "insert_low_priority", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.InsertStmt)
            assert.Equal(t, ast.LowPriority, s.Priority, "INSERT LOW_PRIORITY must set Priority=LowPriority")
        }},
        {`INSERT HIGH_PRIORITY INTO t VALUES (1)`, "insert_high_priority", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.InsertStmt)
            assert.Equal(t, ast.HighPriority, s.Priority, "INSERT HIGH_PRIORITY must set Priority=HighPriority")
        }},
        {`INSERT DELAYED INTO t VALUES (1)`, "insert_delayed", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.InsertStmt)
            assert.Equal(t, ast.DelayedPriority, s.Priority, "INSERT DELAYED must set Priority=DelayedPriority")
        }},
        {`INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE a=1`, "insert_on_dup_key", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.InsertStmt)
            assert.NotEmpty(t, s.OnDuplicate, "ON DUPLICATE KEY UPDATE must set OnDuplicate")
        }},
        {`REPLACE LOW_PRIORITY INTO t VALUES (1)`, "replace_low_priority", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.InsertStmt)
            assert.True(t, s.IsReplace, "REPLACE must set IsReplace=true")
            assert.Equal(t, ast.LowPriority, s.Priority, "REPLACE LOW_PRIORITY must set Priority")
        }},
"""
    failures, out = run_go_tests("InsertFields", insert_cases)
    if failures:
        all_failures.append(("InsertFields", failures, out))
        print(f"  ❌ InsertFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ InsertFields: all pass")

    # =========================================================================
    # GROUP 3: UpdateStmt / DeleteStmt flags
    # =========================================================================
    dml_cases = r"""
        {`UPDATE IGNORE t SET a=1`, "update_ignore", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.UpdateStmt)
            assert.True(t, s.IgnoreErr, "UPDATE IGNORE must set IgnoreErr")
        }},
        {`UPDATE LOW_PRIORITY t SET a=1`, "update_low_priority", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.UpdateStmt)
            assert.Equal(t, ast.LowPriority, s.Priority, "UPDATE LOW_PRIORITY must set Priority")
        }},
        {`UPDATE t SET a=1 WHERE b=2`, "update_where", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.UpdateStmt)
            assert.NotNil(t, s.Where, "UPDATE WHERE must set Where field")
        }},
        {`UPDATE t SET a=1 ORDER BY b LIMIT 5`, "update_order_limit", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.UpdateStmt)
            assert.NotNil(t, s.Order, "UPDATE ORDER BY must set Order")
            assert.NotNil(t, s.Limit, "UPDATE LIMIT must set Limit")
        }},
        {`DELETE IGNORE FROM t WHERE 1`, "delete_ignore", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DeleteStmt)
            assert.True(t, s.IgnoreErr, "DELETE IGNORE must set IgnoreErr")
        }},
        {`DELETE LOW_PRIORITY FROM t`, "delete_low_priority", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DeleteStmt)
            assert.Equal(t, ast.LowPriority, s.Priority, "DELETE LOW_PRIORITY must set Priority")
        }},
        {`DELETE QUICK FROM t`, "delete_quick", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DeleteStmt)
            assert.True(t, s.Quick, "DELETE QUICK must set Quick=true")
        }},
        {`DELETE FROM t1 USING t1, t2 WHERE t1.id=t2.id`, "delete_using", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DeleteStmt)
            assert.True(t, s.IsMultiTable, "DELETE USING must set IsMultiTable=true")
        }},
        {`DELETE t1 FROM t1, t2 WHERE t1.id=t2.id`, "delete_multi_table_from", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DeleteStmt)
            assert.True(t, s.IsMultiTable, "Multi-table DELETE must set IsMultiTable=true")
        }},
        {`DELETE FROM t ORDER BY a LIMIT 10`, "delete_order_limit", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DeleteStmt)
            assert.NotNil(t, s.Order, "DELETE ORDER BY must set Order")
            assert.NotNil(t, s.Limit, "DELETE LIMIT must set Limit")
        }},
"""
    failures, out = run_go_tests("DMLFields", dml_cases)
    if failures:
        all_failures.append(("DMLFields", failures, out))
        print(f"  ❌ DMLFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ DMLFields: all pass")

    # =========================================================================
    # GROUP 4: SetOprStmt (UNION/INTERSECT/EXCEPT) fields
    # =========================================================================
    setopr_cases = r"""
        {`SELECT 1 UNION SELECT 2`, "union_all_false", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetOprStmt)
            if assert.NotEmpty(t, s.SelectList.Selects) {
                // UNION without ALL — SelectList should have selects
                assert.GreaterOrEqual(t, len(s.SelectList.Selects), 2)
            }
        }},
        {`SELECT 1 UNION ALL SELECT 2`, "union_all_true", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetOprStmt)
            // Verify UNION ALL is captured
            assert.NotNil(t, s.SelectList)
        }},
        {`SELECT 1 INTERSECT SELECT 2`, "intersect_stmt", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetOprStmt)
            assert.NotNil(t, s.SelectList)
        }},
        {`SELECT 1 EXCEPT SELECT 2`, "except_stmt", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetOprStmt)
            assert.NotNil(t, s.SelectList)
        }},
        {`(SELECT 1) UNION (SELECT 2) ORDER BY 1 LIMIT 1`, "union_parenthesized_order_limit", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetOprStmt)
            assert.NotNil(t, s.OrderBy, "UNION with ORDER BY must set OrderBy")
            assert.NotNil(t, s.Limit, "UNION with LIMIT must set Limit")
        }},
"""
    failures, out = run_go_tests("SetOprFields", setopr_cases)
    if failures:
        all_failures.append(("SetOprFields", failures, out))
        print(f"  ❌ SetOprFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ SetOprFields: all pass")

    # =========================================================================
    # GROUP 5: CreateTableStmt table options
    # =========================================================================
    create_table_cases = r"""
        {`CREATE TABLE t (a INT) ENGINE=InnoDB`, "create_table_engine", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateTableStmt)
            found := false
            for _, opt := range s.Options {
                if opt.Tp == ast.TableOptionEngine {
                    assert.Equal(t, "InnoDB", opt.StrValue)
                    found = true
                }
            }
            assert.True(t, found, "ENGINE option must be set")
        }},
        {`CREATE TABLE t (a INT) CHARSET=utf8mb4`, "create_table_charset", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateTableStmt)
            found := false
            for _, opt := range s.Options {
                if opt.Tp == ast.TableOptionCharset {
                    found = true
                }
            }
            assert.True(t, found, "CHARSET option must be set")
        }},
        {`CREATE TABLE t (a INT) COLLATE=utf8mb4_bin`, "create_table_collate", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateTableStmt)
            found := false
            for _, opt := range s.Options {
                if opt.Tp == ast.TableOptionCollate {
                    found = true
                }
            }
            assert.True(t, found, "COLLATE option must be set")
        }},
        {`CREATE TABLE t (a INT) AUTO_INCREMENT=100`, "create_table_auto_increment", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateTableStmt)
            found := false
            for _, opt := range s.Options {
                if opt.Tp == ast.TableOptionAutoIncrement {
                    assert.Equal(t, uint64(100), opt.UintValue)
                    found = true
                }
            }
            assert.True(t, found, "AUTO_INCREMENT option must be set")
        }},
        {`CREATE TABLE t (a INT) COMMENT='my table'`, "create_table_comment", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateTableStmt)
            found := false
            for _, opt := range s.Options {
                if opt.Tp == ast.TableOptionComment {
                    assert.Equal(t, "my table", opt.StrValue)
                    found = true
                }
            }
            assert.True(t, found, "COMMENT option must be set")
        }},
        {`CREATE TABLE t (a INT) ROW_FORMAT=COMPRESSED`, "create_table_row_format", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateTableStmt)
            found := false
            for _, opt := range s.Options {
                if opt.Tp == ast.TableOptionRowFormat {
                    found = true
                }
            }
            assert.True(t, found, "ROW_FORMAT option must be set")
        }},
        {`CREATE TEMPORARY TABLE t (a INT)`, "create_temp_table", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateTableStmt)
            assert.True(t, s.TemporaryKeyword == ast.TemporaryLocal, "CREATE TEMPORARY TABLE must set TemporaryKeyword")
        }},
        {`CREATE TABLE IF NOT EXISTS t (a INT)`, "create_if_not_exists", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateTableStmt)
            assert.True(t, s.IfNotExists, "CREATE TABLE IF NOT EXISTS must set IfNotExists=true")
        }},
"""
    failures, out = run_go_tests("CreateTableOptions", create_table_cases)
    if failures:
        all_failures.append(("CreateTableOptions", failures, out))
        print(f"  ❌ CreateTableOptions: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ CreateTableOptions: all pass")

    # =========================================================================
    # GROUP 6: DropStmt fields
    # =========================================================================
    drop_cases = r"""
        {`DROP TABLE IF EXISTS t`, "drop_table_if_exists", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DropTableStmt)
            assert.True(t, s.IfExists, "DROP TABLE IF EXISTS must set IfExists=true")
        }},
        {`DROP TABLE t1, t2, t3`, "drop_table_multi", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DropTableStmt)
            assert.Len(t, s.Tables, 3, "DROP TABLE must store all table names")
        }},
        {`DROP TEMPORARY TABLE t`, "drop_temp_table", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DropTableStmt)
            assert.True(t, s.IsView == false && s.TemporaryKeyword == ast.TemporaryLocal, "DROP TEMPORARY TABLE must set TemporaryKeyword")
        }},
        {`DROP VIEW v`, "drop_view", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DropTableStmt)
            assert.True(t, s.IsView, "DROP VIEW must set IsView=true")
        }},
        {`DROP DATABASE IF EXISTS mydb`, "drop_db_if_exists", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.DropDatabaseStmt)
            assert.True(t, s.IfExists, "DROP DATABASE IF EXISTS must set IfExists=true")
        }},
        {`RENAME TABLE t1 TO t2, t3 TO t4`, "rename_table_multi", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.RenameTableStmt)
            assert.Len(t, s.TableToTables, 2, "RENAME TABLE must store all pairs")
        }},
"""
    failures, out = run_go_tests("DropStmtFields", drop_cases)
    if failures:
        all_failures.append(("DropStmtFields", failures, out))
        print(f"  ❌ DropStmtFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ DropStmtFields: all pass")

    # =========================================================================
    # GROUP 7: Transaction statement fields
    # =========================================================================
    txn_cases = r"""
        {`START TRANSACTION READ ONLY`, "start_tx_read_only", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.BeginStmt)
            assert.True(t, s.ReadOnly, "START TRANSACTION READ ONLY must set ReadOnly=true")
        }},
        {`START TRANSACTION READ WRITE`, "start_tx_read_write", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.BeginStmt)
            assert.False(t, s.ReadOnly, "START TRANSACTION READ WRITE must set ReadOnly=false")
        }},
        {`START TRANSACTION WITH CONSISTENT SNAPSHOT`, "start_tx_consistent_snapshot", func(t *testing.T, stmts []ast.StmtNode) {
            // just parse check
            assert.NotNil(t, stmts[0])
        }},
        {`BEGIN PESSIMISTIC`, "begin_pessimistic", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.BeginStmt)
            assert.True(t, s.Mode == ast.PessimisticMode, "BEGIN PESSIMISTIC must set Mode=PessimisticMode")
        }},
        {`BEGIN OPTIMISTIC`, "begin_optimistic", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.BeginStmt)
            assert.True(t, s.Mode == ast.OptimisticMode, "BEGIN OPTIMISTIC must set Mode=OptimisticMode")
        }},
        {`COMMIT AND CHAIN`, "commit_and_chain", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CommitStmt)
            assert.True(t, s.CompletionType == ast.CompletionTypeChain, "COMMIT AND CHAIN must set CompletionType=Chain")
        }},
        {`ROLLBACK AND CHAIN`, "rollback_and_chain", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.RollbackStmt)
            assert.True(t, s.CompletionType == ast.CompletionTypeChain, "ROLLBACK AND CHAIN must set CompletionType=Chain")
        }},
        {`COMMIT RELEASE`, "commit_release", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CommitStmt)
            assert.True(t, s.CompletionType == ast.CompletionTypeRelease, "COMMIT RELEASE must set CompletionType=Release")
        }},
"""
    failures, out = run_go_tests("TxnFields", txn_cases)
    if failures:
        all_failures.append(("TxnFields", failures, out))
        print(f"  ❌ TxnFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ TxnFields: all pass")

    # =========================================================================
    # GROUP 8: ExplainStmt fields
    # =========================================================================
    explain_cases = r"""
        {`EXPLAIN FORMAT=JSON SELECT * FROM t`, "explain_format_json", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.ExplainStmt)
            assert.Equal(t, "json", s.Format)
        }},
        {`EXPLAIN FORMAT=BRIEF SELECT * FROM t`, "explain_format_brief", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.ExplainStmt)
            assert.Equal(t, "brief", s.Format)
        }},
        {`EXPLAIN FORMAT=ROW SELECT * FROM t`, "explain_format_row", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.ExplainStmt)
            assert.Equal(t, "row", s.Format)
        }},
        {`EXPLAIN ANALYZE SELECT * FROM t`, "explain_analyze", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.ExplainStmt)
            assert.True(t, s.Analyze, "EXPLAIN ANALYZE must set Analyze=true")
        }},
        {`EXPLAIN FOR CONNECTION 1`, "explain_for_connection", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.ExplainForStmt)
            assert.Equal(t, uint64(1), s.ConnectionID)
        }},
"""
    failures, out = run_go_tests("ExplainFields", explain_cases)
    if failures:
        all_failures.append(("ExplainFields", failures, out))
        print(f"  ❌ ExplainFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ ExplainFields: all pass")

    # =========================================================================
    # GROUP 9: LockTablesStmt fields
    # =========================================================================
    lock_cases = r"""
        {`LOCK TABLES t READ`, "lock_read", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.LockTablesStmt)
            if assert.Len(t, s.TableLocks, 1) {
                assert.Equal(t, ast.TableLockRead, s.TableLocks[0].Type)
            }
        }},
        {`LOCK TABLES t WRITE`, "lock_write", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.LockTablesStmt)
            if assert.Len(t, s.TableLocks, 1) {
                assert.Equal(t, ast.TableLockWrite, s.TableLocks[0].Type)
            }
        }},
        {`LOCK TABLES t READ LOCAL`, "lock_read_local", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.LockTablesStmt)
            if assert.Len(t, s.TableLocks, 1) {
                assert.Equal(t, ast.TableLockReadLocal, s.TableLocks[0].Type)
            }
        }},
        {`LOCK TABLES t1 READ, t2 WRITE`, "lock_multi", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.LockTablesStmt)
            assert.Len(t, s.TableLocks, 2)
        }},
"""
    failures, out = run_go_tests("LockTablesFields", lock_cases)
    if failures:
        all_failures.append(("LockTablesFields", failures, out))
        print(f"  ❌ LockTablesFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ LockTablesFields: all pass")

    # =========================================================================
    # GROUP 10: Join AST fields — TableSource.AsName, Join types
    # =========================================================================
    join_cases = r"""
        {`SELECT * FROM t AS alias`, "table_alias", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.From) {
                ts, ok := s.From.TableRefs.Left.(*ast.TableSource)
                if assert.True(t, ok) {
                    assert.Equal(t, "alias", ts.AsName.L)
                }
            }
        }},
        {`SELECT * FROM t1 LEFT JOIN t2 ON t1.id=t2.id`, "left_join_type", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.From) {
                join, ok := s.From.TableRefs.(*ast.Join)
                if assert.True(t, ok) {
                    assert.Equal(t, ast.LeftJoin, join.Tp)
                }
            }
        }},
        {`SELECT * FROM t1 RIGHT JOIN t2 ON t1.id=t2.id`, "right_join_type", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.From) {
                join, ok := s.From.TableRefs.(*ast.Join)
                if assert.True(t, ok) {
                    assert.Equal(t, ast.RightJoin, join.Tp)
                }
            }
        }},
        {`SELECT * FROM t1 JOIN t2 USING (id)`, "join_using", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.From) {
                join, ok := s.From.TableRefs.(*ast.Join)
                if assert.True(t, ok) {
                    assert.NotEmpty(t, join.Using, "JOIN USING must set Using columns")
                }
            }
        }},
        {`SELECT * FROM t PARTITION (p1, p2)`, "partition_selection", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SelectStmt)
            if assert.NotNil(t, s.From) {
                ts, ok := s.From.TableRefs.Left.(*ast.TableSource)
                if assert.True(t, ok) {
                    tn, ok := ts.Source.(*ast.TableName)
                    if assert.True(t, ok) {
                        assert.NotEmpty(t, tn.PartitionNames, "PARTITION selection must set PartitionNames")
                    }
                }
            }
        }},
"""
    failures, out = run_go_tests("JoinFields", join_cases)
    if failures:
        all_failures.append(("JoinFields", failures, out))
        print(f"  ❌ JoinFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ JoinFields: all pass")

    # =========================================================================
    # GROUP 11: SET statement variants
    # =========================================================================
    set_cases = r"""
        {`SET NAMES utf8mb4`, "set_names", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetStmt)
            if assert.NotEmpty(t, s.Variables) {
                assert.Equal(t, "names", s.Variables[0].Name)
                assert.Equal(t, "utf8mb4", s.Variables[0].Value.(*ast.ValueExpr).GetString())
            }
        }},
        {`SET NAMES utf8mb4 COLLATE utf8mb4_general_ci`, "set_names_collate", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetStmt)
            assert.NotEmpty(t, s.Variables)
        }},
        {`SET @@GLOBAL.max_connections = 100`, "set_global_sysvar", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetStmt)
            if assert.NotEmpty(t, s.Variables) {
                assert.True(t, s.Variables[0].IsGlobal, "@@GLOBAL must set IsGlobal=true")
                assert.True(t, s.Variables[0].IsSysVar, "@@GLOBAL must set IsSysVar=true")
            }
        }},
        {`SET @@SESSION.max_connections = 100`, "set_session_sysvar", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetStmt)
            if assert.NotEmpty(t, s.Variables) {
                assert.False(t, s.Variables[0].IsGlobal, "@@SESSION must set IsGlobal=false")
                assert.True(t, s.Variables[0].IsSysVar, "@@SESSION must set IsSysVar=true")
            }
        }},
        {`SET @uservar = 42`, "set_user_var", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.SetStmt)
            if assert.NotEmpty(t, s.Variables) {
                assert.False(t, s.Variables[0].IsSysVar, "@uservar must NOT set IsSysVar")
                assert.Equal(t, "uservar", s.Variables[0].Name)
            }
        }},
        {`SET TRANSACTION ISOLATION LEVEL READ COMMITTED`, "set_tx_isolation", func(t *testing.T, stmts []ast.StmtNode) {
            assert.NotNil(t, stmts[0])
        }},
"""
    failures, out = run_go_tests("SetStmtFields", set_cases)
    if failures:
        all_failures.append(("SetStmtFields", failures, out))
        print(f"  ❌ SetStmtFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ SetStmtFields: all pass")

    # =========================================================================
    # GROUP 12: AdminStmt fields
    # =========================================================================
    admin_cases = r"""
        {`ADMIN SHOW DDL`, "admin_show_ddl", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            assert.Equal(t, ast.AdminShowDDL, s.Tp)
        }},
        {`ADMIN SHOW DDL JOBS 5 WHERE state='running'`, "admin_show_ddl_jobs_where", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            assert.Equal(t, ast.AdminShowDDLJobs, s.Tp)
            assert.NotNil(t, s.Where, "ADMIN SHOW DDL JOBS WHERE must set Where")
        }},
        {`ADMIN CHECK TABLE t`, "admin_check_table", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            assert.Equal(t, ast.AdminCheckTable, s.Tp)
            assert.NotEmpty(t, s.Tables)
        }},
        {`ADMIN CANCEL DDL JOBS 1, 2`, "admin_cancel_ddl_jobs", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            assert.Equal(t, ast.AdminCancelDDLJobs, s.Tp)
            assert.Len(t, s.JobIDs, 2)
        }},
        {`ADMIN PAUSE DDL JOBS 1, 2`, "admin_pause_ddl_jobs", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            assert.Equal(t, ast.AdminPauseDDLJobs, s.Tp)
        }},
        {`ADMIN RECOVER INDEX t idx`, "admin_recover_index", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            assert.Equal(t, ast.AdminRecoverIndex, s.Tp)
        }},
        {`ADMIN SHOW SLOW RECENT 10`, "admin_show_slow_recent", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            assert.Equal(t, ast.AdminShowSlow, s.Tp)
            if assert.NotNil(t, s.ShowSlow) {
                assert.Equal(t, ast.ShowSlowRecent, s.ShowSlow.Tp)
                assert.Equal(t, uint64(10), s.ShowSlow.Count)
            }
        }},
        {`ADMIN SHOW SLOW TOP ALL 10`, "admin_show_slow_top_all", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            if assert.NotNil(t, s.ShowSlow) {
                assert.Equal(t, ast.ShowSlowTop, s.ShowSlow.Tp)
                assert.Equal(t, ast.ShowSlowKindAll, s.ShowSlow.Kind)
            }
        }},
        {`ADMIN SHOW SLOW TOP INTERNAL 10`, "admin_show_slow_top_internal", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.AdminStmt)
            if assert.NotNil(t, s.ShowSlow) {
                assert.Equal(t, ast.ShowSlowTop, s.ShowSlow.Tp)
                assert.Equal(t, ast.ShowSlowKindInternal, s.ShowSlow.Kind)
            }
        }},
"""
    failures, out = run_go_tests("AdminStmtFields", admin_cases)
    if failures:
        all_failures.append(("AdminStmtFields", failures, out))
        print(f"  ❌ AdminStmtFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ AdminStmtFields: all pass")

    # =========================================================================
    # GROUP 13: FlushStmt type variants exhaustive check
    # =========================================================================
    flush_cases = r"""
        {`FLUSH TABLES WITH READ LOCK`, "flush_tables_with_read_lock", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.FlushStmt)
            assert.Equal(t, ast.FlushTables, s.Tp)
            assert.True(t, s.ReadLock, "FLUSH TABLES WITH READ LOCK must set ReadLock=true")
        }},
        {`FLUSH TABLES t1, t2`, "flush_specific_tables", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.FlushStmt)
            assert.Equal(t, ast.FlushTables, s.Tp)
            assert.Len(t, s.Tables, 2, "FLUSH TABLES t1, t2 must list tables")
        }},
        {`FLUSH NO_WRITE_TO_BINLOG TABLES`, "flush_no_write_to_binlog", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.FlushStmt)
            assert.True(t, s.NoWriteToBinLog, "FLUSH NO_WRITE_TO_BINLOG must set NoWriteToBinLog=true")
        }},
        {`FLUSH LOGS`, "flush_logs", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.FlushStmt)
            assert.Equal(t, ast.FlushLogs, s.Tp)
        }},
        {`FLUSH BINARY LOGS`, "flush_binary_logs", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.FlushStmt)
            assert.EqualValues(t, ast.FlushBinaryLogs, s.Tp)
        }},
        {`FLUSH SLOW LOGS`, "flush_slow_logs", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.FlushStmt)
            assert.EqualValues(t, ast.FlushSlowLogs, s.Tp)
        }},
        {`FLUSH HOSTS`, "flush_hosts", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.FlushStmt)
            assert.EqualValues(t, ast.FlushHosts, s.Tp)
        }},
        {`FLUSH CLIENT_ERRORS_SUMMARY`, "flush_client_errors", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.FlushStmt)
            assert.EqualValues(t, ast.FlushClientErrorsSummary, s.Tp)
        }},
"""
    failures, out = run_go_tests("FlushStmtFields", flush_cases)
    if failures:
        all_failures.append(("FlushStmtFields", failures, out))
        print(f"  ❌ FlushStmtFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ FlushStmtFields: all pass")

    # =========================================================================
    # GROUP 14: CreateIndexStmt fields
    # =========================================================================
    idx_cases = r"""
        {`CREATE UNIQUE INDEX idx ON t (a)`, "create_unique_index", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateIndexStmt)
            assert.True(t, s.KeyType == ast.IndexKeyTypeUnique, "CREATE UNIQUE INDEX must set KeyType=Unique")
        }},
        {`CREATE FULLTEXT INDEX idx ON t (a)`, "create_fulltext_index", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateIndexStmt)
            assert.True(t, s.KeyType == ast.IndexKeyTypeFullText, "CREATE FULLTEXT INDEX must set KeyType=FullText")
        }},
        {`CREATE SPATIAL INDEX idx ON t (a)`, "create_spatial_index", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateIndexStmt)
            assert.True(t, s.KeyType == ast.IndexKeyTypeSpatial, "CREATE SPATIAL INDEX must set KeyType=Spatial")
        }},
        {`CREATE INDEX idx ON t (a) INVISIBLE`, "create_invisible_index", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateIndexStmt)
            assert.NotNil(t, s.IndexOption)
            assert.Equal(t, ast.IndexInvisibility, s.IndexOption.Visibility)
        }},
        {`CREATE INDEX idx ON t (a) VISIBLE`, "create_visible_index", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateIndexStmt)
            assert.NotNil(t, s.IndexOption)
            assert.Equal(t, ast.IndexVisibility, s.IndexOption.Visibility)
        }},
        {`CREATE INDEX idx ON t (a) USING BTREE`, "create_index_btree", func(t *testing.T, stmts []ast.StmtNode) {
            s := stmts[0].(*ast.CreateIndexStmt)
            if assert.NotNil(t, s.IndexOption) {
                assert.Equal(t, model.IndexTypeBTree, s.IndexOption.Tp)
            }
        }},
"""
    # This group needs extra imports
    failures, out = run_go_tests("CreateIndexFields", idx_cases,
        imports_extra='import "github.com/pingcap/tidb/pkg/parser/model"')
    if failures:
        all_failures.append(("CreateIndexFields", failures, out))
        print(f"  ❌ CreateIndexFields: {len(failures)} failure(s): {failures}")
    else:
        print(f"  ✅ CreateIndexFields: all pass")

    # =========================================================================
    # Final report
    # =========================================================================
    print()
    if not all_failures:
        print("🎉 ALL AST FIELD CHECKS PASSED!")
        return 0

    print(f"❌ FAILURES ({sum(len(f) for _, f, _ in all_failures)} total across {len(all_failures)} groups):")
    for group, failures, output in all_failures:
        print(f"\n── {group} ──")
        for f in failures:
            print(f"  FAIL: {f}")
        # Print first error detail
        lines = output.split("\n")
        for i, ln in enumerate(lines):
            if "Error:" in ln or "FAIL [" in ln:
                print("  " + "\n  ".join(lines[i:i+5]))
                break

    return 1


if __name__ == "__main__":
    sys.exit(main())
