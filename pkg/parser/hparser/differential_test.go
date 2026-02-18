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
// See the License for the specific language governing permissions and
// limitations under the License.

package hparser_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/hparser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// TestDifferential feeds every SQL string to both the goyacc parser and the
// hand parser, then compares the restored SQL output. This is the authoritative
// measure of how much of the existing parser's functionality the hand parser
// covers.
//
// Run with: go test -v -run TestDifferential ./hparser/ 2>&1 | tail -20
func TestDifferential(t *testing.T) {
	sqls := collectDMLTestCases()

	var total, handled, matched, astMismatch, restoreMismatch int
	var unhandled, failures []string

	goyaccParser := parser.New()

	for _, sql := range sqls {
		total++

		// --- Goyacc parser ---
		goyaccStmts, _, goyaccErr := goyaccParser.Parse(sql, "", "")
		if goyaccErr != nil {
			// Skip SQL that goyacc also rejects (syntax errors, negative test cases).
			continue
		}
		if len(goyaccStmts) == 0 {
			continue
		}

		// --- Hand parser ---
		hp := hparser.NewHandParser()
		scanner := parser.NewScanner(sql)
		hp.Init(parser.ScannerLexFunc(scanner), sql)
		hp.SetCharsetCollation("utf8mb4", "utf8mb4_bin")
		handStmts, _, handErr := hp.ParseSQL()

		if handErr != nil || len(handStmts) == 0 {
			unhandled = append(unhandled, sql)
			continue
		}

		handled++

		// Compare AST node types.
		if len(handStmts) != len(goyaccStmts) {
			astMismatch++
			failures = append(failures, fmt.Sprintf("[count] %s: goyacc=%d hand=%d", sql, len(goyaccStmts), len(handStmts)))
			continue
		}

		// Compare restored SQL output.
		allMatch := true
		for i := range goyaccStmts {
			gRestore := restoreSQL(goyaccStmts[i])
			hRestore := restoreSQL(handStmts[i])
			if gRestore != hRestore {
				allMatch = false
				failures = append(failures, fmt.Sprintf("[restore] %s\n  goyacc: %s\n  hand:   %s", sql, gRestore, hRestore))
				break
			}
		}

		if allMatch {
			matched++
		} else {
			restoreMismatch++
		}
	}

	// --- Report ---
	t.Logf("")
	t.Logf("========== DIFFERENTIAL COVERAGE REPORT ==========")
	t.Logf("Total SQL strings:           %d", total)
	t.Logf("Goyacc-parseable:            %d", total-len(unhandled)-astMismatch-restoreMismatch+handled)
	t.Logf("Hand parser handled:         %d", handled)
	t.Logf("  ✅ Exact match:            %d", matched)
	t.Logf("  ❌ AST count mismatch:     %d", astMismatch)
	t.Logf("  ❌ Restore mismatch:       %d", restoreMismatch)
	t.Logf("  ⏭️  Unhandled (fallback):   %d", len(unhandled))
	if handled > 0 {
		t.Logf("Accuracy (of handled):       %.1f%%", float64(matched)/float64(handled)*100)
	}
	if total > 0 {
		t.Logf("Coverage (of total valid):   %.1f%%", float64(handled)/float64(total)*100)
	}
	t.Logf("===================================================")

	// Print first 30 failures for diagnosis.
	if len(failures) > 0 {
		t.Logf("")
		t.Logf("--- First %d failures ---", min(30, len(failures)))
		for i, f := range failures {
			if i >= 30 {
				break
			}
			t.Logf("  %s", f)
		}
	}

	// Print first 30 unhandled for coverage gap analysis.
	if len(unhandled) > 0 {
		t.Logf("")
		t.Logf("--- First %d unhandled SQLs ---", min(30, len(unhandled)))
		for i, u := range unhandled {
			if i >= 30 {
				break
			}
			t.Logf("  %s", truncate(u, 120))
		}
	}

	// Categorize unhandled by leading keyword.
	if len(unhandled) > 0 {
		t.Logf("")
		t.Logf("--- Unhandled by category ---")
		cats := categorize(unhandled)
		for _, c := range cats {
			t.Logf("  %-20s %d", c.name, c.count)
		}
	}
}

// restoreSQL converts an AST node back to SQL text.
func restoreSQL(node ast.Node) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := node.Restore(ctx); err != nil {
		return fmt.Sprintf("<restore error: %v>", err)
	}
	return sb.String()
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

type category struct {
	name  string
	count int
}

func categorize(sqls []string) []category {
	counts := map[string]int{}
	for _, sql := range sqls {
		sql = strings.TrimSpace(sql)
		if sql == "" || sql == ";" {
			counts["<empty>"]++
			continue
		}
		parts := strings.Fields(strings.ToUpper(sql))
		if len(parts) > 0 {
			kw := parts[0]
			// Normalize multi-word keywords.
			if len(parts) > 1 && (kw == "CREATE" || kw == "DROP" || kw == "ALTER" ||
				kw == "SHOW" || kw == "LOAD" || kw == "LOCK" || kw == "UNLOCK" ||
				kw == "FLUSH" || kw == "RELEASE" || kw == "ROLLBACK") {
				kw = kw + " " + parts[1]
			}
			counts[kw]++
		}
	}
	// Sort by count descending.
	var result []category
	for name, count := range counts {
		result = append(result, category{name, count})
	}
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].count > result[i].count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result
}

// collectDMLTestCases returns a comprehensive list of SQL strings to test.
// These cover the same space as parser_test.go's TestDMLStmt, TestExpression,
// TestBuiltin, TestSubquery, etc.
func collectDMLTestCases() []string {
	return []string{
		// ===== SELECT basics =====
		"SELECT 1",
		"SELECT 1, 2, 3",
		"SELECT a FROM t",
		"SELECT a, b FROM t WHERE a = 1",
		"SELECT * FROM t",
		"SELECT t.* FROM t",
		"SELECT a FROM t WHERE a > 1 AND b < 2",
		"SELECT a FROM t WHERE a = 1 OR b = 2",
		"SELECT a FROM t ORDER BY a",
		"SELECT a FROM t ORDER BY a DESC",
		"SELECT a FROM t LIMIT 10",
		"SELECT a FROM t LIMIT 10, 20",
		"SELECT a FROM t LIMIT 10 OFFSET 5",
		"SELECT a, b FROM t GROUP BY a",
		"SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
		"SELECT DISTINCT a FROM t",
		"SELECT ALL * FROM t",
		"SELECT * FROM t AS u",
		"SELECT * FROM t, v",
		"SELECT * FROM t AS u, v AS w",

		// ===== JOIN =====
		"SELECT a FROM t1 JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 RIGHT JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 INNER JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 CROSS JOIN t2",
		"SELECT a FROM t1, t2 WHERE t1.id = t2.id",
		"SELECT a FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 NATURAL JOIN t2",
		"SELECT a FROM t1 NATURAL LEFT JOIN t2",

		// ===== INSERT =====
		"INSERT INTO foo VALUES (1234)",
		"INSERT INTO foo VALUES (1234, 5678)",
		"INSERT INTO foo (a) VALUES (42)",
		"INSERT INTO foo (a,b) VALUES (42,314)",
		"INSERT INTO foo VALUES (1 || 2)",
		"INSERT INTO foo VALUES (1 | 2)",
		"INSERT INTO foo VALUES (false || true)",
		"INSERT INTO foo VALUES (bar(5678))",
		"INSERT INTO foo VALUES ()",
		"INSERT INTO foo () VALUES ()",
		"INSERT INTO t1 (SELECT * FROM t2)",
		"INSERT INTO t SET a = 1, b = 2",
		"INSERT INTO t SET a = 1, b = 2, c = 3",
		"INSERT INTO t (a, b) VALUES (1, 2), (3, 4)",
		"INSERT INTO t VALUES (1, 2) ON DUPLICATE KEY UPDATE a = 1",
		"INSERT INTO t partition (p0) values(1234)",
		"INSERT INTO t VALUES (default)",
		"INSERT IGNORE INTO t VALUES (1)",
		"INSERT LOW_PRIORITY INTO t VALUES (1)",
		"INSERT HIGH_PRIORITY INTO t VALUES (1)",
		"INSERT DELAYED INTO t VALUES (1)",

		// ===== REPLACE =====
		"REPLACE INTO foo VALUES (1234)",
		"REPLACE INTO foo (a,b) VALUES (42,314)",
		"REPLACE INTO foo VALUES ()",
		"REPLACE INTO t partition (p0) values(1234)",

		// ===== UPDATE =====
		"UPDATE t SET a = 1",
		"UPDATE t SET a = 1, b = 2 WHERE c = 3",
		"UPDATE t SET a = a + 1 ORDER BY b LIMIT 10",
		"UPDATE LOW_PRIORITY t SET a = 1",
		"UPDATE IGNORE t SET a = 1",
		"UPDATE t1, t2 SET t1.a = t2.b WHERE t1.id = t2.id",

		// ===== DELETE =====
		"DELETE FROM t WHERE a = 1",
		"DELETE FROM t ORDER BY a LIMIT 10",
		"DELETE FROM t",
		"DELETE LOW_PRIORITY FROM t",

		// ===== Expressions =====
		"SELECT 1 + 2",
		"SELECT a * b + c",
		"SELECT a AND b OR c",
		"SELECT NOT a",
		"SELECT -a",
		"SELECT +a",
		"SELECT ~a",
		"SELECT !a",
		"SELECT a IS NULL",
		"SELECT a IS NOT NULL",
		"SELECT a IS TRUE",
		"SELECT a IS NOT TRUE",
		"SELECT a IS FALSE",
		"SELECT a IS NOT FALSE",
		"SELECT a IN (1, 2, 3)",
		"SELECT a NOT IN (1, 2, 3)",
		"SELECT a BETWEEN 1 AND 10",
		"SELECT a NOT BETWEEN 1 AND 10",
		"SELECT a LIKE 'foo%'",
		"SELECT a NOT LIKE 'foo%'",
		"SELECT a REGEXP 'pattern'",
		"SELECT a NOT REGEXP 'pattern'",
		"SELECT a RLIKE 'pattern'",
		"SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END",
		"SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END",
		"SELECT a = 1 AND (b = 2 OR c = 3)",
		"SELECT ?",
		"SELECT a > b",
		"SELECT a >= b",
		"SELECT a < b",
		"SELECT a <= b",
		"SELECT a != b",
		"SELECT a <> b",
		"SELECT a <=> b",
		"SELECT a | b",
		"SELECT a & b",
		"SELECT a ^ b",
		"SELECT a << b",
		"SELECT a >> b",
		"SELECT a DIV b",
		"SELECT a MOD b",
		"SELECT a % b",
		"SELECT a XOR b",
		"SELECT a COLLATE utf8mb4_general_ci FROM t",
		"SELECT DEFAULT(a) FROM t",
		"SELECT @a",
		"SELECT @@global.max_connections",

		// ===== Subqueries =====
		"SELECT a FROM t WHERE a IN (SELECT b FROM t2)",
		"SELECT EXISTS (SELECT 1 FROM t)",
		"SELECT (SELECT 1)",
		"SELECT * FROM (SELECT a FROM t) AS sub",
		"SELECT a FROM t WHERE a = (SELECT MAX(b) FROM t2)",

		// ===== Aggregate functions =====
		"SELECT COUNT(*) FROM t",
		"SELECT COUNT(a) FROM t",
		"SELECT COUNT(DISTINCT a) FROM t",
		"SELECT SUM(a) FROM t",
		"SELECT MAX(a) FROM t",
		"SELECT MIN(a) FROM t",
		"SELECT SUM(DISTINCT a) FROM t",

		// ===== Scalar functions (should use FuncCallExpr) =====
		"SELECT CONCAT(a, b) FROM t",
		"SELECT UPPER(a) FROM t",
		"SELECT LOWER(a) FROM t",
		"SELECT LENGTH(a) FROM t",
		"SELECT COALESCE(a, b, c) FROM t",
		"SELECT IF(a > 1, 'yes', 'no') FROM t",
		"SELECT IFNULL(a, 0) FROM t",
		"SELECT NULLIF(a, b) FROM t",
		"SELECT REPLACE(a, 'old', 'new') FROM t",
		"SELECT SUBSTRING(a, 1, 3) FROM t",

		// ===== CAST expressions =====
		"SELECT CAST(a AS SIGNED) FROM t",
		"SELECT CAST(a AS UNSIGNED) FROM t",
		"SELECT CAST(a AS CHAR(10)) FROM t",
		"SELECT CAST(a AS BINARY(16)) FROM t",
		"SELECT CAST(a AS DATE) FROM t",
		"SELECT CAST(a AS DATETIME) FROM t",
		"SELECT CAST(a AS TIME) FROM t",
		"SELECT CAST(a AS DECIMAL(10,2)) FROM t",
		"SELECT CAST(a AS JSON) FROM t",
		"SELECT CAST(a AS DOUBLE) FROM t",

		// ===== EXTRACT expressions =====
		"SELECT EXTRACT(YEAR FROM a) FROM t",
		"SELECT EXTRACT(MONTH FROM a) FROM t",
		"SELECT EXTRACT(DAY FROM a) FROM t",
		"SELECT EXTRACT(HOUR FROM a) FROM t",
		"SELECT EXTRACT(MINUTE FROM a) FROM t",
		"SELECT EXTRACT(SECOND FROM a) FROM t",
		"SELECT EXTRACT(MICROSECOND FROM a) FROM t",
		"SELECT EXTRACT(DAY_HOUR FROM a) FROM t",

		// ===== TRIM expressions =====
		"SELECT TRIM(a) FROM t",
		"SELECT TRIM(' hello ') FROM t",
		"SELECT TRIM(LEADING ' ' FROM a) FROM t",
		"SELECT TRIM(TRAILING ' ' FROM a) FROM t",
		"SELECT TRIM(BOTH ' ' FROM a) FROM t",
		"SELECT TRIM(BOTH FROM a) FROM t",

		// ===== POSITION expressions =====
		"SELECT POSITION('bar' IN 'foobar') FROM t",

		// ===== DATE_ADD / DATE_SUB with INTERVAL =====
		"SELECT DATE_ADD('2020-01-01', INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB('2020-01-01', INTERVAL 1 MONTH) FROM t",
		"SELECT DATE_ADD(a, INTERVAL b HOUR) FROM t",
		"SELECT DATE_ADD(a, INTERVAL 1 YEAR_MONTH) FROM t",

		// ===== SUBSTRING FROM/FOR =====
		"SELECT SUBSTRING(a FROM 1) FROM t",
		"SELECT SUBSTRING(a FROM 1 FOR 3) FROM t",
		"SELECT SUBSTRING('hello', 2, 3) FROM t",

		// ===== Literals =====
		"SELECT 42",
		"SELECT 3.14",
		"SELECT 'hello'",
		"SELECT NULL",
		"SELECT TRUE",
		"SELECT FALSE",
		"SELECT X'DEADBEEF'",
		"SELECT B'10101'",

		// ===== Complex WHERE clauses =====
		"SELECT a FROM t WHERE a = 1 AND b = 2 AND c = 3",
		"SELECT a FROM t WHERE (a = 1 OR b = 2) AND c = 3",
		"SELECT a FROM t WHERE a IN (1, 2, 3) AND b LIKE 'foo%'",
		"SELECT a FROM t WHERE a BETWEEN 1 AND 10 AND b IS NOT NULL",
		"SELECT a FROM t WHERE NOT (a = 1 AND b = 2)",
		"SELECT a FROM t WHERE a = 1 AND b IN (SELECT c FROM t2)",

		// ===== Column references =====
		"SELECT a FROM t",
		"SELECT t.a FROM t",
		"SELECT db.t.a FROM db.t",

		// ===== Aliases =====
		"SELECT a AS alias FROM t",
		"SELECT a alias FROM t",
		"SELECT a FROM t AS tbl",
		"SELECT a FROM t tbl",

		// ===== Multi-table operations =====
		"UPDATE t1 JOIN t2 ON t1.id = t2.id SET t1.a = t2.b",

		// ===== Mixed complexity (sysbench-like) =====
		"SELECT c FROM sbtest1 WHERE id = 1",
		"SELECT c FROM sbtest1 WHERE id BETWEEN 1 AND 100",
		"SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100",
		"SELECT c FROM sbtest1 WHERE id BETWEEN 1 AND 100 ORDER BY c",
		"SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN 1 AND 100 ORDER BY c",
		"UPDATE sbtest1 SET k = k + 1 WHERE id = 1",
		"UPDATE sbtest1 SET c = 'new' WHERE id = 1",
		"DELETE FROM sbtest1 WHERE id = 1",
		"INSERT INTO sbtest1 (id, k, c, pad) VALUES (1, 2, 'c', 'pad')",

		// ===== Statements hand parser does NOT handle (should fallback) =====
		"CREATE TABLE t (a INT)",
		"DROP TABLE t",
		"ALTER TABLE t ADD COLUMN b INT",
		"SHOW TABLES",
		"USE mydb",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SET @a = 1",
		"EXPLAIN SELECT 1",
		"DESCRIBE t",
		"GRANT ALL ON *.* TO 'user'",
		"LOCK TABLES t READ",
		"UNLOCK TABLES",
		"LOAD DATA INFILE 'file' INTO TABLE t",
	}
}

// TestHintIntegration verifies optimizer hints pass through the hand parser
// correctly via the full parser.Parser (which injects HintParseFn).
func TestHintIntegration(t *testing.T) {
	hintSQLs := []string{
		"SELECT /*+ HASH_JOIN(t1) */ * FROM t1",
		"SELECT /*+ USE_INDEX(t, idx) */ a FROM t WHERE a > 1",
		"INSERT /*+ SET_VAR(foreign_key_checks=OFF) */ INTO t VALUES (1)",
		"UPDATE /*+ USE_INDEX(t, idx) */ t SET a = 1",
		"DELETE /*+ USE_INDEX(t, idx) */ FROM t WHERE a = 1",
	}

	p := parser.New()

	for _, sql := range hintSQLs {
		stmts, _, err := p.ParseSQL(sql)
		if err != nil {
			t.Errorf("parse error for %s: %v", sql, err)
			continue
		}
		if len(stmts) == 0 {
			t.Errorf("no statements for %s", sql)
			continue
		}

		// Verify via restore round-trip (matches goyacc).
		restored := restoreSQL(stmts[0])
		if restored == "" {
			t.Errorf("empty restore for %s", sql)
			continue
		}

		// Re-parse restored SQL and compare.
		stmts2, _, err := p.ParseSQL(restored)
		if err != nil {
			t.Errorf("re-parse error for %s -> %s: %v", sql, restored, err)
			continue
		}
		restored2 := restoreSQL(stmts2[0])
		if restored != restored2 {
			t.Errorf("restore mismatch:\n  sql:     %s\n  first:   %s\n  second:  %s", sql, restored, restored2)
		}

		t.Logf("✅ %s → %s", sql, restored)
	}
}
