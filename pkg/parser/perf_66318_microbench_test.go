package parser

import (
	"strings"
	"testing"
)

func benchParseSQL(b *testing.B, sql string) {
	p := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := p.ParseSQL(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchParseSQLExpectError(b *testing.B, sql string) {
	p := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := p.ParseSQL(sql)
		if err == nil {
			b.Fatalf("expected error, got nil (sql=%q)", sql)
		}
	}
}

func BenchmarkPerf66318MicroHotPathSysbench(b *testing.B) {
	// Sysbench-like hot path queries (text protocol and PS-like placeholders).
	cases := []struct {
		name string
		sql  string
	}{
		{"point_select_const", "SELECT c FROM sbtest1 WHERE id=1"},
		{"point_select_param", "SELECT c FROM sbtest1 WHERE id=?"},
		{"point_select_for_update", "SELECT c FROM sbtest1 WHERE id=? FOR UPDATE"},
		{"update_index_param", "UPDATE sbtest1 SET k=k+1 WHERE id=?"},
		{"update_non_index_param", "UPDATE sbtest1 SET c=? WHERE id=?"},
		{"delete_param", "DELETE FROM sbtest1 WHERE id=?"},
		{"insert_param", "INSERT INTO sbtest1(id,k,c,pad) VALUES(?,?,?,?)"},
		{"replace_param", "REPLACE INTO sbtest1(id,k,c,pad) VALUES(?,?,?,?)"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchParseSQL(b, tc.sql)
		})
	}
}

func BenchmarkPerf66318MicroHotPathCommon(b *testing.B) {
	// Common session init / metadata queries seen in real connections.
	cases := []struct {
		name string
		sql  string
	}{
		{"set_autocommit", "SET autocommit = 1"},
		{"set_names", "SET NAMES utf8mb4"},
		{"set_charset_results_null", "SET character_set_results = NULL"},
		{"set_tidb_sysvar", "SET @@tidb_enable_window_function = 1"},
		{"show_variables_tidb", "SHOW VARIABLES LIKE 'tidb%'"},
		{"show_status_threads", "SHOW STATUS LIKE 'Threads_running'"},
		{"select_version", "SELECT VERSION()"},
		{"use_db", "USE test"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchParseSQL(b, tc.sql)
		})
	}
}

func BenchmarkPerf66318MicroMediumComplexity(b *testing.B) {
	cases := []struct {
		name string
		sql  string
	}{
		{"join_order_limit", strings.TrimSpace(`
SELECT t1.a, t2.b
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
WHERE t1.a > 10 AND t2.b IS NOT NULL
ORDER BY t2.b DESC
LIMIT 100`)},
		{"subquery_in", strings.TrimSpace(`
SELECT *
FROM t
WHERE id IN (SELECT id FROM t2 WHERE v > 10)`)},
		{"exists", strings.TrimSpace(`
SELECT *
FROM t
WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)`)},
		{"derived_alias_keyword", "SELECT * FROM (SELECT 1 AS c) AS `rank`"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchParseSQL(b, tc.sql)
		})
	}
}

func BenchmarkPerf66318MicroHighComplexity(b *testing.B) {
	cases := []struct {
		name string
		sql  string
	}{
		{"cte", strings.TrimSpace(`
WITH cte AS (
  SELECT id, v FROM t WHERE v > 10
)
SELECT * FROM cte WHERE id < 100`)},
		{"window", strings.TrimSpace(`
SELECT id,
       SUM(v) OVER (PARTITION BY id % 10 ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
FROM t`)},
		{"hint", strings.TrimSpace(`
SELECT /*+ HASH_JOIN(t1, t2) */ t1.a, t2.b
FROM t1 JOIN t2 ON t1.id = t2.id
WHERE t1.a > 10`)},
		{"partition_ddl", strings.TrimSpace(`
CREATE TABLE p (
  id INT,
  v  INT
)
PARTITION BY RANGE (id) (
  PARTITION p0 VALUES LESS THAN (10),
  PARTITION p1 VALUES LESS THAN (100),
  PARTITION pmax VALUES LESS THAN (MAXVALUE)
)`)},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchParseSQL(b, tc.sql)
		})
	}
}

func BenchmarkPerf66318MicroMultibyteText(b *testing.B) {
	cases := []struct {
		name string
		sql  string
	}{
		{"utf8_zh", "SELECT '你好，世界'"},
		{"utf8mb4_emoji", "SELECT _utf8mb4 '😀'"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchParseSQL(b, tc.sql)
		})
	}
}

func BenchmarkPerf66318MicroErrorPaths(b *testing.B) {
	// Unterminated string literal triggers long near-text handling.
	longInvalid := "SELECT '" + strings.Repeat("a", 5000) + " FROM t"
	cases := []struct {
		name string
		sql  string
	}{
		{"begin", "SELCT 1"},
		{"middle", "SELECT 1 FRM t"},
		{"near_eof", "SELECT 1 FROM t WHERE 1 ="},
		{"long_invalid", longInvalid},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchParseSQLExpectError(b, tc.sql)
		})
	}
}

