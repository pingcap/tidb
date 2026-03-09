package parser

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func genINListSQL(n int) string {
	// SELECT ... WHERE id IN (1,2,3...)
	var sb strings.Builder
	sb.Grow(32 + n*6)
	sb.WriteString("SELECT * FROM t WHERE id IN (")
	for i := 1; i <= n; i++ {
		if i > 1 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.Itoa(i))
	}
	sb.WriteByte(')')
	return sb.String()
}

func genInsertValuesSQL(rows int) string {
	// INSERT INTO t(a,b,c) VALUES (1,2,3),(4,5,6)...
	var sb strings.Builder
	sb.Grow(64 + rows*24)
	sb.WriteString("INSERT INTO t(a,b,c) VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		// Keep numbers small to avoid generating very long tokens.
		a := i + 1
		b := (i % 100) + 1
		c := (i % 10) + 1

		sb.WriteByte('(')
		sb.WriteString(strconv.Itoa(a))
		sb.WriteByte(',')
		sb.WriteString(strconv.Itoa(b))
		sb.WriteByte(',')
		sb.WriteString(strconv.Itoa(c))
		sb.WriteByte(')')
	}
	return sb.String()
}

func genUnionChainSQL(n int) string {
	// SELECT 1 UNION ALL SELECT 2 ...
	var sb strings.Builder
	sb.Grow(32 + n*24)
	sb.WriteString("SELECT 1")
	for i := 2; i <= n; i++ {
		sb.WriteString(" UNION ALL SELECT ")
		sb.WriteString(strconv.Itoa(i))
	}
	return sb.String()
}

func genCreateTableSQL(cols int) string {
	// CREATE TABLE with many columns + a PK + a secondary index.
	// The goal is to stress the DDL parsing path and look for inflection points.
	var sb strings.Builder
	sb.Grow(256 + cols*32)
	sb.WriteString("CREATE TABLE t (")
	for i := 0; i < cols; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		// Mix a few types; keep it simple and always valid.
		if i == 0 {
			sb.WriteString("c0 BIGINT NOT NULL")
			continue
		}

		sb.WriteByte('c')
		sb.WriteString(strconv.Itoa(i))
		if i%10 == 0 {
			sb.WriteString(" VARCHAR(32)")
		} else {
			sb.WriteString(" INT")
		}
	}
	// Add constraints/indexes.
	sb.WriteString(", PRIMARY KEY (c0)")
	if cols > 1 {
		sb.WriteString(", KEY idx_c1 (c1)")
	}
	sb.WriteString(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	return sb.String()
}

func genLongSQL(targetBytes int) string {
	// Keep the SQL structure constant and vary total length by a long comment.
	// This stresses lexer/reader behavior on large inputs without exploding AST size.
	base := "SELECT 1 FROM t WHERE id = 1"
	// " /*" + "*/" is 4 chars plus the space we add.
	overhead := len(base) + len(" /*") + len("*/")
	pad := targetBytes - overhead
	if pad < 0 {
		pad = 0
	}
	return base + " /*" + strings.Repeat("a", pad) + "*/"
}

func BenchmarkPerf66318GrowthINList(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}
	for _, n := range sizes {
		sql := genINListSQL(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(sql)))
			benchParseSQL(b, sql)
		})
	}
}

func BenchmarkPerf66318GrowthInsertValues(b *testing.B) {
	sizes := []int{1, 10, 100, 1000}
	for _, n := range sizes {
		sql := genInsertValuesSQL(n)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(sql)))
			benchParseSQL(b, sql)
		})
	}
}

func BenchmarkPerf66318GrowthUnionChain(b *testing.B) {
	sizes := []int{2, 10, 100, 500}
	for _, n := range sizes {
		sql := genUnionChainSQL(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(sql)))
			benchParseSQL(b, sql)
		})
	}
}

func BenchmarkPerf66318GrowthCreateTable(b *testing.B) {
	sizes := []int{10, 100, 500, 1000}
	for _, n := range sizes {
		sql := genCreateTableSQL(n)
		b.Run(fmt.Sprintf("cols=%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(sql)))
			benchParseSQL(b, sql)
		})
	}
}

func BenchmarkPerf66318GrowthSQLLength(b *testing.B) {
	sizes := []int{1 << 10, 10 << 10, 100 << 10, 1 << 20}
	for _, n := range sizes {
		sql := genLongSQL(n)
		b.Run(fmt.Sprintf("bytes=%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(sql)))
			benchParseSQL(b, sql)
		})
	}
}
