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
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/hparser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// Benchmark SQL strings used by bench_test.go in the parent package.
var benchSQLs = map[string]string{
	"SimpleSelect":  "SELECT a FROM t WHERE a = 1",
	"SimpleInsert":  "INSERT INTO t (a, b, c) VALUES (1, 2, 3)",
	"SimpleUpdate":  "UPDATE t SET a = 1 WHERE b = 2",
	"SimpleDelete":  "DELETE FROM t WHERE a = 1",
	"SelectComplex": "SELECT a, b, c FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.a > 1 AND t2.b < 10 ORDER BY t1.a LIMIT 100",
	"SelectWhere3":  "SELECT a FROM t WHERE a = 1 AND b = 2 AND c = 3",
	"PointGet":      "SELECT a FROM t WHERE id = 1",
}

// BenchmarkHandParser benchmarks the hand-written parser.
func BenchmarkHandParser(b *testing.B) {
	for name, sql := range benchSQLs {
		b.Run(name, func(b *testing.B) {
			hp := hparser.NewHandParser()
			// Pre-create Scanner + LexFunc to match Goyacc's reuse pattern.
			scanner := parser.NewScanner(sql)
			lexFunc := parser.ScannerLexFunc(scanner)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				hp.Reset()
				scanner.ResetTo(sql)
				hp.Init(lexFunc, sql)
				stmts, _, err := hp.ParseSQL()
				if err != nil {
					b.Fatal(err)
				}
				if len(stmts) == 0 {
					b.Fatal("no statements parsed")
				}
			}
		})
	}
}

// BenchmarkGoyaccParser benchmarks the existing goyacc-generated parser.
func BenchmarkGoyaccParser(b *testing.B) {
	for name, sql := range benchSQLs {
		b.Run(name, func(b *testing.B) {
			p := parser.New()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				stmts, _, err := p.Parse(sql, "", "")
				if err != nil {
					b.Fatal(err)
				}
				if len(stmts) == 0 {
					b.Fatal("no statements parsed")
				}
			}
		})
	}
}
