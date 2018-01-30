// Copyright 2017 PingCAP, Inc.
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

package parser

import (
	"testing"
)

func sysebchSelect(parser *Parser, sql string, num int, b *testing.B) {
	for i := 0; i < num; i++ {
		_, err := parser.Parse(sql, "", "")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSysbenchSelect1024(b *testing.B) {
	parser := New()
	sql := "SELECT pad FROM sbtest1 WHERE id=1;"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sysebchSelect(parser, sql, 1024, b)
	}
	b.ReportAllocs()
}

func BenchmarkSysbenchSelect256(b *testing.B) {
	parser := New()
	sql := "SELECT pad FROM sbtest1 WHERE id=1;"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sysebchSelect(parser, sql, 256, b)
	}
	b.ReportAllocs()
}

func BenchmarkSysbenchSelect128(b *testing.B) {
	parser := New()
	sql := "SELECT pad FROM sbtest1 WHERE id=1;"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sysebchSelect(parser, sql, 128, b)
	}
	b.ReportAllocs()
}

func BenchmarkParse4096(b *testing.B) {
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range table {
			parse(parser, v, 4096, b)
		}
	}
	b.ReportAllocs()
}

func BenchmarkParse1024(b *testing.B) {
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range table {
			parse(parser, v, 1024, b)
		}
	}
	b.ReportAllocs()
}

var table = []string{
	"insert into t values (1), (2), (3)",
	"insert into t values (4), (5), (6), (7)",
	"select c from t where c > 2",
	"select c from t where c > 1234567890123456789012345678901234567890.0;",
}

func BenchmarkParse512(b *testing.B) {
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range table {
			parse(parser, v, 512, b)
		}

	}
	b.ReportAllocs()
}

func parse(parser *Parser, sql string, num int, b *testing.B) {
	for i := 0; i < num; i++ {
		_, err := parser.Parse(sql, "", "")
		if err != nil {
			b.Failed()
		}
	}
}

func BenchmarkParse256(b *testing.B) {
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range table {
			parse(parser, v, 256, b)
		}

	}
	b.ReportAllocs()
}
