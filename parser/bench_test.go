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

func BenchmarkSysbenchSelect(b *testing.B) {
	parser := New()
	sql := "SELECT pad FROM sbtest1 WHERE id=1;"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.Parse(sql, "", "")
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
}

func BenchmarkParse(b *testing.B) {
	var table = []string{
		"insert into t values (1), (2), (3)",
		"insert into t values (4), (5), (6), (7)",
		"select c from t where c > 2",
	}
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range table {
			_, err := parser.Parse(v, "", "")
			if err != nil {
				b.Failed()
			}
		}
	}
	b.ReportAllocs()
}
