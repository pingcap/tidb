// Copyright 2021 PingCAP, Inc.
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

package set

import (
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkFloat64SetMemoryUsage(b *testing.B) {
	b.ReportAllocs()
	type testCase struct {
		rowNum int
	}
	cases := []testCase{
		{rowNum: 0},
		{rowNum: 100},
		{rowNum: 10000},
		{rowNum: 1000000},
		{rowNum: 851968}, // 6.5 * (1 << 17)
		{rowNum: 851969}, // 6.5 * (1 << 17) + 1
		{rowNum: 425984}, // 6.5 * (1 << 16),
		{rowNum: 425985}, // 6.5 * (1 << 16) + 1
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("MapRows %v", c.rowNum), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				float64Set, _ := NewFloat64SetWithMemoryUsage()
				for num := 0; num < c.rowNum; num++ {
					float64Set.Insert(float64(num))
				}
			}
		})
	}
}

func BenchmarkInt64SetMemoryUsage(b *testing.B) {
	b.ReportAllocs()
	type testCase struct {
		rowNum int
	}
	cases := []testCase{
		{rowNum: 0},
		{rowNum: 100},
		{rowNum: 10000},
		{rowNum: 1000000},
		{rowNum: 851968}, // 6.5 * (1 << 17)
		{rowNum: 851969}, // 6.5 * (1 << 17) + 1
		{rowNum: 425984}, // 6.5 * (1 << 16)
		{rowNum: 425985}, // 6.5 * (1 << 16) + 1
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("MapRows %v", c.rowNum), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				int64Set, _ := NewInt64SetWithMemoryUsage()
				for num := 0; num < c.rowNum; num++ {
					int64Set.Insert(int64(num))
				}
			}
		})
	}
}

func BenchmarkStringSetMemoryUsage(b *testing.B) {
	b.ReportAllocs()
	type testCase struct {
		rowNum int
	}
	cases := []testCase{
		{rowNum: 0},
		{rowNum: 100},
		{rowNum: 10000},
		{rowNum: 1000000},
		{rowNum: 851968}, // 6.5 * (1 << 17)
		{rowNum: 851969}, // 6.5 * (1 << 17) + 1
		{rowNum: 425984}, // 6.5 * (1 << 16)
		{rowNum: 425985}, // 6.5 * (1 << 16) + 1
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("MapRows %v", c.rowNum), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				stringSet, _ := NewStringSetWithMemoryUsage()
				for num := 0; num < c.rowNum; num++ {
					stringSet.Insert(strconv.Itoa(num))
				}
			}
		})
	}
}
