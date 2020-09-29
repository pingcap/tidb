// Copyright 2020 PingCAP, Inc.
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

package selection

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

type testSuite struct{}

var _ = Suite(&testSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	TestingT(t)
}

type testSlice []int

func (a testSlice) Len() int           { return len(a) }
func (a testSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a testSlice) Less(i, j int) bool { return a[i] < a[j] }

var (
	globalCase10000000 testSlice
	globalCase1000000  testSlice
	globalCase100000   testSlice
	globalCase10000    testSlice
	globalCase1000     testSlice
	globalCase100      testSlice
	globalCase50       testSlice
)

func init() {
	globalCase10000000 = randomTestCase(10000000)
	globalCase1000000 = randomTestCase(1000000)
	globalCase100000 = randomTestCase(100000)
	globalCase10000 = randomTestCase(10000)
	globalCase1000 = randomTestCase(1000)
	globalCase100 = randomTestCase(100)
	globalCase50 = randomTestCase(50)
}

func (s *testSuite) TestSelection(c *C) {
	data := testSlice{1, 2, 3, 4, 5}
	index := Select(data, 3)
	c.Assert(data[index], Equals, 3)
}

func (s *testSuite) TestSelectionWithDuplicate(c *C) {
	data := testSlice{1, 2, 3, 3, 5}
	index := Select(data, 3)
	c.Assert(data[index], Equals, 3)
	index = Select(data, 5)
	c.Assert(data[index], Equals, 5)
}

func (s *testSuite) TestSelectionWithRandomCase(c *C) {
	data := randomTestCase(1000000)
	index := Select(data, 500000)
	actual := data[index]
	sort.Stable(data)
	expected := data[499999]
	c.Assert(actual, Equals, expected)
}

func (s *testSuite) TestSelectionWithSerialCase(c *C) {
	data := serialTestCase(1000000)
	sort.Sort(sort.Reverse(data))
	index := Select(data, 500000)
	actual := data[index]
	sort.Stable(data)
	expected := data[499999]
	c.Assert(actual, Equals, expected)
}

func randomTestCase(size int) testSlice {
	data := make(testSlice, 0, size)
	rand.Seed(time.Now().Unix())
	for i := 0; i < size; i++ {
		data = append(data, rand.Int()%100)
	}
	return data
}

func serialTestCase(size int) testSlice {
	data := make(testSlice, 0, size)
	for i := 0; i < size; i++ {
		data = append(data, i)
	}
	return data
}

func BenchmarkSelection(b *testing.B) {
	runSelect := func(b *testing.B, testCase testSlice, benchFunc func(*testing.B, testSlice, int)) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(testCase) {
				k = len(testCase) / b.N * i
			} else {
				k = i%len(testCase) + 1
			}
			benchFunc(b, testCase, k)
		}
	}
	b.ReportAllocs()
	// Ten Million
	b.Run("BenchmarkIntroSelection10000000", func(b *testing.B) {
		runSelect(b, globalCase10000000, benchmarkIntroSelection)
	})
	b.Run("BenchmarkQuickSelection10000000", func(b *testing.B) {
		runSelect(b, globalCase10000000, benchmarkQuickSelection)
	})
	b.Run("BenchmarkSort10000000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCase10000000)
		}
	})
	// Million
	b.Run("BenchmarkIntroSelection1000000", func(b *testing.B) {
		runSelect(b, globalCase1000000, benchmarkIntroSelection)
	})
	b.Run("BenchmarkQuickSelection1000000", func(b *testing.B) {
		runSelect(b, globalCase1000000, benchmarkQuickSelection)
	})
	b.Run("BenchmarkSort1000000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCase1000000)
		}
	})
	// Hundred thousands
	b.Run("BenchmarkIntroSelection100000", func(b *testing.B) {
		runSelect(b, globalCase100000, benchmarkIntroSelection)
	})
	b.Run("BenchmarkQuickSelection100000", func(b *testing.B) {
		runSelect(b, globalCase100000, benchmarkQuickSelection)
	})
	b.Run("BenchmarkSort100000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCase100000)
		}
	})
	// Ten thousands
	b.Run("BenchmarkIntroSelection10000", func(b *testing.B) {
		runSelect(b, globalCase10000, benchmarkIntroSelection)
	})
	b.Run("BenchmarkQuickSelection10000", func(b *testing.B) {
		runSelect(b, globalCase10000, benchmarkQuickSelection)
	})
	b.Run("BenchmarkSort10000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCase10000)
		}
	})
	// Thousand
	b.Run("BenchmarkIntroSelection1000", func(b *testing.B) {
		runSelect(b, globalCase1000, benchmarkIntroSelection)
	})
	b.Run("BenchmarkQuickSelection1000", func(b *testing.B) {
		runSelect(b, globalCase1000, benchmarkQuickSelection)
	})
	b.Run("BenchmarkSort1000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCase1000)
		}
	})
	// Hundred
	b.Run("BenchmarkIntroSelection100", func(b *testing.B) {
		runSelect(b, globalCase100, benchmarkIntroSelection)
	})
	b.Run("BenchmarkQuickSelection100", func(b *testing.B) {
		runSelect(b, globalCase100, benchmarkQuickSelection)
	})
	b.Run("BenchmarkSort100", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCase100)
		}
	})
	// Tens
	b.Run("BenchmarkIntroSelection50", func(b *testing.B) {
		runSelect(b, globalCase50, benchmarkIntroSelection)
	})
	b.Run("BenchmarkQuickSelection50", func(b *testing.B) {
		runSelect(b, globalCase50, benchmarkQuickSelection)
	})
	b.Run("BenchmarkSort50", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCase50)
		}
	})
}

func benchmarkIntroSelection(b *testing.B, testCase testSlice, k int) {
	b.StopTimer()
	data := make(testSlice, len(testCase))
	copy(data, testCase)
	b.StartTimer()
	Select(data, k)
}

func benchmarkQuickSelection(b *testing.B, testCase testSlice, k int) {
	b.StopTimer()
	data := make(testSlice, len(testCase))
	copy(data, testCase)
	b.StartTimer()
	quickselect(data, 0, len(testCase)-1, k-1)
}

func benchmarkSort(b *testing.B, testCase testSlice) {
	b.StopTimer()
	data := make(testSlice, len(testCase))
	copy(data, testCase)
	b.StartTimer()
	sort.Sort(data)
}
