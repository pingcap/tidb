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

package selection_test

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/selection"
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
	globalCaseMillion  testSlice
	globalCaseThousand testSlice
	globalCaseHundred  testSlice
	globalCaseTens     testSlice
)

func init() {
	globalCaseMillion = randomTestCase(1000000)
	globalCaseThousand = randomTestCase(1000)
	globalCaseHundred = randomTestCase(100)
	globalCaseTens = randomTestCase(50)
}

func (s *testSuite) TestSelection(c *C) {
	data := testSlice{1, 2, 3, 4, 5}
	index := selection.Select(data, 3)
	c.Assert(data[index], Equals, 3)
}

func (s *testSuite) TestSelectionWithDuplicate(c *C) {
	data := testSlice{1, 2, 3, 3, 5}
	index := selection.Select(data, 3)
	c.Assert(data[index], Equals, 3)
	index = selection.Select(data, 5)
	c.Assert(data[index], Equals, 5)
}

func (s *testSuite) TestSelectionWithRandomCase(c *C) {
	data := randomTestCase(1000000)
	index := selection.Select(data, 500000)
	actual := data[index]
	sort.Stable(data)
	expected := data[499999]
	c.Assert(actual, Equals, expected)
}

func (s *testSuite) TestSelectionWithSerialCase(c *C) {
	data := serialTestCase(1000000)
	sort.Sort(sort.Reverse(data))
	index := selection.Select(data, 500000)
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
	b.ReportAllocs()
	b.Run("BenchmarkSelection1000000", benchmarkSelectionMillion)
	b.Run("BenchmarkSort1000000", benchmarkSortMillion)
	b.Run("BenchmarkSelection1000", benchmarkSelectionThousand)
	b.Run("BenchmarkSort1000", benchmarkSortThousand)
	b.Run("BenchmarkSelection100", benchmarkSelectionHundred)
	b.Run("BenchmarkSort100", benchmarkSortHundred)
	b.Run("BenchmarkSelection50", benchmarkSelectionTens)
	b.Run("BenchmarkSort50", benchmarkSortTens)
}

func benchmarkSelectionMillion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseMillion))
		copy(data, globalCaseMillion)
		b.StartTimer()
		selection.Select(data, len(globalCaseMillion))
	}
}

func benchmarkSortMillion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseMillion))
		copy(data, globalCaseMillion)
		b.StartTimer()
		sort.Sort(data)
	}
}

func benchmarkSelectionThousand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseThousand))
		copy(data, globalCaseThousand)
		b.StartTimer()
		selection.Select(data, len(globalCaseThousand))
	}
}

func benchmarkSortThousand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseThousand))
		copy(data, globalCaseThousand)
		b.StartTimer()
		sort.Sort(data)
	}
}

func benchmarkSelectionHundred(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseHundred))
		copy(data, globalCaseHundred)
		b.StartTimer()
		selection.Select(data, len(globalCaseHundred))
	}
}

func benchmarkSortHundred(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseHundred))
		copy(data, globalCaseHundred)
		b.StartTimer()
		sort.Sort(data)
	}
}

func benchmarkSelectionTens(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseTens))
		copy(data, globalCaseTens)
		b.StartTimer()
		selection.Select(data, len(globalCaseTens))
	}
}

func benchmarkSortTens(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseTens))
		copy(data, globalCaseTens)
		b.StartTimer()
		sort.Sort(data)
	}
}
