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
	b.ReportAllocs()
	// Million
	b.Run("BenchmarkIntroSelection1000000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(globalCaseMillion) {
				k = len(globalCaseMillion) / b.N * i
			} else {
				k = i%len(globalCaseMillion) + 1
			}
			benchmarkIntroSelection(b, globalCaseMillion, k)
		}
	})
	b.Run("BenchmarkQuickSelection1000000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(globalCaseMillion) {
				k = len(globalCaseMillion) / b.N * i
			} else {
				k = i%len(globalCaseMillion) + 1
			}
			benchmarkQuickSelection(b, globalCaseMillion, k)
		}
	})
	b.Run("BenchmarkSort1000000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCaseMillion)
		}
	})
	// Thousand
	b.Run("BenchmarkIntroSelection1000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(globalCaseThousand) {
				k = len(globalCaseThousand) / b.N * i
			} else {
				k = i%len(globalCaseThousand) + 1
			}
			benchmarkIntroSelection(b, globalCaseThousand, k)
		}
	})
	b.Run("BenchmarkQuickSelection1000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(globalCaseThousand) {
				k = len(globalCaseThousand) / b.N * i
			} else {
				k = i%len(globalCaseThousand) + 1
			}
			benchmarkQuickSelection(b, globalCaseThousand, k)
		}
	})
	b.Run("BenchmarkSort1000", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCaseThousand)
		}
	})
	// Hundred
	b.Run("BenchmarkIntroSelection100", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(globalCaseHundred) {
				k = len(globalCaseHundred) / b.N * i
			} else {
				k = i%len(globalCaseHundred) + 1
			}
			benchmarkIntroSelection(b, globalCaseHundred, k)
		}
	})
	b.Run("BenchmarkQuickSelection100", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(globalCaseHundred) {
				k = len(globalCaseHundred) / b.N * i
			} else {
				k = i%len(globalCaseHundred) + 1
			}
			benchmarkQuickSelection(b, globalCaseHundred, k)
		}
	})
	b.Run("BenchmarkSort100", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCaseHundred)
		}
	})
	// Tens
	b.Run("BenchmarkIntroSelection50", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(globalCaseTens) {
				k = len(globalCaseTens) / b.N * i
			} else {
				k = i%len(globalCaseTens) + 1
			}
			benchmarkIntroSelection(b, globalCaseTens, k)
		}
	})
	b.Run("BenchmarkQuickSelection50", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var k int
			if b.N < len(globalCaseTens) {
				k = len(globalCaseTens) / b.N * i
			} else {
				k = i%len(globalCaseTens) + 1
			}
			benchmarkQuickSelection(b, globalCaseTens, k)
		}
	})
	b.Run("BenchmarkSort50", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			benchmarkSort(b, globalCaseTens)
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

// func benchmarkIntroSelectionMillion(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseMillion))
// 		copy(data, globalCaseMillion)
// 		b.StartTimer()
// 		Select(data, len(globalCaseMillion)/2)
// 	}
// }

// func benchmarkQuickSelectionMillion(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseMillion))
// 		copy(data, globalCaseMillion)
// 		b.StartTimer()
// 		quickselect(data, 0, len(globalCaseMillion)-1, len(globalCaseMillion)/2)
// 	}
// }

// func benchmarkSortMillion(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseMillion))
// 		copy(data, globalCaseMillion)
// 		b.StartTimer()
// 		sort.Sort(data)
// 	}
// }

// func benchmarkSelectionThousand(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseThousand))
// 		copy(data, globalCaseThousand)
// 		b.StartTimer()
// 		Select(data, len(globalCaseThousand)/2)
// 	}
// }

// func benchmarkSortThousand(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseThousand))
// 		copy(data, globalCaseThousand)
// 		b.StartTimer()
// 		sort.Sort(data)
// 	}
// }

// func benchmarkSelectionHundred(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseHundred))
// 		copy(data, globalCaseHundred)
// 		b.StartTimer()
// 		Select(data, len(globalCaseHundred)/2)
// 	}
// }

// func benchmarkSortHundred(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseHundred))
// 		copy(data, globalCaseHundred)
// 		b.StartTimer()
// 		sort.Sort(data)
// 	}
// }

// func benchmarkSelectionTens(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseTens))
// 		copy(data, globalCaseTens)
// 		b.StartTimer()
// 		Select(data, len(globalCaseTens))
// 	}
// }

// func benchmarkSortTens(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		b.StopTimer()
// 		data := make(testSlice, len(globalCaseTens))
// 		copy(data, globalCaseTens)
// 		b.StartTimer()
// 		sort.Sort(data)
// 	}
// }
