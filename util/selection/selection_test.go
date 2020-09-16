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
)

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

func TestSelection(t *testing.T) {
	s := testSlice{1, 2, 3, 4, 5}
	index := Select(s, 3)
	if s[index] != 3 {
		t.Fail()
	}
}
func TestSelectionWithDuplicate(t *testing.T) {
	s := testSlice{1, 2, 3, 3, 5}
	index := Select(s, 3)
	if s[index] != 3 {
		t.Fail()
	}
	index = Select(s, 5)
	if s[index] != 5 {
		t.Fail()
	}
}

func TestSelectionWithRandomCase(t *testing.T) {
	data := randomTestCase(1000000)
	index := Select(data, 500000)
	value := data[index]
	sort.Stable(data)
	expected := data[499999]
	if value != expected {
		t.Errorf("Expected: %v, Actual: %v\n%v", expected, value, data)
	}
}

func TestSelectionWithSerialCase(t *testing.T) {
	data := serialTestCase(100)
	sort.Sort(sort.Reverse(data))
	index := Select(data, 50)
	value := data[index]
	sort.Stable(data)
	expected := data[49]
	if value != expected {
		t.Errorf("Expected: %v, Actual: %v\n%v", expected, value, data)
	}
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

func BenchmarkSelectionMillion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseMillion))
		copy(data, globalCaseMillion)
		b.StartTimer()
		Select(data, len(globalCaseMillion))
	}
}

func BenchmarkSortMillion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseMillion))
		copy(data, globalCaseMillion)
		b.StartTimer()
		sort.Sort(data)
	}
}

func BenchmarkSelectionThousand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseThousand))
		copy(data, globalCaseThousand)
		b.StartTimer()
		Select(data, len(globalCaseThousand))
	}
}

func BenchmarkSortThousand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseThousand))
		copy(data, globalCaseThousand)
		b.StartTimer()
		sort.Sort(data)
	}
}

func BenchmarkSelectionHundred(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseHundred))
		copy(data, globalCaseHundred)
		b.StartTimer()
		Select(data, len(globalCaseHundred))
	}
}

func BenchmarkSortHundred(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseHundred))
		copy(data, globalCaseHundred)
		b.StartTimer()
		sort.Sort(data)
	}
}

func BenchmarkSelectionTens(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseTens))
		copy(data, globalCaseTens)
		b.StartTimer()
		Select(data, len(globalCaseTens))
	}
}

func BenchmarkSortTens(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make(testSlice, len(globalCaseTens))
		copy(data, globalCaseTens)
		b.StartTimer()
		sort.Sort(data)
	}
}
