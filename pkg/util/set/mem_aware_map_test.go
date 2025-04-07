// Copyright 2022 PingCAP, Inc.
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
	"strconv"
	"testing"
)

var result int

var inputs = []struct {
	input int
}{
	{input: 1},
	{input: 100},
	{input: 10000},
	{input: 1000000},
}

func memAwareIntMap(size int) int {
	var x int
	m := NewMemAwareMap[int, int]()
	for j := range size {
		m.Set(j, j)
	}
	for j := range size {
		x, _ = m.Get(j)
	}
	return x
}

func nativeIntMap(size int) int {
	var x int
	m := make(map[int]int)
	for j := range size {
		m[j] = j
	}

	for j := range size {
		x = m[j]
	}
	return x
}

func BenchmarkMemAwareIntMap(b *testing.B) {
	for _, s := range inputs {
		b.Run("MemAwareIntMap_"+strconv.Itoa(s.input), func(b *testing.B) {
			var x int
			for range b.N {
				x = memAwareIntMap(s.input)
			}
			result = x
		})
	}
}

func BenchmarkNativeIntMap(b *testing.B) {
	for _, s := range inputs {
		b.Run("NativeIntMap_"+strconv.Itoa(s.input), func(b *testing.B) {
			var x int
			for range b.N {
				x = nativeIntMap(s.input)
			}
			result = x
		})
	}
}
