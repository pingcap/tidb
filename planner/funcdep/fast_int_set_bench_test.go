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

package funcdep

import (
	"testing"

	"golang.org/x/tools/container/intsets"
)

func BenchmarkMapIntSet_Difference(b *testing.B) {
	intSetA := NewIntSet()
	for i := 0; i < 200000; i++ {
		intSetA[i] = struct{}{}
	}
	intSetB := NewIntSet()
	for i := 100000; i < 300000; i++ {
		intSetB[i] = struct{}{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmp := NewIntSet()
		tmp.Difference2(intSetA, intSetB)
		//intSetA.SubsetOf(intSetB)
	}
}

func BenchmarkIntSet_Difference(b *testing.B) {
	intSetA := &intsets.Sparse{}
	for i := 0; i < 200000; i++ {
		intSetA.Insert(i)
	}
	intSetB := &intsets.Sparse{}
	for i := 100000; i < 300000; i++ {
		intSetA.Insert(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmp := &intsets.Sparse{}
		tmp.Difference(intSetA, intSetB)
		//intSetA.SubsetOf(intSetB)
	}
}

func BenchmarkFastIntSet_Difference(b *testing.B) {
	intSetA := NewFastIntSet()
	for i := 0; i < 200000; i++ {
		intSetA.Insert(i)
	}
	intSetB := NewFastIntSet()
	for i := 100000; i < 300000; i++ {
		intSetA.Insert(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intSetA.Difference(intSetB)
		//intSetA.SubsetOf(intSetB)
	}
}

func BenchmarkIntSet_Insert(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intSet := NewIntSet()
		for j := 0; j < 64; j++ {
			intSet.Insert(j)
		}
	}
}

func BenchmarkSparse_Insert(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intSet := &intsets.Sparse{}
		for j := 0; j < 64; j++ {
			intSet.Insert(j)
		}
	}
}

func BenchmarkFastIntSet_Insert(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intSet := NewFastIntSet()
		for j := 0; j < 64; j++ {
			intSet.Insert(j)
		}
	}
}

// BenchMarkResult
//
// Test with Difference (traverse and allocation) (size means the intersection size)
// +--------------------------------------------------------------------------------+
// |     size   |   map int set   |  basic sparse int set  |   fast sparse int set  |
// +------------+-----------------+------------------------+------------------------+
// |       64   |     3203 ns/op  |         64 ns/op       |        5.750 ns/op     |
// |     1000   |   244284 ns/op  |        822 ns/op       |        919.8 ns/op     |
// |    10000   |  2940130 ns/op  |       8071 ns/op       |         8686 ns/op     |
// |   100000   | 41283606 ns/op  |      83320 ns/op       |        85563 ns/op     |
// +------------+-----------------+------------------------+------------------------+
//
// This test is under same operation with same data with following analysis:
// MapInt and Sparse are all temporarily allocated for intermediate result. Since
// MapInt need to reallocate the capacity with unit as int(64) which is expensive
// than a bit of occupation in Sparse reallocation.
//
// From the space utilization and allocation times, here in favour of sparse.
//
// Test with Insert (allocation)
// +----------------------------------------------------------------------------+
// |  size  |   map int set   |  basic sparse int set  |   fast sparse int set  |
// +--------+-----------------+------------------------+------------------------+
// |     64 |     5705 ns/op  |         580 ns/op      |           234 ns/op    |
// |   1000 |   122906 ns/op  |        7991 ns/op      |         10606 ns/op    |
// |  10000 |   845961 ns/op  |      281134 ns/op      |        303006 ns/op    |
// | 100000 | 15529859 ns/op  |    31273573 ns/op      |      30752177 ns/op    |
// +--------------------------+------------------------+------------------------+
//
// From insert, map insert take much time than sparse does when insert size is under
// 100 thousand. While when the size grows bigger, map insert take less time to do that
// (maybe cost more memory usage), that because sparse need to traverse the chain to
// find the suitable block to insert.
//
// From insert, if set size is larger than 100 thousand, map-set is preferred, otherwise, sparse is good.
//
// Test with Subset (traverse) (sizes means the size A / B)
// +---------------------------------------------------------------------------------+
// |  size  |   map int set   |  basic sparse int set  |   fast sparse int set  |
// +--------+-----------------+------------------------+------------------------+
// |     64 |    59.47 ns/op  |       3.775 ns/op      |        2.727 ns/op     |
// |   1000 |    68.9 ns/op   |       3.561 ns/op      |        22.64 ns/op     |
// |  20000 |   104.7 ns/op   |       3.502 ns/op      |        23.92 ns/op     |
// | 200000 |   249.8 ns/op   |       3.504 ns/op      |        22.11 ns/op     |
// +--------------------------+------------------------+------------------------+
//
// This is the most amazing part we have been tested. Map set need to compute the equality
// about the every key int in the map with others. While sparse only need to do is just to
// fetch every bitmap (every bit implies a number) and to the bit operation together.
//
// FastIntSet's performance is quite like Sparse IntSet, because they have the same implementation
// inside. While FastIntSet have some optimizations with small range (0-63), so we add an
// extra test for size with 64, from the number above, they did have some effects, especially
// in computing the difference (bit | technically).
//
// From all above, we are in favour of sparse. sparse to store fast-int-set instead of using map.
