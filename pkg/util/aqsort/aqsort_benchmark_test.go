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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aqsort

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"
)

func BenchmarkByteSorters(b *testing.B) {
	aqsReuse := func(n int) func([][]byte) {
		s := &Sorter{tmp: make([][]byte, n)}
		return s.SortBytes
	}

	sorters := []struct {
		name string
		sort func([][]byte)
	}{
		{
			name: "AQSAlloc",
			sort: SortBytes,
		},
		{
			name: "AQSReuse",
			sort: nil, // filled per-size
		},
		{
			name: "StdSortSlice",
			sort: func(keys [][]byte) {
				sort.Slice(keys, func(i, j int) bool {
					return bytes.Compare(keys[i], keys[j]) < 0
				})
			},
		},
		{
			name: "SlicesSortFunc",
			sort: func(keys [][]byte) {
				slices.SortFunc(keys, bytes.Compare)
			},
		},
	}

	cases := []struct {
		name string
		gen  func(rng *rand.Rand, n int) [][]byte
	}{
		{
			name: "random_len16",
			gen: func(rng *rand.Rand, n int) [][]byte {
				keys := make([][]byte, n)
				for i := 0; i < n; i++ {
					b := make([]byte, 16)
					_, _ = rng.Read(b)
					keys[i] = b
				}
				return keys
			},
		},
		{
			name: "common_prefix32_suffix8",
			gen: func(rng *rand.Rand, n int) [][]byte {
				prefix := make([]byte, 32)
				_, _ = rng.Read(prefix)
				keys := make([][]byte, n)
				for i := 0; i < n; i++ {
					k := make([]byte, 40)
					copy(k, prefix)
					_, _ = rng.Read(k[32:])
					keys[i] = k
				}
				return keys
			},
		},
		{
			name: "common_prefix32_varlen_suffix0to16",
			gen: func(rng *rand.Rand, n int) [][]byte {
				prefix := make([]byte, 32)
				_, _ = rng.Read(prefix)
				keys := make([][]byte, n)
				for i := 0; i < n; i++ {
					suffixLen := rng.Intn(17) // include 0 (prefix-only)
					k := make([]byte, 32+suffixLen)
					copy(k, prefix)
					if suffixLen > 0 {
						_, _ = rng.Read(k[32:])
					}
					keys[i] = k
				}
				return keys
			},
		},
	}

	sizes := []int{1_000, 10_000, 100_000}

	for _, tc := range cases {
		for _, n := range sizes {
			seed := int64(1) // deterministic across sorters
			orig := tc.gen(rand.New(rand.NewSource(seed)), n)
			work := make([][]byte, len(orig))

			for _, s := range sorters {
				if s.name == "AQSReuse" {
					s.sort = aqsReuse(n)
				}
				b.Run(fmt.Sprintf("%s/%s/n=%d", s.name, tc.name, n), func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						copy(work, orig)
						b.StartTimer()
						s.sort(work)
					}
				})
			}
		}
	}
}
