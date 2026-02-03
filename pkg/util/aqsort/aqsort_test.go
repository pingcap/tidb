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
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"
)

func TestSortBytesMatchesStdSort(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		keys [][]byte
	}{
		{
			name: "empty",
			keys: nil,
		},
		{
			name: "single",
			keys: [][]byte{[]byte("x")},
		},
		{
			name: "nil-and-empty",
			keys: [][]byte{nil, {}, []byte(""), []byte("a"), []byte("aa"), []byte("a"), nil},
		},
		{
			name: "prefix-heavy",
			keys: [][]byte{
				[]byte("t0001_r00000001"),
				[]byte("t0001_r00000002"),
				[]byte("t0001_r00000010"),
				[]byte("t0001_r00000003"),
				[]byte("t0001"),
				[]byte("t0001_r"),
				[]byte("t0001_r00000001"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := clone(tc.keys)
			want := clone(tc.keys)

			SortBytes(got)
			sort.Slice(want, func(i, j int) bool {
				return bytes.Compare(want[i], want[j]) < 0
			})

			if err := equalByteSlices(got, want); err != "" {
				t.Fatalf("%s", err)
			}
		})
	}
}

func TestSortBytesRandom(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		n := rng.Intn(500) + 1
		keys := make([][]byte, n)
		for i := 0; i < n; i++ {
			ln := rng.Intn(64)
			b := make([]byte, ln)
			_, _ = rng.Read(b)
			keys[i] = b
		}

		got := clone(keys)
		want := clone(keys)

		SortBytes(got)
		sort.Slice(want, func(i, j int) bool {
			return bytes.Compare(want[i], want[j]) < 0
		})

		if err := equalByteSlices(got, want); err != "" {
			t.Fatalf("iter=%d: %s", iter, err)
		}
	}
}

func TestPairSorterMatchesStdSort(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 100; iter++ {
		n := rng.Intn(500) + 1
		pairs := make([]Pair[int], n)
		for i := 0; i < n; i++ {
			k := make([]byte, 16)
			_, _ = rng.Read(k[:12])
			binary.BigEndian.PutUint32(k[12:], uint32(i))
			pairs[i] = Pair[int]{Key: k, Val: i}
		}

		got := make([]Pair[int], len(pairs))
		want := make([]Pair[int], len(pairs))
		copy(got, pairs)
		copy(want, pairs)

		var s PairSorter[int]
		s.Sort(got)
		sort.Slice(want, func(i, j int) bool {
			return bytes.Compare(want[i].Key, want[j].Key) < 0
		})

		for i := range got {
			if !bytes.Equal(got[i].Key, want[i].Key) || got[i].Val != want[i].Val {
				t.Fatalf("iter=%d: mismatch at index %d", iter, i)
			}
		}
	}
}

func clone(keys [][]byte) [][]byte {
	out := make([][]byte, len(keys))
	copy(out, keys)
	return out
}

func equalByteSlices(a, b [][]byte) string {
	if len(a) != len(b) {
		return "length mismatch"
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return "mismatch at index " + itoa(i)
		}
	}
	return ""
}

func itoa(i int) string {
	// Small helper to avoid importing strconv in the test package hot path.
	if i == 0 {
		return "0"
	}
	var buf [32]byte
	n := 0
	for i > 0 {
		buf[n] = byte('0' + i%10)
		i /= 10
		n++
	}
	// reverse
	for l, r := 0, n-1; l < r; l, r = l+1, r-1 {
		buf[l], buf[r] = buf[r], buf[l]
	}
	return string(buf[:n])
}
