// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package simplesst

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetMaxOverlapping(t *testing.T) {
	// [1, 3), [2, 4)
	points := []Endpoint{
		{Key: []byte{1}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{3}, Tp: ExclusiveEnd, Weight: 1},
		{Key: []byte{2}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{4}, Tp: ExclusiveEnd, Weight: 1},
	}
	require.EqualValues(t, 2, GetMaxOverlapping(points))
	// [1, 3), [2, 4), [3, 5)
	points = []Endpoint{
		{Key: []byte{1}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{3}, Tp: ExclusiveEnd, Weight: 1},
		{Key: []byte{2}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{4}, Tp: ExclusiveEnd, Weight: 1},
		{Key: []byte{3}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{5}, Tp: ExclusiveEnd, Weight: 1},
	}
	require.EqualValues(t, 2, GetMaxOverlapping(points))
	// [1, 3], [2, 4], [3, 5]
	points = []Endpoint{
		{Key: []byte{1}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{3}, Tp: InclusiveEnd, Weight: 1},
		{Key: []byte{2}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{4}, Tp: InclusiveEnd, Weight: 1},
		{Key: []byte{3}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{5}, Tp: InclusiveEnd, Weight: 1},
	}
	require.EqualValues(t, 3, GetMaxOverlapping(points))
}

func TestRemoveDuplicates(t *testing.T) {
	valGetter := func(e *int) []byte {
		return []byte{byte(*e)}
	}
	cases := []struct {
		in   []int
		out  []int
		dups []int
	}{
		// no duplicates
		{in: []int{}, out: []int{}, dups: []int{}},
		{in: []int{1}, out: []int{1}, dups: []int{}},
		{in: []int{1, 2}, out: []int{1, 2}, dups: []int{}},
		{in: []int{1, 2, 3}, out: []int{1, 2, 3}, dups: []int{}},
		{in: []int{1, 2, 3, 4, 5}, out: []int{1, 2, 3, 4, 5}, dups: []int{}},
		// duplicates at beginning
		{in: []int{1, 1}, out: []int{}, dups: []int{1, 1}},
		{in: []int{1, 1, 1}, out: []int{}, dups: []int{1, 1, 1}},
		{in: []int{1, 1, 2, 3}, out: []int{2, 3}, dups: []int{1, 1}},
		{in: []int{1, 1, 1, 2, 3}, out: []int{2, 3}, dups: []int{1, 1, 1}},
		// duplicates in middle
		{in: []int{1, 2, 2, 3}, out: []int{1, 3}, dups: []int{2, 2}},
		{in: []int{1, 2, 2, 2, 3}, out: []int{1, 3}, dups: []int{2, 2, 2}},
		{in: []int{1, 2, 2, 2, 3, 3, 4}, out: []int{1, 4}, dups: []int{2, 2, 2, 3, 3}},
		{in: []int{1, 2, 2, 2, 3, 3, 4, 4, 5}, out: []int{1, 5}, dups: []int{2, 2, 2, 3, 3, 4, 4}},
		{in: []int{1, 2, 2, 2, 3, 4, 4, 5}, out: []int{1, 3, 5}, dups: []int{2, 2, 2, 4, 4}},
		{in: []int{1, 2, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 8, 9}, out: []int{1, 3, 6, 7, 9}, dups: []int{2, 2, 2, 4, 4, 5, 5, 8, 8}},
		// duplicates at end
		{in: []int{1, 2, 3, 3}, out: []int{1, 2}, dups: []int{3, 3}},
		{in: []int{1, 2, 3, 3, 3}, out: []int{1, 2}, dups: []int{3, 3, 3}},
		// mixing
		{in: []int{1, 1, 2, 3, 3, 4}, out: []int{2, 4}, dups: []int{1, 1, 3, 3}},
		{in: []int{1, 2, 3, 3, 4, 4}, out: []int{1, 2}, dups: []int{3, 3, 4, 4}},
		{in: []int{1, 1, 2, 3, 4, 4}, out: []int{2, 3}, dups: []int{1, 1, 4, 4}},
		{in: []int{1, 1, 2, 2, 3, 3}, out: []int{}, dups: []int{1, 1, 2, 2, 3, 3}},
		{in: []int{1, 1, 2, 2, 2, 3, 3}, out: []int{}, dups: []int{1, 1, 2, 2, 2, 3, 3}},
		{in: []int{1, 1, 2, 2, 2, 3, 3, 4, 4}, out: []int{}, dups: []int{1, 1, 2, 2, 2, 3, 3, 4, 4}},
		{in: []int{1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5}, out: []int{}, dups: []int{1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5}},
		{in: []int{1, 1, 2, 2, 2, 3, 4, 4, 5, 5}, out: []int{3}, dups: []int{1, 1, 2, 2, 2, 4, 4, 5, 5}},
		{in: []int{1, 1, 2, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 8, 9, 9}, out: []int{3, 6, 7}, dups: []int{1, 1, 2, 2, 2, 4, 4, 5, 5, 8, 8, 9, 9}},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			require.True(t, slices.IsSorted(c.in))
			require.True(t, slices.IsSorted(c.out))
			require.True(t, slices.IsSorted(c.dups))
			require.Equal(t, len(c.dups), len(c.in)-len(c.out))
			tmpIn := make([]int, len(c.in))
			copy(tmpIn, c.in)
			out, dups, dupCnt := RemoveDuplicates(tmpIn, valGetter, true)
			require.EqualValues(t, c.out, out)
			require.EqualValues(t, c.dups, dups)
			require.Equal(t, dupCnt, len(dups))

			tmpIn = make([]int, len(c.in))
			copy(tmpIn, c.in)
			out, dups, dupCnt = RemoveDuplicates(tmpIn, valGetter, false)
			require.EqualValues(t, c.out, out)
			require.Empty(t, dups)
			require.Equal(t, dupCnt, len(c.dups))
		})
	}
}

func TestRemoveDuplicatesMoreThan2(t *testing.T) {
	valGetter := func(e *int) []byte {
		return []byte{byte(*e)}
	}
	cases := []struct {
		in    []int
		out   []int
		dups  []int
		total int
	}{
		// no duplicates
		{in: []int{}, out: []int{}, dups: []int{}, total: 0},
		{in: []int{1}, out: []int{1}, dups: []int{}, total: 0},
		{in: []int{1, 2}, out: []int{1, 2}, dups: []int{}, total: 0},
		{in: []int{1, 2, 3}, out: []int{1, 2, 3}, dups: []int{}, total: 0},
		{in: []int{1, 2, 3, 4, 5}, out: []int{1, 2, 3, 4, 5}, dups: []int{}, total: 0},
		// duplicates at beginning
		{in: []int{1, 1}, out: []int{1, 1}, dups: []int{}, total: 2},
		{in: []int{1, 1, 1}, out: []int{1, 1}, dups: []int{1}, total: 3},
		{in: []int{1, 1, 1, 1}, out: []int{1, 1}, dups: []int{1, 1}, total: 4},
		{in: []int{1, 1, 1, 1, 1}, out: []int{1, 1}, dups: []int{1, 1, 1}, total: 5},
		{in: []int{1, 1, 2, 3}, out: []int{1, 1, 2, 3}, dups: []int{}, total: 2},
		{in: []int{1, 1, 1, 2, 3}, out: []int{1, 1, 2, 3}, dups: []int{1}, total: 3},
		{in: []int{1, 1, 1, 1, 2, 3}, out: []int{1, 1, 2, 3}, dups: []int{1, 1}, total: 4},
		// duplicates in middle
		{in: []int{1, 2, 2, 3}, out: []int{1, 2, 2, 3}, dups: []int{}, total: 2},
		{in: []int{1, 2, 2, 2, 3}, out: []int{1, 2, 2, 3}, dups: []int{2}, total: 3},
		{in: []int{1, 2, 2, 2, 2, 3}, out: []int{1, 2, 2, 3}, dups: []int{2, 2}, total: 4},
		{in: []int{1, 2, 2, 2, 2, 2, 3}, out: []int{1, 2, 2, 3}, dups: []int{2, 2, 2}, total: 5},
		{in: []int{1, 2, 2, 2, 3, 3, 4}, out: []int{1, 2, 2, 3, 3, 4}, dups: []int{2}, total: 5},
		{in: []int{1, 2, 2, 2, 3, 3, 4, 4, 5}, out: []int{1, 2, 2, 3, 3, 4, 4, 5}, dups: []int{2}, total: 7},
		{in: []int{1, 2, 2, 2, 3, 4, 4, 5}, out: []int{1, 2, 2, 3, 4, 4, 5}, dups: []int{2}, total: 5},
		{in: []int{1, 2, 2, 2, 3, 4, 4, 5, 5, 5, 6, 7, 8, 8, 9}, out: []int{1, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 8, 9}, dups: []int{2, 5}, total: 10},
		// duplicates at end
		{in: []int{1, 2, 3, 3}, out: []int{1, 2, 3, 3}, dups: []int{}, total: 2},
		{in: []int{1, 2, 3, 3, 3}, out: []int{1, 2, 3, 3}, dups: []int{3}, total: 3},
		{in: []int{1, 2, 3, 3, 3, 3}, out: []int{1, 2, 3, 3}, dups: []int{3, 3}, total: 4},
		{in: []int{1, 2, 3, 3, 3, 3, 3}, out: []int{1, 2, 3, 3}, dups: []int{3, 3, 3}, total: 5},
		// mixing
		{in: []int{1, 1, 1, 1, 1, 2, 3, 3, 3, 4}, out: []int{1, 1, 2, 3, 3, 4}, dups: []int{1, 1, 1, 3}, total: 8},
		{in: []int{1, 2, 3, 3, 3, 4, 4, 4}, out: []int{1, 2, 3, 3, 4, 4}, dups: []int{3, 4}, total: 6},
		{in: []int{1, 1, 1, 2, 3, 4, 4, 4}, out: []int{1, 1, 2, 3, 4, 4}, dups: []int{1, 4}, total: 6},
		{in: []int{1, 1, 1, 2, 2, 2, 3, 3, 3}, out: []int{1, 1, 2, 2, 3, 3}, dups: []int{1, 2, 3}, total: 9},
		{in: []int{1, 1, 2, 2, 2, 3, 3}, out: []int{1, 1, 2, 2, 3, 3}, dups: []int{2}, total: 7},
		{in: []int{1, 1, 2, 2, 2, 3, 3, 4, 4, 4}, out: []int{1, 1, 2, 2, 3, 3, 4, 4}, dups: []int{2, 4}, total: 10},
		{in: []int{1, 1, 2, 2, 2, 3, 3, 4, 4, 4, 5, 5}, out: []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}, dups: []int{2, 4}, total: 12},
		{in: []int{1, 1, 2, 2, 2, 3, 4, 4, 4, 5, 5, 5}, out: []int{1, 1, 2, 2, 3, 4, 4, 5, 5}, dups: []int{2, 4, 5}, total: 11},
		{in: []int{1, 1, 2, 2, 2, 3, 4, 4, 5, 5, 5, 6, 7, 8, 8, 9, 9}, out: []int{1, 1, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 8, 9, 9}, dups: []int{2, 5}, total: 14},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			require.True(t, slices.IsSorted(c.in))
			require.True(t, slices.IsSorted(c.out))
			require.True(t, slices.IsSorted(c.dups))
			require.Equal(t, len(c.dups), len(c.in)-len(c.out))
			tmpIn := make([]int, len(c.in))
			copy(tmpIn, c.in)
			out, dups, totalDup := RemoveDuplicatesMoreThanTwo(tmpIn, valGetter)
			require.EqualValues(t, c.out, out)
			require.EqualValues(t, c.dups, dups)
			require.Equal(t, c.total, totalDup)
		})
	}
}
