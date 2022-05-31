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

package copr

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
)

func TestCopRanges(t *testing.T) {
	ranges := []kv.KeyRange{
		{StartKey: []byte("a"), EndKey: []byte("b")},
		{StartKey: []byte("c"), EndKey: []byte("d")},
		{StartKey: []byte("e"), EndKey: []byte("f")},
	}

	checkEqual(t, &KeyRanges{mid: ranges}, ranges, true)
	checkEqual(t, &KeyRanges{first: &ranges[0], mid: ranges[1:]}, ranges, true)
	checkEqual(t, &KeyRanges{mid: ranges[:2], last: &ranges[2]}, ranges, true)
	checkEqual(t, &KeyRanges{first: &ranges[0], mid: ranges[1:2], last: &ranges[2]}, ranges, true)
}

func TestCopRangeSplit(t *testing.T) {
	first := &kv.KeyRange{StartKey: []byte("a"), EndKey: []byte("b")}
	mid := []kv.KeyRange{
		{StartKey: []byte("c"), EndKey: []byte("d")},
		{StartKey: []byte("e"), EndKey: []byte("g")},
		{StartKey: []byte("l"), EndKey: []byte("o")},
	}
	last := &kv.KeyRange{StartKey: []byte("q"), EndKey: []byte("t")}
	const left = true
	const right = false

	// input range:  [c-d) [e-g) [l-o)
	ranges := &KeyRanges{mid: mid}
	testSplit(t, ranges, right,
		splitCase{"c", buildCopRanges("c", "d", "e", "g", "l", "o")},
		splitCase{"d", buildCopRanges("e", "g", "l", "o")},
		splitCase{"f", buildCopRanges("f", "g", "l", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o)
	ranges = &KeyRanges{first: first, mid: mid}
	testSplit(t, ranges, right,
		splitCase{"a", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"c", buildCopRanges("c", "d", "e", "g", "l", "o")},
		splitCase{"m", buildCopRanges("m", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o) [q-t)
	ranges = &KeyRanges{first: first, mid: mid, last: last}
	testSplit(t, ranges, right,
		splitCase{"f", buildCopRanges("f", "g", "l", "o", "q", "t")},
		splitCase{"h", buildCopRanges("l", "o", "q", "t")},
		splitCase{"r", buildCopRanges("r", "t")},
	)

	// input range:  [c-d) [e-g) [l-o)
	ranges = &KeyRanges{mid: mid}
	testSplit(t, ranges, left,
		splitCase{"m", buildCopRanges("c", "d", "e", "g", "l", "m")},
		splitCase{"g", buildCopRanges("c", "d", "e", "g")},
		splitCase{"g", buildCopRanges("c", "d", "e", "g")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o)
	ranges = &KeyRanges{first: first, mid: mid}
	testSplit(t, ranges, left,
		splitCase{"d", buildCopRanges("a", "b", "c", "d")},
		splitCase{"d", buildCopRanges("a", "b", "c", "d")},
		splitCase{"o", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o) [q-t)
	ranges = &KeyRanges{first: first, mid: mid, last: last}
	testSplit(t, ranges, left,
		splitCase{"o", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"p", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"t", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o", "q", "t")},
	)
}

func checkEqual(t *testing.T, copRanges *KeyRanges, ranges []kv.KeyRange, slice bool) {
	require.Equal(t, copRanges.Len(), len(ranges))
	for i := range ranges {
		require.EqualValues(t, copRanges.At(i), ranges[i])
	}
	if slice {
		for i := 0; i <= copRanges.Len(); i++ {
			for j := i; j <= copRanges.Len(); j++ {
				checkEqual(t, copRanges.Slice(i, j), ranges[i:j], false)
			}
		}
	}
}

type splitCase struct {
	key string
	*KeyRanges
}

func testSplit(t *testing.T, ranges *KeyRanges, checkLeft bool, cases ...splitCase) {
	for _, tt := range cases {
		left, right := ranges.Split([]byte(tt.key))
		expect := tt.KeyRanges
		if checkLeft {
			checkEqual(t, left, expect.mid, false)
		} else {
			checkEqual(t, right, expect.mid, false)
		}
	}
}
