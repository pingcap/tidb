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

package export

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func keys(ss ...string) []kv.Key {
	ks := make([]kv.Key, len(ss))
	for i, s := range ss {
		ks[i] = kv.Key(s)
	}
	return ks
}

func TestGroupBoundaries(t *testing.T) {
	b := keys("a", "b", "c", "d", "e") // 4 ranges

	// Two groups: contiguous, in key order, endpoints preserved, span-joined.
	groups := groupBoundaries(b, 2)
	require.Len(t, groups, 2)
	require.Equal(t, keys("a", "b", "c"), groups[0])
	require.Equal(t, keys("c", "d", "e"), groups[1])
	require.Equal(t, kv.Key("a"), groups[0][0])
	require.Equal(t, kv.Key("e"), groups[len(groups)-1][len(groups[len(groups)-1])-1])

	// Deterministic across calls.
	require.Equal(t, groups, groupBoundaries(b, 2))

	// One group covers the whole span.
	one := groupBoundaries(b, 1)
	require.Len(t, one, 1)
	require.Equal(t, b, one[0])

	// More groups than ranges: at most one range per group, none empty.
	many := groupBoundaries(b, 10)
	require.Len(t, many, 4)
	for _, g := range many {
		require.Len(t, g, 2)
	}
}

func TestSpanCntFor(t *testing.T) {
	// Explicit subtaskRegions overrides: ceil(regionCnt/batch).
	require.Equal(t, 5, spanCntFor(100, 4, 20))
	require.Equal(t, 1, spanCntFor(10, 4, 100))
	// Auto: batch = ceil(regionCnt/nodeCnt), spans = ceil(regionCnt/batch) ~ nodeCnt.
	require.Equal(t, 4, spanCntFor(100, 4, 0))
	// Never zero.
	require.Equal(t, 1, spanCntFor(0, 0, 0))
	require.GreaterOrEqual(t, spanCntFor(1, 1, 0), 1)
	// Capped by maxRegionsPerSubtask: huge table on one node still splits.
	require.GreaterOrEqual(t, spanCntFor(10*maxRegionsPerSubtask, 1, 0), 10)
}

func TestSpansToUnits(t *testing.T) {
	g0 := [][]kv.Key{keys("a", "c"), keys("c", "z")} // 2 spans of table part pid=100
	units, next := spansToUnits(2, 100, g0, 0)
	require.Len(t, units, 2)
	require.Equal(t, Unit{TableIdx: 2, PhysicalID: 100, Start: []byte("a"), End: []byte("c"), NameOrdinal: 0}, units[0])
	require.Equal(t, Unit{TableIdx: 2, PhysicalID: 100, Start: []byte("c"), End: []byte("z"), NameOrdinal: 1}, units[1])
	require.Equal(t, 2, next)

	// A second partition of the same table continues the table-local ordinal,
	// so file names never collide across partitions of one table.
	g1 := [][]kv.Key{keys("a", "z")}
	units2, next2 := spansToUnits(2, 200, g1, next)
	require.Equal(t, 2, units2[0].NameOrdinal)
	require.Equal(t, int64(200), units2[0].PhysicalID)
	require.Equal(t, 3, next2)
}
