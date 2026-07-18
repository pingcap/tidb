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

package cardinality

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestBuildColEstimateCacheKeyRangesLimit(t *testing.T) {
	pointRange := func(v types.Datum) *ranger.Range {
		return &ranger.Range{LowVal: []types.Datum{v}, HighVal: []types.Datum{v}}
	}

	// Typical predicate: key is built and carries the identifying fields.
	small := []*ranger.Range{pointRange(types.NewIntDatum(1)), pointRange(types.NewIntDatum(2))}
	key, ok := buildColEstimateCacheKey(10, 20, true, small, 100, 5)
	require.True(t, ok)
	require.Equal(t, int64(10), key.physicalID)
	require.Equal(t, int64(20), key.colInfoID)
	require.True(t, key.pkIsHandle)
	require.Equal(t, int64(100), key.realtimeCount)
	require.Equal(t, int64(5), key.modifyCount)
	require.NotEmpty(t, key.rangesKey)

	// Range-count fast path: more ranges than can fit under the byte limit
	// (each serializes to at least 4 bytes) must bail before serializing.
	manyRanges := make([]*ranger.Range, colEstimateCacheRangesKeyLimit/4+1)
	for i := range manyRanges {
		manyRanges[i] = pointRange(types.NewIntDatum(int64(i)))
	}
	_, ok = buildColEstimateCacheKey(10, 20, false, manyRanges, 100, 5)
	require.False(t, ok)

	// Byte-limit path: few ranges, but oversized serialized values must also
	// bail so a pathological statement cannot retain arbitrary cache memory.
	hugeVal := types.NewStringDatum(strings.Repeat("x", colEstimateCacheRangesKeyLimit+1))
	_, ok = buildColEstimateCacheKey(10, 20, false, []*ranger.Range{pointRange(hugeVal)}, 100, 5)
	require.False(t, ok)
}
