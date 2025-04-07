// Copyright 2023 PingCAP, Inc.
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

package internal

import (
	"testing"

	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/stretchr/testify/require"
)

// AssertTableEqual is to assert whether two table is equal
func AssertTableEqual(t *testing.T, a *statistics.Table, b *statistics.Table) {
	require.Equal(t, b.RealtimeCount, a.RealtimeCount)
	require.Equal(t, b.ModifyCount, a.ModifyCount)
	require.Equal(t, a.ColNum(), b.ColNum())
	a.ForEachColumnImmutable(func(id int64, col *statistics.Column) bool {
		bCol := b.GetCol(id)
		require.NotNil(t, bCol)
		require.True(t, statistics.HistogramEqual(&col.Histogram, &bCol.Histogram, false))
		if col.CMSketch == nil {
			require.Nil(t, bCol.CMSketch)
		} else {
			require.True(t, col.CMSketch.Equal(bCol.CMSketch))
		}
		// The nil case has been considered in (*TopN).Equal() so we don't need to consider it here.
		require.Truef(t, col.TopN.Equal(bCol.TopN), "%v, %v", col.TopN, bCol.TopN)
		return false
	})
	require.Equal(t, a.IdxNum(), b.IdxNum())
	a.ForEachIndexImmutable(func(id int64, idx *statistics.Index) bool {
		bIdx := b.GetIdx(id)
		require.NotNil(t, bIdx)
		require.True(t, statistics.HistogramEqual(&idx.Histogram, &bIdx.Histogram, false))
		if idx.CMSketch == nil {
			require.Nil(t, bIdx.CMSketch)
		} else {
			require.True(t, idx.CMSketch.Equal(bIdx.CMSketch))
		}
		require.True(t, idx.TopN.Equal(bIdx.TopN))
		return false
	})
	require.True(t, IsSameExtendedStats(a.ExtendedStats, b.ExtendedStats))
	require.True(t, statistics.ColAndIdxExistenceMapIsEqual(a.ColAndIdxExistenceMap, b.ColAndIdxExistenceMap))
}

// IsSameExtendedStats is to judge whether the extended states is the same.
func IsSameExtendedStats(a, b *statistics.ExtendedStatsColl) bool {
	aEmpty := (a == nil) || len(a.Stats) == 0
	bEmpty := (b == nil) || len(b.Stats) == 0
	if (aEmpty && !bEmpty) || (!aEmpty && bEmpty) {
		return false
	}
	if aEmpty && bEmpty {
		return true
	}
	if len(a.Stats) != len(b.Stats) {
		return false
	}
	for aKey, aItem := range a.Stats {
		bItem, ok := b.Stats[aKey]
		if !ok {
			return false
		}
		for i, id := range aItem.ColIDs {
			if id != bItem.ColIDs[i] {
				return false
			}
		}
		if (aItem.Tp != bItem.Tp) || (aItem.ScalarVals != bItem.ScalarVals) || (aItem.StringVals != bItem.StringVals) {
			return false
		}
	}
	return true
}
