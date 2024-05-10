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
	require.Len(t, a.Columns, len(b.Columns))
	for i := range a.Columns {
		require.True(t, statistics.HistogramEqual(&a.Columns[i].Histogram, &b.Columns[i].Histogram, false))
		if a.Columns[i].CMSketch == nil {
			require.Nil(t, b.Columns[i].CMSketch)
		} else {
			require.True(t, a.Columns[i].CMSketch.Equal(b.Columns[i].CMSketch))
		}
		// The nil case has been considered in (*TopN).Equal() so we don't need to consider it here.
		require.Truef(t, a.Columns[i].TopN.Equal(b.Columns[i].TopN), "%v, %v", a.Columns[i].TopN, b.Columns[i].TopN)
	}
	require.Len(t, a.Indices, len(b.Indices))
	for i := range a.Indices {
		require.True(t, statistics.HistogramEqual(&a.Indices[i].Histogram, &b.Indices[i].Histogram, false))
		if a.Indices[i].CMSketch == nil {
			require.Nil(t, b.Indices[i].CMSketch)
		} else {
			require.True(t, a.Indices[i].CMSketch.Equal(b.Indices[i].CMSketch))
		}
		require.True(t, a.Indices[i].TopN.Equal(b.Indices[i].TopN))
	}
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
