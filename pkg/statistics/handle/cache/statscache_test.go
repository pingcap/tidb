// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a Copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"testing"

	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal"
	handle_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	promtestutils "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestCacheOfBatchUpdate(t *testing.T) {
	markedAsUpdated := make([]int64, 0)
	markedAsDeleted := make([]int64, 0)
	testBatchSize := 3
	cached := newCacheOfBatchUpdate(testBatchSize, func(toUpdate []*statistics.Table, toDelete []int64) {
		for _, table := range toUpdate {
			markedAsUpdated = append(markedAsUpdated, table.PhysicalID)
		}
		markedAsDeleted = append(markedAsDeleted, toDelete...)
	})

	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 1}})
	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 2}})
	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 3}})
	require.Len(t, markedAsUpdated, 0)
	require.Len(t, cached.toUpdate, 3)
	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 4}})
	require.Len(t, markedAsUpdated, 3)
	require.Equal(t, int64(1), markedAsUpdated[0])
	require.Equal(t, int64(2), markedAsUpdated[1])
	require.Equal(t, int64(3), markedAsUpdated[2])
	require.Len(t, cached.toUpdate, 1)
	require.Equal(t, int64(4), cached.toUpdate[0].PhysicalID)

	cached.addToDelete(5)
	cached.addToDelete(6)
	cached.addToDelete(7)
	require.Len(t, markedAsDeleted, 0)
	require.Len(t, cached.toDelete, 3)
	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 8}})
	require.Len(t, cached.toUpdate, 2)
	cached.addToDelete(9)
	require.Len(t, markedAsDeleted, 3)
	require.Equal(t, int64(5), markedAsDeleted[0])
	require.Equal(t, int64(6), markedAsDeleted[1])
	require.Equal(t, int64(7), markedAsDeleted[2])
	require.Len(t, cached.toDelete, 1)
	require.Equal(t, int64(9), cached.toDelete[0])
	require.Len(t, markedAsUpdated, 5)
	require.Equal(t, int64(4), markedAsUpdated[3])
	require.Equal(t, int64(8), markedAsUpdated[4])

	cached.flush()
	require.Len(t, cached.toUpdate, 0)
	require.Len(t, cached.toDelete, 0)
	require.Len(t, markedAsDeleted, 4)
	require.Equal(t, int64(9), markedAsDeleted[3])
}

func TestUpdateStatsHealthyMetrics(t *testing.T) {
	resetHealthyGauges()
	defer resetHealthyGauges()

	fakeAnalyzeVersion := uint64(3959837493728947298)
	tableHasnotAnalyzed := newMockTable(t, 0, false, 2000, 1000, 0)              // never analyzed -> bucket [0,50)
	tableLowHealthy := newMockTable(t, 1, false, 2000, 1100, fakeAnalyzeVersion) // healthy = 44 -> bucket [0,50)
	tableMidHealthy := newMockTable(t, 2, false, 2000, 920, fakeAnalyzeVersion)  // healthy = 54 -> bucket [50,55)
	tableHighHealthy := newMockTable(t, 3, false, 2000, 200, fakeAnalyzeVersion) // healthy = 90 -> bucket [80,100)
	tablePerfect := newMockTable(t, 4, false, 2000, 0, fakeAnalyzeVersion)       // healthy = 100 -> bucket [100,100]
	tablePseudo := newMockTable(t, 5, true, 10000, 0, 0)                         // pseudo -> pseudo
	// NOTE: Tables with fewer than 1,000 rows that have been analyzed should still fall into the correct bucket.
	tableSmallTable := newMockTable(t, 6, false, 800, 500, fakeAnalyzeVersion) // healthy = 37.5 -> bucket [0,50)
	tableHasnotAnalyzedAndSmall := newMockTable(t, 7, false, 800, 500, 0)      // never analyzed and small table -> unneeded analyze

	cacheImpl := &StatsCacheImpl{}
	cacheImpl.Store(&StatsCache{
		c: newMockStatsCacheInner(tableHasnotAnalyzed, tableLowHealthy, tableMidHealthy, tableHighHealthy, tablePerfect, tablePseudo, tableSmallTable, tableHasnotAnalyzedAndSmall),
	})

	cacheImpl.UpdateStatsHealthyMetrics()

	require.Len(t, handle_metrics.StatsHealthyGauges, handle_metrics.StatsHealthyBucketCount)
	expected := []struct {
		label string
		value float64
	}{
		{label: "[0,50)", value: 3},
		{label: "[50,55)", value: 1},
		{label: "[55,60)", value: 0},
		{label: "[60,70)", value: 0},
		{label: "[70,80)", value: 0},
		{label: "[80,100)", value: 1},
		{label: "[100,100]", value: 1},
		{label: "[0,100]", value: 8},
		{label: "unneeded analyze", value: 1},
		{label: "pseudo", value: 1},
	}
	for idx, gauge := range handle_metrics.StatsHealthyGauges {
		cfg := handle_metrics.HealthyBucketConfigs[idx]
		require.Equal(t, expected[idx].label, cfg.Label)
		require.Equal(t, expected[idx].value, promtestutils.ToFloat64(gauge))
	}
}

func newMockTable(t *testing.T, physicalID int64, pseudo bool, realtimeCount, modifyCount int64, analyzeVersion uint64) *statistics.Table {
	hist := statistics.NewHistColl(physicalID, realtimeCount, modifyCount, 0, 0)
	table := &statistics.Table{
		HistColl:              *hist,
		ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMapWithoutSize(),
		LastAnalyzeVersion:    analyzeVersion,
	}
	table.Pseudo = pseudo
	return table
}

func resetHealthyGauges() {
	for _, gauge := range handle_metrics.StatsHealthyGauges {
		gauge.Set(0)
	}
}

type mockStatsCacheInner struct {
	items map[int64]*statistics.Table
}

func newMockStatsCacheInner(tables ...*statistics.Table) internal.StatsCacheInner {
	m := &mockStatsCacheInner{
		items: make(map[int64]*statistics.Table, len(tables)),
	}
	for _, tbl := range tables {
		if tbl == nil {
			continue
		}
		id := tbl.PhysicalID
		m.items[id] = tbl
	}
	return m
}

func (m *mockStatsCacheInner) Values() []*statistics.Table {
	values := make([]*statistics.Table, 0, len(m.items))
	for _, tbl := range m.items {
		values = append(values, tbl)
	}
	return values
}

func (m *mockStatsCacheInner) Get(id int64) (*statistics.Table, bool) {
	panic("not implemented")
}

func (m *mockStatsCacheInner) Put(id int64, tbl *statistics.Table) bool {
	panic("not implemented")
}

func (m *mockStatsCacheInner) Del(id int64) {
	panic("not implemented")
}

func (m *mockStatsCacheInner) Cost() int64 {
	panic("not implemented")
}

func (m *mockStatsCacheInner) Len() int {
	panic("not implemented")
}

func (m *mockStatsCacheInner) Copy() internal.StatsCacheInner {
	panic("not implemented")
}

func (*mockStatsCacheInner) SetCapacity(int64) {
	panic("not implemented")
}

func (*mockStatsCacheInner) Close() {
	panic("not implemented")
}

func (*mockStatsCacheInner) TriggerEvict() {
	panic("not implemented")
}
