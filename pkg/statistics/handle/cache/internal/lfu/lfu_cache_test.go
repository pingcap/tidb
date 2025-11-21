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

package lfu

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/testutil"
	"github.com/stretchr/testify/require"
)

var (
	mockCMSMemoryUsage = int64(4)
)

func TestLFUPutGetDel(t *testing.T) {
	capacity := int64(100)
	lfu, err := NewLFU(capacity)
	require.NoError(t, err)
	mockTable := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	mockTableID := int64(1)
	lfu.Put(mockTableID, mockTable)
	lfu.wait()
	lfu.Del(mockTableID)
	v, ok := lfu.Get(mockTableID)
	require.False(t, ok)
	require.Nil(t, v)
	lfu.wait()
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
	require.Equal(t, 0, len(lfu.Values()))
}

func TestLFUFreshMemUsage(t *testing.T) {
	lfu, err := NewLFU(10000)
	require.NoError(t, err)
	t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	require.Equal(t, mockCMSMemoryUsage+mockCMSMemoryUsage, t1.MemoryUsage().TotalMemUsage)
	t2 := testutil.NewMockStatisticsTable(2, 2, true, false, false)
	require.Equal(t, 2*mockCMSMemoryUsage+2*mockCMSMemoryUsage, t2.MemoryUsage().TotalMemUsage)
	t3 := testutil.NewMockStatisticsTable(3, 3, true, false, false)
	require.Equal(t, 3*mockCMSMemoryUsage+3*mockCMSMemoryUsage, t3.MemoryUsage().TotalMemUsage)
	lfu.Put(int64(1), t1)
	lfu.Put(int64(2), t2)
	lfu.Put(int64(3), t3)
	lfu.wait()
	require.Equal(t, lfu.Cost(), 6*mockCMSMemoryUsage+6*mockCMSMemoryUsage)
	t4 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	lfu.Put(int64(1), t4)
	lfu.wait()
	require.Equal(t, lfu.Cost(), 7*mockCMSMemoryUsage+6*mockCMSMemoryUsage)
	t5 := testutil.NewMockStatisticsTable(2, 2, true, false, false)
	lfu.Put(int64(1), t5)
	lfu.wait()
	require.Equal(t, lfu.Cost(), 7*mockCMSMemoryUsage+7*mockCMSMemoryUsage)

	t6 := testutil.NewMockStatisticsTable(1, 2, true, false, false)
	lfu.Put(int64(1), t6)
	require.Equal(t, lfu.Cost(), 7*mockCMSMemoryUsage+6*mockCMSMemoryUsage)

	t7 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	lfu.Put(int64(1), t7)
	require.Equal(t, lfu.Cost(), 6*mockCMSMemoryUsage+6*mockCMSMemoryUsage)
	lfu.wait()
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
}

func TestLFUPutTooBig(t *testing.T) {
	lfu, err := NewLFU(1)
	require.NoError(t, err)
	mockTable := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	// put mockTable, the index should be evicted but the table still exists in the list.
	lfu.Put(int64(1), mockTable)
	_, ok := lfu.Get(int64(1))
	require.True(t, ok)
	lfu.wait()
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
}

func TestCacheLen(t *testing.T) {
	capacity := int64(12)
	lfu, err := NewLFU(capacity)
	require.NoError(t, err)
	t1 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	require.Equal(t, int64(12), t1.MemoryUsage().TotalTrackingMemUsage())
	lfu.Put(int64(1), t1)
	t2 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	// put t2, t1 should be evicted 2 items and still exists in the list
	lfu.Put(int64(2), t2)
	lfu.wait()
	require.Equal(t, lfu.Len(), 2)
	require.Equal(t, uint64(8), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())

	// put t3, t1/t2 should be evicted all items. but t1/t2 still exists in the list
	t3 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	lfu.Put(int64(3), t3)
	lfu.wait()
	require.Equal(t, lfu.Len(), 3)
	require.Equal(t, uint64(12), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
}

func TestLFUCachePutGetWithManyConcurrency(t *testing.T) {
	// to test DATA RACE
	capacity := int64(100000000000)
	lfu, err := NewLFU(capacity)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(2000)
	for i := range 1000 {
		go func(i int) {
			defer wg.Done()
			t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
			lfu.Put(int64(i), t1)
		}(i)
		go func(i int) {
			defer wg.Done()
			lfu.Get(int64(i))
		}(i)
	}
	wg.Wait()
	lfu.wait()
	require.Equal(t, lfu.Len(), 1000)
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
	require.Equal(t, 1000, len(lfu.Values()))
}

func TestLFUCachePutGetWithManyConcurrency2(t *testing.T) {
	// to test DATA RACE
	capacity := int64(100000000000)
	lfu, err := NewLFU(capacity)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(10)
	for range 5 {
		go func() {
			defer wg.Done()
			for n := range 1000 {
				t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
				lfu.Put(int64(n), t1)
			}
		}()
	}
	for range 5 {
		go func() {
			defer wg.Done()
			for n := range 1000 {
				lfu.Get(int64(n))
			}
		}()
	}
	wg.Wait()
	lfu.wait()
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
	require.Equal(t, 1000, len(lfu.Values()))
}

func TestLFUCachePutGetWithManyConcurrencyAndSmallConcurrency(t *testing.T) {
	// to test DATA RACE

	capacity := int64(100)
	lfu, err := NewLFU(capacity)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(10)
	for range 5 {
		go func() {
			defer wg.Done()
			for range 1000 {
				for n := range 50 {
					t1 := testutil.NewMockStatisticsTable(1, 1, true, true, true)
					lfu.Put(int64(n), t1)
				}
			}
		}()
	}
	time.Sleep(1 * time.Second)
	for range 5 {
		go func() {
			defer wg.Done()
			for range 1000 {
				for n := range 50 {
					tbl, ok := lfu.Get(int64(n))
					require.True(t, ok)
					checkTable(t, tbl)
				}
			}
		}()
	}
	wg.Wait()
	lfu.wait()
	v, ok := lfu.Get(rand.Int63n(50))
	require.True(t, ok)
	v.ForEachColumnImmutable(func(_ int64, c *statistics.Column) bool {
		require.Equal(t, c.GetEvictedStatus(), statistics.AllEvicted)
		return false
	})
	v.ForEachIndexImmutable(func(_ int64, i *statistics.Index) bool {
		require.Equal(t, i.GetEvictedStatus(), statistics.AllEvicted)
		return false
	})
}

func checkTable(t *testing.T, tbl *statistics.Table) {
	tbl.ForEachColumnImmutable(func(_ int64, column *statistics.Column) bool {
		if column.GetEvictedStatus() == statistics.AllEvicted {
			require.Nil(t, column.TopN)
			require.Equal(t, 0, cap(column.Histogram.Buckets))
		} else {
			require.NotNil(t, column.TopN)
			require.Greater(t, cap(column.Histogram.Buckets), 0)
		}
		return false
	})
	tbl.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
		if idx.GetEvictedStatus() == statistics.AllEvicted {
			require.Nil(t, idx.TopN)
			require.Equal(t, 0, cap(idx.Histogram.Buckets))
		} else {
			require.NotNil(t, idx.TopN)
			require.Greater(t, cap(idx.Histogram.Buckets), 0)
		}
		return false
	})
}

func TestLFUReject(t *testing.T) {
	capacity := int64(100000000000)
	lfu, err := NewLFU(capacity)
	require.NoError(t, err)
	t1 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	require.Equal(t, 2*mockCMSMemoryUsage+mockCMSMemoryUsage, t1.MemoryUsage().TotalTrackingMemUsage())
	lfu.Put(1, t1)
	lfu.wait()
	require.Equal(t, lfu.Cost(), 2*mockCMSMemoryUsage+mockCMSMemoryUsage)

	lfu.SetCapacity(2*mockCMSMemoryUsage + mockCMSMemoryUsage - 1)

	t2 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	require.True(t, lfu.Put(2, t2))
	lfu.wait()
	time.Sleep(3 * time.Second)
	require.Equal(t, int64(0), lfu.Cost())
	require.Len(t, lfu.Values(), 2)
	v, ok := lfu.Get(2)
	require.True(t, ok)
	v.ForEachColumnImmutable(func(_ int64, c *statistics.Column) bool {
		require.Equal(t, statistics.AllEvicted, c.GetEvictedStatus())
		return false
	})
	v.ForEachIndexImmutable(func(_ int64, i *statistics.Index) bool {
		require.Equal(t, statistics.AllEvicted, i.GetEvictedStatus())
		return false
	})
}

func TestMemoryControl(t *testing.T) {
	capacity := int64(100000000000)
	lfu, err := NewLFU(capacity)
	require.NoError(t, err)
	t1 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	require.Equal(t, 2*mockCMSMemoryUsage+mockCMSMemoryUsage, t1.MemoryUsage().TotalTrackingMemUsage())
	lfu.Put(1, t1)
	lfu.wait()

	for i := 2; i <= 1000; i++ {
		t1 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
		require.Equal(t, 2*mockCMSMemoryUsage+mockCMSMemoryUsage, t1.MemoryUsage().TotalTrackingMemUsage())
		lfu.Put(int64(i), t1)
	}
	require.Equal(t, 1000*(2*mockCMSMemoryUsage+mockCMSMemoryUsage), lfu.Cost())

	for i := 1000; i > 990; i-- {
		lfu.SetCapacity(int64(i-1) * (2*mockCMSMemoryUsage + mockCMSMemoryUsage))
		lfu.wait()
		require.Equal(t, int64(i-1)*(2*mockCMSMemoryUsage+mockCMSMemoryUsage), lfu.Cost())
	}
	for i := 990; i > 100; i = i - 100 {
		lfu.SetCapacity(int64(i-1) * (2*mockCMSMemoryUsage + mockCMSMemoryUsage))
		lfu.wait()
		require.Equal(t, int64(i-1)*(2*mockCMSMemoryUsage+mockCMSMemoryUsage), lfu.Cost())
	}
	lfu.SetCapacity(int64(10) * (2*mockCMSMemoryUsage + mockCMSMemoryUsage))
	lfu.wait()
	require.Equal(t, int64(10)*(2*mockCMSMemoryUsage+mockCMSMemoryUsage), lfu.Cost())
	lfu.SetCapacity(0)
	lfu.wait()
	require.Equal(t, int64(10)*(2*mockCMSMemoryUsage+mockCMSMemoryUsage), lfu.Cost())
}

func TestMemoryControlWithUpdate(t *testing.T) {
	capacity := int64(100)
	lfu, err := NewLFU(capacity)
	require.NoError(t, err)
	for i := range 100 {
		t1 := testutil.NewMockStatisticsTable(i, 1, true, false, false)
		lfu.Put(1, t1)
	}
	require.Eventually(t, func() bool {
		return int64(0) == lfu.Cost()
	}, 5*time.Second, 100*time.Millisecond)
}

// TestLFUSameObjectUpdate tests that when the same table object is modified
// and put again, the cost is correctly updated based on the stored cost,
// not the recalculated cost from the (already modified) object.
func TestLFUSameObjectUpdate(t *testing.T) {
	lfu, err := NewLFU(10000)
	require.NoError(t, err)

	// Create a table with 1 column and 1 index
	// Cost = (1 col + 1 idx) * mockCMSMemoryUsage = 2 * 4 = 8
	table := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	require.Equal(t, int64(8), table.MemoryUsage().TotalTrackingMemUsage())

	// First Put: should add full cost
	lfu.Put(1, table)
	lfu.wait()
	require.Equal(t, int64(8), lfu.Cost())

	// Modify the SAME table object by adding another column
	// This simulates updating table statistics in place
	table.SetCol(2, &statistics.Column{
		Info:              &model.ColumnInfo{ID: 2},
		CMSketch:          statistics.NewCMSketch(1, 1),
		StatsVer:          statistics.Version2,
		StatsLoadedStatus: statistics.NewStatsAllEvictedStatus(),
	})

	// Verify the table's memory usage has increased
	// Cost = (2 col + 1 idx) * mockCMSMemoryUsage = 3 * 4 = 12
	require.Equal(t, int64(12), table.MemoryUsage().TotalTrackingMemUsage())

	// Put the SAME object again with updated memory usage
	lfu.Put(1, table)
	lfu.wait()

	// Cost should be correctly updated to reflect the new memory usage
	// Expected: 12 (not 20 which would happen if we added 12 again)
	// The stored old cost (8) should be subtracted, and new cost (12) added
	// Net effect: 8 + 12 - 8 = 12
	require.Equal(t, int64(12), lfu.Cost())

	// Modify again by adding another column
	table.SetCol(3, &statistics.Column{
		Info:              &model.ColumnInfo{ID: 3},
		CMSketch:          statistics.NewCMSketch(1, 1),
		StatsVer:          statistics.Version2,
		StatsLoadedStatus: statistics.NewStatsAllEvictedStatus(),
	})

	// Cost = (3 col + 1 idx) * mockCMSMemoryUsage = 4 * 4 = 16
	require.Equal(t, int64(16), table.MemoryUsage().TotalTrackingMemUsage())

	// Put the same object a third time
	lfu.Put(1, table)
	lfu.wait()

	// Cost should be updated from 12 to 16
	require.Equal(t, int64(16), lfu.Cost())

	// Verify we can still get the table
	retrieved, ok := lfu.Get(1)
	require.True(t, ok)
	require.NotNil(t, retrieved)
	require.Equal(t, int64(16), retrieved.MemoryUsage().TotalTrackingMemUsage())
}

// TestLFUSameObjectEviction tests that when the same table object is evicted
// and then re-inserted, the cost tracking remains correct.
func TestLFUSameObjectEviction(t *testing.T) {
	// Create a small cache that can hold approximately 3 tables of cost 12 each
	// Cost per table: (1 col + 1 idx) * 4 = 8
	// Cache capacity: 30 (can hold ~3 tables)
	lfu, err := NewLFU(30)
	require.NoError(t, err)

	// Create and put the first table (this will be evicted later)
	table1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	require.Equal(t, int64(8), table1.MemoryUsage().TotalTrackingMemUsage())
	lfu.Put(1, table1)
	lfu.wait()
	require.Equal(t, int64(8), lfu.Cost())

	// Put more tables to fill the cache and trigger eviction of table1
	table2 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	table3 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	table4 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	table5 := testutil.NewMockStatisticsTable(1, 1, true, false, false)

	lfu.Put(2, table2)
	lfu.Put(3, table3)
	lfu.Put(4, table4)
	lfu.Put(5, table5)
	lfu.wait()

	// At this point, some tables should be evicted
	// Cost should be approximately within capacity (with some buffer for internal overhead)
	// The exact eviction behavior depends on ristretto's policy, but cost should be reasonable
	require.LessOrEqual(t, lfu.Cost(), int64(50)) // Allow some overhead

	// Now modify table1 (which may have been evicted) and re-insert it
	// This tests that evicted items can be updated and re-inserted correctly
	table1.SetCol(2, &statistics.Column{
		Info:              &model.ColumnInfo{ID: 2},
		CMSketch:          statistics.NewCMSketch(1, 1),
		StatsVer:          statistics.Version2,
		StatsLoadedStatus: statistics.NewStatsAllEvictedStatus(),
	})
	require.Equal(t, int64(12), table1.MemoryUsage().TotalTrackingMemUsage())

	// Get the current cost before re-inserting
	costBefore := lfu.Cost()

	// Re-insert the modified table1
	lfu.Put(1, table1)
	lfu.wait()

	// The cost change should reflect the delta of table1's memory usage
	// If table1 was evicted and stored in resultKeySet with its evicted cost,
	// the cost should increase by approximately (12 - evicted_cost)
	// If it wasn't evicted, cost should change by (12 - 8) = 4
	costAfter := lfu.Cost()
	costDelta := costAfter - costBefore

	// The cost delta should be reasonable (not duplicating the full cost)
	// It should be around 0-12 depending on eviction state
	require.GreaterOrEqual(t, costDelta, int64(-8)) // Allow some decrease if something was evicted
	require.LessOrEqual(t, costDelta, int64(12))    // But not more than the new table cost

	// Verify we can retrieve the updated table
	retrieved, ok := lfu.Get(1)
	require.True(t, ok)
	require.NotNil(t, retrieved)
	require.Equal(t, int64(12), retrieved.MemoryUsage().TotalTrackingMemUsage())

	// Verify cache consistency
	require.GreaterOrEqual(t, lfu.Cost(), int64(0))
}

// TestLFUEvictionCostTracking tests that cost tracking is correct during evictions.
func TestLFUEvictionCostTracking(t *testing.T) {
	// Create a cache with limited capacity
	// Each table costs (1 col + 1 idx) * 4 = 8
	lfu, err := NewLFU(50)
	require.NoError(t, err)

	// Track tables we create
	tables := make([]*statistics.Table, 10)
	for i := 0; i < 10; i++ {
		tables[i] = testutil.NewMockStatisticsTable(1, 1, true, false, false)
		lfu.Put(int64(i+1), tables[i])
	}
	lfu.wait()

	// Cost should be within capacity (some items will be evicted)
	initialCost := lfu.Cost()
	require.LessOrEqual(t, initialCost, int64(100)) // Allow some buffer

	// All tables should still be accessible via Get (either from cache or resultKeySet)
	for i := 0; i < 10; i++ {
		retrieved, ok := lfu.Get(int64(i + 1))
		require.True(t, ok, "table %d should be accessible", i+1)
		require.NotNil(t, retrieved)
	}

	// Now modify one of the tables and re-insert it
	tables[0].SetCol(2, &statistics.Column{
		Info:              &model.ColumnInfo{ID: 2},
		CMSketch:          statistics.NewCMSketch(1, 1),
		StatsVer:          statistics.Version2,
		StatsLoadedStatus: statistics.NewStatsAllEvictedStatus(),
	})

	costBefore := lfu.Cost()
	lfu.Put(1, tables[0])
	lfu.wait()
	costAfter := lfu.Cost()

	// Cost should change by a reasonable amount (the delta of the modified table)
	costDelta := costAfter - costBefore
	require.GreaterOrEqual(t, costDelta, int64(-10))
	require.LessOrEqual(t, costDelta, int64(15))

	// Verify the modified table is retrievable
	retrieved, ok := lfu.Get(1)
	require.True(t, ok)
	require.Equal(t, int64(12), retrieved.MemoryUsage().TotalTrackingMemUsage())

	// Final cost should still be reasonable
	require.GreaterOrEqual(t, lfu.Cost(), int64(0))
	require.LessOrEqual(t, lfu.Cost(), int64(150))
}
