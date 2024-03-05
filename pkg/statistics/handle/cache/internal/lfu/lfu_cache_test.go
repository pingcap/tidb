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
	for i := 0; i < 1000; i++ {
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
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			for n := 0; n < 1000; n++ {
				t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
				lfu.Put(int64(n), t1)
			}
		}()
	}
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			for n := 0; n < 1000; n++ {
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
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			for c := 0; c < 1000; c++ {
				for n := 0; n < 50; n++ {
					t1 := testutil.NewMockStatisticsTable(1, 1, true, true, true)
					lfu.Put(int64(n), t1)
				}
			}
		}()
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			for c := 0; c < 1000; c++ {
				for n := 0; n < 50; n++ {
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
	for _, c := range v.Columns {
		require.Equal(t, c.GetEvictedStatus(), statistics.AllEvicted)
	}
	for _, i := range v.Indices {
		require.Equal(t, i.GetEvictedStatus(), statistics.AllEvicted)
	}
}

func checkTable(t *testing.T, tbl *statistics.Table) {
	for _, column := range tbl.Columns {
		if column.GetEvictedStatus() == statistics.AllEvicted {
			require.Nil(t, column.TopN)
			require.Equal(t, 0, cap(column.Histogram.Buckets))
		} else {
			require.NotNil(t, column.TopN)
			require.Greater(t, cap(column.Histogram.Buckets), 0)
		}
	}
	for _, idx := range tbl.Indices {
		if idx.GetEvictedStatus() == statistics.AllEvicted {
			require.Nil(t, idx.TopN)
			require.Equal(t, 0, cap(idx.Histogram.Buckets))
		} else {
			require.NotNil(t, idx.TopN)
			require.Greater(t, cap(idx.Histogram.Buckets), 0)
		}
	}
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
	for _, c := range v.Columns {
		require.Equal(t, statistics.AllEvicted, c.GetEvictedStatus())
	}
	for _, i := range v.Indices {
		require.Equal(t, statistics.AllEvicted, i.GetEvictedStatus())
	}
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
