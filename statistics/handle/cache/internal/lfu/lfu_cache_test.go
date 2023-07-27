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
	"sync"
	"testing"

	"github.com/pingcap/tidb/statistics/handle/cache/internal/testutil"
	"github.com/stretchr/testify/require"
)

var (
	mockCMSMemoryUsage  = int64(4)
	mockTopNMemoryUsage = int64(64)
	mockHistMemoryUsage = int64(289)
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
	v, ok := lfu.Get(mockTableID, false)
	require.False(t, ok)
	require.Nil(t, v)
	lfu.wait()
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
	require.Equal(t, 0, len(lfu.Values()))
}

func TestLFUFreshMemUsage(t *testing.T) {
	lfu, err := NewLFU(1000)
	require.NoError(t, err)
	t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	t2 := testutil.NewMockStatisticsTable(2, 2, true, false, false)
	t3 := testutil.NewMockStatisticsTable(3, 3, true, false, false)
	lfu.Put(int64(1), t1)
	lfu.Put(int64(2), t2)
	lfu.Put(int64(3), t3)
	require.Equal(t, lfu.Cost(), 6*mockCMSMemoryUsage+6*mockCMSMemoryUsage)
	t4 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	lfu.Put(int64(1), t4)
	require.Equal(t, lfu.Cost(), 6*mockCMSMemoryUsage+7*mockCMSMemoryUsage)
	t5 := testutil.NewMockStatisticsTable(2, 2, true, false, false)
	lfu.Put(int64(1), t5)
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
	// put mockTable, the index should be evicted
	lfu.Put(int64(1), mockTable)
	_, ok := lfu.Get(int64(1), false)
	require.False(t, ok)
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
	require.Equal(t, lfu.Len(), 1)
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())

	// put t3, t1/t2 should be evicted all items and disappeared from the list
	t3 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	lfu.Put(int64(3), t3)
	lfu.wait()
	require.Equal(t, lfu.Len(), 1)
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
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
			lfu.Get(int64(i), true)
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
				lfu.Get(int64(n), true)
			}
		}()
	}
	wg.Wait()
	lfu.wait()
	require.Equal(t, uint64(lfu.Cost()), lfu.metrics().CostAdded()-lfu.metrics().CostEvicted())
	require.Equal(t, 1000, len(lfu.Values()))
}
