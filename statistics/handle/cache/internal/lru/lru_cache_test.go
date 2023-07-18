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

package lru

import (
	"testing"

	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/cache/internal/testutil"
	"github.com/stretchr/testify/require"
)

var (
	mockCMSMemoryUsage  = int64(4)
	mockTopNMemoryUsage = int64(64)
	mockHistMemoryUsage = int64(289)
)

func TestLRUPutGetDel(t *testing.T) {
	capacity := int64(100)
	lru := NewStatsLruCache(capacity)
	require.Equal(t, capacity, lru.capacity())
	mockTable := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	mockTableID := int64(1)
	lru.Put(mockTableID, mockTable, false)
	v, ok := lru.Get(mockTableID, false)
	require.True(t, ok)
	require.Equal(t, mockTable, v)
	lru.Del(mockTableID)
	v, ok = lru.Get(mockTableID, false)
	require.False(t, ok)
	require.Nil(t, v)

	// assert byQuery api
	lru.Put(mockTableID, mockTable, true)
	v, ok = lru.Get(mockTableID, true)
	require.True(t, ok)
	require.Equal(t, mockTable, v)
	lru.Del(mockTableID)
	v, ok = lru.Get(mockTableID, false)
	require.False(t, ok)
	require.Nil(t, v)
}

func TestLRUEvict(t *testing.T) {
	capacity := int64(24)
	lru := NewStatsLruCache(capacity)
	t1 := testutil.NewMockStatisticsTable(2, 0, true, false, false)
	require.Equal(t, t1.MemoryUsage().TotalIdxTrackingMemUsage(), int64(0))
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), 2*mockCMSMemoryUsage)

	// Put t1, assert TotalMemUsage and TotalColTrackingMemUsage
	lru.Put(int64(1), t1, false)
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), t1.MemoryUsage().TotalTrackingMemUsage())

	// Put t2, assert TotalMemUsage and TotalColTrackingMemUsage
	t2 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	lru.Put(int64(2), t2, false)
	require.Equal(t, lru.Cost(), 4*mockCMSMemoryUsage+1*mockCMSMemoryUsage)

	// Put t3, a column of t1 should be evicted
	t3 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	lru.Put(int64(3), t3, false)

	require.Equal(t, lru.Len(), 3)
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), mockCMSMemoryUsage)
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage+t2.MemoryUsage().TotalMemUsage+t3.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), 4*mockCMSMemoryUsage+2*mockCMSMemoryUsage)

	// Put t4, all indices' cmsketch of other tables should be evicted
	t4 := testutil.NewMockStatisticsTable(3, 3, true, false, false)
	lru.Put(int64(4), t4, false)

	require.Equal(t, lru.Len(), 4)
	require.Equal(t, t1.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, t2.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, t3.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage+
		t2.MemoryUsage().TotalMemUsage+
		t3.MemoryUsage().TotalMemUsage+
		t4.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), 3*mockCMSMemoryUsage+3*mockCMSMemoryUsage)
}

func TestLRUCopy(t *testing.T) {
	lru := NewStatsLruCache(1000)
	tables := make([]*statistics.Table, 0)
	for i := 0; i < 5; i++ {
		tables = append(tables, testutil.NewMockStatisticsTable(1, 1, true, false, false))
	}

	// insert 1,2,3 into old lru
	for i := 0; i < 3; i++ {
		mockTable := tables[i]
		key := int64(i)
		lru.Put(key, mockTable, false)
		value, ok := lru.Get(key, false)
		require.True(t, ok)
		require.Equal(t, value, mockTable)
	}
	newLRU := lru.Copy()
	// assert new lru has same elements as old lru
	for i := 0; i < 3; i++ {
		key := int64(i)
		value, ok := newLRU.Get(key, false)
		require.True(t, ok)
		require.EqualValues(t, value, tables[i])
	}
	// delete new lru element
	newLRU.Del(int64(1))
	_, ok := newLRU.Get(int64(1), false)
	require.False(t, ok)

	// old lru has no affect
	for i := 0; i < 3; i++ {
		key := int64(i)
		value, ok := lru.Get(key, false)
		require.True(t, ok)
		require.Equal(t, value, tables[i])
	}
}

func TestLRUFreshMemUsage(t *testing.T) {
	lru := NewStatsLruCache(1000)
	t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	t2 := testutil.NewMockStatisticsTable(2, 2, true, false, false)
	t3 := testutil.NewMockStatisticsTable(3, 3, true, false, false)
	lru.Put(int64(1), t1, false)
	lru.Put(int64(2), t2, false)
	lru.Put(int64(3), t3, false)
	require.Equal(t, lru.Cost(), 6*mockCMSMemoryUsage+6*mockCMSMemoryUsage)
	testutil.MockTableAppendColumn(t1)
	lru.Put(int64(1), t1, false)
	require.Equal(t, lru.Cost(), 6*mockCMSMemoryUsage+7*mockCMSMemoryUsage)
	testutil.MockTableAppendIndex(t1)
	lru.Put(int64(1), t1, false)
	require.Equal(t, lru.Cost(), 7*mockCMSMemoryUsage+7*mockCMSMemoryUsage)

	testutil.MockTableRemoveColumn(t1)
	lru.Put(int64(1), t1, false)
	require.Equal(t, lru.Cost(), 7*mockCMSMemoryUsage+6*mockCMSMemoryUsage)

	testutil.MockTableRemoveIndex(t1)
	lru.Put(int64(1), t1, false)
	require.Equal(t, lru.Cost(), 6*mockCMSMemoryUsage+6*mockCMSMemoryUsage)
}

func TestLRUPutTooBig(t *testing.T) {
	lru := NewStatsLruCache(1)
	mockTable := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	// put mockTable, the index should be evicted
	lru.Put(int64(1), mockTable, false)
	_, ok := lru.Get(int64(1), false)
	require.True(t, ok)
	require.Equal(t, lru.Cost(), int64(0))
	require.Equal(t, mockTable.MemoryUsage().TotalTrackingMemUsage(), int64(0))
}

func TestCacheLen(t *testing.T) {
	capacity := int64(12)
	stats := NewStatsLruCache(capacity)

	t1 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	stats.Put(int64(1), t1, false)
	t2 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	// put t2, t1 should be evicted 2 items and still exists in the list
	stats.Put(int64(2), t2, false)
	require.Equal(t, stats.lru.cache.Len(), 3)
	require.Equal(t, t1.MemoryUsage().TotalTrackingMemUsage(), int64(4))
	require.Equal(t, stats.Len(), 2)

	// put t3, t1/t2 should be evicted all items and disappeared from the list
	t3 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	stats.Put(int64(3), t3, false)

	require.Equal(t, stats.lru.cache.Len(), 3)
	require.Equal(t, t1.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, t2.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, stats.Len(), 3)
}

func TestLRUMove(t *testing.T) {
	capacity := int64(100)
	s := NewStatsLruCache(capacity)
	t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	t1ID := int64(1)
	t2 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	t2ID := int64(2)
	s.Put(t1ID, t1, false)
	s.Put(t2ID, t2, false)
	// assert t2 element should be front element
	front := s.lru.cache.Front().Value.(*lruCacheItem)
	require.Equal(t, t2ID, front.tblID)
	// assert t1 element should be front element after GetByQuery
	s.Get(t1ID, true)
	front = s.lru.cache.Front().Value.(*lruCacheItem)
	require.Equal(t, t1ID, front.tblID)
}

func TestLRUEvictPolicy(t *testing.T) {
	capacity := int64(999)
	s := NewStatsLruCache(capacity)
	t1 := testutil.NewMockStatisticsTable(1, 0, true, true, true)
	s.Put(1, t1, false)
	require.Equal(t, s.TotalCost(), mockCMSMemoryUsage+mockTopNMemoryUsage+mockHistMemoryUsage)
	require.Equal(t, s.Cost(), mockCMSMemoryUsage+mockTopNMemoryUsage+mockHistMemoryUsage)
	cost := s.Cost()
	// assert column's cms got evicted and topn remained
	s.SetCapacity(cost - 1)
	require.Equal(t, s.Cost(), mockTopNMemoryUsage+mockHistMemoryUsage)
	require.Nil(t, t1.Columns[1].CMSketch)
	require.True(t, t1.Columns[1].IsCMSEvicted())
	require.NotNil(t, t1.Columns[1].TopN)
	require.False(t, t1.Columns[1].IsTopNEvicted())
	require.False(t, t1.Columns[1].IsAllEvicted())
	// assert both column's cms and topn got evicted, hist remained
	cost = s.Cost()
	s.SetCapacity(cost - 1)
	require.Equal(t, s.Cost(), mockHistMemoryUsage)
	require.Nil(t, t1.Columns[1].CMSketch)
	require.True(t, t1.Columns[1].IsCMSEvicted())
	require.Nil(t, t1.Columns[1].TopN)
	require.True(t, t1.Columns[1].IsTopNEvicted())
	require.False(t, t1.Columns[1].IsAllEvicted())

	// assert all stats got evicted
	s.SetCapacity(1)
	require.Equal(t, s.Cost(), int64(0))
	require.Nil(t, t1.Columns[1].CMSketch)
	require.True(t, t1.Columns[1].IsCMSEvicted())
	require.Nil(t, t1.Columns[1].TopN)
	require.True(t, t1.Columns[1].IsTopNEvicted())
	require.True(t, t1.Columns[1].IsAllEvicted())

	s = NewStatsLruCache(capacity)
	t2 := testutil.NewMockStatisticsTable(0, 1, true, true, true)
	s.Put(2, t2, false)
	require.Equal(t, s.TotalCost(), mockCMSMemoryUsage+mockTopNMemoryUsage+mockHistMemoryUsage)
	require.Equal(t, s.Cost(), mockCMSMemoryUsage+mockTopNMemoryUsage+mockHistMemoryUsage)
	cost = s.Cost()
	// assert index's cms got evicted and topn remained
	s.SetCapacity(cost - 1)
	require.Equal(t, s.Cost(), mockTopNMemoryUsage+mockHistMemoryUsage)
	require.Nil(t, t2.Indices[1].CMSketch)
	require.NotNil(t, t2.Indices[1].TopN)
	require.False(t, t2.Indices[1].IsTopNEvicted())
	require.False(t, t2.Indices[1].IsAllEvicted())

	// assert both index's cms and topn got evicted, hist remained
	cost = s.Cost()
	s.SetCapacity(cost - 1)
	require.Equal(t, s.Cost(), mockHistMemoryUsage)
	require.Nil(t, t2.Indices[1].CMSketch)
	require.True(t, t2.Indices[1].IsCMSEvicted())
	require.Nil(t, t2.Indices[1].TopN)
	require.True(t, t2.Indices[1].IsTopNEvicted())
	require.False(t, t2.Indices[1].IsAllEvicted())

	// assert all stats got evicted
	s.SetCapacity(1)
	require.Equal(t, s.Cost(), int64(0))
	require.Nil(t, t2.Indices[1].CMSketch)
	require.True(t, t2.Indices[1].IsCMSEvicted())
	require.Nil(t, t2.Indices[1].TopN)
	require.True(t, t2.Indices[1].IsTopNEvicted())
	require.True(t, t2.Indices[1].IsAllEvicted())
}
