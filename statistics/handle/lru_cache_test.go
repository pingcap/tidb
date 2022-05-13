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

package handle

import (
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/stretchr/testify/require"
)

var (
	indexMemoryUsage             = int64(4)
	columnTotalMemoryUsage       = statistics.EmptyHistogramSize + 4
	indexTotalMemoryUsage        = statistics.EmptyHistogramSize + 4
	indexEvictedTotalMemoryUsage = statistics.EmptyHistogramSize
)

// each column and index consumes 4 bytes memory
func newMockStatisticsTable(columns int, indices int) *statistics.Table {
	t := &statistics.Table{}
	t.Columns = make(map[int64]*statistics.Column)
	t.Indices = make(map[int64]*statistics.Index)
	for i := 1; i <= columns; i++ {
		t.Columns[int64(i)] = &statistics.Column{
			Info:     &model.ColumnInfo{ID: int64(i)},
			CMSketch: statistics.NewCMSketch(1, 1),
		}
	}
	for i := 1; i <= indices; i++ {
		t.Indices[int64(i)] = &statistics.Index{
			Info:     &model.IndexInfo{ID: int64(i)},
			CMSketch: statistics.NewCMSketch(1, 1),
		}
	}
	return t
}

func mockTableAppendColumn(t *statistics.Table) {
	index := int64(len(t.Columns) + 1)
	t.Columns[index] = &statistics.Column{
		Info:     &model.ColumnInfo{ID: index},
		CMSketch: statistics.NewCMSketch(1, 1),
	}
}

func mockTableAppendIndex(t *statistics.Table) {
	index := int64(len(t.Columns) + 1)
	t.Indices[index] = &statistics.Index{
		Info:     &model.IndexInfo{ID: index},
		CMSketch: statistics.NewCMSketch(1, 1),
	}
}

func TestLRUPutGetDel(t *testing.T) {
	capacity := int64(100)
	lru := newStatsLruCache(capacity)
	require.Equal(t, capacity, lru.capacity())
	mockTable := newMockStatisticsTable(1, 1)
	mockTableID := int64(1)
	lru.Put(mockTableID, mockTable)
	v, ok := lru.Get(mockTableID)
	require.True(t, ok)
	require.Equal(t, mockTable, v)
	lru.Del(mockTableID)
	v, ok = lru.Get(mockTableID)
	require.False(t, ok)
	require.Nil(t, v)

	// assert byQuery api
	lru.PutByQuery(mockTableID, mockTable)
	v, ok = lru.GetByQuery(mockTableID)
	require.True(t, ok)
	require.Equal(t, mockTable, v)
	lru.Del(mockTableID)
	v, ok = lru.Get(mockTableID)
	require.False(t, ok)
	require.Nil(t, v)
}

func TestLRUEvict(t *testing.T) {
	capacity := int64(12)
	lru := newStatsLruCache(capacity)
	t1 := newMockStatisticsTable(1, 2)
	require.Equal(t, t1.MemoryUsage().TotalMemUsage, 1*columnTotalMemoryUsage+2*indexTotalMemoryUsage)
	require.Equal(t, t1.MemoryUsage().TotalIdxTrackingMemUsage(), 2*indexMemoryUsage)

	// Put t1, assert TotalMemUsage and TotalColTrackingMemUsage
	lru.Put(int64(1), t1)
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), t1.MemoryUsage().TotalIdxTrackingMemUsage())

	// Put t2, assert TotalMemUsage and TotalColTrackingMemUsage
	t2 := newMockStatisticsTable(2, 1)
	lru.Put(int64(2), t2)
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage+t2.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), t1.MemoryUsage().TotalIdxTrackingMemUsage()+t2.MemoryUsage().TotalIdxTrackingMemUsage())

	// Put t3, an index CMSketch of t1 should be evicted
	t3 := newMockStatisticsTable(3, 1)
	lru.Put(int64(3), t3)
	require.Equal(t, lru.Len(), 3)
	require.Equal(t, t1.MemoryUsage().TotalIdxTrackingMemUsage(), indexMemoryUsage)
	require.Equal(t, lru.TotalCost(), 6*columnTotalMemoryUsage+3*indexTotalMemoryUsage+1*indexEvictedTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 3*indexMemoryUsage)
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage+t2.MemoryUsage().TotalMemUsage+t3.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), t1.MemoryUsage().TotalIdxTrackingMemUsage()+
		t2.MemoryUsage().TotalIdxTrackingMemUsage()+t3.MemoryUsage().TotalIdxTrackingMemUsage())

	// Put t4, all indices' cmsketch of other tables should be evicted
	t4 := newMockStatisticsTable(4, 3)
	lru.Put(int64(4), t4)
	require.Equal(t, lru.Len(), 4)
	require.Equal(t, t1.MemoryUsage().TotalIdxTrackingMemUsage(), int64(0))
	require.Equal(t, t2.MemoryUsage().TotalIdxTrackingMemUsage(), int64(0))
	require.Equal(t, t3.MemoryUsage().TotalIdxTrackingMemUsage(), int64(0))
	require.Equal(t, lru.TotalCost(), 3*indexTotalMemoryUsage+10*columnTotalMemoryUsage+4*indexEvictedTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 3*indexMemoryUsage)
}

func TestLRUCopy(t *testing.T) {
	lru := newStatsLruCache(1000)
	tables := make([]*statistics.Table, 0)
	for i := 0; i < 5; i++ {
		tables = append(tables, newMockStatisticsTable(1, 1))
	}

	// insert 1,2,3 into old lru
	for i := 0; i < 3; i++ {
		mockTable := tables[i]
		key := int64(i)
		lru.Put(key, mockTable)
		value, ok := lru.Get(key)
		require.True(t, ok)
		require.Equal(t, value, mockTable)
	}
	newLRU := lru.Copy()
	// assert new lru has same elements as old lru
	for i := 0; i < 3; i++ {
		key := int64(i)
		value, ok := newLRU.Get(key)
		require.True(t, ok)
		require.EqualValues(t, value, tables[i])
	}
	// delete new lru element
	newLRU.Del(int64(1))
	_, ok := newLRU.Get(int64(1))
	require.False(t, ok)

	// old lru has no affect
	for i := 0; i < 3; i++ {
		key := int64(i)
		value, ok := lru.Get(key)
		require.True(t, ok)
		require.Equal(t, value, tables[i])
	}
}

func TestLRUFreshMemUsage(t *testing.T) {
	lru := newStatsLruCache(1000)
	t1 := newMockStatisticsTable(1, 1)
	t2 := newMockStatisticsTable(2, 2)
	t3 := newMockStatisticsTable(3, 3)
	lru.Put(int64(1), t1)
	lru.Put(int64(2), t2)
	lru.Put(int64(3), t3)
	require.Equal(t, lru.TotalCost(), 6*columnTotalMemoryUsage+6*indexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 6*indexMemoryUsage)
	mockTableAppendColumn(t1)
	lru.FreshMemUsage()
	require.Equal(t, lru.TotalCost(), 7*columnTotalMemoryUsage+6*indexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 6*indexMemoryUsage)
	mockTableAppendIndex(t1)
	lru.FreshMemUsage()
	require.Equal(t, lru.TotalCost(), 7*columnTotalMemoryUsage+7*indexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 7*indexMemoryUsage)
}

func TestLRUPutTooBig(t *testing.T) {
	lru := newStatsLruCache(1)
	mockTable := newMockStatisticsTable(1, 1)
	// put mockTable, the index should be evicted
	lru.Put(int64(1), mockTable)
	_, ok := lru.Get(int64(1))
	require.True(t, ok)
	require.Equal(t, lru.TotalCost(), columnTotalMemoryUsage+indexEvictedTotalMemoryUsage)
	require.Equal(t, lru.Cost(), int64(0))
	require.Equal(t, mockTable.MemoryUsage().TotalIdxTrackingMemUsage(), int64(0))
}

func TestCacheLen(t *testing.T) {
	capacity := int64(8)
	stats := newStatsLruCache(capacity)
	t1 := newMockStatisticsTable(1, 2)
	stats.Put(int64(1), t1)
	t2 := newMockStatisticsTable(1, 1)
	// put t2, t1 should be evicted 1 index and still exists in the list
	stats.Put(int64(2), t2)
	require.Equal(t, stats.lru.cache.Len(), 2)
	require.Equal(t, t1.MemoryUsage().TotalIdxTrackingMemUsage(), indexMemoryUsage)
	require.Equal(t, stats.Len(), 2)

	// put t3, t1 should be evicted 2 index at all and disappeared from the list
	t3 := newMockStatisticsTable(1, 1)
	stats.Put(int64(3), t3)
	require.Equal(t, stats.lru.cache.Len(), 2)
	require.Equal(t, t1.MemoryUsage().TotalIdxTrackingMemUsage(), int64(0))
	require.Equal(t, stats.Len(), 3)
}

func TestLRUMove(t *testing.T) {
	capacity := int64(100)
	s := newStatsLruCache(capacity)
	t1 := newMockStatisticsTable(1, 1)
	t1ID := int64(1)
	t2 := newMockStatisticsTable(1, 1)
	t2ID := int64(2)
	s.Put(t1ID, t1)
	s.Put(t2ID, t2)
	// assert t2 element should be front element
	front := s.lru.cache.Front().Value.(*lruCacheItem)
	require.Equal(t, t2ID, front.tblID)
	// assert t1 element should be front element after GetByQuery
	s.GetByQuery(t1ID)
	front = s.lru.cache.Front().Value.(*lruCacheItem)
	require.Equal(t, t1ID, front.tblID)
}
