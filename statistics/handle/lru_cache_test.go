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
	mockIndexMemoryUsage       = int64(4)
	mockColumnMemoryUsage      = int64(4)
	mockColumnTotalMemoryUsage = statistics.EmptyHistogramSize + 4
	mockIndexTotalMemoryUsage  = statistics.EmptyHistogramSize + 4
)

// each column and index consumes 4 bytes memory
func newMockStatisticsTable(columns int, indices int) *statistics.Table {
	t := &statistics.Table{}
	t.Columns = make(map[int64]*statistics.Column)
	t.Indices = make(map[int64]*statistics.Index)
	for i := 1; i <= columns; i++ {
		t.Columns[int64(i)] = &statistics.Column{
			Info:            &model.ColumnInfo{ID: int64(i)},
			CMSketch:        statistics.NewCMSketch(1, 1),
			ColLoadedStatus: statistics.NewColFullLoadStatus(),
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
	index := int64(len(t.Indices) + 1)
	t.Indices[index] = &statistics.Index{
		Info:     &model.IndexInfo{ID: index},
		CMSketch: statistics.NewCMSketch(1, 1),
	}
}

func mockTableRemoveColumn(t *statistics.Table) {
	delete(t.Columns, int64(len(t.Columns)))
}

func mockTableRemoveIndex(t *statistics.Table) {
	delete(t.Indices, int64(len(t.Indices)))
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
	capacity := int64(24)
	lru := newStatsLruCache(capacity)
	t1 := newMockStatisticsTable(2, 0)
	require.Equal(t, t1.MemoryUsage().TotalMemUsage, 2*mockColumnTotalMemoryUsage)
	require.Equal(t, t1.MemoryUsage().TotalIdxTrackingMemUsage(), int64(0))
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), 2*mockColumnMemoryUsage)

	// Put t1, assert TotalMemUsage and TotalColTrackingMemUsage
	lru.Put(int64(1), t1)
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), t1.MemoryUsage().TotalTrackingMemUsage())

	// Put t2, assert TotalMemUsage and TotalColTrackingMemUsage
	t2 := newMockStatisticsTable(2, 1)
	lru.Put(int64(2), t2)
	require.Equal(t, lru.TotalCost(), 4*mockColumnTotalMemoryUsage+1*mockIndexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 4*mockColumnMemoryUsage+1*mockIndexMemoryUsage)

	// Put t3, a column of t1 should be evicted
	t3 := newMockStatisticsTable(1, 1)
	lru.Put(int64(3), t3)
	require.Equal(t, lru.Len(), 3)
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), mockColumnMemoryUsage)
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage+t2.MemoryUsage().TotalMemUsage+t3.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), 4*mockColumnMemoryUsage+2*mockIndexMemoryUsage)

	// Put t4, all indices' cmsketch of other tables should be evicted
	t4 := newMockStatisticsTable(3, 3)
	lru.Put(int64(4), t4)
	require.Equal(t, lru.Len(), 4)
	require.Equal(t, t1.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, t2.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, t3.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, lru.TotalCost(), t1.MemoryUsage().TotalMemUsage+
		t2.MemoryUsage().TotalMemUsage+
		t3.MemoryUsage().TotalMemUsage+
		t4.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.Cost(), 3*mockColumnMemoryUsage+3*mockIndexMemoryUsage)
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
	require.Equal(t, lru.TotalCost(), 6*mockColumnTotalMemoryUsage+6*mockIndexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 6*mockIndexMemoryUsage+6*mockColumnMemoryUsage)
	mockTableAppendColumn(t1)
	lru.FreshMemUsage()
	require.Equal(t, lru.TotalCost(), 7*mockColumnTotalMemoryUsage+6*mockIndexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 6*mockIndexMemoryUsage+7*mockColumnMemoryUsage)
	mockTableAppendIndex(t1)
	lru.FreshMemUsage()
	require.Equal(t, lru.TotalCost(), 7*mockColumnTotalMemoryUsage+7*mockIndexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 7*mockIndexMemoryUsage+7*mockColumnMemoryUsage)

	mockTableRemoveColumn(t1)
	lru.Put(int64(1), t1)
	require.Equal(t, lru.TotalCost(), 6*mockColumnTotalMemoryUsage+7*mockIndexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 7*mockIndexMemoryUsage+6*mockColumnMemoryUsage)

	mockTableRemoveIndex(t1)
	lru.Put(int64(1), t1)
	require.Equal(t, lru.TotalCost(), 6*mockColumnTotalMemoryUsage+6*mockIndexTotalMemoryUsage)
	require.Equal(t, lru.Cost(), 6*mockIndexMemoryUsage+6*mockColumnMemoryUsage)
}

func TestLRUPutTooBig(t *testing.T) {
	lru := newStatsLruCache(1)
	mockTable := newMockStatisticsTable(1, 1)
	// put mockTable, the index should be evicted
	lru.Put(int64(1), mockTable)
	_, ok := lru.Get(int64(1))
	require.True(t, ok)
	require.Equal(t, lru.TotalCost(), mockColumnTotalMemoryUsage-mockColumnMemoryUsage+mockIndexTotalMemoryUsage-mockIndexMemoryUsage)
	require.Equal(t, lru.Cost(), int64(0))
	require.Equal(t, mockTable.MemoryUsage().TotalTrackingMemUsage(), int64(0))
}

func TestCacheLen(t *testing.T) {
	capacity := int64(12)
	stats := newStatsLruCache(capacity)
	t1 := newMockStatisticsTable(2, 1)
	stats.Put(int64(1), t1)
	t2 := newMockStatisticsTable(1, 1)
	// put t2, t1 should be evicted 2 items and still exists in the list
	stats.Put(int64(2), t2)
	require.Equal(t, stats.lru.cache.Len(), 3)
	require.Equal(t, t1.MemoryUsage().TotalTrackingMemUsage(), int64(4))
	require.Equal(t, stats.Len(), 2)

	// put t3, t1/t2 should be evicted all items and disappeared from the list
	t3 := newMockStatisticsTable(2, 1)
	stats.Put(int64(3), t3)
	require.Equal(t, stats.lru.cache.Len(), 3)
	require.Equal(t, t1.MemoryUsage().TotalTrackingMemUsage(), int64(0))
	require.Equal(t, t2.MemoryUsage().TotalTrackingMemUsage(), int64(0))
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
