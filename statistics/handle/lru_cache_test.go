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
	columnMemoryUsage = int64(4)
	indexMemoryUsage  = int64(4)
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
	lru := newInternalLRUCache(capacity)
	require.Equal(t, capacity, lru.capacity)
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
}

func TestLRUEvict(t *testing.T) {
	capacity := int64(12)
	lru := newInternalLRUCache(capacity)
	t1 := newMockStatisticsTable(2, 1)
	require.Equal(t, t1.MemoryUsage().TotalMemUsage, 2*columnMemoryUsage+1*indexMemoryUsage)
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), 2*columnMemoryUsage)
	lru.Put(int64(1), t1)
	require.Equal(t, lru.totalCost, t1.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.trackingCost, t1.MemoryUsage().TotalColTrackingMemUsage())
	t2 := newMockStatisticsTable(1, 2)
	lru.Put(int64(2), t2)
	require.Equal(t, lru.totalCost, t1.MemoryUsage().TotalMemUsage+t2.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.trackingCost, t1.MemoryUsage().TotalColTrackingMemUsage()+t2.MemoryUsage().TotalColTrackingMemUsage())

	// Put t3, a column CMSketch of t1 should be evicted
	t3 := newMockStatisticsTable(1, 3)
	lru.Put(int64(3), t3)
	require.Equal(t, lru.Len(), 3)
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), columnMemoryUsage)
	require.Equal(t, lru.totalCost, 3*columnMemoryUsage+6*indexMemoryUsage)
	require.Equal(t, lru.trackingCost, 3*columnMemoryUsage)
	require.Equal(t, lru.totalCost, t1.MemoryUsage().TotalMemUsage+t2.MemoryUsage().TotalMemUsage+t3.MemoryUsage().TotalMemUsage)
	require.Equal(t, lru.trackingCost, t1.MemoryUsage().TotalColTrackingMemUsage()+
		t2.MemoryUsage().TotalColTrackingMemUsage()+t3.MemoryUsage().TotalColTrackingMemUsage())

	// Put t4, all columns cmsketch of other tables should be evicted
	t4 := newMockStatisticsTable(3, 4)
	lru.Put(int64(4), t4)
	require.Equal(t, lru.Len(), 4)
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), int64(0))
	require.Equal(t, t2.MemoryUsage().TotalColTrackingMemUsage(), int64(0))
	require.Equal(t, t3.MemoryUsage().TotalColTrackingMemUsage(), int64(0))
	require.Equal(t, lru.totalCost, 3*columnMemoryUsage+10*indexMemoryUsage)
	require.Equal(t, lru.trackingCost, 3*columnMemoryUsage)
}

func TestLRUCopy(t *testing.T) {
	lru := newInternalLRUCache(1000)
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
	lru := newInternalLRUCache(1000)
	t1 := newMockStatisticsTable(1, 1)
	t2 := newMockStatisticsTable(2, 2)
	t3 := newMockStatisticsTable(3, 3)
	lru.Put(int64(1), t1)
	lru.Put(int64(2), t2)
	lru.Put(int64(3), t3)
	require.Equal(t, lru.totalCost, 6*columnMemoryUsage+6*indexMemoryUsage)
	require.Equal(t, lru.trackingCost, 6*columnMemoryUsage)
	mockTableAppendColumn(t1)
	lru.FreshMemUsage()
	require.Equal(t, lru.totalCost, 7*columnMemoryUsage+6*indexMemoryUsage)
	require.Equal(t, lru.trackingCost, 7*columnMemoryUsage)
	mockTableAppendIndex(t1)
	lru.FreshMemUsage()
	require.Equal(t, lru.totalCost, 7*columnMemoryUsage+7*indexMemoryUsage)
	require.Equal(t, lru.trackingCost, 7*columnMemoryUsage)
}

func TestLRUFreshTableMemUsage(t *testing.T) {
	lru := newInternalLRUCache(1000)
	t1 := newMockStatisticsTable(1, 1)
	t2 := newMockStatisticsTable(2, 2)
	t3 := newMockStatisticsTable(3, 3)
	lru.Put(int64(1), t1)
	lru.Put(int64(2), t2)
	lru.Put(int64(3), t3)
	require.Equal(t, lru.totalCost, 6*columnMemoryUsage+6*indexMemoryUsage)
	require.Equal(t, lru.trackingCost, 6*columnMemoryUsage)
	mockTableAppendColumn(t1)
	lru.FreshTableCost(int64(1))
	require.Equal(t, lru.totalCost, 7*columnMemoryUsage+6*indexMemoryUsage)
	require.Equal(t, lru.trackingCost, 7*columnMemoryUsage)
	mockTableAppendIndex(t1)
	lru.FreshTableCost(int64(1))
	require.Equal(t, lru.totalCost, 7*columnMemoryUsage+7*indexMemoryUsage)
	require.Equal(t, lru.trackingCost, 7*columnMemoryUsage)
}

func TestLRUPutTooBig(t *testing.T) {
	lru := newInternalLRUCache(1)
	mockTable := newMockStatisticsTable(1, 1)
	success := lru.Put(int64(1), mockTable)
	require.False(t, success)
}

func TestCacheLen(t *testing.T) {
	capacity := int64(8)
	lru := newInternalLRUCache(capacity)
	t1 := newMockStatisticsTable(2, 1)
	lru.Put(int64(1), t1)
	t2 := newMockStatisticsTable(1, 1)
	// put t2, t1 should be evicted 1 column and still exists in the list
	lru.Put(int64(2), t2)
	require.Equal(t, lru.cache.Len(), 2)
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), columnMemoryUsage)
	require.Equal(t, lru.Len(), 2)

	// put t3, t1 should be evicted 2 columns at all and disappeared from the list
	t3 := newMockStatisticsTable(1, 1)
	lru.Put(int64(3), t3)
	require.Equal(t, lru.cache.Len(), 2)
	require.Equal(t, t1.MemoryUsage().TotalColTrackingMemUsage(), int64(0))
	require.Equal(t, lru.Len(), 3)
}
