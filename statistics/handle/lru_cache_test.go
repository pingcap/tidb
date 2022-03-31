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

	"github.com/pingcap/tidb/statistics"
	"github.com/stretchr/testify/require"
)

func newMockStatisticsTable() *statistics.Table {
	t := &statistics.Table{}
	t.Columns = make(map[int64]*statistics.Column)
	t.Columns[int64(1)] = &statistics.Column{
		CMSketch: statistics.NewCMSketch(1, 1),
	}
	return t
}

func TestLRUOp(t *testing.T) {
	lru := newInternalLRUCache(12)
	require.Equal(t, int64(12), lru.capacity)

	// insert 1,2,3
	for i := 1; i <= 3; i++ {
		mockTable := newMockStatisticsTable()
		key := int64(i)
		lru.Put(key, mockTable)
		value, ok := lru.Get(key)
		require.True(t, ok)
		require.Equal(t, value, mockTable)
	}
	_, ok := lru.Get(int64(4))
	require.False(t, ok)

	// insert 4, become 2,3,4
	t4 := newMockStatisticsTable()
	lru.Put(int64(4), t4)
	v, ok := lru.Get(int64(4))
	require.True(t, ok)
	require.Equal(t, v, t4)
	_, ok = lru.Get(int64(1))
	require.False(t, ok)

	// delete 4, become 2,3
	lru.Del(int64(4))
	_, ok = lru.Get(int64(4))
	require.False(t, ok)

	// insert 5, become 2,3,5
	t5 := newMockStatisticsTable()
	lru.Put(int64(5), t5)
	v, ok = lru.Get(int64(5))
	require.True(t, ok)
	require.Equal(t, v, t5)

	v, ok = lru.Get(int64(3))
	require.True(t, ok)
	v, ok = lru.Get(int64(2))
	require.True(t, ok)
}

func TestLRUCopy(t *testing.T) {
	lru := newInternalLRUCache(12)
	require.Equal(t, int64(12), lru.capacity)

	tables := make([]*statistics.Table, 0)
	for i := 0; i < 5; i++ {
		tables = append(tables, newMockStatisticsTable())
	}

	// insert 1,2,3
	for i := 0; i < 3; i++ {
		mockTable := tables[i]
		key := int64(i)
		lru.Put(key, mockTable)
		value, ok := lru.Get(key)
		require.True(t, ok)
		require.Equal(t, value, mockTable)
	}
	newLRU := lru.Copy()
	for i := 0; i < 3; i++ {
		key := int64(i)
		value, ok := newLRU.Get(key)
		require.True(t, ok)
		require.EqualValues(t, value, tables[i])
	}
	newLRU.Del(int64(1))
	_, ok := newLRU.Get(int64(1))
	require.False(t, ok)

	for i := 0; i < 3; i++ {
		key := int64(i)
		value, ok := lru.Get(key)
		require.True(t, ok)
		require.Equal(t, value, tables[i])
	}
}
