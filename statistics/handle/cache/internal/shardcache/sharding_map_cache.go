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

package shardcache

import (
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/cache/internal"
)

const tablesCacheShardCnt = 256

// MapCache is a cache based on map.
type MapCache struct {
	tables               [tablesCacheShardCnt]*tableCache
	memUsage             int64
	maxTableStatsVersion uint64
}

// NewMapCache creates a new map cache.
func NewMapCache() *MapCache {
	var tables [tablesCacheShardCnt]*tableCache
	for v := range tables {
		tables[v] = newTableCache()
	}
	return &MapCache{
		tables:   tables,
		memUsage: 0,
	}
}

// Get implements statsCacheInner
func (m *MapCache) Get(k int64, _ bool) (*statistics.Table, bool) {
	table := m.tables[k%tablesCacheShardCnt]
	return table.Get(k)
}

// Put implements statsCacheInner
func (m *MapCache) Put(k int64, v *statistics.Table, _ bool) {
	table := m.tables[k%tablesCacheShardCnt]
	delta := table.Put(k, v)
	m.memUsage += delta
}

// Del implements statsCacheInner
func (m *MapCache) Del(k int64) {
	table := m.tables[k%tablesCacheShardCnt]
	m.memUsage -= table.Delete(k)
}

// Cost implements statsCacheInner
func (m *MapCache) Cost() int64 {
	return m.memUsage
}

// Keys implements statsCacheInner
func (m *MapCache) Keys() []int64 {
	ks := make([]int64, 0, len(m.tables))
	for _, table := range m.tables {
		table.Iterate(func(k int64, _ *statistics.Table) {
			ks = append(ks, k)
		})
	}
	return ks
}

// Values implements statsCacheInner
func (m *MapCache) Values() []*statistics.Table {
	vs := make([]*statistics.Table, 0, len(m.tables))
	for _, table := range m.tables {
		table.Iterate(func(_ int64, v *statistics.Table) {
			vs = append(vs, v)
		})
	}
	return vs
}

// Map implements statsCacheInner
func (m *MapCache) Map() map[int64]*statistics.Table {
	t := make(map[int64]*statistics.Table, len(m.tables))
	for _, table := range m.tables {
		table.Iterate(func(k int64, v *statistics.Table) {
			t[k] = v
		})
	}
	return t
}

// Len implements statsCacheInner
func (m *MapCache) Len() int {
	var s int
	for idx := range m.tables {
		s += m.tables[idx].Len()
	}
	return s
}

// FreshMemUsage implements statsCacheInner
func (m *MapCache) FreshMemUsage() {
	for _, table := range m.tables {
		m.memUsage += table.FreshMemUsage()
	}
}

// Copy implements statsCacheInner
// TODO(hawkingrei): remove Copy. it is unnecessary that ShardMap use COW.
func (m *MapCache) Copy() internal.StatsCacheInner {
	var tables [tablesCacheShardCnt]*tableCache
	for v := range tables {
		tables[v] = m.tables[v]
	}
	newM := &MapCache{
		tables:               tables,
		memUsage:             m.memUsage,
		maxTableStatsVersion: m.maxTableStatsVersion,
	}
	return newM
}

// SetCapacity implements statsCacheInner
func (*MapCache) SetCapacity(int64) {}

// Front implements statsCacheInner
func (*MapCache) Front() int64 {
	return 0
}

// Version implements StatsCacheInner
func (m *MapCache) Version() uint64 {
	return m.maxTableStatsVersion
}
