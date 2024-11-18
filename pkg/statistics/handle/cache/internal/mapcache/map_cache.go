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

package mapcache

import (
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal"
)

type cacheItem struct {
	value *statistics.Table
	key   int64
	cost  int64
}

func (c cacheItem) copy() cacheItem {
	return cacheItem{
		key:   c.key,
		value: c.value,
		cost:  c.cost,
	}
}

// MapCache is a cache based on map.
type MapCache struct {
	tables   map[int64]cacheItem
	memUsage int64
}

// NewMapCache creates a new map cache.
func NewMapCache() *MapCache {
	return &MapCache{
		tables:   make(map[int64]cacheItem),
		memUsage: 0,
	}
}

// Get implements StatsCacheInner
func (m *MapCache) Get(k int64) (*statistics.Table, bool) {
	v, ok := m.tables[k]
	return v.value, ok
}

// Put implements StatsCacheInner
func (m *MapCache) Put(k int64, v *statistics.Table) bool {
	item, ok := m.tables[k]
	if ok {
		oldCost := item.cost
		newCost := v.MemoryUsage().TotalMemUsage
		item.value = v
		item.cost = newCost
		m.tables[k] = item
		m.memUsage += newCost - oldCost
		return true
	}
	cost := v.MemoryUsage().TotalMemUsage
	item = cacheItem{
		key:   k,
		value: v,
		cost:  cost,
	}
	m.tables[k] = item
	m.memUsage += cost
	return true
}

// Del implements StatsCacheInner
func (m *MapCache) Del(k int64) {
	item, ok := m.tables[k]
	if !ok {
		return
	}
	delete(m.tables, k)
	m.memUsage -= item.cost
}

// Cost implements StatsCacheInner
func (m *MapCache) Cost() int64 {
	return m.memUsage
}

// Keys implements StatsCacheInner
func (m *MapCache) Keys() []int64 {
	ks := make([]int64, 0, len(m.tables))
	for k := range m.tables {
		ks = append(ks, k)
	}
	return ks
}

// Values implements StatsCacheInner
func (m *MapCache) Values() []*statistics.Table {
	vs := make([]*statistics.Table, 0, len(m.tables))
	for _, v := range m.tables {
		vs = append(vs, v.value)
	}
	return vs
}

// Len implements StatsCacheInner
func (m *MapCache) Len() int {
	return len(m.tables)
}

// Copy implements StatsCacheInner
func (m *MapCache) Copy() internal.StatsCacheInner {
	newM := &MapCache{
		tables:   make(map[int64]cacheItem, len(m.tables)),
		memUsage: m.memUsage,
	}
	for k, v := range m.tables {
		newM.tables[k] = v.copy()
	}
	return newM
}

// SetCapacity implements StatsCacheInner
func (*MapCache) SetCapacity(int64) {}

// Close implements StatsCacheInner
func (*MapCache) Close() {}
