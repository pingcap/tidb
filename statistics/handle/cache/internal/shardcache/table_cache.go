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
	"sync"

	"github.com/pingcap/tidb/statistics"
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

type tableCache struct {
	Item map[int64]cacheItem
	mu   sync.RWMutex
}

func newTableCache() *tableCache {
	item := make(map[int64]cacheItem)
	result := &tableCache{
		Item: item,
	}
	return result
}

func (t *tableCache) Get(k int64) (*statistics.Table, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	value, ok := t.Item[k]
	return value.value, ok
}

func (t *tableCache) Put(k int64, v *statistics.Table) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	item, ok := t.Item[k]
	if ok {
		oldCost := item.cost
		newCost := v.MemoryUsage().TotalMemUsage
		item.value = v
		item.cost = newCost
		t.Item[k] = item
		return newCost - oldCost
	}
	cost := v.MemoryUsage().TotalMemUsage
	item = cacheItem{
		key:   k,
		value: v,
		cost:  cost,
	}
	t.Item[k] = item
	return cost
}

func (t *tableCache) Delete(k int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	item, ok := t.Item[k]
	if !ok {
		return 0
	}
	delete(t.Item, k)
	return item.cost
}

func (t *tableCache) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Item)
}

func (t *tableCache) Iterate(f func(k int64, v *statistics.Table)) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for k, v := range t.Item {
		f(k, v.value)
	}
}

func (t *tableCache) FreshMemUsage() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	delta := int64(0)
	for _, v := range t.Item {
		oldCost := v.cost
		newCost := v.value.MemoryUsage().TotalMemUsage
		delta = delta + (newCost - oldCost)
	}
	return delta
}
