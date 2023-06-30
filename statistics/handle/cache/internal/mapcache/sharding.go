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
	"sync/atomic"

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
	ref  atomic.Int32
}

func newTableCache() *tableCache {
	item := make(map[int64]cacheItem)
	result := &tableCache{
		Item: item,
	}
	result.ref.Store(1)
	return result
}

func (t *tableCache) AddRef() {
	t.ref.Add(1)
}

func (t *tableCache) DecRef() {
	v := t.ref.Add(-1)
	switch {
	case v < 0:
		panic("Decrementing non-positive ref count")
	case v == 0:
	}
}

func (t *tableCache) Get(k int64) (*statistics.Table, bool) {
	value, ok := t.Item[k]
	return value.value, ok
}

func (t *tableCache) Put(k int64, v *statistics.Table) int64 {
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

func (t *tableCache) Len() int {
	return len(t.Item)
}

func (t *tableCache) Iterate(f func(k int64, v *statistics.Table)) {
	for k, v := range t.Item {
		f(k, v.value)
	}
}

func (t *tableCache) FreshMemUsage() int64 {
	delta := int64(0)
	for _, v := range t.Item {
		oldCost := v.cost
		newCost := v.value.MemoryUsage().TotalMemUsage
		delta = delta + (newCost - oldCost)
	}
	return delta
}

func (t *tableCache) Clone() *tableCache {
	newItem := make(map[int64]cacheItem, len(t.Item))
	for k, v := range t.Item {
		newItem[k] = v.copy()
	}
	newT := &tableCache{
		Item: newItem,
	}
	newT.ref.Store(1)
	return newT
}

type tableCacheView struct {
	tableCache *tableCache
}

func newTableCacheView() *tableCacheView {
	c := newTableCache()
	return &tableCacheView{tableCache: c}
}

func (v *tableCacheView) Clone() *tableCacheView {
	if v == nil {
		panic("cannot clone a nil view")
	}
	v.tableCache.AddRef()
	newV := &tableCacheView{
		tableCache: v.tableCache,
	}
	return newV
}

func (v *tableCacheView) Release() {
	if v == nil {
		panic("cannot release a nil view")
	}
	v.tableCache.DecRef()
}

func (v *tableCacheView) share() bool {
	return v.tableCache.ref.Load() > 1
}

func (v *tableCacheView) Get(k int64) (*statistics.Table, bool) {
	return v.tableCache.Get(k)
}

func (v *tableCacheView) Put(k int64, tbl *statistics.Table) int64 {
	if v.share() {
		defer v.tableCache.DecRef()
		v.tableCache = v.tableCache.Clone()
	}
	return v.tableCache.Put(k, tbl)
}

func (v *tableCacheView) Del(k int64) int64 {
	item, ok := v.tableCache.Item[k]
	if !ok {
		return 0
	}
	if v.share() {
		defer v.tableCache.DecRef()
		v.tableCache = v.tableCache.Clone()
	}
	delete(v.tableCache.Item, k)
	return item.cost
}

func (v *tableCacheView) Iterate(f func(k int64, v *statistics.Table)) {
	v.tableCache.Iterate(f)
}

func (v *tableCacheView) Len() int {
	return v.tableCache.Len()
}

func (v *tableCacheView) FreshMemUsage() int64 {
	return v.tableCache.FreshMemUsage()
}
