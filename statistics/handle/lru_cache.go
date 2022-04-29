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
	"container/list"
	"sync"

	"github.com/pingcap/tidb/statistics"
)

type statsInnerCache struct {
	sync.RWMutex
	elements map[int64]*lruMapElement
	// lru maintains index lru cache
	lru *innerIndexLruCache
}

func newStatsLruCache(c int64) *statsInnerCache {
	s := &statsInnerCache{
		elements: make(map[int64]*lruMapElement),
		lru:      newInnerIndexLruCache(c),
	}
	s.lru.onEvict = s.onEvict
	return s
}

type innerIndexLruCache struct {
	capacity     int64
	trackingCost int64
	elements     map[int64]map[int64]*list.Element
	cache        *list.List
	onEvict      func(tblID int64)
}

func newInnerIndexLruCache(c int64) *innerIndexLruCache {
	return &innerIndexLruCache{
		capacity: c,
		cache:    list.New(),
		elements: make(map[int64]map[int64]*list.Element, 0),
	}
}

type lruCacheItem struct {
	tblID       int64
	idxID       int64
	index       *statistics.Index
	idxMemUsage *statistics.IndexMemUsage
}

type lruMapElement struct {
	tbl         *statistics.Table
	tblMemUsage *statistics.TableMemoryUsage
}

// GetByQuery implements statsCacheInner
func (s *statsInnerCache) GetByQuery(tblID int64) (*statistics.Table, bool) {
	s.Lock()
	defer s.Unlock()
	element, ok := s.elements[tblID]
	if !ok {
		return nil, false
	}
	// move element
	for idxID := range element.tblMemUsage.IndicesMemUsage {
		s.lru.get(tblID, idxID)
	}
	return element.tbl, true
}

// Get implements statsCacheInner
func (s *statsInnerCache) Get(tblID int64) (*statistics.Table, bool) {
	s.Lock()
	defer s.Unlock()
	element, ok := s.elements[tblID]
	if !ok {
		return nil, false
	}
	return element.tbl, true
}

// PutByQuery implements statsCacheInner
func (s *statsInnerCache) PutByQuery(tblID int64, tbl *statistics.Table) {
	s.Lock()
	defer s.Unlock()
	s.put(tblID, tbl, true)
}

// Put implements statsCacheInner
func (s *statsInnerCache) Put(tblID int64, tbl *statistics.Table) {
	s.Lock()
	defer s.Unlock()
	s.put(tblID, tbl, false)
}

func (s *statsInnerCache) put(tblID int64, tbl *statistics.Table, needMove bool) {
	element, exist := s.elements[tblID]
	if exist {
		oldtbl := element.tbl
		newTblMem := tbl.MemoryUsage()
		deletedIdx := make([]int64, 0)
		for oldIdxID := range oldtbl.Indices {
			_, exist := tbl.Indices[oldIdxID]
			if !exist {
				deletedIdx = append(deletedIdx, oldIdxID)
			}
		}
		for idxID, index := range tbl.Indices {
			idxMem := newTblMem.IndicesMemUsage[idxID]
			if idxMem.TrackingMemUsage() < 1 {
				deletedIdx = append(deletedIdx, idxID)
				continue
			}
			s.lru.put(tblID, idxID, index, idxMem, true, needMove)
		}
		for _, idxID := range deletedIdx {
			s.lru.del(tblID, idxID)
		}
		// idx mem usage might be changed before, thus we recalculate the tblMem usage
		element.tbl = tbl
		element.tblMemUsage = tbl.MemoryUsage()
		return
	}
	tblMem := tbl.MemoryUsage()
	for idxID, idx := range tbl.Indices {
		idxMem := tblMem.IndicesMemUsage[idxID]
		s.lru.put(tblID, idxID, idx, idxMem, true, needMove)
	}
	// index mem usage might be changed due to evict, thus we recalculate the tblMem usage
	s.elements[tblID] = &lruMapElement{
		tbl:         tbl,
		tblMemUsage: tbl.MemoryUsage(),
	}
}

// Del implements statsCacheInner
func (s *statsInnerCache) Del(tblID int64) {
	s.Lock()
	defer s.Unlock()
	element, exist := s.elements[tblID]
	if !exist {
		return
	}
	for idxID := range element.tblMemUsage.IndicesMemUsage {
		s.lru.del(tblID, idxID)
	}
	delete(s.elements, tblID)
}

// Cost implements statsCacheInner
func (s *statsInnerCache) Cost() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.lru.trackingCost
}

func (s *statsInnerCache) totalCost() int64 {
	s.RLock()
	defer s.RUnlock()
	totalCost := int64(0)
	for tblID, ele := range s.elements {
		s.freshTableCost(tblID, ele)
		totalCost += ele.tblMemUsage.TotalMemUsage
	}
	return totalCost
}

// Keys implements statsCacheInner
func (s *statsInnerCache) Keys() []int64 {
	s.RLock()
	defer s.RUnlock()
	r := make([]int64, 0, len(s.elements))
	for tblID := range s.elements {
		r = append(r, tblID)
	}
	return r
}

// Values implements statsCacheInner
func (s *statsInnerCache) Values() []*statistics.Table {
	s.RLock()
	defer s.RUnlock()
	r := make([]*statistics.Table, 0, len(s.elements))
	for _, v := range s.elements {
		r = append(r, v.tbl)
	}
	return r
}

// Map implements statsCacheInner
func (s *statsInnerCache) Map() map[int64]*statistics.Table {
	s.RLock()
	defer s.RUnlock()
	r := make(map[int64]*statistics.Table, len(s.elements))
	for k, v := range s.elements {
		r[k] = v.tbl
	}
	return r
}

// Len implements statsCacheInner
func (s *statsInnerCache) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.elements)
}

// FreshMemUsage implements statsCacheInner
func (s *statsInnerCache) FreshMemUsage() {
	s.Lock()
	defer s.Unlock()
	for tblID, element := range s.elements {
		s.freshTableCost(tblID, element)
	}
}

// FreshTableCost implements statsCacheInner
func (s *statsInnerCache) FreshTableCost(tblID int64) {
	s.Lock()
	defer s.Unlock()
	element, exist := s.elements[tblID]
	if !exist {
		return
	}
	s.freshTableCost(tblID, element)
}

// Copy implements statsCacheInner
func (s *statsInnerCache) Copy() statsCacheInner {
	s.RLock()
	defer s.RUnlock()
	newCache := newStatsLruCache(s.lru.capacity)
	newCache.lru = s.lru.copy()
	for tblID, element := range s.elements {
		newCache.elements[tblID] = element
	}
	return newCache
}

func (s *statsInnerCache) onEvict(tblID int64) {
	element, exist := s.elements[tblID]
	if !exist {
		return
	}
	element.tblMemUsage = element.tbl.MemoryUsage()
}

func (s *statsInnerCache) freshTableCost(tblID int64, element *lruMapElement) {
	newTblMem := element.tbl.MemoryUsage()
	for idxID, idx := range element.tbl.Indices {
		s.lru.put(tblID, idxID, idx, newTblMem.IndicesMemUsage[idxID], true, false)
	}
	// tbl mem usage might be changed due to evict
	element.tblMemUsage = element.tbl.MemoryUsage()
}

func (s *statsInnerCache) capacity() int64 {
	return s.lru.capacity
}

func (c *innerIndexLruCache) get(tblID, idxID int64) (*lruCacheItem, bool) {
	v, ok := c.elements[tblID]
	if !ok {
		return nil, false
	}
	ele, ok := v[idxID]
	if !ok {
		return nil, false
	}
	return ele.Value.(*lruCacheItem), true
}

func (c *innerIndexLruCache) del(tblID, idxID int64) {
	v, ok := c.elements[tblID]
	if !ok {
		return
	}
	ele, ok := v[idxID]
	if !ok {
		return
	}
	delete(c.elements[tblID], idxID)
	c.cache.Remove(ele)
}

func (c *innerIndexLruCache) put(tblID, idxID int64, newIdx *statistics.Index, newIdxMem *statistics.IndexMemUsage,
	needEvict, needMove bool) {
	defer func() {
		if needEvict {
			c.evictIfNeeded()
		}
	}()
	if c.capacity < newIdxMem.TrackingMemUsage() {
		newIdx.DropEvicted()
		newIdxMem = newIdx.MemoryUsage()
	}
	v, ok := c.elements[tblID]
	if !ok {
		c.elements[tblID] = make(map[int64]*list.Element)
		v = c.elements[tblID]
	}
	element, exist := v[idxID]
	if exist {
		oldItem := element.Value.(*lruCacheItem)
		oldIdxMemUsage := oldItem.idxMemUsage
		oldItem.index = newIdx
		oldItem.idxMemUsage = newIdxMem
		c.calculateCost(newIdxMem, oldIdxMemUsage)
		if needMove {
			c.cache.MoveToFront(element)
		}
		return
	}
	newItem := &lruCacheItem{
		tblID:       tblID,
		idxID:       idxID,
		index:       newIdx,
		idxMemUsage: newIdxMem,
	}
	newElement := c.cache.PushFront(newItem)
	v[idxID] = newElement
	c.calculateCost(newIdxMem, &statistics.IndexMemUsage{})
}

func (c *innerIndexLruCache) evictIfNeeded() {
	curr := c.cache.Back()
	for c.trackingCost > c.capacity {
		prev := curr.Prev()
		item := curr.Value.(*lruCacheItem)
		oldIdxMem := item.idxMemUsage
		// evict cmSketches
		item.index.DropEvicted()
		newIdxMem := item.index.MemoryUsage()
		c.calculateCost(newIdxMem, oldIdxMem)
		// remove from lru
		c.cache.Remove(curr)
		delete(c.elements[item.tblID], item.idxID)
		if c.onEvict != nil {
			c.onEvict(item.tblID)
		}
		curr = prev
	}
}

func (c *innerIndexLruCache) calculateCost(newUsage, oldUsage *statistics.IndexMemUsage) {
	c.trackingCost += newUsage.TrackingMemUsage() - oldUsage.TrackingMemUsage()
}

func (c *innerIndexLruCache) copy() *innerIndexLruCache {
	newLRU := newInnerIndexLruCache(c.capacity)
	curr := c.cache.Back()
	for curr != nil {
		item := curr.Value.(*lruCacheItem)
		newLRU.put(item.tblID, item.idxID, item.index, item.idxMemUsage, false, false)
		curr = curr.Prev()
	}
	return newLRU
}
