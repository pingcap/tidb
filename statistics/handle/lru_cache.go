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

type lruCacheItem struct {
	tblID       int64
	column      *statistics.Column
	colMemUsage *statistics.ColumnMemUsage
}

type lruMapElement struct {
	tbl         *statistics.Table
	tblMemUsage *statistics.TableMemoryUsage
	// colElements records the columns which stored in the lru cache
	colElements map[int64]*list.Element
}

// internalLRUCache is a simple least recently used cache
type internalLRUCache struct {
	sync.RWMutex
	capacity int64
	// trackingCost records the tracking memory usage of the elements stored in the internalLRUCache
	// trackingCost should be kept under capacity by evict policy
	trackingCost int64
	// totalCost records the total memory usage of the elements stored in the internalLRUCache
	totalCost int64
	elements  map[int64]*lruMapElement
	// cache maintains elements in list.
	// Note that if the element's trackingMemUsage is 0, it will be removed from cache in order to keep cache not too long
	cache *list.List
}

// newInternalLRUCache returns internalLRUCache
func newInternalLRUCache(capacity int64) *internalLRUCache {
	if capacity < 1 {
		panic("capacity of LRU Cache should be at least 1.")
	}
	return &internalLRUCache{
		capacity: capacity,
		elements: make(map[int64]*lruMapElement),
		cache:    list.New(),
	}
}

func (l *internalLRUCache) Get(key int64) (*statistics.Table, bool) {
	l.Lock()
	defer l.Unlock()
	return l.get(key, true)
}

func (l *internalLRUCache) get(key int64, move bool) (*statistics.Table, bool) {
	element, exists := l.elements[key]
	if !exists {
		return nil, false
	}
	if move {
		for _, col := range element.colElements {
			l.cache.MoveToFront(col)
		}
	}
	return element.tbl, true
}

func (l *internalLRUCache) Put(key int64, value *statistics.Table) {
	l.Lock()
	defer l.Unlock()
	l.put(key, value, value.MemoryUsage(), true, false)
}

func (l *internalLRUCache) put(tblID int64, tbl *statistics.Table, tblMemUsage *statistics.TableMemoryUsage,
	tryEvict, move bool) {
	// If the item TotalColTrackingMemUsage is larger than capacity, we will drop some structures in order to put it in cache
	for l.capacity < tblMemUsage.TotalColTrackingMemUsage() {
		for _, col := range tbl.Columns {
			col.DropEvicted()
			tblMemUsage = tbl.MemoryUsage()
			if l.capacity >= tblMemUsage.TotalColTrackingMemUsage() {
				break
			}
		}
	}

	element, exists := l.elements[tblID]
	if exists {
		oldMemUsage := element.tblMemUsage
		element.tbl = tbl
		element.tblMemUsage = tblMemUsage
		l.updateColElements(element, tblID, tbl, tblMemUsage, move)
		l.calculateCost(tblMemUsage, oldMemUsage)
		return
	}
	element = &lruMapElement{
		tbl:         tbl,
		tblMemUsage: tblMemUsage,
		colElements: make(map[int64]*list.Element, 0),
	}
	insertCols := make([]*lruCacheItem, 0)
	for colID, col := range tbl.Columns {
		insertCols = append(insertCols, &lruCacheItem{
			tblID:       tblID,
			column:      col,
			colMemUsage: tblMemUsage.ColumnsMemUsage[colID],
		})
	}
	l.insertElements(insertCols, element)
	l.elements[tblID] = element
	l.calculateCost(tblMemUsage, &statistics.TableMemoryUsage{})

	if tryEvict {
		l.evictIfNeeded()
	}
}

func (l *internalLRUCache) Del(key int64) {
	l.Lock()
	defer l.Unlock()
	l.del(key)
}

func (l *internalLRUCache) del(key int64) bool {
	element, exists := l.elements[key]
	if !exists {
		return false
	}
	for _, col := range element.colElements {
		l.cache.Remove(col)
	}
	delete(l.elements, key)
	l.calculateCost(&statistics.TableMemoryUsage{}, element.tblMemUsage)
	return true
}

// Cost returns the current cost
func (l *internalLRUCache) Cost() int64 {
	l.RLock()
	defer l.RUnlock()
	return l.totalCost
}

// Keys returns the current Keys
func (l *internalLRUCache) Keys() []int64 {
	l.RLock()
	defer l.RUnlock()
	r := make([]int64, 0, len(l.elements))
	for tblID := range l.elements {
		r = append(r, tblID)
	}
	return r
}

// Values returns the current Values
func (l *internalLRUCache) Values() []*statistics.Table {
	l.RLock()
	defer l.RUnlock()
	r := make([]*statistics.Table, 0, len(l.elements))
	for _, v := range l.elements {
		r = append(r, v.tbl)
	}
	return r
}

// Map returns the map of table statistics
func (l *internalLRUCache) Map() map[int64]*statistics.Table {
	l.RLock()
	defer l.RUnlock()
	r := make(map[int64]*statistics.Table, len(l.elements))
	for k, v := range l.elements {
		r[k] = v.tbl
	}
	return r
}

// Len returns the current length
func (l *internalLRUCache) Len() int {
	l.RLock()
	defer l.RUnlock()
	return len(l.elements)
}

// FreshMemUsage re-calculate the memory message
func (l *internalLRUCache) FreshMemUsage() {
	l.Lock()
	defer l.Unlock()
	for tblID, element := range l.elements {
		l.freshTableCost(tblID, element, false)
	}
	l.evictIfNeeded()
}

// FreshTableCost re-calculate the memory message for the certain key
func (l *internalLRUCache) FreshTableCost(tblID int64) {
	l.Lock()
	defer l.Unlock()
	element, ok := l.elements[tblID]
	if !ok {
		return
	}
	l.freshTableCost(tblID, element, true)
}

func (l *internalLRUCache) freshTableCost(tblID int64, element *lruMapElement, evict bool) {
	oldMemUsage := element.tblMemUsage
	newMemUsage := element.tbl.MemoryUsage()
	element.tblMemUsage = newMemUsage
	l.calculateCost(newMemUsage, oldMemUsage)
	l.updateColElements(element, tblID, element.tbl, newMemUsage, false)
	if evict {
		l.evictIfNeeded()
	}
}

// Copy returns a replication of LRU
// Note that Copy will also maintain the lru order in new lru cache
func (l *internalLRUCache) Copy() statsCacheInner {
	l.RLock()
	defer l.RUnlock()
	newCache := newInternalLRUCache(l.capacity)
	node := l.cache.Back()
	for node != nil {
		tblID := node.Value.(*lruCacheItem).tblID
		tblElement := l.elements[tblID]
		l.put(tblID, tblElement.tbl, tblElement.tblMemUsage, false, false)
	}
	return newCache
}

// updateTable will updates element cost and columns Elements
func (l *internalLRUCache) updateColElements(element *lruMapElement, tblID int64, tbl *statistics.Table, tblMemUsage *statistics.TableMemoryUsage, move bool) {
	// deletedColElements indicates the elements needs to be removed from the list
	deletedColElements := make(map[int64]*list.Element, 0)
	// updatedCols indicates the elements needs to be moved in the list
	updatedCols := make([]*list.Element, 0)
	// insertCols indicates the elements needs to be inserted into the list
	insertCols := make([]*lruCacheItem, 0)

	// Here indicates some columns needs to be removed after updated comparing old table and new table
	oldColElements := element.colElements
	for colID, oldColElement := range oldColElements {
		_, exists := tbl.Columns[colID]
		if !exists {
			deletedColElements[colID] = oldColElement
		}
	}

	for colID, col := range tbl.Columns {
		newColMemUsage := tblMemUsage.ColumnsMemUsage[colID]
		colElement, ok := element.colElements[colID]
		if ok {
			item := colElement.Value.(*lruCacheItem)
			item.column = col
			item.colMemUsage = newColMemUsage
			if newColMemUsage.TrackingMemUsage() > 0 {
				updatedCols = append(updatedCols, colElement)
			} else {
				// Here indicates the updated column element's TrackingMemUsage becomes 0,
				// thus we need to remove it from list
				deletedColElements[colID] = colElement
			}
		} else {
			if newColMemUsage.TrackingMemUsage() > 0 {
				insertCols = append(insertCols, &lruCacheItem{
					tblID:       tblID,
					column:      col,
					colMemUsage: newColMemUsage,
				})
			}
		}
	}
	l.removeElements(deletedColElements, element)
	l.updateElements(updatedCols, move)
	l.insertElements(insertCols, element)
}

func (l *internalLRUCache) removeElements(delElements map[int64]*list.Element, m *lruMapElement) {
	for colID, del := range delElements {
		l.cache.Remove(del)
		delete(m.colElements, colID)
	}
}

func (l *internalLRUCache) updateElements(updates []*list.Element, move bool) {
	if move {
		for _, update := range updates {
			l.cache.MoveToFront(update)
		}
	}
}

func (l *internalLRUCache) insertElements(inserts []*lruCacheItem, m *lruMapElement) {
	for _, insert := range inserts {
		newElement := l.cache.PushFront(insert)
		m.colElements[insert.column.Info.ID] = newElement
	}
}

func (l *internalLRUCache) evictIfNeeded() {
	curr := l.cache.Back()
	for !l.underCapacity() {
		prev := curr.Prev()
		item := curr.Value.(*lruCacheItem)
		col := item.column
		col.DropEvicted()
		l.cache.Remove(curr)
		delete(l.elements[item.tblID].colElements, col.Info.ID)
		l.calculateColCost(col.MemoryUsage(), item.colMemUsage)
		curr = prev
	}
}

func (l *internalLRUCache) underCapacity() bool {
	return l.trackingCost <= l.capacity
}

func (l *internalLRUCache) calculateCost(newUsage, oldUsage *statistics.TableMemoryUsage) {
	l.totalCost += newUsage.TotalMemUsage - oldUsage.TotalMemUsage
	l.trackingCost += newUsage.TotalColTrackingMemUsage() - oldUsage.TotalColTrackingMemUsage()
}

func (l *internalLRUCache) calculateColCost(newUsage, oldUsage *statistics.ColumnMemUsage) {
	l.totalCost += newUsage.TotalMemUsage - oldUsage.TotalMemUsage
	l.trackingCost += newUsage.TrackingMemUsage() - oldUsage.TrackingMemUsage()
}
