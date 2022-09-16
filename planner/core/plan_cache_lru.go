// Package core Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package core

import (
	"container/list"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
)

// planCacheEntry wraps Key and Value. It's the value of list.Element.
type planCacheEntry struct {
	PlanKey   kvcache.Key
	PlanValue kvcache.Value
}

// LRUPlanCache is a dedicated least recently used cache, Only used for plan cache.
type LRUPlanCache struct {
	capacity uint
	size     uint
	// buckets replace the map in general LRU
	buckets map[string]map[*list.Element]struct{}
	lruList *list.List
	// lock make cache thread safe
	lock sync.Mutex

	// pickFromBucket get one element from bucket. The LRUPlanCache can not work if it is nil
	pickFromBucket func(map[*list.Element]struct{}, []*types.FieldType) (*list.Element, bool)
	// onEvict will be called if any eviction happened, only for test use now
	onEvict func(kvcache.Key, kvcache.Value)

	// 0 indicates no quota
	quota uint64
	guard float64
}

// NewLRUPlanCache creates a PCLRUCache object, whose capacity is "capacity".
// NOTE: "capacity" should be a positive value.
func NewLRUPlanCache(capacity uint, guard float64, quota uint64,
	pickFromBucket func(map[*list.Element]struct{}, []*types.FieldType) (*list.Element, bool)) *LRUPlanCache {
	if capacity < 1 {
		capacity = 100
		logutil.BgLogger().Info("capacity of LRU cache is less than 1, will use default value(100) init cache")
	}
	return &LRUPlanCache{
		capacity:       capacity,
		size:           0,
		buckets:        make(map[string]map[*list.Element]struct{}, 1), //Generally one query has one plan
		lruList:        list.New(),
		pickFromBucket: pickFromBucket,
		quota:          quota,
		guard:          guard,
	}
}

// Get tries to find the corresponding value according to the given key.
func (l *LRUPlanCache) Get(key kvcache.Key, paramTypes []*types.FieldType) (value kvcache.Value, ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()

	bucket, bucketExist := l.buckets[string(hack.String(key.Hash()))]
	if bucketExist {
		if element, exist := l.pickFromBucket(bucket, paramTypes); exist {
			l.lruList.MoveToFront(element)
			return element.Value.(*planCacheEntry).PlanValue, true
		}
	}
	return nil, false
}

// Put puts the (key, value) pair into the LRU Cache.
func (l *LRUPlanCache) Put(key kvcache.Key, value kvcache.Value, paramTypes []*types.FieldType) {
	l.lock.Lock()
	defer l.lock.Unlock()

	hash := string(key.Hash())
	bucket, bucketExist := l.buckets[hash]
	if bucketExist {
		if element, exist := l.pickFromBucket(bucket, paramTypes); exist {
			element.Value.(*planCacheEntry).PlanValue = value
			l.lruList.MoveToFront(element)
			return
		}
	} else {
		l.buckets[hash] = make(map[*list.Element]struct{}, 1)
	}

	newCacheEntry := &planCacheEntry{
		PlanKey:   key,
		PlanValue: value,
	}
	element := l.lruList.PushFront(newCacheEntry)
	l.buckets[hash][element] = struct{}{}
	l.size++
	if l.size > l.capacity {
		l.removeOldest()
	}
	l.memoryControl()
}

// Delete deletes the multi-values from the LRU Cache.
func (l *LRUPlanCache) Delete(key kvcache.Key) {
	l.lock.Lock()
	defer l.lock.Unlock()

	hash := hack.String(key.Hash())
	bucket, bucketExist := l.buckets[string(hash)]
	if bucketExist {
		for element := range bucket {
			l.lruList.Remove(element)
			l.size--
		}
		delete(l.buckets, string(hash))
	}
}

// DeleteAll deletes all elements from the LRU Cache.
func (l *LRUPlanCache) DeleteAll() {
	l.lock.Lock()
	defer l.lock.Unlock()

	for lru := l.lruList.Back(); lru != nil; lru = l.lruList.Back() {
		l.lruList.Remove(lru)
		l.size--
	}
	l.buckets = make(map[string]map[*list.Element]struct{}, 1)
}

// Size gets the current cache size.
func (l *LRUPlanCache) Size() int {
	l.lock.Lock()
	defer l.lock.Unlock()

	return int(l.size)
}

// SetCapacity sets capacity of the cache.
func (l *LRUPlanCache) SetCapacity(capacity uint) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	if capacity < 1 {
		return errors.New("capacity of LRU cache should be at least 1")
	}
	l.capacity = capacity
	for l.size > l.capacity {
		l.removeOldest()
	}
	return nil
}

// removeOldest removes the oldest element from the cache.
func (l *LRUPlanCache) removeOldest() {
	lru := l.lruList.Back()
	if l.onEvict != nil {
		l.onEvict(lru.Value.(*planCacheEntry).PlanKey, lru.Value.(*planCacheEntry).PlanValue)
	}

	l.lruList.Remove(lru)
	l.removeFromBucket(lru)
	l.size--
}

// removeFromBucket remove element from bucket
func (l *LRUPlanCache) removeFromBucket(element *list.Element) {
	bucket := l.buckets[string(hack.String(element.Value.(*planCacheEntry).PlanKey.Hash()))]
	delete(bucket, element)
}

// memoryControl control the memory by quota and guard
func (l *LRUPlanCache) memoryControl() {
	if l.quota == 0 || l.guard == 0 {
		return
	}

	memUsed, _ := memory.InstanceMemUsed()
	for memUsed > uint64(float64(l.quota)*(1.0-l.guard)) {
		l.removeOldest()
		memUsed, _ = memory.InstanceMemUsed()
	}
}

// PickPlanFromBucket pick one plan from bucket
func PickPlanFromBucket(bucket map[*list.Element]struct{}, paramTypes []*types.FieldType) (*list.Element, bool) {
	for k := range bucket {
		plan := k.Value.(*planCacheEntry).PlanValue.(*PlanCacheValue)
		if plan.ParamTypes.CheckTypesCompatibility4PC(paramTypes) {
			return k, true
		}
	}
	return nil, false
}
