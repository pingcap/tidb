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

	"github.com/pingcap/errors"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	utilpc "github.com/pingcap/tidb/pkg/util/plancache"
	"github.com/pingcap/tidb/pkg/util/syncutil"
)

// planCacheEntry wraps Key and Value. It's the value of list.Element.
type planCacheEntry struct {
	PlanKey   kvcache.Key
	PlanValue kvcache.Value
}

// MemoryUsage return the memory usage of planCacheEntry
func (e *planCacheEntry) MemoryUsage() (sum int64) {
	if e == nil {
		return
	}

	return e.PlanKey.(*planCacheKey).MemoryUsage() + e.PlanValue.(*PlanCacheValue).MemoryUsage()
}

// LRUPlanCache is a dedicated least recently used cache, Only used for plan cache.
type LRUPlanCache struct {
	capacity uint
	size     uint
	// buckets replace the map in general LRU
	buckets map[string]map[*list.Element]struct{}
	lruList *list.List
	// lock make cache thread safe
	lock syncutil.RWMutex
	// onEvict will be called if any eviction happened, only for test use now
	onEvict func(kvcache.Key, kvcache.Value)

	// 0 indicates no quota
	quota uint64
	guard float64

	memoryUsageTotal int64
	sctx             sessionctx.Context
}

// NewLRUPlanCache creates a PCLRUCache object, whose capacity is "capacity".
// NOTE: "capacity" should be a positive value.
func NewLRUPlanCache(capacity uint, guard float64, quota uint64, sctx sessionctx.Context, _ bool) *LRUPlanCache {
	if capacity < 1 {
		capacity = 100
		logutil.BgLogger().Info("capacity of LRU cache is less than 1, will use default value(100) init cache")
	}
	return &LRUPlanCache{
		capacity: capacity,
		size:     0,
		buckets:  make(map[string]map[*list.Element]struct{}, 1), //Generally one query has one plan
		lruList:  list.New(),
		quota:    quota,
		guard:    guard,
		sctx:     sctx,
	}
}

// strHashKey control deep or Shallow copy of string
func strHashKey(key kvcache.Key, deepCopy bool) string {
	if deepCopy {
		return string(key.Hash())
	}
	return string(hack.String(key.Hash()))
}

// Get tries to find the corresponding value according to the given key.
func (l *LRUPlanCache) Get(key kvcache.Key, opts *utilpc.PlanCacheMatchOpts) (value kvcache.Value, ok bool) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	bucket, bucketExist := l.buckets[strHashKey(key, false)]
	if bucketExist {
		if element, exist := l.pickFromBucket(bucket, opts); exist {
			l.lruList.MoveToFront(element)
			return element.Value.(*planCacheEntry).PlanValue, true
		}
	}
	return nil, false
}

// Put puts the (key, value) pair into the LRU Cache.
func (l *LRUPlanCache) Put(key kvcache.Key, value kvcache.Value, opts *utilpc.PlanCacheMatchOpts) {
	l.lock.Lock()
	defer l.lock.Unlock()

	hash := strHashKey(key, true)
	bucket, bucketExist := l.buckets[hash]
	if bucketExist {
		if element, exist := l.pickFromBucket(bucket, opts); exist {
			l.updateInstanceMetric(&planCacheEntry{PlanKey: key, PlanValue: value}, element.Value.(*planCacheEntry))
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
	l.updateInstanceMetric(newCacheEntry, nil)
	if l.size > l.capacity {
		l.removeOldest()
	}
	l.memoryControl()
}

// Delete deletes the multi-values from the LRU Cache.
func (l *LRUPlanCache) Delete(key kvcache.Key) {
	l.lock.Lock()
	defer l.lock.Unlock()

	hash := strHashKey(key, false)
	bucket, bucketExist := l.buckets[hash]
	if bucketExist {
		for element := range bucket {
			l.updateInstanceMetric(nil, element.Value.(*planCacheEntry))
			l.lruList.Remove(element)
			l.size--
		}
		delete(l.buckets, hash)
	}
}

// DeleteAll deletes all elements from the LRU Cache.
func (l *LRUPlanCache) DeleteAll() {
	if l == nil {
		return
	}
	l.lock.Lock()
	defer l.lock.Unlock()

	// update metrics
	if l.sctx.GetSessionVars().EnablePreparedPlanCacheMemoryMonitor {
		core_metrics.GetPlanCacheInstanceMemoryUsage().Sub(float64(l.memoryUsageTotal))
	}
	core_metrics.GetPlanCacheInstanceNumCounter().Sub(float64(l.size))

	// reset all fields
	l.size = 0
	l.buckets = make(map[string]map[*list.Element]struct{}, 1)
	l.lruList = list.New()
	l.memoryUsageTotal = 0
}

// Size gets the current cache size.
func (l *LRUPlanCache) Size() int {
	l.lock.RLock()
	defer l.lock.RUnlock()

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

// MemoryUsage return the memory usage of LRUPlanCache
func (l *LRUPlanCache) MemoryUsage() (sum int64) {
	if l == nil {
		return
	}
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.memoryUsageTotal
}

// Close do some clean work for LRUPlanCache when close the session
func (l *LRUPlanCache) Close() {
	l.DeleteAll()
}

// removeOldest removes the oldest element from the cache.
func (l *LRUPlanCache) removeOldest() {
	lru := l.lruList.Back()
	if lru == nil {
		return
	}
	if l.onEvict != nil {
		l.onEvict(lru.Value.(*planCacheEntry).PlanKey, lru.Value.(*planCacheEntry).PlanValue)
	}

	l.updateInstanceMetric(nil, lru.Value.(*planCacheEntry))
	l.lruList.Remove(lru)
	l.removeFromBucket(lru)
	l.size--
}

// removeFromBucket remove element from bucket
func (l *LRUPlanCache) removeFromBucket(element *list.Element) {
	hash := strHashKey(element.Value.(*planCacheEntry).PlanKey, false)
	bucket := l.buckets[hash]
	delete(bucket, element)
	if len(bucket) == 0 {
		delete(l.buckets, hash)
	}
}

// memoryControl control the memory by quota and guard
func (l *LRUPlanCache) memoryControl() {
	if l.quota == 0 || l.guard == 0 {
		return
	}

	memUsed, _ := memory.InstanceMemUsed()
	for memUsed > uint64(float64(l.quota)*(1.0-l.guard)) && l.size > 0 {
		l.removeOldest()
		memUsed, _ = memory.InstanceMemUsed()
	}
}

// PickPlanFromBucket pick one plan from bucket
func (l *LRUPlanCache) pickFromBucket(bucket map[*list.Element]struct{}, matchOpts *utilpc.PlanCacheMatchOpts) (*list.Element, bool) {
	for k := range bucket {
		if matchOpts == nil { // for PointGet Plan
			return k, true
		}

		plan := k.Value.(*planCacheEntry).PlanValue.(*PlanCacheValue)
		// check param types' compatibility
		ok1 := checkTypesCompatibility4PC(plan.matchOpts.ParamTypes, matchOpts.ParamTypes)
		if !ok1 {
			continue
		}

		// check limit offset and key if equal and check switch if enabled
		ok2 := checkUint64SliceIfEqual(plan.matchOpts.LimitOffsetAndCount, matchOpts.LimitOffsetAndCount)
		if !ok2 {
			continue
		}
		if len(plan.matchOpts.LimitOffsetAndCount) > 0 && !l.sctx.GetSessionVars().EnablePlanCacheForParamLimit {
			// offset and key slice matched, but it is a plan with param limit and the switch is disabled
			continue
		}
		// check subquery switch state
		if plan.matchOpts.HasSubQuery && !l.sctx.GetSessionVars().EnablePlanCacheForSubquery {
			continue
		}
		// table stats has changed
		// this check can be disabled by turning off system variable tidb_plan_cache_invalidation_on_fresh_stats
		if l.sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats &&
			plan.matchOpts.StatsVersionHash != matchOpts.StatsVersionHash {
			continue
		}

		// below are some SQL variables that can affect the plan
		if plan.matchOpts.ForeignKeyChecks != matchOpts.ForeignKeyChecks {
			continue
		}
		return k, true
	}
	return nil, false
}

func checkUint64SliceIfEqual(a, b []uint64) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// updateInstanceMetric update the memory usage and plan num for show in grafana
func (l *LRUPlanCache) updateInstanceMetric(in, out *planCacheEntry) {
	updateInstancePlanNum(in, out)
	if l == nil || !l.sctx.GetSessionVars().EnablePreparedPlanCacheMemoryMonitor {
		return
	}

	if in != nil && out != nil { // replace plan
		core_metrics.GetPlanCacheInstanceMemoryUsage().Sub(float64(out.MemoryUsage()))
		core_metrics.GetPlanCacheInstanceMemoryUsage().Add(float64(in.MemoryUsage()))
		l.memoryUsageTotal += in.MemoryUsage() - out.MemoryUsage()
	} else if in != nil { // put plan
		core_metrics.GetPlanCacheInstanceMemoryUsage().Add(float64(in.MemoryUsage()))
		l.memoryUsageTotal += in.MemoryUsage()
	} else { // delete plan
		core_metrics.GetPlanCacheInstanceMemoryUsage().Sub(float64(out.MemoryUsage()))
		l.memoryUsageTotal -= out.MemoryUsage()
	}
}

// updateInstancePlanNum update the plan num
func updateInstancePlanNum(in, out *planCacheEntry) {
	if in != nil && out != nil { // replace plan
		return
	} else if in != nil { // put plan
		core_metrics.GetPlanCacheInstanceNumCounter().Add(1)
	} else { // delete plan
		core_metrics.GetPlanCacheInstanceNumCounter().Sub(1)
	}
}
