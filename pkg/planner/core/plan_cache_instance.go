// Copyright 2024 PingCAP, Inc.
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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"go.uber.org/atomic"
)

func init() {
	domain.NewInstancePlanCache = func(softMemLimit, hardMemLimit int64) sessionctx.InstancePlanCache {
		return NewInstancePlanCache(softMemLimit, hardMemLimit)
	}
}

// NewInstancePlanCache creates a new instance level plan cache.
func NewInstancePlanCache(softMemLimit, hardMemLimit int64) sessionctx.InstancePlanCache {
	planCache := new(instancePlanCache)
	planCache.softMemLimit.Store(softMemLimit)
	planCache.hardMemLimit.Store(hardMemLimit)
	return planCache
}

type instancePCNode struct {
	value    *PlanCacheValue
	lastUsed atomic.Time
	next     atomic.Pointer[instancePCNode]
}

// instancePlanCache is a lock-free implementation of InstancePlanCache interface.
// [key1] --> [headNode1] --> [node1] --> [node2] --> [node3]
// [key2] --> [headNode2] --> [node4] --> [node5]
// [key3] --> [headNode3] --> [node6] --> [node7] --> [node8]
// headNode.value is always empty, headNode is designed to make it easier to implement.
type instancePlanCache struct {
	heads   sync.Map
	totCost atomic.Int64
	totPlan atomic.Int64

	evictMutex   sync.Mutex
	inEvict      atomic.Bool
	softMemLimit atomic.Int64
	hardMemLimit atomic.Int64
}

func (pc *instancePlanCache) getHead(key string, create bool) *instancePCNode {
	headNode, ok := pc.heads.Load(key)
	if ok { // cache hit
		return headNode.(*instancePCNode)
	}
	if !create { // cache miss
		return nil
	}
	newHeadNode := pc.createNode(nil)
	actual, _ := pc.heads.LoadOrStore(key, newHeadNode)
	if headNode, ok := actual.(*instancePCNode); ok { // for safety
		return headNode
	}
	return nil
}

// Get gets the cached value according to key and paramTypes.
func (pc *instancePlanCache) Get(key string, paramTypes any) (value any, ok bool) {
	headNode := pc.getHead(key, false)
	if headNode == nil { // cache miss
		return nil, false
	}
	return pc.getPlanFromList(headNode, paramTypes)
}

func (pc *instancePlanCache) getPlanFromList(headNode *instancePCNode, paramTypes any) (any, bool) {
	for node := headNode.next.Load(); node != nil; node = node.next.Load() {
		if checkTypesCompatibility4PC(node.value.paramTypes, paramTypes) { // v.Plan is read-only, no need to lock
			if !pc.inEvict.Load() {
				node.lastUsed.Store(time.Now()) // atomically update the lastUsed field
			}
			return node.value, true
		}
	}
	return nil, false
}

// Put puts the key and values into the cache.
// Due to some thread-safety issues, this Put operation might fail, use the returned succ to indicate it.
func (pc *instancePlanCache) Put(key string, value, paramTypes any) (succ bool) {
	if pc.inEvict.Load() {
		return // do nothing if eviction is in progress
	}
	vMem := value.(*PlanCacheValue).MemoryUsage()
	if vMem+pc.totCost.Load() > pc.hardMemLimit.Load() {
		return // do nothing if it exceeds the hard limit
	}
	headNode := pc.getHead(key, true)
	if headNode == nil {
		return false // for safety
	}
	if _, ok := pc.getPlanFromList(headNode, paramTypes); ok {
		return // some other thread has inserted the same plan before
	}
	if pc.inEvict.Load() {
		return // do nothing if eviction is in progress
	}

	firstNode := headNode.next.Load()
	currNode := pc.createNode(value)
	currNode.next.Store(firstNode)
	if headNode.next.CompareAndSwap(firstNode, currNode) { // if failed, some other thread has updated this node,
		pc.totCost.Add(vMem) // then skip this Put and wait for the next time.
		pc.totPlan.Add(1)
		succ = true
	}
	return
}

// Evict evicts some values. There should be a background thread to perform the eviction.
// step 1: iterate all values to collect their last_used
// step 2: estimate an eviction threshold time based on all last_used values
// step 3: iterate all values again and evict qualified values
func (pc *instancePlanCache) Evict(evictAll bool) (detailInfo string, numEvicted int) {
	pc.evictMutex.Lock() // make sure only one thread to trigger eviction for safety
	defer pc.evictMutex.Unlock()
	pc.inEvict.Store(true)
	defer pc.inEvict.Store(false)
	currentTot, softLimit := pc.totCost.Load(), pc.softMemLimit.Load()
	if !evictAll && currentTot < softLimit {
		detailInfo = fmt.Sprintf("memory usage is below the soft limit, currentTot: %v, softLimit: %v", currentTot, softLimit)
		return
	}
	lastUsedTimes := make([]time.Time, 0, 64)
	pc.foreach(func(_, this *instancePCNode) bool { // step 1
		lastUsedTimes = append(lastUsedTimes, this.lastUsed.Load())
		return false
	})
	var threshold time.Time
	if evictAll {
		threshold = time.Now().Add(time.Hour * 24) // a future time
	} else {
		threshold = pc.calcEvictionThreshold(lastUsedTimes) // step 2
	}
	detailInfo = fmt.Sprintf("evict threshold: %v", threshold)
	pc.foreach(func(prev, this *instancePCNode) bool { // step 3
		if !this.lastUsed.Load().After(threshold) { // if lastUsed<=threshold, evict this value
			if prev.next.CompareAndSwap(this, this.next.Load()) { // have to use CAS since
				pc.totCost.Sub(this.value.MemoryUsage()) //  it might have been updated by other thread
				pc.totPlan.Sub(1)
				numEvicted++
				return true
			}
		}
		return false
	})

	// post operation: clear empty heads in pc.Heads
	keys, headNodes := pc.headNodes()
	for i, headNode := range headNodes {
		if headNode.next.Load() == nil {
			pc.heads.Delete(keys[i])
		}
	}
	return
}

// MemUsage returns the memory usage of this plan cache.
func (pc *instancePlanCache) MemUsage() int64 {
	return pc.totCost.Load()
}

// Size returns the number of plans in this plan cache.
func (pc *instancePlanCache) Size() int64 {
	return pc.totPlan.Load()
}

func (pc *instancePlanCache) calcEvictionThreshold(lastUsedTimes []time.Time) (t time.Time) {
	if len(lastUsedTimes) == 0 {
		return
	}
	totCost, softMemLimit := pc.totCost.Load(), pc.softMemLimit.Load()
	avgPerPlan := totCost / int64(len(lastUsedTimes))
	if avgPerPlan <= 0 {
		return
	}
	memToRelease := totCost - softMemLimit
	// (... +avgPerPlan-1) is used to try to keep the final memory usage below the soft mem limit.
	numToEvict := (memToRelease + avgPerPlan - 1) / avgPerPlan
	if numToEvict <= 0 {
		return
	}
	sort.Slice(lastUsedTimes, func(i, j int) bool {
		return lastUsedTimes[i].Before(lastUsedTimes[j])
	})
	if len(lastUsedTimes) < int(numToEvict) {
		return // for safety, avoid index-of-range panic
	}
	return lastUsedTimes[numToEvict-1]
}

func (pc *instancePlanCache) foreach(callback func(prev, this *instancePCNode) (thisRemoved bool)) {
	_, headNodes := pc.headNodes()
	for _, headNode := range headNodes {
		for prev, this := headNode, headNode.next.Load(); this != nil; {
			thisRemoved := callback(prev, this)
			if !thisRemoved { // this node is removed, no need to update the prev node in this case
				prev, this = this, this.next.Load()
			} else {
				this = this.next.Load()
			}
		}
	}
}

func (pc *instancePlanCache) headNodes() ([]string, []*instancePCNode) {
	keys := make([]string, 0, 64)
	headNodes := make([]*instancePCNode, 0, 64)
	pc.heads.Range(func(k, v any) bool {
		keys = append(keys, k.(string))
		headNodes = append(headNodes, v.(*instancePCNode))
		return true
	})
	return keys, headNodes
}

func (*instancePlanCache) createNode(value any) *instancePCNode {
	node := new(instancePCNode)
	if value != nil {
		node.value = value.(*PlanCacheValue)
	}
	node.lastUsed.Store(time.Now())
	return node
}

// GetLimits gets the memory limit of this plan cache.
func (pc *instancePlanCache) GetLimits() (softLimit, hardLimit int64) {
	return pc.softMemLimit.Load(), pc.hardMemLimit.Load()
}

// SetLimits sets the memory limit of this plan cache.
func (pc *instancePlanCache) SetLimits(softLimit, hardLimit int64) {
	pc.softMemLimit.Store(softLimit)
	pc.hardMemLimit.Store(hardLimit)
}
