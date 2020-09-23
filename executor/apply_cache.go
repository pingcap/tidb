// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
)

// applyCache is used in the apply executor. When we get the same value of the outer row.
// We fetch the inner rows in the cache not to fetch them in the inner executor.
type applyCache struct {
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	memTracker  *memory.Tracker // track memory usage.
}

type applyCacheKey []byte

func (key applyCacheKey) Hash() []byte {
	return key
}

func applyCacheKVMem(key applyCacheKey, value *chunk.List) int64 {
	return int64(len(key)) + value.GetMemTracker().BytesConsumed()
}

func newApplyCache(ctx sessionctx.Context) (*applyCache, error) {
	// since applyCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(mathutil.MaxUint, 0.1, 0)
	c := applyCache{
		cache:       cache,
		memCapacity: ctx.GetSessionVars().NestedLoopJoinCacheCapacity,
		memTracker:  memory.NewTracker(memory.LabelForApplyCache, -1),
	}
	return &c, nil
}

// Get gets a cache item according to cache key.
func (c *applyCache) Get(key applyCacheKey) (*chunk.List, error) {
	value, hit := c.cache.Get(&key)
	if !hit {
		return nil, nil
	}
	typedValue := value.(*chunk.List)
	return typedValue, nil
}

// Set inserts an item to the cache.
func (c *applyCache) Set(key applyCacheKey, value *chunk.List) (bool, error) {
	mem := applyCacheKVMem(key, value)
	if mem > c.memCapacity { // ignore this kv pair if its size is too large
		return false, nil
	}
	for mem+c.memTracker.BytesConsumed() > c.memCapacity {
		evictedKey, evictedValue, evicted := c.cache.RemoveOldest()
		if !evicted {
			return false, nil
		}
		c.memTracker.Consume(-applyCacheKVMem(evictedKey.(applyCacheKey), evictedValue.(*chunk.List)))
	}
	c.memTracker.Consume(mem)
	c.cache.Put(key, value)
	return true, nil
}

// GetMemTracker returns the memory tracker of this apply cache.
func (c *applyCache) GetMemTracker() *memory.Tracker {
	return c.memTracker
}
