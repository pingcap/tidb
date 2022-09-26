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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"sync"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
)

// applyCache is used in the apply executor. When we get the same value of the outer row.
// We fetch the inner rows in the cache not to fetch them in the inner executor.
type applyCache struct {
	lock        sync.Mutex
	cache       *kvcache.SimpleLRUCache // cache.Get/Put are not thread-safe, so it's protected by the lock above
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
		memCapacity: ctx.GetSessionVars().MemQuotaApplyCache,
		memTracker:  memory.NewTracker(memory.LabelForApplyCache, -1),
	}
	return &c, nil
}

func (c *applyCache) get(key applyCacheKey) (value kvcache.Value, ok bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cache.Get(key)
}

func (c *applyCache) put(key applyCacheKey, val kvcache.Value) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.Put(key, val)
}

// Get gets a cache item according to cache key. It's thread-safe.
func (c *applyCache) Get(key applyCacheKey) (*chunk.List, error) {
	value, hit := c.get(key)
	if !hit {
		return nil, nil
	}
	typedValue := value.(*chunk.List)
	return typedValue, nil
}

// Set inserts an item to the cache. It's thread-safe.
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
	c.put(key, value)
	return true, nil
}

// GetMemTracker returns the memory tracker of this apply cache.
func (c *applyCache) GetMemTracker() *memory.Tracker {
	return c.memTracker
}
