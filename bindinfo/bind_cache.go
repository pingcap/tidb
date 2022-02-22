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

package bindinfo

import (
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
)

// bindCache uses the LRU cache to store the bindRecord.
// The key of the LRU cache is original sql, the value is a slice of BindRecord.
// Note: The bindCache is not thread-safe.
type bindCache struct {
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	memTracker  *memory.Tracker // track memory usage.
}

type bindCacheKey string

func (key bindCacheKey) Hash() []byte {
	return hack.Slice(string(key))
}

func bindCacheKVMem(key bindCacheKey, value []*BindRecord) int64 {
	var valMem int64
	for _, bindRecord := range value {
		valMem += int64(bindRecord.size())
	}
	return int64(len(key.Hash())) + valMem
}

func newBindCache(memCapacity int64) *bindCache {
	// since bindCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(mathutil.MaxUint, 0.1, 0)
	c := bindCache{
		cache:       cache,
		memCapacity: memCapacity,
		memTracker:  memory.NewTracker(memory.LabelForBindCache, -1),
	}
	return &c
}

// get gets a cache item according to cache key. It's not thread-safe.
// Only other functions of the bindCache can use this function.
// The return value is not read-only, but it is only can be used in other functions which are also in the bind_cache.go.
func (c *bindCache) get(key bindCacheKey) []*BindRecord {
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.([]*BindRecord)
	return typedValue
}

// set inserts an item to the cache. It's not thread-safe.
// Only other functions of the bindCache can use this function.
func (c *bindCache) set(key bindCacheKey, value []*BindRecord) bool {
	mem := bindCacheKVMem(key, value)
	if mem > c.memCapacity { // ignore this kv pair if its size is too large
		return false
	}
	bindRecords := c.get(key)
	if bindRecords != nil {
		// Remove the origin key-value pair.
		mem -= bindCacheKVMem(key, bindRecords)
	}
	for mem+c.memTracker.BytesConsumed() > c.memCapacity {
		evictedKey, evictedValue, evicted := c.cache.RemoveOldest()
		if !evicted {
			return false
		}
		c.memTracker.Consume(-bindCacheKVMem(evictedKey.(bindCacheKey), evictedValue.([]*BindRecord)))
	}
	c.memTracker.Consume(mem)
	c.cache.Put(key, value)
	return true
}

// delete remove an item from the cache. It's not thread-safe.
// Only other functions of the bindCache can use this function.
func (c *bindCache) delete(key bindCacheKey) bool {
	bindRecords := c.get(key)
	if bindRecords != nil {
		mem := bindCacheKVMem(key, bindRecords)
		c.cache.Delete(key)
		c.memTracker.Consume(-mem)
	}
	return true
}

// The return value is not read-only, but it shouldn't be changed in the caller functions.
func (c bindCache) getBindRecord(hash, normdOrigSQL, db string) *BindRecord {
	bindRecords := c.get(bindCacheKey(hash))
	for _, bindRecord := range bindRecords {
		if bindRecord.OriginalSQL == normdOrigSQL {
			return bindRecord
		}
	}
	return nil
}

// getAllBindRecords return all the bindRecords from the bindCache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
func (c *bindCache) getAllBindRecords() []*BindRecord {
	values := c.cache.Values()
	var bindRecords []*BindRecord
	for _, vals := range values {
		bindRecords = append(bindRecords, vals.([]*BindRecord)...)
	}
	return bindRecords
}

func (c bindCache) setBindRecord(hash string, meta *BindRecord) {
	cacheKey := bindCacheKey(hash)
	metas := c.get(cacheKey)
	for i := range metas {
		if metas[i].OriginalSQL == meta.OriginalSQL {
			metas[i] = meta
		}
	}
	c.set(cacheKey, []*BindRecord{meta})
}

// removeDeletedBindRecord removes the BindRecord which has same originSQL with specified BindRecord.
func (c *bindCache) removeDeletedBindRecord(hash string, meta *BindRecord) {
	metas := c.get(bindCacheKey(hash))
	if metas == nil {
		return
	}

	for i := len(metas) - 1; i >= 0; i-- {
		if metas[i].isSame(meta) {
			metas[i] = metas[i].remove(meta)
			if len(metas[i].Bindings) == 0 {
				metas = append(metas[:i], metas[i+1:]...)
			}
			if len(metas) == 0 {
				c.delete(bindCacheKey(hash))
				return
			}
		}
	}
	c.set(bindCacheKey(hash), metas)
}

func (c *bindCache) setMemCapacity(capacity int64) {
	// Only change the capacity size without affecting the cached bindRecord
	c.memCapacity = capacity
}

func (c *bindCache) getMemCapacity() int64 {
	return c.memCapacity
}

func (c bindCache) copy() *bindCache {
	newCache := newBindCache(c.memCapacity)
	keys := c.cache.Keys()
	for _, key := range keys {
		cacheKey := key.(bindCacheKey)
		v := c.get(cacheKey)
		bindRecords := make([]*BindRecord, len(v))
		copy(bindRecords, v)
		newCache.set(cacheKey, bindRecords)
	}
	return newCache
}
