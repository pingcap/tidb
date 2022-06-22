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
	"errors"
	"sync"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
)

// bindCache uses the LRU cache to store the bindRecord.
// The key of the LRU cache is original sql, the value is a slice of BindRecord.
// Note: The bindCache should be accessed with lock.
type bindCache struct {
	lock        sync.Mutex
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	memTracker  *memory.Tracker // track memory usage.
}

type bindCacheKey string

func (key bindCacheKey) Hash() []byte {
	return hack.Slice(string(key))
}

func calcBindCacheKVMem(key bindCacheKey, value []*BindRecord) int64 {
	var valMem int64
	for _, bindRecord := range value {
		valMem += int64(bindRecord.size())
	}
	return int64(len(key.Hash())) + valMem
}

func newBindCache() *bindCache {
	// since bindCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(mathutil.MaxUint, 0, 0)
	c := bindCache{
		cache:       cache,
		memCapacity: variable.MemQuotaBindingCache.Load(),
		memTracker:  memory.NewTracker(memory.LabelForBindCache, -1),
	}
	return &c
}

// get gets a cache item according to cache key. It's not thread-safe.
// Note: Only other functions of the bindCache file can use this function.
// Don't use this function directly in other files in bindinfo package.
// The return value is not read-only, but it is only can be used in other functions which are also in the bind_cache.go.
func (c *bindCache) get(key bindCacheKey) []*BindRecord {
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.([]*BindRecord)
	return typedValue
}

// getCopiedVal gets a copied cache item according to cache key.
// The return value can be modified.
// If you want to modify the return value, use the 'getCopiedVal' function rather than 'get' function.
// We use the copy on write way to operate the bindRecord in cache for safety and accuracy of memory usage.
func (c *bindCache) getCopiedVal(key bindCacheKey) []*BindRecord {
	bindRecords := c.get(key)
	if bindRecords != nil {
		copiedRecords := make([]*BindRecord, len(bindRecords))
		for i, bindRecord := range bindRecords {
			copiedRecords[i] = bindRecord.shallowCopy()
		}
		return copiedRecords
	}
	return bindRecords
}

// set inserts an item to the cache. It's not thread-safe.
// Only other functions of the bindCache can use this function.
// The set operation will return error message when the memory usage of binding_cache exceeds its capacity.
func (c *bindCache) set(key bindCacheKey, value []*BindRecord) (ok bool, err error) {
	mem := calcBindCacheKVMem(key, value)
	if mem > c.memCapacity { // ignore this kv pair if its size is too large
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
		return
	}
	bindRecords := c.get(key)
	if bindRecords != nil {
		// Remove the origin key-value pair.
		mem -= calcBindCacheKVMem(key, bindRecords)
	}
	for mem+c.memTracker.BytesConsumed() > c.memCapacity {
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
		evictedKey, evictedValue, evicted := c.cache.RemoveOldest()
		if !evicted {
			return
		}
		c.memTracker.Consume(-calcBindCacheKVMem(evictedKey.(bindCacheKey), evictedValue.([]*BindRecord)))
	}
	c.memTracker.Consume(mem)
	c.cache.Put(key, value)
	ok = true
	return
}

// delete remove an item from the cache. It's not thread-safe.
// Only other functions of the bindCache can use this function.
func (c *bindCache) delete(key bindCacheKey) bool {
	bindRecords := c.get(key)
	if bindRecords != nil {
		mem := calcBindCacheKVMem(key, bindRecords)
		c.cache.Delete(key)
		c.memTracker.Consume(-mem)
		return true
	}
	return false
}

// GetBindRecord gets the BindRecord from the cache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindCache) GetBindRecord(hash, normdOrigSQL, db string) *BindRecord {
	c.lock.Lock()
	defer c.lock.Unlock()
	bindRecords := c.get(bindCacheKey(hash))
	for _, bindRecord := range bindRecords {
		if bindRecord.OriginalSQL == normdOrigSQL {
			return bindRecord
		}
	}
	return nil
}

// GetAllBindRecords return all the bindRecords from the bindCache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindCache) GetAllBindRecords() []*BindRecord {
	c.lock.Lock()
	defer c.lock.Unlock()
	values := c.cache.Values()
	var bindRecords []*BindRecord
	for _, vals := range values {
		bindRecords = append(bindRecords, vals.([]*BindRecord)...)
	}
	return bindRecords
}

// SetBindRecord sets the BindRecord to the cache.
// The function is thread-safe.
func (c *bindCache) SetBindRecord(hash string, meta *BindRecord) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	cacheKey := bindCacheKey(hash)
	metas := c.getCopiedVal(cacheKey)
	for i := range metas {
		if metas[i].OriginalSQL == meta.OriginalSQL {
			metas[i] = meta
		}
	}
	_, err = c.set(cacheKey, []*BindRecord{meta})
	return
}

// RemoveBindRecord removes the BindRecord which has same originSQL with specified BindRecord.
// The function is thread-safe.
func (c *bindCache) RemoveBindRecord(hash string, meta *BindRecord) {
	c.lock.Lock()
	defer c.lock.Unlock()
	metas := c.getCopiedVal(bindCacheKey(hash))
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
	// This function can guarantee the memory usage for the cache will never grow up.
	// So we don't need to handle the return value here.
	_, _ = c.set(bindCacheKey(hash), metas)
}

// SetMemCapacity sets the memory capacity for the cache.
// The function is thread-safe.
func (c *bindCache) SetMemCapacity(capacity int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	// Only change the capacity size without affecting the cached bindRecord
	c.memCapacity = capacity
}

// GetMemUsage get the memory Usage for the cache.
// The function is thread-safe.
func (c *bindCache) GetMemUsage() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.memTracker.BytesConsumed()
}

// GetMemCapacity get the memory capacity for the cache.
// The function is thread-safe.
func (c *bindCache) GetMemCapacity() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.memCapacity
}

// Copy copies a new bindCache from the origin cache.
// The function is thread-safe.
func (c *bindCache) Copy() (newCache *bindCache, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	newCache = newBindCache()
	if c.memTracker.BytesConsumed() > newCache.GetMemCapacity() {
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
	}
	keys := c.cache.Keys()
	for _, key := range keys {
		cacheKey := key.(bindCacheKey)
		v := c.get(cacheKey)
		bindRecords := make([]*BindRecord, len(v))
		copy(bindRecords, v)
		// The memory usage of cache has been handled at the beginning of this function.
		// So we don't need to handle the return value here.
		_, _ = newCache.set(cacheKey, bindRecords)
	}
	return newCache, err
}
