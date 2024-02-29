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

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// BindingCache is the interface for the cache of the SQL plan bindings.
type BindingCache interface {
	// GetBinding gets the binding for the specified sqlDigest.
	GetBinding(sqlDigest string) Bindings
	// GetAllBindings gets all the bindings in the cache.
	GetAllBindings() Bindings
	// SetBinding sets the binding for the specified sqlDigest.
	SetBinding(sqlDigest string, meta Bindings) (err error)
	// RemoveBinding removes the binding for the specified sqlDigest.
	RemoveBinding(sqlDigest string)
	// SetMemCapacity sets the memory capacity for the cache.
	SetMemCapacity(capacity int64)
	// GetMemUsage gets the memory usage of the cache.
	GetMemUsage() int64
	// GetMemCapacity gets the memory capacity of the cache.
	GetMemCapacity() int64
	// Copy copies the cache.
	Copy() (newCache BindingCache, err error)
	// Size returns the number of items in the cache.
	Size() int
}

// bindingCache uses the LRU cache to store the bindings.
// The key of the LRU cache is original sql, the value is a slice of Bindings.
// Note: The bindingCache should be accessed with lock.
type bindingCache struct {
	lock        sync.Mutex
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	memTracker  *memory.Tracker // track memory usage.
}

type bindingCacheKey string

func (key bindingCacheKey) Hash() []byte {
	return hack.Slice(string(key))
}

func calcBindCacheKVMem(key bindingCacheKey, value Bindings) int64 {
	var valMem int64
	valMem += int64(value.size())
	return int64(len(key.Hash())) + valMem
}

func newBindCache() BindingCache {
	// since bindingCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(mathutil.MaxUint, 0, 0)
	c := bindingCache{
		cache:       cache,
		memCapacity: variable.MemQuotaBindingCache.Load(),
		memTracker:  memory.NewTracker(memory.LabelForBindCache, -1),
	}
	return &c
}

// get gets a cache item according to cache key. It's not thread-safe.
// Note: Only other functions of the bindingCache file can use this function.
// Don't use this function directly in other files in bindinfo package.
// The return value is not read-only, but it is only can be used in other functions which are also in the bind_cache.go.
func (c *bindingCache) get(key bindingCacheKey) Bindings {
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.(Bindings)
	return typedValue
}

// set inserts an item to the cache. It's not thread-safe.
// Only other functions of the bindingCache can use this function.
// The set operation will return error message when the memory usage of binding_cache exceeds its capacity.
func (c *bindingCache) set(key bindingCacheKey, value Bindings) (ok bool, err error) {
	mem := calcBindCacheKVMem(key, value)
	if mem > c.memCapacity { // ignore this kv pair if its size is too large
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
		return
	}
	bindings := c.get(key)
	if bindings != nil {
		// Remove the origin key-value pair.
		mem -= calcBindCacheKVMem(key, bindings)
	}
	for mem+c.memTracker.BytesConsumed() > c.memCapacity {
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
		evictedKey, evictedValue, evicted := c.cache.RemoveOldest()
		if !evicted {
			return
		}
		c.memTracker.Consume(-calcBindCacheKVMem(evictedKey.(bindingCacheKey), evictedValue.(Bindings)))
	}
	c.memTracker.Consume(mem)
	c.cache.Put(key, value)
	ok = true
	return
}

// delete remove an item from the cache. It's not thread-safe.
// Only other functions of the bindingCache can use this function.
func (c *bindingCache) delete(key bindingCacheKey) bool {
	bindings := c.get(key)
	if bindings != nil {
		mem := calcBindCacheKVMem(key, bindings)
		c.cache.Delete(key)
		c.memTracker.Consume(-mem)
		return true
	}
	return false
}

// GetBinding gets the Bindings from the cache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindingCache) GetBinding(sqlDigest string) Bindings {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.get(bindingCacheKey(sqlDigest))
}

// GetAllBindings return all the bindings from the bindingCache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindingCache) GetAllBindings() Bindings {
	c.lock.Lock()
	defer c.lock.Unlock()
	values := c.cache.Values()
	bindings := make(Bindings, 0, len(values))
	for _, vals := range values {
		bindings = append(bindings, vals.(Bindings)...)
	}
	return bindings
}

// SetBinding sets the Bindings to the cache.
// The function is thread-safe.
func (c *bindingCache) SetBinding(sqlDigest string, meta Bindings) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	cacheKey := bindingCacheKey(sqlDigest)
	_, err = c.set(cacheKey, meta)
	return
}

// RemoveBinding removes the Bindings which has same originSQL with specified Bindings.
// The function is thread-safe.
func (c *bindingCache) RemoveBinding(sqlDigest string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.delete(bindingCacheKey(sqlDigest))
}

// SetMemCapacity sets the memory capacity for the cache.
// The function is thread-safe.
func (c *bindingCache) SetMemCapacity(capacity int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	// Only change the capacity size without affecting the cached bindings
	c.memCapacity = capacity
}

// GetMemUsage get the memory Usage for the cache.
// The function is thread-safe.
func (c *bindingCache) GetMemUsage() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.memTracker.BytesConsumed()
}

// GetMemCapacity get the memory capacity for the cache.
// The function is thread-safe.
func (c *bindingCache) GetMemCapacity() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.memCapacity
}

// Copy copies a new bindingCache from the origin cache.
// The function is thread-safe.
func (c *bindingCache) Copy() (BindingCache, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	var err error
	newCache := newBindCache().(*bindingCache)
	if c.memTracker.BytesConsumed() > newCache.GetMemCapacity() {
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
	}
	keys := c.cache.Keys()
	for _, key := range keys {
		cacheKey := key.(bindingCacheKey)
		v := c.get(cacheKey)
		if _, err := newCache.set(cacheKey, v); err != nil {
			return nil, err
		}
	}
	return newCache, err
}

func (c *bindingCache) Size() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cache.Size()
}
