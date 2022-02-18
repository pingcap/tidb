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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
)

// bindCache
type bindCache struct {
	ctx         sessionctx.Context
	cache       *kvcache.SimpleLRUCache // cache.Get/Put are not thread-safe, so it's protected by the lock above
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
		// TODO: use the memTracker struct for the BindRecord
		valMem += int64(bindRecord.size())
	}
	return int64(len(key.Hash())) + valMem
}

func newBindCache(ctx sessionctx.Context) *bindCache {
	// since bindCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(mathutil.MaxUint, 0.1, 0)
	c := bindCache{
		ctx:         ctx,
		cache:       cache,
		memCapacity: ctx.GetSessionVars().MemQuotaBindCache,
		memTracker:  memory.NewTracker(memory.LabelForBindCache, -1),
	}
	return &c
}

// Get gets a cache item according to cache key. It's thread-safe.
func (c *bindCache) Get(key bindCacheKey) []*BindRecord {
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.([]*BindRecord)
	return typedValue
}

// Set inserts an item to the cache. It's thread-safe.
func (c *bindCache) Set(key bindCacheKey, value []*BindRecord) bool {
	mem := bindCacheKVMem(key, value)
	if mem > c.memCapacity { // ignore this kv pair if its size is too large
		return false
	}
	bindRecords := c.Get(key)
	if bindRecords != nil {
		for _, bindRecord := range bindRecords {
			mem -= int64(bindRecord.size())
		}
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

// Delete remove an item from the cache. It's thread-safe.
func (c *bindCache) Delete(key bindCacheKey) bool {
	value, ok := c.cache.Get(key)
	if ok {
		mem := bindCacheKVMem(key, value.([]*BindRecord))
		c.cache.Delete(key)
		c.memTracker.Consume(-mem)
	}
	return true
}

// GetAllBindRecords.
func (c *bindCache) GetAllBindRecords() []*BindRecord {
	values := c.cache.Values()
	var bindRecords []*BindRecord
	for _, vals := range values {
		bindRecords = append(bindRecords, vals.([]*BindRecord)...)
	}
	return bindRecords
}

// GetMemTracker returns the memory tracker of this apply cache.
func (c *bindCache) GetMemTracker() *memory.Tracker {
	return c.memTracker
}

// removeDeletedBindRecord removes the BindRecord which has same originSQL with specified BindRecord.
func (c *bindCache) removeDeletedBindRecord(hash string, meta *BindRecord) {
	metas := c.Get(bindCacheKey(hash))
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
				c.Delete(bindCacheKey(hash))
				return
			}
		}
	}
	c.Set(bindCacheKey(hash), metas)
}

func (c bindCache) setBindRecord(hash string, meta *BindRecord) {
	cacheKey := bindCacheKey(hash)
	value, ok := c.cache.Get(cacheKey)
	if ok {
		metas := value.([]*BindRecord)
		for i := range metas {
			if metas[i].OriginalSQL == meta.OriginalSQL {
				metas[i] = meta
				return
			}
		}
	}
	c.Set(cacheKey, []*BindRecord{meta})
}

func (c bindCache) getBindRecord(hash, normdOrigSQL, db string) *BindRecord {
	bindRecords := c.Get(bindCacheKey(hash))
	for _, bindRecord := range bindRecords {
		if bindRecord.OriginalSQL == normdOrigSQL {
			return bindRecord
		}
	}
	return nil
}

func (c bindCache) copy() *bindCache {
	newCache := newBindCache(c.ctx)
	keys := c.cache.Keys()
	for _, key := range keys {
		cacheKey := key.(bindCacheKey)
		v := c.Get(cacheKey)
		bindRecords := make([]*BindRecord, len(v))
		copy(bindRecords, v)
		newCache.Set(cacheKey, bindRecords)
	}
	return newCache
}
