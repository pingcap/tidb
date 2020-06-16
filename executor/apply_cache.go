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
	"fmt"
	"unsafe"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// applyCache is used in the apply executor. When we get the same value of the outer row.
// We fetch the inner rows in the cache not to fetch them in the inner executor.
type applyCache struct {
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	memTracker  *memory.Tracker // track memory usage.
}

type applyCacheKey struct {
	Data []byte
}

func (key *applyCacheKey) Hash() []byte {
	return key.Data
}

// applyCacheValue is used to store the value of <key, value> pair in applyCache
type applyCacheValue struct {
	Data *chunk.List
}

var applyCacheLabel fmt.Stringer = stringutil.StringerStr("applyCache")

func newApplyCache(ctx sessionctx.Context) (*applyCache, error) {
	// num means the count of the cached element
	num := uint(ctx.GetSessionVars().ApplyCacheCapacity / 100)
	cache := kvcache.NewSimpleLRUCache(num, 0.1, uint64(ctx.GetSessionVars().ApplyCacheCapacity))
	c := applyCache{
		cache:       cache,
		memCapacity: ctx.GetSessionVars().ApplyCacheCapacity,
		memTracker:  memory.NewTracker(applyCacheLabel, -1),
	}
	return &c, nil
}

// Get gets a cache item according to cache key.
func (c *applyCache) Get(key applyCacheKey) (*applyCacheValue, error) {
	value, hit := c.cache.Get(&key)
	if !hit {
		return nil, nil
	}
	typedValue := value.(*applyCacheValue)
	return typedValue, nil
}

// Set inserts an item to the cache.
func (c *applyCache) Set(key *applyCacheKey, value *applyCacheValue) (bool, error) {
	mem := int64(unsafe.Sizeof(key.Data)) + value.Data.GetMemTracker().BytesConsumed()
	// When the <key, value> pair's memory consumption is larger than cache's max capacity,
	// we do not to store the <key, value> pair.
	if mem > c.memCapacity {
		return false, nil
	}
	for mem+c.memTracker.BytesConsumed() > c.memCapacity {
		evictedKey, evictedValue, evicted := c.cache.RemoveOldest()
		if !evicted {
			return false, nil
		}
		c.memTracker.Consume(-(int64(unsafe.Sizeof(evictedKey)) + evictedValue.(*applyCacheValue).Data.GetMemTracker().BytesConsumed()))
	}
	c.memTracker.Consume(mem)
	c.cache.Put(key, value)
	return true, nil
}

// GetMemTracker returns the memory tracker of this apply cache.
func (c *applyCache) GetMemTracker() *memory.Tracker {
	return c.memTracker
}
