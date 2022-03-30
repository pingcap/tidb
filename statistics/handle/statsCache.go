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

package handle

import (
	"encoding/binary"

	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/kvcache"
)

type kvCache interface {
	Get(int64) (*statistics.Table, bool)
	Put(int64, *statistics.Table)
	Del(int64)
	Cost() int64
	Keys() []int64
	Values() []*statistics.Table
	Len() int
	Copy() kvCache
}

func newStatsCache() statsCache {
	return statsCache{
		kvCache: newInternalLRUKVCache(),
	}
}

// statsCache caches the tables in memory for Handle.
type statsCache struct {
	// version is the latest version of cache. It is bumped when new records of `mysql.stats_meta` are loaded into cache.
	version uint64
	// minorVersion is to differentiate the cache when the version is unchanged while the cache contents are
	// modified indeed. This can happen when we load extra column histograms into cache, or when we modify the cache with
	// statistics feedbacks, etc. We cannot bump the version then because no new changes of `mysql.stats_meta` are loaded,
	// while the override of statsCache is in a copy-on-write way, to make sure the statsCache is unchanged by others during the
	// the interval of 'copy' and 'write', every 'write' should bump / check this minorVersion if the version keeps
	// unchanged.
	// This bump / check logic is encapsulated in `statsCache.update` and `updateStatsCache`, callers don't need to care
	// about this minorVersion actually.
	minorVersion uint64

	kvCache
}

func (sc statsCache) copy() statsCache {
	newCache := newStatsCache()
	newCache.version = sc.version
	newCache.minorVersion = sc.minorVersion
	sc.kvCache = sc.kvCache.Copy()
	return newCache
}

// update updates the statistics table cache using copy on write.
func (sc statsCache) update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) statsCache {
	newCache := sc.copy()
	if newVersion == newCache.version {
		newCache.minorVersion += uint64(1)
	} else {
		newCache.version = newVersion
		newCache.minorVersion = uint64(0)
	}
	for _, tbl := range tables {
		id := tbl.PhysicalID
		newCache.Put(id, tbl)
	}
	for _, id := range deletedIDs {
		newCache.Del(id)
	}
	return newCache
}

type internalLRUKVCache struct {
	cache *kvcache.StandardLRUCache
}

func newInternalLRUKVCache() kvCache {
	c := &internalLRUKVCache{}
	c.cache = kvcache.NewStandardLRUCache(1 << 30)
	return c
}

type statsCacheKey int64

func (key statsCacheKey) Hash() []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(key))
	return buf
}

func (c *internalLRUKVCache) Get(key int64) (*statistics.Table, bool) {
	k := statsCacheKey(key)
	v, ok := c.cache.Get(k)
	if !ok {
		return nil, false
	}
	return v.(*statistics.Table), true
}

func (c *internalLRUKVCache) Put(key int64, t *statistics.Table) {
	k := statsCacheKey(key)
	c.cache.Put(k, t, uint64(t.MemoryUsage()))
}

func (c *internalLRUKVCache) Del(key int64) {
	k := statsCacheKey(key)
	c.cache.Delete(k)
}

func (c *internalLRUKVCache) Cost() int64 {
	return int64(c.cache.Cost())
}

func (c *internalLRUKVCache) Keys() []int64 {
	ks := c.cache.Keys()
	r := make([]int64, 0, len(ks))
	for _, k := range ks {
		r = append(r, int64(k.(statsCacheKey)))
	}
	return r
}

func (c *internalLRUKVCache) Values() []*statistics.Table {
	vs := c.cache.Values()
	r := make([]*statistics.Table, 0, len(vs))
	for _, v := range vs {
		r = append(r, v.(*statistics.Table))
	}
	return r
}

func (c *internalLRUKVCache) Len() int {
	return c.cache.Len()
}

func (c *internalLRUKVCache) Copy() kvCache {
	newC := &internalLRUKVCache{}
	newC.cache = c.cache.Copy()
	return newC
}
