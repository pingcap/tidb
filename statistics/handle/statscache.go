package handle

import (
	"encoding/binary"
	"sync"

	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// statsCache caches Regions loaded from PD.
type statsCache struct {
	mu          sync.Mutex
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	version     uint64
	memTracker  *memory.Tracker // track memory usage.
}
type statsCacheKey int64

func (key statsCacheKey) Hash() []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(key))
	return buf
}

// newstatsCache returns a new statsCahce with capacity maxMemoryLimit(initial 1G)
func newstatsCache(maxMemoryLimit int64) (*statsCache, error) {
	// since newstatsCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(uint(maxMemoryLimit), 0.1, 0)
	c := statsCache{
		cache:       cache,
		memCapacity: maxMemoryLimit,
		memTracker:  memory.NewTracker(stringutil.StringerStr("statsCache"), -1),
	}
	return &c, nil
}

// Lookup get table with id.
func (sc *statsCache) Lookup(id int64) (*statistics.Table, bool) {
	var key statsCacheKey = statsCacheKey(id)
	value, hit := sc.cache.Get(key)
	if !hit {
		return nil, false
	}
	table := value.(*statistics.Table)
	return table, true
}

// Insert insert a new table to tables and update the cache.
// if bytesconsumed is more than capacity, remove oldest cache and add metadata of it
func (sc *statsCache) Insert(table *statistics.Table) (bool, error) {
	var key statsCacheKey = statsCacheKey(table.PhysicalID)
	mem := table.MemoryUsage()
	if mem > sc.memCapacity { // ignore this kv pair if its size is too large
		return false, nil
	}
	for mem+sc.memTracker.BytesConsumed() > sc.memCapacity {
		evictedKey, evictedValue, evicted := sc.cache.RemoveOldest()
		if !evicted {
			return false, nil
		}
		sc.memTracker.Consume(-evictedValue.(*statistics.Table).MemoryUsage())
		sc.cache.Put(evictedKey, evictedValue.(*statistics.Table).CopyMeta())
	}
	sc.Erase(table.PhysicalID)
	sc.memTracker.Consume(mem)
	sc.cache.Put(key, table)
	return true, nil
}

// Erase Erase a stateCache with physical id
func (sc *statsCache) Erase(deletedID int64) bool {
	table, hit := sc.Lookup(deletedID)
	if !hit {
		return false
	}
	sc.memTracker.Consume(-table.MemoryUsage())

	var key statsCacheKey = statsCacheKey(deletedID)
	sc.cache.Delete(key)
	return true
}

// Update updates the statistics table cache.
func (sc *statsCache) Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) {
	sc.mu.Lock()
	if sc.version <= newVersion {
		sc.version = newVersion
		for _, tbl := range tables {
			sc.Insert(tbl)
		}
		for _, id := range deletedIDs {
			sc.Erase(id)
		}
	}
	sc.mu.Unlock()
}

func (sc *statsCache) GetVersion() uint64 {
	return sc.version
}

// initStatsCache should be called after the tables and their stats are initilazed
// using tables map and version to init statscache
func (sc *statsCache) initStatsCache(tables map[int64]*statistics.Table, version uint64) {
	for _, tb := range tables {
		sc.Insert(tb)
	}
	sc.version = version
	return
}
