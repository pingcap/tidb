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

package kv

import (
	"context"
	"maps"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/coocood/freecache"
)

var KVCacheCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "server",
		Name:      "kv_cache",
		Help:      "Counter of kv cache hit/miss.",
	}, []string{"result"})

var (
	HitCounter  = KVCacheCounter.WithLabelValues("hit")
	MissCounter = KVCacheCounter.WithLabelValues("miss")
)

func init() {
	prometheus.MustRegister(KVCacheCounter)
}

type (
	cacheDB struct {
		memTables atomic.Pointer[map[int64]*freecache.Cache]
	}

	// MemManager adds a cache between transaction buffer and the storage to reduce requests to the storage.
	// Beware, it uses table ID for partition tables, because the keys are unique for partition tables.
	// no matter the physical IDs are the same or not.
	MemManager interface {
		// UnionGet gets the value from cacheDB first, if it not exists,
		// it gets the value from the snapshot, then caches the value in cacheDB.
		UnionGet(ctx context.Context, tid int64, snapshot Snapshot, key Key) ([]byte, error)
		BatchUnionGet(ctx context.Context, tid int64, snapshot Snapshot, keys []Key) (map[string][]byte, error)
		// Delete releases the cache by tableID.
		Delete(tableID int64)
	}
)

// Set set the key/value in cacheDB.
func (c *cacheDB) set(tableID int64, key Key, value []byte) error {
	memTables := c.memTables.Load()
	table, ok := (*memTables)[tableID]
	if !ok {
		table = freecache.NewCache(32 * 1024 * 1024 * 1024)
		newMemTables := maps.Clone(*memTables)
		newMemTables[tableID] = table
		c.memTables.Store(&newMemTables)
		return nil
	}
	return table.Set(key, value, 0)
}

// Get gets the value from cacheDB.
func (c *cacheDB) get(tableID int64, key Key) []byte {
	memTables := c.memTables.Load()
	table, ok := (*memTables)[tableID]
	if !ok {
		return nil
	}
	if val, err := table.Get(key); err == nil {
		return val
	}
	return nil
}

// UnionGet implements MemManager UnionGet interface.
func (c *cacheDB) UnionGet(ctx context.Context, tid int64, snapshot Snapshot, key Key) (val []byte, err error) {
	val = c.get(tid, key)
	// key does not exist then get from snapshot and set to cache
	if val != nil {
		HitCounter.Inc()
	} else {
		MissCounter.Inc()
		val, err = snapshot.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		err = c.set(tid, key, val)
		if err != nil {
			return nil, err
		}
	}
	return val, nil
}

func (c *cacheDB) BatchUnionGet(ctx context.Context, tid int64, snapshot Snapshot, keys []Key) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	misses := make([]Key, 0, len(keys))
	for _, key := range keys {
		val := c.get(tid, key)
		if val != nil {
			HitCounter.Inc()
			result[string(key)] = val
		} else {
			MissCounter.Inc()
			misses = append(misses, key)
		}
	}
	missesValues, err := snapshot.BatchGet(ctx, misses)
	if err != nil {
		return nil, err
	}
	for key, val := range missesValues {
		err = c.set(tid, Key(key), val)
		if err != nil {
			return nil, err
		}
		result[key] = val
	}
	return result, err
}

// Delete delete and reset table from tables in cacheDB by tableID
func (c *cacheDB) Delete(tableID int64) {
	tables := c.memTables.Load()
	newTables := maps.Clone(*tables)
	delete(newTables, tableID)
	c.memTables.Store(&newTables)
}

// NewCacheDB news the cacheDB.
func NewCacheDB() MemManager {
	mm := new(cacheDB)
	mm.memTables.Store(&map[int64]*freecache.Cache{})
	return mm
}
