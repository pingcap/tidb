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
	"sync"

	"github.com/coocood/freecache"
)

type (
	cacheDB struct {
		mu        sync.RWMutex
		memTables map[int64]*freecache.Cache
	}

	// MemManager adds a cache between transaction buffer and the storage to reduce requests to the storage.
	// Beware, it uses table ID for partition tables, because the keys are unique for partition tables.
	// no matter the physical IDs are the same or not.
	MemManager interface {
		// UnionGet gets the value from cacheDB first, if it not exists,
		// it gets the value from the snapshot, then caches the value in cacheDB.
		UnionGet(ctx context.Context, tid int64, snapshot Snapshot, key Key) ([]byte, error)
		// Delete releases the cache by tableID.
		Delete(tableID int64)
	}
)

// Set set the key/value in cacheDB.
func (c *cacheDB) set(tableID int64, key Key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	table, ok := c.memTables[tableID]
	if !ok {
		table = freecache.NewCache(100 * 1024 * 1024)
		c.memTables[tableID] = table
	}
	return table.Set(key, value, 0)
}

// Get gets the value from cacheDB.
func (c *cacheDB) get(tableID int64, key Key) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if table, ok := c.memTables[tableID]; ok {
		if val, err := table.Get(key); err == nil {
			return val
		}
	}
	return nil
}

// UnionGet implements MemManager UnionGet interface.
func (c *cacheDB) UnionGet(ctx context.Context, tid int64, snapshot Snapshot, key Key) (val []byte, err error) {
	val = c.get(tid, key)
	// key does not exist then get from snapshot and set to cache
	if val == nil {
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

// Delete delete and reset table from tables in cacheDB by tableID
func (c *cacheDB) Delete(tableID int64) {
	c.mu.Lock()
	if k, ok := c.memTables[tableID]; ok {
		k.Clear()
		delete(c.memTables, tableID)
	}
	c.mu.Unlock()
}

// NewCacheDB news the cacheDB.
func NewCacheDB() MemManager {
	mm := new(cacheDB)
	mm.memTables = make(map[int64]*freecache.Cache)
	return mm
}
