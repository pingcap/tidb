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

package kv

import (
	"context"
	"sync"
)

type (
	cacheDB struct {
		mu        sync.RWMutex
		memTables map[int64]*memdb
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
		table = newMemDB()
		c.memTables[tableID] = table
	}
	err := table.Set(key, value)
	if err != nil && ErrTxnTooLarge.Equal(err) {
		// If it reaches the upper limit, refresh a new memory buffer.
		c.memTables[tableID] = newMemDB()
		return nil
	}
	return err
}

// Get gets the value from cacheDB.
func (c *cacheDB) get(ctx context.Context, tableID int64, key Key) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if table, ok := c.memTables[tableID]; ok {
		if val, err := table.Get(ctx, key); err == nil {
			return val
		}
	}
	return nil
}

// UnionGet implements MemManager UnionGet interface.
func (c *cacheDB) UnionGet(ctx context.Context, tid int64, snapshot Snapshot, key Key) ([]byte, error) {
	val := c.get(ctx, tid, key)
	// key does not exist then get from snapshot and set to cache
	if val == nil {
		val, err := snapshot.Get(ctx, key)
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
		k.Reset()
		delete(c.memTables, tableID)
	}
	c.mu.Unlock()
}

// NewCacheDB news the cacheDB.
func NewCacheDB() MemManager {
	mm := new(cacheDB)
	mm.memTables = make(map[int64]*memdb)
	return mm
}
