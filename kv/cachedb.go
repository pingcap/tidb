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

	"github.com/pingcap/tidb/sessionctx"
)

type (
	cachedb struct {
		mu        sync.RWMutex
		memTables map[int64]*memdb
	}

	// MemManager add in executor and tikv for reduce query tikv
	// Beware, using table ID for partition tables, because the keys are unique for partition tables
	// no matter the physical IDs are the same or not.
	MemManager interface {
		Set(tableID int64, key Key, value []byte) error
		Get(ctx context.Context, tableID int64, key Key) []byte
		Delete(tableID int64)
	}
)

// Set set value in cache
func (c *cachedb) Set(tableID int64, key Key, value []byte) error {
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

// Get gets value from memory
func (c *cachedb) Get(ctx context.Context, tableID int64, key Key) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if table, ok := c.memTables[tableID]; ok {
		if val, err := table.Get(ctx, key); err == nil {
			return val
		}
	}
	return nil
}

// Delete delete and reset table from tables in memory by tableID
func (c *cachedb) Delete(tableID int64) {
	c.mu.Lock()
	if k, ok := c.memTables[tableID]; ok {
		k.Reset()
		delete(c.memTables, tableID)
	}
	c.mu.Unlock()
}

// NewCacheDB new cachedb
func NewCacheDB() MemManager {
	mm := new(cachedb)
	mm.memTables = make(map[int64]*memdb)
	return mm
}

type CacheBatchGetter struct {
	ctx      sessionctx.Context
	tid      int64
	snapshot Snapshot
}

func (b *CacheBatchGetter) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	cacheDB := b.ctx.GetStore().GetMemCache()
	vals := make(map[string][]byte)
	for _, key := range keys {
		val := cacheDB.Get(ctx, b.tid, key)
		// key does not exist then get from snapshot and set to cache
		if val == nil {
			val, err := b.snapshot.Get(ctx, key)
			if err != nil {
				return nil, err
			}

			err = cacheDB.Set(b.tid, key, val)
			if err != nil {
				return nil, err
			}
		}
		vals[string(key)] = val
	}
	return vals, nil
}

func NewCacheBatchGetter(ctx sessionctx.Context, tid int64, snapshot Snapshot) *CacheBatchGetter {
	return &CacheBatchGetter{ctx, tid, snapshot}
}
