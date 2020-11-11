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
	cachedb struct {
		mu        sync.RWMutex
		memTables map[int64]*memdb
	}

	// MemManager add in executor and tikv for reduce query tikv
	MemManager interface {
		Set(tableID int64, key Key, value []byte) error
		Get(ctx context.Context, tableID int64, key Key) []byte
		Release()
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
	if err != nil {
		return err
	}

	return nil
}

// Get gets value from memory
func (c *cachedb) Get(ctx context.Context, tableID int64, key Key) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if table, ok := c.memTables[tableID]; ok {
		if val, err := table.Get(ctx, key); err == nil {
			return val
		}
		return nil
	}

	return nil
}

// Release release memory for DDL unlock table and remove in cache tables
func (c *cachedb) Release() {
	c.mu.Lock()
	for _, v := range c.memTables {
		v.Reset()
	}
	c.memTables = make(map[int64]*memdb)
	c.mu.Unlock()
}

// NewCacheDB new cachedb
func NewCacheDB() MemManager {
	mm := new(cachedb)
	mm.memTables = make(map[int64]*memdb)
	return mm
}
