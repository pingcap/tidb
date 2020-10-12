package kv

import (
	"context"
	"fmt"
	"sync"
)

var mm *cachedb

type (
	cachedb struct {
		mu        sync.RWMutex
		memTables map[int64]*memdb
	}

	// MemManager add in executor and tikv for reduce query tikv
	MemManager interface {
		Set(tableID int64, key Key, values []byte) error
		Get(ctx context.Context, tableID int64, key Key) ([]byte, error)
		Release(tableID int64)
	}
)

// Set set values in cache
func (c *cachedb) Set(tableID int64, key Key, values []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	table, ok := c.memTables[tableID]
	if !ok {
		table = newMemDB()
		c.memTables[tableID] = table
	}

	err := table.Set(key, values)
	if err != nil {
		return err
	}

	return nil
}

// Get get values from memory
func (c *cachedb) Get(ctx context.Context, tableID int64, key Key) ([]byte, error) {
	c.mu.RLock()
	if table, ok := c.memTables[tableID]; ok {
		c.mu.RUnlock()
		return table.Get(ctx, key)
	}

	c.mu.RUnlock()
	return nil, fmt.Errorf("tableID %d not found", tableID)
}

// Release release memory for DDL unlock table and remove in cache tables
func (c *cachedb) Release(tableID int64) {
	c.mu.Lock()
	if table, ok := c.memTables[tableID]; ok {
		table.Reset()
		delete(c.memTables, tableID)
	}
	c.mu.Unlock()
}

// Cache get memcache
func Cache() MemManager {
	return mm
}

// NewCacheDB new cachedb
func NewCacheDB() {
	mm = new(cachedb)
	mm.memTables = make(map[int64]*memdb)
}
