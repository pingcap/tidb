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
		memTables map[int64]*memoryTable
	}

	memoryTable struct {
		m      *memdb
		isLock bool
	}

	// MemManager add in executor and tikv for reduce query tikv
	MemManager interface {
		Set(tableID int64, key Key, values []byte) error
		Get(ctx context.Context, tableID int64, key Key) ([]byte, error)
		Release(tableID int64)
		IsLock(tableID int64) (bool, error)
		ReleaseAll()
	}
)

// Set set values in cache
func (c *cachedb) Set(tableID int64, key Key, values []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	table := newMemDB()
	err := table.Set(key, values)
	if err != nil {
		return err
	}

	memTable := new(memoryTable)
	memTable.m = table
	c.memTables[tableID] = memTable
	return nil
}

// Get get values from memory
func (c *cachedb) Get(ctx context.Context, tableID int64, key Key) ([]byte, error) {
	c.mu.RLock()
	if table, ok := c.memTables[tableID]; ok {
		c.mu.RUnlock()
		return table.m.Get(ctx, key)
	}

	c.mu.RUnlock()
	return nil, fmt.Errorf("tableID %d not found", tableID)
}

// Release release memory for DDL unlock table and remove in cache tables
func (c *cachedb) Release(tableID int64) {
	c.mu.Lock()
	if table, ok := c.memTables[tableID]; ok {
		table.m.Reset()
		delete(c.memTables, tableID)
	}

	c.mu.Unlock()
}

func (c *cachedb) IsLock(tableID int64) (bool, error) {
	if table, ok := c.memTables[tableID]; ok {
		return table.isLock, nil
	}

	return false, fmt.Errorf("tableID %d not found", tableID)
}

// ReleaseAll release all memory for DDL unlock table and remove in cache tables
func (c *cachedb) ReleaseAll() {
	c.mu.Lock()
	c.memTables = make(map[int64]*memoryTable)
	c.mu.Unlock()
}

// Cache get memcache
func Cache() MemManager {
	return mm
}

func init() {
	mm = new(cachedb)
	mm.memTables = make(map[int64]*memoryTable)
}

// NewCacheDB new cachedb
func NewCacheDB() {
	mm = new(cachedb)
	mm.memTables = make(map[int64]*memoryTable)
}
