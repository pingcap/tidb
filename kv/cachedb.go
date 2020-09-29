package kv

import (
	"context"
	"fmt"
)

var cc MemManager

type (
	cachedb struct {
		memTables map[string]*memdb
	}

	// MemManager add in executor and tikv for reduce query tikv
	MemManager interface {
		Set(tableName string, key Key, values []byte) error
		Get(ctx context.Context, tableName string, key Key) ([]byte, error)
		Release(tableName string)
	}
)

// Set set values in cache
func (c *cachedb) Set(tableName string, key Key, values []byte) error {
	table, ok := c.memTables[tableName]
	if !ok {
		c.memTables[tableName] = newMemDB()
	}

	return table.Set(key, values)
}

// Get get values from memory
func (c *cachedb) Get(ctx context.Context, tableName string, key Key) ([]byte, error) {
	if table, ok := c.memTables[tableName]; ok {
		return table.Get(ctx, key)
	}

	return nil, fmt.Errorf("key %s not found", key)
}

// Release release memory for DDL unlock table and remove in cache tables
func (c *cachedb) Release(tableName string) {
	if table, ok := c.memTables[tableName]; ok {
		table.Reset()
		delete(c.memTables, tableName)
	}
}

// Cache get memcache
func Cache() MemManager {
	return cc
}

// NewCacheDB new cachedb
func NewCacheDB() {
	cc = &cachedb{
		memTables: make(map[string]*memdb, 0),
	}
}
