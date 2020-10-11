package kv

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/tablecodec"
)

var mm MemManager

type (
	cachedb struct {
		memTables map[int64]*memdb
	}

	// MemManager add in executor and tikv for reduce query tikv
	MemManager interface {
		Set(key Key, values []byte) error
		Get(ctx context.Context, key Key) ([]byte, error)
		Release(key Key)
	}
)

// Set set values in cache
func (c *cachedb) Set(key Key, values []byte) error {
	tableID, _, _, err := tablecodec.DecodeIndexKey(key)
	if err != nil {
		return nil, err
	}
	table, ok := c.memTables[tableID]
	if !ok {
		c.memTables[tableID] = newMemDB()
	}

	return table.Set(key, values)
}

// Get get values from memory
func (c *cachedb) Get(ctx context.Context, key Key) ([]byte, error) {
	tableID, _, _, err := tablecodec.DecodeIndexKey(key)
	if err != nil {
		return nil, err
	}
	if table, ok := c.memTables[tableID]; ok {
		return table.Get(ctx, key)
	}

	return nil, fmt.Errorf("key %s not found", key)
}

// Release release memory for DDL unlock table and remove in cache tables
func (c *cachedb) Release(key Key) {
	tableID, _, _, err := tablecodec.DecodeIndexKey(key)
	if err != nil {
		return
	}
	if table, ok := c.memTables[tableID]; ok {
		table.Reset()
		delete(c.memTables, tableID)
	}
}

// Cache get memcache
func Cache() MemManager {
	return mm
}

// NewCacheDB new cachedb
func NewCacheDB() {
	mm = &cachedb{
		memTables: make(map[string]*memdb, 0),
	}
}
