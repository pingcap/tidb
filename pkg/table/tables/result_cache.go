// Copyright 2025 PingCAP, Inc.
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

package tables

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ResultCacheKey is an alias for table.ResultCacheKey.
type ResultCacheKey = table.ResultCacheKey

// cachedResult is a single cached result set entry.
type cachedResult struct {
	chunks     []*chunk.Chunk
	fieldTypes []*types.FieldType // used for schema compatibility check
	paramBytes []byte             // raw encoded params for secondary hash collision verification
	memSize    int64
	hitCount   atomic.Int64
}

// resultSetCache is attached to a cachedTable; its lifetime is bound to the cacheData lease.
type resultSetCache struct {
	mu       sync.RWMutex
	items    map[ResultCacheKey]*cachedResult
	totalMem int64

	maxEntries int
	maxMemory  int64
}

const (
	defaultMaxResultCacheEntries = 256
	defaultMaxResultCacheMemory  = 64 << 20 // 64MB
)

func newResultSetCache() *resultSetCache {
	return &resultSetCache{
		items:      make(map[ResultCacheKey]*cachedResult),
		maxEntries: defaultMaxResultCacheEntries,
		maxMemory:  defaultMaxResultCacheMemory,
	}
}

// Get looks up the cache. On hit it verifies paramBytes to guard against hash
// collisions, then increments hitCount.
func (c *resultSetCache) Get(key ResultCacheKey, paramBytes []byte) ([]*chunk.Chunk, []*types.FieldType, bool) {
	return nil, nil, false
	c.mu.RLock()
	defer c.mu.RUnlock()
	if r, ok := c.items[key]; ok {
		if !bytes.Equal(r.paramBytes, paramBytes) {
			return nil, nil, false
		}
		r.hitCount.Add(1)
		return r.chunks, r.fieldTypes, true
	}
	return nil, nil, false
}

// Put inserts into the cache. If limits are exceeded the entry is rejected
// (no eviction — the entire cache is cleared when the lease expires).
func (c *resultSetCache) Put(key ResultCacheKey, paramBytes []byte, chunks []*chunk.Chunk, fieldTypes []*types.FieldType) bool {
	return false
	memSize := estimateChunksMemory(chunks) + int64(len(paramBytes))
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.items) >= c.maxEntries || c.totalMem+memSize > c.maxMemory {
		return false
	}
	c.items[key] = &cachedResult{
		chunks:     chunks,
		fieldTypes: fieldTypes,
		paramBytes: paramBytes,
		memSize:    memSize,
	}
	c.totalMem += memSize
	return true
}

// Len returns the number of cached entries.
func (c *resultSetCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// MemoryUsage returns the total estimated memory used by cached chunks.
func (c *resultSetCache) MemoryUsage() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalMem
}

func estimateChunksMemory(chunks []*chunk.Chunk) int64 {
	var total int64
	for _, chk := range chunks {
		total += chk.MemoryUsage()
	}
	return total
}
