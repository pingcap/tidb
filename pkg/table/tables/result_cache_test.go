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
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func makeTestChunk() *chunk.Chunk {
	ft := types.NewFieldType(mysql.TypeLonglong)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 4)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 2)
	return chk
}

func TestResultCacheGetMiss(t *testing.T) {
	c := newResultSetCache()
	key := ResultCacheKey{ParamHash: 42}
	chunks, fts, ok := c.Get(key)
	require.False(t, ok)
	require.Nil(t, chunks)
	require.Nil(t, fts)
}

func TestResultCachePutAndGet(t *testing.T) {
	c := newResultSetCache()
	chk := makeTestChunk()
	ft := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ft}
	key := ResultCacheKey{PlanDigest: [16]byte{1}, ParamHash: 100}

	ok := c.Put(key, []*chunk.Chunk{chk}, fts)
	require.True(t, ok)
	require.Equal(t, 1, c.Len())

	gotChunks, gotFts, hit := c.Get(key)
	require.True(t, hit)
	require.Len(t, gotChunks, 1)
	require.Equal(t, chk, gotChunks[0])
	require.Equal(t, fts, gotFts)
}

func TestResultCacheHitCount(t *testing.T) {
	c := newResultSetCache()
	chk := makeTestChunk()
	ft := types.NewFieldType(mysql.TypeLonglong)
	key := ResultCacheKey{PlanDigest: [16]byte{2}}

	c.Put(key, []*chunk.Chunk{chk}, []*types.FieldType{ft})

	for i := 0; i < 5; i++ {
		c.Get(key)
	}

	c.mu.RLock()
	r := c.items[key]
	c.mu.RUnlock()
	require.Equal(t, int64(5), r.hitCount.Load())
}

func TestResultCacheMaxEntries(t *testing.T) {
	c := newResultSetCache()
	c.maxEntries = 2

	chk := makeTestChunk()
	ft := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ft}

	require.True(t, c.Put(ResultCacheKey{ParamHash: 1}, []*chunk.Chunk{chk}, fts))
	require.True(t, c.Put(ResultCacheKey{ParamHash: 2}, []*chunk.Chunk{chk}, fts))
	require.False(t, c.Put(ResultCacheKey{ParamHash: 3}, []*chunk.Chunk{chk}, fts))
	require.Equal(t, 2, c.Len())
}

func TestResultCacheMaxMemory(t *testing.T) {
	c := newResultSetCache()
	chk := makeTestChunk()
	mem := estimateChunksMemory([]*chunk.Chunk{chk})
	// Allow room for exactly one entry.
	c.maxMemory = mem

	ft := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ft}

	require.True(t, c.Put(ResultCacheKey{ParamHash: 1}, []*chunk.Chunk{chk}, fts))
	require.False(t, c.Put(ResultCacheKey{ParamHash: 2}, []*chunk.Chunk{chk}, fts))
	require.Equal(t, mem, c.MemoryUsage())
}

func TestResultCacheConcurrency(t *testing.T) {
	c := newResultSetCache()
	chk := makeTestChunk()
	ft := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ft}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := ResultCacheKey{ParamHash: uint64(i % 10)}
			c.Put(key, []*chunk.Chunk{chk}, fts)
			c.Get(key)
			c.Len()
			c.MemoryUsage()
		}(i)
	}
	wg.Wait()
	require.True(t, c.Len() <= 10)
}
