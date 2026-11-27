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

package applycache

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestApplyCache(t *testing.T) {
	ctx := mock.NewContext()
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	value := make([]*chunk.List, 3)
	key := make([][]byte, 3)
	for i := range 3 {
		value[i] = chunk.NewList(fields, 1, 1)
		srcChunk := chunk.NewChunkWithCapacity(fields, 1)
		srcChunk.AppendInt64(0, int64(i))
		srcRow := srcChunk.GetRow(0)
		value[i].AppendRow(srcRow)
		key[i] = []byte(strings.Repeat(strconv.Itoa(i), 100))

		require.Equal(t, int64(100)+value[i].GetChunk(0).MemoryUsage(), applyCacheKVMem(key[i], value[i]))
	}
	ctx.GetSessionVars().MemQuotaApplyCache = int64(len(key[0])) + value[0].GetChunk(0).MemoryUsage()
	applyCache, err := NewApplyCache(ctx)
	require.NoError(t, err)

	ok, err := applyCache.Set(key[0], value[0])
	require.NoError(t, err)
	require.True(t, ok)
	result, err := applyCache.Get(key[0])
	require.NoError(t, err)
	require.NotNil(t, result)

	ok, err = applyCache.Set(key[1], value[1])
	require.NoError(t, err)
	require.True(t, ok)
	result, err = applyCache.Get(key[1])
	require.NoError(t, err)
	require.NotNil(t, result)

	ok, err = applyCache.Set(key[2], value[2])
	require.NoError(t, err)
	require.True(t, ok)
	result, err = applyCache.Get(key[2])
	require.NoError(t, err)
	require.NotNil(t, result)

	// Both key[0] and key[1] are not in the cache
	result, err = applyCache.Get(key[0])
	require.NoError(t, err)
	require.Nil(t, result)

	result, err = applyCache.Get(key[1])
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestApplyCacheMemoryAccounting(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeVarchar)}
	key := []byte("key")
	newChunk := func(size int) *chunk.Chunk {
		chk := chunk.NewChunkWithCapacity(fields, 1)
		chk.AppendString(0, strings.Repeat("x", size))
		return chk
	}
	t.Run("quota includes row memory", func(t *testing.T) {
		value := chunk.NewList(fields, 1, 1)
		value.AppendRow(newChunk(128).GetRow(0))
		mem := int64(len(key)) + value.GetChunk(0).MemoryUsage()
		for _, quota := range []int64{0, int64(len(key)), mem - 1, mem} {
			ctx := mock.NewContext()
			ctx.GetSessionVars().MemQuotaApplyCache = quota
			cache, err := NewApplyCache(ctx)
			require.NoError(t, err)
			ok, err := cache.Set(key, value)
			require.NoError(t, err)
			require.Equal(t, quota >= mem, ok)
			if ok {
				require.Equal(t, mem, cache.GetMemTracker().BytesConsumed())
			} else {
				require.Zero(t, cache.GetMemTracker().BytesConsumed())
			}
		}
	})

	t.Run("append and add", func(t *testing.T) {
		value := chunk.NewList(fields, 2, 2)
		require.Equal(t, int64(len(key)), applyCacheKVMem(key, value))
		for _, size := range []int{1, 4096, 1} {
			value.AppendRow(newChunk(size).GetRow(0))
			mem := int64(len(key))
			for i := range value.NumChunks() {
				mem += value.GetChunk(i).MemoryUsage()
			}
			require.Equal(t, mem, applyCacheKVMem(key, value))
		}
		mem := int64(len(key)) + value.GetChunk(0).MemoryUsage() + value.GetChunk(1).MemoryUsage()
		added := newChunk(128)
		value.Add(added)
		mem += added.MemoryUsage()
		require.Equal(t, mem, applyCacheKVMem(key, value))

		ctx := mock.NewContext()
		ctx.GetSessionVars().MemQuotaApplyCache = mem - 1
		cache, err := NewApplyCache(ctx)
		require.NoError(t, err)
		ok, err := cache.Set(key, value)
		require.NoError(t, err)
		require.False(t, ok)
		require.Zero(t, cache.GetMemTracker().BytesConsumed())
	})

	t.Run("shared tracker", func(t *testing.T) {
		first := chunk.NewList(fields, 1, 1)
		first.Add(newChunk(128))
		value := chunk.NewListWithMemTracker(fields, 1, 1, first.GetMemTracker())
		value.Add(newChunk(256))
		mem := int64(len(key)) + value.GetChunk(0).MemoryUsage()
		require.Equal(t, mem, applyCacheKVMem(key, value))
		ctx := mock.NewContext()
		ctx.GetSessionVars().MemQuotaApplyCache = mem
		cache, err := NewApplyCache(ctx)
		require.NoError(t, err)
		ok, err := cache.Set(key, value)
		require.NoError(t, err)
		require.True(t, ok)
		first.Add(newChunk(4096))
		require.Equal(t, mem, applyCacheKVMem(key, value))
		require.Equal(t, mem, cache.GetMemTracker().BytesConsumed())

		replacement := chunk.NewList(fields, 1, 1)
		replacement.Add(newChunk(128))
		ok, err = cache.Set([]byte("new"), replacement)
		require.NoError(t, err)
		require.True(t, ok)
		result, err := cache.Get(key)
		require.NoError(t, err)
		require.Nil(t, result)
		require.Equal(t, int64(3)+replacement.GetChunk(0).MemoryUsage(), cache.GetMemTracker().BytesConsumed())
	})

	t.Run("retained chunks after reset", func(t *testing.T) {
		value := chunk.NewList(fields, 1, 1)
		for range 3 {
			value.AppendRow(newChunk(128).GetRow(0))
		}
		retained := []*chunk.Chunk{value.GetChunk(0), value.GetChunk(1), value.GetChunk(2)}
		value.Reset()
		for range 2 {
			mem := int64(len(key))
			for _, chk := range retained {
				mem += chk.MemoryUsage()
			}
			require.Equal(t, mem, applyCacheKVMem(key, value))
			value.AppendRow(newChunk(4096).GetRow(0))
		}
		value.Clear()
		require.Equal(t, int64(len(key)), applyCacheKVMem(key, value))
	})
}

func TestApplyCacheReplacement(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	newValue := func(numChunks int) *chunk.List {
		value := chunk.NewList(fields, 1, 1)
		for range numChunks {
			chk := chunk.NewChunkWithCapacity(fields, 1)
			chk.AppendInt64(0, 1)
			value.Add(chk)
		}
		return value
	}
	small, large, oversized := newValue(1), newValue(2), newValue(4)
	smallMem := int64(1) + small.GetChunk(0).MemoryUsage()
	largeMem := int64(1) + large.GetChunk(0).MemoryUsage() + large.GetChunk(1).MemoryUsage()
	ctx := mock.NewContext()
	ctx.GetSessionVars().MemQuotaApplyCache = 2 * smallMem
	cache, err := NewApplyCache(ctx)
	require.NoError(t, err)
	set := func(key string, value *chunk.List) {
		t.Helper()
		ok, err := cache.Set([]byte(key), value)
		require.NoError(t, err)
		require.True(t, ok)
	}
	get := func(key string) *chunk.List {
		t.Helper()
		value, err := cache.Get([]byte(key))
		require.NoError(t, err)
		return value
	}

	set("a", small)
	set("a", small)
	require.Equal(t, 1, cache.cache.Size())
	require.Equal(t, smallMem, cache.GetMemTracker().BytesConsumed())
	set("b", small)
	set("a", small)
	require.Equal(t, 2, cache.cache.Size())
	require.Equal(t, 2*smallMem, cache.GetMemTracker().BytesConsumed())
	// Replacing a makes it most recently used without evicting b.
	set("c", small)
	require.Nil(t, get("b"))
	require.Same(t, small, get("a"))
	require.Same(t, small, get("c"))

	// Growing a needs to evict c, but must first release a's old charge.
	set("a", large)
	require.Nil(t, get("c"))
	require.Same(t, large, get("a"))
	require.Equal(t, largeMem, cache.GetMemTracker().BytesConsumed())
	set("a", small)
	require.Equal(t, smallMem, cache.GetMemTracker().BytesConsumed())
	set("b", small)

	// A rejected replacement preserves both the old entry and LRU order.
	keys := cache.cache.Keys()
	ok, err := cache.Set([]byte("a"), oversized)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, keys, cache.cache.Keys())
	require.Equal(t, 2*smallMem, cache.GetMemTracker().BytesConsumed())
	require.Same(t, small, get("a"))
	require.Same(t, small, get("b"))
}

func TestApplyCacheConcurrent(t *testing.T) {
	for _, numKeys := range []int{1, 16} {
		t.Run(strconv.Itoa(numKeys)+" keys", func(t *testing.T) {
			fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
			chk := chunk.NewChunkWithCapacity(fields, 1)
			chk.AppendInt64(0, 1)
			value := chunk.NewList(fields, 1, 1)
			value.Add(chk)
			entryMem := int64(2) + chk.MemoryUsage()
			ctx := mock.NewContext()
			ctx.GetSessionVars().MemQuotaApplyCache = 3 * entryMem
			cache, err := NewApplyCache(ctx)
			require.NoError(t, err)

			var wg sync.WaitGroup
			start := make(chan struct{})
			for worker := range 16 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					key := strconv.Itoa(worker % numKeys)
					key = strings.Repeat("0", 2-len(key)) + key
					<-start
					for range 100 {
						ok, err := cache.Set([]byte(key), value)
						if err != nil || !ok {
							t.Errorf("Set failed: admitted=%v, err=%v", ok, err)
							return
						}
						result, err := cache.Get([]byte(key))
						if err != nil || (result != nil && result != value) || (numKeys == 1 && result == nil) {
							t.Errorf("Get returned an unexpected value or error: %v", err)
							return
						}
						cache.lock.Lock()
						used := cache.GetMemTracker().BytesConsumed()
						want := int64(cache.cache.Size()) * entryMem
						cache.lock.Unlock()
						if used != want || used < 0 || used > cache.memCapacity {
							t.Errorf("inconsistent accounting: used=%d, entries=%d, capacity=%d", used, want, cache.memCapacity)
							return
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			require.Equal(t, int64(cache.cache.Size())*entryMem, cache.GetMemTracker().BytesConsumed())
			require.LessOrEqual(t, cache.GetMemTracker().BytesConsumed(), cache.memCapacity)
			require.LessOrEqual(t, cache.GetMemTracker().MaxConsumed(), cache.memCapacity)
		})
	}
}
