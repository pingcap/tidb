// Copyright 2017 PingCAP, Inc.
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

package kvcache

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/require"
)

type mockCacheKey struct {
	hash []byte
	key  int64
}

func (mk *mockCacheKey) Hash() []byte {
	if mk.hash != nil {
		return mk.hash
	}
	mk.hash = make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		mk.hash[i] = byte((mk.key >> ((i - 1) * 8)) & 0xff)
	}
	return mk.hash
}

func newMockHashKey(key int64) *mockCacheKey {
	return &mockCacheKey{
		key: key,
	}
}

func TestPut(t *testing.T) {
	t.Parallel()

	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lruMaxMem := NewSimpleLRUCache(3, 0, maxMem)
	lruZeroQuota := NewSimpleLRUCache(3, 0, 0)
	require.Equal(t, uint(3), lruMaxMem.capacity)
	require.Equal(t, uint(3), lruMaxMem.capacity)

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)
	maxMemDroppedKv := make(map[Key]Value)
	zeroQuotaDroppedKv := make(map[Key]Value)

	// test onEvict function
	lruMaxMem.SetOnEvict(func(key Key, value Value) {
		maxMemDroppedKv[key] = value
	})
	// test onEvict function on 0 value of quota
	lruZeroQuota.SetOnEvict(func(key Key, value Value) {
		zeroQuotaDroppedKv[key] = value
	})
	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lruMaxMem.Put(keys[i], vals[i])
		lruZeroQuota.Put(keys[i], vals[i])
	}
	require.Equal(t, lruMaxMem.size, lruMaxMem.capacity)
	require.Equal(t, lruZeroQuota.size, lruZeroQuota.capacity)
	require.Equal(t, uint(3), lruMaxMem.size)
	require.Equal(t, lruZeroQuota.size, lruMaxMem.size)

	// test for non-existent elements
	require.Len(t, maxMemDroppedKv, 2)
	for i := 0; i < 2; i++ {
		element, exists := lruMaxMem.elements[string(keys[i].Hash())]
		require.False(t, exists)
		require.Nil(t, element)
		require.Equal(t, vals[i], maxMemDroppedKv[keys[i]])
		require.Equal(t, vals[i], zeroQuotaDroppedKv[keys[i]])
	}

	// test for existent elements
	root := lruMaxMem.cache.Front()
	require.NotNil(t, root)
	for i := 4; i >= 2; i-- {
		entry, ok := root.Value.(*cacheEntry)
		require.True(t, ok)
		require.NotNil(t, entry)

		// test key
		key := entry.key
		require.NotNil(t, key)
		require.Equal(t, keys[i], key)

		element, exists := lruMaxMem.elements[string(keys[i].Hash())]
		require.True(t, exists)
		require.NotNil(t, element)
		require.Equal(t, root, element)

		// test value
		value, ok := entry.value.(int64)
		require.True(t, ok)
		require.Equal(t, vals[i], value)

		root = root.Next()
	}

	// test for end of double-linked list
	require.Nil(t, root)
}

func TestZeroQuota(t *testing.T) {
	t.Parallel()
	lru := NewSimpleLRUCache(100, 0, 0)
	require.Equal(t, uint(100), lru.capacity)

	keys := make([]*mockCacheKey, 100)
	vals := make([]int64, 100)

	for i := 0; i < 100; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	require.Equal(t, lru.size, lru.capacity)
	require.Equal(t, uint(100), lru.size)
}

func TestOOMGuard(t *testing.T) {
	t.Parallel()
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := NewSimpleLRUCache(3, 1.0, maxMem)
	require.Equal(t, uint(3), lru.capacity)

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	require.Equal(t, uint(0), lru.size)

	// test for non-existent elements
	for i := 0; i < 5; i++ {
		element, exists := lru.elements[string(keys[i].Hash())]
		require.False(t, exists)
		require.Nil(t, element)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := NewSimpleLRUCache(3, 0, maxMem)

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}

	// test for non-existent elements
	for i := 0; i < 2; i++ {
		value, exists := lru.Get(keys[i])
		require.False(t, exists)
		require.Nil(t, value)
	}

	for i := 2; i < 5; i++ {
		value, exists := lru.Get(keys[i])
		require.True(t, exists)
		require.NotNil(t, value)
		require.Equal(t, vals[i], value)
		require.Equal(t, uint(3), lru.size)
		require.Equal(t, uint(3), lru.capacity)

		root := lru.cache.Front()
		require.NotNil(t, root)

		entry, ok := root.Value.(*cacheEntry)
		require.True(t, ok)
		require.Equal(t, keys[i], entry.key)

		value, ok = entry.value.(int64)
		require.True(t, ok)
		require.Equal(t, vals[i], value)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := NewSimpleLRUCache(3, 0, maxMem)

	keys := make([]*mockCacheKey, 3)
	vals := make([]int64, 3)

	for i := 0; i < 3; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	require.Equal(t, 3, int(lru.size))

	lru.Delete(keys[1])
	value, exists := lru.Get(keys[1])
	require.False(t, exists)
	require.Nil(t, value)
	require.Equal(t, 2, int(lru.size))

	_, exists = lru.Get(keys[0])
	require.True(t, exists)

	_, exists = lru.Get(keys[2])
	require.True(t, exists)
}

func TestDeleteAll(t *testing.T) {
	t.Parallel()
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := NewSimpleLRUCache(3, 0, maxMem)

	keys := make([]*mockCacheKey, 3)
	vals := make([]int64, 3)

	for i := 0; i < 3; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	require.Equal(t, 3, int(lru.size))

	lru.DeleteAll()

	for i := 0; i < 3; i++ {
		value, exists := lru.Get(keys[i])
		require.False(t, exists)
		require.Nil(t, value)
		require.Equal(t, 0, int(lru.size))
	}
}

func TestValues(t *testing.T) {
	t.Parallel()
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := NewSimpleLRUCache(5, 0, maxMem)

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}

	values := lru.Values()
	require.Equal(t, 5, len(values))
	for i := 0; i < 5; i++ {
		require.Equal(t, int64(4-i), values[i])
	}
}

func TestPutProfileName(t *testing.T) {
	t.Parallel()
	lru := NewSimpleLRUCache(3, 0, 10)
	require.Equal(t, uint(3), lru.capacity)
	tem := reflect.TypeOf(*lru)
	pt := reflect.TypeOf(lru)
	functionName := ""
	for i := 0; i < pt.NumMethod(); i++ {
		if pt.Method(i).Name == "Put" {
			functionName = "Put"
		}
	}
	pName := fmt.Sprintf("%s.(*%s).%s", tem.PkgPath(), tem.Name(), functionName)
	require.Equal(t, ProfileName, pName)
}
