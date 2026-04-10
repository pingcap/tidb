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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvcache_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/memory"
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
	for i := range 8 {
		mk.hash[i] = byte((mk.key >> ((uint(i) - 1) * 8)) & 0xff)
	}
	return mk.hash
}

func newMockHashKey(key int64) *mockCacheKey {
	return &mockCacheKey{
		key: key,
	}
}

func RunPut(t *testing.T) {
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lruMaxMem := kvcache.NewSimpleLRUCache(3, 0, maxMem)
	lruZeroQuota := kvcache.NewSimpleLRUCache(3, 0, 0)
	require.Equal(t, uint(3), lruMaxMem.Capacity())
	require.Equal(t, uint(3), lruMaxMem.Capacity())

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)
	maxMemDroppedKv := make(map[kvcache.Key]kvcache.Value)
	zeroQuotaDroppedKv := make(map[kvcache.Key]kvcache.Value)

	// test onEvict function
	lruMaxMem.SetOnEvict(func(key kvcache.Key, value kvcache.Value) {
		maxMemDroppedKv[key] = value
	})
	// test onEvict function on 0 value of quota
	lruZeroQuota.SetOnEvict(func(key kvcache.Key, value kvcache.Value) {
		zeroQuotaDroppedKv[key] = value
	})
	for i := range 5 {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lruMaxMem.Put(keys[i], vals[i])
		lruZeroQuota.Put(keys[i], vals[i])
	}
	require.Equal(t, lruMaxMem.Elements(), lruMaxMem.Capacity())
	require.Equal(t, lruZeroQuota.Elements(), lruZeroQuota.Capacity())
	require.Equal(t, uint(3), lruMaxMem.Elements())
	require.Equal(t, lruZeroQuota.Elements(), lruMaxMem.Elements())

	// test for non-existent elements
	require.Len(t, maxMemDroppedKv, 2)
	for i := range 2 {
		element, exists := lruMaxMem.GetElements()[string(keys[i].Hash())]
		require.False(t, exists)
		require.Nil(t, element)
		require.Equal(t, vals[i], maxMemDroppedKv[keys[i]])
		require.Equal(t, vals[i], zeroQuotaDroppedKv[keys[i]])
	}

	// test for existent elements
	root := lruMaxMem.GetCache().Front()
	require.NotNil(t, root)
	for i := 4; i >= 2; i-- {
		entry, ok := root.Value.(*cacheEntry)
		require.True(t, ok)
		require.NotNil(t, entry)

		// test key
		key := entry.key
		require.NotNil(t, key)
		require.Equal(t, keys[i], key)

		element, exists := lruMaxMem.GetElements()[string(keys[i].Hash())]
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

func RunZeroQuota(t *testing.T) {
	lru := kvcache.NewSimpleLRUCache(100, 0, 0)
	require.Equal(t, uint(100), lru.Capacity())

	keys := make([]*mockCacheKey, 100)
	vals := make([]int64, 100)

	for i := range 100 {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	require.Equal(t, lru.Elements(), lru.Capacity())
	require.Equal(t, uint(100), lru.Elements())
}

func RunOOMGuard(t *testing.T) {
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := kvcache.NewSimpleLRUCache(3, 1.0, maxMem)
	require.Equal(t, uint(3), lru.Capacity())

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := range 5 {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	require.Equal(t, uint(0), lru.Elements())

	// test for non-existent elements
	for i := range 5 {
		element, exists := lru.GetElements()[string(keys[i].Hash())]
		require.False(t, exists)
		require.Nil(t, element)
	}
}

func RunGet(t *testing.T) {
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := kvcache.NewSimpleLRUCache(3, 0, maxMem)

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := range 5 {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}

	// test for non-existent elements
	for i := range 2 {
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
		require.Equal(t, uint(3), lru.Capacity())

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

func RunDelete(t *testing.T) {
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := kvcache.NewSimpleLRUCache(3, 0, maxMem)

	keys := make([]*mockCacheKey, 3)
	vals := make([]int64, 3)

	for i := range 3 {
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

func RunDeleteAll(t *testing.T) {
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := kvcache.NewSimpleLRUCache(3, 0, maxMem)

	keys := make([]*mockCacheKey, 3)
	vals := make([]int64, 3)

	for i := range 3 {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	require.Equal(t, 3, int(lru.size))

	lru.DeleteAll()

	for i := range 3 {
		value, exists := lru.Get(keys[i])
		require.False(t, exists)
		require.Nil(t, value)
		require.Equal(t, 0, int(lru.size))
	}
}

func RunValues(t *testing.T) {
	maxMem, err := memory.MemTotal()
	require.NoError(t, err)

	lru := kvcache.NewSimpleLRUCache(5, 0, maxMem)

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := range 5 {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}

	values := lru.Values()
	require.Equal(t, 5, len(values))
	for i := range 5 {
		require.Equal(t, int64(4-i), values[i])
	}
}

func RunPutProfileName(t *testing.T) {
	lru := kvcache.NewSimpleLRUCache(3, 0, 10)
	require.Equal(t, uint(3), lru.Capacity())
	tem := reflect.TypeOf(*lru)
	pt := reflect.TypeOf(lru)
	functionName := ""
	for i := range pt.NumMethod() {
		if pt.Method(i).Name == "Put" {
			functionName = "Put"
		}
	}
	pName := fmt.Sprintf("%s.(*%s).%s", tem.PkgPath(), tem.Name(), functionName)
	require.Equal(t, ProfileName, pName)
}
