// Copyright 2022 PingCAP, Inc.
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

package kvcache

import (
	"container/list"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakePlan struct {
	a int64
	s []string
}

func pickFromBucket(bucket []*list.Element, itB interface{}) (*list.Element, int, bool) {
	for i, element := range bucket {
		itemsA := element.Value.(*cacheEntry).value.(*fakePlan).s
		itemsB := itB.([]string)
		flag := true
		for j := 0; j < len(itemsA); j++ {
			if itemsA[j] != itemsB[j] {
				flag = false
				break
			}
		}
		if flag {
			return element, i, true
		}
	}
	return nil, -1, false
}

func TestLRUPCPut(t *testing.T) {
	pcLRU := NewLRUPlanCache(3, pickFromBucket)
	require.Equal(t, uint(3), pcLRU.capacity)

	keys := make([]*mockCacheKey, 5)
	vals := make([]*fakePlan, 5)
	pTypes := [][]string{{"a", "0"}, {"b", "1"}, {"c", "2"}, {"d", "3"}, {"e", "4"}}
	maxMemDroppedKv := make(map[Key]Value)

	// test onEvict function
	pcLRU.SetOnEvict(func(key Key, value Value) {
		maxMemDroppedKv[key] = value
	})

	// one key multi value
	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(1)
		vals[i] = &fakePlan{
			a: int64(i),
			s: pTypes[i],
		}
		pcLRU.Put(keys[i], vals[i], pTypes[i])
	}
	require.Equal(t, pcLRU.size, pcLRU.capacity)
	require.Equal(t, uint(3), pcLRU.size)

	// test for non-existent elements
	require.Len(t, maxMemDroppedKv, 2)
	for i := 0; i < 2; i++ {
		bucket, exist := pcLRU.buckets[string(keys[i].Hash())]
		require.True(t, exist)
		for _, element := range bucket {
			require.NotEqual(t, vals[i], element.Value.(*cacheEntry).value)
		}
		require.Equal(t, vals[i], maxMemDroppedKv[keys[i]])
	}

	// test for existent elements
	root := pcLRU.cache.Front()
	require.NotNil(t, root)
	for i := 4; i >= 2; i-- {
		entry, ok := root.Value.(*cacheEntry)
		require.True(t, ok)
		require.NotNil(t, entry)

		// test key
		key := entry.key
		require.NotNil(t, key)
		require.Equal(t, keys[i], key)

		bucket, exist := pcLRU.buckets[string(keys[i].Hash())]
		require.True(t, exist)
		element, _, exist := pcLRU.choose(bucket, pTypes[i])
		require.NotNil(t, element)
		require.True(t, exist)
		require.Equal(t, root, element)

		// test value
		value, ok := entry.value.(*fakePlan)
		require.True(t, ok)
		require.Equal(t, vals[i], value)

		root = root.Next()
	}

	// test for end of double-linked list
	require.Nil(t, root)
}

func TestLRUPCGet(t *testing.T) {
	lru := NewLRUPlanCache(3, pickFromBucket)

	keys := make([]*mockCacheKey, 5)
	vals := make([]*fakePlan, 5)
	pTypes := [][]string{{"a", "0"}, {"b", "1"}, {"c", "2"}, {"d", "3"}, {"e", "4"}}

	// 5 bucket
	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = &fakePlan{
			a: int64(i),
			s: pTypes[i],
		}
		lru.Put(keys[i], vals[i], pTypes[i])
	}

	// test for non-existent elements
	for i := 0; i < 2; i++ {
		value, exists := lru.Get(keys[i], pTypes[i])
		require.False(t, exists)
		require.Nil(t, value)
	}

	for i := 2; i < 5; i++ {
		value, exists := lru.Get(keys[i], pTypes[i])
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

		value, ok = entry.value.(*fakePlan)
		require.True(t, ok)
		require.Equal(t, vals[i], value)
	}
}

func TestLRUPCGet2(t *testing.T) {
	lru := NewLRUPlanCache(3, pickFromBucket)

	keys := make([]*mockCacheKey, 5)
	vals := make([]*fakePlan, 5)
	pTypes := [][]string{{"a", "0"}, {"b", "1"}, {"c", "2"}, {"d", "3"}, {"e", "4"}}

	// 5 bucket
	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i % 3))
		vals[i] = &fakePlan{
			a: int64(i),
			s: pTypes[i],
		}
		lru.Put(keys[i], vals[i], pTypes[i])
	}

	// test for non-existent elements
	for i := 0; i < 2; i++ {
		value, exists := lru.Get(keys[i], pTypes[i])
		require.False(t, exists)
		require.Nil(t, value)
	}

	for i := 2; i < 5; i++ {
		value, exists := lru.Get(keys[i], pTypes[i])
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

		value, ok = entry.value.(*fakePlan)
		require.True(t, ok)
		require.Equal(t, vals[i], value)
	}
}

func TestLRUPCDelete(t *testing.T) {
	lru := NewLRUPlanCache(3, pickFromBucket)

	keys := make([]*mockCacheKey, 3)
	vals := make([]*fakePlan, 3)
	pTypes := [][]string{{"a", "0"}, {"b", "1"}, {"c", "2"}}

	for i := 0; i < 3; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = &fakePlan{
			a: int64(i),
			s: pTypes[i],
		}
		lru.Put(keys[i], vals[i], pTypes[i])
	}
	require.Equal(t, 3, int(lru.size))

	lru.Delete(keys[1], pTypes[1])
	value, exists := lru.Get(keys[1], pTypes[1])
	require.False(t, exists)
	require.Nil(t, value)
	require.Equal(t, 2, int(lru.size))

	_, exists = lru.Get(keys[0], pTypes[0])
	require.True(t, exists)

	_, exists = lru.Get(keys[2], pTypes[2])
	require.True(t, exists)
}

func TestLRUPCDeleteAll(t *testing.T) {
	lru := NewLRUPlanCache(3, pickFromBucket)

	keys := make([]*mockCacheKey, 3)
	vals := make([]*fakePlan, 3)
	pTypes := [][]string{{"a", "0"}, {"b", "1"}, {"c", "2"}}

	for i := 0; i < 3; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = &fakePlan{
			a: int64(i),
			s: pTypes[i],
		}
		lru.Put(keys[i], vals[i], pTypes[i])
	}
	require.Equal(t, 3, int(lru.size))

	lru.DeleteAll()

	for i := 0; i < 3; i++ {
		value, exists := lru.Get(keys[i], pTypes[i])
		require.False(t, exists)
		require.Nil(t, value)
		require.Equal(t, 0, int(lru.size))
	}
}

func TestLRUPCKeys(t *testing.T) {
	lru := NewLRUPlanCache(5, pickFromBucket)

	keys := make([]*mockCacheKey, 5)
	vals := make([]*fakePlan, 5)
	pTypes := [][]string{{"a", "0"}, {"b", "1"}, {"c", "2"}, {"d", "3"}, {"e", "4"}}

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = &fakePlan{
			a: int64(i),
			s: pTypes[i],
		}
		lru.Put(keys[i], vals[i], pTypes[i])
	}

	ks := lru.Keys()
	require.Equal(t, 5, len(ks))
	for i := 0; i < 5; i++ {
		require.Equal(t, keys[4-i], ks[i])
	}
}

func TestLRUPCValues(t *testing.T) {
	lru := NewLRUPlanCache(5, pickFromBucket)

	keys := make([]*mockCacheKey, 5)
	vals := make([]*fakePlan, 5)
	pTypes := [][]string{{"a", "0"}, {"b", "1"}, {"c", "2"}, {"d", "3"}, {"e", "4"}}

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = &fakePlan{
			a: int64(i),
			s: pTypes[i],
		}
		lru.Put(keys[i], vals[i], pTypes[i])
	}

	values := lru.Values()
	require.Equal(t, 5, len(values))
	for i := 0; i < 5; i++ {
		require.Equal(t, vals[4-i], values[i])
	}
}
