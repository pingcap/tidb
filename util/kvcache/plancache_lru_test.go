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

func TestPutPlanCacheLRU(t *testing.T) {

	pcLRU := NewPCLRUCache(3, pickFromBucket)
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
		bucket, _ := pcLRU.buckets[string(keys[i].Hash())]
		//element, exist, _ := pcLRU.choose(elements)
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

		bucket, _ := pcLRU.buckets[string(keys[i].Hash())]
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
