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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRUOp(t *testing.T) {
	lru := NewStandardLRUCache(3)
	require.Equal(t, uint64(3), lru.capacity)

	// insert 1,2,3
	for i := 1; i <= 3; i++ {
		key := newMockHashKey(int64(i))
		lru.Put(key, i, 1)
		value, ok := lru.Get(key)
		require.True(t, ok)
		require.Equal(t, value.(int), i)
	}
	_, ok := lru.Get(newMockHashKey(4))
	require.False(t, ok)

	// insert 4, become 2,3,4
	lru.Put(newMockHashKey(4), 4, 1)
	v, ok := lru.Get(newMockHashKey(4))
	require.True(t, ok)
	require.Equal(t, v.(int), 4)
	_, ok = lru.Get(newMockHashKey(1))
	require.False(t, ok)

	// delete 4, become 2,3
	lru.Delete(newMockHashKey(4))
	_, ok = lru.Get(newMockHashKey(4))
	require.False(t, ok)

	// insert 5, become 2,3,5
	lru.Put(newMockHashKey(5), 5, 1)
	v, ok = lru.Get(newMockHashKey(5))
	require.True(t, ok)
	require.Equal(t, v.(int), 5)

	v, ok = lru.Get(newMockHashKey(3))
	require.True(t, ok)
	require.Equal(t, v.(int), 3)

	v, ok = lru.Get(newMockHashKey(2))
	require.True(t, ok)
	require.Equal(t, v.(int), 2)
}

func TestLRUCopy(t *testing.T) {
	lru := NewStandardLRUCache(3)
	require.Equal(t, uint64(3), lru.capacity)

	// insert 1,2,3
	for i := 1; i <= 3; i++ {
		key := newMockHashKey(int64(i))
		lru.Put(key, i, 1)
		value, ok := lru.Get(key)
		require.True(t, ok)
		require.Equal(t, value.(int), i)
	}
	newLRU := lru.Copy()
	for i := 1; i <= 3; i++ {
		key := newMockHashKey(int64(i))
		value, ok := newLRU.Get(key)
		require.True(t, ok)
		require.Equal(t, value.(int), i)
	}
	newLRU.Delete(newMockHashKey(1))
	_, ok := newLRU.Get(newMockHashKey(1))
	require.False(t, ok)

	for i := 1; i <= 3; i++ {
		key := newMockHashKey(int64(i))
		value, ok := lru.Get(key)
		require.True(t, ok)
		require.Equal(t, value.(int), i)
	}
}
