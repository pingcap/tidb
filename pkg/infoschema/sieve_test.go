// Copyright 2024 PingCAP, Inc.
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

package infoschema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
)

func TestGetAndSet(t *testing.T) {
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	cache := newSieve[int, int](10 * size.MB)

	for _, v := range items {
		cache.Set(v, v*10)
	}

	for _, v := range items {
		val, ok := cache.Get(v)
		require.True(t, ok)
		require.Equal(t, v*10, val)
	}

	cache.Close()
}

func TestRemove(t *testing.T) {
	cache := newSieve[int, int](10 * size.MB)
	cache.Set(1, 10)

	val, ok := cache.Get(1)
	require.True(t, ok)
	require.Equal(t, 10, val)

	// After removing the key, it should not be found
	removed := cache.Remove(1)
	require.True(t, removed)

	_, ok = cache.Get(1)
	require.False(t, ok)

	// This should not panic
	removed = cache.Remove(-1)
	require.False(t, removed)

	cache.Close()
}

func TestSievePolicy(t *testing.T) {
	var e entry[int, int]
	cache := newSieve[int, int](10 * e.Size())
	oneHitWonders := []int{1, 2, 3, 4, 5}
	popularObjects := []int{6, 7, 8, 9, 10}

	// add objects to the cache
	for _, v := range oneHitWonders {
		cache.Set(v, v)
	}
	for _, v := range popularObjects {
		cache.Set(v, v)
	}

	// hit popular objects
	for _, v := range popularObjects {
		_, ok := cache.Get(v)
		require.True(t, ok)
	}

	// add another objects to the cache
	for _, v := range oneHitWonders {
		cache.Set(v*10, v*10)
	}

	// check popular objects are not evicted
	for _, v := range popularObjects {
		_, ok := cache.Get(v)
		require.True(t, ok)
	}

	cache.Close()
}

func TestContains(t *testing.T) {
	cache := newSieve[string, string](10 * size.MB)
	require.False(t, cache.Contains("hello"))

	cache.Set("hello", "world")
	require.True(t, cache.Contains("hello"))

	cache.Close()
}

func TestCacheSize(t *testing.T) {
	var e entry[int, int]
	sz := e.Size()

	cache := newSieve[int, int](10 * size.MB)
	require.Equal(t, uint64(0), cache.Size())

	cache.Set(1, 1)
	require.Equal(t, 1*sz, cache.Size())

	// duplicated keys only update the recent-ness of the key and value
	cache.Set(1, 1)
	require.Equal(t, 1*sz, cache.Size())

	cache.Set(2, 2)
	require.Equal(t, 2*sz, cache.Size())

	cache.Close()
}

func TestPurge(t *testing.T) {
	cache := newSieve[int, int](10 * size.MB)
	cache.Set(1, 1)
	cache.Set(2, 2)
	require.Equal(t, 2, cache.Len())

	cache.Purge()
	require.Equal(t, 0, cache.Len())

	cache.Close()
}
