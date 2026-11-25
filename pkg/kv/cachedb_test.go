// Copyright 2026 PingCAP, Inc.
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

package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/coocood/freecache"
	"github.com/stretchr/testify/require"
)

func TestCacheDBUnionGet(t *testing.T) {
	ctx := context.Background()
	key := Key("key")
	const tableID int64 = 1

	t.Run("cache hit", func(t *testing.T) {
		cache := NewCacheDB()
		store := newMockMap()
		snapshot := &mockSnapshot{store: store}
		value := []byte("value")
		require.NoError(t, store.Set(key, value))

		got, err := cache.UnionGet(ctx, tableID, snapshot, key)
		require.NoError(t, err)
		require.Equal(t, value, got)

		// A cache hit must not read the snapshot again.
		require.NoError(t, store.Delete(key))
		got, err = cache.UnionGet(ctx, tableID, snapshot, key)
		require.NoError(t, err)
		require.Equal(t, value, got)
	})

	t.Run("oversized value", func(t *testing.T) {
		cache := NewCacheDB().(*cacheDB)
		store := newMockMap()
		snapshot := &mockSnapshot{store: store}
		value := bytes.Repeat([]byte("v"), 200*1024)
		require.NoError(t, store.Set(key, value))
		require.ErrorIs(t, cache.set(tableID, key, value), freecache.ErrLargeEntry)

		// Values rejected by the cache must still be readable on every miss.
		for range 2 {
			got, err := cache.UnionGet(ctx, tableID, snapshot, key)
			require.NoError(t, err)
			require.Equal(t, value, got)
			require.Nil(t, cache.get(tableID, key))
		}
	})

	t.Run("snapshot errors", func(t *testing.T) {
		cache := NewCacheDB().(*cacheDB)
		var cfg InjectionConfig
		snapshot := &InjectedSnapshot{cfg: &cfg}
		for _, snapshotErr := range []error{ErrNotExist, context.Canceled} {
			cfg.SetGetError(snapshotErr)
			got, err := cache.UnionGet(ctx, tableID, snapshot, key)
			require.ErrorIs(t, err, snapshotErr)
			require.Nil(t, got)
			require.Empty(t, cache.memTables)
		}
	})
}
