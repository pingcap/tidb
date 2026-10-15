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

package orphandata

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/importinto/conflictpath"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

type walkEntry struct {
	path string
	size int64
}

type walkStorage struct {
	storeapi.Storage
	entries   []walkEntry
	walkCount int
}

func (s *walkStorage) WalkDir(
	ctx context.Context,
	_ *storeapi.WalkOption,
	fn func(path string, size int64) error,
) error {
	s.walkCount++
	for _, entry := range s.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(entry.path, entry.size); err != nil {
			return err
		}
	}
	return nil
}

func TestScan(t *testing.T) {
	t.Run("count objects and sum bytes", func(t *testing.T) {
		ctx := context.Background()
		store := objstore.NewMemStorage()
		require.NoError(t, store.WriteFile(ctx, "123/meta.json", []byte("123")))
		require.NoError(t, store.WriteFile(ctx, "p00000001/456/data", []byte("45678")))
		require.NoError(t, store.WriteFile(ctx, "unknown/file", nil))

		stats, err := scanOrphanData(ctx, store, nil)
		require.NoError(t, err)
		require.Equal(t, scanStats{
			sizeBytes:     8,
			objectCount:   3,
			sampleObjects: []string{"123/meta.json", "p00000001/456/data", "unknown/file"},
		}, stats)
	})

	t.Run("ignore negative object size", func(t *testing.T) {
		store := &walkStorage{
			Storage: objstore.NewMemStorage(),
			entries: []walkEntry{{path: "unknown/file", size: -1}},
		}

		stats, err := scanOrphanData(context.Background(), store, nil)
		require.NoError(t, err)
		require.Equal(t, scanStats{
			objectCount:   1,
			sampleObjects: []string{"unknown/file"},
		}, stats)
		require.Equal(t, 1, store.walkCount)
	})

	t.Run("skip retained conflict-row namespace", func(t *testing.T) {
		ctx := context.Background()
		store := objstore.NewMemStorage()
		require.NoError(t, store.WriteFile(ctx, "123/meta.json", []byte("123")))
		require.NoError(t, store.WriteFile(ctx, "conflicted-rows/9/3-uuid/0_1", []byte("conflict")))

		stats, err := scanOrphanData(ctx, store, []string{conflictpath.StoragePrefix})
		require.NoError(t, err)
		require.Equal(t, scanStats{
			sizeBytes:     3,
			objectCount:   1,
			sampleObjects: []string{"123/meta.json"},
		}, stats)
	})

	t.Run("sample is bounded", func(t *testing.T) {
		entries := make([]walkEntry, 0, sampleObjectLimit+1)
		for i := 0; i <= sampleObjectLimit; i++ {
			entries = append(entries, walkEntry{path: fmt.Sprintf("object-%02d/file", i), size: 1})
		}
		store := &walkStorage{
			Storage: objstore.NewMemStorage(),
			entries: entries,
		}

		stats, err := scanOrphanData(context.Background(), store, nil)
		require.NoError(t, err)
		require.Equal(t, int64(len(entries)), stats.objectCount)
		require.Len(t, stats.sampleObjects, sampleObjectLimit)
		require.True(t, stats.truncated)
		require.Equal(t, entries[0].path, stats.sampleObjects[0])
	})
}
