// Copyright 2023 PingCAP, Inc.
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

package external

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPrettyFileNames(t *testing.T) {
	filenames := []string{
		"/tmp/br/backup/1/1_1.sst",
		"/tmp/br/2/1_2.sst",
		"/tmp/123/1/1_3",
	}
	expected := []string{
		"1/1_1.sst",
		"2/1_2.sst",
		"1/1_3",
	}
	require.Equal(t, expected, prettyFileNames(filenames))
}

func TestSeekPropsOffsets(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()

	rc1 := &rangePropertiesCollector{
		props: []*rangeProperty{
			{
				key:    []byte("key1"),
				offset: 10,
			},
			{
				key:    []byte("key3"),
				offset: 30,
			},
			{
				key:    []byte("key5"),
				offset: 50,
			},
		},
	}
	file1 := "/test1"
	w1, err := store.Create(ctx, file1, nil)
	require.NoError(t, err)
	_, err = w1.Write(ctx, rc1.encode())
	require.NoError(t, err)
	err = w1.Close(ctx)
	require.NoError(t, err)

	rc2 := &rangePropertiesCollector{
		props: []*rangeProperty{
			{
				key:    []byte("key2"),
				offset: 20,
			},
			{
				key:    []byte("key4"),
				offset: 40,
			},
		},
	}
	file2 := "/test2"
	w2, err := store.Create(ctx, file2, nil)
	require.NoError(t, err)
	_, err = w2.Write(ctx, rc2.encode())
	require.NoError(t, err)
	err = w2.Close(ctx)
	require.NoError(t, err)

	got, err := seekPropsOffsets(ctx, []byte("key2.5"), []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, got)
	got, err = seekPropsOffsets(ctx, []byte("key3"), []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{30, 20}, got)
	got, err = seekPropsOffsets(ctx, []byte("key0"), []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 0}, got)
	got, err = seekPropsOffsets(ctx, []byte("key999"), []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{50, 40}, got)
}

func TestGetAllFileNames(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()
	w := NewWriterBuilder().
		SetMemorySizeLimit(20).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, 0, "/subtask/prefix")
	kvPairs := make([]common.KvPair, 0, 100)
	for i := 0; i < 100; i++ {
		kvPairs = append(kvPairs, common.KvPair{
			Key: []byte{byte(i)},
			Val: []byte{byte(i)},
		})
	}
	err := w.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvPairs))
	require.NoError(t, err)
	_, err = w.Close(ctx)
	require.NoError(t, err)

	fileHandle, statFiles, err := GetAllFileNames(ctx, store, "/subtask")
	require.Equal(t, []string{"/prefix/0"}, statFiles)
	_ = fileHandle
}
