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
	_, err = seekPropsOffsets(ctx, []byte("key0"), []string{file1, file2}, store)
	require.ErrorContains(t, err, "start key 6b657930 is too small for stat files [/test1 /test2]")
	got, err = seekPropsOffsets(ctx, []byte("key1"), []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 0}, got)
	got, err = seekPropsOffsets(ctx, []byte("key999"), []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{50, 40}, got)

	file3 := "/test3"
	w3, err := store.Create(ctx, file3, nil)
	require.NoError(t, err)
	err = w3.Close(ctx)
	require.NoError(t, err)

	file4 := "/test4"
	w4, err := store.Create(ctx, file4, nil)
	require.NoError(t, err)
	_, err = w4.Write(ctx, rc1.encode())
	require.NoError(t, err)
	err = w4.Close(ctx)
	require.NoError(t, err)
	got, err = seekPropsOffsets(ctx, []byte("key3"), []string{file1, file2, file3, file4}, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{30, 20, 0, 30}, got)
}

func TestGetAllFileNames(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()
	w := NewWriterBuilder().
		SetMemorySizeLimit(20).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, "/subtask", 0)
	kvPairs := make([]common.KvPair, 0, 30)
	for i := 0; i < 30; i++ {
		kvPairs = append(kvPairs, common.KvPair{
			Key: []byte{byte(i)},
			Val: []byte{byte(i)},
		})
	}
	err := w.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvPairs))
	require.NoError(t, err)
	_, err = w.Close(ctx)
	require.NoError(t, err)

	w2 := NewWriterBuilder().
		SetMemorySizeLimit(20).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, "/subtask", 3)
	err = w2.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvPairs))
	require.NoError(t, err)
	_, err = w2.Close(ctx)
	require.NoError(t, err)

	w3 := NewWriterBuilder().
		SetMemorySizeLimit(20).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, "/subtask", 12)
	err = w3.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvPairs))
	require.NoError(t, err)
	_, err = w3.Close(ctx)
	require.NoError(t, err)

	dataFiles, statFiles, err := GetAllFileNames(ctx, store, "/subtask")
	require.NoError(t, err)
	require.Equal(t, []string{
		"/subtask/0_stat/0", "/subtask/0_stat/1", "/subtask/0_stat/2",
		"/subtask/12_stat/0", "/subtask/12_stat/1", "/subtask/12_stat/2",
		"/subtask/3_stat/0", "/subtask/3_stat/1", "/subtask/3_stat/2",
	}, statFiles)
	require.Equal(t, []string{
		"/subtask/0/0", "/subtask/0/1", "/subtask/0/2",
		"/subtask/12/0", "/subtask/12/1", "/subtask/12/2",
		"/subtask/3/0", "/subtask/3/1", "/subtask/3/2",
	}, dataFiles)
}

func TestCleanUpFiles(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()
	w := NewWriterBuilder().
		SetMemorySizeLimit(20).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, "/subtask", 0)
	kvPairs := make([]common.KvPair, 0, 30)
	for i := 0; i < 30; i++ {
		kvPairs = append(kvPairs, common.KvPair{
			Key: []byte{byte(i)},
			Val: []byte{byte(i)},
		})
	}
	err := w.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvPairs))
	require.NoError(t, err)
	_, err = w.Close(ctx)
	require.NoError(t, err)

	dataFiles, statFiles, err := GetAllFileNames(ctx, store, "/subtask")
	require.NoError(t, err)
	require.Equal(t, []string{
		"/subtask/0_stat/0", "/subtask/0_stat/1", "/subtask/0_stat/2",
	}, statFiles)
	require.Equal(t, []string{
		"/subtask/0/0", "/subtask/0/1", "/subtask/0/2",
	}, dataFiles)

	require.NoError(t, CleanUpFiles(ctx, store, "/subtask", 10))

	dataFiles, statFiles, err = GetAllFileNames(ctx, store, "/subtask")
	require.NoError(t, err)
	require.Equal(t, []string(nil), statFiles)
	require.Equal(t, []string(nil), dataFiles)
}
