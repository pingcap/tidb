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

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type trackOpenMemStorage struct {
	*storage.MemStorage
	opened atomic.Int32
}

func (s *trackOpenMemStorage) Open(ctx context.Context, path string) (storage.ExternalFileReader, error) {
	s.opened.Inc()
	r, err := s.MemStorage.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return &trackOpenFileReader{r, s}, nil
}

type trackOpenFileReader struct {
	storage.ExternalFileReader
	store *trackOpenMemStorage
}

func (r *trackOpenFileReader) Close() error {
	err := r.ExternalFileReader.Close()
	if err != nil {
		return err
	}
	r.store.opened.Dec()
	return nil
}

func TestMergeIter(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	filenames := []string{"/test1", "/test2", "/test3"}
	data := [][][2]string{
		{},
		{{"key1", "value1"}, {"key3", "value3"}},
		{{"key2", "value2"}},
	}
	for i, filename := range filenames {
		writer, err := memStore.Create(ctx, filename, nil)
		require.NoError(t, err)
		rc := &rangePropertiesCollector{
			propSizeIdxDistance: 100,
			propKeysIdxDistance: 2,
		}
		rc.reset()
		kvStore, err := NewKeyValueStore(ctx, writer, rc, 1, 1)
		require.NoError(t, err)
		for _, kv := range data[i] {
			err = kvStore.AddKeyValue([]byte(kv[0]), []byte(kv[1]))
			require.NoError(t, err)
		}
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	trackStore := &trackOpenMemStorage{MemStorage: memStore}
	mergeIter, err := NewMergeIter(ctx, filenames, []uint64{0, 0, 0}, trackStore, 5)
	require.NoError(t, err)
	// close one empty file immediately in NewMergeIter
	require.EqualValues(t, 2, trackStore.opened.Load())

	got := make([][2]string, 0)
	for mergeIter.Valid() {
		mergeIter.Next()
		got = append(got, [2]string{string(mergeIter.Key()), string(mergeIter.Value())})
	}
	expected := [][2]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}
	require.Equal(t, expected, got)
	err = mergeIter.Close()
	require.NoError(t, err)
	require.EqualValues(t, 0, trackStore.opened.Load())
}
