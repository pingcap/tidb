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
	"io"
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

func TestMergeKVIter(t *testing.T) {
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
	iter, err := NewMergeKVIter(ctx, filenames, []uint64{0, 0, 0}, trackStore, 5)
	require.NoError(t, err)
	// close one empty file immediately in NewMergeKVIter
	require.EqualValues(t, 2, trackStore.opened.Load())

	got := make([][2]string, 0)
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.False(t, iter.Next())
	require.NoError(t, iter.Error())

	expected := [][2]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}
	require.Equal(t, expected, got)
	err = iter.Close()
	require.NoError(t, err)
	require.EqualValues(t, 0, trackStore.opened.Load())
}

func TestOneUpstream(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	filenames := []string{"/test1"}
	data := [][][2]string{
		{{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}},
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
	iter, err := NewMergeKVIter(ctx, filenames, []uint64{0, 0, 0}, trackStore, 5)
	require.NoError(t, err)
	require.EqualValues(t, 1, trackStore.opened.Load())

	got := make([][2]string, 0)
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.False(t, iter.Next())
	require.NoError(t, iter.Error())

	expected := [][2]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}
	require.Equal(t, expected, got)
	err = iter.Close()
	require.NoError(t, err)
	require.EqualValues(t, 0, trackStore.opened.Load())
}

func TestAllEmpty(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	filenames := []string{"/test1", "/test2"}
	for _, filename := range filenames {
		writer, err := memStore.Create(ctx, filename, nil)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	trackStore := &trackOpenMemStorage{MemStorage: memStore}
	iter, err := NewMergeKVIter(ctx, []string{filenames[0]}, []uint64{0}, trackStore, 5)
	require.NoError(t, err)
	require.EqualValues(t, 0, trackStore.opened.Load())
	require.False(t, iter.Next())
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())

	iter, err = NewMergeKVIter(ctx, filenames, []uint64{0, 0}, trackStore, 5)
	require.NoError(t, err)
	require.EqualValues(t, 0, trackStore.opened.Load())
	require.False(t, iter.Next())
	require.NoError(t, iter.Close())
}

func TestCorruptContent(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	filenames := []string{"/test1", "/test2"}
	data := [][][2]string{
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
		if i == 0 {
			_, err = writer.Write(ctx, []byte("corrupt"))
		}
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	trackStore := &trackOpenMemStorage{MemStorage: memStore}
	iter, err := NewMergeKVIter(ctx, filenames, []uint64{0, 0, 0}, trackStore, 5)
	require.NoError(t, err)
	require.EqualValues(t, 2, trackStore.opened.Load())

	got := make([][2]string, 0)
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.True(t, iter.Next())
	got = append(got, [2]string{string(iter.Key()), string(iter.Value())})
	require.False(t, iter.Next())
	require.ErrorIs(t, iter.Error(), io.ErrUnexpectedEOF)

	expected := [][2]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}
	require.Equal(t, expected, got)
	err = iter.Close()
	require.NoError(t, err)
	require.EqualValues(t, 0, trackStore.opened.Load())
}
