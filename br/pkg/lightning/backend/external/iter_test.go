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
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/exp/rand"
)

type trackOpenMemStorage struct {
	*storage.MemStorage
	opened atomic.Int32
}

func (s *trackOpenMemStorage) Open(ctx context.Context, path string, _ *storage.ReaderOption) (storage.ExternalFileReader, error) {
	s.opened.Inc()
	r, err := s.MemStorage.Open(ctx, path, nil)
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
			propSizeDist: 100,
			propKeysDist: 2,
		}
		rc.reset()
		kvStore, err := NewKeyValueStore(ctx, writer, rc, 1)
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
			propSizeDist: 100,
			propKeysDist: 2,
		}
		rc.reset()
		kvStore, err := NewKeyValueStore(ctx, writer, rc, 1)
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
			propSizeDist: 100,
			propKeysDist: 2,
		}
		rc.reset()
		kvStore, err := NewKeyValueStore(ctx, writer, rc, 1)
		require.NoError(t, err)
		for _, kv := range data[i] {
			err = kvStore.AddKeyValue([]byte(kv[0]), []byte(kv[1]))
			require.NoError(t, err)
		}
		if i == 0 {
			_, err = writer.Write(ctx, []byte("corrupt"))
			require.NoError(t, err)
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

func generateMockFileReader() *kvReader {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	filename := "/test1"
	size := 100_000
	writer, err := memStore.Create(ctx, filename, nil)
	if err != nil {
		panic(err)
	}
	rc := &rangePropertiesCollector{
		propSizeDist: 100,
		propKeysDist: 2,
	}
	rc.reset()
	kvStore, err := NewKeyValueStore(ctx, writer, rc, 1)
	if err != nil {
		panic(err)
	}
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("key%09d", i)
		val := fmt.Sprintf("value%09d", i)
		err = kvStore.AddKeyValue([]byte(key), []byte(val))
		if err != nil {
			panic(err)
		}
	}
	err = writer.Close(ctx)
	if err != nil {
		panic(err)
	}
	rd, err := newKVReader(ctx, filename, memStore, 0, 1_000)
	if err != nil {
		panic(err)
	}
	return rd
}

func BenchmarkValueT(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rd := generateMockFileReader()
		opener := func() (*kvReaderProxy, error) {
			return &kvReaderProxy{r: rd}, nil
		}
		it, err := newMergeIter[kvPair, kvReaderProxy](ctx, []readerOpenerFn[kvPair, kvReaderProxy]{opener})
		if err != nil {
			panic(err)
		}
		b.StartTimer()
		for it.next() {
			e := it.currElem()
			_ = e
		}
	}
}

type kvReaderPointerProxy struct {
	p string
	r *kvReader
}

func (p kvReaderPointerProxy) path() string {
	return p.p
}

func (p kvReaderPointerProxy) next() (*kvPair, error) {
	k, v, err := p.r.nextKV()
	if err != nil {
		return nil, err
	}
	return &kvPair{key: k, value: v}, nil
}

func (p kvReaderPointerProxy) setReadMode(useConcurrency bool) {
	p.r.byteReader.switchReaderMode(useConcurrency)
}

func (p kvReaderPointerProxy) close() error {
	return p.r.Close()
}

func BenchmarkPointerT(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rd := generateMockFileReader()
		opener := func() (*kvReaderPointerProxy, error) {
			return &kvReaderPointerProxy{r: rd}, nil
		}
		it, err := newMergeIter[*kvPair, kvReaderPointerProxy](ctx, []readerOpenerFn[*kvPair, kvReaderPointerProxy]{opener})
		if err != nil {
			panic(err)
		}
		b.StartTimer()
		for it.next() {
			e := it.currElem()
			_ = e
		}
	}
}

func TestMergeIterSwitchMode(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)

	testMergeIterSwitchMode(t, func(key []byte, i int) []byte {
		_, err := rand.Read(key)
		require.NoError(t, err)
		return key
	})
	testMergeIterSwitchMode(t, func(key []byte, i int) []byte {
		_, err := rand.Read(key)
		require.NoError(t, err)
		binary.BigEndian.PutUint64(key, uint64(i))
		return key
	})
	testMergeIterSwitchMode(t, func(key []byte, i int) []byte {
		_, err := rand.Read(key)
		require.NoError(t, err)
		if (i/100000)%2 == 0 {
			binary.BigEndian.PutUint64(key, uint64(i)<<40)
		}
		return key
	})
}

func testMergeIterSwitchMode(t *testing.T, f func([]byte, int) []byte) {
	st, clean := NewS3WithBucketAndPrefix(t, "test", "prefix/")
	defer clean()

	// Prepare
	writer := NewWriterBuilder().
		SetPropKeysDistance(100).
		SetMemorySizeLimit(512*1024).
		Build(st, "testprefix", "0")

	ConcurrentReaderBufferSize = 4 * 1024

	kvCount := 500000
	keySize := 100
	valueSize := 10
	kvs := make([]common.KvPair, 1)
	kvs[0] = common.KvPair{
		Key: make([]byte, keySize),
		Val: make([]byte, valueSize),
	}
	for i := 0; i < kvCount; i++ {
		kvs[0].Key = f(kvs[0].Key, i)
		_, err := rand.Read(kvs[0].Val[0:])
		require.NoError(t, err)
		err = writer.WriteRow(context.Background(), kvs[0].Key, kvs[0].Val, nil)
		require.NoError(t, err)
	}
	err := writer.Close(context.Background())
	require.NoError(t, err)

	dataNames, _, err := GetAllFileNames(context.Background(), st, "")
	require.NoError(t, err)

	offsets := make([]uint64, len(dataNames))

	iter, err := NewMergeKVIter(context.Background(), dataNames, offsets, st, 2048)
	require.NoError(t, err)

	for iter.Next() {
	}
	err = iter.Close()
	require.NoError(t, err)
}
