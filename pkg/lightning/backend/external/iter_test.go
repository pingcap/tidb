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
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
		kvStore, err := NewKeyValueStore(ctx, writer, rc)
		require.NoError(t, err)
		for _, kv := range data[i] {
			err = kvStore.addEncodedData(getEncodedData([]byte(kv[0]), []byte(kv[1])))
			require.NoError(t, err)
		}
		kvStore.Close()
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	trackStore := &trackOpenMemStorage{MemStorage: memStore}
	iter, err := NewMergeKVIter(ctx, filenames, []uint64{0, 0, 0}, trackStore, 5, true, 0)
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
		kvStore, err := NewKeyValueStore(ctx, writer, rc)
		require.NoError(t, err)
		for _, kv := range data[i] {
			err = kvStore.addEncodedData(getEncodedData([]byte(kv[0]), []byte(kv[1])))
			require.NoError(t, err)
		}
		kvStore.Close()
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	trackStore := &trackOpenMemStorage{MemStorage: memStore}
	iter, err := NewMergeKVIter(ctx, filenames, []uint64{0, 0, 0}, trackStore, 5, true, 0)
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
	iter, err := NewMergeKVIter(ctx, []string{filenames[0]}, []uint64{0}, trackStore, 5, false, 0)
	require.NoError(t, err)
	require.EqualValues(t, 0, trackStore.opened.Load())
	require.False(t, iter.Next())
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())

	iter, err = NewMergeKVIter(ctx, filenames, []uint64{0, 0}, trackStore, 5, false, 0)
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
		kvStore, err := NewKeyValueStore(ctx, writer, rc)
		require.NoError(t, err)
		for _, kv := range data[i] {
			err = kvStore.addEncodedData(getEncodedData([]byte(kv[0]), []byte(kv[1])))
			require.NoError(t, err)
		}
		kvStore.Close()
		if i == 0 {
			_, err = writer.Write(ctx, []byte("corrupt"))
			require.NoError(t, err)
		}
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	trackStore := &trackOpenMemStorage{MemStorage: memStore}
	iter, err := NewMergeKVIter(ctx, filenames, []uint64{0, 0, 0}, trackStore, 5, true, 0)
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

func TestMergeIterSwitchMode(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)

	testMergeIterSwitchMode(t, func(key []byte, i int) []byte {
		_, err := rand.Read(key)
		require.NoError(t, err)
		return key
	})
	t.Log("success one case")
	testMergeIterSwitchMode(t, func(key []byte, i int) []byte {
		_, err := rand.Read(key)
		require.NoError(t, err)
		binary.BigEndian.PutUint64(key, uint64(i))
		return key
	})
	t.Log("success two cases")
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

	ConcurrentReaderBufferSizePerConc = 4 * 1024

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

	iter, err := NewMergeKVIter(context.Background(), dataNames, offsets, st, 2048, true, 0)
	require.NoError(t, err)

	for iter.Next() {
	}
	err = iter.Close()
	require.NoError(t, err)
}

type eofReader struct {
	storage.ExternalFileReader
}

func (r eofReader) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

func (r eofReader) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func TestReadAfterCloseConnReader(t *testing.T) {
	ctx := context.Background()

	reader := &byteReader{
		ctx:           ctx,
		storageReader: eofReader{},
		smallBuf:      []byte{0, 255, 255, 255, 255, 255, 255, 255},
		curBufOffset:  8,
		logger:        logutil.Logger(ctx),
	}
	reader.curBuf = [][]byte{reader.smallBuf}
	pool := membuf.NewPool()
	reader.concurrentReader.largeBufferPool = pool.NewBuffer()
	reader.concurrentReader.store = storage.NewMemStorage()

	// set current reader to concurrent reader, and then close it
	reader.concurrentReader.now = true
	err := reader.switchConcurrentMode(false)
	require.NoError(t, err)

	wrapKVReader := &kvReader{reader}
	_, _, err = wrapKVReader.nextKV()
	require.ErrorIs(t, err, io.EOF)
}

func TestHotspot(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()

	// 2 files, check hotspot is 0 -> nil -> 1 -> 0 -> 1
	keys := [][]string{
		{"key00", "key01", "key02", "key06", "key07"},
		{"key03", "key04", "key05", "key08", "key09"},
	}
	value := make([]byte, 5)
	filenames := []string{"/test0", "/test1"}
	for i, filename := range filenames {
		writer, err := store.Create(ctx, filename, nil)
		require.NoError(t, err)
		rc := &rangePropertiesCollector{
			propSizeDist: 100,
			propKeysDist: 2,
		}
		rc.reset()
		kvStore, err := NewKeyValueStore(ctx, writer, rc)
		require.NoError(t, err)
		for _, k := range keys[i] {
			err = kvStore.addEncodedData(getEncodedData([]byte(k), value))
			require.NoError(t, err)
		}
		kvStore.Close()
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	// readerBufSize = 8+5+8+5, every KV will cause reload
	iter, err := NewMergeKVIter(ctx, filenames, make([]uint64, len(filenames)), store, 26, true, 0)
	require.NoError(t, err)
	iter.iter.checkHotspotPeriod = 2
	// after read key00 and key01 from reader_0, it becomes hotspot
	require.True(t, iter.Next())
	require.Equal(t, "key00", string(iter.Key()))
	require.True(t, iter.Next())
	require.Equal(t, "key01", string(iter.Key()))
	require.True(t, iter.Next())
	r0 := &iter.iter.readers[0].r.byteReader.concurrentReader
	require.True(t, r0.expected)
	require.True(t, r0.now)
	r1 := &iter.iter.readers[1].r.byteReader.concurrentReader
	require.False(t, r1.expected)
	require.False(t, r1.now)
	// after read key02 and key03 from reader_0 and reader_1, no hotspot
	require.Equal(t, "key02", string(iter.Key()))
	require.True(t, iter.Next())
	require.Equal(t, "key03", string(iter.Key()))
	require.True(t, iter.Next())
	require.False(t, r0.expected)
	require.False(t, r0.now)
	require.False(t, r1.expected)
	require.False(t, r1.now)
	// after read key04 and key05 from reader_1, it becomes hotspot
	require.Equal(t, "key04", string(iter.Key()))
	require.True(t, iter.Next())
	require.Equal(t, "key05", string(iter.Key()))
	require.True(t, iter.Next())
	require.False(t, r0.expected)
	require.False(t, r0.now)
	require.True(t, r1.expected)
	require.True(t, r1.now)
	// after read key06 and key07 from reader_0, it becomes hotspot
	require.Equal(t, "key06", string(iter.Key()))
	require.True(t, iter.Next())
	require.Equal(t, "key07", string(iter.Key()))
	require.True(t, iter.Next())
	require.Nil(t, iter.iter.readers[0])
	require.False(t, r1.expected)
	require.False(t, r1.now)
	// after read key08 and key09 from reader_1, it becomes hotspot
	require.Equal(t, "key08", string(iter.Key()))
	require.True(t, iter.Next())
	require.Equal(t, "key09", string(iter.Key()))
	require.False(t, iter.Next())
	require.Nil(t, iter.iter.readers[1])

	require.NoError(t, iter.Error())
}

func TestMemoryUsageWhenHotspotChange(t *testing.T) {
	backup := ConcurrentReaderBufferSizePerConc
	ConcurrentReaderBufferSizePerConc = 100 * 1024 * 1024 // 100MB, make memory leak more obvious
	t.Cleanup(func() {
		ConcurrentReaderBufferSizePerConc = backup
	})

	getMemoryInUse := func() uint64 {
		runtime.GC()
		s := runtime.MemStats{}
		runtime.ReadMemStats(&s)
		return s.HeapInuse
	}

	ctx := context.Background()
	dir := t.TempDir()
	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	// check if we will leak 100*100MB = 1GB memory
	cur := 0
	largeChunk := make([]byte, 10*1024*1024)
	filenames := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		filename := fmt.Sprintf("/test%06d", i)
		filenames = append(filenames, filename)
		writer, err := store.Create(ctx, filename, nil)
		require.NoError(t, err)
		rc := &rangePropertiesCollector{
			propSizeDist: 100,
			propKeysDist: 2,
		}
		rc.reset()
		kvStore, err := NewKeyValueStore(ctx, writer, rc)
		require.NoError(t, err)
		for j := 0; j < 1000; j++ {
			key := fmt.Sprintf("key%06d", cur)
			val := fmt.Sprintf("value%06d", cur)
			err = kvStore.addEncodedData(getEncodedData([]byte(key), []byte(val)))
			require.NoError(t, err)
			cur++
		}
		for j := 0; j <= 12; j++ {
			key := fmt.Sprintf("key999%06d", cur+j)
			err = kvStore.addEncodedData(getEncodedData([]byte(key), largeChunk))
			require.NoError(t, err)
		}
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	beforeMem := getMemoryInUse()

	iter, err := NewMergeKVIter(ctx, filenames, make([]uint64, len(filenames)), store, 1024, true, 16)
	require.NoError(t, err)
	iter.iter.checkHotspotPeriod = 10
	i := 0
	for cur > 0 {
		cur--
		require.True(t, iter.Next())
		require.Equal(t, fmt.Sprintf("key%06d", i), string(iter.Key()))
		require.Equal(t, fmt.Sprintf("value%06d", i), string(iter.Value()))
		i++
	}

	afterMem := getMemoryInUse()
	t.Logf("memory usage: %d -> %d", beforeMem, afterMem)
	delta := afterMem - beforeMem
	// before the fix, delta is about 7.5GB
	require.Less(t, delta, uint64(4*1024*1024*1024))
	_ = iter.Close()
}

type myInt int

func (m myInt) sortKey() []byte {
	return []byte{byte(m)}
}

func (m myInt) cloneInnerFields() {}

func (m myInt) len() int { return 1 }

type intReader struct {
	ints   []int
	refCnt *atomic.Int64
}

const errInt = -1

func (i *intReader) path() string { return "" }

func (i *intReader) next() (myInt, error) {
	if len(i.ints) == 0 {
		return 0, io.EOF
	}
	ret := i.ints[0]
	i.ints = i.ints[1:]
	if ret == errInt {
		return 0, fmt.Errorf("mock error")
	}
	return myInt(ret), nil
}

func (i *intReader) switchConcurrentMode(bool) error { return nil }

func (i *intReader) close() error {
	i.refCnt.Dec()
	return nil
}

func buildOpener(in [][]int, refCnt *atomic.Int64) []readerOpenerFn[myInt, *intReader] {
	ret := make([]readerOpenerFn[myInt, *intReader], 0, len(in))
	for _, ints := range in {
		ints := ints
		ret = append(ret, func() (**intReader, error) {
			refCnt.Inc()
			r := &intReader{ints, refCnt}
			return &r, nil
		})
	}
	return ret
}

func TestLimitSizeMergeIter(t *testing.T) {
	ctx := context.Background()
	refCnt := atomic.NewInt64(0)
	readerOpeners := buildOpener([][]int{
		{1, 2, 3}, {4, 5, 6}, {7, 8, 9},
	}, refCnt)
	weight := []int64{1, 1, 1}

	oneToNine := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	for limit := int64(1); limit <= 4; limit++ {
		refCnt.Store(0)
		i, err := newLimitSizeMergeIter(ctx, readerOpeners, weight, limit)
		require.NoError(t, err)
		var got []int
		ok, _ := i.next()
		for ok {
			got = append(got, int(i.curr))
			require.LessOrEqual(t, refCnt.Load(), limit)
			ok, _ = i.next()
		}
		require.NoError(t, i.err)
		require.Equal(t, oneToNine, got)

		// check it can return error
		for errIdx := 1; errIdx <= 9; errIdx++ {
			nums := make([]int, 9)
			for i := range nums {
				nums[i] = i + 1
				if nums[i] == errIdx {
					nums[i] = errInt
				}
			}
			readerOpeners := buildOpener([][]int{nums[:3], nums[3:6], nums[6:]}, refCnt)
			i, err = newLimitSizeMergeIter(ctx, readerOpeners, weight, limit)
			if err != nil {
				require.EqualError(t, err, "mock error")
				continue
			}
			var got []int
			ok, _ = i.next()
			for ok {
				got = append(got, int(i.curr))
				ok, _ = i.next()
			}
			require.ErrorContains(t, i.err, "mock error")
			require.Less(t, len(got), 9)
		}
	}
}

func TestLimitSizeMergeIterDiffWeight(t *testing.T) {
	ctx := context.Background()
	refCnt := atomic.NewInt64(0)
	readerOpeners := buildOpener([][]int{
		{1, 4, 7}, {2, 5}, {3},
		{10},
		{11, 14, 17}, {12, 15}, {13},
	}, refCnt)
	weight := []int64{
		1, 1, 1,
		3,
		1, 1, 1,
	}
	limit := int64(3)

	expected := []int{1, 2, 3, 4, 5, 7, 10, 11, 12, 13, 14, 15, 17}
	expectedRefCnt := []int64{3, 3, 3, 2, 2, 1, 1, 3, 3, 3, 2, 2, 1}
	iter, err := newLimitSizeMergeIter(ctx, readerOpeners, weight, limit)
	require.NoError(t, err)
	for i, exp := range expected {
		ok, _ := iter.next()
		require.True(t, ok)
		require.Equal(t, exp, int(iter.curr), "i: %d", i)
		require.Equal(t, expectedRefCnt[i], refCnt.Load(), "i: %d", i)
	}

	ok, _ := iter.next()
	require.False(t, ok)
	require.NoError(t, iter.err)
	require.Equal(t, int64(0), refCnt.Load())
}

type slowOpenStorage struct {
	*storage.MemStorage
	sleep   time.Duration
	openCnt atomic.Int32
}

func (s *slowOpenStorage) Open(
	ctx context.Context,
	filePath string,
	o *storage.ReaderOption,
) (storage.ExternalFileReader, error) {
	time.Sleep(s.sleep)
	s.openCnt.Inc()
	return s.MemStorage.Open(ctx, filePath, o)
}

func TestMergePropBaseIter(t *testing.T) {
	// this test should be finished around 1 second. However, due to CI is not
	// stable, we don't check the time.
	oneOpenSleep := time.Second

	fileNum := 16
	filenames := make([]string, fileNum)
	for i := range filenames {
		filenames[i] = fmt.Sprintf("/test%06d", i)
	}
	ctx := context.Background()
	store := &slowOpenStorage{
		MemStorage: storage.NewMemStorage(),
		sleep:      oneOpenSleep,
	}
	for i, filename := range filenames {
		writer, err := store.Create(ctx, filename, nil)
		require.NoError(t, err)
		prop := &rangeProperty{firstKey: []byte{byte(i)}}
		buf := encodeMultiProps(nil, []*rangeProperty{prop})
		_, err = writer.Write(ctx, buf)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	multiStat := MultipleFilesStat{MaxOverlappingNum: 1}
	for _, f := range filenames {
		multiStat.Filenames = append(multiStat.Filenames, [2]string{"", f})
	}
	iter, err := newMergePropBaseIter(ctx, multiStat, store)
	require.NoError(t, err)
	for i := 0; i < fileNum; i++ {
		p, err := iter.next()
		require.NoError(t, err)
		require.EqualValues(t, i, p.firstKey[0])
	}

	_, err = iter.next()
	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, fileNum, store.openCnt.Load())
}

func TestEmptyBaseReader4LimitSizeMergeIter(t *testing.T) {
	fileNum := 100
	filenames := make([]string, fileNum)
	for i := range filenames {
		filenames[i] = fmt.Sprintf("/test%06d", i)
	}
	ctx := context.Background()
	store := &slowOpenStorage{
		MemStorage: storage.NewMemStorage(),
	}
	// empty file so reader will be closed at init
	for _, filename := range filenames {
		writer, err := store.Create(ctx, filename, nil)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	multiStat := MultipleFilesStat{MaxOverlappingNum: 1}
	for _, f := range filenames {
		multiStat.Filenames = append(multiStat.Filenames, [2]string{"", f})
	}
	iter, err := newMergePropBaseIter(ctx, multiStat, store)
	require.NoError(t, err)

	_, err = iter.next()
	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, fileNum, store.openCnt.Load())
}

func TestCloseLimitSizeMergeIterHalfway(t *testing.T) {
	fileNum := 10000
	filenames := make([]string, fileNum)
	for i := range filenames {
		filenames[i] = fmt.Sprintf("/test%06d", i)
	}
	ctx := context.Background()
	store := &trackOpenMemStorage{MemStorage: storage.NewMemStorage()}

	for i, filename := range filenames {
		writer, err := store.Create(ctx, filename, nil)
		require.NoError(t, err)
		prop := &rangeProperty{firstKey: []byte{byte(i)}}
		buf := encodeMultiProps(nil, []*rangeProperty{prop})
		_, err = writer.Write(ctx, buf)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	multiStat := MultipleFilesStat{MaxOverlappingNum: 1}
	for _, f := range filenames {
		multiStat.Filenames = append(multiStat.Filenames, [2]string{"", f})
	}
	iter, err := newMergePropBaseIter(ctx, multiStat, store)
	require.NoError(t, err)

	_, err = iter.next()
	require.NoError(t, err)

	err = iter.close()
	require.NoError(t, err)
	require.EqualValues(t, 0, store.opened.Load())
}
