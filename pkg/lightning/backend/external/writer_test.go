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
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

// only used in testing for now.
func mergeOverlappingFilesImpl(ctx context.Context,
	paths []string,
	store storage.ExternalStorage,
	readBufferSize int,
	newFilePrefix string,
	writerID string,
	memSizeLimit uint64,
	blockSize int,
	writeBatchCount uint64,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	checkHotspot bool,
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.String("writer-id", writerID),
		zap.Int("file-count", len(paths)),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	zeroOffsets := make([]uint64, len(paths))
	iter, err := NewMergeKVIter(ctx, paths, zeroOffsets, store, readBufferSize, checkHotspot, 0)
	if err != nil {
		return err
	}
	defer func() {
		err := iter.Close()
		if err != nil {
			logutil.Logger(ctx).Warn("close iterator failed", zap.Error(err))
		}
	}()

	writer := NewWriterBuilder().
		SetMemorySizeLimit(memSizeLimit).
		SetBlockSize(blockSize).
		SetOnCloseFunc(onClose).
		SetPropSizeDistance(propSizeDist).
		SetPropKeysDistance(propKeysDist).
		Build(store, newFilePrefix, writerID)

	// currently use same goroutine to do read and write. The main advantage is
	// there's no KV copy and iter can reuse the buffer.
	for iter.Next() {
		err = writer.WriteRow(ctx, iter.Key(), iter.Value(), nil)
		if err != nil {
			return err
		}
	}
	err = iter.Error()
	if err != nil {
		return err
	}
	return writer.Close(ctx)
}

func TestWriter(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()

	w := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		Build(memStore, "/test", "0")

	writer := NewEngineWriter(w)

	kvCnt := rand.Intn(10) + 10
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
		randLen := rand.Intn(10) + 1
		kvs[i].Key = make([]byte, randLen)
		_, err := rand.Read(kvs[i].Key)
		require.NoError(t, err)
		randLen = rand.Intn(10) + 1
		kvs[i].Val = make([]byte, randLen)
		_, err = rand.Read(kvs[i].Val)
		require.NoError(t, err)
	}

	require.NoError(t, writer.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvs)))
	_, err := writer.Close(ctx)
	require.NoError(t, err)

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	bufSize := rand.Intn(100) + 1
	kvReader, err := newKVReader(ctx, "/test/0/0", memStore, 0, bufSize)
	require.NoError(t, err)
	for i := 0; i < kvCnt; i++ {
		key, value, err := kvReader.nextKV()
		require.NoError(t, err)
		require.Equal(t, kvs[i].Key, key)
		require.Equal(t, kvs[i].Val, value)
	}
	_, _, err = kvReader.nextKV()
	require.Equal(t, io.EOF, err)
	require.NoError(t, kvReader.Close())

	statReader, err := newStatsReader(ctx, memStore, "/test/0_stat/0", bufSize)
	require.NoError(t, err)

	var keyCnt uint64 = 0
	for {
		p, err := statReader.nextProp()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		keyCnt += p.keys
	}
	require.Equal(t, uint64(kvCnt), keyCnt)
	require.NoError(t, statReader.Close())
}

func TestWriterFlushMultiFileNames(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()

	writer := NewWriterBuilder().
		SetPropKeysDistance(2).
		SetMemorySizeLimit(3*(lengthBytes*2+20)).
		SetBlockSize(3*(lengthBytes*2+20)).
		Build(memStore, "/test", "0")

	// 200 bytes key values.
	kvCnt := 10
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
		kvs[i].Key = make([]byte, 10)
		_, err := rand.Read(kvs[i].Key)
		require.NoError(t, err)
		kvs[i].Val = make([]byte, 10)
		_, err = rand.Read(kvs[i].Val)
		require.NoError(t, err)
	}
	for _, pair := range kvs {
		err := writer.WriteRow(ctx, pair.Key, pair.Val, nil)
		require.NoError(t, err)
	}

	err := writer.Close(ctx)
	require.NoError(t, err)

	var dataFiles, statFiles []string
	err = memStore.WalkDir(ctx, &storage.WalkOption{SubDir: "/test"}, func(path string, size int64) error {
		if strings.Contains(path, "_stat") {
			statFiles = append(statFiles, path)
		} else {
			dataFiles = append(dataFiles, path)
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, dataFiles, 4)
	require.Len(t, statFiles, 4)
	for i := 0; i < 4; i++ {
		require.Equal(t, dataFiles[i], fmt.Sprintf("/test/0/%d", i))
		require.Equal(t, statFiles[i], fmt.Sprintf("/test/0_stat/%d", i))
	}
}

func TestWriterDuplicateDetect(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()

	writer := NewWriterBuilder().
		SetPropKeysDistance(2).
		SetMemorySizeLimit(1000).
		SetKeyDuplicationEncoding(true).
		Build(memStore, "/test", "0")
	kvCount := 20
	for i := 0; i < kvCount; i++ {
		v := i
		if v == kvCount/2 {
			v-- // insert a duplicate key.
		}
		key, val := []byte{byte(v)}, []byte{byte(v)}
		err := writer.WriteRow(ctx, key, val, dbkv.IntHandle(i))
		require.NoError(t, err)
	}
	err := writer.Close(ctx)
	require.NoError(t, err)

	// test MergeOverlappingFiles will not change duplicate detection functionality.
	err = mergeOverlappingFilesImpl(
		ctx,
		[]string{"/test/0/0"},
		memStore,
		100,
		"/test2",
		"mergeID",
		1000,
		1000,
		8*1024,
		1*size.MB,
		2,
		nil,
		false,
	)
	require.NoError(t, err)

	keys := make([][]byte, 0, kvCount)
	values := make([][]byte, 0, kvCount)

	kvReader, err := newKVReader(ctx, "/test2/mergeID/0", memStore, 0, 100)
	require.NoError(t, err)
	for i := 0; i < kvCount; i++ {
		key, value, err := kvReader.nextKV()
		require.NoError(t, err)
		clonedKey := make([]byte, len(key))
		copy(clonedKey, key)
		clonedVal := make([]byte, len(value))
		copy(clonedVal, value)
		keys = append(keys, clonedKey)
		values = append(values, clonedVal)
	}
	_, _, err = kvReader.nextKV()
	require.Equal(t, io.EOF, err)
	require.NoError(t, kvReader.Close())

	dir := t.TempDir()
	db, err := pebble.Open(path.Join(dir, "duplicate"), nil)
	require.NoError(t, err)
	keyAdapter := common.DupDetectKeyAdapter{}
	data := &MemoryIngestData{
		keyAdapter:         keyAdapter,
		duplicateDetection: true,
		duplicateDB:        db,
		dupDetectOpt:       common.DupDetectOpt{ReportErrOnDup: true},
		keys:               keys,
		values:             values,
		ts:                 123,
	}
	pool := membuf.NewPool()
	defer pool.Destroy()
	iter := data.NewIter(ctx, nil, nil, pool)

	for iter.First(); iter.Valid(); iter.Next() {
	}
	err = iter.Error()
	require.Error(t, err)
	require.Contains(t, err.Error(), "found duplicate key")
}

func TestMultiFileStat(t *testing.T) {
	s := &MultipleFilesStat{
		Filenames: [][2]string{
			{"3", "5"}, {"1", "3"}, {"2", "4"},
		},
	}
	// [3, 5], [1, 3], [2, 4]
	startKeys := []dbkv.Key{{3}, {1}, {2}}
	endKeys := []dbkv.Key{{5}, {3}, {4}}
	s.build(startKeys, endKeys)
	require.EqualValues(t, []byte{1}, s.MinKey)
	require.EqualValues(t, []byte{5}, s.MaxKey)
	require.EqualValues(t, 3, s.MaxOverlappingNum)
	require.Equal(t, [][2]string{{"1", "3"}, {"2", "4"}, {"3", "5"}}, s.Filenames)
}

func TestMultiFileStatOverlap(t *testing.T) {
	s1 := MultipleFilesStat{MinKey: dbkv.Key{1}, MaxKey: dbkv.Key{100}, MaxOverlappingNum: 100}
	s2 := MultipleFilesStat{MinKey: dbkv.Key{5}, MaxKey: dbkv.Key{102}, MaxOverlappingNum: 90}
	s3 := MultipleFilesStat{MinKey: dbkv.Key{111}, MaxKey: dbkv.Key{200}, MaxOverlappingNum: 200}
	require.EqualValues(t, 200, GetMaxOverlappingTotal([]MultipleFilesStat{s1, s2, s3}))

	s3.MaxOverlappingNum = 70
	require.EqualValues(t, 190, GetMaxOverlappingTotal([]MultipleFilesStat{s1, s2, s3}))

	s3.MinKey = dbkv.Key{0}
	require.EqualValues(t, 260, GetMaxOverlappingTotal([]MultipleFilesStat{s1, s2, s3}))
}

func TestWriterMultiFileStat(t *testing.T) {
	oldMultiFileStatNum := multiFileStatNum
	t.Cleanup(func() {
		multiFileStatNum = oldMultiFileStatNum
	})
	multiFileStatNum = 3

	ctx := context.Background()
	memStore := storage.NewMemStorage()
	var summary *WriterSummary
	closeFn := func(s *WriterSummary) {
		summary = s
	}

	writer := NewWriterBuilder().
		SetPropKeysDistance(2).
		SetMemorySizeLimit(52).
		SetBlockSize(52). // 2 KV pair will trigger flush
		SetOnCloseFunc(closeFn).
		Build(memStore, "/test", "0")

	kvs := make([]common.KvPair, 0, 18)
	// [key01, key02], [key03, key04], [key05, key06]
	for i := 1; i <= 6; i++ {
		kvs = append(kvs, common.KvPair{
			Key: []byte(fmt.Sprintf("key%02d", i)),
			Val: []byte("56789"),
		})
	}
	// [key11, key13], [key12, key15], [key14, key16]
	kvs = append(kvs, common.KvPair{
		Key: []byte("key11"),
		Val: []byte("56789"),
	})
	kvs = append(kvs, common.KvPair{
		Key: []byte("key13"),
		Val: []byte("56789"),
	})
	kvs = append(kvs, common.KvPair{
		Key: []byte("key12"),
		Val: []byte("56789"),
	})
	kvs = append(kvs, common.KvPair{
		Key: []byte("key15"),
		Val: []byte("56789"),
	})
	kvs = append(kvs, common.KvPair{
		Key: []byte("key14"),
		Val: []byte("56789"),
	})
	kvs = append(kvs, common.KvPair{
		Key: []byte("key16"),
		Val: []byte("56789"),
	})
	// [key20, key22], [key21, key23], [key22, key24]
	for i := 0; i < 3; i++ {
		kvs = append(kvs, common.KvPair{
			Key: []byte(fmt.Sprintf("key2%d", i)),
			Val: []byte("56789"),
		})
		kvs = append(kvs, common.KvPair{
			Key: []byte(fmt.Sprintf("key2%d", i+2)),
			Val: []byte("56789"),
		})
	}

	for _, pair := range kvs {
		err := writer.WriteRow(ctx, pair.Key, pair.Val, nil)
		require.NoError(t, err)
	}

	err := writer.Close(ctx)
	require.NoError(t, err)

	require.Equal(t, 3, len(summary.MultipleFilesStats))
	expected := MultipleFilesStat{
		MinKey: []byte("key01"),
		MaxKey: []byte("key06"),
		Filenames: [][2]string{
			{"/test/0/0", "/test/0_stat/0"},
			{"/test/0/1", "/test/0_stat/1"},
			{"/test/0/2", "/test/0_stat/2"},
		},
		MaxOverlappingNum: 1,
	}
	require.Equal(t, expected, summary.MultipleFilesStats[0])
	expected = MultipleFilesStat{
		MinKey: []byte("key11"),
		MaxKey: []byte("key16"),
		Filenames: [][2]string{
			{"/test/0/3", "/test/0_stat/3"},
			{"/test/0/4", "/test/0_stat/4"},
			{"/test/0/5", "/test/0_stat/5"},
		},
		MaxOverlappingNum: 2,
	}
	require.Equal(t, expected, summary.MultipleFilesStats[1])
	expected = MultipleFilesStat{
		MinKey: []byte("key20"),
		MaxKey: []byte("key24"),
		Filenames: [][2]string{
			{"/test/0/6", "/test/0_stat/6"},
			{"/test/0/7", "/test/0_stat/7"},
			{"/test/0/8", "/test/0_stat/8"},
		},
		MaxOverlappingNum: 3,
	}
	require.Equal(t, expected, summary.MultipleFilesStats[2])
	require.EqualValues(t, "key01", summary.Min)
	require.EqualValues(t, "key24", summary.Max)

	allDataFiles := make([]string, 9)
	for i := range allDataFiles {
		allDataFiles[i] = fmt.Sprintf("/test/0/%d", i)
	}

	err = mergeOverlappingFilesImpl(
		ctx,
		allDataFiles,
		memStore,
		100,
		"/test2",
		"mergeID",
		52,
		52,
		8*1024,
		1*size.MB,
		2,
		closeFn,
		true,
	)
	require.NoError(t, err)
	require.Equal(t, 3, len(summary.MultipleFilesStats))
	expected = MultipleFilesStat{
		MinKey: []byte("key01"),
		MaxKey: []byte("key06"),
		Filenames: [][2]string{
			{"/test2/mergeID/0", "/test2/mergeID_stat/0"},
			{"/test2/mergeID/1", "/test2/mergeID_stat/1"},
			{"/test2/mergeID/2", "/test2/mergeID_stat/2"},
		},
		MaxOverlappingNum: 1,
	}
	require.Equal(t, expected, summary.MultipleFilesStats[0])
	expected = MultipleFilesStat{
		MinKey: []byte("key11"),
		MaxKey: []byte("key16"),
		Filenames: [][2]string{
			{"/test2/mergeID/3", "/test2/mergeID_stat/3"},
			{"/test2/mergeID/4", "/test2/mergeID_stat/4"},
			{"/test2/mergeID/5", "/test2/mergeID_stat/5"},
		},
		MaxOverlappingNum: 1,
	}
	require.Equal(t, expected, summary.MultipleFilesStats[1])
	expected = MultipleFilesStat{
		MinKey: []byte("key20"),
		MaxKey: []byte("key24"),
		Filenames: [][2]string{
			{"/test2/mergeID/6", "/test2/mergeID_stat/6"},
			{"/test2/mergeID/7", "/test2/mergeID_stat/7"},
			{"/test2/mergeID/8", "/test2/mergeID_stat/8"},
		},
		MaxOverlappingNum: 1,
	}
	require.Equal(t, expected, summary.MultipleFilesStats[2])
	require.EqualValues(t, "key01", summary.Min)
	require.EqualValues(t, "key24", summary.Max)
}

func TestWriterSort(t *testing.T) {
	t.Skip("it only tests the performance of sorty")
	commonPrefix := "abcabcabcabcabcabcabcabc"

	kvs := make([]common.KvPair, 1000000)
	for i := 0; i < 1000000; i++ {
		kvs[i].Key = []byte(commonPrefix + strconv.Itoa(int(rand.Int31())))
		kvs[i].Val = []byte(commonPrefix)
	}

	kvs2 := make([]common.KvPair, 1000000)
	copy(kvs2, kvs)

	ts := time.Now()
	sorty.MaxGor = 8
	sorty.Sort(len(kvs), func(i, j, r, s int) bool {
		if bytes.Compare(kvs[i].Key, kvs[j].Key) < 0 { // strict comparator like < or >
			if r != s {
				kvs[r], kvs[s] = kvs[s], kvs[r]
			}
			return true
		}
		return false
	})
	t.Log("thread quick sort", time.Since(ts).String())

	ts = time.Now()
	slices.SortFunc(kvs2, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})
	t.Log("quick sort", time.Since(ts).String())

	for i := 0; i < 1000000; i++ {
		require.True(t, bytes.Compare(kvs[i].Key, kvs2[i].Key) == 0)
	}
}

type writerFirstCloseFailStorage struct {
	storage.ExternalStorage
	shouldFail bool
}

func (s *writerFirstCloseFailStorage) Create(
	ctx context.Context,
	path string,
	option *storage.WriterOption,
) (storage.ExternalFileWriter, error) {
	w, err := s.ExternalStorage.Create(ctx, path, option)
	if err != nil {
		return nil, err
	}
	if strings.Contains(path, statSuffix) {
		return &firstCloseFailWriter{ExternalFileWriter: w, shouldFail: &s.shouldFail}, nil
	}
	return w, nil
}

type firstCloseFailWriter struct {
	storage.ExternalFileWriter
	shouldFail *bool
}

func (w *firstCloseFailWriter) Close(ctx context.Context) error {
	if *w.shouldFail {
		*w.shouldFail = false
		return fmt.Errorf("first close fail")
	}
	return w.ExternalFileWriter.Close(ctx)
}

func TestFlushKVsRetry(t *testing.T) {
	ctx := context.Background()
	store := &writerFirstCloseFailStorage{ExternalStorage: storage.NewMemStorage(), shouldFail: true}

	writer := NewWriterBuilder().
		SetPropKeysDistance(4).
		SetMemorySizeLimit(100).
		SetBlockSize(100). // 2 KV pair will trigger flush
		Build(store, "/test", "0")
	err := writer.WriteRow(ctx, []byte("key1"), []byte("val1"), nil)
	require.NoError(t, err)
	err = writer.WriteRow(ctx, []byte("key3"), []byte("val3"), nil)
	require.NoError(t, err)
	err = writer.WriteRow(ctx, []byte("key2"), []byte("val2"), nil)
	require.NoError(t, err)
	// manually test flushKVs
	err = writer.flushKVs(ctx, false)
	require.NoError(t, err)

	require.False(t, store.shouldFail)

	r, err := newStatsReader(ctx, store, "/test/0_stat/0", 100)
	require.NoError(t, err)
	p, err := r.nextProp()
	lastKey := []byte{}
	for err != io.EOF {
		require.NoError(t, err)
		require.True(t, bytes.Compare(lastKey, p.firstKey) < 0)
		lastKey = append(lastKey[:0], p.firstKey...)
		p, err = r.nextProp()
	}
}
