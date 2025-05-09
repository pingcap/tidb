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
	goerrors "errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
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
	for i := range kvCnt {
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
	for i := range kvCnt {
		key, value, err := kvReader.nextKV()
		require.NoError(t, err)
		require.Equal(t, kvs[i].Key, key)
		require.Equal(t, kvs[i].Val, value)
	}
	_, _, err = kvReader.nextKV()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, kvReader.Close())

	statReader, err := newStatsReader(ctx, memStore, "/test/0_stat/0", bufSize)
	require.NoError(t, err)

	var keyCnt uint64 = 0
	for {
		p, err := statReader.nextProp()
		if goerrors.Is(err, io.EOF) {
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
	for i := range kvCnt {
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
	for i := range 4 {
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
	for i := range kvCount {
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

	kvs := make([]kvPair, 0, kvCount)

	kvReader, err := newKVReader(ctx, "/test2/mergeID/0", memStore, 0, 100)
	require.NoError(t, err)
	for range kvCount {
		key, value, err := kvReader.nextKV()
		require.NoError(t, err)
		clonedKey := make([]byte, len(key))
		copy(clonedKey, key)
		clonedVal := make([]byte, len(value))
		copy(clonedVal, value)
		kvs = append(kvs, kvPair{key: clonedKey, value: clonedVal})
	}
	_, _, err = kvReader.nextKV()
	require.ErrorIs(t, err, io.EOF)
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
		kvs:                kvs,
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
			Key: fmt.Appendf(nil, "key%02d", i),
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
	for i := range 3 {
		kvs = append(kvs, common.KvPair{
			Key: fmt.Appendf(nil, "key2%d", i),
			Val: []byte("56789"),
		})
		kvs = append(kvs, common.KvPair{
			Key: fmt.Appendf(nil, "key2%d", i+2),
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
	for i := range 1000000 {
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

	for i := range 1000000 {
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
	for !goerrors.Is(err, io.EOF) {
		require.NoError(t, err)
		require.True(t, bytes.Compare(lastKey, p.firstKey) < 0)
		lastKey = append(lastKey[:0], p.firstKey...)
		p, err = r.nextProp()
	}
}

func TestGetAdjustedIndexBlockSize(t *testing.T) {
	require.EqualValues(t, 1*units.MiB, GetAdjustedBlockSize(1*units.MiB))
	require.EqualValues(t, 16*units.MiB, GetAdjustedBlockSize(15*units.MiB))
	require.EqualValues(t, 16*units.MiB, GetAdjustedBlockSize(16*units.MiB))
	require.EqualValues(t, 17*units.MiB, GetAdjustedBlockSize(17*units.MiB))
	require.EqualValues(t, 16*units.MiB, GetAdjustedBlockSize(166*units.MiB))
}

func readKVFile(t *testing.T, store storage.ExternalStorage, filename string) []kvPair {
	t.Helper()
	reader, err := newKVReader(context.Background(), filename, store, 0, units.KiB)
	require.NoError(t, err)
	kvs := make([]kvPair, 0)
	for {
		key, value, err := reader.nextKV()
		if goerrors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		kvs = append(kvs, kvPair{key: slices.Clone(key), value: slices.Clone(value)})
	}
	return kvs
}

type testWriter interface {
	WriteRow(ctx context.Context, key, val []byte, handle dbkv.Handle) error
	Close(ctx context.Context) error
}

func TestWriterOnDup(t *testing.T) {
	getWriterFn := func(store storage.ExternalStorage, b *WriterBuilder) testWriter {
		return b.Build(store, "/test", "0")
	}
	doTestWriterOnDupRecord(t, false, getWriterFn)
	doTestWriterOnDupRemove(t, false, getWriterFn)
}

func doTestWriterOnDupRecord(t *testing.T, testingOneFile bool, getWriter func(store storage.ExternalStorage, b *WriterBuilder) testWriter) {
	t.Helper()
	ctx := context.Background()
	store := storage.NewMemStorage()
	var summary *WriterSummary
	doGetWriter := func(store storage.ExternalStorage, builder *WriterBuilder) testWriter {
		builder = builder.SetOnCloseFunc(func(s *WriterSummary) { summary = s }).SetOnDup(engineapi.OnDuplicateKeyRecord)
		return getWriter(store, builder)
	}

	t.Run("write nothing", func(t *testing.T) {
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		require.NoError(t, writer.Close(ctx))
		require.Empty(t, summary.Min)
		require.Empty(t, summary.Max)
		require.Zero(t, summary.TotalCnt)
		require.Zero(t, summary.TotalSize)
		require.Zero(t, summary.ConflictInfo.Count)
		require.Empty(t, summary.ConflictInfo.Files)
	})

	t.Run("all duplicated", func(t *testing.T) {
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		for range 5 {
			require.NoError(t, writer.WriteRow(ctx, []byte("1111"), []byte("vvvv"), nil))
		}
		require.NoError(t, writer.Close(ctx))
		require.EqualValues(t, []byte("1111"), summary.Min)
		require.EqualValues(t, []byte("1111"), summary.Max)
		require.EqualValues(t, 2, summary.TotalCnt)
		require.EqualValues(t, 16, summary.TotalSize)
		require.EqualValues(t, 3, summary.ConflictInfo.Count)
		require.Len(t, summary.ConflictInfo.Files, 1)
		kvs := readKVFile(t, store, summary.ConflictInfo.Files[0])
		require.Len(t, kvs, 3)
		for _, p := range kvs {
			require.Equal(t, []byte("1111"), p.key)
			require.Equal(t, []byte("vvvv"), p.value)
		}
	})

	t.Run("with different duplicated kv, first kv not duplicated", func(t *testing.T) {
		// each KV will take 24 bytes, so we flush every 10 KVs
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		input := []struct {
			pair *kvPair
			cnt  int
		}{
			{pair: &kvPair{key: []byte("2222"), value: []byte("vvvv")}, cnt: 1},
			{pair: &kvPair{key: []byte("1111"), value: []byte("vvvv")}, cnt: 1},
			{pair: &kvPair{key: []byte("6666"), value: []byte("vvvv")}, cnt: 3},
			{pair: &kvPair{key: []byte("7777"), value: []byte("vvvv")}, cnt: 5},
		}
		if testingOneFile {
			sort.Slice(input, func(i, j int) bool {
				return bytes.Compare(input[i].pair.key, input[j].pair.key) < 0
			})
		}
		for _, p := range input {
			for i := 0; i < p.cnt; i++ {
				require.NoError(t, writer.WriteRow(ctx, p.pair.key, p.pair.value, nil))
			}
		}
		require.NoError(t, writer.Close(ctx))
		require.EqualValues(t, []byte("1111"), summary.Min)
		require.EqualValues(t, []byte("7777"), summary.Max)
		require.EqualValues(t, 6, summary.TotalCnt)
		require.EqualValues(t, 48, summary.TotalSize)
		require.EqualValues(t, 4, summary.ConflictInfo.Count)
		require.Len(t, summary.ConflictInfo.Files, 1)
		kvs := readKVFile(t, store, summary.ConflictInfo.Files[0])
		require.EqualValues(t, []kvPair{
			{key: []byte("6666"), value: []byte("vvvv")},
			{key: []byte("7777"), value: []byte("vvvv")},
			{key: []byte("7777"), value: []byte("vvvv")},
			{key: []byte("7777"), value: []byte("vvvv")},
		}, kvs)
	})

	t.Run("with different duplicated kv, first kv duplicated", func(t *testing.T) {
		// each KV will take 24 bytes, so we flush every 10 KVs
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		input := []struct {
			pair *kvPair
			cnt  int
		}{
			{pair: &kvPair{key: []byte("5555"), value: []byte("vvvv")}, cnt: 2},
			{pair: &kvPair{key: []byte("2222"), value: []byte("vvvv")}, cnt: 3},
			{pair: &kvPair{key: []byte("1111"), value: []byte("vvvv")}, cnt: 5},
			{pair: &kvPair{key: []byte("6666"), value: []byte("vvvv")}, cnt: 1},
			{pair: &kvPair{key: []byte("7777"), value: []byte("vvvv")}, cnt: 1},
			{pair: &kvPair{key: []byte("4444"), value: []byte("vvvv")}, cnt: 4},
			{pair: &kvPair{key: []byte("3333"), value: []byte("vvvv")}, cnt: 4},
		}
		if testingOneFile {
			sort.Slice(input, func(i, j int) bool {
				return bytes.Compare(input[i].pair.key, input[j].pair.key) < 0
			})
		}
		for _, p := range input {
			for i := 0; i < p.cnt; i++ {
				require.NoError(t, writer.WriteRow(ctx, p.pair.key, p.pair.value, nil))
			}
		}
		require.NoError(t, writer.Close(ctx))
		require.EqualValues(t, []byte("1111"), summary.Min)
		require.EqualValues(t, []byte("7777"), summary.Max)
		require.EqualValues(t, 12, summary.TotalCnt)
		require.EqualValues(t, 96, summary.TotalSize)
		require.EqualValues(t, 8, summary.ConflictInfo.Count)
		if testingOneFile {
			require.Len(t, summary.ConflictInfo.Files, 1)
			kvs := readKVFile(t, store, summary.ConflictInfo.Files[0])
			require.EqualValues(t, []kvPair{
				{key: []byte("1111"), value: []byte("vvvv")},
				{key: []byte("1111"), value: []byte("vvvv")},
				{key: []byte("1111"), value: []byte("vvvv")},
				{key: []byte("2222"), value: []byte("vvvv")},
				{key: []byte("3333"), value: []byte("vvvv")},
				{key: []byte("3333"), value: []byte("vvvv")},
				{key: []byte("4444"), value: []byte("vvvv")},
				{key: []byte("4444"), value: []byte("vvvv")},
			}, kvs)
		} else {
			require.Len(t, summary.ConflictInfo.Files, 2)
			kvs := readKVFile(t, store, summary.ConflictInfo.Files[0])
			require.EqualValues(t, []kvPair{
				{key: []byte("1111"), value: []byte("vvvv")},
				{key: []byte("1111"), value: []byte("vvvv")},
				{key: []byte("1111"), value: []byte("vvvv")},
				{key: []byte("2222"), value: []byte("vvvv")},
			}, kvs)
			kvs = readKVFile(t, store, summary.ConflictInfo.Files[1])
			require.EqualValues(t, []kvPair{
				{key: []byte("3333"), value: []byte("vvvv")},
				{key: []byte("3333"), value: []byte("vvvv")},
				{key: []byte("4444"), value: []byte("vvvv")},
				{key: []byte("4444"), value: []byte("vvvv")},
			}, kvs)
		}
	})
}

func doTestWriterOnDupRemove(t *testing.T, testingOneFile bool, getWriter func(storage.ExternalStorage, *WriterBuilder) testWriter) {
	t.Helper()
	ctx := context.Background()
	store := storage.NewMemStorage()
	var summary *WriterSummary
	doGetWriter := func(store storage.ExternalStorage, builder *WriterBuilder) testWriter {
		builder = builder.SetOnCloseFunc(func(s *WriterSummary) { summary = s }).SetOnDup(engineapi.OnDuplicateKeyRemove)
		return getWriter(store, builder)
	}

	t.Run("write nothing", func(t *testing.T) {
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		require.NoError(t, writer.Close(ctx))
		require.Empty(t, summary.Min)
		require.Empty(t, summary.Max)
		require.Zero(t, summary.TotalCnt)
		require.Zero(t, summary.TotalSize)
		require.Zero(t, summary.ConflictInfo.Count)
		require.Empty(t, summary.ConflictInfo.Files)
	})

	t.Run("all duplicated", func(t *testing.T) {
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		for range 5 {
			require.NoError(t, writer.WriteRow(ctx, []byte("1111"), []byte("vvvv"), nil))
		}
		require.NoError(t, writer.Close(ctx))
		require.EqualValues(t, dbkv.Key(nil), summary.Min)
		require.EqualValues(t, dbkv.Key(nil), summary.Max)
		require.EqualValues(t, 0, summary.TotalCnt)
		require.EqualValues(t, 0, summary.TotalSize)
		require.Empty(t, summary.MultipleFilesStats)
		require.EqualValues(t, 0, summary.ConflictInfo.Count)
		require.Empty(t, summary.ConflictInfo.Files)
	})

	t.Run("with different duplicated kv, first kv not duplicated", func(t *testing.T) {
		// each KV will take 24 bytes, so we flush every 10 KVs
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		input := []struct {
			pair *kvPair
			cnt  int
		}{
			{pair: &kvPair{key: []byte("2222"), value: []byte("vvvv")}, cnt: 1},
			{pair: &kvPair{key: []byte("1111"), value: []byte("vvvv")}, cnt: 1},
			{pair: &kvPair{key: []byte("6666"), value: []byte("vvvv")}, cnt: 3},
			{pair: &kvPair{key: []byte("7777"), value: []byte("vvvv")}, cnt: 5},
		}
		if testingOneFile {
			sort.Slice(input, func(i, j int) bool {
				return bytes.Compare(input[i].pair.key, input[j].pair.key) < 0
			})
		}
		for _, p := range input {
			for i := 0; i < p.cnt; i++ {
				require.NoError(t, writer.WriteRow(ctx, p.pair.key, p.pair.value, nil))
			}
		}
		require.NoError(t, writer.Close(ctx))
		require.EqualValues(t, []byte("1111"), summary.Min)
		require.EqualValues(t, []byte("2222"), summary.Max)
		require.EqualValues(t, 2, summary.TotalCnt)
		require.EqualValues(t, 16, summary.TotalSize)
		require.EqualValues(t, 0, summary.ConflictInfo.Count)
		require.Empty(t, summary.ConflictInfo.Files)
	})

	t.Run("with different duplicated kv, first kv duplicated", func(t *testing.T) {
		// each KV will take 24 bytes, so we flush every 10 KVs
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		input := []struct {
			pair *kvPair
			cnt  int
		}{
			{pair: &kvPair{key: []byte("5555"), value: []byte("vvvv")}, cnt: 2},
			{pair: &kvPair{key: []byte("2222"), value: []byte("vvvv")}, cnt: 3},
			{pair: &kvPair{key: []byte("1111"), value: []byte("vvvv")}, cnt: 5},
			{pair: &kvPair{key: []byte("6666"), value: []byte("vvvv")}, cnt: 1},
			{pair: &kvPair{key: []byte("7777"), value: []byte("vvvv")}, cnt: 1},
			{pair: &kvPair{key: []byte("4444"), value: []byte("vvvv")}, cnt: 4},
			{pair: &kvPair{key: []byte("3333"), value: []byte("vvvv")}, cnt: 4},
		}
		if testingOneFile {
			sort.Slice(input, func(i, j int) bool {
				return bytes.Compare(input[i].pair.key, input[j].pair.key) < 0
			})
		}
		for _, p := range input {
			for i := 0; i < p.cnt; i++ {
				require.NoError(t, writer.WriteRow(ctx, p.pair.key, p.pair.value, nil))
			}
		}
		require.NoError(t, writer.Close(ctx))
		require.EqualValues(t, []byte("6666"), summary.Min)
		require.EqualValues(t, []byte("7777"), summary.Max)
		require.EqualValues(t, 2, summary.TotalCnt)
		require.EqualValues(t, 16, summary.TotalSize)
		require.Len(t, summary.MultipleFilesStats, 1)
		require.Len(t, summary.MultipleFilesStats[0].Filenames, 1)
		require.EqualValues(t, 0, summary.ConflictInfo.Count)
		require.Empty(t, summary.ConflictInfo.Files)
	})
}
