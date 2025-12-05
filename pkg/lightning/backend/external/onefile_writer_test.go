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
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestOnefileWriterBasic(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()

	// 1. write into one file.
	// 2. read kv file and check result.
	// 3. read stat file and check result.
	var kvAndStat [2]string
	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetOnCloseFunc(func(summary *WriterSummary) { kvAndStat = summary.MultipleFilesStats[0].Filenames[0] }).
		BuildOneFile(memStore, "/test", "0")

	writer.InitPartSizeAndLogger(ctx, 5*1024*1024)

	kvCnt := 100
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

	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}

	require.NoError(t, writer.Close(ctx))

	bufSize := rand.Intn(100) + 1
	kvReader, err := NewKVReader(ctx, kvAndStat[0], memStore, 0, bufSize)
	require.NoError(t, err)
	for i := range kvCnt {
		key, value, err := kvReader.NextKV()
		require.NoError(t, err)
		require.Equal(t, kvs[i].Key, key)
		require.Equal(t, kvs[i].Val, value)
	}
	_, _, err = kvReader.NextKV()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, kvReader.Close())

	statReader, err := newStatsReader(ctx, memStore, kvAndStat[1], bufSize)
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

func TestOnefileWriterStat(t *testing.T) {
	distanceCntArr := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	kvCntArr := []int{10, 100, 200, 1000} // won't large than DefaultMemSizeLimit.
	// 1. write into one file.
	// 2. read kv file and check result.
	// 3. read stat file and check result.
	for _, kvCnt := range kvCntArr {
		for _, distance := range distanceCntArr {
			t.Run(fmt.Sprintf("kvCnt=%d, distance=%d", kvCnt, distance), func(t *testing.T) {
				checkOneFileWriterStatWithDistance(t, kvCnt, distance, DefaultMemSizeLimit, "test"+strconv.Itoa(int(distance)))
			})
		}
	}
}

func checkOneFileWriterStatWithDistance(t *testing.T, kvCnt int, keysDistance uint64, memSizeLimit uint64, prefix string) {
	var kvAndStat [2]string
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(keysDistance).
		SetOnCloseFunc(func(summary *WriterSummary) { kvAndStat = summary.MultipleFilesStats[0].Filenames[0] }).
		BuildOneFile(memStore, "/"+prefix, "0")

	writer.InitPartSizeAndLogger(ctx, 5*1024*1024)
	kvs := make([]common.KvPair, 0, kvCnt)
	for i := range kvCnt {
		kvs = append(kvs, common.KvPair{
			Key: fmt.Appendf(nil, "key%02d", i),
			Val: []byte("56789"),
		})
	}
	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}
	require.NoError(t, writer.Close(ctx))

	bufSize := rand.Intn(100) + 1
	kvReader, err := NewKVReader(ctx, kvAndStat[0], memStore, 0, bufSize)
	require.NoError(t, err)
	for i := range kvCnt {
		key, value, err := kvReader.NextKV()
		require.NoError(t, err)
		require.Equal(t, kvs[i].Key, key)
		require.Equal(t, kvs[i].Val, value)
	}
	_, _, err = kvReader.NextKV()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, kvReader.Close())

	statReader, err := newStatsReader(ctx, memStore, kvAndStat[1], bufSize)
	require.NoError(t, err)

	var keyCnt uint64 = 0
	idx := 0
	for {
		p, err := statReader.nextProp()
		if goerrors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		keyCnt += p.keys
		require.Equal(t, kvs[idx].Key, p.firstKey)
		lastIdx := idx + int(keysDistance) - 1
		if lastIdx >= len(kvs) {
			lastIdx = len(kvs) - 1
		}
		require.Equal(t, kvs[lastIdx].Key, p.lastKey)
		idx += int(keysDistance)
	}
	require.Equal(t, uint64(kvCnt), keyCnt)
	require.NoError(t, statReader.Close())
}

func TestMergeOverlappingFilesInternal(t *testing.T) {
	changePropDist(t, defaultPropSizeDist, 2)
	// 1. Write to 3 files.
	// 2. merge 3 files into one file.
	// 3. read one file and check result.
	// 4. check duplicate key.
	var kvAndStats [][2]string
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer := NewWriterBuilder().
		SetMemorySizeLimit(1000).
		SetOnCloseFunc(func(summary *WriterSummary) { kvAndStats = summary.MultipleFilesStats[0].Filenames }).
		Build(memStore, "/test", "0")

	kvCount := 2000000
	kvSize := 0
	for i := range kvCount {
		v := i
		if v == kvCount/2 {
			v-- // insert a duplicate key.
		}
		key, val := []byte{byte(v)}, []byte{byte(v)}
		kvSize += len(key) + len(val)
		require.NoError(t, writer.WriteRow(ctx, key, val, dbkv.IntHandle(i)))
	}
	require.NoError(t, writer.Close(ctx))
	readBufSizeBak := DefaultReadBufferSize
	memLimitBak := defaultOneWriterMemSizeLimit
	t.Cleanup(func() {
		DefaultReadBufferSize = readBufSizeBak
		defaultOneWriterMemSizeLimit = memLimitBak
	})
	DefaultReadBufferSize = 100
	defaultOneWriterMemSizeLimit = 1000

	collector := &execute.TestCollector{}

	dataFiles := make([]string, 0, len(kvAndStats))
	for _, f := range kvAndStats {
		dataFiles = append(dataFiles, f[0])
	}
	var onefile [2]string
	require.NoError(t, mergeOverlappingFilesInternal(
		ctx,
		dataFiles,
		memStore,
		int64(5*size.MB),
		"/test2",
		"mergeID",
		1000,
		func(summary *WriterSummary) { onefile = summary.MultipleFilesStats[0].Filenames[0] },
		collector,
		true,
		engineapi.OnDuplicateKeyIgnore,
		1,
	))

	require.EqualValues(t, kvCount, collector.Rows.Load())
	require.EqualValues(t, kvSize, collector.Bytes.Load())

	kvs := make([]KVPair, 0, kvCount)

	kvReader, err := NewKVReader(ctx, onefile[0], memStore, 0, 100)
	require.NoError(t, err)
	for range kvCount {
		key, value, err := kvReader.NextKV()
		require.NoError(t, err)
		clonedKey := make([]byte, len(key))
		copy(clonedKey, key)
		clonedVal := make([]byte, len(value))
		copy(clonedVal, value)
		kvs = append(kvs, KVPair{Key: clonedKey, Value: clonedVal})
	}
	_, _, err = kvReader.NextKV()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, kvReader.Close())

	data := &MemoryIngestData{
		kvs: kvs,
		ts:  123,
	}
	pool := membuf.NewPool()
	defer pool.Destroy()
	iter := data.NewIter(ctx, nil, nil, pool)

	for iter.First(); iter.Valid(); iter.Next() {
	}
	err = iter.Error()
	require.NoError(t, err)
}

func TestOnefileWriterManyRows(t *testing.T) {
	changePropDist(t, defaultPropSizeDist, 2)
	// 1. write into one file with sorted order.
	// 2. merge one file.
	// 3. read kv file and check the result.
	// 4. check the writeSummary.
	var kvAndStat [2]string
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer := NewWriterBuilder().
		SetMemorySizeLimit(1000).
		SetOnCloseFunc(func(summary *WriterSummary) { kvAndStat = summary.MultipleFilesStats[0].Filenames[0] }).
		BuildOneFile(memStore, "/test", "0")

	writer.InitPartSizeAndLogger(ctx, 5*1024*1024)

	kvCnt := 100000
	expectedTotalSize := 0
	kvs := make([]common.KvPair, kvCnt)
	for i := range kvCnt {
		randLen := rand.Intn(10) + 1
		kvs[i].Key = make([]byte, randLen)
		_, err := rand.Read(kvs[i].Key)
		expectedTotalSize += randLen

		require.NoError(t, err)
		randLen = rand.Intn(10) + 1
		kvs[i].Val = make([]byte, randLen)
		_, err = rand.Read(kvs[i].Val)
		require.NoError(t, err)
		expectedTotalSize += randLen
	}

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}
	require.NoError(t, writer.Close(ctx))

	var resSummary *WriterSummary
	onClose := func(summary *WriterSummary) {
		resSummary = summary
	}
	readBufSizeBak := DefaultReadBufferSize
	memLimitBak := defaultOneWriterMemSizeLimit
	t.Cleanup(func() {
		DefaultReadBufferSize = readBufSizeBak
		defaultOneWriterMemSizeLimit = memLimitBak
	})
	DefaultReadBufferSize = 100
	defaultOneWriterMemSizeLimit = 1000
	require.NoError(t, mergeOverlappingFilesInternal(
		ctx,
		[]string{kvAndStat[0]},
		memStore,
		int64(5*size.MB),
		"/test2",
		"mergeID",
		1000,
		onClose,
		nil,
		true,
		engineapi.OnDuplicateKeyIgnore,
		1,
	))

	bufSize := rand.Intn(100) + 1
	kvAndStat2 := resSummary.MultipleFilesStats[0].Filenames[0]
	kvReader, err := NewKVReader(ctx, kvAndStat2[0], memStore, 0, bufSize)
	require.NoError(t, err)
	for i := range kvCnt {
		key, value, err := kvReader.NextKV()
		require.NoError(t, err)
		require.Equal(t, kvs[i].Key, key)
		require.Equal(t, kvs[i].Val, value)
	}
	_, _, err = kvReader.NextKV()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, kvReader.Close())

	// check writerSummary.
	expected := MultipleFilesStat{
		MinKey:            kvs[0].Key,
		MaxKey:            kvs[len(kvs)-1].Key,
		Filenames:         [][2]string{kvAndStat2},
		MaxOverlappingNum: 1,
	}
	require.EqualValues(t, expected.MinKey, resSummary.Min)
	require.EqualValues(t, expected.MaxKey, resSummary.Max)
	require.Equal(t, expected.Filenames, resSummary.MultipleFilesStats[0].Filenames)
	require.Equal(t, expected.MaxOverlappingNum, resSummary.MultipleFilesStats[0].MaxOverlappingNum)
	require.EqualValues(t, expectedTotalSize, resSummary.TotalSize)
	require.EqualValues(t, kvCnt, resSummary.TotalCnt)
}

func TestOnefilePropOffset(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	memSizeLimit := (rand.Intn(10) + 1) * 200

	// 1. write into one file.
	// 2. read stat file and check offset ascending.
	var kvAndStat [2]string
	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetBlockSize(memSizeLimit).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		SetOnCloseFunc(func(summary *WriterSummary) { kvAndStat = summary.MultipleFilesStats[0].Filenames[0] }).
		BuildOneFile(memStore, "/test", "0")

	writer.InitPartSizeAndLogger(ctx, 5*1024*1024)

	kvCnt := 10000
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

	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}

	require.NoError(t, writer.Close(ctx))

	rd, err := newStatsReader(ctx, memStore, kvAndStat[1], 4096)
	require.NoError(t, err)
	lastOffset := uint64(0)
	for {
		prop, err := rd.nextProp()
		if goerrors.Is(err, io.EOF) {
			break
		}
		require.GreaterOrEqual(t, prop.offset, lastOffset)
		lastOffset = prop.offset
	}
}

type testOneFileWriter struct {
	*OneFileWriter
}

func (w *testOneFileWriter) WriteRow(ctx context.Context, key, val []byte, _ dbkv.Handle) error {
	return w.OneFileWriter.WriteRow(ctx, key, val)
}

func TestOnefileWriterOnDup(t *testing.T) {
	getWriterFn := func(store storage.ExternalStorage, b *WriterBuilder) testWriter {
		writer := b.BuildOneFile(store, "/onefile", "0")
		writer.InitPartSizeAndLogger(context.Background(), 1024)
		return &testOneFileWriter{OneFileWriter: writer}
	}
	doTestWriterOnDupRecord(t, true, getWriterFn)
	doTestWriterOnDupRemove(t, true, getWriterFn)
}

func TestOnefileWriterDupError(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()

	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetOnDup(engineapi.OnDuplicateKeyError).
		BuildOneFile(memStore, "/test", "0")

	writer.InitPartSizeAndLogger(ctx, 5*1024*1024)

	kvCnt := 10
	kvs := make([]common.KvPair, kvCnt)
	for i := range kvCnt {
		kvs[i].Key = []byte(strconv.Itoa(i))
		kvs[i].Val = []byte(strconv.Itoa(i * i))
	}

	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}
	// write duplicate key
	err := writer.WriteRow(ctx, kvs[kvCnt-1].Key, kvs[kvCnt-1].Val)
	require.Error(t, err)
	require.True(t, common.ErrFoundDuplicateKeys.Equal(err))
}

func TestOneFileWriterOnDupRemove(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	store := storage.NewMemStorage()
	var summary *WriterSummary
	doGetWriter := func(store storage.ExternalStorage, builder *WriterBuilder) *OneFileWriter {
		builder = builder.SetOnCloseFunc(func(s *WriterSummary) { summary = s }).SetOnDup(engineapi.OnDuplicateKeyRemove)
		writer := builder.BuildOneFile(store, "/onefile", "0")
		writer.InitPartSizeAndLogger(ctx, 1024)
		return writer
	}

	t.Run("all duplicated", func(t *testing.T) {
		builder := NewWriterBuilder().SetPropKeysDistance(4).SetMemorySizeLimit(240).SetBlockSize(240)
		writer := doGetWriter(store, builder)
		for i := 0; i < 5; i++ {
			require.NoError(t, writer.WriteRow(ctx, []byte("1111"), []byte("vvvv")))
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
			pair *KVPair
			cnt  int
		}{
			{pair: &KVPair{Key: []byte("1111"), Value: []byte("vvvv")}, cnt: 1},
			{pair: &KVPair{Key: []byte("2222"), Value: []byte("vvvv")}, cnt: 1},
			{pair: &KVPair{Key: []byte("6666"), Value: []byte("vvvv")}, cnt: 3},
			{pair: &KVPair{Key: []byte("7777"), Value: []byte("vvvv")}, cnt: 5},
		}
		for _, p := range input {
			for i := 0; i < p.cnt; i++ {
				require.NoError(t, writer.WriteRow(ctx, p.pair.Key, p.pair.Value))
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
			pair *KVPair
			cnt  int
		}{
			{pair: &KVPair{Key: []byte("1111"), Value: []byte("vvvv")}, cnt: 5},
			{pair: &KVPair{Key: []byte("2222"), Value: []byte("vvvv")}, cnt: 3},
			{pair: &KVPair{Key: []byte("3333"), Value: []byte("vvvv")}, cnt: 4},
			{pair: &KVPair{Key: []byte("4444"), Value: []byte("vvvv")}, cnt: 4},
			{pair: &KVPair{Key: []byte("5555"), Value: []byte("vvvv")}, cnt: 2},
			{pair: &KVPair{Key: []byte("6666"), Value: []byte("vvvv")}, cnt: 1},
			{pair: &KVPair{Key: []byte("7777"), Value: []byte("vvvv")}, cnt: 1},
		}
		for _, p := range input {
			for i := 0; i < p.cnt; i++ {
				require.NoError(t, writer.WriteRow(ctx, p.pair.Key, p.pair.Value))
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
