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
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	dbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
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
	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		BuildOneFile(memStore, "/test", "0")

	require.NoError(t, writer.Init(ctx, 5*1024*1024))

	kvCnt := 100
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

	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}

	require.NoError(t, writer.Close(ctx))

	bufSize := rand.Intn(100) + 1
	kvReader, err := newKVReader(ctx, "/test/0/one-file", memStore, 0, bufSize)
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

	statReader, err := newStatsReader(ctx, memStore, "/test/0_stat/one-file", bufSize)
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

func TestOnefileWriterStat(t *testing.T) {
	distanceCntArr := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	kvCntArr := []int{10, 100, 200, 1000} // won't large than DefaultMemSizeLimit.
	// 1. write into one file.
	// 2. read kv file and check result.
	// 3. read stat file and check result.
	for _, kvCnt := range kvCntArr {
		for _, distance := range distanceCntArr {
			checkOneFileWriterStatWithDistance(t, kvCnt, distance, DefaultMemSizeLimit, "test"+strconv.Itoa(int(distance)))
		}
	}
}

func checkOneFileWriterStatWithDistance(t *testing.T, kvCnt int, keysDistance uint64, memSizeLimit uint64, prefix string) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(keysDistance).
		BuildOneFile(memStore, "/"+prefix, "0")

	require.NoError(t, writer.Init(ctx, 5*1024*1024))
	kvs := make([]common.KvPair, 0, kvCnt)
	for i := 0; i < kvCnt; i++ {
		kvs = append(kvs, common.KvPair{
			Key: []byte(fmt.Sprintf("key%02d", i)),
			Val: []byte("56789"),
		})
	}
	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}
	require.NoError(t, writer.Close(ctx))

	bufSize := rand.Intn(100) + 1
	kvReader, err := newKVReader(ctx, "/"+prefix+"/0/one-file", memStore, 0, bufSize)
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

	statReader, err := newStatsReader(ctx, memStore, "/"+prefix+"/0_stat/one-file", bufSize)
	require.NoError(t, err)

	var keyCnt uint64 = 0
	idx := 0
	for {
		p, err := statReader.nextProp()
		if err == io.EOF {
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
	// 1. Write to 5 files.
	// 2. merge 5 files into one file.
	// 3. read one file and check result.
	// 4. check duplicate key.
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer := NewWriterBuilder().
		SetMemorySizeLimit(1000).
		SetKeyDuplicationEncoding(true).
		Build(memStore, "/test", "0")

	kvCount := 2000000
	for i := 0; i < kvCount; i++ {
		v := i
		if v == kvCount/2 {
			v-- // insert a duplicate key.
		}
		key, val := []byte{byte(v)}, []byte{byte(v)}
		require.NoError(t, writer.WriteRow(ctx, key, val, dbkv.IntHandle(i)))
	}
	require.NoError(t, writer.Close(ctx))
	readBufSizeBak := defaultReadBufferSize
	memLimitBak := defaultOneWriterMemSizeLimit
	t.Cleanup(func() {
		defaultReadBufferSize = readBufSizeBak
		defaultOneWriterMemSizeLimit = memLimitBak
	})
	defaultReadBufferSize = 100
	defaultOneWriterMemSizeLimit = 1000
	require.NoError(t, mergeOverlappingFilesInternal(
		ctx,
		[]string{"/test/0/0", "/test/0/1", "/test/0/2", "/test/0/3", "/test/0/4"},
		memStore,
		int64(5*size.MB),
		"/test2",
		"mergeID",
		1000,
		nil,
		true,
	))

	keys := make([][]byte, 0, kvCount)
	values := make([][]byte, 0, kvCount)

	kvReader, err := newKVReader(ctx, "/test2/mergeID/one-file", memStore, 0, 100)
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

func TestOnefileWriterManyRows(t *testing.T) {
	changePropDist(t, defaultPropSizeDist, 2)
	// 1. write into one file with sorted order.
	// 2. merge one file.
	// 3. read kv file and check the result.
	// 4. check the writeSummary.
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer := NewWriterBuilder().
		SetMemorySizeLimit(1000).
		BuildOneFile(memStore, "/test", "0")

	require.NoError(t, writer.Init(ctx, 5*1024*1024))

	kvCnt := 100000
	expectedTotalSize := 0
	kvs := make([]common.KvPair, kvCnt)
	for i := 0; i < kvCnt; i++ {
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
	readBufSizeBak := defaultReadBufferSize
	memLimitBak := defaultOneWriterMemSizeLimit
	t.Cleanup(func() {
		defaultReadBufferSize = readBufSizeBak
		defaultOneWriterMemSizeLimit = memLimitBak
	})
	defaultReadBufferSize = 100
	defaultOneWriterMemSizeLimit = 1000
	require.NoError(t, mergeOverlappingFilesInternal(
		ctx,
		[]string{"/test/0/one-file"},
		memStore,
		int64(5*size.MB),
		"/test2",
		"mergeID",
		1000,
		onClose,
		true,
	))

	bufSize := rand.Intn(100) + 1
	kvReader, err := newKVReader(ctx, "/test2/mergeID/one-file", memStore, 0, bufSize)
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

	// check writerSummary.
	expected := MultipleFilesStat{
		MinKey: kvs[0].Key,
		MaxKey: kvs[len(kvs)-1].Key,
		Filenames: [][2]string{
			{"/test2/mergeID/one-file", "/test2/mergeID_stat/one-file"},
		},
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
	writer := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetBlockSize(memSizeLimit).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		BuildOneFile(memStore, "/test", "0")

	require.NoError(t, writer.Init(ctx, 5*1024*1024))

	kvCnt := 10000
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

	for _, item := range kvs {
		require.NoError(t, writer.WriteRow(ctx, item.Key, item.Val))
	}

	require.NoError(t, writer.Close(ctx))

	rd, err := newStatsReader(ctx, memStore, "/test/0_stat/one-file", 4096)
	require.NoError(t, err)
	lastOffset := uint64(0)
	for {
		prop, err := rd.nextProp()
		if err == io.EOF {
			break
		}
		require.GreaterOrEqual(t, prop.offset, lastOffset)
		lastOffset = prop.offset
	}
}
