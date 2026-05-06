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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

type countingOpenMemStorage struct {
	*objstore.MemStorage
	targetPath string
	openCount  atomic.Int32
}

func (s *countingOpenMemStorage) Open(
	ctx context.Context,
	path string,
	o *storeapi.ReaderOption,
) (objectio.Reader, error) {
	if path == s.targetPath {
		s.openCount.Add(1)
	}
	return s.MemStorage.Open(ctx, path, o)
}

func TestReadAllDataBasic(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := objstore.NewMemStorage()
	memSizeLimit := (rand.Intn(10) + 1) * 400

	var summary *WriterSummary
	w := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		SetBlockSize(memSizeLimit).
		SetOnCloseFunc(func(s *WriterSummary) { summary = s }).
		Build(memStore, "/test", "0")

	writer := NewEngineWriter(w)
	kvCnt := rand.Intn(10) + 10000
	kvs := make([]common.KvPair, kvCnt)
	for i := range kvCnt {
		kvs[i] = common.KvPair{
			Key: fmt.Appendf(nil, "key%05d", i),
			Val: []byte("56789"),
		}
	}

	require.NoError(t, writer.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs(kvs)))
	_, err := writer.Close(ctx)
	require.NoError(t, err)

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	datas, stats := getKVAndStatFiles(summary)

	testReadAndCompare(ctx, t, kvs, memStore, datas, stats, kvs[0].Key, memSizeLimit)
}

func TestReadAllOneFile(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := objstore.NewMemStorage()
	memSizeLimit := (rand.Intn(10) + 1) * 400

	var summary *WriterSummary
	w := NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		SetOnCloseFunc(func(s *WriterSummary) { summary = s }).
		BuildOneFile(memStore, "/test", "0")

	w.InitPartSizeAndLogger(ctx, int64(5*size.MB))

	kvCnt := rand.Intn(10) + 10000
	kvs := make([]common.KvPair, kvCnt)
	for i := range kvCnt {
		kvs[i] = common.KvPair{
			Key: fmt.Appendf(nil, "key%05d", i),
			Val: []byte("56789"),
		}
		require.NoError(t, w.WriteRow(ctx, kvs[i].Key, kvs[i].Val))
	}

	require.NoError(t, w.Close(ctx))

	slices.SortFunc(kvs, func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	datas, stats := getKVAndStatFiles(summary)

	testReadAndCompare(ctx, t, kvs, memStore, datas, stats, kvs[0].Key, memSizeLimit)
}

func TestReadLargeFile(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()
	backup := ConcurrentReaderBufferSizePerConc
	t.Cleanup(func() {
		ConcurrentReaderBufferSizePerConc = backup
	})
	ConcurrentReaderBufferSizePerConc = 512 * 1024

	var summary *WriterSummary
	w := NewWriterBuilder().
		SetPropSizeDistance(128*1024).
		SetPropKeysDistance(1000).
		SetOnCloseFunc(func(s *WriterSummary) { summary = s }).
		BuildOneFile(memStore, "/test", "0")

	w.InitPartSizeAndLogger(ctx, int64(5*size.MB))

	val := make([]byte, 10000)
	for i := range 10000 {
		key := fmt.Appendf(nil, "key%06d", i)
		require.NoError(t, w.WriteRow(ctx, key, val))
	}
	require.NoError(t, w.Close(ctx))

	datas, stats := getKVAndStatFiles(summary)
	require.Len(t, datas, 1)

	failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/external/assertReloadAtMostOnce", "return()")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/external/assertReloadAtMostOnce")

	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc),
	)
	output := &memKVsAndBuffers{}
	startKey := []byte("key000000")
	maxKey := []byte("key004998")
	endKey := []byte("key004999")
	readRanges, err := getReadRangeFromProps(ctx, [][]byte{startKey, endKey}, stats, memStore)
	require.NoError(t, err)

	err = readAllData(
		ctx, memStore, datas,
		make([]cachedReader, len(datas)),
		startKey, endKey,
		readRanges[0].Start,
		readRanges[1].End,
		smallBlockBufPool, largeBlockBufPool, output)
	require.NoError(t, err)
	output.build(ctx)
	require.Equal(t, startKey, output.kvs[0].Key)
	require.Equal(t, maxKey, output.kvs[len(output.kvs)-1].Key)
}

func TestReadAllDataReuseSequentialReaderAcrossBatches(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()

	var summary *WriterSummary
	w := NewWriterBuilder().
		SetOnCloseFunc(func(s *WriterSummary) { summary = s }).
		BuildOneFile(memStore, "/test", "0")
	w.InitPartSizeAndLogger(ctx, int64(5*size.MB))

	for i := range 5000 {
		key := []byte(fmt.Sprintf("key%06d", i))
		require.NoError(t, w.WriteRow(ctx, key, []byte("value")))
	}
	require.NoError(t, w.Close(ctx))

	datas, stats := getKVAndStatFiles(summary)
	require.Len(t, datas, 1)

	store := &countingOpenMemStorage{
		MemStorage: memStore,
		targetPath: datas[0],
	}
	jobKeys := [][]byte{
		[]byte("key000000"),
		[]byte("key002500"),
		[]byte("key004999"),
	}
	readRanges, err := getReadRangeFromProps(ctx, jobKeys, stats, store)
	require.NoError(t, err)

	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc),
	)
	cachedReaders := make([]cachedReader, len(datas))
	defer func() {
		require.NoError(t, closeCachedReaders(cachedReaders))
	}()

	firstOutput := &memKVsAndBuffers{}
	err = readAllData(
		ctx, store, datas, cachedReaders,
		jobKeys[0], jobKeys[1],
		readRanges[0].Start, readRanges[1].End,
		smallBlockBufPool, largeBlockBufPool, firstOutput,
	)
	require.NoError(t, err)

	secondOutput := &memKVsAndBuffers{}
	err = readAllData(
		ctx, store, datas, cachedReaders,
		jobKeys[1], jobKeys[2],
		readRanges[1].Start, readRanges[2].End,
		smallBlockBufPool, largeBlockBufPool, secondOutput,
	)
	require.NoError(t, err)

	firstOutput.build(ctx)
	secondOutput.build(ctx)
	require.EqualValues(t, 1, store.openCount.Load())
	require.Len(t, firstOutput.kvs, 2500)
	require.Len(t, secondOutput.kvs, 2499)
	require.Equal(t, []byte("key000000"), firstOutput.kvs[0].Key)
	require.Equal(t, []byte("key002499"), firstOutput.kvs[len(firstOutput.kvs)-1].Key)
	require.Equal(t, []byte("key002500"), secondOutput.kvs[0].Key)
	require.Equal(t, []byte("key004998"), secondOutput.kvs[len(secondOutput.kvs)-1].Key)
}

func TestReadKVFilesAsync(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()

	var summary *WriterSummary
	w := NewWriterBuilder().
		SetBlockSize(units.KiB).
		SetMemorySizeLimit(units.KiB).
		SetOnCloseFunc(func(s *WriterSummary) { summary = s }).
		Build(memStore, "/test", "0")

	const kvCnt = 100
	expectedKVs := make([]common.KvPair, kvCnt)
	for i := range kvCnt {
		expectedKVs[i] = common.KvPair{
			Key: fmt.Appendf(nil, "key%05d", i),
			Val: []byte(fmt.Sprintf("val%05d", i)),
		}
		require.NoError(t, w.WriteRow(ctx, expectedKVs[i].Key, expectedKVs[i].Val, nil))
	}

	require.NoError(t, w.Close(ctx))

	datas, _ := getKVAndStatFiles(summary)
	require.Len(t, datas, 4)

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	kvCh := ReadKVFilesAsync(egCtx, eg, memStore, datas)

	readKVs := make([]common.KvPair, 0, kvCnt)
	for kv := range kvCh {
		readKVs = append(readKVs, common.KvPair{Key: kv.Key, Val: kv.Value})
	}

	err := eg.Wait()
	require.NoError(t, err)

	require.Equal(t, expectedKVs, readKVs)
}
