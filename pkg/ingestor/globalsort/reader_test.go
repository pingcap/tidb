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

package globalsort

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
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
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

	var summary *simplesst.WriterSummary
	w := simplesst.NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		SetBlockSize(memSizeLimit).
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { summary = s }).
		Build(memStore, "/test", "0")

	writer := simplesst.NewEngineWriter(w)
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

	var summary *simplesst.WriterSummary
	w := simplesst.NewWriterBuilder().
		SetPropSizeDistance(100).
		SetPropKeysDistance(2).
		SetMemorySizeLimit(uint64(memSizeLimit)).
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { summary = s }).
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
	backup := simplesst.ConcurrentReaderBufferSizePerConc
	t.Cleanup(func() {
		simplesst.ConcurrentReaderBufferSizePerConc = backup
	})
	simplesst.ConcurrentReaderBufferSizePerConc = 512 * 1024

	var summary *simplesst.WriterSummary
	w := simplesst.NewWriterBuilder().
		SetPropSizeDistance(128*1024).
		SetPropKeysDistance(1000).
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { summary = s }).
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

	failpoint.Enable("github.com/pingcap/tidb/pkg/ingestor/globalsort/assertReloadAtMostOnce", "return()")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ingestor/globalsort/assertReloadAtMostOnce")

	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(simplesst.ConcurrentReaderBufferSizePerConc),
	)
	output := &memKVsAndBuffers{}
	startKey := []byte("key000000")
	maxKey := []byte("key004998")
	endKey := []byte("key004999")
	readRanges, fileVersions, err := simplesst.GetReadRangeFromPropsWithFileFormats(ctx, [][]byte{startKey, endKey}, stats, memStore)
	require.NoError(t, err)

	cachedReaders := make([]cachedReader, len(datas))
	for i := range cachedReaders {
		cachedReaders[i].setFormat(fileVersions[i])
	}
	err = readAllData(
		ctx, memStore, datas,
		cachedReaders,
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

	var summary *simplesst.WriterSummary
	w := simplesst.NewWriterBuilder().
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { summary = s }).
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
	readRanges, fileVersions, err := simplesst.GetReadRangeFromPropsWithFileFormats(ctx, jobKeys, stats, store)
	require.NoError(t, err)

	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(simplesst.ConcurrentReaderBufferSizePerConc),
	)
	cachedReaders := make([]cachedReader, len(datas))
	for i := range cachedReaders {
		cachedReaders[i].setFormat(fileVersions[i])
	}
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

func TestReadAllDataRetryClosesCachedReader(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()
	var summary *simplesst.WriterSummary
	w := simplesst.NewWriterBuilder().
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { summary = s }).
		BuildOneFile(memStore, "/retry", "0")
	w.InitPartSizeAndLogger(ctx, int64(5*size.MB))
	for i := range 32 {
		require.NoError(t, w.WriteRow(ctx, []byte(fmt.Sprintf("key%06d", i)), []byte("value")))
	}
	require.NoError(t, w.Close(ctx))
	data, stats := getKVAndStatFiles(summary)
	store := &countingOpenMemStorage{MemStorage: memStore, targetPath: data[0]}
	keys := [][]byte{[]byte("key000000"), []byte("key999999")}
	ranges, versions, err := simplesst.GetReadRangeFromPropsWithFileFormats(ctx, keys, stats, store)
	require.NoError(t, err)
	readers := make([]cachedReader, len(data))
	for i := range readers {
		readers[i].setFormat(versions[i])
	}
	t.Cleanup(func() { require.NoError(t, closeCachedReaders(readers)) })
	largePool := membuf.NewPool(membuf.WithBlockSize(simplesst.ConcurrentReaderBufferSizePerConc))
	t.Cleanup(largePool.Destroy)
	limitedPool := membuf.NewPool(
		membuf.WithBlockSize(smallBlockSize),
		membuf.WithPoolMemoryLimiter(membuf.NewLimiter(smallBlockSize/2)),
	)
	t.Cleanup(limitedPool.Destroy)
	output := &memKVsAndBuffers{}
	err = readAllData(ctx, store, data, readers, keys[0], keys[1], ranges[0].Start, ranges[1].End,
		limitedPool, largePool, output)
	require.ErrorIs(t, err, membuf.ErrCannotAcquireMemory)
	require.Nil(t, readers[0].r, "release the advanced reader before waiting and retrying the same range")

	pool := membuf.NewPool(membuf.WithBlockSize(smallBlockSize))
	t.Cleanup(pool.Destroy)
	require.NoError(t, readAllData(ctx, store, data, readers, keys[0], keys[1], ranges[0].Start,
		ranges[1].End, pool, largePool, output))
	t.Cleanup(func() {
		for _, buf := range output.memKVBuffers {
			buf.Destroy()
		}
	})
	output.build(ctx)
	require.Len(t, output.kvs, 32)
	for i, pair := range output.kvs {
		require.Equal(t, fmt.Sprintf("key%06d", i), string(pair.Key))
	}
	require.EqualValues(t, 2, store.openCount.Load())
}

func TestReadKVFilesAsync(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()

	var summary *simplesst.WriterSummary
	w := simplesst.NewWriterBuilder().
		SetBlockSize(units.KiB).
		SetMemorySizeLimit(units.KiB).
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { summary = s }).
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

// writeCompressedFilesForTest writes `kvCnt` KVs via a Writer in v1 mode and
// returns the resulting data / stat file paths plus the original expected
// KVs in write order. Used by the end-to-end v1 read-path tests below.
func writeCompressedFilesForTest(ctx context.Context, t *testing.T, memStore *objstore.MemStorage, prefix string, kvCnt int) ([]string, []string, []common.KvPair) {
	t.Helper()
	var summary *simplesst.WriterSummary
	w := simplesst.NewWriterBuilder().
		SetMemorySizeLimit(1024).
		SetBlockSize(256).
		SetPropSizeDistance(64).
		SetPropKeysDistance(8).
		SetCompression(simplesst.CompressionZstd).
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { summary = s }).
		Build(memStore, prefix, "writer-v1")
	expected := make([]common.KvPair, kvCnt)
	for i := range kvCnt {
		expected[i] = common.KvPair{
			Key: fmt.Appendf(nil, "k-%05d", i),
			Val: fmt.Appendf(nil, "v-%05d-payloadpayload", i),
		}
		require.NoError(t, w.WriteRow(ctx, expected[i].Key, expected[i].Val, nil))
	}
	require.NoError(t, w.Close(ctx))
	require.NotNil(t, summary)
	datas, stats := getKVAndStatFiles(summary)
	require.NotEmpty(t, datas)
	return datas, stats, expected
}

// TestNewMergeKVIter_V1Files asserts that simplesst.NewMergeKVIter merges v1 zstd
// files correctly end-to-end via the auto-detect dispatch. MergeSortStepMeta
// does not carry stat files, so the merge step must detect the data format.
func TestNewMergeKVIter_V1Files(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()

	datas, _, expected := writeCompressedFilesForTest(ctx, t, memStore, "/rt-merge", 200)
	require.GreaterOrEqual(t, len(datas), 2,
		"test must produce at least 2 v1 data files to exercise the merge dispatch path")

	slices.SortFunc(expected, func(a, b common.KvPair) int {
		return bytes.Compare(a.Key, b.Key)
	})

	offsets := make([]uint64, len(datas))
	iter, err := simplesst.NewMergeKVIter(ctx, datas, offsets, memStore, 1024, false, 1)
	require.NoError(t, err)
	defer func() { _ = iter.Close() }()

	got := make([]common.KvPair, 0, len(expected))
	for iter.Next() {
		got = append(got, common.KvPair{
			Key: bytes.Clone(iter.Key()),
			Val: bytes.Clone(iter.Value()),
		})
	}
	require.NoError(t, iter.Error())
	require.Equal(t, expected, got)
}

// TestNewMergeKVIter_V1RejectsNonZeroStartOffset locks in the fail-fast
// behavior when a caller accidentally passes a non-zero pathsStartOffset
// for a v1 file. The merge iterator cannot honor v0 uncompressed-byte
// offsets on a zstd stream, so silently starting from zero would produce
// duplicate or out-of-range KVs. Instead, simplesst.NewMergeKVIter surfaces the
// mistake as an error.
//
// Note: this test does NOT call iter.Close() on the error path. When
// simplesst.NewMergeKVIter returns an error, the returned *MergeKVIter wraps a
// nil internal mergeIter, and Close() would panic — that is a separate
// pre-existing quirk of the constructor not in scope for this change.
func TestNewMergeKVIter_V1RejectsNonZeroStartOffset(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()

	datas, _, _ := writeCompressedFilesForTest(ctx, t, memStore, "/rt-merge-rejectoff", 100)
	require.NotEmpty(t, datas)

	// Non-zero offset on a v1 file must be rejected at open time.
	offsets := make([]uint64, len(datas))
	offsets[0] = 128
	_, err := simplesst.NewMergeKVIter(ctx, datas, offsets, memStore, 1024, false, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-zero pathsStartOffset")
}

// TestNewMergeKVIter_MixedV0V1 asserts that simplesst.NewMergeKVIter correctly merges
// a set containing both v0 and v1 files. Rolling upgrades and partial
// backfills can plausibly leave the merge step with a mixed set.
func TestNewMergeKVIter_MixedV0V1(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()

	// v1 half: even-indexed keys.
	var v1Summary *simplesst.WriterSummary
	v1w := simplesst.NewWriterBuilder().
		SetMemorySizeLimit(1024).
		SetBlockSize(256).
		SetPropSizeDistance(64).
		SetPropKeysDistance(8).
		SetCompression(simplesst.CompressionZstd).
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { v1Summary = s }).
		Build(memStore, "/rt-mixed-v1", "writer-v1")
	// v0 half: odd-indexed keys.
	var v0Summary *simplesst.WriterSummary
	v0w := simplesst.NewWriterBuilder().
		SetMemorySizeLimit(1024).
		SetBlockSize(256).
		SetPropSizeDistance(64).
		SetPropKeysDistance(8).
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { v0Summary = s }).
		Build(memStore, "/rt-mixed-v0", "writer-v0")

	const totalKVs = 120
	expected := make([]common.KvPair, 0, totalKVs)
	for i := range totalKVs {
		kv := common.KvPair{
			Key: fmt.Appendf(nil, "k-%05d", i),
			Val: fmt.Appendf(nil, "v-%05d-payload", i),
		}
		expected = append(expected, kv)
		if i%2 == 0 {
			require.NoError(t, v1w.WriteRow(ctx, kv.Key, kv.Val, nil))
		} else {
			require.NoError(t, v0w.WriteRow(ctx, kv.Key, kv.Val, nil))
		}
	}
	require.NoError(t, v1w.Close(ctx))
	require.NoError(t, v0w.Close(ctx))

	v1Datas, _ := getKVAndStatFiles(v1Summary)
	v0Datas, _ := getKVAndStatFiles(v0Summary)
	require.NotEmpty(t, v1Datas)
	require.NotEmpty(t, v0Datas)
	datas := append(append([]string(nil), v1Datas...), v0Datas...)

	slices.SortFunc(expected, func(a, b common.KvPair) int {
		return bytes.Compare(a.Key, b.Key)
	})

	offsets := make([]uint64, len(datas))
	iter, err := simplesst.NewMergeKVIter(ctx, datas, offsets, memStore, 1024, false, 1)
	require.NoError(t, err)
	defer func() { _ = iter.Close() }()

	got := make([]common.KvPair, 0, len(expected))
	for iter.Next() {
		got = append(got, common.KvPair{
			Key: bytes.Clone(iter.Key()),
			Val: bytes.Clone(iter.Value()),
		})
	}
	require.NoError(t, iter.Error())
	require.Equal(t, expected, got)
}

// TestReadAllData_V1Files asserts that readAllData can consume v1
// zstd-compressed intermediate files in the ingest step. For v1 files,
// simplesst.GetReadRangeFromPropsWithFileFormats returns physical compressed byte offsets, and the
// cachedReader is expected to dispatch to a v1-aware ranged reader (added
// in a follow-up commit on top of this rebase).
func TestReadAllData_V1Files(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()

	datas, stats, expected := writeCompressedFilesForTest(ctx, t, memStore, "/rt-readall", 200)

	slices.SortFunc(expected, func(a, b common.KvPair) int {
		return bytes.Compare(a.Key, b.Key)
	})
	startKey := expected[0].Key
	endKey := append(bytes.Clone(expected[len(expected)-1].Key), 0) // exclusive upper bound

	readRanges, fileVersions, err := simplesst.GetReadRangeFromPropsWithFileFormats(ctx, [][]byte{startKey, endKey}, stats, memStore)
	require.NoError(t, err)

	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(simplesst.ConcurrentReaderBufferSizePerConc),
	)
	cachedReaders := make([]cachedReader, len(datas))
	for i := range cachedReaders {
		cachedReaders[i].setFormat(fileVersions[i])
	}
	defer func() {
		require.NoError(t, closeCachedReaders(cachedReaders))
	}()
	output := &memKVsAndBuffers{}
	err = readAllData(
		ctx, memStore, datas, cachedReaders,
		startKey, endKey,
		readRanges[0].Start,
		readRanges[1].End,
		smallBlockBufPool, largeBlockBufPool, output)
	require.NoError(t, err)
	output.build(ctx)

	got := make([]common.KvPair, 0, len(expected))
	for i := range output.kvs {
		got = append(got, common.KvPair{
			Key: bytes.Clone(output.kvs[i].Key),
			Val: bytes.Clone(output.kvs[i].Value),
		})
	}
	slices.SortFunc(got, func(a, b common.KvPair) int {
		return bytes.Compare(a.Key, b.Key)
	})
	require.Equal(t, expected, got)
}

// TestReadKVFilesAsync_V1Files asserts that ReadKVFilesAsync (used by
// conflict-resolution paths) can consume v1 files via the streaming v1
// reader dispatch in readOneKVFile2Ch.
func TestReadKVFilesAsync_V1Files(t *testing.T) {
	ctx := context.Background()
	memStore := objstore.NewMemStorage()

	datas, _, expected := writeCompressedFilesForTest(ctx, t, memStore, "/rt-async-v1", 150)

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	kvCh := ReadKVFilesAsync(egCtx, eg, memStore, datas)

	got := make([]common.KvPair, 0, len(expected))
	for kv := range kvCh {
		got = append(got, common.KvPair{
			Key: bytes.Clone(kv.Key),
			Val: bytes.Clone(kv.Value),
		})
	}
	require.NoError(t, eg.Wait())
	// ReadKVFilesAsync preserves write order within each file, and
	// writeCompressedFilesForTest writes in ascending key order, so the
	// expected order is already the ascending-key order.
	require.Equal(t, expected, got)
}
