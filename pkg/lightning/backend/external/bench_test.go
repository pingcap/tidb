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
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

var testingStorageURI = flag.String("testing-storage-uri", "", "the URI of the storage used for testing")

type writeTestSuite struct {
	store              storage.ExternalStorage
	source             kvSource
	memoryLimit        int
	beforeCreateWriter func()
	afterWriterClose   func()

	optionalFilePath string
	onClose          OnCloseFunc
}

func writePlainFile(s *writeTestSuite) {
	ctx := context.Background()
	filePath := "/test/writer"
	if s.optionalFilePath != "" {
		filePath = s.optionalFilePath
	}
	_ = s.store.DeleteFile(ctx, filePath)
	buf := make([]byte, s.memoryLimit)
	offset := 0
	flush := func(w storage.ExternalFileWriter) {
		n, err := w.Write(ctx, buf[:offset])
		intest.AssertNoError(err)
		intest.Assert(offset == n)
		offset = 0
	}

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer, err := s.store.Create(ctx, filePath, nil)
	intest.AssertNoError(err)
	key, val, _ := s.source.next()
	for key != nil {
		if offset+len(key)+len(val) > len(buf) {
			flush(writer)
		}
		offset += copy(buf[offset:], key)
		offset += copy(buf[offset:], val)
		key, val, _ = s.source.next()
	}
	flush(writer)
	err = writer.Close(ctx)
	intest.AssertNoError(err)
	if s.afterWriterClose != nil {
		s.afterWriterClose()
	}
}

func cleanOldFiles(ctx context.Context, store storage.ExternalStorage, subDir string) {
	dataFiles, statFiles, err := GetAllFileNames(ctx, store, subDir)
	intest.AssertNoError(err)
	err = store.DeleteFiles(ctx, dataFiles)
	intest.AssertNoError(err)
	err = store.DeleteFiles(ctx, statFiles)
	intest.AssertNoError(err)
}

func writeExternalFile(s *writeTestSuite) {
	ctx := context.Background()
	filePath := "/test/writer"
	if s.optionalFilePath != "" {
		filePath = s.optionalFilePath
	}
	cleanOldFiles(ctx, s.store, filePath)
	builder := NewWriterBuilder().
		SetMemorySizeLimit(uint64(s.memoryLimit)).
		SetOnCloseFunc(s.onClose)

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer := builder.Build(s.store, filePath, "writerID")
	key, val, h := s.source.next()
	for key != nil {
		err := writer.WriteRow(ctx, key, val, h)
		intest.AssertNoError(err)
		key, val, h = s.source.next()
	}
	err := writer.Close(ctx)
	intest.AssertNoError(err)
	if s.afterWriterClose != nil {
		s.afterWriterClose()
	}
}

func writeExternalOneFile(s *writeTestSuite) {
	ctx := context.Background()
	filePath := "/test/writer"
	if s.optionalFilePath != "" {
		filePath = s.optionalFilePath
	}
	cleanOldFiles(ctx, s.store, filePath)
	builder := NewWriterBuilder().
		SetMemorySizeLimit(uint64(s.memoryLimit))

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer := builder.BuildOneFile(
		s.store, filePath, "writerID")
	intest.AssertNoError(writer.Init(ctx, 20*1024*1024))
	var minKey, maxKey []byte

	key, val, _ := s.source.next()
	minKey = key
	for key != nil {
		maxKey = key
		err := writer.WriteRow(ctx, key, val)
		intest.AssertNoError(err)
		key, val, _ = s.source.next()
	}
	intest.AssertNoError(writer.Close(ctx))
	s.onClose(&WriterSummary{
		Min: minKey,
		Max: maxKey,
	})
	if s.afterWriterClose != nil {
		s.afterWriterClose()
	}
}

// TestCompareWriter should be run like
// go test ./pkg/lightning/backend/external -v -timeout=1h --tags=intest -test.run TestCompareWriter --testing-storage-uri="s3://xxx".
func TestCompareWriter(t *testing.T) {
	externalStore := openTestingStorage(t)
	expectedKVSize := 2 * 1024 * 1024 * 1024
	memoryLimit := 256 * 1024 * 1024
	testIdx := 0
	seed := time.Now().Nanosecond()
	t.Logf("random seed: %d", seed)
	var (
		now     time.Time
		elapsed time.Duration

		p = newProfiler(true, true)
	)
	beforeTest := func() {
		testIdx++
		p.beforeTest()

		now = time.Now()
	}
	afterClose := func() {
		elapsed = time.Since(now)
		p.afterTest()
	}

	suite := &writeTestSuite{
		memoryLimit:        memoryLimit,
		beforeCreateWriter: beforeTest,
		afterWriterClose:   afterClose,
	}

	stores := map[string]storage.ExternalStorage{
		"external store": externalStore,
		"memory store":   storage.NewMemStorage(),
	}
	writerTestFn := map[string]func(*writeTestSuite){
		"plain file":        writePlainFile,
		"external file":     writeExternalFile,
		"external one file": writeExternalOneFile,
	}

	// not much difference between size 3 & 10
	keyCommonPrefix := []byte{1, 2, 3}

	for _, kvSize := range [][2]int{{20, 1000}, {20, 100}, {20, 10}} {
		expectedKVNum := expectedKVSize / (kvSize[0] + kvSize[1])
		sources := map[string]func() kvSource{}
		sources["ascending key"] = func() kvSource {
			return newAscendingKeySource(expectedKVNum, kvSize[0], kvSize[1], keyCommonPrefix)
		}
		sources["random key"] = func() kvSource {
			return newRandomKeySource(expectedKVNum, kvSize[0], kvSize[1], keyCommonPrefix, seed)
		}
		for sourceName, sourceGetter := range sources {
			for storeName, store := range stores {
				for writerName, fn := range writerTestFn {
					if writerName == "plain file" && storeName == "external store" {
						// about 27MB/s
						continue
					}
					suite.store = store
					source := sourceGetter()
					suite.source = source
					t.Logf("test %d: %s, %s, %s, key size: %d, value size: %d",
						testIdx+1, sourceName, storeName, writerName, kvSize[0], kvSize[1])
					fn(suite)
					speed := float64(source.outputSize()) / elapsed.Seconds() / 1024 / 1024
					t.Logf("test %d: speed for %d bytes: %.2f MB/s", testIdx, source.outputSize(), speed)
					suite.source = nil
				}
			}
		}
	}
}

type readTestSuite struct {
	store              storage.ExternalStorage
	subDir             string
	totalKVCnt         int
	concurrency        int
	memoryLimit        int
	mergeIterHotspot   bool
	beforeCreateReader func()
	afterReaderClose   func()
}

func readFileSequential(t *testing.T, s *readTestSuite) {
	ctx := context.Background()
	files, _, err := GetAllFileNames(ctx, s.store, "/"+s.subDir)
	intest.AssertNoError(err)

	buf := make([]byte, s.memoryLimit)
	if s.beforeCreateReader != nil {
		s.beforeCreateReader()
	}
	var totalFileSize atomic.Int64
	startTime := time.Now()
	for _, file := range files {
		reader, err := s.store.Open(ctx, file, nil)
		intest.AssertNoError(err)
		var sz int
		for {
			n, err := reader.Read(buf)
			sz += n
			if err != nil {
				break
			}
		}
		intest.Assert(err == io.EOF)
		totalFileSize.Add(int64(sz))
		err = reader.Close()
		intest.AssertNoError(err)
	}
	if s.afterReaderClose != nil {
		s.afterReaderClose()
	}
	t.Logf(
		"sequential read speed for %s bytes(%d files): %s/s",
		units.BytesSize(float64(totalFileSize.Load())),
		len(files),
		units.BytesSize(float64(totalFileSize.Load())/time.Since(startTime).Seconds()),
	)
}

func readFileConcurrently(t *testing.T, s *readTestSuite) {
	ctx := context.Background()
	files, _, err := GetAllFileNames(ctx, s.store, "/"+s.subDir)
	intest.AssertNoError(err)

	conc := min(s.concurrency, len(files))
	var eg errgroup.Group
	eg.SetLimit(conc)

	if s.beforeCreateReader != nil {
		s.beforeCreateReader()
	}
	var totalFileSize atomic.Int64
	startTime := time.Now()
	for i := range files {
		file := files[i]
		eg.Go(func() error {
			buf := make([]byte, s.memoryLimit/conc)
			reader, err := s.store.Open(ctx, file, nil)
			intest.AssertNoError(err)
			var sz int
			for {
				n, err := reader.Read(buf)
				sz += n
				if err != nil {
					break
				}
			}
			intest.Assert(err == io.EOF)
			totalFileSize.Add(int64(sz))
			err = reader.Close()
			intest.AssertNoError(err)
			return nil
		})
	}
	err = eg.Wait()
	intest.AssertNoError(err)
	if s.afterReaderClose != nil {
		s.afterReaderClose()
	}
	totalDur := time.Since(startTime)
	t.Logf(
		"concurrent read speed for %s bytes(%d files): %s/s, total-dur=%s",
		units.BytesSize(float64(totalFileSize.Load())),
		len(files),
		units.BytesSize(float64(totalFileSize.Load())/totalDur.Seconds()), totalDur,
	)
}

func readMergeIter(t *testing.T, s *readTestSuite) {
	ctx := context.Background()
	files, _, err := GetAllFileNames(ctx, s.store, "/"+s.subDir)
	intest.AssertNoError(err)

	if s.beforeCreateReader != nil {
		s.beforeCreateReader()
	}

	startTime := time.Now()
	var totalSize int
	readBufSize := s.memoryLimit / len(files)
	zeroOffsets := make([]uint64, len(files))
	iter, err := NewMergeKVIter(ctx, files, zeroOffsets, s.store, readBufSize, s.mergeIterHotspot, 0)
	intest.AssertNoError(err)

	kvCnt := 0
	for iter.Next() {
		kvCnt++
		totalSize += len(iter.Key()) + len(iter.Value()) + lengthBytes*2
	}
	intest.Assert(kvCnt == s.totalKVCnt)
	err = iter.Close()
	intest.AssertNoError(err)
	if s.afterReaderClose != nil {
		s.afterReaderClose()
	}
	t.Logf(
		"merge iter read (hotspot=%t) speed for %s bytes: %s/s",
		s.mergeIterHotspot,
		units.BytesSize(float64(totalSize)),
		units.BytesSize(float64(totalSize)/time.Since(startTime).Seconds()),
	)
}

func TestCompareReaderEvenlyDistributedContent(t *testing.T) {
	fileSize := 50 * 1024 * 1024
	fileCnt := 24
	subDir := "evenly_distributed"
	store := openTestingStorage(t)

	kvCnt, _, _ := createEvenlyDistributedFiles(store, fileSize, fileCnt, subDir)
	memoryLimit := 64 * 1024 * 1024

	var (
		elapsed time.Duration

		p = newProfiler(true, true)
	)

	suite := &readTestSuite{
		store:              store,
		totalKVCnt:         kvCnt,
		concurrency:        100,
		memoryLimit:        memoryLimit,
		beforeCreateReader: p.beforeTest,
		afterReaderClose:   p.afterTest,
		subDir:             subDir,
	}

	readFileSequential(t, suite)
	t.Logf(
		"sequential read speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)

	readFileConcurrently(t, suite)
	t.Logf(
		"concurrent read speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)

	readMergeIter(t, suite)
	t.Logf(
		"merge iter read speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)
}

var (
	objectPrefix = flag.String("object-prefix", "ascending", "object prefix")
	fileSize     = flag.Int("file-size", 50*units.MiB, "file size")
	fileCount    = flag.Int("file-count", 24, "file count")
	concurrency  = flag.Int("concurrency", 100, "concurrency")
	memoryLimit  = flag.Int("memory-limit", 64*units.MiB, "memory limit")
	skipCreate   = flag.Bool("skip-create", false, "skip create files")
)

func TestReadFileConcurrently(t *testing.T) {
	testCompareReaderWithContent(t, createAscendingFiles, readFileConcurrently)
}

func TestReadFileSequential(t *testing.T) {
	testCompareReaderWithContent(t, createAscendingFiles, readFileSequential)
}

func TestReadMergeIterCheckHotspot(t *testing.T) {
	testCompareReaderWithContent(t, createAscendingFiles, func(t *testing.T, suite *readTestSuite) {
		suite.mergeIterHotspot = true
		readMergeIter(t, suite)
	})
}

func TestReadMergeIterWithoutCheckHotspot(t *testing.T) {
	testCompareReaderWithContent(t, createAscendingFiles, readMergeIter)
}

func testCompareReaderWithContent(
	t *testing.T,
	createFn func(store storage.ExternalStorage, fileSize int, fileCount int, objectPrefix string) (int, kv.Key, kv.Key),
	fn func(t *testing.T, suite *readTestSuite),
) {
	store := openTestingStorage(t)
	kvCnt := 0
	if !*skipCreate {
		kvCnt, _, _ = createFn(store, *fileSize, *fileCount, *objectPrefix)
	}
	p := newProfiler(true, true)

	suite := &readTestSuite{
		store:              store,
		totalKVCnt:         kvCnt,
		concurrency:        *concurrency,
		memoryLimit:        *memoryLimit,
		beforeCreateReader: p.beforeTest,
		afterReaderClose:   p.afterTest,
		subDir:             *objectPrefix,
	}

	fn(t, suite)
}

type mergeTestSuite struct {
	store            storage.ExternalStorage
	subDir           string
	totalKVCnt       int
	concurrency      int
	memoryLimit      int
	mergeIterHotspot bool
	minKey           kv.Key
	maxKey           kv.Key
	beforeMerge      func()
	afterMerge       func()
}

func mergeStep(t *testing.T, s *mergeTestSuite) {
	ctx := context.Background()
	datas, _, err := GetAllFileNames(ctx, s.store, "/"+s.subDir)
	intest.AssertNoError(err)

	mergeOutput := "merge_output"
	totalSize := atomic.NewUint64(0)
	onClose := func(s *WriterSummary) {
		totalSize.Add(s.TotalSize)
	}
	if s.beforeMerge != nil {
		s.beforeMerge()
	}

	now := time.Now()
	err = MergeOverlappingFiles(
		ctx,
		datas,
		s.store,
		int64(5*size.MB),
		mergeOutput,
		DefaultBlockSize,
		onClose,
		s.concurrency,
		s.mergeIterHotspot,
	)

	intest.AssertNoError(err)
	if s.afterMerge != nil {
		s.afterMerge()
	}
	elapsed := time.Since(now)
	t.Logf(
		"merge speed for %d bytes in %s with %d concurrency, speed: %.2f MB/s",
		totalSize.Load(),
		elapsed,
		s.concurrency,
		float64(totalSize.Load())/elapsed.Seconds()/1024/1024,
	)
}

func newMergeStep(t *testing.T, s *mergeTestSuite) {
	ctx := context.Background()
	datas, stats, err := GetAllFileNames(ctx, s.store, "/"+s.subDir)
	intest.AssertNoError(err)

	mergeOutput := "merge_output"
	totalSize := atomic.NewUint64(0)
	onClose := func(s *WriterSummary) {
		totalSize.Add(s.TotalSize)
	}
	if s.beforeMerge != nil {
		s.beforeMerge()
	}

	now := time.Now()
	err = MergeOverlappingFilesV2(
		ctx,
		mockOneMultiFileStat(datas, stats),
		s.store,
		s.minKey,
		s.maxKey.Next(),
		int64(5*size.MB),
		mergeOutput,
		"test",
		DefaultBlockSize,
		8*1024,
		1*size.MB,
		8*1024,
		onClose,
		s.concurrency,
		s.mergeIterHotspot,
	)

	intest.AssertNoError(err)
	if s.afterMerge != nil {
		s.afterMerge()
	}
	elapsed := time.Since(now)
	t.Logf(
		"new merge speed for %d bytes in %s, speed: %.2f MB/s",
		totalSize.Load(),
		elapsed,
		float64(totalSize.Load())/elapsed.Seconds()/1024/1024,
	)
}

func testCompareMergeWithContent(
	t *testing.T,
	concurrency int,
	createFn func(store storage.ExternalStorage, fileSize int, fileCount int, objectPrefix string) (int, kv.Key, kv.Key),
	fn func(t *testing.T, suite *mergeTestSuite),
	p *profiler,
) {
	store := openTestingStorage(t)
	kvCnt := 0
	var minKey, maxKey kv.Key
	if !*skipCreate {
		kvCnt, minKey, maxKey = createFn(store, *fileSize, *fileCount, *objectPrefix)
	}

	suite := &mergeTestSuite{
		store:            store,
		totalKVCnt:       kvCnt,
		concurrency:      concurrency,
		memoryLimit:      *memoryLimit,
		beforeMerge:      p.beforeTest,
		afterMerge:       p.afterTest,
		subDir:           *objectPrefix,
		minKey:           minKey,
		maxKey:           maxKey,
		mergeIterHotspot: true,
	}

	fn(t, suite)
}

func TestMergeBench(t *testing.T) {
	p := newProfiler(true, true)
	testCompareMergeWithContent(t, 1, createAscendingFiles, mergeStep, p)
	testCompareMergeWithContent(t, 1, createEvenlyDistributedFiles, mergeStep, p)
	testCompareMergeWithContent(t, 2, createAscendingFiles, mergeStep, p)
	testCompareMergeWithContent(t, 2, createEvenlyDistributedFiles, mergeStep, p)
	testCompareMergeWithContent(t, 4, createAscendingFiles, mergeStep, p)
	testCompareMergeWithContent(t, 4, createEvenlyDistributedFiles, mergeStep, p)
	testCompareMergeWithContent(t, 8, createAscendingFiles, mergeStep, p)
	testCompareMergeWithContent(t, 8, createEvenlyDistributedFiles, mergeStep, p)
	testCompareMergeWithContent(t, 8, createAscendingFiles, newMergeStep, p)
	testCompareMergeWithContent(t, 8, createEvenlyDistributedFiles, newMergeStep, p)
}

func TestReadAllDataLargeFiles(t *testing.T) {
	ctx := context.Background()
	store := openTestingStorage(t)

	// ~ 100B * 20M = 2GB
	source := newAscendingKeyAsyncSource(20*1024*1024, 10, 90, nil)
	// ~ 1KB * 2M = 2GB
	source2 := newAscendingKeyAsyncSource(2*1024*1024, 10, 990, nil)
	var minKey, maxKey kv.Key
	recordMinMax := func(s *WriterSummary) {
		minKey = s.Min
		maxKey = s.Max
	}
	suite := &writeTestSuite{
		store:            store,
		source:           source,
		memoryLimit:      256 * 1024 * 1024,
		optionalFilePath: "/test/file",
		onClose:          recordMinMax,
	}
	suite2 := &writeTestSuite{
		store:            store,
		source:           source2,
		memoryLimit:      256 * 1024 * 1024,
		optionalFilePath: "/test/file2",
		onClose:          recordMinMax,
	}
	writeExternalOneFile(suite)
	t.Logf("minKey: %s, maxKey: %s", minKey, maxKey)
	writeExternalOneFile(suite2)
	t.Logf("minKey: %s, maxKey: %s", minKey, maxKey)

	dataFiles, statFiles, err := GetAllFileNames(ctx, store, "")
	intest.AssertNoError(err)
	intest.Assert(len(dataFiles) == 2)

	// choose the two keys so that expected concurrency is 579 and 19
	startKey, err := hex.DecodeString("00000001000000000000")
	intest.AssertNoError(err)
	endKey, err := hex.DecodeString("00a00000000000000000")
	intest.AssertNoError(err)
	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc),
	)
	output := &memKVsAndBuffers{}
	now := time.Now()

	err = readAllData(ctx, store, dataFiles, statFiles, startKey, endKey, smallBlockBufPool, largeBlockBufPool, output)
	t.Logf("read all data cost: %s", time.Since(now))
	intest.AssertNoError(err)
}

func TestReadAllData(t *testing.T) {
	// test the case that thread=16, where we will load ~3.2GB data once and this
	// step will at most have 4000 files to read, test the case that we have
	//
	// 1000 files read one KV (~100B), 1000 files read ~900KB, 90 files read 10MB,
	// 1 file read 1G. total read size = 1000*100B + 1000*900KB + 90*10MB + 1*1G = 2.8G

	ctx := context.Background()
	store := openTestingStorage(t)
	readRangeStart := []byte("key00")
	readRangeEnd := []byte("key88888888")
	keyAfterRange := []byte("key9")
	keyAfterRange2 := []byte("key9")
	eg := errgroup.Group{}

	fileIdx := 0
	val := make([]byte, 90)
	if *skipCreate {
		goto finishCreateFiles
	}

	cleanOldFiles(ctx, store, "/")

	for ; fileIdx < 1000; fileIdx++ {
		fileIdx := fileIdx
		eg.Go(func() error {
			fileName := fmt.Sprintf("/test%d", fileIdx)
			writer := NewWriterBuilder().BuildOneFile(store, fileName, "writerID")
			err := writer.Init(ctx, 5*1024*1024)
			require.NoError(t, err)
			key := []byte(fmt.Sprintf("key0%d", fileIdx))
			err = writer.WriteRow(ctx, key, val)
			require.NoError(t, err)

			// write some extra data that is greater than readRangeEnd
			err = writer.WriteRow(ctx, keyAfterRange, val)
			require.NoError(t, err)
			err = writer.WriteRow(ctx, keyAfterRange2, make([]byte, 100*1024))
			require.NoError(t, err)

			return writer.Close(ctx)
		})
	}
	require.NoError(t, eg.Wait())
	t.Log("finish writing 1000 files of 100B")

	for ; fileIdx < 2000; fileIdx++ {
		fileIdx := fileIdx
		eg.Go(func() error {
			fileName := fmt.Sprintf("/test%d", fileIdx)
			writer := NewWriterBuilder().BuildOneFile(store, fileName, "writerID")
			err := writer.Init(ctx, 5*1024*1024)
			require.NoError(t, err)

			kvSize := 0
			keyIdx := 0
			for kvSize < 900*1024 {
				key := []byte(fmt.Sprintf("key%06d_%d", keyIdx, fileIdx))
				keyIdx++
				kvSize += len(key) + len(val)
				err = writer.WriteRow(ctx, key, val)
				require.NoError(t, err)
			}

			// write some extra data that is greater than readRangeEnd
			err = writer.WriteRow(ctx, keyAfterRange, val)
			require.NoError(t, err)
			err = writer.WriteRow(ctx, keyAfterRange2, make([]byte, 300*1024))
			require.NoError(t, err)
			return writer.Close(ctx)
		})
	}
	require.NoError(t, eg.Wait())
	t.Log("finish writing 1000 files of 900KB")

	for ; fileIdx < 2090; fileIdx++ {
		fileIdx := fileIdx
		eg.Go(func() error {
			fileName := fmt.Sprintf("/test%d", fileIdx)
			writer := NewWriterBuilder().BuildOneFile(store, fileName, "writerID")
			err := writer.Init(ctx, 5*1024*1024)
			require.NoError(t, err)

			kvSize := 0
			keyIdx := 0
			for kvSize < 10*1024*1024 {
				key := []byte(fmt.Sprintf("key%09d_%d", keyIdx, fileIdx))
				keyIdx++
				kvSize += len(key) + len(val)
				err = writer.WriteRow(ctx, key, val)
				require.NoError(t, err)
			}

			// write some extra data that is greater than readRangeEnd
			err = writer.WriteRow(ctx, keyAfterRange, val)
			require.NoError(t, err)
			err = writer.WriteRow(ctx, keyAfterRange2, make([]byte, 900*1024))
			require.NoError(t, err)
			return writer.Close(ctx)
		})
	}
	require.NoError(t, eg.Wait())
	t.Log("finish writing 90 files of 10MB")

	for ; fileIdx < 2091; fileIdx++ {
		fileName := fmt.Sprintf("/test%d", fileIdx)
		writer := NewWriterBuilder().BuildOneFile(store, fileName, "writerID")
		err := writer.Init(ctx, 5*1024*1024)
		require.NoError(t, err)

		kvSize := 0
		keyIdx := 0
		for kvSize < 1024*1024*1024 {
			key := []byte(fmt.Sprintf("key%010d_%d", keyIdx, fileIdx))
			keyIdx++
			kvSize += len(key) + len(val)
			err = writer.WriteRow(ctx, key, val)
			require.NoError(t, err)
		}

		// write some extra data that is greater than readRangeEnd
		err = writer.WriteRow(ctx, keyAfterRange, val)
		require.NoError(t, err)
		err = writer.WriteRow(ctx, keyAfterRange2, make([]byte, 900*1024))
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)
	}
	t.Log("finish writing 1 file of 1G")

finishCreateFiles:

	dataFiles, statFiles, err := GetAllFileNames(ctx, store, "/")
	require.NoError(t, err)
	require.Equal(t, 2091, len(dataFiles))

	p := newProfiler(true, true)
	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc),
	)
	output := &memKVsAndBuffers{}
	p.beforeTest()
	now := time.Now()
	err = readAllData(ctx, store, dataFiles, statFiles, readRangeStart, readRangeEnd, smallBlockBufPool, largeBlockBufPool, output)
	require.NoError(t, err)
	output.build(ctx)
	elapsed := time.Since(now)
	p.afterTest()
	t.Logf("readAllData time cost: %s, size: %d", elapsed.String(), output.size)
}
