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
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var testingStorageURI = flag.String("testing-storage-uri", "", "the URI of the storage used for testing")

func openTestingStorage(t *testing.T) storage.ExternalStorage {
	if *testingStorageURI == "" {
		t.Skip("testingStorageURI is not set")
	}
	s, err := storage.NewFromURL(context.Background(), *testingStorageURI)
	require.NoError(t, err)
	return s
}

type kvSource interface {
	next() (key, value []byte, handle kv.Handle)
	outputSize() int
}

type ascendingKeyGenerator struct {
	keySize         int
	keyCommonPrefix []byte
	count           int
	curKey          []byte
	keyOutCh        chan []byte
}

func generateAscendingKey(
	count int,
	keySize int,
	keyCommonPrefix []byte,
) chan []byte {
	c := &ascendingKeyGenerator{
		keySize:         keySize,
		count:           count,
		keyCommonPrefix: keyCommonPrefix,
		keyOutCh:        make(chan []byte, 100),
	}
	c.curKey = make([]byte, keySize)
	copy(c.curKey, keyCommonPrefix)
	c.run()
	return c.keyOutCh
}

func (c *ascendingKeyGenerator) run() {
	keyCommonPrefixSize := len(c.keyCommonPrefix)
	incSuffixLen := int(math.Ceil(math.Log2(float64(c.count)) / 8))
	if c.keySize-keyCommonPrefixSize < incSuffixLen {
		panic(fmt.Sprintf("key size %d is too small, keyCommonPrefixSize: %d, incSuffixLen: %d",
			c.keySize, keyCommonPrefixSize, incSuffixLen))
	}

	go func() {
		defer close(c.keyOutCh)
		for i := 0; i < c.count; i++ {
			// ret to use most left bytes to alternate the key
			for j := keyCommonPrefixSize + incSuffixLen - 1; j >= keyCommonPrefixSize; j-- {
				c.curKey[j]++
				if c.curKey[j] != 0 {
					break
				}
			}
			c.keyOutCh <- slices.Clone(c.curKey)
		}
	}()
}

type ascendingKeySource struct {
	valueSize int
	keys      [][]byte
	keysIdx   int
	totalSize int
}

func newAscendingKeySource(
	count int,
	keySize int,
	valueSize int,
	keyCommonPrefix []byte,
) *ascendingKeySource {
	keyCh := generateAscendingKey(count, keySize, keyCommonPrefix)
	s := &ascendingKeySource{
		valueSize: valueSize,
		keys:      make([][]byte, count),
	}
	for i := 0; i < count; i++ {
		key := <-keyCh
		s.keys[i] = key
		s.totalSize += len(key) + valueSize
	}
	return s
}

func (s *ascendingKeySource) next() (key, value []byte, handle kv.Handle) {
	if s.keysIdx >= len(s.keys) {
		return nil, nil, nil
	}
	key = s.keys[s.keysIdx]
	s.keysIdx++
	return key, make([]byte, s.valueSize), nil
}

func (s *ascendingKeySource) outputSize() int {
	return s.totalSize
}

type ascendingKeyAsyncSource struct {
	valueSize int
	keyOutCh  chan []byte
	totalSize int
}

func newAscendingKeyAsyncSource(
	count int,
	keySize int,
	valueSize int,
	keyCommonPrefix []byte,
) *ascendingKeyAsyncSource {
	s := &ascendingKeyAsyncSource{
		valueSize: valueSize,
		keyOutCh:  generateAscendingKey(count, keySize, keyCommonPrefix),
	}
	return s
}

func (s *ascendingKeyAsyncSource) next() (key, value []byte, handle kv.Handle) {
	key, ok := <-s.keyOutCh
	if !ok {
		return nil, nil, nil
	}
	s.totalSize += len(key) + s.valueSize
	return key, make([]byte, s.valueSize), nil
}

func (s *ascendingKeyAsyncSource) outputSize() int {
	return s.totalSize
}

type randomKeyGenerator struct {
	keySize         int
	keyCommonPrefix []byte
	rnd             *rand.Rand
	count           int
	curKey          []byte
	keyOutCh        chan []byte
}

func generateRandomKey(
	count int,
	keySize int,
	keyCommonPrefix []byte,
	seed int,
) chan []byte {
	c := &randomKeyGenerator{
		keySize:         keySize,
		count:           count,
		keyCommonPrefix: keyCommonPrefix,
		rnd:             rand.New(rand.NewSource(int64(seed))),
		keyOutCh:        make(chan []byte, 100),
	}
	c.curKey = make([]byte, keySize)
	copy(c.curKey, keyCommonPrefix)
	c.run()
	return c.keyOutCh
}

func (c *randomKeyGenerator) run() {
	keyCommonPrefixSize := len(c.keyCommonPrefix)
	incSuffixLen := int(math.Ceil(math.Log2(float64(c.count)) / 8))
	randomLen := c.keySize - keyCommonPrefixSize - incSuffixLen
	if randomLen < 0 {
		panic(fmt.Sprintf("key size %d is too small, keyCommonPrefixSize: %d, incSuffixLen: %d",
			c.keySize, keyCommonPrefixSize, incSuffixLen))
	}

	go func() {
		defer close(c.keyOutCh)
		for i := 0; i < c.count; i++ {
			c.rnd.Read(c.curKey[keyCommonPrefixSize : keyCommonPrefixSize+randomLen])
			for j := len(c.curKey) - 1; j >= keyCommonPrefixSize+randomLen; j-- {
				c.curKey[j]++
				if c.curKey[j] != 0 {
					break
				}
			}
			c.keyOutCh <- slices.Clone(c.curKey)
		}
	}()
}

type randomKeySource struct {
	valueSize int
	keys      [][]byte
	keysIdx   int
	totalSize int
}

func newRandomKeySource(
	count int,
	keySize int,
	valueSize int,
	keyCommonPrefix []byte,
	seed int,
) *randomKeySource {
	keyCh := generateRandomKey(count, keySize, keyCommonPrefix, seed)
	s := &randomKeySource{
		valueSize: valueSize,
		keys:      make([][]byte, count),
	}
	for i := 0; i < count; i++ {
		key := <-keyCh
		s.keys[i] = key
		s.totalSize += len(key) + valueSize
	}
	return s
}

func (s *randomKeySource) next() (key, value []byte, handle kv.Handle) {
	if s.keysIdx >= len(s.keys) {
		return nil, nil, nil
	}
	key = s.keys[s.keysIdx]
	s.keysIdx++
	return key, make([]byte, s.valueSize), nil
}

func (s *randomKeySource) outputSize() int {
	return s.totalSize
}

type writeTestSuite struct {
	store              storage.ExternalStorage
	source             kvSource
	memoryLimit        int
	beforeCreateWriter func()
	beforeWriterClose  func()
	afterWriterClose   func()
}

func writePlainFile(s *writeTestSuite) {
	ctx := context.Background()
	filePath := "/test/writer"
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
	if s.beforeWriterClose != nil {
		s.beforeWriterClose()
	}
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
	cleanOldFiles(ctx, s.store, filePath)
	builder := NewWriterBuilder().
		SetMemorySizeLimit(uint64(s.memoryLimit))

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
	if s.beforeWriterClose != nil {
		s.beforeWriterClose()
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
	cleanOldFiles(ctx, s.store, filePath)
	builder := NewWriterBuilder().
		SetMemorySizeLimit(uint64(s.memoryLimit))

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer := builder.BuildOneFile(
		s.store, filePath, "writerID")
	intest.AssertNoError(writer.Init(ctx, 20*1024*1024, 20))
	key, val, _ := s.source.next()
	for key != nil {
		err := writer.WriteRow(ctx, key, val)
		intest.AssertNoError(err)
		key, val, _ = s.source.next()
	}
	if s.beforeWriterClose != nil {
		s.beforeWriterClose()
	}
	intest.AssertNoError(writer.Close(ctx))
	if s.afterWriterClose != nil {
		s.afterWriterClose()
	}
}

// TestCompareWriter should be run like
// go test ./br/pkg/lightning/backend/external -v -timeout=1h --tags=intest -test.run TestCompareWriter --testing-storage-uri="s3://xxx".
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
		file    *os.File
		err     error
	)
	beforeTest := func() {
		testIdx++
		file, err = os.Create(fmt.Sprintf("cpu-profile-%d.prof", testIdx))
		intest.AssertNoError(err)
		err = pprof.StartCPUProfile(file)
		intest.AssertNoError(err)
		now = time.Now()
	}
	beforeClose := func() {
		file, err = os.Create(fmt.Sprintf("heap-profile-%d.prof", testIdx))
		intest.AssertNoError(err)
		// check heap profile to see the memory usage is expected
		err = pprof.WriteHeapProfile(file)
		intest.AssertNoError(err)
	}
	afterClose := func() {
		elapsed = time.Since(now)
		pprof.StopCPUProfile()
	}

	suite := &writeTestSuite{
		memoryLimit:        memoryLimit,
		beforeCreateWriter: beforeTest,
		beforeWriterClose:  beforeClose,
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
	beforeReaderClose  func()
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
	for i, file := range files {
		reader, err := s.store.Open(ctx, file, nil)
		intest.AssertNoError(err)
		var size int
		for {
			n, err := reader.Read(buf)
			size += n
			if err != nil {
				break
			}
		}
		intest.Assert(err == io.EOF)
		totalFileSize.Add(int64(size))
		if i == len(files)-1 {
			if s.beforeReaderClose != nil {
				s.beforeReaderClose()
			}
		}
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
	var once sync.Once

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
			var size int
			for {
				n, err := reader.Read(buf)
				size += n
				if err != nil {
					break
				}
			}
			intest.Assert(err == io.EOF)
			totalFileSize.Add(int64(size))
			once.Do(func() {
				if s.beforeReaderClose != nil {
					s.beforeReaderClose()
				}
			})
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

func createEvenlyDistributedFiles(
	store storage.ExternalStorage,
	fileSize, fileCount int,
	subDir string,
) (int, kv.Key, kv.Key) {
	ctx := context.Background()

	cleanOldFiles(ctx, store, "/"+subDir)

	value := make([]byte, 100)
	kvCnt := 0
	var minKey, maxKey kv.Key
	for i := 0; i < fileCount; i++ {
		builder := NewWriterBuilder().
			SetBlockSize(10 * 1024 * 1024).
			SetMemorySizeLimit(uint64(float64(fileSize) * 1.1))
		writer := builder.Build(
			store,
			"/"+subDir,
			fmt.Sprintf("%d", i),
		)

		keyIdx := i
		totalSize := 0
		for totalSize < fileSize {
			key := fmt.Sprintf("key_%09d", keyIdx)
			if len(minKey) == 0 && len(maxKey) == 0 {
				minKey = []byte(key)
				maxKey = []byte(key)
			} else {
				minKey = BytesMin(minKey, []byte(key))
				maxKey = BytesMax(maxKey, []byte(key))
			}
			err := writer.WriteRow(ctx, []byte(key), value, nil)
			intest.AssertNoError(err)
			keyIdx += fileCount
			totalSize += len(key) + len(value)
			kvCnt++
		}
		err := writer.Close(ctx)
		intest.AssertNoError(err)
	}
	return kvCnt, minKey, maxKey
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
		if kvCnt == s.totalKVCnt/2 {
			if s.beforeReaderClose != nil {
				s.beforeReaderClose()
			}
		}
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
	fileIdx := 0
	var (
		now     time.Time
		elapsed time.Duration
		file    *os.File
		err     error
	)
	beforeTest := func() {
		fileIdx++
		file, err = os.Create(fmt.Sprintf("cpu-profile-%d.prof", fileIdx))
		intest.AssertNoError(err)
		err = pprof.StartCPUProfile(file)
		intest.AssertNoError(err)
		now = time.Now()
	}
	beforeClose := func() {
		file, err = os.Create(fmt.Sprintf("heap-profile-%d.prof", fileIdx))
		intest.AssertNoError(err)
		// check heap profile to see the memory usage is expected
		err = pprof.WriteHeapProfile(file)
		intest.AssertNoError(err)
	}
	afterClose := func() {
		elapsed = time.Since(now)
		pprof.StopCPUProfile()
	}

	suite := &readTestSuite{
		store:              store,
		totalKVCnt:         kvCnt,
		concurrency:        100,
		memoryLimit:        memoryLimit,
		beforeCreateReader: beforeTest,
		beforeReaderClose:  beforeClose,
		afterReaderClose:   afterClose,
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

func createAscendingFiles(
	store storage.ExternalStorage,
	fileSize, fileCount int,
	subDir string,
) (int, kv.Key, kv.Key) {
	ctx := context.Background()

	cleanOldFiles(ctx, store, "/"+subDir)

	keyIdx := 0
	value := make([]byte, 100)
	kvCnt := 0
	var minKey, maxKey kv.Key
	for i := 0; i < fileCount; i++ {
		builder := NewWriterBuilder().
			SetMemorySizeLimit(uint64(float64(fileSize) * 1.1))
		writer := builder.Build(
			store,
			"/"+subDir,
			fmt.Sprintf("%d", i),
		)

		totalSize := 0
		var key string
		for totalSize < fileSize {
			key = fmt.Sprintf("key_%09d", keyIdx)
			if i == 0 && totalSize == 0 {
				minKey = []byte(key)
			}
			err := writer.WriteRow(ctx, []byte(key), value, nil)
			intest.AssertNoError(err)
			keyIdx++
			totalSize += len(key) + len(value)
			kvCnt++
		}
		if i == fileCount-1 {
			maxKey = []byte(key)
		}
		err := writer.Close(ctx)
		intest.AssertNoError(err)
	}
	return kvCnt, minKey, maxKey
}

var (
	objectPrefix      = flag.String("object-prefix", "ascending", "object prefix")
	fileSize          = flag.Int("file-size", 50*units.MiB, "file size")
	fileCount         = flag.Int("file-count", 24, "file count")
	concurrency       = flag.Int("concurrency", 100, "concurrency")
	writerConcurrency = flag.Int("writer-concurrency", 20, "writer concurrency")
	memoryLimit       = flag.Int("memory-limit", 64*units.MiB, "memory limit")
	skipCreate        = flag.Bool("skip-create", false, "skip create files")
	fileName          = flag.String("file-name", "test", "file name for tests")
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
	fn func(t *testing.T, suite *readTestSuite)) {
	store := openTestingStorage(t)
	kvCnt := 0
	if !*skipCreate {
		kvCnt, _, _ = createFn(store, *fileSize, *fileCount, *objectPrefix)
	}
	fileIdx := 0
	var (
		file *os.File
		err  error
	)
	beforeTest := func() {
		fileIdx++
		file, err = os.Create(fmt.Sprintf("cpu-profile-%d.prof", fileIdx))
		intest.AssertNoError(err)
		err = pprof.StartCPUProfile(file)
		intest.AssertNoError(err)
	}
	beforeClose := func() {
		file, err = os.Create(fmt.Sprintf("heap-profile-%d.prof", fileIdx))
		intest.AssertNoError(err)
		// check heap profile to see the memory usage is expected
		err = pprof.WriteHeapProfile(file)
		intest.AssertNoError(err)
	}
	afterClose := func() {
		pprof.StopCPUProfile()
	}

	suite := &readTestSuite{
		store:              store,
		totalKVCnt:         kvCnt,
		concurrency:        *concurrency,
		memoryLimit:        *memoryLimit,
		beforeCreateReader: beforeTest,
		beforeReaderClose:  beforeClose,
		afterReaderClose:   afterClose,
		subDir:             *objectPrefix,
	}

	fn(t, suite)
}

const largeAscendingDataPath = "large_ascending_data"

// TestPrepareLargeData will write 1000 * 256MB data to the storage.
func TestPrepareLargeData(t *testing.T) {
	store := openTestingStorage(t)
	ctx := context.Background()

	cleanOldFiles(ctx, store, largeAscendingDataPath)

	fileSize := 256 * 1024 * 1024
	fileCnt := 1000
	keySize := 20
	valueSize := 100
	concurrency := runtime.NumCPU() / 2
	filePerConcUpperBound := (fileCnt + concurrency - 1) / concurrency

	size := atomic.NewInt64(0)
	now := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		i := i
		go func() {
			defer wg.Done()
			writer := NewWriterBuilder().
				SetMemorySizeLimit(uint64(fileSize)).
				Build(store, largeAscendingDataPath, fmt.Sprintf("%02d", i))
			endFile := min((i+1)*filePerConcUpperBound, fileCnt)
			startFile := min(i*filePerConcUpperBound, endFile)
			if startFile == endFile {
				return
			}

			// slightly reduce total size to avoid generate a small file at the end
			totalSize := fileSize*(endFile-startFile) - 20*1024*1024
			kvCnt := totalSize / (keySize + valueSize + 16)
			source := newAscendingKeyAsyncSource(kvCnt, keySize, valueSize, []byte{byte(i)})
			key, val, _ := source.next()
			for key != nil {
				err := writer.WriteRow(ctx, key, val, nil)
				intest.AssertNoError(err)
				size.Add(int64(len(key) + len(val)))
				key, val, _ = source.next()
			}
			err := writer.Close(ctx)
			intest.AssertNoError(err)
		}()
	}
	wg.Wait()
	elapsed := time.Since(now)
	t.Logf("write %d bytes in %s, speed: %.2f MB/s",
		size.Load(), elapsed, float64(size.Load())/elapsed.Seconds()/1024/1024)
	dataFiles, _, err := GetAllFileNames(ctx, store, largeAscendingDataPath)
	intest.AssertNoError(err)

	r, err := store.Open(ctx, dataFiles[0], nil)
	intest.AssertNoError(err)
	firstFileSize, err := r.GetFileSize()
	intest.AssertNoError(err)
	err = r.Close()
	intest.AssertNoError(err)

	r, err = store.Open(ctx, dataFiles[len(dataFiles)-1], nil)
	intest.AssertNoError(err)
	lastFileSize, err := r.GetFileSize()
	intest.AssertNoError(err)
	err = r.Close()
	intest.AssertNoError(err)
	t.Logf("total %d data files, first file size: %.2f MB, last file size: %.2f MB",
		len(dataFiles), float64(firstFileSize)/1024/1024, float64(lastFileSize)/1024/1024)
}

type mergeTestSuite struct {
	store             storage.ExternalStorage
	subDir            string
	totalKVCnt        int
	concurrency       int
	writerConcurrency int
	mergeConcurrency  int
	memoryLimit       int
	mergeIterHotspot  bool
	minKey            kv.Key
	maxKey            kv.Key
	beforeMerge       func()
	afterMerge        func()
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
		64*1024,
		mergeOutput,
		DefaultBlockSize,
		DefaultMemSizeLimit,
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
		datas,
		stats,
		s.store,
		s.minKey,
		s.maxKey.Next(),
		int64(5*size.MB),
		mergeOutput,
		"test",
		DefaultBlockSize,
		1*size.MB,
		8*1024,
		onClose,
		s.concurrency,
		s.writerConcurrency,
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

func newMergeStepOpt(t *testing.T, s *mergeTestSuite) {
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
	err = MergeOverlappingFilesOpt(
		ctx,
		datas,
		stats,
		s.store,
		s.minKey,
		s.maxKey.Next(),
		int64(5*size.MB),
		mergeOutput,
		"test",
		DefaultBlockSize,
		1*size.MB,
		8*1024,
		onClose,
		s.concurrency,
		s.mergeConcurrency,
		s.mergeIterHotspot,
	)

	intest.AssertNoError(err)
	if s.afterMerge != nil {
		s.afterMerge()
	}
	elapsed := time.Since(now)
	t.Logf(
		"new merge parallel speed for %d bytes in %s, speed: %.2f MB/s",
		totalSize.Load(),
		elapsed,
		float64(totalSize.Load())/elapsed.Seconds()/1024/1024,
	)
}

func testCompareMergeWithContent(
	t *testing.T,
	concurrency int,
	mergeConcurrency int,
	createFn func(store storage.ExternalStorage, fileSize int, fileCount int, objectPrefix string) (int, kv.Key, kv.Key),
	fn func(t *testing.T, suite *mergeTestSuite)) {
	store := openTestingStorage(t)
	kvCnt := 0
	var minKey, maxKey kv.Key
	if !*skipCreate {
		kvCnt, minKey, maxKey = createFn(store, *fileSize, *fileCount, *objectPrefix)
	}

	fileIdx := 0
	var (
		file *os.File
		err  error
	)
	beforeTest := func() {
		file, err = os.Create(fmt.Sprintf("cpu-profile-%d.prof", fileIdx))
		intest.AssertNoError(err)
		err = pprof.StartCPUProfile(file)
		intest.AssertNoError(err)
	}

	afterTest := func() {
		pprof.StopCPUProfile()
	}

	suite := &mergeTestSuite{
		store:             store,
		totalKVCnt:        kvCnt,
		concurrency:       concurrency,
		writerConcurrency: *writerConcurrency,
		mergeConcurrency:  mergeConcurrency,
		memoryLimit:       *memoryLimit,
		beforeMerge:       beforeTest,
		afterMerge:        afterTest,
		subDir:            *objectPrefix,
		minKey:            minKey,
		maxKey:            maxKey,
		mergeIterHotspot:  true,
	}

	fn(t, suite)
}

func TestMergeBench(t *testing.T) {
	// testCompareMergeWithContent(t, 1, 0, createAscendingFiles, mergeStep)
	// testCompareMergeWithContent(t, 1, 0, createEvenlyDistributedFiles, mergeStep)
	// testCompareMergeWithContent(t, 2, 0, createAscendingFiles, mergeStep)
	// testCompareMergeWithContent(t, 2, 0, createEvenlyDistributedFiles, mergeStep)
	// testCompareMergeWithContent(t, 4, 0, createAscendingFiles, mergeStep)
	// testCompareMergeWithContent(t, 4, 0, createEvenlyDistributedFiles, mergeStep)
	// testCompareMergeWithContent(t, 8, 0, createAscendingFiles, mergeStep)
	// testCompareMergeWithContent(t, 8, 0, createEvenlyDistributedFiles, mergeStep)
	// // testCompareMergeWithContent(t, 8, 0, createAscendingFiles, newMergeStep)
	// // testCompareMergeWithContent(t, 8, 0, createEvenlyDistributedFiles, newMergeStep)
	// testCompareMergeWithContent(t, 8, 1, createAscendingFiles, newMergeStepOpt)
	// testCompareMergeWithContent(t, 8, 1, createEvenlyDistributedFiles, newMergeStepOpt)
	// testCompareMergeWithContent(t, 8, 2, createAscendingFiles, newMergeStepOpt)
	// testCompareMergeWithContent(t, 8, 2, createEvenlyDistributedFiles, newMergeStepOpt)
	// testCompareMergeWithContent(t, 8, 4, createAscendingFiles, newMergeStepOpt)
	testCompareMergeWithContent(t, 8, 4, createEvenlyDistributedFiles, newMergeStepOpt)
}

func TestReadStatFile(t *testing.T) {
	ctx := context.Background()
	store := openTestingStorage(t)
	// rd, _ := newStatsReader(ctx, store, *fileName, 4096)
	// for {

	// 	prop, err := rd.nextProp()
	// 	if err == io.EOF {
	// 		break
	// 	}
	// 	logutil.BgLogger().Info("read one prop",
	// 		zap.Int("prop len", prop.len()),
	// 		zap.Binary("first key", prop.firstKey),
	// 		zap.Binary("last key", prop.lastKey),
	// 		zap.Int("prop offset", int(prop.offset)),
	// 		zap.Int("prop size", int(prop.size)),
	// 		zap.Int("prop keys", int(prop.keys)))
	// }
	str := "dIAAAAAAAABuX3IBMTAyNjE2MTD/VkxqcHZpSXL/blFERUt1OXD/eHhlOUlUYVP/U3RJRFdHc3L/AAAAAAAAAAD3"
	key, _ := base64.RawStdEncoding.DecodeString(str)
	// offset, _ := seekPropsOffsets(ctx, key, []string{*fileName}, store, false)
	// logutil.BgLogger().Info("ywq test start off", zap.Any("off", offset))
	str = "dIAAAAAAAABuX3IBMTUyMDAwMDD/cVc0c3Bkcmb/NExsb1pEaXX/Q3hFT2YwSG3/enpoZUZ1QUz/AAAAAAAAAAD3AA=="
	// str = "dIAAAAAAAABuX3IBMTAyNjE2MTD/VkxqcHZpSXL/blFERUt1OXD/eHhlOUlUYVP/U3RJRFdHc3L/AAAAAAAAAAD3"
	endkey, _ := base64.RawStdEncoding.DecodeString(str)
	// offset, _ = seekPropsOffsets(ctx, endkey, []string{*fileName}, store, false)
	// logutil.BgLogger().Info("ywq test end off", zap.Any("off", offset))
	logutil.BgLogger().Info("ywq test compare", zap.Any("cmp", bytes.Compare(key, endkey)))

	start1, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAACPX2mAAAAAAAAAAgExMDAwMDAwMf9uUG1Gdm9vMf9wcWR6Qk9DSv9MWWtSZXI3T/9GNExwRklXb/8AAAAAAAAAAPc=")
	end1, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAACPX2mAAAAAAAAAAgExNjczNjE2Mf9Bd2dNR1hnR/8xT2tjYUlaTv9sY1ZFOUhnef8yZzg4WFo1Yf8AAAAAAAAAAPc=")
	logutil.BgLogger().Info("ywq test compare", zap.Any("cmp1", bytes.Compare(start1, end1)))

	start2, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAACPX2mAAAAAAAAAAgExNjczNjE2Mf9Bd2dNR1hnR/8xT2tjYUlaTv9sY1ZFOUhnef8yZzg4WFo1Yf8AAAAAAAAAAPc=")
	end2, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAACPX2mAAAAAAAAAAgEzODQ5MTUzR/9CakpPYWdhbP9nNUZkbjdpbv9KZUN3UXJwbf9kbzFyd3JJAP4=")
	logutil.BgLogger().Info("ywq test compare", zap.Any("cmp2", bytes.Compare(start2, end2)))

	start3, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAACPX2mAAAAAAAAAAgEzODQ5MTUzR/9CakpPYWdhbP9nNUZkbjdpbv9KZUN3UXJwbf9kbzFyd3JJAP4=")
	end3, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAACPX2mAAAAAAAAAAgExMjQwMDAwMP92bW9GdzRMWP9xY2dxb1o4bf91cFBFREp6Uf8yN1lLTEJwNP8AAAAAAAAAAPcA")
	logutil.BgLogger().Info("ywq test compare", zap.Any("cmp3", bytes.Compare(start3, end3)))

	start4, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAACPX2mAAAAAAAAAAgExMDAwMDAwMf9uUG1Gdm9vMf9wcWR6Qk9DSv9MWWtSZXI3T/9GNExwRklXb/8AAAAAAAAAAPc=")
	end4, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAACPX2mAAAAAAAAAAgExMjQwMDAwMP92bW9GdzRMWP9xY2dxb1o4bf91cFBFREp6Uf8yN1lLTEJwNP8AAAAAAAAAAPcA")
	logutil.BgLogger().Info("ywq test compare", zap.Any("cmp4", bytes.Compare(start4, end4)))

	min, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAABuX3IBMTAwMDAwMDD/S1FDWmV2aUv/THZCd0hrUGv/cjBYOElnTEH/b1VRaW41clr/AAAAAAAAAAD3")    // read from stat
	wtfmin, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAABuX3IBMTA1MDAwMDH/b1JVMTE2RWX/aWUxOTk5dVL/Z21ObEFiWHL/cExNcDFFYzn/AAAAAAAAAAD3") // from multi stat
	logutil.BgLogger().Info("ywq test compare", zap.Any("cmp5", bytes.Compare(min, wtfmin)))

	max, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAABuX3IBOWtEeFROeTH/anlHeTg1M1n/SmdrNnZ6eXX/R3ZiendoTnX/aQAAAAAAAAD4")
	wtfmax, _ := base64.RawStdEncoding.DecodeString("dIAAAAAAAABuX3IBMTUyMDAwMDD/cVc0c3Bkcmb/NExsb1pEaXX/Q3hFT2YwSG3/enpoZUZ1QUz/AAAAAAAAAAD3AA==")
	logutil.BgLogger().Info("ywq test compare", zap.Any("cmp6", bytes.Compare(max, wtfmax)))

	files := []string{"60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/0", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/1", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/2", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/3", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/4", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/5", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/6", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/7", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/8", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/9", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/10", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/11", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/12", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/13", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/14", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/15", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/16", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/17", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/18", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/19", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/20", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/21", "60001/60001/data/91d8bb17-5153-4009-be1a-6521a0e55329_stat/22", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/0", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/1", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/2", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/3", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/4", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/5", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/6", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/7", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/8", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/9", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/10", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/11", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/12", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/13", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/14", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/15", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/16", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/17", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/18", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/19", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/20", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/21", "60001/60001/data/cc346564-1529-4bb2-b32b-d7a98840d35c_stat/22", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/0", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/1", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/2", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/3", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/4", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/5", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/6", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/7", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/8", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/9", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/10",
		"60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/11", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/12", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/13", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/14", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/15", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/16", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/17", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/18", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/19", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/20", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/21", "60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/22", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/0", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/1", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/2", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/3", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/4", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/5", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/6", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/7", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/8", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/9", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/10", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/11", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/12", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/13", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/14", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/15", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/16", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/17", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/18", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/19", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/20", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/21", "60001/60001/data/b423eed4-39c8-4c43-b10b-44f3aeddc2c9_stat/22", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/0", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/1", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/2", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/3", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/4", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/5", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/6", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/7", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/8", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/9", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/10", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/11", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/12", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/13", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/14", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/15", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/16", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/17", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/18", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/19", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/20", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/21", "60001/60001/data/1b92b29c-fee3-4e84-8af9-43ca22174550_stat/22", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/0", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/1", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/2", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/3", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/4", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/5", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/6", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/7", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/8", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/9", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/10", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/11", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/12", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/13", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/14", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/15", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/16", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/17", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/18", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/19", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/20", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/21", "60001/60001/data/6b856330-9968-4009-b425-333212d35038_stat/22", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/0", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/1", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/2", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/3", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/4", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/5", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/6", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/7", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/8", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/9", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/10", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/11", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/12", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/13", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/14", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/15", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/16", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/17", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/18", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/19", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/20", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/21", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/22", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/23", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/24", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/25", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/26", "60001/60001/data/6cd10493-2fd4-4138-a020-3f0697e1d149_stat/27", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/0", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/1", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/2", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/3", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/4", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/5", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/6", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/7", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/8", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/9", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/10", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/11", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/12", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/13", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/14", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/15", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/16", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/17", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/18", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/19", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/20", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/21", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/22", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/23", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/24", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/25", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/26", "60001/60001/data/a85869ee-ded9-49b0-a4eb-9309e63b5dec_stat/27", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/0", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/1", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/2", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/3", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/4", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/5", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/6", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/7", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/8", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/9", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/10", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/11", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/12", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/13", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/14", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/15", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/16", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/17", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/18", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/19",
		"60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/20", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/21", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/22", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/23", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/24", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/25", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/26", "60001/60001/data/92045fe1-e511-406c-9d14-75b2a63bbb6b_stat/27", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/0", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/1", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/2", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/3", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/4", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/5", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/6", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/7", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/8", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/9", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/10", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/11", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/12", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/13", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/14", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/15", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/16", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/17", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/18", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/19", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/20", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/21", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/22", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/23", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/24", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/25", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/26", "60001/60001/data/5bce9a46-e276-4287-badb-1dcd840755ae_stat/27", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/0", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/1", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/2", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/3", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/4", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/5", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/6", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/7", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/8", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/9", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/10", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/11", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/12", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/13", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/14", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/15", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/16", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/17", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/18", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/19", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/20", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/21", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/22", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/23", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/24", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/25", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/26", "60001/60001/data/080f7f5e-cbb0-4ce7-ac3b-8eaec21e1459_stat/27", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/0", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/1", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/2", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/3", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/4", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/5", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/6", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/7", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/8", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/9", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/10", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/11", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/12", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/13", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/14", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/15", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/16", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/17", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/18", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/19", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/20", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/21", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/22", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/23", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/24", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/25", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/26", "60001/60001/data/34b3a996-a7f2-48d8-9d06-b6c48b969cd8_stat/27", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/0", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/1", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/2", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/3", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/4", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/5", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/6", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/7", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/8", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/9", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/10", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/11", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/12", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/13", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/14", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/15", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/16", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/17", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/18", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/19", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/20", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/21", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/22", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/23", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/24", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/25", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/26", "60001/60001/data/cd56b7cb-259d-4df6-a823-20aa8b6060d5_stat/27", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/0", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/1", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/2", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/3", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/4", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/5", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/6", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/7", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/8", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/9", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/10", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/11", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/12", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/13", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/14", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/15", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/16", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/17", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/18", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/19", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/20", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/21", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/22", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/23", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/24", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/25", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/26", "60001/60001/data/17c73e27-9ec5-47c9-a58c-912d51aaf159_stat/27", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/0", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/1", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/2", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/3", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/4", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/5", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/6", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/7", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/8",
		"60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/9", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/10", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/11", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/12", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/13", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/14", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/15", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/16", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/17", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/18", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/19", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/20", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/21", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/22", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/23", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/24", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/25", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/26", "60001/60001/data/2524ab61-a0f4-4576-8bd4-eb6199432f10_stat/27", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/0", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/1", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/2", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/3", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/4", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/5", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/6", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/7", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/8", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/9", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/10", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/11", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/12", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/13", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/14", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/15", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/16", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/17", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/18", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/19", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/20", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/21", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/22", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/23", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/24", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/25", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/26", "60001/60001/data/8476a649-6066-4e50-88b7-1a0ccf8435e4_stat/27", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/0", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/1", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/2", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/3", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/4", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/5", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/6", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/7", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/8", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/9", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/10", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/11", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/12", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/13", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/14", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/15", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/16", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/17", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/18", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/19", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/20", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/21", "60001/60002/data/60768ab7-8fc8-4047-8f69-0628c80e0b22_stat/22", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/0", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/1", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/2", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/3", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/4", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/5", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/6", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/7", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/8", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/9", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/10", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/11", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/12", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/13", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/14", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/15", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/16", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/17", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/18", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/19", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/20", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/21", "60001/60002/data/54d29934-2754-43eb-a806-d3cdf7a1cbf0_stat/22", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/0", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/1", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/2", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/3", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/4", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/5", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/6", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/7", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/8", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/9", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/10", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/11", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/12", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/13", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/14", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/15", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/16", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/17", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/18", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/19", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/20", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/21", "60001/60002/data/e0638f21-7ad9-415a-829e-aeddae1834eb_stat/22", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/0", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/1", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/2", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/3", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/4", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/5", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/6", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/7", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/8", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/9", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/10", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/11", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/12", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/13", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/14", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/15", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/16", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/17", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/18", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/19", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/20", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/21", "60001/60002/data/41e24b71-56f7-4e14-bee2-d2c00deda79f_stat/22", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/0", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/1", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/2", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/3", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/4", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/5", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/6", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/7", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/8", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/9", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/10", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/11", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/12", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/13", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/14", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/15", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/16", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/17",
		"60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/18", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/19", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/20", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/21", "60001/60002/data/2ad95db5-a00a-47d9-a6b3-cb506b89a78b_stat/22", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/0", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/1", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/2", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/3", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/4", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/5", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/6", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/7", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/8", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/9", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/10", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/11", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/12", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/13", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/14", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/15", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/16", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/17", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/18", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/19", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/20", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/21", "60001/60002/data/52379438-eb90-4590-979a-b62ad2d4ba17_stat/22", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/0", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/1", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/2", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/3", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/4", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/5", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/6", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/7", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/8", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/9", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/10", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/11", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/12", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/13", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/14", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/15", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/16", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/17", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/18", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/19", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/20", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/21", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/22", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/23", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/24", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/25", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/26", "60001/60002/data/ae419b87-fbce-4c5a-a94a-917bed82084b_stat/27", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/0", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/1", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/2", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/3", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/4", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/5", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/6", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/7", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/8", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/9", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/10", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/11", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/12", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/13", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/14", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/15", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/16", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/17", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/18", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/19", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/20", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/21", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/22", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/23", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/24", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/25", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/26", "60001/60002/data/1991db39-067f-4d4f-90d0-00f5b5d075ae_stat/27", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/0", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/1", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/2", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/3", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/4", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/5", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/6", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/7", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/8", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/9", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/10", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/11", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/12", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/13", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/14", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/15", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/16", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/17", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/18", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/19", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/20", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/21", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/22", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/23", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/24", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/25", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/26", "60001/60002/data/d1899b27-51c5-491a-b00f-842e4f97a3c9_stat/27", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/0", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/1", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/2", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/3", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/4", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/5", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/6", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/7", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/8", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/9", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/10", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/11", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/12", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/13", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/14", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/15", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/16", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/17", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/18", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/19", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/20", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/21", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/22", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/23", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/24", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/25", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/26", "60001/60002/data/76be2547-14b7-482c-a9f1-4ed709c8fcb3_stat/27", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/0", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/1", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/2", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/3", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/4", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/5", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/6", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/7", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/8", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/9", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/10", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/11", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/12", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/13", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/14", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/15", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/16",
		"60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/17", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/18", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/19", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/20", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/21", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/22", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/23", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/24", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/25", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/26", "60001/60002/data/2b657467-1ebf-42e1-83a4-f2bda1389ad4_stat/27", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/0", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/1", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/2", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/3", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/4", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/5", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/6", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/7", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/8", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/9", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/10", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/11", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/12", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/13", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/14", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/15", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/16", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/17", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/18", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/19", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/20", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/21", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/22", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/23", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/24", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/25", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/26", "60001/60002/data/c52810c8-8d72-4466-9689-2abdc1eaadd9_stat/27", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/0", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/1", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/2", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/3", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/4", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/5", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/6", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/7", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/8", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/9", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/10", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/11", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/12", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/13", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/14", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/15", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/16", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/17", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/18", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/19", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/20", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/21", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/22", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/23", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/24", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/25", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/26", "60001/60002/data/65ef2637-4d19-48a1-9de4-823507d3ff43_stat/27", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/0", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/1", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/2", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/3", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/4", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/5", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/6", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/7", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/8", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/9", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/10", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/11", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/12", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/13", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/14", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/15", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/16", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/17", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/18", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/19", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/20", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/21", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/22", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/23", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/24", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/25", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/26", "60001/60002/data/d2bd780d-2e0f-4950-9bec-4b4032bd9163_stat/27", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/0", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/1", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/2", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/3", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/4", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/5", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/6", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/7", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/8", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/9", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/10", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/11", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/12", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/13", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/14", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/15", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/16", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/17", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/18", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/19", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/20", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/21", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/22", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/23", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/24", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/25", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/26", "60001/60002/data/6d645bf7-9d83-4e21-802c-8c6dd382dd1c_stat/27", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/0", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/1", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/2", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/3", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/4", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/5", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/6", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/7", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/8", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/9", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/10", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/11", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/12", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/13", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/14", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/15", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/16", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/17", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/18", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/19", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/20", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/21", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/22", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/23", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/24", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/25", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/26", "60001/60002/data/acc04d51-c9b2-4722-a43d-b6dbb94ea137_stat/27", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/0", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/1", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/2", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/3", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/4", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/5",
		"60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/6", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/7", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/8", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/9", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/10", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/11", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/12", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/13", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/14", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/15", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/16", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/17", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/18", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/19", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/20", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/21", "60001/60003/data/2f696c6b-33d8-473e-8e82-7082a1f8a34f_stat/22", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/0", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/1", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/2", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/3", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/4", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/5", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/6", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/7", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/8", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/9", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/10", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/11", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/12", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/13", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/14", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/15", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/16", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/17", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/18", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/19", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/20", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/21", "60001/60003/data/7e3b26c2-0e75-45c8-81fb-f94b02b3a818_stat/22", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/0", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/1", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/2", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/3", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/4", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/5", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/6", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/7", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/8", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/9", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/10", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/11", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/12", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/13", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/14", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/15", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/16", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/17", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/18", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/19", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/20", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/21", "60001/60003/data/6fc674a7-e0b2-45df-a008-99c3b31e86c9_stat/22", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/0", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/1", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/2", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/3", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/4", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/5", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/6", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/7", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/8", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/9", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/10", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/11", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/12", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/13", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/14", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/15", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/16", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/17", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/18", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/19", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/20", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/21", "60001/60003/data/12a7acd6-d12b-4db9-bf04-47dfaaa1493a_stat/22", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/0", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/1", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/2", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/3", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/4", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/5", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/6", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/7", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/8", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/9", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/10", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/11", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/12", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/13", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/14", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/15", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/16", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/17", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/18", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/19", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/20", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/21", "60001/60003/data/f41f960d-b262-43ae-b20e-6a513a52eb8a_stat/22", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/0", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/1", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/2", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/3", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/4", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/5", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/6", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/7", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/8", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/9", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/10", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/11", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/12", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/13", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/14", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/15", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/16", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/17", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/18", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/19", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/20", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/21", "60001/60003/data/ce874f0d-e2b5-4b58-ad42-0ba3fab74237_stat/22", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/0", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/1", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/2", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/3", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/4", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/5", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/6", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/7", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/8", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/9", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/10", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/11", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/12", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/13", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/14", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/15", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/16", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/17", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/18", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/19", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/20", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/21", "60001/60003/data/34827643-a6b5-47ed-be1e-3d0b7cfde108_stat/22", "60001/60003/data/a5b3b9fa-bc15-420a-92fb-38155a3a19bd_stat/0", "60001/60003/data/a5b3b9fa-bc15-420a-92fb-38155a3a19bd_stat/1",
		"60001/60003/data/a5b3b9fa-bc15-420a-92fb-38155a3a19bd_stat/2"}
	var minKey, maxKey kv.Key
	logutil.BgLogger().Info("ywq test", zap.Any("len", len(files)))
	nfiles := []string{"60001/60001/data/9551d1f0-8403-4a62-8636-175a191964e0_stat/0"}
	for _, file := range nfiles {
		statsReader, err := newStatsReader(ctx, store, file, 4096)
		require.NoError(t, err)
		var offset uint64
		offset = 0
		for {
			p, err := statsReader.nextProp()
			if err == io.EOF {
				break
			}
			if p.offset > offset {
				offset = p.offset
			}
			if len(minKey) == 0 {
				minKey = kv.Key(p.firstKey).Clone()
				maxKey = kv.Key(p.lastKey).Clone()
			}
			if minKey.Cmp(p.firstKey) > 0 {
				minKey = kv.Key(p.firstKey).Clone()
			}
			if maxKey.Cmp(p.lastKey) < 0 {
				maxKey = kv.Key(p.lastKey).Clone()
			}
		}
		logutil.BgLogger().Info("ywq test offset", zap.Any("offset", offset))
	}

	logutil.BgLogger().Info("ywq test comp", zap.Binary("min", minKey), zap.Binary("max", maxKey))
}
