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

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

var testingStorageURI = flag.String("testing-storage-uri", "", "the URI of the storage used for testing")

func openTestingStorage(t *testing.T) storage.ExternalStorage {
	if *testingStorageURI == "" {
		t.Skip("testingStorageURI is not set")
	}
	s, err := storage.NewFromURL(context.Background(), *testingStorageURI, nil)
	intest.AssertNoError(err)
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

func writeExternalFile(s *writeTestSuite) {
	ctx := context.Background()
	filePath := "/test/writer"
	files, statFiles, err := GetAllFileNames(ctx, s.store, filePath)
	intest.AssertNoError(err)
	err = s.store.DeleteFiles(ctx, files)
	intest.AssertNoError(err)
	err = s.store.DeleteFiles(ctx, statFiles)
	intest.AssertNoError(err)
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
	err = writer.Close(ctx)
	intest.AssertNoError(err)
	if s.afterWriterClose != nil {
		s.afterWriterClose()
	}
}

func writeExternalOneFile(s *writeTestSuite) {
	ctx := context.Background()
	filePath := "/test/writer"
	files, statFiles, err := GetAllFileNames(ctx, s.store, filePath)
	intest.AssertNoError(err)
	err = s.store.DeleteFiles(ctx, files)
	intest.AssertNoError(err)
	err = s.store.DeleteFiles(ctx, statFiles)
	intest.AssertNoError(err)
	builder := NewWriterBuilder().
		SetMemorySizeLimit(uint64(s.memoryLimit))

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer := builder.BuildOneFile(
		s.store, filePath, "writerID")
	_ = writer.Init(ctx, 20*1024*1024)
	key, val, _ := s.source.next()
	for key != nil {
		err := writer.WriteRow(ctx, key, val)
		intest.AssertNoError(err)
		key, val, _ = s.source.next()
	}
	if s.beforeWriterClose != nil {
		s.beforeWriterClose()
	}
	err = writer.Close(ctx)
	intest.AssertNoError(err)
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

func readFileSequential(s *readTestSuite) {
	ctx := context.Background()
	files, _, err := GetAllFileNames(ctx, s.store, "/"+s.subDir)
	intest.AssertNoError(err)

	buf := make([]byte, s.memoryLimit)
	if s.beforeCreateReader != nil {
		s.beforeCreateReader()
	}
	for i, file := range files {
		reader, err := s.store.Open(ctx, file, nil)
		intest.AssertNoError(err)
		_, err = reader.Read(buf)
		for err == nil {
			_, err = reader.Read(buf)
		}
		intest.Assert(err == io.EOF)
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
}

func readFileConcurrently(s *readTestSuite) {
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
	for _, file := range files {
		eg.Go(func() error {
			buf := make([]byte, s.memoryLimit/conc)
			reader, err := s.store.Open(ctx, file, nil)
			intest.AssertNoError(err)
			_, err = reader.Read(buf)
			for err == nil {
				_, err = reader.Read(buf)
			}
			intest.Assert(err == io.EOF)
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
}

func createEvenlyDistributedFiles(
	t *testing.T,
	fileSize, fileCount int,
	subDir string,
) (storage.ExternalStorage, int) {
	store := openTestingStorage(t)
	ctx := context.Background()

	files, statFiles, err := GetAllFileNames(ctx, store, "/"+subDir)
	intest.AssertNoError(err)
	err = store.DeleteFiles(ctx, files)
	intest.AssertNoError(err)
	err = store.DeleteFiles(ctx, statFiles)
	intest.AssertNoError(err)

	value := make([]byte, 100)
	kvCnt := 0
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
			err := writer.WriteRow(ctx, []byte(key), value, nil)
			intest.AssertNoError(err)
			keyIdx += fileCount
			totalSize += len(key) + len(value)
			kvCnt++
		}
		err := writer.Close(ctx)
		intest.AssertNoError(err)
	}
	return store, kvCnt
}

func readMergeIter(s *readTestSuite) {
	ctx := context.Background()
	files, _, err := GetAllFileNames(ctx, s.store, "/"+s.subDir)
	intest.AssertNoError(err)

	if s.beforeCreateReader != nil {
		s.beforeCreateReader()
	}

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
	}
	intest.Assert(kvCnt == s.totalKVCnt)
	err = iter.Close()
	intest.AssertNoError(err)
	if s.afterReaderClose != nil {
		s.afterReaderClose()
	}
}

func TestCompareReaderEvenlyDistributedContent(t *testing.T) {
	fileSize := 50 * 1024 * 1024
	fileCnt := 24
	subDir := "evenly_distributed"
	store, kvCnt := createEvenlyDistributedFiles(t, fileSize, fileCnt, subDir)
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

	readFileSequential(suite)
	t.Logf(
		"sequential read speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)

	readFileConcurrently(suite)
	t.Logf(
		"concurrent read speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)

	readMergeIter(suite)
	t.Logf(
		"merge iter read speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)
}

func createAscendingFiles(
	t *testing.T,
	fileSize, fileCount int,
	subDir string,
) (storage.ExternalStorage, int) {
	store := openTestingStorage(t)
	ctx := context.Background()

	files, statFiles, err := GetAllFileNames(ctx, store, "/"+subDir)
	intest.AssertNoError(err)
	err = store.DeleteFiles(ctx, files)
	intest.AssertNoError(err)
	err = store.DeleteFiles(ctx, statFiles)
	intest.AssertNoError(err)

	keyIdx := 0
	value := make([]byte, 100)
	kvCnt := 0
	for i := 0; i < fileCount; i++ {
		builder := NewWriterBuilder().
			SetMemorySizeLimit(uint64(float64(fileSize) * 1.1))
		writer := builder.Build(
			store,
			"/"+subDir,
			fmt.Sprintf("%d", i),
		)

		totalSize := 0
		for totalSize < fileSize {
			key := fmt.Sprintf("key_%09d", keyIdx)
			err := writer.WriteRow(ctx, []byte(key), value, nil)
			intest.AssertNoError(err)
			keyIdx++
			totalSize += len(key) + len(value)
			kvCnt++
		}
		err := writer.Close(ctx)
		intest.AssertNoError(err)
	}
	return store, kvCnt
}

func TestCompareReaderAscendingContent(t *testing.T) {
	fileSize := 50 * 1024 * 1024
	fileCnt := 24
	subDir := "ascending"
	store, kvCnt := createAscendingFiles(t, fileSize, fileCnt, subDir)
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

	readFileSequential(suite)
	t.Logf(
		"sequential read speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)

	readFileConcurrently(suite)
	t.Logf(
		"concurrent read speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)

	readMergeIter(suite)
	t.Logf(
		"merge iter read (hotspot=false) speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)

	suite.mergeIterHotspot = true
	readMergeIter(suite)
	t.Logf(
		"merge iter read (hotspot=true) speed for %d bytes: %.2f MB/s",
		fileSize*fileCnt,
		float64(fileSize*fileCnt)/elapsed.Seconds()/1024/1024,
	)
}

const largeAscendingDataPath = "large_ascending_data"

// TestPrepareLargeData will write 1000 * 256MB data to the storage.
func TestPrepareLargeData(t *testing.T) {
	store := openTestingStorage(t)
	ctx := context.Background()

	dataFiles, statFiles, err := GetAllFileNames(ctx, store, largeAscendingDataPath)
	intest.AssertNoError(err)
	err = store.DeleteFiles(ctx, dataFiles)
	intest.AssertNoError(err)
	err = store.DeleteFiles(ctx, statFiles)
	intest.AssertNoError(err)

	fileSize := 256 * 1024 * 1024
	//fileCnt := 1000
	fileCnt := 200
	keySize := 20
	valueSize := 100
	concurrency := runtime.NumCPU()
	filePerConcUpperBound := (fileCnt + concurrency - 1) / concurrency

	size := atomic.NewInt64(0)
	now := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	go func() {
		for {
			select {
			case <-time.After(time.Second):
				now := time.Now()
				file, err := os.Create(fmt.Sprintf("/tmp/heap-%d.prof", now.UnixMicro()))
				intest.AssertNoError(err)
				err = pprof.WriteHeapProfile(file)
				intest.AssertNoError(err)
				file.Close()
			}
		}
	}()
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
			kvCnt := fileSize * (endFile - startFile) / (keySize + valueSize + 16)
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
	dataFiles, _, err = GetAllFileNames(ctx, store, largeAscendingDataPath)
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
