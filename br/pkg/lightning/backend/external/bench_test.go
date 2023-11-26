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
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
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

type ascendingKeySource struct {
	keySize, valueSize  int
	keyCommonPrefixSize int
	count               int
	kvChan              chan [2][]byte
	curKey              []byte
	totalSize           int
}

func newAscendingKeySource(
	count int,
	keySize int,
	valueSize int,
	keyCommonPrefixSize int,
) *ascendingKeySource {
	s := &ascendingKeySource{
		keySize:             keySize,
		valueSize:           valueSize,
		count:               count,
		keyCommonPrefixSize: keyCommonPrefixSize,
	}
	s.curKey = make([]byte, keySize)
	s.kvChan = make(chan [2][]byte, 100)
	s.run()
	return s
}

func (s *ascendingKeySource) run() {
	incSuffixLen := int(math.Ceil(math.Log2(float64(s.count)) / 8))
	if s.keySize-s.keyCommonPrefixSize < incSuffixLen {
		panic(fmt.Sprintf("key size %d is too small, keyCommonPrefixSize: %d, incSuffixLen: %d",
			s.keySize, s.keyCommonPrefixSize, incSuffixLen))
	}
	go func() {
		defer close(s.kvChan)
		for i := 0; i < s.count; i++ {
			for j := len(s.curKey) - 1; j >= s.keyCommonPrefixSize; j-- {
				s.curKey[j]++
				if s.curKey[j] != 0 {
					break
				}
			}
			key := make([]byte, s.keySize)
			copy(key, s.curKey)
			value := make([]byte, s.valueSize)
			s.kvChan <- [2][]byte{key, value}
			s.totalSize += len(key) + len(value)
		}
	}()
}

func (s *ascendingKeySource) next() (key, value []byte, handle kv.Handle) {
	pair, ok := <-s.kvChan
	if !ok {
		return nil, nil, nil
	}
	return pair[0], pair[1], nil
}

func (s *ascendingKeySource) outputSize() int {
	return s.totalSize
}

type randomKeySource struct {
	keySize, valueSize  int
	keyCommonPrefixSize int
	rnd                 *rand.Rand
	count               int
	kvChan              chan [2][]byte
	curKey              []byte
	totalSize           int
}

func newRandomKeySource(
	count int,
	keySize int,
	valueSize int,
	keyCommonPrefixSize int,
	seed int,
) *randomKeySource {
	s := &randomKeySource{
		keySize:             keySize,
		valueSize:           valueSize,
		count:               count,
		keyCommonPrefixSize: keyCommonPrefixSize,
		rnd:                 rand.New(rand.NewSource(int64(seed))),
	}
	s.curKey = make([]byte, keySize)
	s.kvChan = make(chan [2][]byte, 100)
	s.run()
	return s
}

func (s *randomKeySource) run() {
	incSuffixLen := int(math.Ceil(math.Log2(float64(s.count)) / 8))
	randomLen := s.keySize - s.keyCommonPrefixSize - incSuffixLen
	if randomLen < 0 {
		panic(fmt.Sprintf("key size %d is too small, keyCommonPrefixSize: %d, incSuffixLen: %d",
			s.keySize, s.keyCommonPrefixSize, incSuffixLen))
	}
	go func() {
		defer close(s.kvChan)
		for i := 0; i < s.count; i++ {
			s.rnd.Read(s.curKey[s.keyCommonPrefixSize : s.keyCommonPrefixSize+randomLen])
			for j := len(s.curKey) - 1; j >= s.keyCommonPrefixSize+randomLen; j-- {
				s.curKey[j]++
				if s.curKey[j] != 0 {
					break
				}
			}
			key := make([]byte, s.keySize)
			copy(key, s.curKey)
			value := make([]byte, s.valueSize)
			s.kvChan <- [2][]byte{key, value}
			s.totalSize += len(key) + len(value)
		}
	}()
}

func (s *randomKeySource) next() (key, value []byte, handle kv.Handle) {
	pair, ok := <-s.kvChan
	if !ok {
		return nil, nil, nil
	}
	return pair[0], pair[1], nil
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
	filePath := "/test/plain_file"
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
	filePath := "/test/external"
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
	filePath := "/test/external_one_file"
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

func TestCompareWriter(t *testing.T) {
	externalStore := openTestingStorage(t)
	expectedKVSize := 1024 * 1024 * 1024
	memoryLimit := 64 * 1024 * 1024
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

	for _, kvSize := range [][2]int{{20, 1000}, {20, 100}, {20, 10}} {
		expectedKVNum := expectedKVSize / (kvSize[0] + kvSize[1])
		for _, keyCommonPrefixSize := range []int{3, 10} {
			sources := map[string]func() kvSource{}
			sources["ascending key"] = func() kvSource {
				return newAscendingKeySource(expectedKVNum, kvSize[0], kvSize[1], keyCommonPrefixSize)
			}
			sources["random key"] = func() kvSource {
				return newRandomKeySource(expectedKVNum, kvSize[0], kvSize[1], keyCommonPrefixSize, seed)
			}
			for sourceName, sourceGetter := range sources {
				for storeName, store := range stores {
					for writerName, fn := range writerTestFn {
						suite.store = store
						source := sourceGetter()
						suite.source = source
						t.Logf("test %d: %s, %s, %s, key size: %d, value size: %d, key common prefix size: %d",
							testIdx+1, sourceName, storeName, writerName, kvSize[0], kvSize[1], keyCommonPrefixSize)
						fn(suite)
						speed := float64(source.outputSize()) / elapsed.Seconds() / 1024 / 1024
						t.Logf("test %d: speed for %d bytes: %.2f MB/s", testIdx, source.outputSize(), speed)
					}
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
	iter, err := NewMergeKVIter(ctx, files, zeroOffsets, s.store, readBufSize, s.mergeIterHotspot)
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
