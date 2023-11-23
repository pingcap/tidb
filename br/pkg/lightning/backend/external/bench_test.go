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
	b, err := storage.ParseBackend(*testingStorageURI, nil)
	intest.Assert(err == nil)
	s, err := storage.New(context.Background(), b, nil)
	intest.Assert(err == nil)
	return s
}

type kvSource interface {
	next() (key, value []byte, handle kv.Handle)
	outputSize() int
}

type ascendingKeySource struct {
	keySize, valueSize int
	count              int
	kvChan             chan [2][]byte
	curKey             []byte
	totalSize          int
}

func newAscendingKeySource(keySize, valueSize, count int) *ascendingKeySource {
	s := &ascendingKeySource{keySize: keySize, valueSize: valueSize, count: count}
	s.curKey = make([]byte, keySize)
	s.kvChan = make(chan [2][]byte, 100)
	s.run()
	return s
}

func (s *ascendingKeySource) run() {
	go func() {
		defer close(s.kvChan)
		for i := 0; i < s.count; i++ {
			for j := len(s.curKey) - 1; j >= 0; j-- {
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
	buf := make([]byte, s.memoryLimit)
	offset := 0
	flush := func(w storage.ExternalFileWriter) {
		n, err := w.Write(ctx, buf[:offset])
		intest.Assert(err == nil)
		intest.Assert(offset == n)
		offset = 0
	}

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer, err := s.store.Create(ctx, "test/plain_file", nil)
	intest.Assert(err == nil)
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
	intest.Assert(err == nil)
	if s.afterWriterClose != nil {
		s.afterWriterClose()
	}
}

func writeExternalFile(s *writeTestSuite) {
	ctx := context.Background()
	builder := NewWriterBuilder().
		SetMemorySizeLimit(uint64(s.memoryLimit))

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer := builder.Build(s.store, "test/external", "writerID")
	key, val, h := s.source.next()
	for key != nil {
		err := writer.WriteRow(ctx, key, val, h)
		intest.Assert(err == nil)
		key, val, h = s.source.next()
	}
	if s.beforeWriterClose != nil {
		s.beforeWriterClose()
	}
	err := writer.Close(ctx)
	intest.Assert(err == nil)
	if s.afterWriterClose != nil {
		s.afterWriterClose()
	}
}

func writeExternalOneFile(s *writeTestSuite) {
	ctx := context.Background()
	builder := NewWriterBuilder().
		SetMemorySizeLimit(uint64(s.memoryLimit))

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer := builder.BuildOneFile(
		s.store, "test/external", "writerID")
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
	err := writer.Close(ctx)
	intest.AssertNoError(err)
	if s.afterWriterClose != nil {
		s.afterWriterClose()
	}
}

func TestCompareWriter(t *testing.T) {
	store := openTestingStorage(t)
	sourceKVNum := 10000000
	source := newAscendingKeySource(20, 100, sourceKVNum)
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
		intest.Assert(err == nil)
		err = pprof.StartCPUProfile(file)
		intest.Assert(err == nil)
		now = time.Now()
	}
	beforeClose := func() {
		file, err = os.Create(fmt.Sprintf("heap-profile-%d.prof", fileIdx))
		intest.Assert(err == nil)
		// check heap profile to see the memory usage is expected
		err = pprof.WriteHeapProfile(file)
		intest.Assert(err == nil)
	}
	afterClose := func() {
		elapsed = time.Since(now)
		pprof.StopCPUProfile()
	}

	suite := &writeTestSuite{
		store:              store,
		source:             source,
		memoryLimit:        memoryLimit,
		beforeCreateWriter: beforeTest,
		beforeWriterClose:  beforeClose,
		afterWriterClose:   afterClose,
	}

	writePlainFile(suite)
	baseSpeed := float64(source.outputSize()) / elapsed.Seconds() / 1024 / 1024
	t.Logf("base speed for %d bytes: %.2f MB/s", source.outputSize(), baseSpeed)

	suite.source = newAscendingKeySource(20, 100, sourceKVNum)
	writeExternalFile(suite)
	writerSpeed := float64(source.outputSize()) / elapsed.Seconds() / 1024 / 1024
	t.Logf("writer speed for %d bytes: %.2f MB/s", source.outputSize(), writerSpeed)

	suite.source = newAscendingKeySource(20, 100, sourceKVNum)
	writeExternalOneFile(suite)
	writerSpeed = float64(source.outputSize()) / elapsed.Seconds() / 1024 / 1024
	t.Logf("one file writer speed for %d bytes: %.2f MB/s", source.outputSize(), writerSpeed)
}

type readTestSuite struct {
	store              storage.ExternalStorage
	totalKVCnt         int
	concurrency        int
	memoryLimit        int
	beforeCreateReader func()
	beforeReaderClose  func()
	afterReaderClose   func()
}

func readFileSequential(s *readTestSuite) {
	ctx := context.Background()
	files, _, err := GetAllFileNames(ctx, s.store, "evenly_distributed")
	intest.Assert(err == nil)

	buf := make([]byte, s.memoryLimit)
	if s.beforeCreateReader != nil {
		s.beforeCreateReader()
	}
	for i, file := range files {
		reader, err := s.store.Open(ctx, file, nil)
		intest.Assert(err == nil)
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
		intest.Assert(err == nil)
	}
	if s.afterReaderClose != nil {
		s.afterReaderClose()
	}
}

func readFileConcurrently(s *readTestSuite) {
	ctx := context.Background()
	files, _, err := GetAllFileNames(ctx, s.store, "evenly_distributed")
	intest.Assert(err == nil)

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
			intest.Assert(err == nil)
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
			intest.Assert(err == nil)
			return nil
		})
	}
	err = eg.Wait()
	intest.Assert(err == nil)
	if s.afterReaderClose != nil {
		s.afterReaderClose()
	}
}

func createEvenlyDistributedFiles(
	t *testing.T,
	fileSize, fileCount int,
) (storage.ExternalStorage, int) {
	store := openTestingStorage(t)
	ctx := context.Background()

	files, statFiles, err := GetAllFileNames(ctx, store, "evenly_distributed")
	intest.Assert(err == nil)
	err = store.DeleteFiles(ctx, files)
	intest.Assert(err == nil)
	err = store.DeleteFiles(ctx, statFiles)
	intest.Assert(err == nil)

	value := make([]byte, 100)
	kvCnt := 0
	for i := 0; i < fileCount; i++ {
		builder := NewWriterBuilder().
			SetBlockSize(10 * 1024 * 1024).
			SetMemorySizeLimit(uint64(float64(fileSize) * 1.1))
		writer := builder.Build(
			store,
			"evenly_distributed",
			fmt.Sprintf("%d", i),
		)

		keyIdx := i
		totalSize := 0
		for totalSize < fileSize {
			key := fmt.Sprintf("key_%09d", keyIdx)
			err := writer.WriteRow(ctx, []byte(key), value, nil)
			intest.Assert(err == nil)
			keyIdx += fileCount
			totalSize += len(key) + len(value)
			kvCnt++
		}
		err := writer.Close(ctx)
		intest.Assert(err == nil)
	}
	return store, kvCnt
}

func readMergeIter(s *readTestSuite) {
	ctx := context.Background()
	files, _, err := GetAllFileNames(ctx, s.store, "evenly_distributed")
	intest.Assert(err == nil)

	if s.beforeCreateReader != nil {
		s.beforeCreateReader()
	}

	readBufSize := s.memoryLimit / len(files)
	zeroOffsets := make([]uint64, len(files))
	iter, err := NewMergeKVIter(ctx, files, zeroOffsets, s.store, readBufSize, false)
	intest.Assert(err == nil)

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
	intest.Assert(err == nil)
	if s.afterReaderClose != nil {
		s.afterReaderClose()
	}
}

func TestCompareReader(t *testing.T) {
	fileSize := 50 * 1024 * 1024
	fileCnt := 24
	store, kvCnt := createEvenlyDistributedFiles(t, fileSize, fileCnt)
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
		intest.Assert(err == nil)
		err = pprof.StartCPUProfile(file)
		intest.Assert(err == nil)
		now = time.Now()
	}
	beforeClose := func() {
		file, err = os.Create(fmt.Sprintf("heap-profile-%d.prof", fileIdx))
		intest.Assert(err == nil)
		// check heap profile to see the memory usage is expected
		err = pprof.WriteHeapProfile(file)
		intest.Assert(err == nil)
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
