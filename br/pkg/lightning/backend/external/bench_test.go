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
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
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

type testSuite struct {
	t                  *testing.T
	store              storage.ExternalStorage
	source             kvSource
	memoryLimit        int
	beforeCreateWriter func()
	beforeWriterClose  func()
	afterWriterClose   func()
}

func writePlainFile(s *testSuite) {
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

func writeExternalFile(s *testSuite) {
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

func writeExternalOneFile(s *testSuite) {
	ctx := context.Background()
	builder := NewWriterBuilder().
		SetMemorySizeLimit(uint64(s.memoryLimit))

	if s.beforeCreateWriter != nil {
		s.beforeCreateWriter()
	}
	writer := builder.BuildOneFile(
		s.store, "test/external", "writerID", false)
	_ = writer.Init(ctx, 20*1024*1024)
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

func TestCompare(t *testing.T) {
	store := openTestingStorage(t)
	source := newAscendingKeySource(20, 100, 10000000)
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

	suite := &testSuite{
		t:                  t,
		store:              store,
		source:             source,
		memoryLimit:        memoryLimit,
		beforeCreateWriter: beforeTest,
		beforeWriterClose:  beforeClose,
		afterWriterClose:   afterClose,
	}

	// writePlainFile(suite)
	// baseSpeed := float64(source.outputSize()) / elapsed.Seconds() / 1024 / 1024
	// t.Logf("base speed for %d bytes: %.2f MB/s", source.outputSize(), baseSpeed)

	// suite.source = newAscendingKeySource(20, 100, 10000000)
	// writeExternalFile(suite)
	// writerSpeed := float64(source.outputSize()) / elapsed.Seconds() / 1024 / 1024
	// t.Logf("writer speed for %d bytes: %.2f MB/s", source.outputSize(), writerSpeed)

	// suite.source = newAscendingKeySource(20, 100, 10000000)
	writeExternalOneFile(suite)
	writerSpeed := float64(source.outputSize()) / elapsed.Seconds() / 1024 / 1024
	t.Logf("one file writer speed for %d bytes: %.2f MB/s", source.outputSize(), writerSpeed)
}
