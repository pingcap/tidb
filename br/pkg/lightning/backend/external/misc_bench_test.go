// Copyright 2024 PingCAP, Inc.
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
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/felixge/fgprof"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func openTestingStorage(t *testing.T) storage.ExternalStorage {
	if *testingStorageURI == "" {
		t.Skip("testingStorageURI is not set")
	}
	s, err := storage.NewFromURL(context.Background(), *testingStorageURI)
	require.NoError(t, err)
	t.Cleanup(s.Close)
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
			// try to use most left bytes to alternate the key
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

type profiler struct {
	onAndOffCPUProf bool
	peakMemProf     bool

	caseIdx int

	onAndOffCPUProfCloser func() error
	heapProfDoneCh        chan struct{}
	heapProfWg            *sync.WaitGroup
}

func newProfiler(onAndOffCPUProf, peakMemProf bool) *profiler {
	return &profiler{
		onAndOffCPUProf: onAndOffCPUProf,
		peakMemProf:     peakMemProf,
	}
}

func (p *profiler) beforeTest() {
	p.caseIdx++
	if p.onAndOffCPUProf {
		fileCPU, err := os.Create(fmt.Sprintf("on-and-off-cpu-%d.prof", p.caseIdx))
		intest.AssertNoError(err)
		p.onAndOffCPUProfCloser = fgprof.Start(fileCPU, fgprof.FormatPprof)
	}
	if p.peakMemProf {
		fileHeap := fmt.Sprintf("heap-%d.prof", p.caseIdx)
		p.heapProfDoneCh, p.heapProfWg = recordHeapForMaxInUse(fileHeap)
	}
}

func (p *profiler) afterTest() {
	if p.onAndOffCPUProf {
		err := p.onAndOffCPUProfCloser()
		intest.AssertNoError(err)
	}
	if p.peakMemProf {
		close(p.heapProfDoneCh)
		p.heapProfWg.Wait()
	}
}

func recordHeapForMaxInUse(filename string) (chan struct{}, *sync.WaitGroup) {
	doneCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	maxInUse := uint64(0)
	go func() {
		defer wg.Done()

		var m runtime.MemStats
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-doneCh:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&m)
				if m.HeapInuse <= maxInUse {
					continue
				}
				maxInUse = m.HeapInuse
				file, err := os.Create(filename)
				intest.AssertNoError(err)
				err = pprof.WriteHeapProfile(file)
				intest.AssertNoError(err)
				err = file.Close()
				intest.AssertNoError(err)
			}
		}
	}()
	return doneCh, &wg
}
