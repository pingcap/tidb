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
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Engine stored sorted key/value pairs in an external storage.
type Engine struct {
	storage    storage.ExternalStorage
	dataFiles  []string
	statsFiles []string
	minKey     []byte
	maxKey     []byte
	splitKeys  [][]byte
	bufPool    *membuf.Pool

	mu   sync.Mutex
	iter map[int]*MergeKVIter

	keyAdapter         common.KeyAdapter
	duplicateDetection bool
	duplicateDB        *pebble.DB
	dupDetectOpt       common.DupDetectOpt
	ts                 uint64

	totalKVSize  int64
	totalKVCount int64

	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
}

// NewExternalEngine creates an (external) engine.
func NewExternalEngine(
	storage storage.ExternalStorage,
	dataFiles []string,
	statsFiles []string,
	minKey []byte,
	maxKey []byte,
	splitKeys [][]byte,
	keyAdapter common.KeyAdapter,
	duplicateDetection bool,
	duplicateDB *pebble.DB,
	dupDetectOpt common.DupDetectOpt,
	ts uint64,
	totalKVSize int64,
	totalKVCount int64,
) common.Engine {
	return &Engine{
		storage:            storage,
		dataFiles:          dataFiles,
		statsFiles:         statsFiles,
		minKey:             minKey,
		maxKey:             maxKey,
		splitKeys:          splitKeys,
		bufPool:            membuf.NewPool(),
		keyAdapter:         keyAdapter,
		duplicateDetection: duplicateDetection,
		duplicateDB:        duplicateDB,
		dupDetectOpt:       dupDetectOpt,
		ts:                 ts,
		totalKVSize:        totalKVSize,
		totalKVCount:       totalKVCount,
		importedKVSize:     atomic.NewInt64(0),
		importedKVCount:    atomic.NewInt64(0),
		iter:               map[int]*MergeKVIter{},
	}
}

// LoadIngestData loads the data from the external storage to memory in [start,
// end) range, so local backend can ingest it. The used byte slice of ingest data
// are allocated from Engine.bufPool and must be released by
// MemoryIngestData.Finish(). For external.Engine, LoadIngestData must be called
// with strictly increasing start / end key.
func (e *Engine) LoadIngestData(ctx context.Context, start, end []byte, iterOffset int) (common.IngestData, error) {
	if bytes.Equal(start, end) {
		return nil, errors.Errorf("start key and end key must not be the same: %s",
			hex.EncodeToString(start))
	}

	now := time.Now()
	keys := make([][]byte, 0, 1024)
	values := make([][]byte, 0, 1024)
	memBuf := e.bufPool.NewBuffer()

	var iter *MergeKVIter

	e.mu.Lock()
	it, ok := e.iter[iterOffset]
	if ok {
		iter = it
		// there should be a key that just exceeds the end key in last LoadIngestData
		// invocation.
		k, v := it.Key(), it.Value()
		keys = append(keys, memBuf.AddBytes(k))
		values = append(values, memBuf.AddBytes(v))
	} else {
		it, err := e.createMergeIter(ctx, start)
		if err != nil {
			e.mu.Unlock()
			return nil, errors.Trace(err)
		}
		e.iter[iterOffset] = it
		iter = it
	}
	e.mu.Unlock()

	cnt := 0
	for iter.Next() {
		cnt++
		k, v := iter.Key(), iter.Value()
		if bytes.Compare(k, start) < 0 {
			continue
		}
		if bytes.Compare(k, end) >= 0 {
			break
		}
		keys = append(keys, memBuf.AddBytes(k))
		values = append(values, memBuf.AddBytes(v))
	}
	if iter.Error() != nil {
		return nil, errors.Trace(iter.Error())
	}

	logutil.Logger(ctx).Info("load data from external storage",
		zap.Duration("cost time", time.Since(now)),
		zap.Int("iterated count", cnt))
	return &MemoryIngestData{
		keyAdapter:         e.keyAdapter,
		duplicateDetection: e.duplicateDetection,
		duplicateDB:        e.duplicateDB,
		dupDetectOpt:       e.dupDetectOpt,
		keys:               keys,
		values:             values,
		ts:                 e.ts,
		memBuf:             memBuf,
		importedKVSize:     e.importedKVSize,
		importedKVCount:    e.importedKVCount,
	}, nil
}

func (e *Engine) createMergeIter(ctx context.Context, start kv.Key) (*MergeKVIter, error) {
	logger := logutil.Logger(ctx)

	var offsets []uint64
	if len(e.statsFiles) == 0 {
		offsets = make([]uint64, len(e.dataFiles))
		logger.Info("no stats files",
			zap.String("startKey", hex.EncodeToString(start)))
	} else {
		offs, err := seekPropsOffsets(ctx, start, e.statsFiles, e.storage)
		if err != nil {
			return nil, errors.Trace(err)
		}
		offsets = offs
		logger.Info("seek props offsets",
			zap.Uint64s("offsets", offsets),
			zap.String("startKey", hex.EncodeToString(start)),
			zap.Strings("dataFiles", prettyFileNames(e.dataFiles)),
			zap.Strings("statsFiles", prettyFileNames(e.statsFiles)))
	}
	iter, err := NewMergeKVIter(ctx, e.dataFiles, offsets, e.storage, 64*1024)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iter, nil
}

// KVStatistics returns the total kv size and total kv count.
func (e *Engine) KVStatistics() (totalKVSize int64, totalKVCount int64) {
	return e.totalKVSize, e.totalKVCount
}

// ImportedStatistics returns the imported kv size and imported kv count.
func (e *Engine) ImportedStatistics() (importedSize int64, importedKVCount int64) {
	return e.importedKVSize.Load(), e.importedKVCount.Load()
}

// ID is the identifier of an engine.
func (e *Engine) ID() string {
	return "external"
}

// GetKeyRange implements common.Engine.
func (e *Engine) GetKeyRange() (firstKey []byte, lastKey []byte, err error) {
	return e.minKey, e.maxKey, nil
}

// SplitRanges split the ranges by split keys provided by external engine.
func (e *Engine) SplitRanges(
	startKey, endKey []byte,
	_, _ int64,
	_ log.Logger,
) ([]common.Range, error) {
	splitKeys := e.splitKeys
	ranges := make([]common.Range, 0, len(splitKeys)+1)
	ranges = append(ranges, common.Range{Start: startKey})
	for i := 0; i < len(splitKeys); i++ {
		ranges[len(ranges)-1].End = splitKeys[i]
		var endK []byte
		if i < len(splitKeys)-1 {
			endK = splitKeys[i+1]
		}
		ranges = append(ranges, common.Range{Start: splitKeys[i], End: endK})
	}
	ranges[len(ranges)-1].End = endKey
	return ranges, nil
}

// Close releases the resources of the engine.
func (e *Engine) Close() error {
	if len(e.iter) == 0 {
		return nil
	}

	var firstErr error

	for _, it := range e.iter {
		if err := it.Close(); err != nil {
			if firstErr != nil {
				firstErr = errors.Trace(err)
			}
			continue
		}
	}

	return firstErr
}

// MemoryIngestData is the in-memory implementation of IngestData.
type MemoryIngestData struct {
	keyAdapter         common.KeyAdapter
	duplicateDetection bool
	duplicateDB        *pebble.DB
	dupDetectOpt       common.DupDetectOpt

	keys   [][]byte
	values [][]byte
	ts     uint64

	memBuf          *membuf.Buffer
	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
}

var _ common.IngestData = (*MemoryIngestData)(nil)

func (m *MemoryIngestData) firstAndLastKeyIndex(lowerBound, upperBound []byte) (int, int) {
	firstKeyIdx := 0
	if len(lowerBound) > 0 {
		lowerBound = m.keyAdapter.Encode(nil, lowerBound, common.MinRowID)
		firstKeyIdx = sort.Search(len(m.keys), func(i int) bool {
			return bytes.Compare(lowerBound, m.keys[i]) <= 0
		})
		if firstKeyIdx == len(m.keys) {
			return -1, -1
		}
	}

	lastKeyIdx := len(m.keys) - 1
	if len(upperBound) > 0 {
		upperBound = m.keyAdapter.Encode(nil, upperBound, common.MinRowID)
		i := sort.Search(len(m.keys), func(i int) bool {
			reverseIdx := len(m.keys) - 1 - i
			return bytes.Compare(upperBound, m.keys[reverseIdx]) > 0
		})
		if i == len(m.keys) {
			// should not happen
			return -1, -1
		}
		lastKeyIdx = len(m.keys) - 1 - i
	}
	return firstKeyIdx, lastKeyIdx
}

// GetFirstAndLastKey implements IngestData.GetFirstAndLastKey.
func (m *MemoryIngestData) GetFirstAndLastKey(lowerBound, upperBound []byte) ([]byte, []byte, error) {
	firstKeyIdx, lastKeyIdx := m.firstAndLastKeyIndex(lowerBound, upperBound)
	if firstKeyIdx < 0 || firstKeyIdx > lastKeyIdx {
		return nil, nil, nil
	}
	firstKey, err := m.keyAdapter.Decode(nil, m.keys[firstKeyIdx])
	if err != nil {
		return nil, nil, err
	}
	lastKey, err := m.keyAdapter.Decode(nil, m.keys[lastKeyIdx])
	if err != nil {
		return nil, nil, err
	}
	return firstKey, lastKey, nil
}

type memoryDataIter struct {
	keys   [][]byte
	values [][]byte

	firstKeyIdx int
	lastKeyIdx  int
	curIdx      int
}

// First implements ForwardIter.
func (m *memoryDataIter) First() bool {
	if m.firstKeyIdx < 0 {
		return false
	}
	m.curIdx = m.firstKeyIdx
	return true
}

// Valid implements ForwardIter.
func (m *memoryDataIter) Valid() bool {
	return m.firstKeyIdx <= m.curIdx && m.curIdx <= m.lastKeyIdx
}

// Next implements ForwardIter.
func (m *memoryDataIter) Next() bool {
	m.curIdx++
	return m.Valid()
}

// Key implements ForwardIter.
func (m *memoryDataIter) Key() []byte {
	return m.keys[m.curIdx]
}

// Value implements ForwardIter.
func (m *memoryDataIter) Value() []byte {
	return m.values[m.curIdx]
}

// Close implements ForwardIter.
func (m *memoryDataIter) Close() error {
	return nil
}

// Error implements ForwardIter.
func (m *memoryDataIter) Error() error {
	return nil
}

type memoryDataDupDetectIter struct {
	iter           *memoryDataIter
	dupDetector    *common.DupDetector
	err            error
	curKey, curVal []byte
}

// First implements ForwardIter.
func (m *memoryDataDupDetectIter) First() bool {
	if m.err != nil || !m.iter.First() {
		return false
	}
	m.curKey, m.curVal, m.err = m.dupDetector.Init(m.iter)
	return m.Valid()
}

// Valid implements ForwardIter.
func (m *memoryDataDupDetectIter) Valid() bool {
	return m.err == nil && m.iter.Valid()
}

// Next implements ForwardIter.
func (m *memoryDataDupDetectIter) Next() bool {
	if m.err != nil {
		return false
	}
	key, val, ok, err := m.dupDetector.Next(m.iter)
	if err != nil {
		m.err = err
		return false
	}
	if !ok {
		return false
	}
	m.curKey, m.curVal = key, val
	return true
}

// Key implements ForwardIter.
func (m *memoryDataDupDetectIter) Key() []byte {
	return m.curKey
}

// Value implements ForwardIter.
func (m *memoryDataDupDetectIter) Value() []byte {
	return m.curVal
}

// Close implements ForwardIter.
func (m *memoryDataDupDetectIter) Close() error {
	return m.dupDetector.Close()
}

// Error implements ForwardIter.
func (m *memoryDataDupDetectIter) Error() error {
	return m.err
}

// NewIter implements IngestData.NewIter.
func (m *MemoryIngestData) NewIter(ctx context.Context, lowerBound, upperBound []byte) common.ForwardIter {
	firstKeyIdx, lastKeyIdx := m.firstAndLastKeyIndex(lowerBound, upperBound)
	iter := &memoryDataIter{
		keys:        m.keys,
		values:      m.values,
		firstKeyIdx: firstKeyIdx,
		lastKeyIdx:  lastKeyIdx,
	}
	if !m.duplicateDetection {
		return iter
	}
	logger := log.FromContext(ctx)
	detector := common.NewDupDetector(m.keyAdapter, m.duplicateDB.NewBatch(), logger, m.dupDetectOpt)
	return &memoryDataDupDetectIter{
		iter:        iter,
		dupDetector: detector,
	}
}

// GetTS implements IngestData.GetTS.
func (m *MemoryIngestData) GetTS() uint64 {
	return m.ts
}

// Finish implements IngestData.Finish.
func (m *MemoryIngestData) Finish(totalBytes, totalCount int64) {
	m.importedKVSize.Add(totalBytes)
	m.importedKVCount.Add(totalCount)
	m.memBuf.Destroy()
}
