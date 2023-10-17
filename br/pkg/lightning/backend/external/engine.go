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
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Engine stored sorted key/value pairs in an external storage.
type Engine struct {
	storage         storage.ExternalStorage
	dataFiles       []string
	statsFiles      []string
	minKey          []byte
	maxKey          []byte
	splitKeys       [][]byte
	regionSplitSize int64
	bufPool         *membuf.Pool

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
	regionSplitSize int64,
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
		regionSplitSize:    regionSplitSize,
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
	}
}

func split[T any](in []T, groupNum int) [][]T {
	if len(in) == 0 {
		return nil
	}
	if groupNum <= 0 {
		groupNum = 1
	}
	ceil := (len(in) + groupNum - 1) / groupNum
	ret := make([][]T, 0, groupNum)
	l := len(in)
	for i := 0; i < l; i += ceil {
		if i+ceil > l {
			ret = append(ret, in[i:])
		} else {
			ret = append(ret, in[i:i+ceil])
		}
	}
	return ret
}

// LoadIngestData loads the data from the external storage to memory in [start,
// end) range, so local backend can ingest it. The used byte slice of ingest data
// are allocated from Engine.bufPool and must be released by
// MemoryIngestData.DecRef().
func (e *Engine) LoadIngestData(
	ctx context.Context,
	regionRanges []common.Range,
	outCh chan<- common.DataAndRange,
) error {
	// estimate we will open at most 1000 files, so if e.dataFiles is small we can
	// try to concurrently process ranges.
	concurrency := int(MergeSortOverlapThreshold) / len(e.dataFiles)
	concurrency = min(concurrency, 8)
	rangeGroups := split(regionRanges, concurrency)

	eg, egCtx := errgroup.WithContext(ctx)
	for _, ranges := range rangeGroups {
		ranges := ranges
		eg.Go(func() error {
			iter, err := e.createMergeIter(egCtx, ranges[0].Start)
			if err != nil {
				return errors.Trace(err)
			}
			defer iter.Close()

			if !iter.Next() {
				return iter.Error()
			}
			for _, r := range ranges {
				results, err := e.loadIngestData(egCtx, iter, r.Start, r.End)
				if err != nil {
					return errors.Trace(err)
				}
				for _, result := range results {
					select {
					case <-egCtx.Done():
						return egCtx.Err()
					case outCh <- result:
					}
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

func (e *Engine) buildIngestData(keys, values [][]byte, buf *membuf.Buffer) *MemoryIngestData {
	return &MemoryIngestData{
		keyAdapter:         e.keyAdapter,
		duplicateDetection: e.duplicateDetection,
		duplicateDB:        e.duplicateDB,
		dupDetectOpt:       e.dupDetectOpt,
		keys:               keys,
		values:             values,
		ts:                 e.ts,
		memBuf:             buf,
		refCnt:             atomic.NewInt64(0),
		importedKVSize:     e.importedKVSize,
		importedKVCount:    e.importedKVCount,
	}
}

// LargeRegionSplitDataThreshold is exposed for test.
var LargeRegionSplitDataThreshold = int(config.SplitRegionSize)

// loadIngestData loads the data from the external storage to memory in [start,
// end) range, and if the range is large enough, it will return multiple data.
// The input `iter` should be called Next() before calling this function.
func (e *Engine) loadIngestData(
	ctx context.Context,
	iter *MergeKVIter,
	start, end []byte,
) ([]common.DataAndRange, error) {
	if bytes.Equal(start, end) {
		return nil, errors.Errorf("start key and end key must not be the same: %s",
			hex.EncodeToString(start))
	}

	now := time.Now()
	keys := make([][]byte, 0, 1024)
	values := make([][]byte, 0, 1024)
	memBuf := e.bufPool.NewBuffer()
	cnt := 0
	size := 0
	largeRegion := e.regionSplitSize > 2*int64(config.SplitRegionSize)
	ret := make([]common.DataAndRange, 0, 1)
	curStart := start

	// there should be a key that just exceeds the end key in last loadIngestData
	// invocation.
	k, v := iter.Key(), iter.Value()
	if len(k) > 0 {
		keys = append(keys, memBuf.AddBytes(k))
		values = append(values, memBuf.AddBytes(v))
		cnt++
		size += len(k) + len(v)
	}

	for iter.Next() {
		k, v = iter.Key(), iter.Value()
		if bytes.Compare(k, start) < 0 {
			continue
		}
		if bytes.Compare(k, end) >= 0 {
			break
		}
		if largeRegion && size > LargeRegionSplitDataThreshold {
			curKey := slices.Clone(k)
			ret = append(ret, common.DataAndRange{
				Data:  e.buildIngestData(keys, values, memBuf),
				Range: common.Range{Start: curStart, End: curKey},
			})
			keys = make([][]byte, 0, 1024)
			values = make([][]byte, 0, 1024)
			size = 0
			curStart = curKey
		}

		keys = append(keys, memBuf.AddBytes(k))
		values = append(values, memBuf.AddBytes(v))
		cnt++
		size += len(k) + len(v)
	}
	if iter.Error() != nil {
		return nil, errors.Trace(iter.Error())
	}

	logutil.Logger(ctx).Info("load data from external storage",
		zap.Duration("cost time", time.Since(now)),
		zap.Int("iterated count", cnt))
	ret = append(ret, common.DataAndRange{
		Data:  e.buildIngestData(keys, values, memBuf),
		Range: common.Range{Start: curStart, End: end},
	})
	return ret, nil
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
			zap.Strings("dataFiles", e.dataFiles),
			zap.Strings("statsFiles", e.statsFiles))
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

// Close implements common.Engine.
func (e *Engine) Close() error { return nil }

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
	refCnt          *atomic.Int64
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

// IncRef implements IngestData.IncRef.
func (m *MemoryIngestData) IncRef() {
	m.refCnt.Inc()
}

// DecRef implements IngestData.DecRef.
func (m *MemoryIngestData) DecRef() {
	if m.refCnt.Dec() == 0 {
		m.memBuf.Destroy()
	}
}

// Finish implements IngestData.Finish.
func (m *MemoryIngestData) Finish(totalBytes, totalCount int64) {
	m.importedKVSize.Add(totalBytes)
	m.importedKVCount.Add(totalCount)

}
