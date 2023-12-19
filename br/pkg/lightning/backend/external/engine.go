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
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// during test on ks3, we found that we can open about 8000 connections to ks3,
// bigger than that, we might receive "connection reset by peer" error, and
// the read speed will be very slow, still investigating the reason.
// Also open too many connections will take many memory in kernel, and the
// test is based on k8s pod, not sure how it will behave on EC2.
// but, ks3 supporter says there's no such limit on connections.
// And our target for global sort is AWS s3, this default value might not fit well.
// TODO: adjust it according to cloud storage.
const maxCloudStorageConnections = 1000

type memKVsAndBuffers struct {
	mu           sync.Mutex
	keys         [][]byte
	values       [][]byte
	memKVBuffers []*membuf.Buffer
	size         int
}

// Engine stored sorted key/value pairs in an external storage.
type Engine struct {
	storage         storage.ExternalStorage
	dataFiles       []string
	statsFiles      []string
	startKey        []byte
	endKey          []byte
	splitKeys       [][]byte
	regionSplitSize int64
	bufPool         *membuf.Pool

	memKVsAndBuffers memKVsAndBuffers

	// checkHotspot is true means we will check hotspot file when using MergeKVIter.
	// if hotspot file is detected, we will use multiple readers to read data.
	// if it's false, MergeKVIter will read each file using 1 reader.
	// this flag also affects the strategy of loading data, either:
	// 	less load routine + check and read hotspot file concurrently (add-index uses this one)
	// 	more load routine + read each file using 1 reader (import-into uses this one)
	checkHotspot          bool
	mergerIterConcurrency int

	keyAdapter         common.KeyAdapter
	duplicateDetection bool
	duplicateDB        *pebble.DB
	dupDetectOpt       common.DupDetectOpt
	workerConcurrency  int
	ts                 uint64

	totalKVSize  int64
	totalKVCount int64

	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
}

const memLimit = 16 * 1024 * 1024 * 1024

// NewExternalEngine creates an (external) engine.
func NewExternalEngine(
	storage storage.ExternalStorage,
	dataFiles []string,
	statsFiles []string,
	startKey []byte,
	endKey []byte,
	splitKeys [][]byte,
	regionSplitSize int64,
	keyAdapter common.KeyAdapter,
	duplicateDetection bool,
	duplicateDB *pebble.DB,
	dupDetectOpt common.DupDetectOpt,
	workerConcurrency int,
	ts uint64,
	totalKVSize int64,
	totalKVCount int64,
	checkHotspot bool,
) common.Engine {
	memLimiter := membuf.NewLimiter(memLimit)
	return &Engine{
		storage:         storage,
		dataFiles:       dataFiles,
		statsFiles:      statsFiles,
		startKey:        startKey,
		endKey:          endKey,
		splitKeys:       splitKeys,
		regionSplitSize: regionSplitSize,
		bufPool: membuf.NewPool(
			membuf.WithBlockNum(0),
			membuf.WithPoolMemoryLimiter(memLimiter),
			membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc),
		),
		checkHotspot:       checkHotspot,
		keyAdapter:         keyAdapter,
		duplicateDetection: duplicateDetection,
		duplicateDB:        duplicateDB,
		dupDetectOpt:       dupDetectOpt,
		workerConcurrency:  workerConcurrency,
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

func (e *Engine) getAdjustedConcurrency() int {
	if e.checkHotspot {
		// estimate we will open at most 8000 files, so if e.dataFiles is small we can
		// try to concurrently process ranges.
		adjusted := maxCloudStorageConnections / len(e.dataFiles)
		if adjusted == 0 {
			return 1
		}
		return min(adjusted, 8)
	}
	adjusted := min(e.workerConcurrency, maxCloudStorageConnections/len(e.dataFiles))
	return max(adjusted, 1)
}

func getFilesReadConcurrency(
	ctx context.Context,
	storage storage.ExternalStorage,
	statsFiles []string,
	startKey, endKey []byte,
) ([]uint64, []uint64, error) {
	result := make([]uint64, len(statsFiles))
	startOffs, err := seekPropsOffsets(ctx, startKey, statsFiles, storage, false)
	if err != nil {
		return nil, nil, err
	}
	endOffs, err := seekPropsOffsets(ctx, endKey, statsFiles, storage, false)
	if err != nil {
		return nil, nil, err
	}
	for i := range statsFiles {
		result[i] = (endOffs[i] - startOffs[i]) / uint64(ConcurrentReaderBufferSizePerConc)
		result[i] = max(result[i], 1)
		logutil.Logger(ctx).Info("found hotspot file in getFilesReadConcurrency",
			zap.String("filename", statsFiles[i]),
			zap.Uint64("startOffset", startOffs[i]),
			zap.Uint64("endOffset", endOffs[i]),
			zap.Uint64("expected concurrency", result[i]),
		)
	}
	return result, startOffs, nil
}

func (e *Engine) loadBatchRegionData(ctx context.Context, startKey, endKey []byte, outCh chan<- common.DataAndRange) error {
	readAndSortRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read_and_sort")
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_and_sort")
	readRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read")
	readDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read")
	sortRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("sort")
	sortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("sort")

	readStart := time.Now()
	err := readAllData(
		ctx,
		e.storage,
		e.dataFiles,
		e.statsFiles,
		startKey,
		endKey,
		e.bufPool,
		&e.memKVsAndBuffers,
	)
	if err != nil {
		return err
	}
	readSecond := time.Since(readStart).Seconds()
	readDurHist.Observe(readSecond)
	logutil.Logger(ctx).Info("reading external storage in loadBatchRegionData",
		zap.Duration("cost time", time.Since(readStart)))
	sortStart := time.Now()
	oldSortyGor := sorty.MaxGor
	sorty.MaxGor = uint64(e.workerConcurrency * 2)
	sorty.Sort(len(e.memKVsAndBuffers.keys), func(i, k, r, s int) bool {
		if bytes.Compare(e.memKVsAndBuffers.keys[i], e.memKVsAndBuffers.keys[k]) < 0 { // strict comparator like < or >
			if r != s {
				e.memKVsAndBuffers.keys[r], e.memKVsAndBuffers.keys[s] = e.memKVsAndBuffers.keys[s], e.memKVsAndBuffers.keys[r]
				e.memKVsAndBuffers.values[r], e.memKVsAndBuffers.values[s] = e.memKVsAndBuffers.values[s], e.memKVsAndBuffers.values[r]
			}
			return true
		}
		return false
	})
	sorty.MaxGor = oldSortyGor
	sortSecond := time.Since(sortStart).Seconds()
	sortDurHist.Observe(sortSecond)
	logutil.Logger(ctx).Info("sorting in loadBatchRegionData",
		zap.Duration("cost time", time.Since(sortStart)))

	readAndSortSecond := time.Since(readStart).Seconds()
	readAndSortDurHist.Observe(readAndSortSecond)

	size := e.memKVsAndBuffers.size
	readAndSortRateHist.Observe(float64(size) / 1024.0 / 1024.0 / readAndSortSecond)
	readRateHist.Observe(float64(size) / 1024.0 / 1024.0 / readSecond)
	sortRateHist.Observe(float64(size) / 1024.0 / 1024.0 / sortSecond)

	data := e.buildIngestData(
		e.memKVsAndBuffers.keys,
		e.memKVsAndBuffers.values,
		e.memKVsAndBuffers.memKVBuffers,
	)

	// release the reference of e.memKVsAndBuffers
	e.memKVsAndBuffers.keys = nil
	e.memKVsAndBuffers.values = nil
	e.memKVsAndBuffers.memKVBuffers = nil

	sendFn := func(dr common.DataAndRange) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outCh <- dr:
		}
		return nil
	}
	return sendFn(common.DataAndRange{
		Data: data,
		Range: common.Range{
			Start: startKey,
			End:   endKey,
		},
	})
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
	// currently we assume the region size is 96MB and will download 96MB*40 = 3.8GB
	// data at once
	regionBatchSize := 40
	failpoint.Inject("LoadIngestDataBatchSize", func(val failpoint.Value) {
		regionBatchSize = val.(int)
	})
	for i := 0; i < len(regionRanges); i += regionBatchSize {
		err := e.loadBatchRegionData(ctx, regionRanges[i].Start, regionRanges[min(i+regionBatchSize, len(regionRanges))-1].End, outCh)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) buildIngestData(keys, values [][]byte, buf []*membuf.Buffer) *MemoryIngestData {
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

// createMergeIter is unused now.
// TODO(lance6716): check the performance of new design and remove it.
func (e *Engine) createMergeIter(ctx context.Context, start kv.Key) (*MergeKVIter, error) {
	logger := logutil.Logger(ctx)

	var offsets []uint64
	if len(e.statsFiles) == 0 {
		offsets = make([]uint64, len(e.dataFiles))
		logger.Info("no stats files",
			zap.String("startKey", hex.EncodeToString(start)))
	} else {
		offs, err := seekPropsOffsets(ctx, start, e.statsFiles, e.storage, e.checkHotspot)
		if err != nil {
			return nil, errors.Trace(err)
		}
		offsets = offs
		logger.Debug("seek props offsets",
			zap.Uint64s("offsets", offsets),
			zap.String("startKey", hex.EncodeToString(start)),
			zap.Strings("dataFiles", e.dataFiles),
			zap.Strings("statsFiles", e.statsFiles))
	}
	iter, err := NewMergeKVIter(
		ctx,
		e.dataFiles,
		offsets,
		e.storage,
		64*1024,
		e.checkHotspot,
		e.mergerIterConcurrency,
	)
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
func (e *Engine) GetKeyRange() (startKey []byte, endKey []byte, err error) {
	return e.startKey, e.endKey, nil
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
func (e *Engine) Close() error {
	if e.bufPool != nil {
		e.bufPool.Destroy()
		e.bufPool = nil
	}
	return nil
}

// Reset resets the memory buffer pool.
func (e *Engine) Reset() error {
	if e.bufPool != nil {
		e.bufPool.Destroy()
		memLimiter := membuf.NewLimiter(memLimit)
		e.bufPool = membuf.NewPool(
			membuf.WithPoolMemoryLimiter(memLimiter),
		)
	}
	return nil
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

	memBuf          []*membuf.Buffer
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

// ReleaseBuf implements ForwardIter.
func (m *memoryDataIter) ReleaseBuf() {}

type memoryDataDupDetectIter struct {
	iter           *memoryDataIter
	dupDetector    *common.DupDetector
	err            error
	curKey, curVal []byte
	buf            *membuf.Buffer
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
	return m.buf.AddBytes(m.curKey)
}

// Value implements ForwardIter.
func (m *memoryDataDupDetectIter) Value() []byte {
	return m.buf.AddBytes(m.curVal)
}

// Close implements ForwardIter.
func (m *memoryDataDupDetectIter) Close() error {
	m.buf.Destroy()
	return m.dupDetector.Close()
}

// Error implements ForwardIter.
func (m *memoryDataDupDetectIter) Error() error {
	return m.err
}

// ReleaseBuf implements ForwardIter.
func (m *memoryDataDupDetectIter) ReleaseBuf() {
	m.buf.Reset()
}

// NewIter implements IngestData.NewIter.
func (m *MemoryIngestData) NewIter(
	ctx context.Context,
	lowerBound, upperBound []byte,
	bufPool *membuf.Pool,
) common.ForwardIter {
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
		buf:         bufPool.NewBuffer(),
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
		for _, b := range m.memBuf {
			b.Destroy()
		}
	}
}

// Finish implements IngestData.Finish.
func (m *MemoryIngestData) Finish(totalBytes, totalCount int64) {
	m.importedKVSize.Add(totalBytes)
	m.importedKVCount.Add(totalCount)

}
