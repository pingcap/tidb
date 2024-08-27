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
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
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
	mu     sync.Mutex
	keys   [][]byte
	values [][]byte
	// memKVBuffers contains two types of buffer, first half are used for small block
	// buffer, second half are used for large one.
	memKVBuffers []*membuf.Buffer
	size         int
	droppedSize  int

	// temporary fields to store KVs to reduce slice allocations.
	keysPerFile        [][][]byte
	valuesPerFile      [][][]byte
	droppedSizePerFile []int
}

func (b *memKVsAndBuffers) build(ctx context.Context) {
	sumKVCnt := 0
	for _, keys := range b.keysPerFile {
		sumKVCnt += len(keys)
	}
	b.droppedSize = 0
	for _, size := range b.droppedSizePerFile {
		b.droppedSize += size
	}
	b.droppedSizePerFile = nil

	logutil.Logger(ctx).Info("building memKVsAndBuffers",
		zap.Int("sumKVCnt", sumKVCnt),
		zap.Int("droppedSize", b.droppedSize))

	b.keys = make([][]byte, 0, sumKVCnt)
	b.values = make([][]byte, 0, sumKVCnt)
	for i := range b.keysPerFile {
		b.keys = append(b.keys, b.keysPerFile[i]...)
		b.keysPerFile[i] = nil
		b.values = append(b.values, b.valuesPerFile[i]...)
		b.valuesPerFile[i] = nil
	}
	b.keysPerFile = nil
	b.valuesPerFile = nil
}

// Engine stored sorted key/value pairs in an external storage.
type Engine struct {
	storage           storage.ExternalStorage
	dataFiles         []string
	statsFiles        []string
	startKey          []byte
	endKey            []byte
	splitKeys         [][]byte
	regionSplitSize   int64
	smallBlockBufPool *membuf.Pool
	largeBlockBufPool *membuf.Pool

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

const (
	memLimit       = 12 * units.GiB
	smallBlockSize = units.MiB
)

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
		smallBlockBufPool: membuf.NewPool(
			membuf.WithBlockNum(0),
			membuf.WithPoolMemoryLimiter(memLimiter),
			membuf.WithBlockSize(smallBlockSize),
		),
		largeBlockBufPool: membuf.NewPool(
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
	offsets, err := seekPropsOffsets(ctx, []kv.Key{startKey, endKey}, statsFiles, storage)
	if err != nil {
		return nil, nil, err
	}
	startOffs, endOffs := offsets[0], offsets[1]
	for i := range statsFiles {
		expectedConc := (endOffs[i] - startOffs[i]) / uint64(ConcurrentReaderBufferSizePerConc)
		// let the stat internals cover the [startKey, endKey) since seekPropsOffsets
		// always return an offset that is less than or equal to the key.
		expectedConc += 1
		// readAllData will enable concurrent read and use large buffer if result[i] > 1
		// when expectedConc < readAllDataConcThreshold, we don't use concurrent read to
		// reduce overhead
		if expectedConc >= readAllDataConcThreshold {
			result[i] = expectedConc
		} else {
			result[i] = 1
		}
		// only log for files with expected concurrency > 1, to avoid too many logs
		if expectedConc > 1 {
			logutil.Logger(ctx).Info("found hotspot file in getFilesReadConcurrency",
				zap.String("filename", statsFiles[i]),
				zap.Uint64("startOffset", startOffs[i]),
				zap.Uint64("endOffset", endOffs[i]),
				zap.Uint64("expectedConc", expectedConc),
				zap.Uint64("concurrency", result[i]),
			)
		}
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
	readDtStartKey := e.keyAdapter.Encode(nil, startKey, common.MinRowID)
	readDtEndKey := e.keyAdapter.Encode(nil, endKey, common.MinRowID)
	err := readAllData(
		ctx,
		e.storage,
		e.dataFiles,
		e.statsFiles,
		readDtStartKey,
		readDtEndKey,
		e.smallBlockBufPool,
		e.largeBlockBufPool,
		&e.memKVsAndBuffers,
	)
	if err != nil {
		return err
	}
	e.memKVsAndBuffers.build(ctx)

	readSecond := time.Since(readStart).Seconds()
	readDurHist.Observe(readSecond)
	logutil.Logger(ctx).Info("reading external storage in loadBatchRegionData",
		zap.Duration("cost time", time.Since(readStart)),
		zap.Int("droppedSize", e.memKVsAndBuffers.droppedSize))

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
	e.memKVsAndBuffers.size = 0

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
	// try to make every worker busy for each batch
	regionBatchSize := e.workerConcurrency
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
	if _, ok := e.keyAdapter.(common.NoopKeyAdapter); ok {
		return e.startKey, e.endKey, nil
	}

	// when duplicate detection feature is enabled, the end key comes from
	// DupDetectKeyAdapter.Encode or Key.Next(). We try to decode it and check the
	// error.

	start, err := e.keyAdapter.Decode(nil, e.startKey)
	if err != nil {
		return nil, nil, err
	}
	end, err := e.keyAdapter.Decode(nil, e.endKey)
	if err == nil {
		return start, end, nil
	}
	// handle the case that end key is from Key.Next()
	if e.endKey[len(e.endKey)-1] != 0 {
		return nil, nil, err
	}
	endEncoded := e.endKey[:len(e.endKey)-1]
	end, err = e.keyAdapter.Decode(nil, endEncoded)
	if err != nil {
		return nil, nil, err
	}
	return start, kv.Key(end).Next(), nil
}

// SplitRanges split the ranges by split keys provided by external engine.
func (e *Engine) SplitRanges(
	startKey, endKey []byte,
	_, _ int64,
	_ log.Logger,
) ([]common.Range, error) {
	splitKeys := e.splitKeys
	for i, k := range e.splitKeys {
		var err error
		splitKeys[i], err = e.keyAdapter.Decode(nil, k)
		if err != nil {
			return nil, err
		}
	}
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
	if e.smallBlockBufPool != nil {
		e.smallBlockBufPool.Destroy()
		e.smallBlockBufPool = nil
	}
	if e.largeBlockBufPool != nil {
		e.largeBlockBufPool.Destroy()
		e.largeBlockBufPool = nil
	}
	e.storage.Close()
	return nil
}

// Reset resets the memory buffer pool.
func (e *Engine) Reset() error {
	memLimiter := membuf.NewLimiter(memLimit)
	if e.smallBlockBufPool != nil {
		e.smallBlockBufPool.Destroy()
		e.smallBlockBufPool = membuf.NewPool(
			membuf.WithBlockNum(0),
			membuf.WithPoolMemoryLimiter(memLimiter),
			membuf.WithBlockSize(smallBlockSize),
		)
	}
	if e.largeBlockBufPool != nil {
		e.largeBlockBufPool.Destroy()
		e.largeBlockBufPool = membuf.NewPool(
			membuf.WithBlockNum(0),
			membuf.WithPoolMemoryLimiter(memLimiter),
			membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc),
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
		m.keys = nil
		m.values = nil
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
