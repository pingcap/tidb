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
	"github.com/docker/go-units"
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// writeStepMemShareCount defines the number of shares of memory per job worker.
// For each job worker, the memory it can use is determined by cpu:mem ratio, say
// a 16c32G machine, each worker can use 2G memory.
// And for the memory corresponding to each job worker, we divide into below and
// total 6.5 shares:
//   - one share used by HTTP and GRPC buf, such as loadBatchRegionData, write TiKV
//   - one share used by loadBatchRegionData to store loaded data batch A
//   - one share used by generateAndSendJob for handle loaded data batch B
//   - one share used by the active job on job worker
//   - 2.5 share for others, and burst allocation to avoid OOM
//
// the share size 'SS' determines the max data size 'RangeS' for a split-range
// which is split out by RangeSplitter.
// split-range is intersected with region to generate range job which is handled
// by range job worker, and each range job corresponding to one ingested SST on TiKV.
// our goal here is to load as many data as possible to make all range job workers
// fully parallelized, while minimizing the number of SSTs (too many SST file, say
// 500K, will cause TiKV slow down when ingest), i.e. to make RangeS larger, and
// also try to make the SST be more even, so we calculate RangeS by:
//   - RS = region size
//   - let TempRangeS = SS
//   - if TempRangeS < RS, RangeS = RS / ceil(RS / TempRangeS) + 1,
//     trailing 1 is for RS divided by odd number.
//   - else RangeS = floor(TempRangeS / RS) * RS.
//
// RangeS for different region size and cpu:mem ratio, the number in parentheses
// is the number of SST files per region:
//
//	|   RS  | RangeS        | RangeS        |
//	|       | cpu:mem=1:1.7 | cpu:mem=1:3.5 |
//	|-------|---------------|---------------|
//	|   96M |       192M(1) |       480M(1) |
//	|  256M |       256M(1) |       512M(1) |
//	|  512M |       256M(2) |       512M(1) |
const writeStepMemShareCount = 6.5

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
	mu  sync.Mutex
	kvs []kvPair
	// memKVBuffers contains two types of buffer, first half are used for small block
	// buffer, second half are used for large one.
	memKVBuffers []*membuf.Buffer
	size         int
	droppedSize  int

	// temporary fields to store KVs to reduce slice allocations.
	kvsPerFile         [][]kvPair
	droppedSizePerFile []int
}

func (b *memKVsAndBuffers) build(ctx context.Context) {
	sumKVCnt := 0
	for _, keys := range b.kvsPerFile {
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

	b.kvs = make([]kvPair, 0, sumKVCnt)
	for i := range b.kvsPerFile {
		b.kvs = append(b.kvs, b.kvsPerFile[i]...)
		b.kvsPerFile[i] = nil
	}
	b.kvsPerFile = nil
}

// Engine stored sorted key/value pairs in an external storage.
type Engine struct {
	storage           storage.ExternalStorage
	dataFiles         []string
	statsFiles        []string
	startKey          []byte
	endKey            []byte
	jobKeys           [][]byte
	splitKeys         [][]byte
	smallBlockBufPool *membuf.Pool
	largeBlockBufPool *membuf.Pool

	memKVsAndBuffers memKVsAndBuffers

	// checkHotspot is true means we will check hotspot file when using MergeKVIter.
	// if hotspot file is detected, we will use multiple readers to read data.
	// if it's false, MergeKVIter will read each file using 1 reader.
	// this flag also affects the strategy of loading data, either:
	// 	less load routine + check and read hotspot file concurrently (add-index uses this one)
	// 	more load routine + read each file using 1 reader (import-into uses this one)
	checkHotspot bool

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
	memLimit        int
}

var _ engineapi.Engine = (*Engine)(nil)

const (
	smallBlockSize = units.MiB
)

// NewExternalEngine creates an (external) engine.
func NewExternalEngine(
	storage storage.ExternalStorage,
	dataFiles []string,
	statsFiles []string,
	startKey []byte,
	endKey []byte,
	jobKeys [][]byte,
	splitKeys [][]byte,
	keyAdapter common.KeyAdapter,
	duplicateDetection bool,
	duplicateDB *pebble.DB,
	dupDetectOpt common.DupDetectOpt,
	workerConcurrency int,
	ts uint64,
	totalKVSize int64,
	totalKVCount int64,
	checkHotspot bool,
	memCapacity int64,
) engineapi.Engine {
	// at most 3 batches can be loaded in memory, see writeStepMemShareCount.
	memLimit := int(float64(memCapacity) / writeStepMemShareCount * 3)
	logutil.BgLogger().Info("create external engine",
		zap.String("memLimitForLoadRange", units.BytesSize(float64(memLimit))))
	memLimiter := membuf.NewLimiter(memLimit)
	return &Engine{
		storage:    storage,
		dataFiles:  dataFiles,
		statsFiles: statsFiles,
		startKey:   startKey,
		endKey:     endKey,
		jobKeys:    jobKeys,
		splitKeys:  splitKeys,
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
		memLimit:           memLimit,
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
	totalFileSize := uint64(0)
	for i := range statsFiles {
		size := endOffs[i] - startOffs[i]
		totalFileSize += size
		expectedConc := size / uint64(ConcurrentReaderBufferSizePerConc)
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
	logutil.Logger(ctx).Info("estimated file size of this range group",
		zap.String("totalSize", units.BytesSize(float64(totalFileSize))))
	return result, startOffs, nil
}

func (e *Engine) loadBatchRegionData(ctx context.Context, jobKeys [][]byte, outCh chan<- engineapi.DataAndRanges) error {
	readAndSortRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read_and_sort")
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_and_sort")
	readRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read")
	readDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read")
	sortRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("sort")
	sortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("sort")

	startKey := jobKeys[0]
	endKey := jobKeys[len(jobKeys)-1]
	readStart := time.Now()
	// read all data in range [startKey, endKey)
	err := readAllData(
		ctx,
		e.storage,
		e.dataFiles,
		e.statsFiles,
		startKey,
		endKey,
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
	var dupKey atomic.Pointer[[]byte]
	sorty.Sort(len(e.memKVsAndBuffers.kvs), func(i, k, r, s int) bool {
		cmp := bytes.Compare(e.memKVsAndBuffers.kvs[i].key, e.memKVsAndBuffers.kvs[k].key)
		if cmp < 0 { // strict comparator like < or >
			if r != s {
				e.memKVsAndBuffers.kvs[r], e.memKVsAndBuffers.kvs[s] = e.memKVsAndBuffers.kvs[s], e.memKVsAndBuffers.kvs[r]
			}
			return true
		}
		if cmp == 0 && i != k {
			cloned := append([]byte(nil), e.memKVsAndBuffers.kvs[i].key...)
			dupKey.Store(&cloned)
		}
		return false
	})
	sorty.MaxGor = oldSortyGor
	sortSecond := time.Since(sortStart).Seconds()
	sortDurHist.Observe(sortSecond)
	logutil.Logger(ctx).Info("sorting in loadBatchRegionData",
		zap.Duration("cost time", time.Since(sortStart)))

	if k := dupKey.Load(); k != nil {
		return errors.Errorf("duplicate key found: %s", hex.EncodeToString(*k))
	}
	readAndSortSecond := time.Since(readStart).Seconds()
	readAndSortDurHist.Observe(readAndSortSecond)

	size := e.memKVsAndBuffers.size
	readAndSortRateHist.Observe(float64(size) / 1024.0 / 1024.0 / readAndSortSecond)
	readRateHist.Observe(float64(size) / 1024.0 / 1024.0 / readSecond)
	sortRateHist.Observe(float64(size) / 1024.0 / 1024.0 / sortSecond)

	data := e.buildIngestData(
		e.memKVsAndBuffers.kvs,
		e.memKVsAndBuffers.memKVBuffers,
	)

	// release the reference of e.memKVsAndBuffers
	e.memKVsAndBuffers.kvs = nil
	e.memKVsAndBuffers.memKVBuffers = nil
	e.memKVsAndBuffers.size = 0

	ranges := make([]engineapi.Range, 0, len(jobKeys)-1)
	prev, err2 := e.keyAdapter.Decode(nil, jobKeys[0])
	if err2 != nil {
		return err
	}
	for i := 1; i < len(jobKeys)-1; i++ {
		cur, err3 := e.keyAdapter.Decode(nil, jobKeys[i])
		if err3 != nil {
			return err3
		}
		ranges = append(ranges, engineapi.Range{
			Start: prev,
			End:   cur,
		})
		prev = cur
	}
	// last range key may be a nextKey so we should try to remove the trailing 0 if decoding failed
	lastKey := jobKeys[len(jobKeys)-1]
	cur, err4 := e.tryDecodeEndKey(lastKey)
	if err4 != nil {
		return err4
	}
	ranges = append(ranges, engineapi.Range{
		Start: prev,
		End:   cur,
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case outCh <- engineapi.DataAndRanges{
		Data:         data,
		SortedRanges: ranges,
	}:
	}
	return nil
}

// LoadIngestData loads the data from the external storage to memory in [start,
// end) range, so local backend can ingest it. The used byte slice of ingest data
// are allocated from Engine.bufPool and must be released by
// MemoryIngestData.DecRef().
func (e *Engine) LoadIngestData(
	ctx context.Context,
	outCh chan<- engineapi.DataAndRanges,
) error {
	// try to make every worker busy for each batch
	rangeBatchSize := e.workerConcurrency
	failpoint.Inject("LoadIngestDataBatchSize", func(val failpoint.Value) {
		rangeBatchSize = val.(int)
	})
	logutil.Logger(ctx).Info("load ingest data", zap.Int("batchSize", rangeBatchSize))
	for start := 0; start < len(e.jobKeys)-1; start += rangeBatchSize {
		// want to generate N ranges, so we need N+1 keys
		end := min(1+start+rangeBatchSize, len(e.jobKeys))
		err := e.loadBatchRegionData(ctx, e.jobKeys[start:end], outCh)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) buildIngestData(kvs []kvPair, buf []*membuf.Buffer) *MemoryIngestData {
	return &MemoryIngestData{
		keyAdapter:         e.keyAdapter,
		duplicateDetection: e.duplicateDetection,
		duplicateDB:        e.duplicateDB,
		dupDetectOpt:       e.dupDetectOpt,
		kvs:                kvs,
		ts:                 e.ts,
		memBuf:             buf,
		refCnt:             atomic.NewInt64(0),
		importedKVSize:     e.importedKVSize,
		importedKVCount:    e.importedKVCount,
	}
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
	if _, ok := e.keyAdapter.(common.NoopKeyAdapter); ok {
		return e.startKey, e.endKey, nil
	}

	startKey, err = e.keyAdapter.Decode(nil, e.startKey)
	if err != nil {
		return nil, nil, err
	}
	endKey, err = e.tryDecodeEndKey(e.endKey)
	if err != nil {
		return nil, nil, err
	}
	return startKey, endKey, nil
}

// GetRegionSplitKeys implements common.Engine.
func (e *Engine) GetRegionSplitKeys() ([][]byte, error) {
	splitKeys := make([][]byte, len(e.splitKeys))
	var (
		err      error
		splitKey []byte
	)
	for i, k := range e.splitKeys {
		if i < len(e.splitKeys)-1 {
			splitKey, err = e.keyAdapter.Decode(nil, k)
		} else {
			splitKey, err = e.tryDecodeEndKey(k)
		}
		if err != nil {
			return nil, err
		}
		splitKeys[i] = splitKey
	}
	return splitKeys, nil
}

// tryDecodeEndKey tries to decode the key from two sources.
// When duplicate detection feature is enabled, the **end key** comes from
// DupDetectKeyAdapter.Encode or Key.Next(). We try to decode it and check the
// error.
func (e *Engine) tryDecodeEndKey(key []byte) (decoded []byte, err error) {
	decoded, err = e.keyAdapter.Decode(nil, key)
	if err == nil {
		return
	}
	if _, ok := e.keyAdapter.(common.NoopKeyAdapter); ok {
		// NoopKeyAdapter.Decode always return nil error
		intest.Assert(false, "Unreachable code path")
		return nil, err
	}
	// handle the case that end key is from Key.Next()
	if key[len(key)-1] != 0 {
		return nil, err
	}
	key = key[:len(key)-1]
	decoded, err = e.keyAdapter.Decode(nil, key)
	if err != nil {
		return nil, err
	}
	return kv.Key(decoded).Next(), nil
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
	memLimiter := membuf.NewLimiter(e.memLimit)
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

	kvs []kvPair
	ts  uint64

	memBuf          []*membuf.Buffer
	refCnt          *atomic.Int64
	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
}

var _ engineapi.IngestData = (*MemoryIngestData)(nil)

func (m *MemoryIngestData) firstAndLastKeyIndex(lowerBound, upperBound []byte) (int, int) {
	firstKeyIdx := 0
	if len(lowerBound) > 0 {
		lowerBound = m.keyAdapter.Encode(nil, lowerBound, common.MinRowID)
		firstKeyIdx = sort.Search(len(m.kvs), func(i int) bool {
			return bytes.Compare(lowerBound, m.kvs[i].key) <= 0
		})
		if firstKeyIdx == len(m.kvs) {
			return -1, -1
		}
	}

	lastKeyIdx := len(m.kvs) - 1
	if len(upperBound) > 0 {
		upperBound = m.keyAdapter.Encode(nil, upperBound, common.MinRowID)
		i := sort.Search(len(m.kvs), func(i int) bool {
			reverseIdx := len(m.kvs) - 1 - i
			return bytes.Compare(upperBound, m.kvs[reverseIdx].key) > 0
		})
		if i == len(m.kvs) {
			// should not happen
			return -1, -1
		}
		lastKeyIdx = len(m.kvs) - 1 - i
	}
	return firstKeyIdx, lastKeyIdx
}

// GetFirstAndLastKey implements IngestData.GetFirstAndLastKey.
func (m *MemoryIngestData) GetFirstAndLastKey(lowerBound, upperBound []byte) ([]byte, []byte, error) {
	firstKeyIdx, lastKeyIdx := m.firstAndLastKeyIndex(lowerBound, upperBound)
	if firstKeyIdx < 0 || firstKeyIdx > lastKeyIdx {
		return nil, nil, nil
	}
	firstKey, err := m.keyAdapter.Decode(nil, m.kvs[firstKeyIdx].key)
	if err != nil {
		return nil, nil, err
	}
	lastKey, err := m.keyAdapter.Decode(nil, m.kvs[lastKeyIdx].key)
	if err != nil {
		return nil, nil, err
	}
	return firstKey, lastKey, nil
}

type memoryDataIter struct {
	kvs []kvPair

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
	return m.kvs[m.curIdx].key
}

// Value implements ForwardIter.
func (m *memoryDataIter) Value() []byte {
	return m.kvs[m.curIdx].value
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
) engineapi.ForwardIter {
	firstKeyIdx, lastKeyIdx := m.firstAndLastKeyIndex(lowerBound, upperBound)
	iter := &memoryDataIter{
		kvs:         m.kvs,
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
		m.kvs = nil
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
