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
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
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
//   - one share used by HTTP and GRPC buf, such as loadRangeBatchData, write TiKV
//   - one share used by loadRangeBatchData to store loaded data batch A
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
// Note: below calculation only consider the memory taken by the KV pair itself,
// golang takes 24*2 = 48B for each KV pair, so if the size of KV pair itself is
// very small, the real memory taken by each KV pair might be doubled, so it's
// only an estimation.
// such as, for a simple table "create table t(id bigint primary key, v bigint, index(v))",
// each index KV is 38B, golang need 86B memory to store it.
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

// getEngineMemoryLimit calculates the memory limit for external engine according
// to the memory capacity.
func getEngineMemoryLimit(memCapacity int64) int {
	// at most 3 batches can be loaded in memory, see writeStepMemShareCount.
	return int(float64(memCapacity) / writeStepMemShareCount * 3)
}

type memKVsAndBuffers struct {
	mu  sync.Mutex
	kvs []KVPair
	// memKVBuffers contains two types of buffer, first half are used for small block
	// buffer, second half are used for large one.
	memKVBuffers []*membuf.Buffer
	size         int
	droppedSize  int

	// temporary fields to store KVs to reduce slice allocations.
	kvsPerFile         [][]KVPair
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

	b.kvs = make([]KVPair, 0, sumKVCnt)
	for i := range b.kvsPerFile {
		b.kvs = append(b.kvs, b.kvsPerFile[i]...)
		b.kvsPerFile[i] = nil
	}
	b.kvsPerFile = nil
}

// Engine stored sorted key/value pairs in an external storage.
type Engine struct {
	storage   objstore.ExternalStorage
	dataFiles []string
	statsFiles        []string
	startKey          []byte
	endKey            []byte
	jobKeys           [][]byte
	splitKeys         [][]byte
	smallBlockBufPool *membuf.Pool
	largeBlockBufPool *membuf.Pool

	memKVsAndBuffers memKVsAndBuffers
	// totalLoadedKVsCount accumulates the total number of KVs loaded in LoadIngestData
	totalLoadedKVsCount atomic.Int64

	// activeIngestDataFlags store flags generated by this engine
	activeIngestDataFlags []*atomic.Bool

	// region job workerPool pool related to this engine.
	workerPool atomic.Pointer[workerpool.Tuner]

	// checkHotspot is true means we will check hotspot file when using MergeKVIter.
	// if hotspot file is detected, we will use multiple readers to read data.
	// if it's false, MergeKVIter will read each file using 1 reader.
	// this flag also affects the strategy of loading data, either:
	// 	less load routine + check and read hotspot file concurrently (add-index uses this one)
	// 	more load routine + read each file using 1 reader (import-into uses this one)
	checkHotspot bool

	workerConcurrency atomic.Int32

	// readyCh is used to notify that the engine's resource has been updated
	readyCh chan struct{}

	ts uint64

	totalKVSize  int64
	totalKVCount int64

	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
	memLimit        int
	onDup           engineapi.OnDuplicateKey
	filePrefix      string
	// below fields are only used when onDup is OnDuplicateKeyRecord.
	recordedDupCnt  int
	recordedDupSize int64
	dupFile    string
	dupWriter  objstore.ExternalFileWriter
	dupKVStore *KeyValueStore
}

var _ engineapi.Engine = (*Engine)(nil)

const (
	smallBlockSize = units.MiB
)

// NewExternalEngine creates an (external) engine.
func NewExternalEngine(
	ctx context.Context,
	storage objstore.ExternalStorage,
	dataFiles []string,
	statsFiles []string,
	startKey []byte,
	endKey []byte,
	jobKeys [][]byte,
	splitKeys [][]byte,
	workerConcurrency int,
	ts uint64,
	totalKVSize int64,
	totalKVCount int64,
	checkHotspot bool,
	memCapacity int64,
	onDup engineapi.OnDuplicateKey,
	filePrefix string,
) *Engine {
	memLimit := getEngineMemoryLimit(memCapacity)
	logutil.Logger(ctx).Info("create external engine",
		zap.String("memLimitForLoadRange", units.BytesSize(float64(memLimit))),
		zap.Int("dataFileCount", len(dataFiles)),
		zap.Int("jobKeysCount", len(jobKeys)),
		zap.Int("splitKeysCount", len(splitKeys)),
	)
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
		checkHotspot:      checkHotspot,
		workerConcurrency: *atomic.NewInt32(int32(workerConcurrency)),
		readyCh:           make(chan struct{}),
		ts:                ts,
		totalKVSize:       totalKVSize,
		totalKVCount:      totalKVCount,
		importedKVSize:    atomic.NewInt64(0),
		importedKVCount:   atomic.NewInt64(0),
		memLimit:          memLimit,
		onDup:             onDup,
		filePrefix:        filePrefix,
	}
}

func getFilesReadConcurrency(
	ctx context.Context,
	storage objstore.ExternalStorage,
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
	// Note: this is the file size of the range group, KV size is smaller, as we
	// need additional 8*2 for each KV.
	logutil.Logger(ctx).Info("estimated file size of this range group",
		zap.String("totalSize", units.BytesSize(float64(totalFileSize))))
	return result, startOffs, nil
}

func (e *Engine) loadRangeBatchData(ctx context.Context, jobKeys [][]byte, outCh chan<- engineapi.DataAndRanges) error {
	readAndSortRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read_and_sort")
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_and_sort")
	readRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read")
	readDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read")
	sortRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("sort")
	sortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("sort")

	failpoint.Inject("mockLoadBatchRegionData", func(_ failpoint.Value) {
		kvs := make([]KVPair, 0)
		kvs = append(kvs, KVPair{[]byte{}, []byte{}})
		data := e.buildIngestData(kvs, nil)
		data.IncRef()
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case outCh <- engineapi.DataAndRanges{
			Data: data,
		}:
			e.activeIngestDataFlags = append(e.activeIngestDataFlags, data.released)
			failpoint.Return(nil)
		}
	})

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

	readDur := time.Since(readStart)
	readSecond := readDur.Seconds()
	readDurHist.Observe(readSecond)

	sortStart := time.Now()
	oldSortyGor := sorty.MaxGor
	sorty.MaxGor = uint64(e.workerConcurrency.Load() * 2)
	var dupKey, dupVal atomic.Pointer[[]byte]
	sorty.Sort(len(e.memKVsAndBuffers.kvs), func(i, k, r, s int) bool {
		cmp := bytes.Compare(e.memKVsAndBuffers.kvs[i].Key, e.memKVsAndBuffers.kvs[k].Key)
		if cmp < 0 { // strict comparator like < or >
			if r != s {
				e.memKVsAndBuffers.kvs[r], e.memKVsAndBuffers.kvs[s] = e.memKVsAndBuffers.kvs[s], e.memKVsAndBuffers.kvs[r]
			}
			return true
		}
		if cmp == 0 && i != k {
			if dupKey.Load() == nil {
				key := slices.Clone(e.memKVsAndBuffers.kvs[i].Key)
				dupKey.Store(&key)
				value := slices.Clone(e.memKVsAndBuffers.kvs[i].Value)
				dupVal.Store(&value)
			}
		}
		return false
	})
	sorty.MaxGor = oldSortyGor
	sortDur := time.Since(sortStart)
	sortSecond := sortDur.Seconds()
	sortDurHist.Observe(sortSecond)

	// we shouldn't handle duplicates for OnDuplicateKeyIgnore, it's the semantic
	// of OnDuplicateKeyError, but to make keep compatible with the old code, we
	// keep this behavior.
	// TODO: remove this when we have have fully integrated the OnDuplicateKey.
	if k, v := dupKey.Load(), dupVal.Load(); k != nil {
		if e.onDup == engineapi.OnDuplicateKeyIgnore {
			return errors.Errorf("duplicate key found: %s", hex.EncodeToString(*k))
		} else if e.onDup == engineapi.OnDuplicateKeyError {
			return common.ErrFoundDuplicateKeys.FastGenByArgs(*k, *v)
		}
	}
	readAndSortSecond := time.Since(readStart).Seconds()
	readAndSortDurHist.Observe(readAndSortSecond)

	size := e.memKVsAndBuffers.size
	readAndSortRateHist.Observe(float64(size) / 1024.0 / 1024.0 / readAndSortSecond)
	readRateHist.Observe(float64(size) / 1024.0 / 1024.0 / readSecond)
	sortRateHist.Observe(float64(size) / 1024.0 / 1024.0 / sortSecond)

	var (
		deduplicatedKVs, dups []KVPair
		dupCount              int
		deduplicateDur        time.Duration
	)
	deduplicatedKVs = e.memKVsAndBuffers.kvs
	dupFound := dupKey.Load() != nil
	if dupFound {
		start := time.Now()
		if e.onDup == engineapi.OnDuplicateKeyRecord {
			if err = e.lazyInitDupWriter(ctx); err != nil {
				return err
			}
			deduplicatedKVs, dups, dupCount = removeDuplicates(deduplicatedKVs, getPairKey, true)
			e.recordedDupCnt += len(dups)
			for _, p := range dups {
				e.recordedDupSize += int64(len(p.Key) + len(p.Value))
				if err = e.dupKVStore.addRawKV(p.Key, p.Value); err != nil {
					return err
				}
			}
		} else if e.onDup == engineapi.OnDuplicateKeyRemove {
			deduplicatedKVs, _, dupCount = removeDuplicates(deduplicatedKVs, getPairKey, false)
		}
		deduplicateDur = time.Since(start)
	}
	logutil.Logger(ctx).Info("load range batch done",
		zap.Duration("readDur", readDur),
		zap.Duration("sortDur", sortDur),
		zap.Int("droppedSize", e.memKVsAndBuffers.droppedSize),
		zap.Int("loadedKVs", len(e.memKVsAndBuffers.kvs)),
		zap.String("loadedSize", units.HumanSize(float64(e.memKVsAndBuffers.size))),
		zap.Stringer("onDup", e.onDup),
		zap.Int("dupCount", dupCount),
		zap.Int("recordedDupCount", len(dups)),
		zap.Int("totalRecordedDupCount", e.recordedDupCnt),
		zap.String("totalRecordedDupSize", units.BytesSize(float64(e.recordedDupSize))),
		zap.Duration("deduplicateDur", deduplicateDur),
	)

	data := e.buildIngestData(
		deduplicatedKVs,
		e.memKVsAndBuffers.memKVBuffers,
	)

	// accumulate the total number of KVs loaded in LoadIngestData
	e.totalLoadedKVsCount.Add(int64(len(deduplicatedKVs)))

	// release the reference of e.memKVsAndBuffers
	e.memKVsAndBuffers.kvs = nil
	e.memKVsAndBuffers.memKVBuffers = nil
	e.memKVsAndBuffers.size = 0

	ranges := make([]engineapi.Range, 0, len(jobKeys)-1)
	prev := slices.Clone(jobKeys[0])
	for i := 1; i < len(jobKeys)-1; i++ {
		cur := slices.Clone(jobKeys[i])
		ranges = append(ranges, engineapi.Range{
			Start: prev,
			End:   cur,
		})
		prev = cur
	}
	lastKey := slices.Clone(jobKeys[len(jobKeys)-1])
	ranges = append(ranges, engineapi.Range{
		Start: prev,
		End:   lastKey,
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case outCh <- engineapi.DataAndRanges{
		Data:         data,
		SortedRanges: ranges,
	}:
		// We can't rely on refCnt to check if the data is released, because
		// the refCnt is initialized to zero when the data is generated.
		// So we use the released field to check if the data is released.
		// If the data is discarded without calling Ref(), it means we meet
		// an error in LoadIngestData, and the input context will be canceled.
		e.activeIngestDataFlags = append(e.activeIngestDataFlags, data.released)
	}
	return nil
}

// updateActiveIngestDataFlags updates ingest data flags to remove released ones.
func (e *Engine) updateActiveIngestDataFlags() {
	res := e.activeIngestDataFlags[:0]
	for _, r := range e.activeIngestDataFlags {
		if !r.Load() {
			res = append(res, r)
		}
	}
	e.activeIngestDataFlags = res
}

// handleConcurrencyChange handles the concurrency change for this engine. If
// change is detected, we need to wait for all previous data being consumed, then
// recreate a new memory pool to this engine. And it will returns the current batch size
func (e *Engine) handleConcurrencyChange(ctx context.Context, currBatchSize int) int {
	newBatchSize := int(e.workerConcurrency.Load())
	failpoint.Inject("LoadIngestDataBatchSize", func(val failpoint.Value) {
		currBatchSize = val.(int)
		newBatchSize = currBatchSize
	})

	if newBatchSize == currBatchSize {
		e.updateActiveIngestDataFlags()
		return currBatchSize
	}

	logger := logutil.Logger(ctx).With(
		zap.Int("prev batch size", currBatchSize),
		zap.Int("new batch size", newBatchSize))

	startTime := time.Now()
	logger.Info("waiting ingest data batch size change")

	tick := time.NewTicker(time.Second)
	defer func() {
		tick.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return currBatchSize
		case <-tick.C:
			e.updateActiveIngestDataFlags()
		}
		if len(e.activeIngestDataFlags) == 0 {
			break
		}
	}

	// Now we can safely reset the memory pool.
	e.Reset()

	logger.Info("load ingest data batch size changed", zap.Duration("used time", time.Since(startTime)))

	// Notify that we have changed the resource usage
	select {
	case <-ctx.Done():
		return currBatchSize
	case e.readyCh <- struct{}{}:
	}

	return newBatchSize
}

// LoadIngestData loads the data from the external storage to memory in [start,
// end) range, so local backend can ingest it. The used byte slice of ingest data
// are allocated from Engine.bufPool and must be released by
// MemoryIngestData.DecRef().
func (e *Engine) LoadIngestData(
	ctx context.Context,
	outCh chan<- engineapi.DataAndRanges,
) (err error) {
	if e.onDup == engineapi.OnDuplicateKeyRecord {
		defer func() {
			err1 := e.closeDupWriterAsNeeded(ctx)
			if err == nil {
				err = err1
			}
		}()
	}
	// try to make every worker busy for each batch
	currBatchSize := int(e.workerConcurrency.Load())
	logutil.Logger(ctx).Info("load ingest data", zap.Int("current batchSize", currBatchSize))

	for start := 0; start < len(e.jobKeys)-1; {
		currBatchSize = e.handleConcurrencyChange(ctx, currBatchSize)
		// want to generate N ranges, so we need N+1 keys
		end := min(1+start+currBatchSize, len(e.jobKeys))
		err = e.loadRangeBatchData(ctx, e.jobKeys[start:end], outCh)
		if err != nil {
			return err
		}
		start += currBatchSize
	}

	// All data has been generated, no need to change the concurrency
	close(e.readyCh)
	return nil
}

// lazyInitDupWriter lazily initializes the duplicate writer.
// we need test on KS3 which will report InvalidArgument if the file is empty.
// GCS is ok with empty file.
func (e *Engine) lazyInitDupWriter(ctx context.Context) error {
	if e.dupWriter != nil {
		return nil
	}
	dupFile := filepath.Join(e.filePrefix, "dup")
	dupWriter, err := e.storage.Create(ctx, dupFile, &objstore.WriterOption{
		// TODO might need to tune concurrency.
		// max 150GiB duplicates can be saved, it should be enough, as we split
		// subtask into 100G each.
		Concurrency: 1,
		PartSize:    3 * MinUploadPartSize})
	if err != nil {
		logutil.Logger(ctx).Error("create dup writer failed", zap.Error(err))
		return err
	}
	e.dupFile = dupFile
	e.dupWriter = dupWriter
	e.dupKVStore = NewKeyValueStore(ctx, e.dupWriter, nil)
	return nil
}

func (e *Engine) closeDupWriterAsNeeded(ctx context.Context) error {
	if e.dupWriter == nil {
		return nil
	}
	kvStore, writer := e.dupKVStore, e.dupWriter
	e.dupKVStore, e.dupWriter = nil, nil
	kvStore.finish()
	if err := writer.Close(ctx); err != nil {
		logutil.Logger(ctx).Error("close dup writer failed", zap.Error(err))
		return err
	}
	return nil
}

func (e *Engine) buildIngestData(kvs []KVPair, buf []*membuf.Buffer) *MemoryIngestData {
	return &MemoryIngestData{
		kvs:             kvs,
		ts:              e.ts,
		memBuf:          buf,
		released:        atomic.NewBool(false),
		refCnt:          atomic.NewInt64(0),
		importedKVSize:  e.importedKVSize,
		importedKVCount: e.importedKVCount,
	}
}

// SetWorkerPool sets the worker pool for this engine.
func (e *Engine) SetWorkerPool(worker workerpool.Tuner) {
	e.workerPool.Store(&worker)
}

// UpdateResource changes the concurrency and the memory pool size of this engine.
func (e *Engine) UpdateResource(ctx context.Context, concurrency int, memCapacity int64) error {
	worker := *e.workerPool.Load()
	if worker == nil {
		return errors.New("region job worker is not initialized, retry later")
	}

	if e.workerConcurrency.Load() == int32(concurrency) {
		return nil
	}

	// Update memLimit first, then update concurrency and wait.
	e.memLimit = getEngineMemoryLimit(memCapacity)
	e.workerConcurrency.Store(int32(concurrency))

	failpoint.InjectCall("afterUpdateWorkerConcurrency")

	// Wait for current data being consumed and new memory pool created.
	select {
	case <-ctx.Done():
		logutil.Logger(ctx).Info("context done when updating resource, skip update resource")
		return ctx.Err()
	case _, ok := <-e.readyCh:
		if ok {
			logutil.Logger(ctx).Info("update resource for external engine",
				zap.Int("concurrency", concurrency),
				zap.String("memLimit", units.BytesSize(float64(e.memLimit))))
			worker.Tune(int32(concurrency), true)
		} else {
			logutil.Logger(ctx).Info("load data finished, skip update resource")
		}
		return nil
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

// GetTotalLoadedKVsCount returns the total number of KVs loaded in LoadIngestData.
func (e *Engine) GetTotalLoadedKVsCount() int64 {
	return e.totalLoadedKVsCount.Load()
}

// ConflictInfo implements common.Engine.
func (e *Engine) ConflictInfo() engineapi.ConflictInfo {
	if e.recordedDupCnt == 0 {
		return engineapi.ConflictInfo{}
	}
	return engineapi.ConflictInfo{
		Count: uint64(e.recordedDupCnt),
		Files: []string{e.dupFile},
	}
}

// ID is the identifier of an engine.
func (e *Engine) ID() string {
	return "external"
}

// GetOnDup returns the OnDuplicateKey action for this engine.
func (e *Engine) GetOnDup() engineapi.OnDuplicateKey {
	return e.onDup
}

// GetKeyRange implements common.Engine.
func (e *Engine) GetKeyRange() (startKey []byte, endKey []byte, err error) {
	return e.startKey, e.endKey, nil
}

// GetRegionSplitKeys implements common.Engine.
func (e *Engine) GetRegionSplitKeys() ([][]byte, error) {
	splitKeys := make([][]byte, 0, len(e.splitKeys))
	for _, k := range e.splitKeys {
		splitKeys = append(splitKeys, slices.Clone(k))
	}
	return splitKeys, nil
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
	return nil
}

// Reset resets the memory buffer pool.
func (e *Engine) Reset() {
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
}

// MemoryIngestData is the in-memory implementation of IngestData.
type MemoryIngestData struct {
	kvs []KVPair
	ts  uint64

	memBuf          []*membuf.Buffer
	released        *atomic.Bool
	refCnt          *atomic.Int64
	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
}

var _ engineapi.IngestData = (*MemoryIngestData)(nil)

func (m *MemoryIngestData) firstAndLastKeyIndex(lowerBound, upperBound []byte) (int, int) {
	firstKeyIdx := 0
	if len(lowerBound) > 0 {
		firstKeyIdx = sort.Search(len(m.kvs), func(i int) bool {
			return bytes.Compare(lowerBound, m.kvs[i].Key) <= 0
		})
		if firstKeyIdx == len(m.kvs) {
			return -1, -1
		}
	}

	lastKeyIdx := len(m.kvs) - 1
	if len(upperBound) > 0 {
		i := sort.Search(len(m.kvs), func(i int) bool {
			reverseIdx := len(m.kvs) - 1 - i
			return bytes.Compare(upperBound, m.kvs[reverseIdx].Key) > 0
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
	firstKey := slices.Clone(m.kvs[firstKeyIdx].Key)
	lastKey := slices.Clone(m.kvs[lastKeyIdx].Key)
	return firstKey, lastKey, nil
}

type memoryDataIter struct {
	kvs []KVPair

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
	return m.kvs[m.curIdx].Key
}

// Value implements ForwardIter.
func (m *memoryDataIter) Value() []byte {
	return m.kvs[m.curIdx].Value
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
	return iter
}

// GetTS implements IngestData.GetTS.
func (m *MemoryIngestData) GetTS() uint64 {
	return m.ts
}

// IncRef implements IngestData.IncRef.
func (m *MemoryIngestData) IncRef() {
	m.refCnt.Inc()
	// Make sure data is not released.
	intest.Assert(!m.released.Load(), "data shouldn't be released when IncRef")
}

// DecRef implements IngestData.DecRef.
func (m *MemoryIngestData) DecRef() {
	if m.refCnt.Dec() == 0 {
		m.kvs = nil
		for _, b := range m.memBuf {
			b.Destroy()
		}
		m.released.Store(true)
	}
}

// Finish implements IngestData.Finish.
func (m *MemoryIngestData) Finish(totalBytes, totalCount int64) {
	m.importedKVSize.Add(totalBytes)
	m.importedKVCount.Add(totalCount)
}
