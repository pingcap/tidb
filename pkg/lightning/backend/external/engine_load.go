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
	"path/filepath"
	"slices"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

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
	dupWriter, err := e.storage.Create(ctx, dupFile, &storeapi.WriterOption{
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
