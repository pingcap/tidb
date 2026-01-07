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
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/operator"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	// MaxMergingFilesPerThread is the maximum number of files that can be merged by a
	// single thread. This value comes from the fact that 16 threads are ok to merge 4k
	// files in parallel, so we set it to 250.
	MaxMergingFilesPerThread = 250
	// MinUploadPartSize is the minimum size of each part when uploading files to
	// external storage, which is 5MiB for both S3 and GCS.
	MinUploadPartSize int64 = 5 * units.MiB
)

var _ execute.Collector = &mergeCollector{}

// mergeCollector collects the bytes and row count in merge step.
type mergeCollector struct {
	summary *execute.SubtaskSummary
	counter prometheus.Counter
}

// NewMergeCollector creates a new merge collector.
func NewMergeCollector(ctx context.Context, summary *execute.SubtaskSummary) *mergeCollector {
	var counter prometheus.Counter
	if me, ok := metric.GetCommonMetric(ctx); ok {
		counter = me.BytesCounter.WithLabelValues(metric.StateMerged)
	}
	return &mergeCollector{
		summary: summary,
		counter: counter,
	}
}

func (*mergeCollector) Accepted(_ int64) {}

func (c *mergeCollector) Processed(bytes, rowCnt int64) {
	if c.summary != nil {
		c.summary.Bytes.Add(bytes)
		c.summary.RowCnt.Add(rowCnt)
	}
	if c.counter != nil {
		c.counter.Add(float64(bytes))
	}
}

type mergeMinimalTask struct {
	files        []string
	fileGroupNum int
	writerID     string
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (*mergeMinimalTask) RecoverArgs() (metricsLabel string, funcInfo string, err error) {
	return "merge_sort", "mergeMinimalTask", nil
}

// MergeOperator is the operator that merges overlapping files.
type MergeOperator struct {
	*operator.AsyncOperator[*mergeMinimalTask, workerpool.None]
}

// NewMergeOperator creates a new MergeOperator instance.
func NewMergeOperator(
	ctx *workerpool.Context,
	store objstore.Storage,
	partSize int64,
	newFilePrefix string,
	blockSize int,
	onWriterClose OnWriterCloseFunc,
	collector execute.Collector,
	concurrency int,
	checkHotspot bool,
	onDup engineapi.OnDuplicateKey,
) *MergeOperator {
	// during encode&sort step, the writer-limit is aligned to block size, so we
	// need align this too. the max additional written size per file is max-block-size.
	// for max-block-size = 32MiB, adding (max-block-size * MaxMergingFilesPerThread)/10000 ~ 1MiB
	// to part-size is enough.
	partSize = max(MinUploadPartSize, partSize+units.MiB)
	logutil.Logger(ctx).Info("create merge operator",
		zap.Int64("part-size", partSize))
	pool := workerpool.NewWorkerPool(
		"mergeOperator",
		util.ImportInto,
		concurrency,
		func() workerpool.Worker[*mergeMinimalTask, workerpool.None] {
			return &mergeWorker{
				ctx:           ctx,
				store:         store,
				partSize:      partSize,
				newFilePrefix: newFilePrefix,
				blockSize:     blockSize,
				onWriterClose: onWriterClose,
				collector:     collector,
				checkHotspot:  checkHotspot,
				onDup:         onDup,
			}
		},
	)

	return &MergeOperator{
		AsyncOperator: operator.NewAsyncOperator(ctx, pool),
	}
}

// String implements the Operator interface.
func (*MergeOperator) String() string {
	return "mergeOperator"
}

type mergeWorker struct {
	ctx context.Context

	store         objstore.Storage
	partSize      int64
	newFilePrefix string
	blockSize     int
	onWriterClose OnWriterCloseFunc
	collector     execute.Collector
	checkHotspot  bool
	onDup         engineapi.OnDuplicateKey
}

func (w *mergeWorker) HandleTask(task *mergeMinimalTask, _ func(workerpool.None)) error {
	return mergeOverlappingFilesInternal(
		w.ctx,
		task.files,
		w.store,
		w.partSize,
		w.newFilePrefix,
		task.writerID,
		w.blockSize,
		w.onWriterClose,
		w.collector,
		w.checkHotspot,
		w.onDup,
		task.fileGroupNum,
	)
}

func (*mergeWorker) Close() error {
	return nil
}

// MergeOverlappingFiles reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
func MergeOverlappingFiles(
	ctx *workerpool.Context,
	paths []string,
	concurrency int,
	op *MergeOperator,
) error {
	dataFilesSlice := splitDataFiles(paths, concurrency)
	logutil.Logger(ctx).Info("start to merge overlapping files",
		zap.Int("file-count", len(paths)),
		zap.Int("file-groups", len(dataFilesSlice)),
		zap.Int("concurrency", concurrency))

	mergeTasks := make([]*mergeMinimalTask, 0, len(dataFilesSlice))
	for _, files := range dataFilesSlice {
		mergeTasks = append(mergeTasks, &mergeMinimalTask{
			files:        files,
			fileGroupNum: len(dataFilesSlice),
			writerID:     uuid.New().String(),
		})
	}

	sourceOp := operator.NewSimpleDataSource(ctx, mergeTasks)
	operator.Compose(sourceOp, op)

	pipe := operator.NewAsyncPipeline(sourceOp, op)
	if err := pipe.Execute(); err != nil {
		return err
	}

	err := pipe.Close()
	if opErr := ctx.OperatorErr(); opErr != nil {
		return opErr
	}
	return err
}

// split input data files into multiple shares evenly, with the max number files
// in each share MaxMergingFilesPerThread, if there are not enough files, merge at
// least 2 files in one batch.
func splitDataFiles(paths []string, concurrency int) [][]string {
	shares := max((len(paths)+MaxMergingFilesPerThread-1)/MaxMergingFilesPerThread, concurrency)
	if len(paths) < 2*concurrency {
		shares = max(1, len(paths)/2)
	}
	dataFilesSlice := make([][]string, 0, shares)
	batchCount := len(paths) / shares
	remainder := len(paths) % shares
	start := 0
	for start < len(paths) {
		end := start + batchCount
		if remainder > 0 {
			end++
			remainder--
		}
		if end > len(paths) {
			end = len(paths)
		}
		dataFilesSlice = append(dataFilesSlice, paths[start:end])
		start = end
	}
	return dataFilesSlice
}

// mergeOverlappingFilesInternal reads from given files whose key range may overlap
// and writes to one new sorted, nonoverlapping files.
// since some memory are taken by library, such as HTTP2, that we cannot calculate
// accurately, here we only consider the memory used by our code, the estimate max
// memory usage of this function is:
//
//	defaultOneWriterMemSizeLimit
//	+ MaxMergingFilesPerThread * (X + DefaultReadBufferSize)
//	+ maxUploadWorkersPerThread * (data-part-size + 5MiB(stat-part-size))
//	+ memory taken by concurrent reading if check-hotspot is enabled
//
// where X is memory used for each read connection, it's http2 for GCP, X might be
// 4 or more MiB, http1 for S3, it's smaller.
//
// with current default values, on machine with 2G per core, the estimate max memory
// usage for import into is:
//
//	128 + 250 * (4 + 64/1024) + 8 * (25.6 + 5) ~ 1.36 GiB
//	where 25.6 is max part-size when there is only data kv = 1024*250/10000 = 25.6MiB
//
// for add-index, it uses more memory as check-hotspot is enabled.
func mergeOverlappingFilesInternal(
	ctx context.Context,
	paths []string,
	store objstore.Storage,
	partSize int64,
	newFilePrefix string,
	writerID string,
	blockSize int,
	onWriterClose OnWriterCloseFunc,
	collector execute.Collector,
	checkHotspot bool,
	onDup engineapi.OnDuplicateKey,
	fileGroupNum int,
) (err error) {
	failpoint.Inject("mergeOverlappingFilesInternal", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			switch v {
			case 1:
				failpoint.Return(errors.Errorf("mock error in mergeOverlappingFilesInternal"))
			case 2:
				panic("mock panic in mergeOverlappingFilesInternal")
			case 3:
				time.Sleep(time.Second * 5)
				failpoint.Return(ctx.Err())
			default:
				failpoint.Return(nil)
			}
		}
	})
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.String("writer-id", writerID),
		zap.Int("file-count", len(paths)),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	zeroOffsets := make([]uint64, len(paths))
	iter, err := NewMergeKVIter(ctx, paths, zeroOffsets, store, DefaultReadBufferSize, checkHotspot, fileGroupNum)
	if err != nil {
		return err
	}
	defer func() {
		err := iter.Close()
		if err != nil {
			logutil.Logger(ctx).Warn("close iterator failed", zap.Error(err))
		}
	}()

	writer := NewWriterBuilder().
		SetMemorySizeLimit(defaultOneWriterMemSizeLimit).
		SetBlockSize(blockSize).
		SetOnCloseFunc(onWriterClose).
		SetOnDup(onDup).
		BuildOneFile(store, newFilePrefix, writerID)
	writer.InitPartSizeAndLogger(ctx, partSize)
	defer func() {
		err2 := writer.Close(ctx)
		if err2 == nil {
			return
		}

		if err == nil {
			err = err2
		} else {
			logutil.Logger(ctx).Warn("close writer failed", zap.Error(err2))
		}
	}()

	// currently use same goroutine to do read and write. The main advantage is
	// there's no KV copy and iter can reuse the buffer.
	for iter.Next() {
		key, value := iter.Key(), iter.Value()
		err = writer.WriteRow(ctx, key, value)
		if err != nil {
			return err
		}

		if collector != nil {
			collector.Processed(int64(len(key)+len(value)), 1)
		}
	}
	return iter.Error()
}
