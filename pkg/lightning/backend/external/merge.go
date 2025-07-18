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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
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

type mergeMinimalTask struct {
	ctx   *operator.Context
	files []string
}

// MergeOperator is the operator that merges overlapping files.
type MergeOperator struct {
	ctx *operator.Context
	*operator.AsyncOperator[*mergeMinimalTask, workerpool.None]
}

// NewMergeOperator creates a new MergeOperator instance.
func NewMergeOperator(
	ctx *operator.Context,
	store storage.ExternalStorage,
	partSize int64,
	newFilePrefix string,
	blockSize int,
	onClose OnCloseFunc,
	concurrency int,
	checkHotspot bool,
	onDup common.OnDuplicateKey,
) *MergeOperator {
	// during encode&sort step, the writer-limit is aligned to block size, so we
	// need align this too. the max additional written size per file is max-block-size.
	// for max-block-size = 32MiB, adding (max-block-size * MaxMergingFilesPerThread)/10000 ~ 1MiB
	// to part-size is enough.
	partSize = max(MinUploadPartSize, partSize+units.MiB)
	logutil.Logger(ctx).Info("merge operator get part size",
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
				onClose:       onClose,
				checkHotspot:  checkHotspot,
				onDup:         onDup,
			}
		},
	)

	return &MergeOperator{
		ctx:           ctx,
		AsyncOperator: operator.NewAsyncOperator(ctx, pool),
	}
}

// String implements the Operator interface.
func (*MergeOperator) String() string {
	return "mergeOperator"
}

// Open opens the operator and starts the worker pool.
func (op *MergeOperator) Open() error {
	return op.AsyncOperator.Open()
}

// Close closes the operator and waits for all workers to finish.
func (op *MergeOperator) Close() error {
	//nolint:errcheck
	op.AsyncOperator.Close()
	return op.ctx.OperatorErr()
}

type mergeWorker struct {
	ctx *operator.Context

	store         storage.ExternalStorage
	partSize      int64
	newFilePrefix string
	blockSize     int
	onClose       OnCloseFunc
	checkHotspot  bool
	onDup         common.OnDuplicateKey
}

func (w *mergeWorker) HandleTask(task *mergeMinimalTask, _ func(workerpool.None)) {
	defer tidbutil.Recover("fast_check_table", "handleTableScanTaskWithRecover", func() {
		task.ctx.OnError(errors.Errorf("panic occurred during merge sort operator"))
	}, false)

	err := mergeOverlappingFilesInternal(
		w.ctx,
		task.files,
		w.store,
		w.partSize,
		w.newFilePrefix,
		uuid.New().String(),
		w.blockSize,
		w.onClose,
		w.checkHotspot,
		w.onDup,
	)

	if err != nil {
		w.ctx.OnError(err)
	}
}

func (*mergeWorker) Close() {}

// MergeOverlappingFiles reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
func MergeOverlappingFiles(
	ctx *operator.Context,
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
			ctx:   ctx,
			files: files,
		})
	}

	sourceOp := operator.NewSimpleDataSource(ctx, mergeTasks)
	operator.Compose(sourceOp, op)

	pipe := operator.NewAsyncPipeline(sourceOp, op)
	if err := pipe.Execute(); err != nil {
		return err
	}

	return pipe.Close()
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
	store storage.ExternalStorage,
	partSize int64,
	newFilePrefix string,
	writerID string,
	blockSize int,
	onClose OnCloseFunc,
	checkHotspot bool,
	onDup common.OnDuplicateKey,
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
	iter, err := NewMergeKVIter(ctx, paths, zeroOffsets, store, DefaultReadBufferSize, checkHotspot, 0)
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
		SetOnCloseFunc(onClose).
		SetOnDup(onDup).
		BuildOneFile(store, newFilePrefix, writerID)
	err = writer.Init(ctx, partSize)
	if err != nil {
		return nil
	}
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
		err = writer.WriteRow(ctx, iter.Key(), iter.Value())
		if err != nil {
			return err
		}
	}
	return iter.Error()
}
