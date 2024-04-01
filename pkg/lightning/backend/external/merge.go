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

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

// MergeOverlappingFiles reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
func MergeOverlappingFiles(
	ctx context.Context,
	paths []string,
	store storage.ExternalStorage,
	partSize int64,
	newFilePrefix string,
	blockSize int,
	onClose OnCloseFunc,
	concurrency int,
	checkHotspot bool,
) error {
	dataFilesSlice := splitDataFiles(paths, concurrency)
	// during encode&sort step, the writer-limit is aligned to block size, so we
	// need align this too. the max additional written size per file is max-block-size.
	// for max-block-size = 32MiB, adding (max-block-size * MaxMergingFilesPerThread)/10000 ~ 1MiB
	// to part-size is enough.
	partSize = max(MinUploadPartSize, partSize+units.MiB)

	logutil.Logger(ctx).Info("start to merge overlapping files",
		zap.Int("file-count", len(paths)),
		zap.Int("file-groups", len(dataFilesSlice)),
		zap.Int("concurrency", concurrency),
		zap.Int64("part-size", partSize))
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	for _, files := range dataFilesSlice {
		files := files
		eg.Go(func() error {
			return mergeOverlappingFilesInternal(
				egCtx,
				files,
				store,
				partSize,
				newFilePrefix,
				uuid.New().String(),
				blockSize,
				onClose,
				checkHotspot,
			)
		})
	}
	return eg.Wait()
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
//	+ MaxMergingFilesPerThread * (X + defaultReadBufferSize)
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
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.String("writer-id", writerID),
		zap.Int("file-count", len(paths)),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	zeroOffsets := make([]uint64, len(paths))
	iter, err := NewMergeKVIter(ctx, paths, zeroOffsets, store, defaultReadBufferSize, checkHotspot, 0)
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
