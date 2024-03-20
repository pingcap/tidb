package external

import (
	"context"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/storage"
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
	partSize = max(MinUploadPartSize, partSize)

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
	shares := max(len(paths)/MaxMergingFilesPerThread, concurrency)
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
//	memSizeLimit
//	+ 20 * partSize
//	+ 20 * 5MiB(stat file, we might not use all part, as stat file is quite small)
//	+ readBufferSize * len(paths)
//	+ memory taken by concurrent reading if check-hotspot is enabled
//
// memSizeLimit = 256 MiB now.
// partSize = index-kv-data-file-size / (10000 / MergeSortOverlapThreshold) for import into.
// readBufferSize = 64 KiB now.
// len(paths) >= kv-files-in-subtask(suppose MergeSortOverlapThreshold) / concurrency
//
// TODO: seems it might OOM if partSize = 256 / (10000/4000) = 100 MiB, when write
// external storage is slow.
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
