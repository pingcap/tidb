package external

import (
	"context"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// MergeOverlappingFiles reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
func MergeOverlappingFiles(ctx context.Context, paths []string, store storage.ExternalStorage, readBufferSize int,
	newFilePrefix string, blockSize int, writeBatchCount uint64, propSizeDist uint64, propKeysDist uint64,
	onClose OnCloseFunc, concurrency int, checkHotspot bool) error {
	var dataFilesSlice [][]string
	batchCount := 1
	if len(paths) > concurrency {
		batchCount = len(paths) / concurrency
	}
	for i := 0; i < len(paths); i += batchCount {
		end := i + batchCount
		if end > len(paths) {
			end = len(paths)
		}
		dataFilesSlice = append(dataFilesSlice, paths[i:end])
	}

	memTotal, err := memory.MemTotal()
	if err != nil {
		return err
	}
	memSize := (memTotal / 2) / uint64(len(dataFilesSlice))

	var eg errgroup.Group
	for _, files := range dataFilesSlice {
		files := files
		eg.Go(func() error {
			return mergeOverlappingFilesImpl(
				ctx,
				files,
				store,
				readBufferSize,
				newFilePrefix,
				uuid.New().String(),
				memSize,
				blockSize,
				writeBatchCount,
				propSizeDist,
				propKeysDist,
				onClose,
				checkHotspot,
			)
		})
	}
	return eg.Wait()
}

func mergeOverlappingFilesImpl(ctx context.Context,
	paths []string,
	store storage.ExternalStorage,
	readBufferSize int,
	newFilePrefix string,
	writerID string,
	memSizeLimit uint64,
	blockSize int,
	writeBatchCount uint64,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	checkHotspot bool,
) error {
	zeroOffsets := make([]uint64, len(paths))
	iter, err := NewMergeKVIter(ctx, paths, zeroOffsets, store, readBufferSize, checkHotspot)
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
		SetMemorySizeLimit(memSizeLimit).
		SetBlockSize(blockSize).
		SetOnCloseFunc(onClose).
		SetWriterBatchCount(writeBatchCount).
		SetPropSizeDistance(propSizeDist).
		SetPropKeysDistance(propKeysDist).
		Build(store, newFilePrefix, writerID)

	// currently use same goroutine to do read and write. The main advantage is
	// there's no KV copy and iter can reuse the buffer.
	for iter.Next() {
		err = writer.WriteRow(ctx, iter.Key(), iter.Value(), nil)
		if err != nil {
			return err
		}
	}
	err = iter.Error()
	if err != nil {
		return err
	}
	return writer.Close(ctx)
}
