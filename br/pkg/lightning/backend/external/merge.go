package external

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// MergeOverlappingFiles reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
func MergeOverlappingFiles(ctx context.Context, paths []string, store storage.ExternalStorage, partSize int64, readBufferSize int,
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

	logutil.Logger(ctx).Info("start to merge overlapping files",
		zap.Int("file-count", len(paths)),
		zap.Int("file-groups", len(dataFilesSlice)),
		zap.Int("concurrency", concurrency))
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	partSize = max(int64(5*size.MB), partSize+int64(1*size.MB))
	for _, files := range dataFilesSlice {
		files := files
		eg.Go(func() error {
			return MergeOverlappingFilesV2(
				egCtx,
				files,
				store,
				partSize,
				readBufferSize,
				newFilePrefix,
				uuid.New().String(),
				DefaultMemSizeLimit,
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

// unused for now.
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
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.String("writer-id", writerID),
		zap.Int("file-count", len(paths)),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

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

// MergeOverlappingFilesV2 reads from given files whose key range may overlap
// and writes to one new sorted, nonoverlapping files.
func MergeOverlappingFilesV2(
	ctx context.Context,
	paths []string,
	store storage.ExternalStorage,
	partSize int64,
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
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.String("writer-id", writerID),
		zap.Int("file-count", len(paths)),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	failpoint.Inject("mergeOverlappingFilesImpl", func(val failpoint.Value) {
		if val.(string) == paths[0] {
			failpoint.Return(errors.New("injected error"))
		} else {
			select {
			case <-ctx.Done():
				failpoint.Return(ctx.Err())
			}
		}
	})

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
		SetWriterBatchCount(writeBatchCount).
		SetPropKeysDistance(propKeysDist).
		SetPropSizeDistance(propSizeDist).
		SetOnCloseFunc(onClose).
		BuildOneFile(store, newFilePrefix, writerID)
	err = writer.Init(ctx, partSize)
	if err != nil {
		return nil
	}
	var minKey, maxKey tidbkv.Key

	// currently use same goroutine to do read and write. The main advantage is
	// there's no KV copy and iter can reuse the buffer.
	for iter.Next() {
		if len(minKey) == 0 {
			minKey = tidbkv.Key(iter.Key()).Clone()
		}
		err = writer.WriteRow(ctx, iter.Key(), iter.Value())
		if err != nil {
			return err
		}
	}
	err = iter.Error()
	if err != nil {
		return err
	}
	maxKey = tidbkv.Key(iter.Key()).Clone()

	var stat MultipleFilesStat
	stat.Filenames = append(stat.Filenames,
		[2]string{writer.dataFile, writer.statFile})
	stat.build([]tidbkv.Key{minKey}, []tidbkv.Key{maxKey})
	if onClose != nil {
		onClose(&WriterSummary{
			WriterID:           writer.writerID,
			Seq:                0,
			Min:                minKey,
			Max:                maxKey,
			TotalSize:          writer.totalSize,
			MultipleFilesStats: []MultipleFilesStat{stat},
		})
	}

	err = writer.Close(ctx)
	if err != nil {
		return err
	}
	return nil
}
