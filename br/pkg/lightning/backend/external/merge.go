package external

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// MergeOverlappingFiles reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
func MergeOverlappingFiles(
	ctx context.Context,
	dataFiles []string,
	statFiles []string,
	store storage.ExternalStorage,
	readBufferSize int,
	newFilePrefix string,
	blockSize int,
	writeBatchCount uint64,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	concurrency int,
	checkHotspot bool,
) error {
	splitter, err := NewRangeSplitter(
		ctx,
		dataFiles,
		statFiles,
		store,
		// TODO(lance6716): fill these arguments
		_, _, _, _,
		checkHotspot,
	)
	if err != nil {
		return err
	}

	regionBatchSize := 40
	regions := make([][]byte, regionBatchSize+10)
	dataFilesInBatch := make(map[string]struct{}, regionBatchSize+10)
	statFilesInBatch := make(map[string]struct{}, regionBatchSize+10)
	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}

	writeToStore := func() error {
		now := time.Now()
		dataFiles := maps.Keys(dataFilesInBatch)
		slices.Sort(dataFiles)
		statFiles := maps.Keys(statFilesInBatch)
		slices.Sort(statFiles)
		err := readAllData(
			ctx,
			store,
			dataFiles,
			statFiles,
			regions[0],
			regions[len(regions)-1],
			bufPool,
			loaded,
		)
		if err != nil {
			return err
		}
		logutil.Logger(ctx).Info("reading external storage in MergeOverlappingFiles",
			zap.Duration("cost time", time.Since(now)))
		now = time.Now()
		sorty.MaxGor = uint64(concurrency)
		sorty.Sort(len(loaded.memKVs), func(i, k, r, s int) bool {
			if bytes.Compare(loaded.memKVs[i].key, loaded.memKVs[k].key) < 0 { // strict comparator like < or >
				if r != s {
					loaded.memKVs[r], loaded.memKVs[s] = loaded.memKVs[s], loaded.memKVs[r]
				}
				return true
			}
			return false
		})
		logutil.Logger(ctx).Info("sorting in MergeOverlappingFiles",
			zap.Duration("cost time", time.Since(now)))

		writer := NewWriterBuilder().
			SetMemorySizeLimit(DefaultMemSizeLimit).
			SetBlockSize(blockSize).
			SetOnCloseFunc(onClose).
			SetWriterBatchCount(writeBatchCount).
			SetPropSizeDistance(propSizeDist).
			SetPropKeysDistance(propKeysDist).
			Build(store, newFilePrefix, uuid.New().String())

		for _, kv := range loaded.memKVs {
			err = writer.WriteRow(ctx, kv.key, kv.value, nil)
			if err != nil {
				return err
			}
		}

		err = writer.Close(ctx)
		if err != nil {
			return err
		}

		// reset batch

		regions = regions[:0]
		dataFilesInBatch = make(map[string]struct{}, regionBatchSize+10)
		statFilesInBatch = make(map[string]struct{}, regionBatchSize+10)
		// only need to release buffers, because readAllData will do other
		// cleanup work.
		for _, buf := range loaded.memKVBuffers {
			buf.Destroy()
		}
		return nil
	}

	for {
		// TODO(lance6716): it's OK that regions are larger than 40?
		if len(regions) >= regionBatchSize {
			if err = writeToStore(); err != nil {
				return err
			}
		}

		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, rangeSplitKeys, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return err
		}

		regions = append(regions, rangeSplitKeys...)
		for _, f := range dataFilesOfGroup {
			dataFilesInBatch[f] = struct{}{}
		}
		for _, f := range statFilesOfGroup {
			statFilesInBatch[f] = struct{}{}
		}

		if len(endKeyOfGroup) == 0 {
			break
		}
	}

	if len(regions) > 0 {
		return writeToStore()
	}
	return nil
}

func mergeOverlappingFilesImpl(ctx context.Context, paths []string, store storage.ExternalStorage, readBufferSize int, newFilePrefix string, writerID string, memSizeLimit uint64, blockSize int, writeBatchCount uint64, propSizeDist uint64, propKeysDist uint64, onClose OnCloseFunc, checkHotspot bool, concurrency int) (err error) {
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
	iter, err := NewMergeKVIter(ctx, paths, zeroOffsets, store, readBufferSize, checkHotspot, concurrency)
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
