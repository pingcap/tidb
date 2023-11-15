package external

import (
	"bytes"
	"context"
	"math"
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
)

// MergeOverlappingFiles reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
func MergeOverlappingFiles(
	ctx context.Context,
	dataFiles []string,
	statFiles []string,
	store storage.ExternalStorage,
	startKey []byte,
	endKey []byte,
	newFilePrefix string,
	blockSize int,
	writeBatchCount uint64,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	concurrency int,
	checkHotspot bool,
) error {
	logutil.Logger(ctx).Info("enter MergeOverlappingFiles",
		zap.Int("data-file-count", len(dataFiles)),
		zap.Int("stat-file-count", len(statFiles)),
		zap.Binary("start-key", startKey),
		zap.Binary("end-key", endKey),
		zap.String("new-file-prefix", newFilePrefix),
		zap.Int("concurrency", concurrency),
	)

	splitter, err := NewRangeSplitter(
		ctx,
		dataFiles,
		statFiles,
		store,
		4*1024*1024*1024,
		math.MaxInt64,
		4*1024*1024*1024,
		math.MaxInt64,
		checkHotspot,
	)
	if err != nil {
		return err
	}

	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}
	curStart := startKey

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return err
		}
		curEnd := endKeyOfGroup
		if len(endKeyOfGroup) == 0 {
			curEnd = endKey
		}

		now := time.Now()
		err = readAllData(
			ctx,
			store,
			dataFilesOfGroup,
			statFilesOfGroup,
			curStart,
			curEnd,
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

		// only need to release buffers, because readAllData will do other
		// cleanup work.
		for _, buf := range loaded.memKVBuffers {
			buf.Destroy()
		}

		curStart = curEnd
		if len(endKeyOfGroup) == 0 {
			break
		}
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
