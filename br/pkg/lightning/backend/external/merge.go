package external

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
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
	partSize int64,
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

	writer := NewWriterBuilder().
		SetMemorySizeLimit(DefaultMemSizeLimit).
		SetBlockSize(blockSize).
		SetWriterBatchCount(writeBatchCount).
		SetPropKeysDistance(propKeysDist).
		SetPropSizeDistance(propSizeDist).
		SetOnCloseFunc(onClose).
		BuildOneFile(store, newFilePrefix, uuid.New().String())
	err = writer.Init(ctx, partSize)
	if err != nil {
		return nil
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
		sorty.Sort(len(loaded.keys), func(i, k, r, s int) bool {
			if bytes.Compare(loaded.keys[i], loaded.keys[k]) < 0 { // strict comparator like < or >
				if r != s {
					loaded.keys[r], loaded.keys[s] = loaded.keys[s], loaded.keys[r]
					loaded.values[r], loaded.values[s] = loaded.values[s], loaded.values[r]
				}
				return true
			}
			return false
		})
		logutil.Logger(ctx).Info("sorting in MergeOverlappingFiles",
			zap.Duration("cost time", time.Since(now)))

		for i, key := range loaded.keys {
			err = writer.WriteRow(ctx, key, loaded.values[i])
			if err != nil {
				return err
			}
		}

		err = writer.Close(ctx)
		if err != nil {
			return err
		}

		curStart = curEnd
		if len(endKeyOfGroup) == 0 {
			break
		}
	}

	var stat MultipleFilesStat
	stat.Filenames = append(stat.Filenames,
		[2]string{writer.dataFile, writer.statFile})
	stat.build([]tidbkv.Key{startKey}, []tidbkv.Key{endKey})
	if onClose != nil {
		onClose(&WriterSummary{
			WriterID:           writer.writerID,
			Seq:                0,
			Min:                startKey,
			Max:                endKey,
			TotalSize:          writer.totalSize,
			MultipleFilesStats: []MultipleFilesStat{stat},
		})
	}
	return nil
}

// // mergeOverlappingFilesV2 reads from given files whose key range may overlap
// // and writes to one new sorted, nonoverlapping files.
// func mergeOverlappingFilesV2(
// 	ctx context.Context,
// 	paths []string,
// 	store storage.ExternalStorage,
// 	partSize int64,
// 	readBufferSize int,
// 	newFilePrefix string,
// 	writerID string,
// 	memSizeLimit uint64,
// 	blockSize int,
// 	writeBatchCount uint64,
// 	propSizeDist uint64,
// 	propKeysDist uint64,
// 	onClose OnCloseFunc,
// 	checkHotspot bool,
// ) (err error) {
// 	task := log.BeginTask(logutil.Logger(ctx).With(
// 		zap.String("writer-id", writerID),
// 		zap.Int("file-count", len(paths)),
// 	), "merge overlapping files")
// 	defer func() {
// 		task.End(zap.ErrorLevel, err)
// 	}()

// 	zeroOffsets := make([]uint64, len(paths))
// 	iter, err := NewMergeKVIter(ctx, paths, zeroOffsets, store, readBufferSize, checkHotspot, 0)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() {
// 		err := iter.Close()
// 		if err != nil {
// 			logutil.Logger(ctx).Warn("close iterator failed", zap.Error(err))
// 		}
// 	}()

// 	writer := NewWriterBuilder().
// 		SetMemorySizeLimit(memSizeLimit).
// 		SetBlockSize(blockSize).
// 		SetWriterBatchCount(writeBatchCount).
// 		SetPropKeysDistance(propKeysDist).
// 		SetPropSizeDistance(propSizeDist).
// 		SetOnCloseFunc(onClose).
// 		BuildOneFile(store, newFilePrefix, writerID)
// 	err = writer.Init(ctx, partSize)
// 	if err != nil {
// 		return nil
// 	}
// 	var minKey, maxKey tidbkv.Key

// 	// currently use same goroutine to do read and write. The main advantage is
// 	// there's no KV copy and iter can reuse the buffer.
// 	for iter.Next() {
// 		if len(minKey) == 0 {
// 			minKey = tidbkv.Key(iter.Key()).Clone()
// 		}
// 		err = writer.WriteRow(ctx, iter.Key(), iter.Value())
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	err = iter.Error()
// 	if err != nil {
// 		return err
// 	}
// 	maxKey = tidbkv.Key(iter.Key()).Clone()

// 	var stat MultipleFilesStat
// 	stat.Filenames = append(stat.Filenames,
// 		[2]string{writer.dataFile, writer.statFile})
// 	stat.build([]tidbkv.Key{minKey}, []tidbkv.Key{maxKey})
// 	if onClose != nil {
// 		onClose(&WriterSummary{
// 			WriterID:           writer.writerID,
// 			Seq:                0,
// 			Min:                minKey,
// 			Max:                maxKey,
// 			TotalSize:          writer.totalSize,
// 			MultipleFilesStats: []MultipleFilesStat{stat},
// 		})
// 	}

// 	err = writer.Close(ctx)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
