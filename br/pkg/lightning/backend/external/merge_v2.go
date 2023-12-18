package external

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

// MergeOverlappingFilesV2 reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
// Using 1 readAllData and 1 writer.
func MergeOverlappingFilesV2(
	ctx context.Context,
	dataFiles []string,
	statFiles []string,
	store storage.ExternalStorage,
	startKey []byte,
	endKey []byte,
	partSize int64,
	newFilePrefix string,
	writerID string,
	blockSize int,
	writeBatchCount uint64,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	concurrency int,
	checkHotspot bool,
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.Int("data-file-count", len(dataFiles)),
		zap.Int("stat-file-count", len(statFiles)),
		zap.Binary("start-key", startKey),
		zap.Binary("end-key", endKey),
		zap.String("new-file-prefix", newFilePrefix),
		zap.Int("concurrency", concurrency),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	rangeGroupSize := 4 * size.GB
	failpoint.Inject("mockRangeGroupSize", func(val failpoint.Value) {
		rangeGroupSize = uint64(val.(int))
	})

	splitter, err := NewRangeSplitter(
		ctx,
		dataFiles,
		statFiles,
		store,
		int64(rangeGroupSize),
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
		SetPropKeysDistance(propKeysDist).
		SetPropSizeDistance(propSizeDist).
		SetOnCloseFunc(onClose).
		BuildOneFile(
			store,
			newFilePrefix,
			writerID)

	err = writer.Init(ctx, partSize)
	if err != nil {
		return nil
	}

	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}
	curStart := kv.Key(startKey).Clone()
	var curEnd kv.Key
	var maxKey kv.Key

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return err
		}
		curEnd = kv.Key(endKeyOfGroup).Clone()
		if len(endKeyOfGroup) == 0 {
			curEnd = kv.Key(endKey).Clone()
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
			zap.Duration("cost time", time.Since(now)),
			zap.Any("key len", len(loaded.keys)))
		now = time.Now()
		for i, key := range loaded.keys {
			err = writer.WriteRow(ctx, key, loaded.values[i])
			if err != nil {
				return err
			}
		}
		logutil.Logger(ctx).Info("writing in MergeOverlappingFiles",
			zap.Duration("cost time", time.Since(now)),
			zap.Any("key len", len(loaded.keys)))
		maxKey = kv.Key(loaded.keys[len(loaded.keys)-1]).Clone()
		curStart = curEnd.Clone()
		loaded.keys = nil
		loaded.values = nil
		loaded.memKVBuffers = nil

		if len(endKeyOfGroup) == 0 {
			break
		}
	}

	var stat MultipleFilesStat
	stat.Filenames = append(stat.Filenames,
		[2]string{writer.dataFile, writer.statFile})

	stat.build([]kv.Key{startKey}, []kv.Key{curEnd})
	if onClose != nil {
		onClose(&WriterSummary{
			WriterID:           writer.writerID,
			Seq:                0,
			Min:                startKey,
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
