package external

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
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

// func printMemoryUsage() {
// 	memStats := runtime.MemStats{}
// 	runtime.ReadMemStats(&memStats)

// 	logutil.BgLogger().Info(fmt.Sprintf("Total Memory: %d bytes\n", memStats.TotalAlloc/1024/1024))
// 	logutil.BgLogger().Info(fmt.Sprintf("Heap Usage: %d bytes\n", memStats.HeapAlloc/1024/1024))
// 	logutil.BgLogger().Info(fmt.Sprintf("HeapIdle: %d bytes\n", memStats.HeapIdle/1024/1024))
// 	logutil.BgLogger().Info(fmt.Sprintf("HeapInuse: %d bytes\n", memStats.HeapInuse/1024/1024))
// 	logutil.BgLogger().Info(fmt.Sprintf("Stack Inuse: %d bytes\n", memStats.StackInuse/1024/1024))
// 	logutil.BgLogger().Info(fmt.Sprintf("MSpan Inuse: %d bytes\n", memStats.MSpanInuse/1024/1024))
// 	logutil.BgLogger().Info(fmt.Sprintf("GCNum: %d\n", memStats.NumGC))
// }

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
	writerConcurrency int,
	checkHotspot bool,
) (err error) {

	// interval := 100

	// go func() {
	// 	for {
	// 		time.Sleep(time.Duration(interval) * time.Millisecond)
	// 		printMemoryUsage()
	// 	}
	// }()

	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.Int("data-file-count", len(dataFiles)),
		zap.Int("stat-file-count", len(statFiles)),
		zap.Binary("start-key", startKey),
		zap.Binary("end-key", endKey),
		zap.String("new-file-prefix", newFilePrefix),
		zap.Int("concurrency", concurrency),
		zap.Int("writer concurrency", writerConcurrency),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	rangesGroupSize := 4 * size.GB
	failpoint.Inject("mockRangesGroupSize", func(val failpoint.Value) {
		rangesGroupSize = uint64(val.(int))
	})

	splitter, err := NewRangeSplitter(
		ctx,
		dataFiles,
		statFiles,
		store,
		int64(rangesGroupSize),
		math.MaxInt64,
		int64(4*size.GB),
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
	defer func() {
		err = splitter.Close()
		if err != nil {
			logutil.Logger(ctx).Warn("close range splitter failed", zap.Error(err))
		}
		err = writer.Close(ctx)
		if err != nil {
			logutil.Logger(ctx).Warn("close writer failed", zap.Error(err))
		}
	}()

	err = writer.Init(ctx, partSize, writerConcurrency)
	if err != nil {
		logutil.Logger(ctx).Warn("init writer failed", zap.Error(err))
		return
	}

	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}
	curStart := kv.Key(startKey).Clone()
	var curEnd kv.Key
	var maxKey, minKey kv.Key
	idx := 0
	for {
		now := time.Now()
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err1 := splitter.SplitOneRangesGroup()
		if err1 != nil {
			logutil.Logger(ctx).Warn("split one ranges group failed", zap.Error(err1))
			return
		}
		curEnd = kv.Key(endKeyOfGroup).Clone()
		if len(endKeyOfGroup) == 0 {
			curEnd = kv.Key(endKey).Clone()
		}
		splitTime := time.Since(now)
		now = time.Now()
		err1 = readAllData(
			ctx,
			store,
			dataFilesOfGroup,
			statFilesOfGroup,
			curStart,
			curEnd,
			bufPool,
			loaded,
		)
		if err1 != nil {
			logutil.Logger(ctx).Warn("read all data failed", zap.Error(err1))
			return
		}
		readTime := time.Since(now)
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
		sortTime := time.Since(now)
		file, _ := os.Create(fmt.Sprintf("heap-profile-test-%d.prof", idx))
		idx++
		_ = pprof.WriteHeapProfile(file)
		now = time.Now()
		for i, key := range loaded.keys {
			err1 = writer.WriteRow(ctx, key, loaded.values[i])
			if err1 != nil {
				logutil.Logger(ctx).Warn("write one row to writer failed", zap.Error(err1))
				return
			}
		}
		writeTime := time.Since(now)
		logutil.Logger(ctx).Info("sort one group in MergeOverlappingFiles",
			zap.Duration("split time", splitTime),
			zap.Duration("read time", readTime),
			zap.Duration("sort time", sortTime),
			zap.Duration("write time", writeTime),
			zap.Int("key len", len(loaded.keys)))
		now = time.Now()

		if len(minKey) == 0 {
			minKey = kv.Key(loaded.keys[0]).Clone()
		}
		maxKey = kv.Key(loaded.keys[len(loaded.keys)-1]).Clone()
		curStart = curEnd.Clone()
		loaded.keys = nil
		loaded.values = nil
		loaded.memKVBuffers = nil
		logutil.Logger(ctx).Info("release time",
			zap.Duration("time", time.Since(now)))
		if len(endKeyOfGroup) == 0 {
			break
		}
	}

	now := time.Now()

	var stat MultipleFilesStat
	stat.Filenames = append(stat.Filenames,
		[2]string{writer.dataFile, writer.statFile})

	stat.build([]kv.Key{minKey}, []kv.Key{curEnd})
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
	logutil.Logger(ctx).Info("close time",
		zap.Duration("time", time.Since(now)))
	return
}

// ywq todo multi threaded.
// todo not use buf
