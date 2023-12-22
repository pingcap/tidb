package external

import (
	"bytes"
	"context"
	"fmt"
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
	"golang.org/x/sync/errgroup"
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
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	concurrency int,
	writerConcurrency int,
	checkHotspot bool,
) (err error) {
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
		err1 := splitter.Close()
		if err1 != nil {
			err = err1
			logutil.Logger(ctx).Warn("close range splitter failed", zap.Error(err))
		}
		err1 = writer.Close(ctx)
		if err1 != nil {
			err = err1
			logutil.Logger(ctx).Warn("close writer failed", zap.Error(err))
		}
	}()
	partSize = max(int64(5*size.MB), partSize+int64(1*size.MB))
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

		if len(minKey) == 0 {
			minKey = kv.Key(loaded.keys[0]).Clone()
		}
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
	return
}

type readerGroup struct {
	dataFiles []string
	statFiles []string
	startKey  kv.Key
	endKey    kv.Key
}

func getGroups(ctx context.Context, splitter *RangeSplitter, startKey kv.Key, endKey kv.Key) ([]*readerGroup, error) {
	readerGroups := make([]*readerGroup, 0, 10)
	curStart := startKey

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		curEnd := endKeyOfGroup
		if len(endKeyOfGroup) == 0 {
			curEnd = endKey
		}
		readerGroups = append(readerGroups, &readerGroup{
			dataFiles: dataFilesOfGroup,
			statFiles: statFilesOfGroup,
			startKey:  curStart.Clone(),
			endKey:    kv.Key(curEnd).Clone(),
		})

		logutil.BgLogger().Info("ywq test keys", zap.Binary("start", curStart), zap.Binary("end", curEnd))
		curStart = kv.Key(curEnd).Clone()
		if len(endKeyOfGroup) == 0 {
			break
		}
	}
	return readerGroups, nil
}

// MergeOverlappingFilesOpt reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
// Using concurrency * (readAllData and writer).
func MergeOverlappingFilesOpt(
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
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	concurrency int,
	mergeConcurrency int,
	checkHotspot bool,
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.Int("data-file-count", len(dataFiles)),
		zap.Int("stat-file-count", len(statFiles)),
		zap.Binary("start-key", startKey),
		zap.Binary("end-key", endKey),
		zap.String("new-file-prefix", newFilePrefix),
		zap.Int("concurrency", concurrency),
		zap.Int("merge concurrency", mergeConcurrency),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	rangesGroupSize := 1 * size.GB
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

	defer func() {
		err1 := splitter.Close()
		if err != nil {
			err = err1
			logutil.Logger(ctx).Warn("close range splitter failed", zap.Error(err))
		}
	}()

	if err != nil {
		logutil.Logger(ctx).Warn("new range splitter failed", zap.Error(err))
		return
	}

	groups, err := getGroups(ctx, splitter, kv.Key(startKey).Clone(), kv.Key(endKey).Clone())
	if err != nil {
		logutil.Logger(ctx).Warn("get data groups failed", zap.Error(err))
		return err
	}
	logutil.Logger(ctx).Info("get file groups for merge step", zap.Int("len", len(groups)))
	for _, g := range groups {
		logutil.BgLogger().Info("ywq test groups", zap.Binary("startKey", g.startKey), zap.Binary("endKey", g.endKey))
	}
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(mergeConcurrency)
	partSize = max(int64(5*size.MB), partSize+int64(1*size.MB))
	groupCountPerCon := math.Ceil(float64(len(groups)) / float64(mergeConcurrency))

	start := 0
	i := 0
	for ; i < mergeConcurrency-1; i++ {
		i := i
		curGroups := groups[start : start+int(groupCountPerCon)]
		eg.Go(func() error {
			return runGroups(egCtx, store, curGroups, partSize, newFilePrefix, fmt.Sprintf("%s%d", writerID, i), blockSize, propSizeDist, propKeysDist, concurrency, onClose)
		})
		start += int(groupCountPerCon)
	}
	if start < len(groups) {
		i := i
		curGroups := groups[start:]
		eg.Go(func() error {
			return runGroups(egCtx, store, curGroups, partSize, newFilePrefix, fmt.Sprintf("%s%d", writerID, i), blockSize, propSizeDist, propKeysDist, concurrency, onClose)
		})
	}
	return eg.Wait()
}

func runGroups(
	ctx context.Context,
	store storage.ExternalStorage,
	rdGroups []*readerGroup,
	partSize int64,
	newFilePrefix string,
	writerID string,
	blockSize int,
	propSizeDist uint64,
	propKeysDist uint64,
	concurrency int,
	onClose OnCloseFunc) (err error) {
	// todo count file num
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.String("writer-id", writerID),
	), "merge one group of overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	writer := NewWriterBuilder().
		SetMemorySizeLimit(DefaultMemSizeLimit*2).
		SetBlockSize(blockSize).
		SetPropKeysDistance(propKeysDist).
		SetPropSizeDistance(propSizeDist).
		SetOnCloseFunc(onClose).
		BuildOneFile(store, newFilePrefix, writerID)

	defer func() {
		err1 := writer.Close(ctx)
		if err1 != nil {
			err = err1
			logutil.Logger(ctx).Warn("close writer failed", zap.Error(err))
		}
	}()

	err = writer.Init(ctx, partSize, 20)
	if err != nil {
		return
	}

	var maxKey kv.Key
	for i, rdGroup := range rdGroups {
		bufPool := membuf.NewPool()
		loaded := &memKVsAndBuffers{}
		now := time.Now()
		err = readAllData(
			ctx,
			store,
			rdGroup.dataFiles,
			rdGroup.statFiles,
			rdGroup.startKey,
			rdGroup.endKey,
			bufPool,
			loaded,
		)
		if err != nil {
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

		now = time.Now()
		for i, key := range loaded.keys {
			err = writer.WriteRow(ctx, key, loaded.values[i])
			if err != nil {
				return err
			}
		}
		writeTime := time.Since(now)

		logutil.Logger(ctx).Info("sort one group in MergeOverlappingFiles",
			zap.Duration("read time", readTime),
			zap.Duration("sort time", sortTime),
			zap.Duration("write time", writeTime),
			zap.Int("key len", len(loaded.keys)))
		if i == len(rdGroups)-1 {
			maxKey = kv.Key(loaded.keys[len(loaded.keys)-1]).Clone()
		}

		loaded.keys = nil
		loaded.values = nil
		loaded.memKVBuffers = nil
	}

	var stat MultipleFilesStat
	stat.Filenames = append(stat.Filenames,
		[2]string{writer.dataFile, writer.statFile})
	stat.build([]kv.Key{rdGroups[0].startKey}, []kv.Key{maxKey})
	if onClose != nil {
		onClose(&WriterSummary{
			WriterID:           writer.writerID,
			Seq:                0,
			Min:                rdGroups[0].startKey,
			Max:                maxKey,
			TotalSize:          writer.totalSize,
			MultipleFilesStats: []MultipleFilesStat{stat},
		})
	}
	return
}
