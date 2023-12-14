package external

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
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
		SetPropKeysDistance(propKeysDist).
		SetPropSizeDistance(propSizeDist).
		SetOnCloseFunc(onClose).
		BuildOneFile(
			store,
			newFilePrefix,
			writerID)
	// todo consider writer conc.
	err = writer.Init(ctx, partSize)
	if err != nil {
		return nil
	}

	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}
	curStart := kv.Key(startKey).Clone()
	var curEnd kv.Key

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
		curStart = curEnd.Clone()
		if len(endKeyOfGroup) == 0 {
			break
		}
	}

	var stat MultipleFilesStat
	stat.Filenames = append(stat.Filenames,
		[2]string{writer.dataFile, writer.statFile})
	stat.build([]tidbkv.Key{startKey}, []tidbkv.Key{curEnd})
	if onClose != nil {
		onClose(&WriterSummary{
			WriterID:           writer.writerID,
			Seq:                0,
			Min:                startKey,
			Max:                curEnd, // ywq todo ... with bug.....
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

type readerGroup struct {
	dataFiles []string
	statFiles []string
	startKey  kv.Key
	endKey    kv.Key
}

func getGroups(ctx context.Context, splitter *RangeSplitter, startKey kv.Key, endKey kv.Key) ([]readerGroup, error) {
	readerGroups := make([]readerGroup, 0, 10)
	curStart := startKey

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		curEnd := kv.Key(endKeyOfGroup).Clone()
		if len(endKeyOfGroup) == 0 {
			curEnd = kv.Key(endKey).Clone()
		}
		readerGroups = append(readerGroups, readerGroup{
			dataFiles: dataFilesOfGroup,
			statFiles: statFilesOfGroup,
			startKey:  curStart,
			endKey:    curEnd,
		})

		curStart = curEnd.Clone()
		if len(endKeyOfGroup) == 0 {
			break
		}
	}
	return readerGroups, nil
}

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
	writeBatchCount uint64,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	concurrency int,
	checkHotspot bool,
) error {
	logutil.Logger(ctx).Info("enter MergeOverlappingFiles opt",
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
		1*1024*1024*1024,
		math.MaxInt64,
		4*1024*1024*1024,
		math.MaxInt64,
		checkHotspot,
	)
	if err != nil {
		return err
	}

	groups, err := getGroups(ctx, splitter, startKey, endKey)
	if err != nil {
		return err
	}
	logutil.Logger(ctx).Info("get groups", zap.Int("len", len(groups)))
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	partSize = max(int64(5*size.MB), partSize+int64(1*size.MB))
	for i, group := range groups {
		group := group
		i := i
		eg.Go(func() error {
			return runOneGroup(egCtx, store, group, partSize, newFilePrefix, fmt.Sprintf("%s%d", writerID, i), blockSize, propSizeDist, propKeysDist, onClose)
		})
	}
	return eg.Wait()
}

func runOneGroup(
	ctx context.Context,
	store storage.ExternalStorage,
	rdGroup readerGroup,
	partSize int64,
	newFilePrefix string,
	wrireID string,
	blockSize int,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc) error {
	logutil.BgLogger().Info("data files", zap.Int("len", len(rdGroup.dataFiles)))
	writer := NewWriterBuilder().
		SetMemorySizeLimit(DefaultMemSizeLimit*2).
		SetBlockSize(blockSize).
		SetPropKeysDistance(propKeysDist).
		SetPropSizeDistance(propSizeDist).
		SetOnCloseFunc(onClose).
		BuildOneFile(store, newFilePrefix, wrireID)

	err := writer.Init(ctx, partSize)
	if err != nil {
		return nil
	}

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
		return err
	}
	logutil.Logger(ctx).Info("reading external storage in MergeOverlappingFiles",
		zap.Duration("cost time", time.Since(now)))

	now = time.Now()
	sorty.MaxGor = uint64(8)
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

	var stat MultipleFilesStat
	stat.Filenames = append(stat.Filenames,
		[2]string{writer.dataFile, writer.statFile})
	stat.build([]tidbkv.Key{rdGroup.startKey}, []tidbkv.Key{loaded.keys[len(loaded.keys)-1]})
	if onClose != nil {
		onClose(&WriterSummary{
			WriterID:           writer.writerID,
			Seq:                0,
			Min:                rdGroup.startKey,
			Max:                loaded.keys[len(loaded.keys)-1],
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
