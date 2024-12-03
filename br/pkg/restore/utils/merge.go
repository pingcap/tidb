// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"go.uber.org/zap"
)

// MergeRangesStat holds statistics for the MergeRanges.
type MergeRangesStat struct {
	TotalFiles         int
	TotalWriteCFFile   int
	TotalDefaultCFFile int
	TotalRegions       int
	MergedRegions      int
	TotalKvs           uint64
	TotalBytes         uint64
}

func (stat *MergeRangesStat) MergeStat(newStat *MergeRangesStat) {
	stat.TotalFiles += newStat.TotalFiles
	stat.TotalWriteCFFile += newStat.TotalWriteCFFile
	stat.TotalDefaultCFFile += newStat.TotalDefaultCFFile
	stat.TotalRegions += newStat.TotalRegions
	stat.MergedRegions += newStat.MergedRegions
	stat.TotalKvs += newStat.TotalKvs
	stat.TotalBytes += newStat.TotalBytes
}

func (stat *MergeRangesStat) Summary() {
	regionBytesAvg := stat.TotalBytes / uint64(stat.TotalRegions)
	regionKeysAvg := stat.TotalKvs / uint64(stat.TotalRegions)
	mergedRegionBytesAvg := stat.TotalBytes / uint64(stat.MergedRegions)
	mergedRegionKeysAvg := stat.TotalKvs / uint64(stat.MergedRegions)
	log.Info("merge and validate file",
		zap.Int("Files(total)", stat.TotalFiles),
		zap.Int("File(write)", stat.TotalWriteCFFile),
		zap.Int("File(default)", stat.TotalDefaultCFFile),
		zap.Int("Region(total)", stat.TotalRegions),
		zap.Uint64("Regoin(keys avg)", regionKeysAvg),
		zap.Uint64("Region(bytes avg)", regionBytesAvg),
		zap.Int("Merged(regions)", stat.MergedRegions),
		zap.Uint64("Merged(keys avg)", mergedRegionKeysAvg),
		zap.Uint64("Merged(bytes avg)", mergedRegionBytesAvg))
}

// MergeAndRewriteFileRanges returns ranges of the files are merged based on
// splitSizeBytes and splitKeyCount.
//
// By merging small ranges, it speeds up restoring a backup that contains many
// small ranges (regions) as it reduces split region and scatter region.
func MergeAndRewriteFileRanges(
	files []*backuppb.File,
	rewriteRules map[int64]*RewriteRules,
	splitSizeBytes,
	splitKeyCount uint64,
) ([]rtree.RangeStats, *MergeRangesStat, error) {
	if len(files) == 0 {
		return []rtree.RangeStats{}, &MergeRangesStat{}, nil
	}
	totalBytes := uint64(0)
	totalKvs := uint64(0)
	totalFiles := len(files)
	writeCFFile := 0
	defaultCFFile := 0

	filesMap := make(map[string][]*backuppb.File)
NEXTFILE:
	for _, file := range files {
		baseFiles := filesMap[string(file.StartKey)]
		// Assert that it has the same end key.
		if len(baseFiles) > 0 && !bytes.Equal(baseFiles[0].EndKey, file.EndKey) {
			log.Panic("there are two files having the same start key, but different end key",
				zap.ByteString("start key", file.StartKey),
				zap.ByteString("file 1 end key", file.EndKey),
				zap.ByteString("file 2 end key", baseFiles[0].EndKey),
			)
		}
		if len(file.TableMetas) > 0 {
			// unique the files, because multi tables maybe refer the same file.
			// Notice that each type of CF can have at most one file in the same range, so
			// it's OK here having O(n^2) finding because len(baseFiles) is at most 2.
			for _, baseFile := range baseFiles {
				if baseFile.Name == file.Name {
					continue NEXTFILE
				}
			}
		}
		filesMap[string(file.StartKey)] = append(filesMap[string(file.StartKey)], file)

		// We skips all default cf files because we don't range overlap.
		if file.Cf == WriteCFName || strings.Contains(file.GetName(), WriteCFName) {
			writeCFFile++
		} else if file.Cf == DefaultCFName || strings.Contains(file.GetName(), DefaultCFName) {
			defaultCFFile++
		}
		totalBytes += file.TotalBytes
		totalKvs += file.TotalKvs
	}
	if writeCFFile == 0 && defaultCFFile == 0 {
		return []rtree.RangeStats{}, nil, errors.Annotatef(berrors.ErrRestoreInvalidBackup,
			"unknown backup data from neither Wrtie CF nor Default CF")
	}

	// RawKV does not have data in write CF.
	totalRegions := writeCFFile
	if defaultCFFile > writeCFFile {
		totalRegions = defaultCFFile
	}

	// Check if files are overlapped
	rangeTree := rtree.NewRangeStatsTree()
	for key := range filesMap {
		files := filesMap[key]
		// rewrite Range for split.
		// so that splitRanges no need to handle rewrite rules any more.
		newUpstreamStartKey, newUpstreamEndKey, tmpRng, err := RewriteFileRange(files[0], rewriteRules)
		if err != nil {
			return nil, nil, errors.Annotatef(berrors.ErrInvalidRange,
				"unable to rewrite range files %+v", files)
		}
		rangeSize := uint64(0)
		rangeCount := uint64(0)
		for _, f := range files {
			rangeSize += f.TotalBytes
			rangeCount += f.TotalKvs
			// Keep the region split key the same as file range
			f.StartKey = newUpstreamStartKey
			f.EndKey = newUpstreamEndKey
			f.TableMetas = slices.DeleteFunc(f.TableMetas, func(tableMeta *backuppb.TableMeta) bool {
				_, exists := rewriteRules[tableMeta.PhysicalId]
				return !exists
			})
		}
		tmpRng.Files = files
		if out := rangeTree.InsertRange(tmpRng, rangeSize, rangeCount); out != nil {
			return nil, nil, errors.Annotatef(berrors.ErrInvalidRange,
				"duplicate range %s files %+v", out, files)
		}
	}

	sortedRanges := rangeTree.MergedRanges(splitSizeBytes, splitKeyCount)
	return sortedRanges, &MergeRangesStat{
		TotalFiles:         totalFiles,
		TotalWriteCFFile:   writeCFFile,
		TotalDefaultCFFile: defaultCFFile,
		TotalRegions:       totalRegions,
		MergedRegions:      len(sortedRanges),
		TotalKvs:           totalKvs,
		TotalBytes:         totalBytes,
	}, nil
}
