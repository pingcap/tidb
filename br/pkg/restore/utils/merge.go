// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
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
	TotalFiles           int
	TotalWriteCFFile     int
	TotalDefaultCFFile   int
	TotalRegions         int
	RegionKeysAvg        int
	RegionBytesAvg       int
	MergedRegions        int
	MergedRegionKeysAvg  int
	MergedRegionBytesAvg int
}

// MergeAndRewriteFileRanges returns ranges of the files are merged based on
// splitSizeBytes and splitKeyCount.
//
// By merging small ranges, it speeds up restoring a backup that contains many
// small ranges (regions) as it reduces split region and scatter region.
func MergeAndRewriteFileRanges(
	files []*backuppb.File,
	rewriteRules *RewriteRules,
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
	for _, file := range files {
		filesMap[string(file.StartKey)] = append(filesMap[string(file.StartKey)], file)

		// Assert that it has the same end key.
		if !bytes.Equal(filesMap[string(file.StartKey)][0].EndKey, file.EndKey) {
			log.Panic("there are two files having the same start key, but different end key",
				zap.ByteString("start key", file.StartKey),
				zap.ByteString("file 1 end key", file.EndKey),
				zap.ByteString("file 2 end key", filesMap[string(file.StartKey)][0].EndKey),
			)
		}
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
		rangeSize := uint64(0)
		rangeCount := uint64(0)
		for _, f := range filesMap[key] {
			rangeSize += f.TotalBytes
			rangeCount += f.TotalKvs
		}
		rg := &rtree.Range{
			StartKey: files[0].GetStartKey(),
			EndKey:   files[0].GetEndKey(),
			Files:    files,
		}
		// rewrite Range for split.
		// so that splitRanges no need to handle rewrite rules any more.
		tmpRng, err := RewriteRange(rg, rewriteRules)
		if err != nil {
			return nil, nil, errors.Annotatef(berrors.ErrInvalidRange,
				"unable to rewrite range files %+v", files)
		}
		if out := rangeTree.InsertRange(tmpRng, rangeSize, rangeCount); out != nil {
			return nil, nil, errors.Annotatef(berrors.ErrInvalidRange,
				"duplicate range %s files %+v", out, files)
		}
	}

	sortedRanges := rangeTree.MergedRanges(splitSizeBytes, splitKeyCount)
	regionBytesAvg := totalBytes / uint64(totalRegions)
	regionKeysAvg := totalKvs / uint64(totalRegions)
	mergedRegionBytesAvg := totalBytes / uint64(len(sortedRanges))
	mergedRegionKeysAvg := totalKvs / uint64(len(sortedRanges))

	return sortedRanges, &MergeRangesStat{
		TotalFiles:           totalFiles,
		TotalWriteCFFile:     writeCFFile,
		TotalDefaultCFFile:   defaultCFFile,
		TotalRegions:         totalRegions,
		RegionKeysAvg:        int(regionKeysAvg),
		RegionBytesAvg:       int(regionBytesAvg),
		MergedRegions:        len(sortedRanges),
		MergedRegionKeysAvg:  int(mergedRegionKeysAvg),
		MergedRegionBytesAvg: int(mergedRegionBytesAvg),
	}, nil
}
