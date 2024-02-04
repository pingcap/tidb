// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/rtree"
)

const (
	writeCFName   = "write"
	defaultCFName = "default"
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

// MergeFileRanges returns ranges of the files are merged based on
// splitSizeBytes and splitKeyCount.
//
// By merging small ranges, it speeds up restoring a backup that contains many
// small ranges (regions) as it reduces split region and scatter region.
func MergeFileRanges(
	files []*backuppb.File, splitSizeBytes, splitKeyCount uint64,
) ([]rtree.Range, *MergeRangesStat, error) {
	if len(files) == 0 {
		return []rtree.Range{}, &MergeRangesStat{}, nil
	}
	totalBytes := uint64(0)
	totalKvs := uint64(0)
	totalFiles := len(files)
	writeCFFile := 0
	defaultCFFile := 0

	filesMap := make(map[string][]*backuppb.File)
	for _, file := range files {
		filesMap[string(file.StartKey)] = append(filesMap[string(file.StartKey)], file)

		// We skips all default cf files because we don't range overlap.
		if file.Cf == writeCFName || strings.Contains(file.GetName(), writeCFName) {
			writeCFFile++
		} else if file.Cf == defaultCFName || strings.Contains(file.GetName(), defaultCFName) {
			defaultCFFile++
		}
		totalBytes += file.TotalBytes
		totalKvs += file.TotalKvs
	}
	if writeCFFile == 0 && defaultCFFile == 0 {
		return []rtree.Range{}, nil, errors.Annotatef(berrors.ErrRestoreInvalidBackup,
			"unknown backup data from neither Wrtie CF nor Default CF")
	}

	// RawKV does not have data in write CF.
	totalRegions := writeCFFile
	if defaultCFFile > writeCFFile {
		totalRegions = defaultCFFile
	}

	// Check if files are overlapped
	rangeTree := rtree.NewRangeTree()
	for key := range filesMap {
		files := filesMap[key]
		if out := rangeTree.InsertRange(rtree.Range{
			StartKey: files[0].GetStartKey(),
			EndKey:   files[0].GetEndKey(),
			Files:    files,
		}); out != nil {
			return nil, nil, errors.Annotatef(berrors.ErrRestoreInvalidRange,
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
