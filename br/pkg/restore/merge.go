// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
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

// NeedsMerge checks whether two ranges needs to be merged.
func NeedsMerge(left, right *rtree.Range, splitSizeBytes, splitKeyCount uint64) bool {
	leftBytes, leftKeys := left.BytesAndKeys()
	rightBytes, rightKeys := right.BytesAndKeys()
	if rightBytes == 0 {
		return true
	}
	if leftBytes+rightBytes > splitSizeBytes {
		return false
	}
	if leftKeys+rightKeys > splitKeyCount {
		return false
	}
	tableID1, indexID1, isRecord1, err1 := tablecodec.DecodeKeyHead(kv.Key(left.StartKey))
	tableID2, indexID2, isRecord2, err2 := tablecodec.DecodeKeyHead(kv.Key(right.StartKey))

	// Failed to decode the file key head... can this happen?
	if err1 != nil || err2 != nil {
		log.Warn("Failed to parse the key head for merging files, skipping",
			logutil.Key("left-start-key", left.StartKey),
			logutil.Key("right-start-key", right.StartKey),
			logutil.AShortError("left-err", err1),
			logutil.AShortError("right-err", err2),
		)
		return false
	}
	// Merge if they are both record keys
	if isRecord1 && isRecord2 {
		// Do not merge ranges in different tables.
		return tableID1 == tableID2
	}
	// If they are all index keys...
	if !isRecord1 && !isRecord2 {
		// Do not merge ranges in different indexes even if they are in the same
		// table, as rewrite rule only supports rewriting one pattern.
		// Merge left and right if they are in the same index.
		return tableID1 == tableID2 && indexID1 == indexID2
	}
	return false
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

	sortedRanges := rangeTree.GetSortedRanges()
	for i := 1; i < len(sortedRanges); {
		if !NeedsMerge(&sortedRanges[i-1], &sortedRanges[i], splitSizeBytes, splitKeyCount) {
			i++
			continue
		}
		sortedRanges[i-1].EndKey = sortedRanges[i].EndKey
		sortedRanges[i-1].Files = append(sortedRanges[i-1].Files, sortedRanges[i].Files...)
		// TODO: this is slow when there are lots of ranges need to merge.
		sortedRanges = append(sortedRanges[:i], sortedRanges[i+1:]...)
	}

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
