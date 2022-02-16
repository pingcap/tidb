// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
)

const (
	// DefaultMergeRegionSizeBytes is the default region split size, 96MB.
	// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
	DefaultMergeRegionSizeBytes uint64 = 96 * units.MiB

	// DefaultMergeRegionKeyCount is the default region key count, 960000.
	DefaultMergeRegionKeyCount uint64 = 960000

	WriteCFName   = "write"
	DefaultCFName = "default"
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
		if file.Cf == WriteCFName || strings.Contains(file.GetName(), WriteCFName) {
			writeCFFile++
		} else if file.Cf == DefaultCFName || strings.Contains(file.GetName(), DefaultCFName) {
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

	needMerge := func(left, right *rtree.Range) bool {
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
		// Do not merge ranges in different tables.
		if tablecodec.DecodeTableID(kv.Key(left.StartKey)) != tablecodec.DecodeTableID(kv.Key(right.StartKey)) {
			return false
		}
		// Do not merge ranges in different indexes even if they are in the same
		// table, as rewrite rule only supports rewriting one pattern.
		// tableID, indexID, indexValues, err
		_, indexID1, _, err1 := tablecodec.DecodeIndexKey(kv.Key(left.StartKey))
		_, indexID2, _, err2 := tablecodec.DecodeIndexKey(kv.Key(right.StartKey))
		// If both of them are index keys, ...
		if err1 == nil && err2 == nil {
			// Merge left and right if they are in the same index.
			return indexID1 == indexID2
		}
		// Otherwise, merge if they are both record keys
		return err1 != nil && err2 != nil
	}
	sortedRanges := rangeTree.GetSortedRanges()
	for i := 1; i < len(sortedRanges); {
		if !needMerge(&sortedRanges[i-1], &sortedRanges[i]) {
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
