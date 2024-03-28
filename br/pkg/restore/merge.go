// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"go.uber.org/zap"
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
		rangeSize := uint64(0)
		for _, f := range filesMap[key] {
			rangeSize += f.Size_
		}
		rg := &rtree.Range{
			StartKey: files[0].GetStartKey(),
			EndKey:   files[0].GetEndKey(),
			Files:    files,
			Size:     rangeSize,
		}
		// rewrite Range for split.
		// so that splitRanges no need to handle rewrite rules any more.
		tmpRng, err := RewriteRange(rg, rewriteRules)
		if err != nil {
			return nil, nil, errors.Annotatef(berrors.ErrInvalidRange,
				"unable to rewrite range files %+v", files)
		}
		if out := rangeTree.InsertRange(*tmpRng); out != nil {
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

func RewriteRange(rg *rtree.Range, rewriteRules *RewriteRules) (*rtree.Range, error) {
	if rewriteRules == nil {
		return rg, nil
	}
	startID := tablecodec.DecodeTableID(rg.StartKey)
	endID := tablecodec.DecodeTableID(rg.EndKey)
	var rule *import_sstpb.RewriteRule
	if startID != endID {
		log.Warn("table id does not match",
			logutil.Key("startKey", rg.StartKey),
			logutil.Key("endKey", rg.EndKey),
			zap.Int64("startID", startID),
			zap.Int64("endID", endID))
		return nil, errors.Annotate(berrors.ErrRestoreTableIDMismatch, "table id mismatch")
	}
	rg.StartKey, rule = replacePrefix(rg.StartKey, rewriteRules)
	if rule == nil {
		log.Warn("cannot find rewrite rule", logutil.Key("key", rg.StartKey))
	} else {
		log.Debug(
			"rewrite start key",
			logutil.Key("key", rg.StartKey), logutil.RewriteRule(rule))
	}
	oldKey := rg.EndKey
	rg.EndKey, rule = replacePrefix(rg.EndKey, rewriteRules)
	if rule == nil {
		log.Warn("cannot find rewrite rule", logutil.Key("key", rg.EndKey))
	} else {
		log.Debug(
			"rewrite end key",
			logutil.Key("origin-key", oldKey),
			logutil.Key("key", rg.EndKey),
			logutil.RewriteRule(rule))
	}
	return rg, nil
}
