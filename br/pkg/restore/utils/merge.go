// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/tablecodec"
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

func generateNewStartKey(file *backuppb.File, rewriteRules map[int64]*RewriteRules) ([]byte, []byte, error) {
	tableID := tablecodec.DecodeTableID(file.StartKey)
	if tableID == 0 {
		log.Warn("table id is 0", logutil.File(file))
		return nil, nil, errors.Annotate(berrors.ErrRestoreTableIDMismatch, "table id mismatch: table id is 0")
	}
	firstTableMetaPhysicalId := file.TableMetas[0].PhysicalId
	if tableID > firstTableMetaPhysicalId {
		log.Warn("table id of start key is larger than the first table meta's physical ID", logutil.File(file))
		return nil, nil, errors.Annotate(berrors.ErrRestoreTableIDMismatch,
			"table id mismatch: table id of start key is larger than the first table meta's physical ID")
	}
	// rewritedStartKey = rewrite(max(StartKey, encodeTableKey(firstTableMeta.PhysicalID)))
	startKey := file.StartKey
	physicalId := tableID
	if tableID < firstTableMetaPhysicalId {
		startKey = tablecodec.EncodeTableIndexPrefix(firstTableMetaPhysicalId, 0)
		physicalId = firstTableMetaPhysicalId
	}
	rules, exists := rewriteRules[physicalId]
	if !exists {
		log.Error("cannot find rewrite rule", zap.Int64("physical id", firstTableMetaPhysicalId))
		return nil, nil, errors.Annotate(berrors.ErrKVRewriteRuleNotFound, "cannot find rewrite rule")
	}
	rewritedStartKey, rule := replacePrefix(startKey, rules)
	if rule == nil {
		log.Error("cannot find rewrite rule", logutil.Key("start key", startKey))
		return nil, nil, errors.Annotate(berrors.ErrKVRewriteRuleNotFound, "cannot find rewrite rule")
	}
	return startKey, rewritedStartKey, nil
}

func generateNewEndKey(file *backuppb.File, rewriteRules map[int64]*RewriteRules) ([]byte, []byte, error) {
	tableID := tablecodec.DecodeTableID(file.EndKey)
	if tableID == 0 {
		log.Warn("table id is 0", logutil.File(file))
		return nil, nil, errors.Annotate(berrors.ErrRestoreTableIDMismatch, "table id mismatch: table id is 0")
	}
	lastTableMetaPhysicalId := file.TableMetas[len(file.TableMetas)-1].PhysicalId
	if tableID < lastTableMetaPhysicalId {
		log.Warn("table id of end key is less than the last table meta's physical ID", logutil.File(file))
		return nil, nil, errors.Annotate(berrors.ErrRestoreTableIDMismatch,
			"table id mismatch: table id of end key is less than the last table meta's physical ID")
	}
	// rewritedEndKey = rewrite(min(EndKey, encodeTableKey(lastTableMeta.PhysicalID)))
	endKey := file.EndKey
	physicalId := tableID
	if tableID > lastTableMetaPhysicalId {
		// TODO: is t{table_id}\xFF OK?
		endKey = append(tablecodec.EncodeTablePrefix(lastTableMetaPhysicalId), 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
		physicalId = lastTableMetaPhysicalId
	}
	rules, exists := rewriteRules[physicalId]
	if !exists {
		log.Error("cannot find rewrite rule", zap.Int64("physical id", physicalId))
		return nil, nil, errors.Annotate(berrors.ErrKVRewriteRuleNotFound, "cannot find rewrite rule")
	}
	rewritedEndKey, rule := replacePrefix(endKey, rules)
	if rule == nil {
		log.Error("cannot find rewrite rule", logutil.Key("start key", endKey))
		return nil, nil, errors.Annotate(berrors.ErrKVRewriteRuleNotFound, "cannot find rewrite rule")
	}
	return endKey, rewritedEndKey, nil
}

func GenerateNewKeyRange(files []*backuppb.File, rewriteRules map[int64]*RewriteRules) (*rtree.Range, error) {
	if len(files[0].TableMetas) == 0 {
		rg, err := RewriteRange(&rtree.Range{StartKey: files[0].StartKey, EndKey: files[0].EndKey}, rewriteRules)
		return rg, errors.Trace(err)
	}
	newStartKey, rewritedStartKey, err := generateNewStartKey(files[0], rewriteRules)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newEndKey, rewritedEndKey, err := generateNewEndKey(files[0], rewriteRules)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, file := range files {
		file.StartKey = newStartKey
		file.EndKey = newEndKey
	}
	return &rtree.Range{StartKey: rewritedStartKey, EndKey: rewritedEndKey}, nil
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
		rangeKey := GetFileRangeKey(file.Name)
		baseFiles := filesMap[rangeKey]
		// Assert that it has the same end key.
		if len(baseFiles) > 0 &&
			(!bytes.Equal(baseFiles[0].StartKey, file.StartKey) || !bytes.Equal(baseFiles[0].EndKey, file.EndKey)) {
			log.Panic("there are two files having the same start key, but different end key",
				zap.ByteString("file 1 start key", file.StartKey),
				zap.ByteString("file 2 start key", baseFiles[0].StartKey),
				zap.ByteString("file 1 end key", file.EndKey),
				zap.ByteString("file 2 end key", baseFiles[0].EndKey),
			)
		}
		if len(file.TableMetas) > 1 {
			// unique the files, because multi tables maybe refer the same file.
			// Notice that each type of CF can have at most one file in the same range, so
			// it's OK here having O(n^2) finding because len(baseFiles) is at most 2.
			for _, baseFile := range baseFiles {
				if baseFile.Name == file.Name {
					continue NEXTFILE
				}
			}
		}
		filesMap[rangeKey] = append(filesMap[rangeKey], file)

		// We skips all default cf files because we don't range overlap.
		if file.Cf == WriteCFName || strings.Contains(file.GetName(), WriteCFName) {
			writeCFFile++
		} else if file.Cf == DefaultCFName || strings.Contains(file.GetName(), DefaultCFName) {
			defaultCFFile++
		}
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
		// clone files and remove unused table metas
		clonedFiles := make([]*backuppb.File, 0, len(files))
		rangeSize := uint64(0)
		rangeCount := uint64(0)
		for _, file := range files {
			clonedFile := *file
			clonedTableMetas := make([]*backuppb.TableMeta, 0, len(clonedFile.TableMetas))
			for _, tableMeta := range file.TableMetas {
				// skip the filtered out kvs
				if tableMeta == nil {
					continue
				}
				// skip the not adjacent kvs
				if _, exists := rewriteRules[tableMeta.PhysicalId]; !exists {
					continue
				}
				clonedTableMetas = append(clonedTableMetas, tableMeta)
			}
			clonedFile.TableMetas = clonedTableMetas
			kvs, bytes := metautil.CalculateKvStatsOnFile(&clonedFile)
			rangeCount += kvs
			rangeSize += bytes
			clonedFiles = append(clonedFiles, &clonedFile)
			// check the table metas physical ids are the same as the first file's
			if len(clonedTableMetas) != len(clonedFiles[0].TableMetas) {
				return []rtree.RangeStats{}, nil, errors.Annotatef(berrors.ErrRestoreInvalidBackup,
					"files from the same range have the different table metas: [count %d!=%d]",
					len(clonedTableMetas), len(clonedFiles[0].TableMetas))
			}
			for i := range clonedTableMetas {
				if clonedTableMetas[i].PhysicalId != clonedFiles[0].TableMetas[i].PhysicalId {
					return []rtree.RangeStats{}, nil, errors.Annotatef(berrors.ErrRestoreInvalidBackup,
						"files from the same range have the different table metas: [index=%d][physical id %d!=%d]",
						i, clonedTableMetas[i].PhysicalId, clonedFiles[0].TableMetas[i].PhysicalId)
				}
			}
		}

		tmpRng, err := GenerateNewKeyRange(clonedFiles, rewriteRules)
		if err != nil {
			return nil, nil, errors.Annotatef(err,
				"unable to rewrite range files %+v", files)
		}
		if out := rangeTree.InsertRange(tmpRng, rangeSize, rangeCount); out != nil {
			return nil, nil, errors.Annotatef(berrors.ErrInvalidRange,
				"duplicate range %s files %+v", out, files)
		}
		totalKvs += rangeCount
		totalBytes += rangeSize
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
