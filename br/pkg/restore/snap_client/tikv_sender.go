// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package snapclient

import (
	"context"
	"sort"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	internalutils "github.com/pingcap/tidb/br/pkg/restore/internal/utils"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"go.uber.org/zap"
)

// DrainResult is the collection of some ranges and theirs metadata.
type DrainResult struct {
	// TablesToSend are tables that would be send at this batch.
	TablesToSend []CreatedTable
	// RewriteRules are the rewrite rules for the tables.
	// the key is the table id after rewritten.
	RewriteRulesMap map[int64]*restoreutils.RewriteRules
	Ranges          []rtree.Range
	// Record which part of ranges belongs to the table
	TableEndOffsetInRanges []int
}

// Files returns all files of this drain result.
func (result DrainResult) Files() []TableIDWithFiles {
	tableIDWithFiles := make([]TableIDWithFiles, 0, len(result.TableEndOffsetInRanges))
	var startOffset int = 0
	for i, endOffset := range result.TableEndOffsetInRanges {
		tableID := result.TablesToSend[i].Table.ID
		ranges := result.Ranges[startOffset:endOffset]
		// each range has at least a default file + a write file
		files := make([]*backuppb.File, 0, len(ranges)*2)
		for _, rg := range ranges {
			files = append(files, rg.Files...)
		}
		var rules *restoreutils.RewriteRules
		if r, ok := result.RewriteRulesMap[tableID]; ok {
			rules = r
		}
		tableIDWithFiles = append(tableIDWithFiles, TableIDWithFiles{
			TableID:      tableID,
			Files:        files,
			RewriteRules: rules,
		})

		// update start offset
		startOffset = endOffset
	}

	return tableIDWithFiles
}

func newDrainResult() DrainResult {
	return DrainResult{
		TablesToSend:           make([]CreatedTable, 0),
		RewriteRulesMap:        restoreutils.EmptyRewriteRulesMap(),
		Ranges:                 make([]rtree.Range, 0),
		TableEndOffsetInRanges: make([]int, 0),
	}
}

// MapTableToFiles makes a map that mapping table ID to its backup files.
// aware that one file can and only can hold one table.
func MapTableToFiles(files []*backuppb.File) map[int64][]*backuppb.File {
	result := map[int64][]*backuppb.File{}
	for _, file := range files {
		tableID := tablecodec.DecodeTableID(file.GetStartKey())
		tableEndID := tablecodec.DecodeTableID(file.GetEndKey())
		if tableID != tableEndID {
			log.Panic("key range spread between many files.",
				zap.String("file name", file.Name),
				logutil.Key("startKey", file.StartKey),
				logutil.Key("endKey", file.EndKey))
		}
		if tableID == 0 {
			log.Panic("invalid table key of file",
				zap.String("file name", file.Name),
				logutil.Key("startKey", file.StartKey),
				logutil.Key("endKey", file.EndKey))
		}
		result[tableID] = append(result[tableID], file)
	}
	return result
}

// fileterOutRanges filter out the files from `drained-range` that exists in the checkpoint set.
func filterOutRanges(checkpointSet map[string]struct{}, drained []rtree.Range, updateCh glue.Progress) []rtree.Range {
	progress := int(0)
	totalKVs := uint64(0)
	totalBytes := uint64(0)
	for i, rg := range drained {
		newFiles := make([]*backuppb.File, 0, len(rg.Files))
		for _, f := range rg.Files {
			rangeKey := getFileRangeKey(f.Name)
			if _, exists := checkpointSet[rangeKey]; exists {
				// the range has been import done, so skip it and
				// update the summary information
				progress += 1
				totalKVs += f.TotalKvs
				totalBytes += f.TotalBytes
			} else {
				newFiles = append(newFiles, f)
			}
		}
		// the newFiles may be empty
		drained[i].Files = newFiles
	}
	if progress > 0 {
		// (split/scatter + download/ingest) / (default cf + write cf)
		updateCh.IncBy(int64(progress) * 2 / 2)
		summary.CollectSuccessUnit(summary.TotalKV, progress, totalKVs)
		summary.CollectSuccessUnit(summary.SkippedKVCountByCheckpoint, progress, totalKVs)
		summary.CollectSuccessUnit(summary.TotalBytes, progress, totalBytes)
		summary.CollectSuccessUnit(summary.SkippedBytesByCheckpoint, progress, totalBytes)
	}
	return drained
}

// SortAndValidateFileRanges sort, merge and validate files by tables and yields
// tables with range.
func SortAndValidateFileRanges(
	ctx context.Context,
	createdTables []CreatedTable,
	allFiles []*backuppb.File,
	checkpointSetWithTableID map[int64]map[string]struct{},
	splitSizeBytes, splitKeyCount uint64,
	updateCh glue.Progress,
) (DrainResult, error) {
	// sort the created table by downstream stream table id
	sort.Slice(createdTables, func(a, b int) bool {
		return createdTables[a].Table.ID < createdTables[b].Table.ID
	})
	// mapping table ID to its backup files
	fileOfTable := MapTableToFiles(allFiles)
	// sort, merge, and validate files in each tables
	result := newDrainResult()
	for _, table := range createdTables {
		files := fileOfTable[table.OldTable.Info.ID]
		if partitions := table.OldTable.Info.Partition; partitions != nil {
			for _, partition := range partitions.Definitions {
				files = append(files, fileOfTable[partition.ID]...)
			}
		}
		for _, file := range files {
			if err := restoreutils.ValidateFileRewriteRule(file, table.RewriteRule); err != nil {
				return result, errors.Trace(err)
			}
		}
		// Merge small ranges to reduce split and scatter regions.
		ranges, stat, err := restoreutils.MergeAndRewriteFileRanges(
			files, table.RewriteRule, splitSizeBytes, splitKeyCount)
		if err != nil {
			return result, errors.Trace(err)
		}
		log.Info("merge and validate file",
			zap.Stringer("database", table.OldTable.DB.Name),
			zap.Stringer("table", table.Table.Name),
			zap.Int("Files(total)", stat.TotalFiles),
			zap.Int("File(write)", stat.TotalWriteCFFile),
			zap.Int("File(default)", stat.TotalDefaultCFFile),
			zap.Int("Region(total)", stat.TotalRegions),
			zap.Int("Regoin(keys avg)", stat.RegionKeysAvg),
			zap.Int("Region(bytes avg)", stat.RegionBytesAvg),
			zap.Int("Merged(regions)", stat.MergedRegions),
			zap.Int("Merged(keys avg)", stat.MergedRegionKeysAvg),
			zap.Int("Merged(bytes avg)", stat.MergedRegionBytesAvg))

		// mapping table ID to its rewrite rule
		result.RewriteRulesMap[table.Table.ID] = table.RewriteRule
		// appending ranges of table, and skip some ranges if recorded by checkpoint
		checkpointSet, exists := checkpointSetWithTableID[table.Table.ID]
		if exists {
			result.Ranges = append(result.Ranges, filterOutRanges(checkpointSet, ranges, updateCh)...)
		} else {
			result.Ranges = append(result.Ranges, ranges...)
		}
		// mark the indexes of each table's ranges' positions
		result.TableEndOffsetInRanges = append(result.TableEndOffsetInRanges, len(result.Ranges))
	}
	result.TablesToSend = createdTables
	return result, nil
}

func (client *SnapClient) RestoreTables(
	ctx context.Context,
	placementRuleManager PlacementRuleManager,
	createdTables []CreatedTable,
	allFiles []*backuppb.File,
	checkpointSetWithTableID map[int64]map[string]struct{},
	splitSizeBytes, splitKeyCount uint64,
	updateCh glue.Progress,
) error {
	placementRuleManager.SetPlacementRule(ctx, createdTables)
	defer placementRuleManager.ResetPlacementRules(ctx)

	result, err := SortAndValidateFileRanges(ctx, createdTables, allFiles, checkpointSetWithTableID, splitSizeBytes, splitKeyCount, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	if err = client.SplitRanges(ctx, result.Ranges, updateCh, false); err != nil {
		return errors.Trace(err)
	}

	if err = client.RestoreSSTFiles(ctx, result.Files(), updateCh); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// SplitRanges implements TiKVRestorer. It splits region by
// data range after rewrite.
func (rc *SnapClient) SplitRanges(
	ctx context.Context,
	ranges []rtree.Range,
	updateCh glue.Progress,
	isRawKv bool,
) error {
	splitClientOpts := make([]split.ClientOptionalParameter, 0, 2)
	splitClientOpts = append(splitClientOpts, split.WithOnSplit(func(keys [][]byte) {
		for range keys {
			updateCh.Inc()
		}
	}))
	if isRawKv {
		splitClientOpts = append(splitClientOpts, split.WithRawKV())
	}

	splitter := internalutils.NewRegionSplitter(split.NewClient(
		rc.pdClient,
		rc.pdHTTPClient,
		rc.tlsConf,
		maxSplitKeysOnce,
		rc.storeCount+1,
		splitClientOpts...,
	))

	return splitter.ExecuteSplit(ctx, ranges)
}
