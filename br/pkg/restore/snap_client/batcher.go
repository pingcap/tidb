// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package snapclient

import (
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/summary"
)

// DrainResult is the collection of some ranges and theirs metadata.
type DrainResult struct {
	// TablesToSend are tables that would be send at this batch.
	TablesToSend []CreatedTable
	// BlankTablesAfterSend are tables that will be full-restored after this batch send.
	BlankTablesAfterSend []CreatedTable
	// RewriteRules are the rewrite rules for the tables.
	// the key is the table id after rewritten.
	RewriteRulesMap map[int64]*utils.RewriteRules
	Ranges          []rtree.RangeStats
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
		var rules *utils.RewriteRules
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
		BlankTablesAfterSend:   make([]CreatedTable, 0),
		RewriteRulesMap:        utils.EmptyRewriteRulesMap(),
		Ranges:                 make([]rtree.RangeStats, 0),
		TableEndOffsetInRanges: make([]int, 0),
	}
}

// fileterOutRanges filter out the files from `drained-range` that exists in the checkpoint set.
func filterOutRanges(checkpointSet map[string]struct{}, drained []rtree.RangeStats, updateCh glue.Progress) []rtree.RangeStats {
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

// drainRanges 'drains' ranges from current tables.
// for example, let a '-' character be a range, assume we have:
// |---|-----|-------|
// |t1 |t2   |t3     |
// after we run drainRanges() with batchSizeThreshold = 6, let '*' be the ranges will be sent this batch :
// |***|***--|-------|
// |t1 |t2   |-------|
//
// drainRanges() will return:
// TablesToSend: [t1, t2] (so we can make them enter restore mode)
// BlankTableAfterSend: [t1] (so we can make them leave restore mode after restoring this batch)
// RewriteRules: rewrite rules for [t1, t2] (so we can restore them)
// Ranges: those stared ranges (so we can restore them)
//
// then, it will leaving the batcher's cachedTables like this:
// |--|-------|
// |t2|t3     |
// as you can see, all restored ranges would be removed.
func drainRanges(
	tableWithRanges []TableWithRange,
	checkpointSetWithTableID map[int64]map[string]struct{},
	updateCh glue.Progress,
) DrainResult {
	result := newDrainResult()

	for offset, thisTable := range tableWithRanges {
		t, exists := checkpointSetWithTableID[thisTable.Table.ID]

		result.RewriteRulesMap[thisTable.Table.ID] = thisTable.RewriteRule
		result.TablesToSend = append(result.TablesToSend, thisTable.CreatedTable)
		result.BlankTablesAfterSend = append(result.BlankTablesAfterSend, thisTable.CreatedTable)
		// let's 'drain' the ranges of current table. This op must not make the batch full.
		if exists {
			result.Ranges = append(result.Ranges, filterOutRanges(t, thisTable.Range, updateCh)...)
		} else {
			result.Ranges = append(result.Ranges, thisTable.Range...)
		}
		result.TableEndOffsetInRanges = append(result.TableEndOffsetInRanges, len(result.Ranges))
		// clear the table length.
		tableWithRanges[offset].Range = []rtree.RangeStats{}
	}

	return result
}
