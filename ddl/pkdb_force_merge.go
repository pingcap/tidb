// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"sort"

	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
)

// GetForceMergeRangesForGCDeleteRange builds the PD force-merge ranges that
// should be reported for one gc_delete_range task.
func GetForceMergeRangesForGCDeleteRange(
	historyJob *model.Job,
	dr ddlutil.DelRangeTask,
	gcLogicalTableCache map[int64]struct{},
) []infosync.ForceMergeKeyRange {
	if !variable.EnableDropTableForceMerge.Load() || historyJob == nil {
		return nil
	}
	if !supportForceMergeForGCDeleteRange(historyJob.Type) {
		return nil
	}

	var tblInfo *model.TableInfo
	if historyJob.BinlogInfo != nil {
		tblInfo = historyJob.BinlogInfo.TableInfo
	}
	if shouldSkipForceMerge(historyJob, tblInfo) {
		return nil
	}

	// Reuse the gc_delete_range task range directly for the physical table/partition
	// that has just been deleted.
	ranges := []infosync.ForceMergeKeyRange{buildForceMergeKeyRange(dr.StartKey, dr.EndKey)}
	if shouldReportLogicalTableRange(historyJob, tblInfo, gcLogicalTableCache) {
		// DROP/TRUNCATE on a partitioned table only creates gc_delete_range tasks for
		// physical partitions. Append the logical table range once as well so PD can
		// force merge the partitioned table's global index keyspace.
		ranges = append(ranges, buildForceMergeTableKeyRange(historyJob.TableID))
	}
	return ranges
}

func supportForceMergeForGCDeleteRange(actionType model.ActionType) bool {
	switch actionType {
	case model.ActionDropTable, model.ActionTruncateTable,
		model.ActionDropTablePartition, model.ActionTruncateTablePartition:
		return true
	}
	return false
}

func shouldSkipForceMerge(historyJob *model.Job, tblInfo *model.TableInfo) bool {
	if historyJob == nil {
		return true
	}

	tableName := historyJob.TableName
	if tblInfo != nil && tableName == "" {
		tableName = tblInfo.Name.L
	}
	return shouldSkipForceMergeTable(historyJob.SchemaName, tableName, tblInfo)
}

func shouldSkipForceMergeTable(schemaName string, tableName string, tblInfo *model.TableInfo) bool {
	if tblInfo != nil {
		if !tblInfo.IsBaseTable() {
			return true
		}
		if tblInfo.TempTableType != model.TempTableNone {
			return true
		}
		if tblInfo.ID&autoid.SystemSchemaIDFlag != 0 {
			return true
		}
	}
	return isSystemTable(schemaName, tableName)
}

func shouldReportLogicalTableRange(
	historyJob *model.Job,
	tblInfo *model.TableInfo,
	gcLogicalTableCache map[int64]struct{},
) bool {
	// Partition-level gc_delete_range tasks do not cover the logical table ID range.
	// That range must be reported separately for DROP/TRUNCATE of partitioned tables
	// because global indexes are stored under the logical table ID prefix.
	if historyJob == nil || historyJob.TableID <= 0 || tblInfo == nil || tblInfo.GetPartitionInfo() == nil {
		return false
	}
	if historyJob.Type != model.ActionDropTable && historyJob.Type != model.ActionTruncateTable {
		return false
	}
	if gcLogicalTableCache != nil {
		if _, ok := gcLogicalTableCache[historyJob.TableID]; ok {
			return false
		}
		gcLogicalTableCache[historyJob.TableID] = struct{}{}
	}
	return true
}

func buildForceMergeKeyRange(startKey []byte, endKey []byte) infosync.ForceMergeKeyRange {
	return infosync.ForceMergeKeyRange{
		StartKey: append([]byte(nil), startKey...),
		EndKey:   append([]byte(nil), endKey...),
	}
}

// GetMergeEmptyRegionsKeyRanges scans the current infoschema in
// [minTableID, maxTableID) and returns the missing table ID keyspace ranges.
func GetMergeEmptyRegionsKeyRanges(
	is infoschema.InfoSchema,
	minTableID int64,
) (maxTableID int64, ranges []infosync.ForceMergeKeyRange) {
	if is == nil {
		return 0, nil
	}
	if minTableID < 1 {
		minTableID = 1
	}

	occupiedTableIDs, maxTableID := collectMergeEmptyRegionsOccupiedTableIDs(is)
	if maxTableID == 0 || minTableID >= maxTableID {
		return maxTableID, nil
	}
	return maxTableID, buildMergeEmptyRegionsKeyRanges(occupiedTableIDs, minTableID, maxTableID)
}

func collectMergeEmptyRegionsOccupiedTableIDs(is infoschema.InfoSchema) ([]int64, int64) {
	occupiedTableIDs := make([]int64, 0)
	maxTableID := int64(0)
	for _, dbInfo := range is.AllSchemas() {
		schemaName := dbInfo.Name.L
		for _, tbl := range is.SchemaTables(dbInfo.Name) {
			tblInfo := tbl.Meta()
			if !mergeEmptyRegionsTableOccupiesKeyspace(tblInfo) {
				continue
			}

			tableIDs := appendMergeEmptyRegionsTableIDs(nil, tblInfo)
			occupiedTableIDs = append(occupiedTableIDs, tableIDs...)
			// Keep skipped live tables as occupied keyspace so the background scan
			// never force merges through system or temporary table ranges.
			if shouldSkipForceMergeTable(schemaName, tblInfo.Name.L, tblInfo) {
				continue
			}
			for _, tableID := range tableIDs {
				if tableID > maxTableID {
					maxTableID = tableID
				}
			}
		}
	}
	return occupiedTableIDs, maxTableID
}

func buildMergeEmptyRegionsKeyRanges(
	occupiedTableIDs []int64,
	minTableID int64,
	maxTableID int64,
) []infosync.ForceMergeKeyRange {
	if minTableID < 1 || minTableID >= maxTableID {
		return nil
	}

	sort.Slice(occupiedTableIDs, func(i, j int) bool {
		return occupiedTableIDs[i] < occupiedTableIDs[j]
	})

	ranges := make([]infosync.ForceMergeKeyRange, 0)
	nextRangeStart := minTableID
	for _, occupiedTableID := range occupiedTableIDs {
		if occupiedTableID < nextRangeStart {
			continue
		}
		if occupiedTableID >= maxTableID {
			break
		}
		if occupiedTableID > nextRangeStart {
			ranges = append(ranges, buildMergeEmptyRegionsTableIDRange(nextRangeStart, occupiedTableID-1))
		}
		nextRangeStart = occupiedTableID + 1
	}
	if nextRangeStart < maxTableID {
		ranges = append(ranges, buildMergeEmptyRegionsTableIDRange(nextRangeStart, maxTableID-1))
	}
	return ranges
}

func mergeEmptyRegionsTableOccupiesKeyspace(tblInfo *model.TableInfo) bool {
	return tblInfo != nil && tblInfo.IsBaseTable()
}

func appendMergeEmptyRegionsTableIDs(dst []int64, tblInfo *model.TableInfo) []int64 {
	if tblInfo == nil {
		return dst
	}

	pi := tblInfo.GetPartitionInfo()
	if pi != nil && len(pi.Definitions) > 0 {
		for _, def := range pi.Definitions {
			if def.ID > 0 {
				dst = append(dst, def.ID)
			}
		}
		// Partitioned table global indexes live under the logical table ID prefix.
		if hasGlobalIndex(tblInfo) && tblInfo.ID > 0 {
			dst = append(dst, tblInfo.ID)
		}
		return dst
	}
	if tblInfo.ID > 0 {
		dst = append(dst, tblInfo.ID)
	}
	return dst
}

func buildMergeEmptyRegionsTableIDRange(startTableID int64, endTableID int64) infosync.ForceMergeKeyRange {
	return infosync.ForceMergeKeyRange{
		StartKey: tablecodec.EncodeTablePrefix(startTableID),
		EndKey:   tablecodec.EncodeTablePrefix(endTableID + 1),
	}
}

func buildForceMergeTableKeyRange(tableID int64) infosync.ForceMergeKeyRange {
	return infosync.ForceMergeKeyRange{
		StartKey: tablecodec.EncodeTablePrefix(tableID),
		EndKey:   tablecodec.EncodeTablePrefix(tableID + 1),
	}
}
