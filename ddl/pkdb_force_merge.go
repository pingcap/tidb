// Copyright 2024 PingCAP, Inc.
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
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

const forceMergeLookbackTableIDs int64 = 99

type forceMergeRange struct {
	StartTableID int64 `json:"start_table_id"`
	EndTableID   int64 `json:"end_table_id"`
}

// ForceMergeScanResult is returned by the manual force-merge scan API.
type ForceMergeScanResult struct {
	MaxTableID int64 `json:"max_table_id"`
	RangeCount int   `json:"range_count"`
}

// ForceMergeScanTask keeps the normalized PD key ranges that should be sent.
type ForceMergeScanTask struct {
	keyRanges []infosync.ForceMergeKeyRange
}

// Send pushes the scanned force-merge ranges to PD.
func (t *ForceMergeScanTask) Send(ctx context.Context) error {
	if t == nil || len(t.keyRanges) == 0 {
		return nil
	}
	return errors.Trace(sendForceMergeKeyRangesToPD(ctx, t.keyRanges))
}

// ScanForceMergeRanges scans the current infoschema and returns both the
// response summary and the task payload that can be sent to PD later.
func ScanForceMergeRanges(is infoschema.InfoSchema) (*ForceMergeScanResult, *ForceMergeScanTask) {
	maxTableID, ranges := buildAllForceMergeRanges(is)
	keyRanges := buildForceMergeKeyRanges(ranges)
	result := &ForceMergeScanResult{
		MaxTableID: maxTableID,
		RangeCount: len(keyRanges),
	}
	if len(keyRanges) == 0 {
		return result, nil
	}
	return result, &ForceMergeScanTask{keyRanges: keyRanges}
}

func buildForceMergeRanges(
	t *meta.Meta,
	schemaName string,
	tblInfo *model.TableInfo,
	physicalTableIDs []int64,
) ([]forceMergeRange, error) {
	failpoint.Inject("forceMergeBuildEntered", func(_ failpoint.Value) {})

	if shouldSkipForceMerge(schemaName, tblInfo) {
		return nil, nil
	}
	if len(physicalTableIDs) == 0 {
		return nil, nil
	}

	candidateTableIDs := make(map[int64]struct{})
	for _, physicalID := range physicalTableIDs {
		if physicalID <= 1 {
			continue
		}
		lowerBound := forceMergeLowerBound(physicalID)
		for tableID := physicalID - 1; tableID >= lowerBound; tableID-- {
			candidateTableIDs[tableID] = struct{}{}
		}
	}

	existingTableIDs, err := findExistingTableIDs(t, candidateTableIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ranges := make([]forceMergeRange, 0, len(physicalTableIDs))
	for _, physicalID := range physicalTableIDs {
		if physicalID <= 0 {
			continue
		}
		ranges = append(ranges, forceMergeRange{
			StartTableID: findForceMergeStartTableID(existingTableIDs, physicalID),
			EndTableID:   physicalID,
		})
	}
	return ranges, nil
}

func shouldSkipForceMerge(schemaName string, tblInfo *model.TableInfo) bool {
	if tblInfo == nil || !tblInfo.IsBaseTable() {
		return true
	}
	if tblInfo.TempTableType != model.TempTableNone {
		return true
	}
	if isSystemTable(schemaName, tblInfo.Name.L) {
		return true
	}
	return tblInfo.ID&autoid.SystemSchemaIDFlag != 0
}

func findExistingTableIDs(t *meta.Meta, candidateTableIDs map[int64]struct{}) (map[int64]struct{}, error) {
	existingTableIDs := make(map[int64]struct{})
	if len(candidateTableIDs) == 0 {
		return existingTableIDs, nil
	}

	dbs, err := t.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, dbInfo := range dbs {
		tables, err := t.ListTables(dbInfo.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, tblInfo := range tables {
			collectExistingTableID(existingTableIDs, candidateTableIDs, tblInfo.ID)
			if pi := tblInfo.GetPartitionInfo(); pi != nil {
				for _, def := range pi.Definitions {
					collectExistingTableID(existingTableIDs, candidateTableIDs, def.ID)
				}
			}
		}
	}
	return existingTableIDs, nil
}

func collectExistingTableID(existingTableIDs, candidateTableIDs map[int64]struct{}, tableID int64) {
	if _, ok := candidateTableIDs[tableID]; ok {
		existingTableIDs[tableID] = struct{}{}
	}
}

func findForceMergeStartTableID(existingTableIDs map[int64]struct{}, physicalID int64) int64 {
	lowerBound := forceMergeLowerBound(physicalID)
	for tableID := physicalID - 1; tableID >= lowerBound; tableID-- {
		if _, ok := existingTableIDs[tableID]; ok {
			return tableID + 1
		}
	}
	return lowerBound
}

func forceMergeLowerBound(physicalID int64) int64 {
	lowerBound := physicalID - forceMergeLookbackTableIDs
	if lowerBound < 1 {
		return 1
	}
	return lowerBound
}

func buildAllForceMergeRanges(is infoschema.InfoSchema) (int64, []forceMergeRange) {
	if is == nil {
		return 0, nil
	}

	existingTableIDs := make(map[int64]struct{})
	maxTableID := int64(0)
	for _, dbInfo := range is.AllSchemas() {
		schemaName := dbInfo.Name.L
		for _, tbl := range is.SchemaTables(dbInfo.Name) {
			tblInfo := tbl.Meta()
			if shouldSkipForceMerge(schemaName, tblInfo) {
				continue
			}

			// Force merge works on physical table ranges, so partitioned tables
			// contribute their partition IDs instead of the logical table ID.
			if pi := tblInfo.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
				for _, def := range pi.Definitions {
					if def.ID <= 0 {
						continue
					}
					existingTableIDs[def.ID] = struct{}{}
					if def.ID > maxTableID {
						maxTableID = def.ID
					}
				}
				continue
			}

			if tblInfo.ID <= 0 {
				continue
			}
			existingTableIDs[tblInfo.ID] = struct{}{}
			if tblInfo.ID > maxTableID {
				maxTableID = tblInfo.ID
			}
		}
	}

	if maxTableID == 0 {
		return 0, nil
	}

	ranges := make([]forceMergeRange, 0)
	rangeStart := int64(0)
	for tableID := int64(1); tableID <= maxTableID; tableID++ {
		if _, ok := existingTableIDs[tableID]; ok {
			if rangeStart > 0 {
				ranges = append(ranges, forceMergeRange{
					StartTableID: rangeStart,
					EndTableID:   tableID - 1,
				})
				rangeStart = 0
			}
			continue
		}
		if rangeStart == 0 {
			rangeStart = tableID
		}
	}
	if rangeStart > 0 {
		ranges = append(ranges, forceMergeRange{
			StartTableID: rangeStart,
			EndTableID:   maxTableID,
		})
	}
	return maxTableID, ranges
}

func sendForceMergeRangesToPD(ctx context.Context, ranges []forceMergeRange) error {
	keyRanges := buildForceMergeKeyRanges(ranges)
	if len(keyRanges) == 0 {
		return nil
	}
	return errors.Trace(sendForceMergeKeyRangesToPD(ctx, keyRanges))
}

func sendForceMergeKeyRangesToPD(ctx context.Context, keyRanges []infosync.ForceMergeKeyRange) error {
	failpoint.Inject("mockForceMergeSendError", func(_ failpoint.Value) {
		failpoint.Return(errors.New("mock force merge send error"))
	})
	failpoint.Inject("mockForceMergeSendSuccess", func(_ failpoint.Value) {
		failpoint.Return(nil)
	})
	return errors.Trace(infosync.AddForceMergeRanges(ctx, keyRanges))
}

func buildForceMergeKeyRanges(ranges []forceMergeRange) []infosync.ForceMergeKeyRange {
	// PD requires the submitted ranges to be sorted and non-overlapping, so normalize
	// the per-table ranges first before converting them into encoded key ranges.
	mergedRanges := mergeForceMergeRanges(ranges)
	if len(mergedRanges) == 0 {
		return nil
	}

	keyRanges := make([]infosync.ForceMergeKeyRange, 0, len(mergedRanges))
	for _, tableRange := range mergedRanges {
		// PD compares region boundary keys directly, so convert the table ID range into
		// the corresponding codec-encoded key range [tablePrefix(start), tablePrefix(end+1)).
		startKey := codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(tableRange.StartTableID))
		endKey := codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(tableRange.EndTableID+1))
		keyRanges = append(keyRanges, infosync.ForceMergeKeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		})
	}
	return keyRanges
}

func mergeForceMergeRanges(ranges []forceMergeRange) []forceMergeRange {
	if len(ranges) == 0 {
		return nil
	}

	validRanges := make([]forceMergeRange, 0, len(ranges))
	for _, tableRange := range ranges {
		if tableRange.StartTableID <= 0 || tableRange.EndTableID < tableRange.StartTableID {
			continue
		}
		validRanges = append(validRanges, tableRange)
	}
	if len(validRanges) == 0 {
		return nil
	}

	sort.Slice(validRanges, func(i, j int) bool {
		if validRanges[i].StartTableID == validRanges[j].StartTableID {
			return validRanges[i].EndTableID < validRanges[j].EndTableID
		}
		return validRanges[i].StartTableID < validRanges[j].StartTableID
	})

	mergedRanges := make([]forceMergeRange, 0, len(validRanges))
	for _, tableRange := range validRanges {
		lastIdx := len(mergedRanges) - 1
		if lastIdx >= 0 && tableRange.StartTableID <= mergedRanges[lastIdx].EndTableID+1 {
			if tableRange.EndTableID > mergedRanges[lastIdx].EndTableID {
				mergedRanges[lastIdx].EndTableID = tableRange.EndTableID
			}
			continue
		}
		mergedRanges = append(mergedRanges, tableRange)
	}
	return mergedRanges
}
