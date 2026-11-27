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

package kvsource

import (
	"context"

	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

// VisibleColumns selects visible writable columns, optionally including stored generated columns.
func VisibleColumns(table *model.TableInfo, includeStoredGenerated bool) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(table.Columns))
	for _, column := range table.Cols() {
		if column == nil || column.Hidden || column.IsVirtualGenerated() {
			continue
		}
		if column.IsGenerated() && !includeStoredGenerated {
			continue
		}
		columns = append(columns, column)
	}
	return columns
}

func physicalTableIDs(table *model.TableInfo) []int64 {
	partition := table.GetPartitionInfo()
	if partition == nil || len(partition.Definitions) == 0 {
		return []int64{table.ID}
	}
	tableIDs := make([]int64, 0, len(partition.Definitions))
	for _, definition := range partition.Definitions {
		tableIDs = append(tableIDs, definition.ID)
	}
	return tableIDs
}

func physicalTableRanges(table *model.TableInfo) []dumpservice.Range {
	tableIDs := physicalTableIDs(table)
	ranges := make([]dumpservice.Range, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		start := tablecodec.GenTableRecordPrefix(tableID)
		ranges = append(ranges, dumpservice.Range{Start: start, End: start.PrefixNext()})
	}
	return ranges
}

// ShardTableRanges discovers service shards for each physical table or partition.
func ShardTableRanges(
	ctx context.Context,
	scanner Scanner,
	table *model.TableInfo,
) ([]dumpservice.Range, error) {
	physicalRanges := physicalTableRanges(table)
	shards := make([]dumpservice.Range, 0, len(physicalRanges))
	for _, physicalRange := range physicalRanges {
		ranges, err := scanner.Shards(ctx, physicalRange.Start, physicalRange.End)
		if err != nil {
			return nil, err
		}
		shards = append(shards, ranges...)
	}
	return shards, nil
}
