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

package executor

import (
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func setTiDBRowIDRangeBuilder(
	reader *TableReaderExecutor,
	scan *plannercore.PhysicalTableScan,
	partitions []table.PhysicalTable,
) {
	if len(scan.TiDBRowIDRangeOps) == 0 {
		return
	}
	allowedPhysicalIDs := make(map[int64]struct{}, len(partitions))
	for _, partition := range partitions {
		allowedPhysicalIDs[partition.GetPhysicalID()] = struct{}{}
	}
	physicalIDs := make([]int64, 0, len(scan.TiDBRowIDRangePhysicalIDs))
	keyRanges := make([]kv.KeyRange, 0, len(scan.TiDBRowIDKeyRanges))
	for i, physicalID := range scan.TiDBRowIDRangePhysicalIDs {
		if len(partitions) > 0 {
			if _, ok := allowedPhysicalIDs[physicalID]; !ok {
				continue
			}
		}
		physicalIDs = append(physicalIDs, physicalID)
		keyRanges = append(keyRanges, scan.TiDBRowIDKeyRanges[i])
	}
	reader.kvRangeBuilder = kvRangeBuilderFromTiDBRowIDRanges{
		physicalIDs: physicalIDs,
		keyRanges:   keyRanges,
	}
}

type kvRangeBuilderFromTiDBRowIDRanges struct {
	physicalIDs []int64
	keyRanges   []kv.KeyRange
}

func (h kvRangeBuilderFromTiDBRowIDRanges) buildKeyRangeSeparately(
	*distsqlctx.DistSQLContext,
	[]*ranger.Range,
) ([]int64, [][]kv.KeyRange, error) {
	ranges := make([][]kv.KeyRange, len(h.keyRanges))
	for i, keyRange := range h.keyRanges {
		ranges[i] = []kv.KeyRange{keyRange}
	}
	return h.physicalIDs, ranges, nil
}

func (h kvRangeBuilderFromTiDBRowIDRanges) buildKeyRange(
	*distsqlctx.DistSQLContext,
	[]*ranger.Range,
) ([][]kv.KeyRange, error) {
	ranges := make([][]kv.KeyRange, len(h.keyRanges))
	for i, keyRange := range h.keyRanges {
		ranges[i] = []kv.KeyRange{keyRange}
	}
	return ranges, nil
}

func findPhysicalTableIDColumn(columns []*model.ColumnInfo) int {
	for i, col := range columns {
		if col.ID == model.ExtraPhysTblID {
			return i
		}
	}
	return -1
}

func fillPhysicalTableID(key []byte, row []types.Datum, columnIdx int) {
	if columnIdx >= 0 {
		row[columnIdx] = types.NewIntDatum(tablecodec.DecodeTableID(key))
	}
}

func (e *MPPGather) isEmptySingleSourceTiDBRowIDRange(err error) bool {
	// The executor builder sets table only for an MPP plan with one data source.
	return e.table != nil && plannercore.IsEmptyTiDBRowIDMPPRangeError(err)
}
