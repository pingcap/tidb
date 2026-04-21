// Copyright 2023 PingCAP, Inc.
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

package cardinality

import (
	statsutil "github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
)

// recordUsedItemStatsStatus only records un-FullLoad item load status during user query
func recordUsedItemStatsStatus(sctx planctx.PlanContext, stats any, tableID, id int64) {
	// Sometimes we try to use stats on _tidb_rowid (id == -1), which must be empty, we ignore this case here.
	if id <= 0 {
		return
	}
	var isIndex, missing bool
	var loadStatus *statistics.StatsLoadedStatus
	switch x := stats.(type) {
	case *statistics.Column:
		isIndex = false
		if x == nil {
			missing = true
		} else {
			loadStatus = &x.StatsLoadedStatus
		}
	case *statistics.Index:
		isIndex = true
		if x == nil {
			missing = true
		} else {
			loadStatus = &x.StatsLoadedStatus
		}
	}

	// no need to record
	if !missing && loadStatus != nil && loadStatus.IsFullLoad() {
		return
	}

	// need to record
	statsRecord := sctx.GetSessionVars().StmtCtx.GetUsedStatsInfo(true)
	if statsRecord.GetUsedInfo(tableID) == nil {
		name, tblInfo := statsutil.GetTblInfoForUsedStatsByPhysicalID(sctx, tableID)
		statsRecord.RecordUsedInfo(tableID, &stmtctx.UsedStatsInfoForTable{
			Name:    name,
			TblInfo: tblInfo,
		})
	}
	recordForTbl := statsRecord.GetUsedInfo(tableID)

	var recordForColOrIdx map[int64]string
	if isIndex {
		if recordForTbl.IndexStatsLoadStatus == nil {
			recordForTbl.IndexStatsLoadStatus = make(map[int64]string, 1)
		}
		recordForColOrIdx = recordForTbl.IndexStatsLoadStatus
	} else {
		if recordForTbl.ColumnStatsLoadStatus == nil {
			recordForTbl.ColumnStatsLoadStatus = make(map[int64]string, 1)
		}
		recordForColOrIdx = recordForTbl.ColumnStatsLoadStatus
	}

	if missing {
		// Figure out whether it's really not existing.
		if recordForTbl.ColAndIdxStatus != nil && recordForTbl.ColAndIdxStatus.(*statistics.ColAndIdxExistenceMap).HasAnalyzed(id, isIndex) {
			// If this item has been analyzed but there's no its stats, we should mark it as uninitialized.
			recordForColOrIdx[id] = statistics.StatsLoadedStatus{}.StatusToString()
		} else {
			// Otherwise, we mark it as missing.
			recordForColOrIdx[id] = "missing"
		}
		return
	}
	recordForColOrIdx[id] = loadStatus.StatusToString()
}
