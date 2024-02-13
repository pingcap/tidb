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

package exec

import (
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

// IndexUsageReporter is a toolkit to report index usage
type IndexUsageReporter struct {
	reporter         *indexusage.StmtIndexUsageCollector
	runtimeStatsColl *execdetails.RuntimeStatsColl
	statsMap         *stmtctx.UsedStatsInfo
}

// NewIndexUsageReporter creates an index usage reporter util
func NewIndexUsageReporter(reporter *indexusage.StmtIndexUsageCollector,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	statsMap *stmtctx.UsedStatsInfo) *IndexUsageReporter {
	return &IndexUsageReporter{
		reporter:         reporter,
		runtimeStatsColl: runtimeStatsColl,
		statsMap:         statsMap,
	}
}

// ReportCopIndexUsageForTable wraps around `ReportCopIndexUsage` to get `tableID` and `physicalTableID` from the
// `table.Table`. If it's expected to calculate the percentage according to the size of partition, the `tbl` argument
// should be a `table.PhysicalTable`, or the percentage will be calculated using the size of whole table.
func (e *IndexUsageReporter) ReportCopIndexUsageForTable(tbl table.Table, indexID int64, planID int) {
	tableID := tbl.Meta().ID
	physicalTableID := tableID
	if physicalTable, ok := tbl.(table.PhysicalTable); ok {
		physicalTableID = physicalTable.GetPhysicalID()
	}

	e.ReportCopIndexUsage(tableID, physicalTableID, indexID, planID)
}

// ReportCopIndexUsage reports the index usage to the inside collector. The index usage will be recorded in the
// `tableID+indexID`, but the percentage is calculated using the size of the table specified by `physicalTableID`.
func (e *IndexUsageReporter) ReportCopIndexUsage(tableID int64, physicalTableID int64, indexID int64, planID int) {
	tableRowCount, ok := e.getTableRowCount(physicalTableID)
	if !ok {
		// skip if the table is empty or the stats is not valid
		return
	}

	copStats := e.runtimeStatsColl.GetCopStats(planID)
	if copStats == nil {
		return
	}
	copStats.Lock()
	defer copStats.Unlock()
	kvReq := copStats.GetTasks()
	accessRows := copStats.GetActRows()

	sample := indexusage.NewSample(0, uint64(kvReq), uint64(accessRows), uint64(tableRowCount))
	e.reporter.Update(tableID, indexID, sample)
}

// ReportPointGetIndexUsage reports the index usage of a point get or batch point get
func (e *IndexUsageReporter) ReportPointGetIndexUsage(tableID int64, physicalTableID int64, indexID int64, planID int, kvRequestTotal int64) {
	tableRowCount, ok := e.getTableRowCount(physicalTableID)
	if !ok {
		// skip if the table is empty or the stats is not valid
		return
	}

	basic := e.runtimeStatsColl.GetBasicRuntimeStats(planID)
	if basic == nil {
		return
	}
	accessRows := basic.GetActRows()

	sample := indexusage.NewSample(0, uint64(kvRequestTotal), uint64(accessRows), uint64(tableRowCount))
	e.reporter.Update(tableID, indexID, sample)
}

// getTableRowCount returns the `RealtimeCount` of a table
func (e *IndexUsageReporter) getTableRowCount(tableID int64) (int64, bool) {
	stats := e.statsMap.GetUsedInfo(tableID)
	if stats == nil {
		return 0, false
	}
	if stats.Version == statistics.PseudoVersion {
		return 0, false
	}
	return stats.RealtimeCount, true
}
