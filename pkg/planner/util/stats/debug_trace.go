// Copyright 2025 PingCAP, Inc.
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

package stats

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/statistics"
)

/*
 Below is debug trace for getStatsTable().
 Part of the logic for collecting information is in statistics/debug_trace.go.
*/

type getStatsTblInfo struct {
	TableName         string
	TblInfoID         int64
	InputPhysicalID   int64
	HandleIsNil       bool
	UsePartitionStats bool
	CountIsZero       bool
	Uninitialized     bool
	Outdated          bool
	StatsTblInfo      *statistics.StatsTblTraceInfo
}

func debugTraceGetStatsTbl(
	s base.PlanContext,
	tblInfo *model.TableInfo,
	pid int64,
	handleIsNil,
	usePartitionStats,
	countIsZero,
	uninitialized,
	outdated bool,
	statsTbl *statistics.Table,
) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := &getStatsTblInfo{
		TableName:         tblInfo.Name.O,
		TblInfoID:         tblInfo.ID,
		InputPhysicalID:   pid,
		HandleIsNil:       handleIsNil,
		UsePartitionStats: usePartitionStats,
		CountIsZero:       countIsZero,
		Uninitialized:     uninitialized,
		Outdated:          outdated,
		StatsTblInfo:      statistics.TraceStatsTbl(statsTbl),
	}
	failpoint.Inject("DebugTraceStableStatsTbl", func(val failpoint.Value) {
		if val.(bool) {
			stabilizeGetStatsTblInfo(traceInfo)
		}
	})
	root.AppendStepToCurrentContext(traceInfo)
}

// Only for test.
func stabilizeGetStatsTblInfo(info *getStatsTblInfo) {
	info.TblInfoID = 100
	info.InputPhysicalID = 100
	tbl := info.StatsTblInfo
	if tbl == nil {
		return
	}
	tbl.PhysicalID = 100
	tbl.Version = 440930000000000000
	for _, col := range tbl.Columns {
		col.LastUpdateVersion = 440930000000000000
	}
	for _, idx := range tbl.Indexes {
		idx.LastUpdateVersion = 440930000000000000
	}
}
