package stats

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
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

/*
 Below is debug trace for AccessPath.
*/

type accessPathForDebugTrace struct {
	IndexName           string `json:",omitempty"`
	AccessConditions    []string
	IndexFilters        []string
	TableFilters        []string
	PartialPaths        []accessPathForDebugTrace `json:",omitempty"`
	CountAfterAccess    float64
	MinCountAfterAccess float64
	MaxCountAfterAccess float64
	CountAfterIndex     float64
}

func convertAccessPathForDebugTrace(ctx expression.EvalContext, path *util.AccessPath, out *accessPathForDebugTrace) {
	if path.Index != nil {
		out.IndexName = path.Index.Name.O
	}
	out.AccessConditions = expression.ExprsToStringsForDisplay(ctx, path.AccessConds)
	out.IndexFilters = expression.ExprsToStringsForDisplay(ctx, path.IndexFilters)
	out.TableFilters = expression.ExprsToStringsForDisplay(ctx, path.TableFilters)
	out.CountAfterAccess = path.CountAfterAccess
	out.MaxCountAfterAccess = path.MaxCountAfterAccess
	out.MinCountAfterAccess = path.MinCountAfterAccess
	out.CountAfterIndex = path.CountAfterIndex
	out.PartialPaths = make([]accessPathForDebugTrace, len(path.PartialIndexPaths))
	for i, partialPath := range path.PartialIndexPaths {
		convertAccessPathForDebugTrace(ctx, partialPath, &out.PartialPaths[i])
	}
}

// DebugTraceAccessPaths records the access paths to the debug trace.
func DebugTraceAccessPaths(s base.PlanContext, paths []*util.AccessPath) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := make([]accessPathForDebugTrace, len(paths))
	for i, partialPath := range paths {
		convertAccessPathForDebugTrace(s.GetExprCtx().GetEvalCtx(), partialPath, &traceInfo[i])
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "Access paths")
}
