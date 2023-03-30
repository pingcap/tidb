package core

import (
	"encoding/json"
	"github.com/pingcap/tidb/planner/util"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util/debug_trace"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/hint"
)

/*
 Below is debug trace for the received command from the client.
 It records the input to the optimizer at the very beginning of query optimization.
*/

func DebugTraceReceivedCommand(s sessionctx.Context, cmd byte, stmtNode ast.StmtNode) {
	sessionVars := s.GetSessionVars()
	trace := debug_trace.GetOrInitDebugTraceRoot(s)

	trace.ReceivedCommand.Command = mysql.Command2Str[cmd]
	trace.ReceivedCommand.ExecutedASTText = stmtNode.Text()

	// Collect information for execute stmt, and record it in debug_trace.ExecuteInfo.
	var binaryParams []expression.Expression
	var planCacheStmt *PlanCacheStmt
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		if execStmt.PrepStmt != nil {
			planCacheStmt, _ = execStmt.PrepStmt.(*PlanCacheStmt)
		}
		if execStmt.BinaryArgs != nil {
			binaryParams, _ = execStmt.BinaryArgs.([]expression.Expression)
		}
	}
	useCursor := mysql.HasCursorExistsFlag(sessionVars.Status)
	// If none of them needs record, we don't need a debug_trace.ExecuteInfo.
	if binaryParams == nil && planCacheStmt == nil && !useCursor {
		return
	}
	execInfo := &debug_trace.ExecuteInfo{}
	trace.ReceivedCommand.ExecuteStmtInfo = execInfo
	execInfo.UseCursor = useCursor
	if planCacheStmt != nil {
		execInfo.PreparedSQL = planCacheStmt.StmtText
	}
	if len(binaryParams) > 0 {
		execInfo.BinaryParamsInfo = make([]debug_trace.BinaryParamInfo, len(binaryParams))
		for i, param := range binaryParams {
			execInfo.BinaryParamsInfo[i].Type = param.GetType().String()
			execInfo.BinaryParamsInfo[i].Value = param.String()
		}
	}
}

type bindingHint struct {
	Hint   *hint.HintsSet
	trying bool
}

func (b *bindingHint) MarshalJSON() ([]byte, error) {
	var tmp map[string]string
	hintStr, err := b.Hint.Restore()
	if err != nil {
		return json.Marshal(err)
	}
	if b.trying {
		tmp["Trying Hint"] = hintStr
	} else {
		tmp["Best Hint"] = hintStr
	}
	return debug_trace.EncodeJSONCommon(tmp)
}

func DebugTraceTryBinding(s sessionctx.Context, binding *hint.HintsSet) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
	traceInfo := &bindingHint{
		Hint:   binding,
		trying: true,
	}
	root.AppendStepToCurrentContext(traceInfo)
}

func DebugTraceBestBinding(s sessionctx.Context, binding *hint.HintsSet) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
	traceInfo := &bindingHint{
		Hint:   binding,
		trying: false,
	}
	root.AppendStepToCurrentContext(traceInfo)
}

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
	StatsTblInfo      *statistics.StatsTblInfo
}

func DebugTraceGetStatsTbl(
	s sessionctx.Context,
	tblInfo *model.TableInfo,
	pid int64,
	handleIsNil,
	usePartitionStats,
	countIsZero,
	uninitialized,
	outdated bool,
	statsTbl *statistics.Table,
) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
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
	root.AppendStepToCurrentContext(traceInfo)
}

type accessPathForDebugTrace struct {
	IndexName        string `json:",omitempty"`
	AccessConditions []string
	IndexFilters     []string
	TableFilters     []string
	PartialPaths     []accessPathForDebugTrace `json:",omitempty"`
	CountAfterAccess float64
	CountAfterIndex  float64
}

func convertAccessPathForDebugTrace(path *util.AccessPath, out *accessPathForDebugTrace) {
	if path.Index != nil {
		out.IndexName = path.Index.Name.O
	}
	out.AccessConditions = expression.ExprsToStringsForDisplay(path.AccessConds)
	out.IndexFilters = expression.ExprsToStringsForDisplay(path.IndexFilters)
	out.TableFilters = expression.ExprsToStringsForDisplay(path.TableFilters)
	out.CountAfterAccess = path.CountAfterAccess
	out.CountAfterIndex = path.CountAfterIndex
	out.PartialPaths = make([]accessPathForDebugTrace, len(path.PartialIndexPaths))
	for i, partialPath := range path.PartialIndexPaths {
		convertAccessPathForDebugTrace(partialPath, &out.PartialPaths[i])
	}
}

func DebugTraceAccessPaths(s sessionctx.Context, paths []*util.AccessPath) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
	traceInfo := make([]accessPathForDebugTrace, len(paths))
	for i, partialPath := range paths {
		convertAccessPathForDebugTrace(partialPath, &traceInfo[i])
	}
	root.AppendStepToCurrentContext(traceInfo)
}
