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

package core

import (
	"strconv"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/hint"
)

/*
 Below is debug trace for the received command from the client.
 It records the input to the optimizer at the very beginning of query optimization.
*/

type receivedCmdInfo struct {
	Command         string
	ExecutedASTText string
	ExecuteStmtInfo *executeInfo
}

type executeInfo struct {
	PreparedSQL      string
	BinaryParamsInfo []binaryParamInfo
	UseCursor        bool
}

type binaryParamInfo struct {
	Type  string
	Value string
}

func (info *binaryParamInfo) MarshalJSON() ([]byte, error) {
	type binaryParamInfoForMarshal binaryParamInfo
	infoForMarshal := new(binaryParamInfoForMarshal)
	quote := `"`
	// We only need the escape functionality of strconv.Quote, the quoting is not needed,
	// so we trim the \" prefix and suffix here.
	infoForMarshal.Type = strings.TrimSuffix(
		strings.TrimPrefix(
			strconv.Quote(info.Type),
			quote),
		quote)
	infoForMarshal.Value = strings.TrimSuffix(
		strings.TrimPrefix(
			strconv.Quote(info.Value),
			quote),
		quote)
	return debugtrace.EncodeJSONCommon(infoForMarshal)
}

// DebugTraceReceivedCommand records the received command from the client to the debug trace.
func DebugTraceReceivedCommand(s base.PlanContext, cmd byte, stmtNode ast.StmtNode) {
	sessionVars := s.GetSessionVars()
	trace := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := new(receivedCmdInfo)
	trace.AppendStepWithNameToCurrentContext(traceInfo, "Received Command")
	traceInfo.Command = mysql.Command2Str[cmd]
	traceInfo.ExecutedASTText = stmtNode.Text()

	// Collect information for execute stmt, and record it in executeInfo.
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
	useCursor := sessionVars.HasStatusFlag(mysql.ServerStatusCursorExists)
	// If none of them needs record, we don't need a executeInfo.
	if binaryParams == nil && planCacheStmt == nil && !useCursor {
		return
	}
	execInfo := &executeInfo{}
	traceInfo.ExecuteStmtInfo = execInfo
	execInfo.UseCursor = useCursor
	if planCacheStmt != nil {
		execInfo.PreparedSQL = planCacheStmt.StmtText
	}
	if len(binaryParams) > 0 {
		execInfo.BinaryParamsInfo = make([]binaryParamInfo, len(binaryParams))
		for i, param := range binaryParams {
			execInfo.BinaryParamsInfo[i].Type = param.GetType(s.GetExprCtx().GetEvalCtx()).String()
			execInfo.BinaryParamsInfo[i].Value = param.String()
		}
	}
}

/*
 Below is debug trace for the hint that matches the current query.
*/

type bindingHint struct {
	Hint   *hint.HintsSet
	trying bool
}

func (b *bindingHint) MarshalJSON() ([]byte, error) {
	tmp := make(map[string]string, 1)
	hintStr, err := b.Hint.Restore()
	if err != nil {
		return debugtrace.EncodeJSONCommon(err)
	}
	if b.trying {
		tmp["Trying Hint"] = hintStr
	} else {
		tmp["Best Hint"] = hintStr
	}
	return debugtrace.EncodeJSONCommon(tmp)
}

// DebugTraceTryBinding records the hint that might be chosen to the debug trace.
func DebugTraceTryBinding(s context.PlanContext, binding *hint.HintsSet) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := &bindingHint{
		Hint:   binding,
		trying: true,
	}
	root.AppendStepToCurrentContext(traceInfo)
}

// DebugTraceBestBinding records the chosen hint to the debug trace.
func DebugTraceBestBinding(s context.PlanContext, binding *hint.HintsSet) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
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

func debugTraceAccessPaths(s base.PlanContext, paths []*util.AccessPath) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := make([]accessPathForDebugTrace, len(paths))
	for i, partialPath := range paths {
		convertAccessPathForDebugTrace(partialPath, &traceInfo[i])
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "Access paths")
}
