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

package debugtrace

import (
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/util"
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
	return EncodeJSONCommon(infoForMarshal)
}

// GetPreparedStmt will be pre-init by core/pkg to avoid import cycle.
var GetPreparedStmt func(stmt ast.StmtNode) (planCacheStmtIsNil bool, NotNilText string, binaryParams []expression.Expression)

// DebugTraceReceivedCommand records the received command from the client to the debug trace.
func DebugTraceReceivedCommand(s context.PlanContext, cmd byte, stmtNode ast.StmtNode) {
	sessionVars := s.GetSessionVars()
	trace := GetOrInitDebugTraceRoot(s)
	traceInfo := new(receivedCmdInfo)
	trace.AppendStepWithNameToCurrentContext(traceInfo, "Received Command")
	traceInfo.Command = mysql.Command2Str[cmd]
	traceInfo.ExecutedASTText = stmtNode.Text()

	// Collect information for execute stmt, and record it in executeInfo.
	var (
		planCacheStmtIsNil bool
		planCacheStmtText  string
		binaryParams       []expression.Expression
	)
	planCacheStmtIsNil, planCacheStmtText, binaryParams = GetPreparedStmt(stmtNode)
	useCursor := sessionVars.HasStatusFlag(mysql.ServerStatusCursorExists)
	// If none of them needs record, we don't need a executeInfo.
	if binaryParams == nil && planCacheStmtIsNil && !useCursor {
		return
	}
	execInfo := &executeInfo{}
	traceInfo.ExecuteStmtInfo = execInfo
	execInfo.UseCursor = useCursor
	if !planCacheStmtIsNil {
		execInfo.PreparedSQL = planCacheStmtText
	}
	if len(binaryParams) > 0 {
		execInfo.BinaryParamsInfo = make([]binaryParamInfo, len(binaryParams))
		for i, param := range binaryParams {
			execInfo.BinaryParamsInfo[i].Type = param.GetType().String()
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
		return EncodeJSONCommon(err)
	}
	if b.trying {
		tmp["Trying Hint"] = hintStr
	} else {
		tmp["Best Hint"] = hintStr
	}
	return EncodeJSONCommon(tmp)
}

// DebugTraceTryBinding records the hint that might be chosen to the debug trace.
func DebugTraceTryBinding(s context.PlanContext, binding *hint.HintsSet) {
	root := GetOrInitDebugTraceRoot(s)
	traceInfo := &bindingHint{
		Hint:   binding,
		trying: true,
	}
	root.AppendStepToCurrentContext(traceInfo)
}

// DebugTraceBestBinding records the chosen hint to the debug trace.
func DebugTraceBestBinding(s context.PlanContext, binding *hint.HintsSet) {
	root := GetOrInitDebugTraceRoot(s)
	traceInfo := &bindingHint{
		Hint:   binding,
		trying: false,
	}
	root.AppendStepToCurrentContext(traceInfo)
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

func DebugTraceAccessPaths(s context.PlanContext, paths []*util.AccessPath) {
	root := GetOrInitDebugTraceRoot(s)
	traceInfo := make([]accessPathForDebugTrace, len(paths))
	for i, partialPath := range paths {
		convertAccessPathForDebugTrace(partialPath, &traceInfo[i])
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "Access paths")
}
