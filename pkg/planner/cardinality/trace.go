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
	"bytes"
	"encoding/json"
	"errors"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

// ceTraceExpr appends an expression and related information into CE trace
func ceTraceExpr(sctx context.PlanContext, tableID int64, tp string, expr expression.Expression, rowCount float64) {
	exprStr, err := exprToString(sctx.GetExprCtx().GetEvalCtx(), expr)
	if err != nil {
		logutil.BgLogger().Debug("Failed to trace CE of an expression", zap.String("category", "OptimizerTrace"),
			zap.Any("expression", expr))
		return
	}
	rec := tracing.CETraceRecord{
		TableID:  tableID,
		Type:     tp,
		Expr:     exprStr,
		RowCount: uint64(rowCount),
	}
	sc := sctx.GetSessionVars().StmtCtx
	sc.OptimizerCETrace = append(sc.OptimizerCETrace, &rec)
}

// exprToString prints an Expression into a string which can appear in a SQL.
//
// It might be too tricky because it makes use of TiDB allowing using internal function name in SQL.
// For example, you can write `eq`(a, 1), which is the same as a = 1.
// We should have implemented this by first implementing a method to turn an expression to an AST
//
//	then call astNode.Restore(), like the Constant case here. But for convenience, we use this trick for now.
//
// It may be more appropriate to put this in expression package. But currently we only use it for CE trace,
//
//	and it may not be general enough to handle all possible expressions. So we put it here for now.
func exprToString(ctx expression.EvalContext, e expression.Expression) (string, error) {
	switch expr := e.(type) {
	case *expression.ScalarFunction:
		var buffer bytes.Buffer
		buffer.WriteString("`" + expr.FuncName.L + "`(")
		switch expr.FuncName.L {
		case ast.Cast:
			for _, arg := range expr.GetArgs() {
				argStr, err := exprToString(ctx, arg)
				if err != nil {
					return "", err
				}
				buffer.WriteString(argStr)
				buffer.WriteString(", ")
				buffer.WriteString(expr.RetType.String())
			}
		default:
			for i, arg := range expr.GetArgs() {
				argStr, err := exprToString(ctx, arg)
				if err != nil {
					return "", err
				}
				buffer.WriteString(argStr)
				if i+1 != len(expr.GetArgs()) {
					buffer.WriteString(", ")
				}
			}
		}
		buffer.WriteString(")")
		return buffer.String(), nil
	case *expression.Column:
		return expr.String(), nil
	case *expression.CorrelatedColumn:
		return "", errors.New("tracing for correlated columns not supported now")
	case *expression.Constant:
		value, err := expr.Eval(ctx, chunk.Row{})
		if err != nil {
			return "", err
		}
		valueExpr := driver.ValueExpr{Datum: value}
		var buffer bytes.Buffer
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buffer)
		err = valueExpr.Restore(restoreCtx)
		if err != nil {
			return "", err
		}
		return buffer.String(), nil
	}
	return "", errors.New("unexpected type of Expression")
}

/*
 Below is debug trace for GetRowCountByXXX().
*/

type getRowCountInput struct {
	Ranges []string
	ID     int64
}

func debugTraceGetRowCountInput(
	s context.PlanContext,
	id int64,
	ranges ranger.Ranges,
) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	newCtx := &getRowCountInput{
		ID:     id,
		Ranges: make([]string, len(ranges)),
	}
	for i, r := range ranges {
		newCtx.Ranges[i] = r.String()
	}
	root.AppendStepToCurrentContext(newCtx)
}

// GetTblInfoForUsedStatsByPhysicalID get table name, partition name and TableInfo that will be used to record used stats.
var GetTblInfoForUsedStatsByPhysicalID func(sctx context.PlanContext, id int64) (fullName string, tblInfo *model.TableInfo)

// recordUsedItemStatsStatus only records un-FullLoad item load status during user query
func recordUsedItemStatsStatus(sctx context.PlanContext, stats any, tableID, id int64) {
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
		name, tblInfo := GetTblInfoForUsedStatsByPhysicalID(sctx, tableID)
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

// ceTraceRange appends a list of ranges and related information into CE trace
func ceTraceRange(sctx context.PlanContext, tableID int64, colNames []string, ranges []*ranger.Range, tp string, rowCount uint64) {
	sc := sctx.GetSessionVars().StmtCtx
	tc := sc.TypeCtx()
	allPoint := true
	for _, ran := range ranges {
		if !ran.IsPointNullable(tc) {
			allPoint = false
			break
		}
	}
	if allPoint {
		tp = tp + "-Point"
	} else {
		tp = tp + "-Range"
	}
	expr, err := ranger.RangesToString(sc, ranges, colNames)
	if err != nil {
		logutil.BgLogger().Debug("Failed to trace CE of ranges", zap.String("category", "OptimizerTrace"), zap.Error(err))
	}
	// We don't need to record meaningless expressions.
	if expr == "" || expr == "true" || expr == "false" {
		return
	}
	ceRecord := tracing.CETraceRecord{
		TableID:  tableID,
		Type:     tp,
		Expr:     expr,
		RowCount: rowCount,
	}
	sc.OptimizerCETrace = append(sc.OptimizerCETrace, &ceRecord)
}

/*
 Below is debug trace for the estimation for each single range inside GetRowCountByXXX().
*/

type startEstimateRangeInfo struct {
	Range            string
	LowValueEncoded  []byte
	HighValueEncoded []byte
	CurrentRowCount  float64
}

func debugTraceStartEstimateRange(
	s context.PlanContext,
	r *ranger.Range,
	lowBytes, highBytes []byte,
	currentCount float64,
) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := &startEstimateRangeInfo{
		CurrentRowCount:  currentCount,
		Range:            r.String(),
		LowValueEncoded:  lowBytes,
		HighValueEncoded: highBytes,
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "Start estimate range")
}

type debugTraceAddRowCountType int8

const (
	debugTraceUnknownTypeAddRowCount debugTraceAddRowCountType = iota
	debugTraceImpossible
	debugTraceUniquePoint
	debugTracePoint
	debugTraceRange
	debugTraceVer1SmallRange
)

var addRowCountTypeToString = map[debugTraceAddRowCountType]string{
	debugTraceUnknownTypeAddRowCount: "Unknown",
	debugTraceImpossible:             "Impossible",
	debugTraceUniquePoint:            "Unique point",
	debugTracePoint:                  "Point",
	debugTraceRange:                  "Range",
	debugTraceVer1SmallRange:         "Small range in ver1 stats",
}

func (d debugTraceAddRowCountType) MarshalJSON() ([]byte, error) {
	return json.Marshal(addRowCountTypeToString[d])
}

type endEstimateRangeInfo struct {
	RowCount float64
	Type     debugTraceAddRowCountType
}

func debugTraceEndEstimateRange(
	s context.PlanContext,
	count float64,
	addType debugTraceAddRowCountType,
) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := &endEstimateRangeInfo{
		RowCount: count,
		Type:     addType,
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "End estimate range")
}
