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

package core

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

type spfreshFilterColumn struct {
	source    tipb.SPFreshFilterExprColumnSource
	fieldType types.FieldType
}

const (
	// Keep filter fields below the worker's 1MiB request limit, leaving space for
	// the query vector, projected columns, and request metadata.
	maxSPFreshSearchFilterBytes = 3 << 18
	// Match the worker's request-validation limit so TiDB does not select a plan
	// that the worker will reject.
	maxSPFreshSearchFilterColumns = 256
)

func hasMatchingSPFreshVectorPath(ds *logicalop.DataSource, vs *expression.VectorHelper) bool {
	if ds == nil {
		return false
	}
	for _, path := range ds.PossibleAccessPaths {
		if path.Index == nil || !path.Index.IsSPFreshIndex() {
			continue
		}
		if vectorHelperMatchesIndex(ds.TableInfo, path.Index, vs) {
			return true
		}
	}
	return false
}

func vectorHelperMatchesIndex(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, helper *expression.VectorHelper) bool {
	if helper == nil || helper.Column == nil || idxInfo == nil || idxInfo.VectorInfo == nil {
		return false
	}
	vecColID, err := model.VectorIndexColumnID(tblInfo, idxInfo)
	if err != nil || vecColID != helper.Column.ID {
		return false
	}
	distanceMetric, ok := model.VectorIndexDistanceMetricByFnName(idxInfo.VectorInfo.Kind, helper.DistanceFnName.L)
	return ok && distanceMetric == idxInfo.VectorInfo.DistanceMetric
}

func extractSPFreshFilterExpr(ds *logicalop.DataSource, idx *model.IndexInfo) (*SPFreshFilterExpr, bool) {
	if ds == nil || idx == nil || idx.VectorInfo == nil {
		return nil, false
	}
	pushedConds, availableCols, ok := prepareSPFreshFilterExpr(ds, idx)
	if !ok || len(pushedConds) == 0 {
		return nil, ok
	}
	filterExpr := expression.ComposeCNFCondition(ds.SCtx().GetExprCtx(), pushedConds...)
	exprColumns, filterSchema, ok := buildSPFreshFilterExprColumns(availableCols, filterExpr)
	if !ok {
		return nil, false
	}
	resolvedFilterExpr, err := filterExpr.ResolveIndices(filterSchema)
	if err != nil {
		return nil, false
	}

	pbExpr := expression.NewPBConverterWithColumnOffsets(ds.SCtx().GetClient(), ds.SCtx().GetExprCtx().GetEvalCtx()).ExprToPB(resolvedFilterExpr)
	if pbExpr == nil || !validateSPFreshFilterColumnRefs(pbExpr, exprColumns) ||
		!rewriteSPFreshTimestampFilterConstants(pbExpr, ds.SCtx().GetExprCtx().GetEvalCtx().Location()) {
		return nil, false
	}
	if (&tipb.SPFreshSearchRequest{
		FilterExpr:        pbExpr,
		FilterExprColumns: exprColumns,
	}).Size() > maxSPFreshSearchFilterBytes {
		return nil, false
	}
	// The protobuf retains only evaluated values, including deferred constants
	// such as NOW() even when the statement has no parameter markers.
	if expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), pushedConds) {
		ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("SPFresh filters with deferred constants require rebinding")
	}
	explainExpr := filterExpr.Clone()
	for _, col := range expression.ExtractColumns(explainExpr) {
		if colInfo := ds.TableInfo.FindColumnByID(col.ID); colInfo != nil {
			col.OrigName = colInfo.Name.L
		}
	}
	return &SPFreshFilterExpr{
		Expr:                  pbExpr,
		ExprColumns:           exprColumns,
		explainInfo:           explainExpr.ExplainInfo(ds.SCtx().GetExprCtx().GetEvalCtx()),
		explainNormalizedInfo: explainExpr.ExplainNormalizedInfo(),
	}, true
}

func validateSPFreshFilterColumnRefs(expr *tipb.Expr, columns []*tipb.SPFreshFilterExprColumn) bool {
	if expr == nil {
		return false
	}
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		remaining, offset, err := codec.DecodeInt(expr.GetVal())
		if err != nil || len(remaining) != 0 || offset < 0 || offset >= int64(len(columns)) {
			return false
		}
		column := columns[offset]
		if column == nil || !proto.Equal(expr.GetFieldType(), column.GetFieldType()) {
			return false
		}
	}
	for _, child := range expr.GetChildren() {
		if !validateSPFreshFilterColumnRefs(child, columns) {
			return false
		}
	}
	return true
}

func prepareSPFreshFilterExpr(
	ds *logicalop.DataSource,
	idx *model.IndexInfo,
) ([]expression.Expression, map[int64]spfreshFilterColumn, bool) {
	// SPFresh eligibility must honor the full query predicate, not only the subset
	// already selected for coprocessor pushdown.
	conds := ds.AllConds
	if len(conds) == 0 {
		return nil, nil, true
	}
	filterConds := make([]expression.Expression, 0, len(conds))
	for _, cond := range conds {
		if cond.IsCorrelated() || !expression.IsImmutableFunc(cond) || spfreshFilterNeedsUnsupportedEvalContext(cond) {
			return nil, nil, false
		}
		if len(expression.ExtractColumns(cond)) > 0 {
			filterConds = append(filterConds, cond)
			continue
		}
		if cond.ConstLevel() != expression.ConstStrict {
			return nil, nil, false
		}
		result, _, err := expression.EvalBool(
			ds.SCtx().GetExprCtx().GetEvalCtx(),
			[]expression.Expression{cond},
			chunk.Row{},
		)
		if err != nil || !result {
			return nil, nil, false
		}
	}
	if len(filterConds) == 0 {
		return nil, nil, true
	}
	availableCols := spfreshFilterAvailableColumns(ds.TableInfo, idx)
	if len(availableCols) == 0 {
		return nil, nil, false
	}

	pushed, remained := expression.PushDownExprs(plannerutil.GetPushDownCtx(ds.SCtx()), filterConds, kv.TiKV)
	if len(remained) > 0 || len(pushed) != len(filterConds) || len(pushed) == 0 {
		return nil, nil, false
	}
	return pushed, availableCols, true
}

// spfreshFilterNeedsUnsupportedEvalContext rejects functions whose semantics
// depend on evaluation properties outside SQL mode and SPFreshEvalContext.
func spfreshFilterNeedsUnsupportedEvalContext(expr expression.Expression) bool {
	switch x := expr.(type) {
	case *expression.ScalarFunction:
		switch x.FuncName.L {
		case ast.Week:
			// WEEK(date) uses default_week_format; WEEK(date, mode) does not.
			if len(x.GetArgs()) == 1 {
				return true
			}
		}
		for _, arg := range x.GetArgs() {
			if spfreshFilterNeedsUnsupportedEvalContext(arg) {
				return true
			}
		}
	case *expression.Constant:
		if x.DeferredExpr != nil {
			return spfreshFilterNeedsUnsupportedEvalContext(x.DeferredExpr)
		}
	}
	return false
}

func buildSPFreshFilterExprColumns(
	available map[int64]spfreshFilterColumn,
	expr expression.Expression,
) ([]*tipb.SPFreshFilterExprColumn, *expression.Schema, bool) {
	if len(available) == 0 || expr == nil {
		return nil, nil, false
	}
	cols := expression.ExtractAllColumnsFromExpressionsInUsedSlices(nil, nil, expr)
	if len(cols) == 0 || len(cols) > maxSPFreshSearchFilterColumns {
		return nil, nil, false
	}
	exprColumns := make([]*tipb.SPFreshFilterExprColumn, 0, len(cols))
	schemaColumns := make([]*expression.Column, 0, len(cols))
	for _, col := range cols {
		if col == nil {
			return nil, nil, false
		}
		availableCol, ok := available[col.ID]
		if !ok {
			return nil, nil, false
		}
		exprColumns = append(exprColumns, &tipb.SPFreshFilterExprColumn{
			ColumnId:  col.ID,
			Source:    availableCol.source,
			FieldType: expression.ToPBFieldType(&availableCol.fieldType),
		})
		schemaColumns = append(schemaColumns, col)
	}
	return exprColumns, expression.NewSchema(schemaColumns...), true
}

func spfreshFilterAvailableColumns(tblInfo *model.TableInfo, idx *model.IndexInfo) map[int64]spfreshFilterColumn {
	if tblInfo == nil || idx == nil || idx.VectorInfo == nil {
		return nil
	}
	storedColumns, err := model.ResolveSPFreshStoredColumns(tblInfo, idx)
	if err != nil {
		return nil
	}
	available := make(map[int64]spfreshFilterColumn, len(idx.VectorInfo.StoredColumnIDs)+1)
	for _, colInfo := range storedColumns {
		available[colInfo.ID] = spfreshFilterColumn{
			source:    tipb.SPFreshFilterExprColumnSource_Stored,
			fieldType: colInfo.FieldType,
		}
	}
	if handleColID, handleFieldType, ok := model.SPFreshHandleColumnInfo(tblInfo); ok {
		if _, ok := available[handleColID]; !ok {
			available[handleColID] = spfreshFilterColumn{
				source:    tipb.SPFreshFilterExprColumnSource_Handle,
				fieldType: handleFieldType,
			}
		}
	}
	return available
}

func rewriteSPFreshTimestampFilterConstants(expr *tipb.Expr, loc *time.Location) bool {
	if expr == nil {
		return true
	}
	children := expr.GetChildren()
	if spfreshTimeCompareSig(expr.GetSig()) && len(children) == 2 {
		if !rewriteSPFreshTimestampFilterConstant(children[0], children[1], loc) {
			return false
		}
		if !rewriteSPFreshTimestampFilterConstant(children[1], children[0], loc) {
			return false
		}
	}
	for _, child := range children {
		if !rewriteSPFreshTimestampFilterConstants(child, loc) {
			return false
		}
	}
	return true
}

func spfreshTimeCompareSig(sig tipb.ScalarFuncSig) bool {
	switch sig {
	case tipb.ScalarFuncSig_LTTime,
		tipb.ScalarFuncSig_LETime,
		tipb.ScalarFuncSig_GTTime,
		tipb.ScalarFuncSig_GETime,
		tipb.ScalarFuncSig_EQTime,
		tipb.ScalarFuncSig_NETime,
		tipb.ScalarFuncSig_NullEQTime:
		return true
	default:
		return false
	}
}

func rewriteSPFreshTimestampFilterConstant(columnExpr, constExpr *tipb.Expr, loc *time.Location) bool {
	if columnExpr == nil || constExpr == nil {
		return true
	}
	if columnExpr.GetTp() != tipb.ExprType_ColumnRef ||
		columnExpr.GetFieldType().GetTp() != int32(mysql.TypeTimestamp) {
		return true
	}
	if constExpr.GetTp() != tipb.ExprType_MysqlTime ||
		constExpr.GetFieldType().GetTp() != int32(mysql.TypeDatetime) {
		return true
	}

	// Time comparisons normalize TIMESTAMP literals to DATETIME. SPFresh
	// workers compare stored TIMESTAMP payloads in UTC, so keep the pushed
	// constant in the same coordinate system.
	encoded, ok := encodeSPFreshTimestampFilterConstant(constExpr, loc)
	if !ok {
		return false
	}
	constExpr.Val = encoded
	constExpr.FieldType.Tp = int32(mysql.TypeTimestamp)
	return true
}

func encodeSPFreshTimestampFilterConstant(expr *tipb.Expr, loc *time.Location) ([]byte, bool) {
	remaining, packed, err := codec.DecodeUint(expr.GetVal())
	if err != nil || len(remaining) != 0 {
		return nil, false
	}

	var value types.Time
	if err := value.FromPackedUint(packed); err != nil {
		return nil, false
	}
	value.SetType(mysql.TypeDatetime)
	fsp := int(expr.GetFieldType().GetDecimal())
	if fsp < 0 {
		fsp = types.DefaultFsp
	}
	value.SetFsp(fsp)

	if loc == nil {
		loc = time.UTC
	}
	encoded, err := codec.EncodeMySQLTime(loc, value, mysql.TypeTimestamp, nil)
	if err != nil {
		return nil, false
	}
	return encoded, true
}

func spfreshScanOutputColumns(ds *logicalop.DataSource) ([]*model.ColumnInfo, []*expression.Column) {
	if ds == nil {
		return nil, nil
	}
	requiredCols := ds.ColsRequiringFullLen
	if requiredCols == nil {
		return plannerutil.CloneColInfos(ds.Columns), plannerutil.CloneCols(ds.Schema().Columns)
	}
	used := expression.GetUsedList(ds.SCtx().GetExprCtx().GetEvalCtx(), requiredCols, ds.Schema())
	outInfos := make([]*model.ColumnInfo, 0, len(ds.Columns))
	outSchemaCols := make([]*expression.Column, 0, len(ds.Schema().Columns))
	for i, schemaCol := range ds.Schema().Columns {
		if used[i] {
			outInfos = append(outInfos, ds.Columns[i].Clone())
			outSchemaCol := schemaCol.Clone().(*expression.Column)
			outSchemaCol.Index = len(outSchemaCols)
			outSchemaCols = append(outSchemaCols, outSchemaCol)
		}
	}
	return outInfos, outSchemaCols
}
