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

package mvmerge

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
)

const (
	deltaTableAlias = "delta"
	mvTableAlias    = "mv"

	deltaCntStarName = "__mvmerge_delta_cnt_star"
	removedRowsName  = "__mvmerge_removed_rows"
)

type BuildOptions struct {
	FromTS uint64
	ToTS   uint64
}

// BuildResult is the optimizer-ready merge source produced by Build.
// It includes the physical plan and all metadata needed by executor-side MV merge.
// Row layout of Plan/OutputNames is fixed:
//  1. MV output columns first (count = MVColumnCount)
//  2. delta columns after MV columns
//
// All offsets in AggInfos (MVOffset and Dependencies) are based on this layout.
type BuildResult struct {
	Plan base.PhysicalPlan
	// OutputNames matches the result schema of Plan (MV columns first, then delta columns).
	// Note: physical plans in TiDB may not keep output names; the caller should provide them via SelectOptimizer.
	OutputNames types.NameSlice

	MVTableID   int64
	BaseTableID int64
	MLogTableID int64

	// MVColumnCount indicates how many columns in Plan.Schema() belong to the MV row shape.
	// The MV columns are always put in the front.
	MVColumnCount int

	// GroupKeyMVOffsets are offsets (0-based) of the group key columns in the MV output schema.
	GroupKeyMVOffsets []int

	// CountStarMVOffset is the offset (0-based) of COUNT(*) in MV output columns.
	// Build returns error when MV definition does not include COUNT(*).
	CountStarMVOffset int

	AggInfos []AggInfo

	// RemovedRowCountDelta is non-nil when MV contains MIN/MAX, used by executor to gate quick-update.
	RemovedRowCountDelta *DeltaColumn
}

type DeltaColumn struct {
	Name   string
	Offset int
}

type AggKind int

const (
	AggCountStar AggKind = iota
	AggCount
	AggSum
	AggMin
	AggMax
)

type AggInfo struct {
	Kind AggKind

	// MVOffset is the offset (0-based) in the MV output schema.
	MVOffset int

	// ArgColName is the base-table column name used as the aggregate argument. Empty for COUNT(*).
	ArgColName string

	// Dependencies stores dependency offsets in merge-source output schema.
	// The meaning depends on Kind:
	//   - AggCountStar / AggCount: [self_delta]
	//   - AggSum: [self_delta, matched_count_expr_mv]
	//     - self_delta: offset of SUM(expr) delta column.
	//     - matched_count_expr_mv: offset of matched COUNT(expr) in MV output.
	//       COUNT(expr) must be updated before SUM(expr), and SUM should read this updated MV value.
	//       The same matched COUNT(expr) can be a dependency for multiple aggregate functions.
	//   - AggMax / AggMin: [self_delta, removed_rows_delta]
	Dependencies []int
}

type aggColInfo struct {
	info      AggInfo
	deltaName string
	argExpr   ast.ExprNode
}

// SelectOptimizer converts the merge-source SELECT statement into a physical plan.
// The caller usually does preprocess -> logical plan build -> optimize.
type SelectOptimizer func(ctx context.Context, sel *ast.SelectStmt) (base.PhysicalPlan, types.NameSlice, error)

func Build(ctx context.Context, sctx base.PlanContext, is infoschema.InfoSchema, mv *model.TableInfo, opt BuildOptions, optimize SelectOptimizer) (*BuildResult, error) {
	// Stage 0: validate MV/MLoG metadata and locate all required tables.
	if mv == nil {
		return nil, errors.New("mv table info is nil")
	}
	if optimize == nil {
		return nil, errors.New("mvmerge: optimize callback is nil")
	}
	if mv.MaterializedView == nil {
		return nil, errors.Errorf("table %s is not a materialized view", mv.Name.O)
	}
	if len(mv.MaterializedView.BaseTableIDs) != 1 {
		return nil, errors.Errorf("materialized view %s has invalid base table list size %d", mv.Name.O, len(mv.MaterializedView.BaseTableIDs))
	}
	baseTableID := mv.MaterializedView.BaseTableIDs[0]
	baseTable, ok := is.TableInfoByID(baseTableID)
	if !ok {
		return nil, errors.Errorf("base table id %d not found in infoschema", baseTableID)
	}
	if baseTable.MaterializedViewBase == nil || baseTable.MaterializedViewBase.MLogID == 0 {
		return nil, errors.Errorf("base table %s has no materialized view log", baseTable.Name.O)
	}
	mlogTableID := baseTable.MaterializedViewBase.MLogID
	mlogTable, ok := is.TableInfoByID(mlogTableID)
	if !ok {
		return nil, errors.Errorf("materialized view log table id %d not found in infoschema", mlogTableID)
	}
	if mlogTable.MaterializedViewLog == nil {
		return nil, errors.Errorf("table %s is not a materialized view log", mlogTable.Name.O)
	}
	if mlogTable.MaterializedViewLog.BaseTableID != baseTableID {
		return nil, errors.Errorf("materialized view log %s does not belong to base table id %d", mlogTable.Name.O, baseTableID)
	}
	if !hasColumn(mlogTable, model.MaterializedViewLogOldNewColumnName) {
		return nil, errors.Errorf("materialized view log %s missing required column %s", mlogTable.Name.O, model.MaterializedViewLogOldNewColumnName)
	}
	if !hasColumn(mlogTable, model.MaterializedViewLogDMLTypeColumnName) {
		return nil, errors.Errorf("materialized view log %s missing required column %s", mlogTable.Name.O, model.MaterializedViewLogDMLTypeColumnName)
	}

	// Stage 1: parse MV definition and derive merge key/aggregate layout from it.
	mvDBName, err := dbNameByTableID(is, mv.ID)
	if err != nil {
		return nil, err
	}

	mvSel, err := parseSelectFromSQL(sctx, mv.MaterializedView.SQLContent)
	if err != nil {
		return nil, err
	}

	groupKeyOffsets, err := extractGroupKeyOffsetsFromMVSelect(mvSel)
	if err != nil {
		return nil, err
	}
	if len(groupKeyOffsets) == 0 {
		return nil, errors.New("materialized view definition has empty GROUP BY")
	}
	// Fast membership check when deciding whether an MV output column is a group key.
	groupKeySet := make(map[int]struct{}, len(groupKeyOffsets))
	for _, off := range groupKeyOffsets {
		groupKeySet[off] = struct{}{}
	}

	aggCols, hasMinMax, err := extractAggInfosFromMVSelect(mvSel)
	if err != nil {
		return nil, err
	}

	if len(mv.Columns) != len(mvSel.Fields.Fields) {
		// This should never happen for valid MV metadata. Keep it as a guard to avoid mismatched join schema.
		return nil, errors.Errorf("mv columns count %d does not match mv query output %d", len(mv.Columns), len(mvSel.Fields.Fields))
	}

	// Stage 2: build merge source SQL in two steps:
	//   1) aggregate mlog rows into per-group deltas
	//   2) left join those deltas with current MV snapshot
	deltaSel, err := buildMLogDeltaSelect(mvDBName, mlogTable, mvSel, mv.Columns, groupKeyOffsets, aggCols, hasMinMax, opt)
	if err != nil {
		return nil, err
	}

	mergeSel, deltaColumns, removedDelta, err := buildMergeSourceSelect(mvDBName, mv, mv.Columns, groupKeySet, groupKeyOffsets, deltaSel, aggCols, hasMinMax)
	if err != nil {
		return nil, err
	}

	// Stage 3: optimize the merge-source SELECT into a physical plan.
	plan, outputNames, err := optimize(ctx, mergeSel)
	if err != nil {
		return nil, err
	}

	expectedLen := len(mv.Columns) + len(deltaColumns)
	if plan.Schema().Len() != expectedLen {
		return nil, errors.Errorf("unexpected merge-source schema length: got %d, expected %d", plan.Schema().Len(), expectedLen)
	}
	if len(outputNames) > 0 && len(outputNames) != expectedLen {
		return nil, errors.Errorf("unexpected merge-source output names length: got %d, expected %d", len(outputNames), expectedLen)
	}

	countStarMVOffset := -1
	for _, ac := range aggCols {
		if ac.info.Kind == AggCountStar {
			countStarMVOffset = ac.info.MVOffset
			break
		}
	}
	if countStarMVOffset < 0 {
		return nil, errors.New("materialized view definition must include COUNT(*) for mvmerge")
	}

	sumToCountExprIdx, err := mapSumToCountExprDependencies(aggCols)
	if err != nil {
		return nil, err
	}

	// Patch delta offsets into AggInfos so executor can read delta payload by offset directly.
	deltaOffsetByName := make(map[string]int, len(deltaColumns))
	for _, dc := range deltaColumns {
		deltaOffsetByName[dc.Name] = dc.Offset
	}
	outAggInfos := make([]AggInfo, 0, len(aggCols))
	for i, ac := range aggCols {
		di := ac.info
		deps := make([]int, 0, 3)
		if ac.deltaName != "" {
			off, ok := deltaOffsetByName[ac.deltaName]
			if !ok {
				return nil, errors.Errorf("internal error: delta column %s not found in output", ac.deltaName)
			}
			deps = append(deps, off)
		}
		switch di.Kind {
		case AggSum:
			countIdx, ok := sumToCountExprIdx[i]
			if !ok {
				return nil, errors.Errorf("internal error: SUM at mv offset %d has no COUNT(expr) dependency", di.MVOffset)
			}
			countAgg := aggCols[countIdx]
			deps = append(deps, countAgg.info.MVOffset)
		case AggMax, AggMin:
			if removedDelta == nil {
				return nil, errors.Errorf("internal error: %v at mv offset %d requires %s delta", di.Kind, di.MVOffset, removedRowsName)
			}
			deps = append(deps, removedDelta.Offset)
		}
		di.Dependencies = deps
		outAggInfos = append(outAggInfos, di)
	}
	removedRowsDeltaOff := -1
	if removedDelta != nil {
		removedRowsDeltaOff = removedDelta.Offset
	}
	if err := validateAggDependencies(outAggInfos, len(mv.Columns), expectedLen, removedRowsDeltaOff); err != nil {
		return nil, err
	}

	res := &BuildResult{
		Plan:              plan,
		OutputNames:       outputNames,
		MVTableID:         mv.ID,
		BaseTableID:       baseTableID,
		MLogTableID:       mlogTableID,
		MVColumnCount:     len(mv.Columns),
		GroupKeyMVOffsets: append([]int(nil), groupKeyOffsets...),
		CountStarMVOffset: countStarMVOffset,
		AggInfos:          outAggInfos,
		RemovedRowCountDelta: func() *DeltaColumn {
			if removedDelta == nil {
				return nil
			}
			c := *removedDelta
			return &c
		}(),
	}
	return res, nil
}

func parseSelectFromSQL(sctx base.PlanContext, sql string) (*ast.SelectStmt, error) {
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.SetParserConfig(sctx.GetSessionVars().BuildParserConfig())
	stmt, err := p.ParseOneStmt(sql, charset, collation)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil, errors.Errorf("expected select statement, got %T", stmt)
	}
	return sel, nil
}

func extractGroupKeyOffsetsFromMVSelect(sel *ast.SelectStmt) ([]int, error) {
	if sel.GroupBy == nil || len(sel.GroupBy.Items) == 0 {
		return nil, errors.New("materialized view definition must have GROUP BY")
	}
	offsetByColName := make(map[string]int, 8)
	for i, f := range sel.Fields.Fields {
		col, ok := f.Expr.(*ast.ColumnNameExpr)
		if !ok {
			continue
		}
		offsetByColName[col.Name.Name.L] = i
	}
	groupOffsets := make([]int, 0, len(sel.GroupBy.Items))
	for _, item := range sel.GroupBy.Items {
		col, ok := item.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return nil, errors.New("GROUP BY expression must be a column name for mvmerge")
		}
		off, ok := offsetByColName[col.Name.Name.L]
		if !ok {
			return nil, errors.Errorf("GROUP BY column %s must appear in SELECT list", col.Name.Name.O)
		}
		groupOffsets = append(groupOffsets, off)
	}
	return groupOffsets, nil
}

func extractAggInfosFromMVSelect(sel *ast.SelectStmt) (aggCols []aggColInfo, hasMinMax bool, _ error) {
	for i, f := range sel.Fields.Fields {
		agg, ok := f.Expr.(*ast.AggregateFuncExpr)
		if !ok {
			continue
		}
		switch agg.F {
		case ast.AggFuncCount:
			if len(agg.Args) != 1 {
				return nil, false, errors.New("COUNT must have exactly one argument for mvmerge stage-1")
			}
			if agg.Distinct {
				return nil, false, errors.New("COUNT(DISTINCT ...) is not supported in mvmerge stage-1")
			}
			if isCountStarOrOneArg(agg.Args[0]) {
				aggCols = append(aggCols, aggColInfo{
					info: AggInfo{
						Kind:     AggCountStar,
						MVOffset: i,
					},
					deltaName: deltaCntStarName,
				})
				continue
			}
			aggCols = append(aggCols, aggColInfo{
				info: func() AggInfo {
					ai := AggInfo{
						Kind:     AggCount,
						MVOffset: i,
					}
					if argCol, ok := agg.Args[0].(*ast.ColumnNameExpr); ok {
						ai.ArgColName = argCol.Name.Name.L
					}
					return ai
				}(),
				deltaName: fmt.Sprintf("__mvmerge_delta_cnt_%d", i),
				argExpr:   stripColumnQualifier(agg.Args[0]),
			})
		case ast.AggFuncSum:
			if len(agg.Args) != 1 {
				return nil, false, errors.New("SUM must have exactly one argument for mvmerge stage-1")
			}
			if agg.Distinct {
				return nil, false, errors.New("SUM(DISTINCT ...) is not supported in mvmerge stage-1")
			}
			ai := AggInfo{
				Kind:     AggSum,
				MVOffset: i,
			}
			if argCol, ok := agg.Args[0].(*ast.ColumnNameExpr); ok {
				ai.ArgColName = argCol.Name.Name.L
			}
			aggCols = append(aggCols, aggColInfo{
				info:      ai,
				deltaName: fmt.Sprintf("__mvmerge_delta_sum_%d", i),
				argExpr:   stripColumnQualifier(agg.Args[0]),
			})
		case ast.AggFuncMax:
			argCol, ok := agg.Args[0].(*ast.ColumnNameExpr)
			if !ok {
				return nil, false, errors.New("MAX argument must be a column name for mvmerge")
			}
			hasMinMax = true
			aggCols = append(aggCols, aggColInfo{
				info: AggInfo{
					Kind:       AggMax,
					MVOffset:   i,
					ArgColName: argCol.Name.Name.L,
				},
				deltaName: fmt.Sprintf("__mvmerge_max_in_added_%d", i),
			})
		case ast.AggFuncMin:
			argCol, ok := agg.Args[0].(*ast.ColumnNameExpr)
			if !ok {
				return nil, false, errors.New("MIN argument must be a column name for mvmerge")
			}
			hasMinMax = true
			aggCols = append(aggCols, aggColInfo{
				info: AggInfo{
					Kind:       AggMin,
					MVOffset:   i,
					ArgColName: argCol.Name.Name.L,
				},
				deltaName: fmt.Sprintf("__mvmerge_min_in_added_%d", i),
			})
		default:
			return nil, false, errors.Errorf("unsupported aggregate function %s in mvmerge stage-1", agg.F)
		}
	}
	return aggCols, hasMinMax, nil
}

func mapSumToCountExprDependencies(aggCols []aggColInfo) (map[int]int, error) {
	sumToCountIdx := make(map[int]int)
	for i, ac := range aggCols {
		if ac.info.Kind != AggSum {
			continue
		}
		if ac.argExpr == nil {
			return nil, errors.Errorf("SUM aggregate argument is nil at mv offset %d", ac.info.MVOffset)
		}
		matchIdx := -1
		for j, cand := range aggCols {
			if cand.info.Kind != AggCount || cand.argExpr == nil {
				continue
			}
			if !exprStructuralEqual(ac.argExpr, cand.argExpr) {
				continue
			}
			if matchIdx >= 0 {
				return nil, errors.Errorf(
					"SUM expression %s at mv offset %d has multiple matching COUNT(expr) dependencies (offsets %d and %d)",
					restoreExpr(ac.argExpr), ac.info.MVOffset, aggCols[matchIdx].info.MVOffset, cand.info.MVOffset,
				)
			}
			matchIdx = j
		}
		if matchIdx < 0 {
			return nil, errors.Errorf(
				"SUM expression %s at mv offset %d requires matching COUNT(expr) in SELECT list",
				restoreExpr(ac.argExpr), ac.info.MVOffset,
			)
		}
		sumToCountIdx[i] = matchIdx
	}
	return sumToCountIdx, nil
}

func validateAggDependencies(aggInfos []AggInfo, mvColumnCount, schemaLen, removedRowsDeltaOff int) error {
	for _, ai := range aggInfos {
		for _, dep := range ai.Dependencies {
			if dep < 0 || dep >= schemaLen {
				return errors.Errorf("invalid dependency offset %d for %v at mv offset %d", dep, ai.Kind, ai.MVOffset)
			}
		}
		switch ai.Kind {
		case AggCountStar, AggCount:
			if len(ai.Dependencies) != 1 {
				return errors.Errorf("%v at mv offset %d expects dependencies [self_delta], got %v", ai.Kind, ai.MVOffset, ai.Dependencies)
			}
			if ai.Dependencies[0] < mvColumnCount {
				return errors.Errorf("%v at mv offset %d has invalid self_delta offset %d", ai.Kind, ai.MVOffset, ai.Dependencies[0])
			}
		case AggSum:
			// [self_delta, matched_count_expr_mv]
			if len(ai.Dependencies) != 2 {
				return errors.Errorf("SUM at mv offset %d expects dependencies [self_delta, matched_count_expr_mv], got %v", ai.MVOffset, ai.Dependencies)
			}
			if ai.Dependencies[0] < mvColumnCount {
				return errors.Errorf("SUM at mv offset %d has invalid self_delta offset %d", ai.MVOffset, ai.Dependencies[0])
			}
			if ai.Dependencies[1] < 0 || ai.Dependencies[1] >= mvColumnCount {
				return errors.Errorf("SUM at mv offset %d has invalid matched_count_expr_mv offset %d", ai.MVOffset, ai.Dependencies[1])
			}
		case AggMax, AggMin:
			// [self_delta, removed_rows_delta]
			if len(ai.Dependencies) != 2 {
				return errors.Errorf("%v at mv offset %d expects dependencies [self_delta, removed_rows_delta], got %v", ai.Kind, ai.MVOffset, ai.Dependencies)
			}
			if ai.Dependencies[0] < mvColumnCount {
				return errors.Errorf("%v at mv offset %d has invalid self_delta offset %d", ai.Kind, ai.MVOffset, ai.Dependencies[0])
			}
			if removedRowsDeltaOff < 0 {
				return errors.Errorf("internal error: %v at mv offset %d requires removed_rows delta", ai.Kind, ai.MVOffset)
			}
			if ai.Dependencies[1] != removedRowsDeltaOff {
				return errors.Errorf("%v at mv offset %d has invalid removed_rows_delta offset %d, expected %d", ai.Kind, ai.MVOffset, ai.Dependencies[1], removedRowsDeltaOff)
			}
		default:
			return errors.Errorf("unsupported aggregate kind %v", ai.Kind)
		}
		if len(ai.Dependencies) == 0 {
			return errors.Errorf("%v at mv offset %d has empty Dependencies", ai.Kind, ai.MVOffset)
		}
	}
	return nil
}

func isCountStarOrOneArg(arg ast.ExprNode) bool {
	if arg == nil {
		return true
	}
	v, ok := arg.(ast.ValueExpr)
	if !ok {
		return false
	}
	switch x := v.GetValue().(type) {
	case int64:
		return x == 1
	case uint64:
		return x == 1
	case int:
		return x == 1
	default:
		return false
	}
}

func stripColumnQualifier(expr ast.ExprNode) ast.ExprNode {
	if expr == nil {
		return nil
	}
	expr.Accept(&columnQualifierStripper{})
	return stripAllParentheses(expr)
}

func exprStructuralEqual(lhs, rhs ast.ExprNode) bool {
	lhs = stripParentheses(lhs)
	rhs = stripParentheses(rhs)
	return reflectStructuralEqual(reflect.ValueOf(lhs), reflect.ValueOf(rhs))
}

func stripParentheses(expr ast.ExprNode) ast.ExprNode {
	for {
		p, ok := expr.(*ast.ParenthesesExpr)
		if !ok || p == nil || p.Expr == nil {
			return expr
		}
		expr = p.Expr
	}
}

func reflectStructuralEqual(lhs, rhs reflect.Value) bool {
	if !lhs.IsValid() || !rhs.IsValid() {
		return !lhs.IsValid() && !rhs.IsValid()
	}
	if lhs.Type() != rhs.Type() {
		return false
	}

	switch lhs.Kind() {
	case reflect.Interface, reflect.Pointer:
		if lhs.IsNil() || rhs.IsNil() {
			return lhs.IsNil() == rhs.IsNil()
		}
		return reflectStructuralEqual(lhs.Elem(), rhs.Elem())
	case reflect.Struct:
		typ := lhs.Type()
		for i := 0; i < lhs.NumField(); i++ {
			f := typ.Field(i)
			// Ignore unexported fields (e.g. parser location/cache fields).
			if f.PkgPath != "" {
				continue
			}
			if !reflectStructuralEqual(lhs.Field(i), rhs.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if lhs.Len() != rhs.Len() {
			return false
		}
		for i := 0; i < lhs.Len(); i++ {
			if !reflectStructuralEqual(lhs.Index(i), rhs.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Map:
		if lhs.IsNil() || rhs.IsNil() {
			return lhs.IsNil() == rhs.IsNil()
		}
		return reflect.DeepEqual(lhs.Interface(), rhs.Interface())
	case reflect.Func:
		return lhs.IsNil() && rhs.IsNil()
	default:
		if lhs.CanInterface() && rhs.CanInterface() {
			return reflect.DeepEqual(lhs.Interface(), rhs.Interface())
		}
		return false
	}
}

func restoreExpr(expr ast.ExprNode) string {
	if expr == nil {
		return "<nil>"
	}
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := expr.Restore(ctx); err != nil {
		return fmt.Sprintf("<restore error: %v>", err)
	}
	return sb.String()
}

func stripAllParentheses(expr ast.ExprNode) ast.ExprNode {
	if expr == nil {
		return nil
	}
	n, ok := expr.Accept(&parenthesesStripper{})
	if !ok {
		return expr
	}
	if e, ok := n.(ast.ExprNode); ok {
		return e
	}
	return expr
}

func buildMLogDeltaSelect(
	dbName pmodel.CIStr,
	mlogTable *model.TableInfo,
	mvSel *ast.SelectStmt,
	mvCols []*model.ColumnInfo,
	groupKeyOffsets []int,
	aggCols []aggColInfo,
	hasMinMax bool,
	opt BuildOptions,
) (*ast.SelectStmt, error) {
	// Strip qualifiers in WHERE so it can be evaluated against mv-log rows (single table).
	if mvSel.Where != nil {
		mvSel.Where.Accept(&columnQualifierStripper{})
	}

	fields := make([]*ast.SelectField, 0, len(groupKeyOffsets)+1+len(aggCols)+1)

	// Group keys: keep base-table column expression, alias to MV column name.
	for _, mvOffset := range groupKeyOffsets {
		mvColName := mvCols[mvOffset].Name
		baseColExpr, err := groupKeyBaseColExprAtOffset(mvSel, mvOffset)
		if err != nil {
			return nil, err
		}
		fields = append(fields, &ast.SelectField{Expr: baseColExpr, AsName: mvColName})
	}

	// old_new is signed delta marker in mlog rows: +1 for added/new row, -1 for removed/old row.
	oldNewCol := colExpr(model.MaterializedViewLogOldNewColumnName)

	// Always compute delta count(*) for stage-1.
	fields = append(fields, &ast.SelectField{
		Expr:   aggSum(oldNewCol),
		AsName: pmodel.NewCIStr(deltaCntStarName),
	})

	// Per aggregate column deltas.
	// For normal aggs (COUNT/SUM), old_new sign naturally encodes add/remove contribution.
	for _, ac := range aggCols {
		switch ac.info.Kind {
		case AggCountStar:
			// already handled above.
			continue
		case AggCount:
			if ac.argExpr == nil {
				return nil, errors.New("COUNT aggregate argument is nil for mvmerge")
			}
			cond := &ast.IsNullExpr{Expr: ac.argExpr, Not: true} // expr IS NOT NULL
			fields = append(fields, &ast.SelectField{
				Expr:   aggSum(ifExpr(cond, oldNewCol, ast.NewValueExpr(int64(0), "", ""))),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		case AggSum:
			if ac.argExpr == nil {
				return nil, errors.New("SUM aggregate argument is nil for mvmerge")
			}
			addedCond := binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(1), "", ""))
			fields = append(fields, &ast.SelectField{
				Expr:   aggSum(ifExpr(addedCond, ac.argExpr, &ast.UnaryOperationExpr{Op: opcode.Minus, V: ac.argExpr})),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		case AggMax:
			// Track only candidates from added rows; removed rows are handled by detail update path.
			argCol := colExpr(ac.info.ArgColName)
			fields = append(fields, &ast.SelectField{
				Expr:   aggMax(ifExpr(binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(1), "", "")), argCol, ast.NewValueExpr(nil, "", ""))),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		case AggMin:
			// Same as MAX: only additions contribute to quick-update candidate.
			argCol := colExpr(ac.info.ArgColName)
			fields = append(fields, &ast.SelectField{
				Expr:   aggMin(ifExpr(binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(1), "", "")), argCol, ast.NewValueExpr(nil, "", ""))),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		default:
			return nil, errors.Errorf("unsupported agg kind %v", ac.info.Kind)
		}
	}

	if hasMinMax {
		// removed_rows := SUM(IF old_new = -1 THEN 1 ELSE 0)
		cond := binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(-1), "", ""))
		fields = append(fields, &ast.SelectField{
			Expr:   aggSum(ifExpr(cond, ast.NewValueExpr(int64(1), "", ""), ast.NewValueExpr(int64(0), "", ""))),
			AsName: pmodel.NewCIStr(removedRowsName),
		})
	}

	mlogFrom := &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{
		Source: &ast.TableName{Schema: dbName, Name: mlogTable.Name},
	}}}

	// Restrict mlog scan to the incremental window (FromTS, ToTS], then apply MV predicate.
	tsCol := colExpr(model.ExtraCommitTSName.L)
	tsRange := andExpr(
		binary(opcode.GT, tsCol, ast.NewValueExpr(opt.FromTS, "", "")),
		binary(opcode.LE, tsCol, ast.NewValueExpr(opt.ToTS, "", "")),
	)
	where := tsRange
	if mvSel.Where != nil {
		where = andExpr(where, mvSel.Where)
	}

	groupBy := &ast.GroupByClause{Items: make([]*ast.ByItem, 0, len(groupKeyOffsets))}
	for _, mvOffset := range groupKeyOffsets {
		baseColExpr, err := groupKeyBaseColExprAtOffset(mvSel, mvOffset)
		if err != nil {
			return nil, err
		}
		groupBy.Items = append(groupBy.Items, &ast.ByItem{Expr: baseColExpr, NullOrder: true})
	}

	return &ast.SelectStmt{
		Fields:  &ast.FieldList{Fields: fields},
		From:    mlogFrom,
		Where:   where,
		GroupBy: groupBy,
	}, nil
}

func buildMergeSourceSelect(
	dbName pmodel.CIStr,
	mv *model.TableInfo,
	mvCols []*model.ColumnInfo,
	groupKeySet map[int]struct{},
	groupKeyOffsets []int,
	deltaSel *ast.SelectStmt,
	aggCols []aggColInfo,
	hasMinMax bool,
) (*ast.SelectStmt, []DeltaColumn, *DeltaColumn, error) {
	deltaSrc := &ast.TableSource{Source: deltaSel, AsName: pmodel.NewCIStr(deltaTableAlias)}
	mvSrc := &ast.TableSource{
		Source: &ast.TableName{Schema: dbName, Name: mv.Name},
		AsName: pmodel.NewCIStr(mvTableAlias),
	}

	// Build null-safe join conditions on group keys.
	var onExpr ast.ExprNode
	for _, mvOffset := range groupKeyOffsets {
		colName := mvCols[mvOffset].Name
		deltaCol := qualColExpr(deltaTableAlias, colName.O)
		mvCol := qualColExpr(mvTableAlias, colName.O)
		cmp := binary(opcode.NullEQ, deltaCol, mvCol)
		if onExpr == nil {
			onExpr = cmp
		} else {
			onExpr = andExpr(onExpr, cmp)
		}
	}
	if onExpr == nil {
		return nil, nil, nil, errors.New("empty join key for mvmerge")
	}

	// Use delta as left side so every changed group survives even when MV row doesn't exist yet.
	join := &ast.Join{
		Left:  deltaSrc,
		Right: mvSrc,
		Tp:    ast.LeftJoin,
		On:    &ast.OnCondition{Expr: onExpr},
	}

	// SELECT: MV columns first (group keys filled by COALESCE), then delta columns.
	fields := make([]*ast.SelectField, 0, len(mvCols)+1+len(aggCols)+1)
	for i, col := range mvCols {
		if _, ok := groupKeySet[i]; ok {
			fields = append(fields, &ast.SelectField{
				// Group key may come from either side: missing MV row for new groups, missing delta row should not happen.
				Expr:   coalesce(qualColExpr(mvTableAlias, col.Name.O), qualColExpr(deltaTableAlias, col.Name.O)),
				AsName: col.Name,
			})
			continue
		}
		fields = append(fields, &ast.SelectField{
			Expr:   qualColExpr(mvTableAlias, col.Name.O),
			AsName: col.Name,
		})
	}

	deltaColumns := make([]DeltaColumn, 0, 1+len(aggCols)+1)
	var removedDelta *DeltaColumn
	nextOffset := len(mvCols)
	// Keep all delta columns contiguous after MV columns, and track their absolute output offsets.
	addDeltaCol := func(name string) int {
		off := nextOffset
		nextOffset++
		deltaColumns = append(deltaColumns, DeltaColumn{Name: name, Offset: off})
		return off
	}

	// delta_cnt_star is always included.
	addDeltaCol(deltaCntStarName)
	fields = append(fields, &ast.SelectField{
		Expr:   qualColExpr(deltaTableAlias, deltaCntStarName),
		AsName: pmodel.NewCIStr(deltaCntStarName),
	})
	for _, ac := range aggCols {
		if ac.info.Kind == AggCountStar {
			continue
		}
		addDeltaCol(ac.deltaName)
		fields = append(fields, &ast.SelectField{
			Expr:   qualColExpr(deltaTableAlias, ac.deltaName),
			AsName: pmodel.NewCIStr(ac.deltaName),
		})
	}
	if hasMinMax {
		off := addDeltaCol(removedRowsName)
		fields = append(fields, &ast.SelectField{
			Expr:   qualColExpr(deltaTableAlias, removedRowsName),
			AsName: pmodel.NewCIStr(removedRowsName),
		})
		removedDelta = &DeltaColumn{Name: removedRowsName, Offset: off}
	}

	return &ast.SelectStmt{
		Fields: &ast.FieldList{Fields: fields},
		From:   &ast.TableRefsClause{TableRefs: join},
	}, deltaColumns, removedDelta, nil
}

func groupKeyBaseColExprAtOffset(mvSel *ast.SelectStmt, mvOffset int) (*ast.ColumnNameExpr, error) {
	if mvOffset < 0 || mvOffset >= len(mvSel.Fields.Fields) {
		return nil, errors.Errorf("invalid mv offset %d", mvOffset)
	}
	f := mvSel.Fields.Fields[mvOffset]
	col, ok := f.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, errors.Errorf("mv field at offset %d is not a group key column", mvOffset)
	}
	// Return a stripped (unqualified) column name expression.
	return colExpr(col.Name.Name.O), nil
}

func dbNameByTableID(is infoschema.InfoSchema, tableID int64) (pmodel.CIStr, error) {
	item, ok := is.TableItemByID(tableID)
	if !ok {
		return pmodel.CIStr{}, errors.Errorf("table id %d not found in infoschema", tableID)
	}
	if item.DBName.L == "" {
		return pmodel.CIStr{}, errors.Errorf("table id %d has empty schema name in infoschema", tableID)
	}
	return item.DBName, nil
}

func hasColumn(t *model.TableInfo, colName string) bool {
	colName = strings.ToLower(colName)
	for _, c := range t.Columns {
		if c.Name.L == colName {
			return true
		}
	}
	return false
}

type columnQualifierStripper struct{}

func (*columnQualifierStripper) Enter(n ast.Node) (ast.Node, bool) {
	cn, ok := n.(*ast.ColumnNameExpr)
	if !ok || cn.Name == nil {
		return n, false
	}
	cn.Name.Schema = pmodel.CIStr{}
	cn.Name.Table = pmodel.CIStr{}
	return n, false
}

func (*columnQualifierStripper) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

type parenthesesStripper struct{}

func (*parenthesesStripper) Enter(n ast.Node) (ast.Node, bool) {
	return n, false
}

func (*parenthesesStripper) Leave(n ast.Node) (ast.Node, bool) {
	if p, ok := n.(*ast.ParenthesesExpr); ok && p.Expr != nil {
		return p.Expr, true
	}
	return n, true
}

func qualColExpr(tableAlias string, colName string) *ast.ColumnNameExpr {
	return &ast.ColumnNameExpr{Name: &ast.ColumnName{
		Table: pmodel.NewCIStr(tableAlias),
		Name:  pmodel.NewCIStr(colName),
	}}
}

func colExpr(colName string) *ast.ColumnNameExpr {
	return &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: pmodel.NewCIStr(colName)}}
}

func binary(op opcode.Op, l, r ast.ExprNode) *ast.BinaryOperationExpr {
	return &ast.BinaryOperationExpr{Op: op, L: l, R: r}
}

func andExpr(l, r ast.ExprNode) ast.ExprNode {
	if l == nil {
		return r
	}
	if r == nil {
		return l
	}
	return binary(opcode.LogicAnd, l, r)
}

func aggSum(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncSum, Args: []ast.ExprNode{arg}}
}

func aggMax(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncMax, Args: []ast.ExprNode{arg}}
}

func aggMin(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncMin, Args: []ast.ExprNode{arg}}
}

func ifExpr(cond, trueExpr, falseExpr ast.ExprNode) *ast.FuncCallExpr {
	return &ast.FuncCallExpr{
		Tp:     ast.FuncCallExprTypeGeneric,
		FnName: pmodel.NewCIStr("IF"),
		Args:   []ast.ExprNode{cond, trueExpr, falseExpr},
	}
}

func coalesce(args ...ast.ExprNode) *ast.FuncCallExpr {
	return &ast.FuncCallExpr{
		Tp:     ast.FuncCallExprTypeGeneric,
		FnName: pmodel.NewCIStr("COALESCE"),
		Args:   args,
	}
}
