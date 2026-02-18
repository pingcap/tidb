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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

	DeltaColumns []DeltaColumn
	AggInfos     []AggInfo

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

	// DeltaOffset is the offset (0-based) in the full merge-source output schema.
	DeltaOffset int

	// NeedsDetailOnRemoval indicates executor must do detail update for groups with removals.
	NeedsDetailOnRemoval bool
}

// SelectOptimizer converts the merge-source SELECT statement into a physical plan.
// The caller usually does preprocess -> logical plan build -> optimize.
type SelectOptimizer func(ctx context.Context, sel *ast.SelectStmt) (base.PhysicalPlan, types.NameSlice, error)

func Build(ctx context.Context, sctx base.PlanContext, is infoschema.InfoSchema, mv *model.TableInfo, opt BuildOptions, optimize SelectOptimizer) (*BuildResult, error) {
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

	deltaSel, err := buildMLogDeltaSelect(mvDBName, mlogTable, mvSel, mv.Columns, groupKeyOffsets, aggCols, hasMinMax, opt)
	if err != nil {
		return nil, err
	}

	mergeSel, deltaColumns, removedDelta, err := buildMergeSourceSelect(mvDBName, mv, mv.Columns, groupKeySet, groupKeyOffsets, deltaSel, aggCols, hasMinMax)
	if err != nil {
		return nil, err
	}

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

	// Patch delta offsets into AggInfos.
	deltaOffsetByName := make(map[string]int, len(deltaColumns))
	for _, dc := range deltaColumns {
		deltaOffsetByName[dc.Name] = dc.Offset
	}
	outAggInfos := make([]AggInfo, 0, len(aggCols))
	for _, ac := range aggCols {
		di := ac.info
		if ac.deltaName != "" {
			off, ok := deltaOffsetByName[ac.deltaName]
			if !ok {
				return nil, errors.Errorf("internal error: delta column %s not found in output", ac.deltaName)
			}
			di.DeltaOffset = off
		}
		outAggInfos = append(outAggInfos, di)
	}

	res := &BuildResult{
		Plan:              plan,
		OutputNames:       outputNames,
		MVTableID:         mv.ID,
		BaseTableID:       baseTableID,
		MLogTableID:       mlogTableID,
		MVColumnCount:     len(mv.Columns),
		GroupKeyMVOffsets: append([]int(nil), groupKeyOffsets...),
		DeltaColumns:      deltaColumns,
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

func extractAggInfosFromMVSelect(sel *ast.SelectStmt) (aggCols []struct {
	info      AggInfo
	deltaName string
}, hasMinMax bool, _ error) {
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
			if !isCountStarOrOneArg(agg.Args[0]) {
				return nil, false, errors.New("only COUNT(*)/COUNT(1) is supported in mvmerge stage-1")
			}
			aggCols = append(aggCols, struct {
				info      AggInfo
				deltaName string
			}{
				info: AggInfo{
					Kind:     AggCountStar,
					MVOffset: i,
				},
				deltaName: deltaCntStarName,
			})
		case ast.AggFuncSum:
			argCol, ok := agg.Args[0].(*ast.ColumnNameExpr)
			if !ok {
				return nil, false, errors.New("SUM argument must be a column name for mvmerge")
			}
			aggCols = append(aggCols, struct {
				info      AggInfo
				deltaName string
			}{
				info: AggInfo{
					Kind:       AggSum,
					MVOffset:   i,
					ArgColName: argCol.Name.Name.L,
				},
				deltaName: fmt.Sprintf("__mvmerge_delta_sum_%d", i),
			})
		case ast.AggFuncMax:
			argCol, ok := agg.Args[0].(*ast.ColumnNameExpr)
			if !ok {
				return nil, false, errors.New("MAX argument must be a column name for mvmerge")
			}
			hasMinMax = true
			aggCols = append(aggCols, struct {
				info      AggInfo
				deltaName string
			}{
				info: AggInfo{
					Kind:                 AggMax,
					MVOffset:             i,
					ArgColName:           argCol.Name.Name.L,
					NeedsDetailOnRemoval: true,
				},
				deltaName: fmt.Sprintf("__mvmerge_max_in_added_%d", i),
			})
		case ast.AggFuncMin:
			argCol, ok := agg.Args[0].(*ast.ColumnNameExpr)
			if !ok {
				return nil, false, errors.New("MIN argument must be a column name for mvmerge")
			}
			hasMinMax = true
			aggCols = append(aggCols, struct {
				info      AggInfo
				deltaName string
			}{
				info: AggInfo{
					Kind:                 AggMin,
					MVOffset:             i,
					ArgColName:           argCol.Name.Name.L,
					NeedsDetailOnRemoval: true,
				},
				deltaName: fmt.Sprintf("__mvmerge_min_in_added_%d", i),
			})
		default:
			return nil, false, errors.Errorf("unsupported aggregate function %s in mvmerge stage-1", agg.F)
		}
	}
	return aggCols, hasMinMax, nil
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

func buildMLogDeltaSelect(
	dbName pmodel.CIStr,
	mlogTable *model.TableInfo,
	mvSel *ast.SelectStmt,
	mvCols []*model.ColumnInfo,
	groupKeyOffsets []int,
	aggCols []struct {
		info      AggInfo
		deltaName string
	},
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

	oldNewCol := colExpr(model.MaterializedViewLogOldNewColumnName)

	// Always compute delta count(*) for stage-1.
	fields = append(fields, &ast.SelectField{
		Expr:   aggSum(oldNewCol),
		AsName: pmodel.NewCIStr(deltaCntStarName),
	})

	// Per aggregate column deltas.
	for _, ac := range aggCols {
		switch ac.info.Kind {
		case AggCountStar:
			// already handled above.
			continue
		case AggSum:
			argCol := colExpr(ac.info.ArgColName)
			fields = append(fields, &ast.SelectField{
				Expr:   aggSum(binary(opcode.Mul, oldNewCol, argCol)),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		case AggMax:
			argCol := colExpr(ac.info.ArgColName)
			fields = append(fields, &ast.SelectField{
				Expr:   aggMax(ifExpr(binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(1), "", "")), argCol, ast.NewValueExpr(nil, "", ""))),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		case AggMin:
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

	// WHERE commit_ts in (FromTS, ToTS] AND mv_where
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
	aggCols []struct {
		info      AggInfo
		deltaName string
	},
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
