// Copyright 2018 PingCAP, Inc.
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
	"context"
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Use cache to avoid allocating memory every time.
var subQueryCheckerPool = &sync.Pool{New: func() any { return &subQueryChecker{} }}

type subQueryChecker struct {
	hasSubQuery bool
}

func (s *subQueryChecker) Enter(in ast.Node) (node ast.Node, skipChildren bool) {
	if s.hasSubQuery {
		return in, true
	}

	if _, ok := in.(*ast.SubqueryExpr); ok {
		s.hasSubQuery = true
		return in, true
	}

	return in, false
}

func (s *subQueryChecker) Leave(in ast.Node) (ast.Node, bool) {
	// Before we enter the sub-query, we should keep visiting its children.
	return in, !s.hasSubQuery
}

func isExprHasSubQuery(expr ast.Node) bool {
	checker := subQueryCheckerPool.Get().(*subQueryChecker)
	defer func() {
		// Do not forget to reset the flag.
		checker.hasSubQuery = false
		subQueryCheckerPool.Put(checker)
	}()
	expr.Accept(checker)
	return checker.hasSubQuery
}

func checkIfAssignmentListHasSubQuery(list []*ast.Assignment) bool {
	return slices.ContainsFunc(list, func(assignment *ast.Assignment) bool {
		return isExprHasSubQuery(assignment.Expr)
	})
}

func tryUpdatePointPlan(ctx base.PlanContext, updateStmt *ast.UpdateStmt, resolveCtx *resolve.Context) base.Plan {
	// Avoid using the point_get when assignment_list contains the sub-query in the UPDATE.
	if checkIfAssignmentListHasSubQuery(updateStmt.List) {
		return nil
	}

	selStmt := &ast.SelectStmt{
		TableHints: updateStmt.TableHints,
		Fields:     &ast.FieldList{},
		From:       updateStmt.TableRefs,
		Where:      updateStmt.Where,
		OrderBy:    updateStmt.Order,
		Limit:      updateStmt.Limit,
	}
	pointGet := tryPointGetPlan(ctx, selStmt, resolveCtx, true)
	if pointGet != nil {
		if pointGet.IsTableDual {
			dual := physicalop.PhysicalTableDual{}.Init(ctx, &property.StatsInfo{}, 0)
			dual.SetOutputNames(pointGet.OutputNames())
			return dual
		}
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointUpdatePlan(ctx, pointGet, pointGet.DBName, pointGet.TblInfo, updateStmt, resolveCtx)
	}
	batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt, resolveCtx)
	if batchPointGet != nil {
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointUpdatePlan(ctx, batchPointGet, batchPointGet.DBName, batchPointGet.TblInfo, updateStmt, resolveCtx)
	}
	return nil
}

func buildPointUpdatePlan(ctx base.PlanContext, pointPlan base.PhysicalPlan, dbName string, tbl *model.TableInfo, updateStmt *ast.UpdateStmt, resolveCtx *resolve.Context) base.Plan {
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv, mysql.UpdatePriv) != nil {
		return nil
	}
	orderedList, allAssignmentsAreConstant := buildOrderedList(ctx, pointPlan, updateStmt.List)
	if orderedList == nil {
		return nil
	}
	handleCols := buildHandleCols(dbName, tbl, pointPlan)
	updatePlan := physicalop.Update{
		SelectPlan:  pointPlan,
		OrderedList: orderedList,
		TblColPosInfos: physicalop.TblColPosInfoSlice{
			physicalop.TblColPosInfo{
				TblID:      tbl.ID,
				Start:      0,
				End:        pointPlan.Schema().Len(),
				HandleCols: handleCols,
			},
		},
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
		VirtualAssignmentsOffset:  len(orderedList),
		IgnoreError:               updateStmt.IgnoreErr,
	}.Init(ctx)
	updatePlan.SetOutputNames(pointPlan.OutputNames())
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	t, _ := is.TableByID(context.Background(), tbl.ID)
	updatePlan.TblID2Table = map[int64]table.Table{
		tbl.ID: t,
	}
	if tbl.GetPartitionInfo() != nil {
		pt := t.(table.PartitionedTable)
		nodeW := resolve.NewNodeWWithCtx(updateStmt.TableRefs.TableRefs, resolveCtx)
		updateTableList := ExtractTableList(nodeW, true)
		updatePlan.PartitionedTable = make([]table.PartitionedTable, 0, len(updateTableList))
		for _, updateTable := range updateTableList {
			if len(updateTable.PartitionNames) > 0 {
				pids := make(map[int64]struct{}, len(updateTable.PartitionNames))
				for _, name := range updateTable.PartitionNames {
					pid, err := tables.FindPartitionByName(tbl, name.L)
					if err != nil {
						return updatePlan
					}
					pids[pid] = struct{}{}
				}
				pt = tables.NewPartitionTableWithGivenSets(pt, pids)
			}
			updatePlan.PartitionedTable = append(updatePlan.PartitionedTable, pt)
		}
	}
	err := updatePlan.BuildOnUpdateFKTriggers(ctx, is, updatePlan.TblID2Table)
	if err != nil {
		return nil
	}
	return updatePlan
}

func buildOrderedList(ctx base.PlanContext, plan base.Plan, list []*ast.Assignment,
) (orderedList []*expression.Assignment, allAssignmentsAreConstant bool) {
	orderedList = make([]*expression.Assignment, 0, len(list))
	allAssignmentsAreConstant = true
	for _, assign := range list {
		idx, err := expression.FindFieldName(plan.OutputNames(), assign.Column)
		if idx == -1 || err != nil {
			return nil, true
		}
		col := plan.Schema().Columns[idx]
		newAssign := &expression.Assignment{
			Col:     col,
			ColName: plan.OutputNames()[idx].ColName,
		}
		defaultExpr := physicalop.ExtractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
		}
		expr, err := rewriteAstExprWithPlanCtx(ctx, assign.Expr, plan.Schema(), plan.OutputNames(), false)
		if err != nil {
			return nil, true
		}
		castToTP := col.GetStaticType()
		if castToTP.GetType() == mysql.TypeEnum && assign.Expr.GetType().EvalType() == types.ETInt {
			castToTP.AddFlag(mysql.EnumSetAsIntFlag)
		}
		expr = expression.BuildCastFunction(ctx.GetExprCtx(), expr, castToTP)
		if allAssignmentsAreConstant {
			_, isConst := expr.(*expression.Constant)
			allAssignmentsAreConstant = isConst
		}

		newAssign.Expr, err = expr.ResolveIndices(plan.Schema())
		if err != nil {
			return nil, true
		}
		orderedList = append(orderedList, newAssign)
	}
	return orderedList, allAssignmentsAreConstant
}

func tryDeletePointPlan(ctx base.PlanContext, delStmt *ast.DeleteStmt, resolveCtx *resolve.Context) base.Plan {
	if delStmt.IsMultiTable {
		return nil
	}
	selStmt := &ast.SelectStmt{
		TableHints: delStmt.TableHints,
		Fields:     &ast.FieldList{},
		From:       delStmt.TableRefs,
		Where:      delStmt.Where,
		OrderBy:    delStmt.Order,
		Limit:      delStmt.Limit,
	}
	if pointGet := tryPointGetPlan(ctx, selStmt, resolveCtx, true); pointGet != nil {
		if pointGet.IsTableDual {
			dual := physicalop.PhysicalTableDual{}.Init(ctx, &property.StatsInfo{}, 0)
			dual.SetOutputNames(pointGet.OutputNames())
			return dual
		}
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointDeletePlan(ctx, pointGet, pointGet.DBName, pointGet.TblInfo, delStmt.IgnoreErr)
	}
	if batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt, resolveCtx); batchPointGet != nil {
		if ctx.GetSessionVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUpdate})
		}
		return buildPointDeletePlan(ctx, batchPointGet, batchPointGet.DBName, batchPointGet.TblInfo, delStmt.IgnoreErr)
	}
	return nil
}

func buildPointDeletePlan(ctx base.PlanContext, pointPlan base.PhysicalPlan, dbName string, tbl *model.TableInfo, ignoreErr bool) base.Plan {
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv, mysql.DeletePriv) != nil {
		return nil
	}
	handleCols := buildHandleCols(dbName, tbl, pointPlan)
	var err error
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	t, _ := is.TableByID(context.Background(), tbl.ID)
	intest.Assert(t != nil, "The point get executor is accessing a table without meta info.")
	colPosInfo, err := initColPosInfo(tbl.ID, pointPlan.OutputNames(), handleCols)
	if err != nil {
		return nil
	}
	err = buildSingleTableColPosInfoForDelete(t, &colPosInfo, 0)
	if err != nil {
		return nil
	}
	delPlan := physicalop.Delete{
		SelectPlan:     pointPlan,
		TblColPosInfos: []physicalop.TblColPosInfo{colPosInfo},
		IgnoreErr:      ignoreErr,
	}.Init(ctx)
	tblID2Table := map[int64]table.Table{tbl.ID: t}
	err = delPlan.BuildOnDeleteFKTriggers(ctx, is, tblID2Table)
	if err != nil {
		return nil
	}
	return delPlan
}

func findCol(tbl *model.TableInfo, colName *ast.ColumnName) *model.ColumnInfo {
	if colName.Name.L == model.ExtraHandleName.L && !tbl.PKIsHandle {
		colInfo := model.NewExtraHandleColInfo()
		colInfo.Offset = len(tbl.Columns) - 1
		return colInfo
	}
	for _, col := range tbl.Columns {
		if col.Name.L == colName.Name.L {
			return col
		}
	}
	return nil
}

func colInfoToColumn(col *model.ColumnInfo, idx int) *expression.Column {
	return &expression.Column{
		RetType:  col.FieldType.Clone(),
		ID:       col.ID,
		UniqueID: int64(col.Offset),
		Index:    idx,
		OrigName: col.Name.L,
	}
}

func buildHandleCols(dbName string, tbl *model.TableInfo, pointget base.PhysicalPlan) util.HandleCols {
	schema := pointget.Schema()
	// fields len is 0 for update and delete.
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				return util.NewIntHandleCols(schema.Columns[i])
			}
		}
	}

	if tbl.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tbl)
		return util.NewCommonHandleCols(tbl, pkIdx, schema.Columns)
	}

	handleCol := colInfoToColumn(model.NewExtraHandleColInfo(), schema.Len())
	schema.Append(handleCol)
	newOutputNames := pointget.OutputNames().Shallow()
	tableAliasName := tbl.Name
	if schema.Len() > 0 {
		tableAliasName = pointget.OutputNames()[0].TblName
	}
	newOutputNames = append(newOutputNames, &types.FieldName{
		DBName:      ast.NewCIStr(dbName),
		TblName:     tableAliasName,
		OrigTblName: tbl.Name,
		ColName:     model.ExtraHandleName,
	})
	pointget.SetOutputNames(newOutputNames)
	return util.NewIntHandleCols(handleCol)
}

// TODO: Remove this, by enabling all types of partitioning
// and update/add tests
func getHashOrKeyPartitionColumnName(ctx base.PlanContext, tbl *model.TableInfo) *ast.CIStr {
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return nil
	}
	if pi.Type != ast.PartitionTypeHash && pi.Type != ast.PartitionTypeKey {
		return nil
	}
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	table, ok := is.TableByID(context.Background(), tbl.ID)
	if !ok {
		return nil
	}
	// PartitionExpr don't need columns and names for hash partition.
	partitionExpr := table.(base.PartitionTable).PartitionExpr()
	if pi.Type == ast.PartitionTypeKey {
		// used to judge whether the key partition contains only one field
		if len(pi.Columns) != 1 {
			return nil
		}
		return &pi.Columns[0]
	}
	expr := partitionExpr.OrigExpr
	col, ok := expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil
	}
	return &col.Name.Name
}
