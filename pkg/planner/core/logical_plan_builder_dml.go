// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"slices"
	"strings"

	"github.com/bits-and-blooms/bitset"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func buildColumns2HandleWithWrtiableColumns(
	names []*types.FieldName,
	tblID2Handle map[int64][]util.HandleCols,
	tblID2Table map[int64]table.Table,
) (cols2Handles physicalop.TblColPosInfoSlice, err error) {
	for tblID, handleCols := range tblID2Handle {
		tbl := tblID2Table[tblID]
		tblLen := len(tbl.WritableCols())
		for _, handleCol := range handleCols {
			offset, err := getTableOffset(names, names[handleCol.GetCol(0).Index])
			if err != nil {
				return nil, err
			}
			end := offset + tblLen
			cols2Handles = append(cols2Handles, physicalop.TblColPosInfo{TblID: tblID, Start: offset, End: end, HandleCols: handleCol})
		}
	}
	slices.SortFunc(cols2Handles, func(a, b physicalop.TblColPosInfo) int {
		return a.Cmp(b)
	})
	return cols2Handles, nil
}

// pruneAndBuildColPositionInfoForDelete prune unneeded columns and construct the column position information.
// We'll have two kinds of columns seen by DELETE:
//  1. The columns that are public. They are the columns that not affected by any DDL.
//  2. The columns that are not public. They are the columns that are affected by DDL.
//     But we need them because the non-public indexes may rely on them.
//
// The two kind of columns forms the whole columns of the table. Public part first.
//
// This function returns the following things:
//  1. TblColPosInfoSlice stores the each table's column's position information in the final mixed row.
//  2. The bitset records which column in the original mixed row is not pruned..
//  3. The error we meet during the algorithm.
func pruneAndBuildColPositionInfoForDelete(
	names []*types.FieldName,
	tblID2Handle map[int64][]util.HandleCols,
	tblID2Table map[int64]table.Table,
	hasFK bool,
) (physicalop.TblColPosInfoSlice, *bitset.BitSet, error) {
	cols2PosInfos := make(physicalop.TblColPosInfoSlice, 0, len(tblID2Handle))
	for tid, handleCols := range tblID2Handle {
		for _, handleCol := range handleCols {
			curColPosInfo, err := initColPosInfo(tid, names, handleCol)
			if err != nil {
				return nil, nil, err
			}
			cols2PosInfos = append(cols2PosInfos, curColPosInfo)
		}
	}
	// Sort by start position. To do the later column pruning.
	slices.SortFunc(cols2PosInfos, func(a, b physicalop.TblColPosInfo) int {
		return a.Cmp(b)
	})
	nonPruned := bitset.New(uint(len(names)))
	nonPruned.SetAll()
	// Always prune the `_tidb_commit_ts` column.
	for i, name := range names {
		if name.ColName.L == model.ExtraCommitTSName.L {
			nonPruned.Clear(uint(i))
			continue
		}
	}
	// prunedColCnt records how many columns in `names` have been pruned before the current table (before the current
	// TblColPosInfo.Start). To avoid repeatedly counting the pruned columns, we use nextCheckIdx to record
	// the next position to check.
	var prunedColCnt, nextCheckIdx int
	var err error
	for i := range cols2PosInfos {
		cols2PosInfo := &cols2PosInfos[i]
		for j := nextCheckIdx; j < cols2PosInfo.Start; j++ {
			if !nonPruned.Test(uint(j)) {
				prunedColCnt++
			}
		}
		nextCheckIdx = cols2PosInfo.Start

		tbl := tblID2Table[cols2PosInfo.TblID]
		tblInfo := tbl.Meta()
		// If it's partitioned table, or has foreign keys, or has partial index, or is point get plan, we can't prune the columns, currently.
		// nonPrunedSet will be nil if it's a point get or has foreign keys.
		// If there is foreign key, we can't prune the columns.
		// Use a very relax check for foreign key cascades and checks.
		// If there's one table containing foreign keys, all of the tables would not do pruning.
		// It should be strict in the future or just support pruning column when there is foreign key.
		skipPruning := tblInfo.GetPartitionInfo() != nil || hasFK || nonPruned == nil
		for _, idx := range tblInfo.Indices {
			if len(idx.ConditionExprString) > 0 {
				// If the index has a partial index condition, we can't prune the columns.
				skipPruning = true
				break
			}
		}
		if skipPruning {
			err = buildSingleTableColPosInfoForDelete(tbl, cols2PosInfo, prunedColCnt)
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		err = pruneAndBuildSingleTableColPosInfoForDelete(tbl, tblInfo.Name.O, names, cols2PosInfo, prunedColCnt, nonPruned)
		if err != nil {
			return nil, nil, err
		}
	}
	return cols2PosInfos, nonPruned, nil
}

// initColPosInfo initializes the column position information.
// It's used before we call pruneAndBuildSingleTableColPosInfoForDelete or buildSingleTableColPosInfoForDelete.
// It initializes the needed information for the following pruning: the tid, starting position and the handle column.
func initColPosInfo(tid int64, names []*types.FieldName, handleCol util.HandleCols) (physicalop.TblColPosInfo, error) {
	offset, err := getTableOffset(names, names[handleCol.GetCol(0).Index])
	if err != nil {
		return physicalop.TblColPosInfo{}, err
	}
	return physicalop.TblColPosInfo{
		TblID:      tid,
		Start:      offset,
		HandleCols: handleCol,
	}, nil
}

// buildSingleTableColPosInfoForDelete builds columns mapping for delete without pruning any columns.
// It's temp code path for partition table, foreign key and point get plan.
func buildSingleTableColPosInfoForDelete(tbl table.Table, colPosInfo *physicalop.TblColPosInfo, prePrunedCount int) error {
	tblLen := len(tbl.DeletableCols())
	colPosInfo.Start -= prePrunedCount
	colPosInfo.End = colPosInfo.Start + tblLen
	for col := range colPosInfo.HandleCols.IterColumns() {
		col.Index -= prePrunedCount
	}
	return nil
}

// pruneAndBuildSingleTableColPosInfoForDelete builds columns mapping for delete.
// And it will try to prune columns that not used in pk or indexes.
func pruneAndBuildSingleTableColPosInfoForDelete(
	t table.Table,
	tableName string,
	names []*types.FieldName,
	colPosInfo *physicalop.TblColPosInfo,
	prePrunedCount int,
	nonPrunedSet *bitset.BitSet,
) error {
	// Columns can be seen by DELETE are the deletable columns.
	deletableCols := t.DeletableCols()
	deletableIdxs := t.DeletableIndices()
	tblLen := len(deletableCols)

	// Fix the start position of the columns.
	originalStart := colPosInfo.Start
	colPosInfo.Start -= prePrunedCount

	// Mark the columns in handle.
	fixedPos := make(map[int]int, len(deletableCols))
	for col := range colPosInfo.HandleCols.IterColumns() {
		fixedPos[col.Index-originalStart] = 0
	}

	// Mark the columns in indexes.
	for _, idx := range deletableIdxs {
		for _, col := range idx.Meta().Columns {
			if col.Offset+originalStart >= len(names) || deletableCols[col.Offset].Name.L != names[col.Offset+originalStart].ColName.L {
				return plannererrors.ErrDeleteNotFoundColumn.GenWithStackByArgs(col.Name.O, tableName)
			}
			fixedPos[col.Offset] = 0
		}
	}

	// Fix the column offsets.
	pruned := 0
	for i := range tblLen {
		if _, ok := fixedPos[i]; !ok {
			nonPrunedSet.Clear(uint(i + originalStart))
			pruned++
			continue
		}
		fixedPos[i] = i - pruned
	}

	// Fix the index layout and fill in table.IndexRowLayoutOption.
	indexColMap := make(map[int64]table.IndexRowLayoutOption, len(deletableIdxs))
	for _, idx := range deletableIdxs {
		idxCols := idx.Meta().Columns
		colPos := make([]int, 0, len(idxCols))
		for _, col := range idxCols {
			colPos = append(colPos, fixedPos[col.Offset])
		}
		indexColMap[idx.Meta().ID] = colPos
	}

	// Fix the column offset of handle columns.
	newStart := originalStart - prePrunedCount
	for col := range colPosInfo.HandleCols.IterColumns() {
		// If the row id the hidden extra row id, it can not be in deletableCols.
		// It will be appended to the end of the row.
		// So we use newStart + tblLen to get the tail, then minus the pruned to the its new offset of the whole mixed row.
		if col.Index-originalStart >= tblLen {
			col.Index = newStart + tblLen - pruned
			continue
		}
		// Its index is the offset in the original mixed row.
		// col.Index-originalStart is the offset in the table.
		// Then we use its offset in the table to find the new offset in the pruned row by fixedPos[col.Index-originalStart].
		// Finally, we plus the newStart to get new offset in the mixed row.
		col.Index = fixedPos[col.Index-originalStart] + newStart
	}
	colPosInfo.End = colPosInfo.Start + tblLen - pruned
	colPosInfo.IndexesRowLayout = indexColMap

	return nil
}

func (b *PlanBuilder) buildUpdate(ctx context.Context, update *ast.UpdateStmt) (base.Plan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(update.TableHints, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current UPDATE statement.
		b.popTableHints()
	}()

	b.inUpdateStmt = true
	b.isForUpdateRead = true

	if update.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		_, err := b.buildWith(ctx, update.With)
		if err != nil {
			return nil, err
		}
	}

	p, err := b.buildResultSetNode(ctx, update.TableRefs.TableRefs, false)
	if err != nil {
		return nil, err
	}

	nodeW := resolve.NewNodeWWithCtx(update.TableRefs.TableRefs, b.resolveCtx)
	tableList := ExtractTableList(nodeW, false)
	for _, t := range tableList {
		dbName := getLowerDB(t.Schema, b.ctx.GetSessionVars())
		// Avoid adding CTE table to the SELECT privilege list, maybe we have better way to do this?
		if _, ok := b.nameMapCTE[t.Name.L]; !ok {
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName, t.Name.L, "", nil)
		}
	}

	oldSchemaLen := p.Schema().Len()
	if update.Where != nil {
		p, err = b.buildSelection(ctx, p, update.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if b.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		if update.TableRefs.TableRefs.Right == nil {
			// buildSelectLock is an optimization that can reduce RPC call.
			// We only need do this optimization for single table update which is the most common case.
			// When TableRefs.Right is nil, it is single table update.
			p, err = b.buildSelectLock(p, &ast.SelectLockInfo{
				LockType: ast.SelectLockForUpdate,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	if update.Order != nil {
		p, err = b.buildSort(ctx, p, update.Order.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	if update.Limit != nil {
		p, err = b.buildLimit(p, update.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Add project to freeze the order of output columns.
	proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldSchemaLen])}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(expression.NewSchema(make([]*expression.Column, oldSchemaLen)...))
	proj.SetOutputNames(make(types.NameSlice, len(p.OutputNames())))
	copy(proj.OutputNames(), p.OutputNames())
	copy(proj.Schema().Columns, p.Schema().Columns[:oldSchemaLen])
	for i := len(proj.OutputNames()) - 1; i >= 0; i-- {
		if proj.OutputNames()[i].ColName.L == model.ExtraCommitTSName.L {
			proj.SetOutputNames(slices.Delete(proj.OutputNames(), i, i+1))
			proj.Schema().Columns = slices.Delete(proj.Schema().Columns, i, i+1)
			proj.Exprs = slices.Delete(proj.Exprs, i, i+1)
		}
	}
	proj.SetChildren(p)
	p = proj

	utlr := &updatableTableListResolver{
		resolveCtx: b.resolveCtx,
	}
	update.Accept(utlr)
	orderedList, np, allAssignmentsAreConstant, err := b.buildUpdateLists(ctx, utlr.updatableTableList, update.List, p)
	if err != nil {
		return nil, err
	}
	p = np

	updt := physicalop.Update{
		OrderedList:               orderedList,
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
		VirtualAssignmentsOffset:  len(update.List),
		IgnoreError:               update.IgnoreErr,
	}.Init(b.ctx)
	updt.SetOutputNames(p.OutputNames())
	// We cannot apply projection elimination when building the subplan, because
	// columns in orderedList cannot be resolved. (^flagEliminateProjection should also be applied in postOptimize)
	updt.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag&^rule.FlagEliminateProjection, p)
	if err != nil {
		return nil, err
	}
	err = updt.ResolveIndices()
	if err != nil {
		return nil, err
	}
	tblID2Handle, err := resolveIndicesForTblID2Handle(b.handleHelper.tailMap(), updt.SelectPlan.Schema())
	if err != nil {
		return nil, err
	}
	tblID2table := make(map[int64]table.Table, len(tblID2Handle))
	for id := range tblID2Handle {
		tblID2table[id], _ = b.is.TableByID(ctx, id)
	}
	updt.TblColPosInfos, err = buildColumns2HandleWithWrtiableColumns(updt.OutputNames(), tblID2Handle, tblID2table)
	if err != nil {
		return nil, err
	}
	updt.PartitionedTable = b.partitionedTable
	updt.TblID2Table = tblID2table
	err = updt.BuildOnUpdateFKTriggers(b.ctx, b.is, tblID2table)
	return updt, err
}

type tblUpdateInfo struct {
	name                string
	pkUpdated           bool
	partitionColUpdated bool
}

// CheckUpdateList checks all related columns in updatable state.
func CheckUpdateList(assignFlags []int, updt *physicalop.Update, newTblID2Table map[int64]table.Table) error {
	updateFromOtherAlias := make(map[int64]tblUpdateInfo)
	for _, content := range updt.TblColPosInfos {
		tbl := newTblID2Table[content.TblID]
		flags := assignFlags[content.Start:content.End]
		var update, updatePK, updatePartitionCol bool
		var partitionColumnNames []ast.CIStr
		if pt, ok := tbl.(table.PartitionedTable); ok && pt != nil {
			partitionColumnNames = pt.GetPartitionColumnNames()
		}

		for i, col := range tbl.WritableCols() {
			// schema may be changed between building plan and building executor
			// If i >= len(flags), it means the target table has been added columns, then we directly skip the check
			if i >= len(flags) {
				continue
			}
			if flags[i] < 0 {
				continue
			}

			if col.State != model.StatePublic {
				return plannererrors.ErrUnknownColumn.GenWithStackByArgs(col.Name, clauseMsg[fieldList])
			}

			update = true
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				updatePK = true
			}
			for _, partColName := range partitionColumnNames {
				if col.Name.L == partColName.L {
					updatePartitionCol = true
				}
			}
		}
		if update {
			// Check for multi-updates on primary key,
			// see https://dev.mysql.com/doc/mysql-errors/5.7/en/server-error-reference.html#error_er_multi_update_key_conflict
			if otherTable, ok := updateFromOtherAlias[tbl.Meta().ID]; ok {
				if otherTable.pkUpdated || updatePK || otherTable.partitionColUpdated || updatePartitionCol {
					return plannererrors.ErrMultiUpdateKeyConflict.GenWithStackByArgs(otherTable.name, updt.OutputNames()[content.Start].TblName.O)
				}
			} else {
				updateFromOtherAlias[tbl.Meta().ID] = tblUpdateInfo{
					name:                updt.OutputNames()[content.Start].TblName.O,
					pkUpdated:           updatePK,
					partitionColUpdated: updatePartitionCol,
				}
			}
		}
	}
	return nil
}

// If tl is CTE, its HintedTable will be nil.
// Only used in build plan from AST after preprocess.
func isCTE(tlW *resolve.TableNameW) bool {
	return tlW == nil
}

func (b *PlanBuilder) buildUpdateLists(ctx context.Context, tableList []*ast.TableName, list []*ast.Assignment, p base.LogicalPlan) (newList []*expression.Assignment, po base.LogicalPlan, allAssignmentsAreConstant bool, e error) {
	b.curClause = fieldList
	// modifyColumns indicates which columns are in set list,
	// and if it is set to `DEFAULT`
	modifyColumns := make(map[string]bool, p.Schema().Len())
	var columnsIdx map[*ast.ColumnName]int
	cacheColumnsIdx := false
	if len(p.OutputNames()) > 16 {
		cacheColumnsIdx = true
		columnsIdx = make(map[*ast.ColumnName]int, len(list))
	}
	for _, assign := range list {
		idx, err := expression.FindFieldName(p.OutputNames(), assign.Column)
		if err != nil {
			return nil, nil, false, err
		}
		if idx < 0 {
			return nil, nil, false, plannererrors.ErrUnknownColumn.GenWithStackByArgs(assign.Column.Name, "field list")
		}
		if cacheColumnsIdx {
			columnsIdx[assign.Column] = idx
		}
		name := p.OutputNames()[idx]
		foundListItem := false
		for _, tl := range tableList {
			tlW := b.resolveCtx.GetTableName(tl)
			if (tl.Schema.L == "" || tl.Schema.L == name.DBName.L) && (tl.Name.L == name.TblName.L) {
				if isCTE(tlW) || tlW.TableInfo.IsView() || tlW.TableInfo.IsSequence() {
					return nil, nil, false, plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(name.TblName.O, "UPDATE")
				}
				foundListItem = true
			}
		}
		if !foundListItem {
			// For case like:
			// 1: update (select * from t1) t1 set b = 1111111 ----- (no updatable table here)
			// 2: update (select 1 as a) as t, t1 set a=1      ----- (updatable t1 don't have column a)
			// --- subQuery is not counted as updatable table.
			return nil, nil, false, plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(name.TblName.O, "UPDATE")
		}
		columnFullName := fmt.Sprintf("%s.%s.%s", name.DBName.L, name.TblName.L, name.ColName.L)
		// We save a flag for the column in map `modifyColumns`
		// This flag indicated if assign keyword `DEFAULT` to the column
		modifyColumns[columnFullName] = physicalop.IsDefaultExprSameColumn(p.OutputNames()[idx:idx+1], assign.Expr)
	}

	// If columns in set list contains generated columns, raise error.
	// And, fill virtualAssignments here; that's for generated columns.
	virtualAssignments := make([]*ast.Assignment, 0)
	for _, tn := range tableList {
		tnW := b.resolveCtx.GetTableName(tn)
		if isCTE(tnW) || tnW.TableInfo.IsView() || tnW.TableInfo.IsSequence() {
			continue
		}

		tableInfo := tnW.TableInfo
		tableVal, found := b.is.TableByID(ctx, tableInfo.ID)
		if !found {
			return nil, nil, false, infoschema.ErrTableNotExists.FastGenByArgs(tnW.DBInfo.Name.O, tableInfo.Name.O)
		}
		for i, colInfo := range tableVal.Cols() {
			if !colInfo.IsGenerated() {
				continue
			}
			columnFullName := fmt.Sprintf("%s.%s.%s", tnW.DBInfo.Name.L, tn.Name.L, colInfo.Name.L)
			isDefault, ok := modifyColumns[columnFullName]
			if ok && colInfo.Hidden {
				return nil, nil, false, plannererrors.ErrUnknownColumn.GenWithStackByArgs(colInfo.Name, clauseMsg[fieldList])
			}
			// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
			// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
			if ok && !isDefault {
				return nil, nil, false, plannererrors.ErrBadGeneratedColumn.GenWithStackByArgs(colInfo.Name.O, tableInfo.Name.O)
			}
			virtualAssignments = append(virtualAssignments, &ast.Assignment{
				Column: &ast.ColumnName{Schema: tn.Schema, Table: tn.Name, Name: colInfo.Name},
				Expr:   tableVal.Cols()[i].GeneratedExpr.Clone(),
			})
		}
	}

	allAssignmentsAreConstant = true
	newList = make([]*expression.Assignment, 0, p.Schema().Len())
	tblDbMap := make(map[string]string, len(tableList))
	for _, tbl := range tableList {
		tblW := b.resolveCtx.GetTableName(tbl)
		if isCTE(tblW) {
			continue
		}
		tblDbMap[tbl.Name.L] = tblW.DBInfo.Name.L
	}

	allAssignments := append(list, virtualAssignments...)
	dependentColumnsModified := make(map[int64]bool)
	for i, assign := range allAssignments {
		var idx int
		var err error
		if cacheColumnsIdx {
			if i, ok := columnsIdx[assign.Column]; ok {
				idx = i
			} else {
				idx, err = expression.FindFieldName(p.OutputNames(), assign.Column)
			}
		} else {
			idx, err = expression.FindFieldName(p.OutputNames(), assign.Column)
		}
		if err != nil {
			return nil, nil, false, err
		}
		col := p.Schema().Columns[idx]
		name := p.OutputNames()[idx]
		var newExpr expression.Expression
		var np base.LogicalPlan
		if i < len(list) {
			// If assign `DEFAULT` to column, fill the `defaultExpr.Name` before rewrite expression
			if expr := physicalop.ExtractDefaultExpr(assign.Expr); expr != nil {
				expr.Name = assign.Column
			}
			newExpr, np, err = b.rewrite(ctx, assign.Expr, p, nil, true)
			if err != nil {
				return nil, nil, false, err
			}
			dependentColumnsModified[col.UniqueID] = true
		} else {
			// rewrite with generation expression
			rewritePreprocess := func(assign *ast.Assignment) func(expr ast.Node) ast.Node {
				return func(expr ast.Node) ast.Node {
					switch x := expr.(type) {
					case *ast.ColumnName:
						return &ast.ColumnName{
							Schema: assign.Column.Schema,
							Table:  assign.Column.Table,
							Name:   x.Name,
						}
					default:
						return expr
					}
				}
			}

			o := b.allowBuildCastArray
			b.allowBuildCastArray = true
			newExpr, np, err = b.rewriteWithPreprocess(ctx, assign.Expr, p, nil, nil, true, rewritePreprocess(assign))
			b.allowBuildCastArray = o
			if err != nil {
				return nil, nil, false, err
			}
			// check if the column may be modified.
			dependentColumns := expression.ExtractDependentColumns(newExpr)
			var mayModified bool
			for _, col := range dependentColumns {
				colTp := col.GetType(b.ctx.GetExprCtx().GetEvalCtx()).GetFlag()
				// If any of the dependent column has on-update-now flag,
				// this virtual generated column may be modified too.
				if mysql.HasOnUpdateNowFlag(colTp) || dependentColumnsModified[col.UniqueID] {
					mayModified = true
					break
				}
			}
			if mayModified {
				dependentColumnsModified[col.UniqueID] = true
			}
			// skip unmodified generated columns
			if !mayModified {
				continue
			}
		}
		if _, isConst := newExpr.(*expression.Constant); !isConst {
			allAssignmentsAreConstant = false
		}
		p = np
		if cols := expression.ExtractColumnSet(newExpr); cols.Len() > 0 {
			b.ctx.GetSessionVars().StmtCtx.ColRefFromUpdatePlan.UnionWith(cols)
		}
		newList = append(newList, &expression.Assignment{Col: col, ColName: name.ColName, Expr: newExpr})
		dbName := name.DBName.L
		// To solve issue#10028, we need to get database name by the table alias name.
		if dbNameTmp, ok := tblDbMap[name.TblName.L]; ok {
			dbName = dbNameTmp
		}
		if dbName == "" {
			dbName = strings.ToLower(b.ctx.GetSessionVars().CurrentDB)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, dbName, name.OrigTblName.L, "", nil)
	}
	return newList, p, allAssignmentsAreConstant, nil
}

func (b *PlanBuilder) buildDelete(ctx context.Context, ds *ast.DeleteStmt) (base.Plan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(ds.TableHints, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current DELETE statement.
		b.popTableHints()
	}()

	b.inDeleteStmt = true
	b.isForUpdateRead = true

	if ds.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		_, err := b.buildWith(ctx, ds.With)
		if err != nil {
			return nil, err
		}
	}

	p, err := b.buildResultSetNode(ctx, ds.TableRefs.TableRefs, false)
	if err != nil {
		return nil, err
	}
	oldSchema := p.Schema()
	oldLen := oldSchema.Len()

	// For explicit column usage, should use the all-public columns.
	if ds.Where != nil {
		p, err = b.buildSelection(ctx, p, ds.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if b.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		if !ds.IsMultiTable {
			p, err = b.buildSelectLock(p, &ast.SelectLockInfo{
				LockType: ast.SelectLockForUpdate,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	if ds.Order != nil {
		p, err = b.buildSort(ctx, p, ds.Order.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	if ds.Limit != nil {
		p, err = b.buildLimit(p, ds.Limit)
		if err != nil {
			return nil, err
		}
	}

	// If the delete is non-qualified it does not require Select Priv
	if ds.Where == nil && ds.Order == nil {
		b.popVisitInfo()
	}
	var authErr error
	sessionVars := b.ctx.GetSessionVars()

	del := physicalop.Delete{
		IsMultiTable: ds.IsMultiTable,
		IgnoreErr:    ds.IgnoreErr,
	}.Init(b.ctx)

	localResolveCtx := resolve.NewContext()
	// Collect visitInfo.
	if ds.Tables != nil {
		// Delete a, b from a, b, c, d... add a and b.
		updatableList := make(map[string]bool)
		tbInfoList := make(map[string]*ast.TableName)
		collectTableName(ds.TableRefs.TableRefs, &updatableList, &tbInfoList)
		for _, tn := range ds.Tables.Tables {
			var canUpdate, foundMatch = false, false
			name := tn.Name.L
			if tn.Schema.L == "" {
				canUpdate, foundMatch = updatableList[name]
			}

			if !foundMatch {
				if tn.Schema.L == "" {
					name = ast.NewCIStr(b.ctx.GetSessionVars().CurrentDB).L + "." + tn.Name.L
				} else {
					name = tn.Schema.L + "." + tn.Name.L
				}
				canUpdate, foundMatch = updatableList[name]
			}
			// check sql like: `delete b from (select * from t) as a, t`
			if !foundMatch {
				return nil, plannererrors.ErrUnknownTable.GenWithStackByArgs(tn.Name.O, "MULTI DELETE")
			}
			// check sql like: `delete a from (select * from t) as a, t`
			if !canUpdate {
				return nil, plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(tn.Name.O, "DELETE")
			}
			tb := tbInfoList[name]
			tnW := b.resolveCtx.GetTableName(tb)
			localResolveCtx.AddTableName(&resolve.TableNameW{
				TableName: tn,
				TableInfo: tnW.TableInfo,
				DBInfo:    tnW.DBInfo,
			})
			tableInfo := tnW.TableInfo
			if tableInfo.IsView() {
				return nil, errors.Errorf("delete view %s is not supported now", tn.Name.O)
			}
			if tableInfo.IsSequence() {
				return nil, errors.Errorf("delete sequence %s is not supported now", tn.Name.O)
			}
			if sessionVars.User != nil {
				authErr = plannererrors.ErrTableaccessDenied.FastGenByArgs("DELETE", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, tb.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, tnW.DBInfo.Name.L, tb.Name.L, "", authErr)
		}
	} else {
		// Delete from a, b, c, d.
		nodeW := resolve.NewNodeWWithCtx(ds.TableRefs.TableRefs, b.resolveCtx)
		tableList := ExtractTableList(nodeW, false)
		for _, v := range tableList {
			tblW := b.resolveCtx.GetTableName(v)
			if isCTE(tblW) {
				return nil, plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(v.Name.O, "DELETE")
			}
			if tblW.TableInfo.IsView() {
				return nil, errors.Errorf("delete view %s is not supported now", v.Name.O)
			}
			if tblW.TableInfo.IsSequence() {
				return nil, errors.Errorf("delete sequence %s is not supported now", v.Name.O)
			}
			dbName := v.Schema.L
			if dbName == "" {
				dbName = b.ctx.GetSessionVars().CurrentDB
			}
			if sessionVars.User != nil {
				authErr = plannererrors.ErrTableaccessDenied.FastGenByArgs("DELETE", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, v.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, dbName, v.Name.L, "", authErr)
		}
	}
	handleColsMap := b.handleHelper.tailMap()
	tblID2Handle, err := resolveIndicesForTblID2Handle(handleColsMap, p.Schema())
	if err != nil {
		return nil, err
	}
	preProjNames := p.OutputNames()[:oldLen]
	if del.IsMultiTable {
		// tblID2TableName is the table map value is an array which contains table aliases.
		// Table ID may not be unique for deleting multiple tables, for statements like
		// `delete from t as t1, t as t2`, the same table has two alias, we have to identify a table
		// by its alias instead of ID.
		tblID2TableName := make(map[int64][]*resolve.TableNameW, len(ds.Tables.Tables))
		for _, tn := range ds.Tables.Tables {
			tnW := localResolveCtx.GetTableName(tn)
			tblID2TableName[tnW.TableInfo.ID] = append(tblID2TableName[tnW.TableInfo.ID], tnW)
		}
		tblID2Handle = del.CleanTblID2HandleMap(tblID2TableName, tblID2Handle, preProjNames)
	}
	tblID2table := make(map[int64]table.Table, len(tblID2Handle))
	for id := range tblID2Handle {
		tblID2table[id], _ = b.is.TableByID(ctx, id)
	}

	err = del.BuildOnDeleteFKTriggers(b.ctx, b.is, tblID2table)
	if err != nil {
		return nil, err
	}

	var nonPruned *bitset.BitSet
	del.TblColPosInfos, nonPruned, err = pruneAndBuildColPositionInfoForDelete(preProjNames, tblID2Handle, tblID2table, len(del.FKCascades) > 0 || len(del.FKChecks) > 0)
	if err != nil {
		return nil, err
	}
	var (
		finalProjCols  []*expression.Column
		finalProjNames types.NameSlice
	)
	if nonPruned != nil {
		// If the pruning happens, we project the columns not pruned as the final output of the below plan.
		finalProjCols = make([]*expression.Column, 0, oldLen/2)
		finalProjNames = make(types.NameSlice, 0, oldLen/2)
		for i, found := nonPruned.NextSet(0); found; i, found = nonPruned.NextSet(i + 1) {
			finalProjCols = append(finalProjCols, p.Schema().Columns[i])
			finalProjNames = append(finalProjNames, p.OutputNames()[i])
		}
	} else {
		// Otherwise, we just use the original schema.
		finalProjCols = make([]*expression.Column, oldLen)
		copy(finalProjCols, p.Schema().Columns[:oldLen])
		finalProjNames = preProjNames.Shallow()
	}
	proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(finalProjCols)}.Init(b.ctx, b.getSelectOffset())
	proj.SetChildren(p)
	proj.SetSchema(expression.NewSchema(finalProjCols...))
	proj.SetOutputNames(finalProjNames)
	p = proj
	del.SetOutputNames(p.OutputNames())
	del.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag, p)

	return del, err
}

func resolveIndicesForTblID2Handle(tblID2Handle map[int64][]util.HandleCols, schema *expression.Schema) (map[int64][]util.HandleCols, error) {
	newMap := make(map[int64][]util.HandleCols, len(tblID2Handle))
	for i, cols := range tblID2Handle {
		for _, col := range cols {
			resolvedCol, err := col.ResolveIndices(schema)
			if err != nil {
				return nil, err
			}
			newMap[i] = append(newMap[i], resolvedCol)
		}
	}
	return newMap, nil
}


type updatableTableListResolver struct {
	updatableTableList []*ast.TableName
	resolveCtx         *resolve.Context
}

func (*updatableTableListResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.UpdateStmt, *ast.TableRefsClause, *ast.Join, *ast.TableSource, *ast.TableName:
		return v, false
	}
	return inNode, true
}

func (u *updatableTableListResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	if v, ok := inNode.(*ast.TableSource); ok {
		if s, ok := v.Source.(*ast.TableName); ok {
			if v.AsName.L != "" {
				newTableName := *s
				newTableName.Name = v.AsName
				newTableName.Schema = ast.NewCIStr("")
				u.updatableTableList = append(u.updatableTableList, &newTableName)
				if tnW := u.resolveCtx.GetTableName(s); tnW != nil {
					u.resolveCtx.AddTableName(&resolve.TableNameW{
						TableName: &newTableName,
						DBInfo:    tnW.DBInfo,
						TableInfo: tnW.TableInfo,
					})
				}
			} else {
				u.updatableTableList = append(u.updatableTableList, s)
			}
		}
	}
	return inNode, true
}
