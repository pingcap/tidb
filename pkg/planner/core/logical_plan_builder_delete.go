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

	"github.com/bits-and-blooms/bitset"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

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
