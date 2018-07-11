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
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

// PointSelectPlan is a fast plan for point select.
type PointSelectPlan struct {
	basePlan
	schema      *expression.Schema
	TblInfo     *model.TableInfo
	IndexInfo   *model.IndexInfo
	Handle      int64
	IndexValues []types.Datum
	ctx         sessionctx.Context
	stats       *statsInfo
}

type nameValuePair struct {
	colName string
	value   types.Datum
}

// Schema implements the Plan interface.
func (fp *PointSelectPlan) Schema() *expression.Schema {
	return fp.schema
}

// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (fp *PointSelectPlan) attach2Task(...task) task {
	return nil
}

// ToPB converts physical plan to tipb executor.
func (fp *PointSelectPlan) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	return nil, nil
}

// ExplainInfo returns operator information to be explained.
func (fp *PointSelectPlan) ExplainInfo() string {
	return "Point_Select"
}

// getChildReqProps gets the required property by child index.
func (fp *PointSelectPlan) getChildReqProps(idx int) *requiredProp {
	return nil
}

// StatsInfo will return the statsInfo for this plan.
func (fp *PointSelectPlan) StatsInfo() *statsInfo {
	return fp.stats
}

// Children gets all the children.
func (fp *PointSelectPlan) Children() []PhysicalPlan {
	return nil
}

// SetChildren sets the children for the plan.
func (fp *PointSelectPlan) SetChildren(...PhysicalPlan) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (fp *PointSelectPlan) ResolveIndices() {}

func tryFastPlan(ctx sessionctx.Context, node ast.Node) Plan {
	if PreparedPlanCacheEnabled || ctx.GetSessionVars().PlanCacheEnabled {
		// plan cache not supported yet.
		return nil
	}
	switch x := node.(type) {
	case *ast.SelectStmt:
		fp := tryFastSelectPlan(ctx, x)
		if fp != nil {
			if checkFastPlanPrivilege(ctx, fp, mysql.SelectPriv) != nil {
				return nil
			}
			return fp
		}
	case *ast.UpdateStmt:
		return tryUpdatePointPlan(ctx, x)
	case *ast.DeleteStmt:
		return tryDeletePointPlan(ctx, x)
	}
	return nil
}

func tryFastSelectPlan(ctx sessionctx.Context, selStmt *ast.SelectStmt) *PointSelectPlan {
	if selStmt.GroupBy != nil || selStmt.Having != nil || selStmt.OrderBy != nil || selStmt.Limit != nil ||
		selStmt.LockTp != ast.SelectLockNone {
		return nil
	}
	tblName := getSingleTableName(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.TableInfo
	if tbl == nil {
		return nil
	}
	for _, col := range tbl.Columns {
		// Do not handle generated columns.
		if col.IsGenerated() {
			return nil
		}
		// Only handle tables that all columns are public.
		if col.State != model.StatePublic {
			return nil
		}
	}
	pairs := make([]nameValuePair, 0, 4)
	pairs = getNameValuePairs(pairs, selStmt.Where)
	if pairs == nil {
		return nil
	}
	handleDatum := findPKHandle(tbl, pairs)
	if handleDatum.Kind() == types.KindInt64 {
		schema := buildSchemaFromFields(ctx, tbl, selStmt.Fields.Fields)
		if schema == nil {
			return nil
		}
		p := newPointSelectPlan(ctx, schema, tbl)
		p.Handle = handleDatum.GetInt64()
		return p
	}
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique {
			continue
		}
		if idxInfo.State != model.StatePublic {
			return nil
		}
		idxValues := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			continue
		}
		schema := buildSchemaFromFields(ctx, tbl, selStmt.Fields.Fields)
		if schema == nil {
			return nil
		}
		p := newPointSelectPlan(ctx, schema, tbl)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		return p
	}
	return nil
}

func newPointSelectPlan(ctx sessionctx.Context, schema *expression.Schema, tbl *model.TableInfo) *PointSelectPlan {
	p := &PointSelectPlan{
		basePlan: newBasePlan(ctx, "Point_Select"),
		schema:   schema,
		TblInfo:  tbl,
	}
	p.stats = &statsInfo{count: 1}
	return p
}

func checkFastPlanPrivilege(ctx sessionctx.Context, fastPlan *PointSelectPlan, checkTypes ...mysql.PrivilegeType) error {
	pm := privilege.GetPrivilegeManager(ctx)
	if pm == nil {
		return nil
	}
	dbName := ctx.GetSessionVars().CurrentDB
	for _, checkType := range checkTypes {
		if !pm.RequestVerification(dbName, fastPlan.TblInfo.Name.L, "", checkType) {
			return errors.New("privilege check fail")
		}
	}
	return nil
}

func buildSchemaFromFields(ctx sessionctx.Context, tbl *model.TableInfo, fields []*ast.SelectField) *expression.Schema {
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns := make([]*expression.Column, 0, len(tbl.Columns)+1)
	if len(fields) == 1 && fields[0].WildCard != nil {
		for _, col := range tbl.Columns {
			columns = append(columns, colInfoToColumn(dbName, tbl.Name, col, len(columns)))
		}
		return expression.NewSchema(columns...)
	}
	if len(fields) > 0 {
		for _, field := range fields {
			colNameExpr, ok := field.Expr.(*ast.ColumnNameExpr)
			if !ok {
				return nil
			}
			col := findCol(tbl, colNameExpr.Name)
			if col == nil {
				return nil
			}
			columns = append(columns, colInfoToColumn(dbName, tbl.Name, col, len(columns)))
		}
		return expression.NewSchema(columns...)
	}
	// fields len is 0 for update and delete.
	var handleCol *expression.Column
	for _, col := range tbl.Columns {
		column := colInfoToColumn(dbName, tbl.Name, col, len(columns))
		if tbl.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			handleCol = column
		}
		columns = append(columns, column)
	}
	if handleCol == nil {
		handleCol = colInfoToColumn(dbName, tbl.Name, model.NewExtraHandleColInfo(), len(columns))
		columns = append(columns, handleCol)
	}
	schema := expression.NewSchema(columns...)
	schema.TblID2Handle = make(map[int64][]*expression.Column)
	schema.TblID2Handle[tbl.ID] = []*expression.Column{handleCol}
	return schema
}

func getSingleTableName(tableRefs *ast.TableRefsClause) *ast.TableName {
	if tableRefs == nil || tableRefs.TableRefs == nil || tableRefs.TableRefs.Right != nil {
		return nil
	}
	tblSrc, ok := tableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil
	}
	if tblSrc.AsName.L != "" {
		return nil
	}
	tblName, ok := tblSrc.Source.(*ast.TableName)
	if !ok {
		return nil
	}
	return tblName
}

func getNameValuePairs(nvPairs []nameValuePair, expr ast.ExprNode) []nameValuePair {
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		return nil
	}
	if binOp.Op == opcode.LogicAnd {
		nvPairs = getNameValuePairs(nvPairs, binOp.L)
		if nvPairs == nil {
			return nil
		}
		nvPairs = getNameValuePairs(nvPairs, binOp.R)
		if nvPairs == nil {
			return nil
		}
		return nvPairs
	} else if binOp.Op == opcode.EQ {
		colName, ok := binOp.L.(*ast.ColumnNameExpr)
		if !ok {
			return nil
		}
		var d types.Datum
		switch x := binOp.R.(type) {
		case *ast.ValueExpr:
			d = x.Datum
		case *ast.ParamMarkerExpr:
			d = x.Datum
		}
		if d.IsNull() {
			return nil
		}
		return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d})
	}
	return nil
}

func findPKHandle(tblInfo *model.TableInfo, pairs []nameValuePair) (d types.Datum) {
	if !tblInfo.PKIsHandle {
		return d
	}
	for _, col := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			i := findInPairs(col.Name.L, pairs)
			if i == -1 {
				return d
			}
			return pairs[i].value
		}
	}
	return d
}

func getIndexValues(idxInfo *model.IndexInfo, pairs []nameValuePair) []types.Datum {
	idxValues := make([]types.Datum, 0, 4)
	if len(idxInfo.Columns) != len(pairs) {
		return nil
	}
	if idxInfo.HasPrefixIndex() {
		return nil
	}
	for _, idxCol := range idxInfo.Columns {
		i := findInPairs(idxCol.Name.L, pairs)
		if i == -1 {
			return nil
		}
		idxValues = append(idxValues, pairs[i].value)
	}
	if len(idxValues) > 0 {
		return idxValues
	}
	return nil
}

func findInPairs(colName string, pairs []nameValuePair) int {
	for i, pair := range pairs {
		if pair.colName == colName {
			return i
		}
	}
	return -1
}

func tryUpdatePointPlan(ctx sessionctx.Context, updateStmt *ast.UpdateStmt) Plan {
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    updateStmt.TableRefs,
		Where:   updateStmt.Where,
		OrderBy: updateStmt.Order,
		Limit:   updateStmt.Limit,
	}
	fastSelect := tryFastSelectPlan(ctx, selStmt)
	if fastSelect == nil {
		return nil
	}
	orderedList := buildOrderedList(ctx, fastSelect, updateStmt.List)
	if orderedList == nil {
		return nil
	}
	udpatePlan := &Update{
		SelectPlan:  fastSelect,
		OrderedList: orderedList,
	}
	udpatePlan.SetSchema(fastSelect.schema)
	return udpatePlan
}

func buildOrderedList(ctx sessionctx.Context, fastSelect *PointSelectPlan, list []*ast.Assignment) []*expression.Assignment {
	orderedList := make([]*expression.Assignment, 0, len(list))
	for _, assign := range list {
		col, err := fastSelect.schema.FindColumn(assign.Column)
		if err != nil {
			return nil
		}
		if col == nil {
			return nil
		}
		newAssign := &expression.Assignment{
			Col: col,
		}
		expr, err := expression.RewriteSimpleExpr(ctx, fastSelect.TblInfo, assign.Expr)
		if err != nil {
			return nil
		}
		expr = expression.BuildCastFunction(ctx, expr, col.GetType())
		newAssign.Expr = expr.ResolveIndices(fastSelect.schema)
		orderedList = append(orderedList, newAssign)
	}
	return orderedList
}

func tryDeletePointPlan(ctx sessionctx.Context, delStmt *ast.DeleteStmt) Plan {
	if delStmt.IsMultiTable {
		return nil
	}
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    delStmt.TableRefs,
		Where:   delStmt.Where,
		OrderBy: delStmt.Order,
		Limit:   delStmt.Limit,
	}
	fastSelect := tryFastSelectPlan(ctx, selStmt)
	if fastSelect == nil {
		return nil
	}
	if checkFastPlanPrivilege(ctx, fastSelect, mysql.SelectPriv, mysql.DeletePriv) != nil {
		return nil
	}
	delPlan := &Delete{
		SelectPlan: fastSelect,
	}
	delPlan.SetSchema(fastSelect.schema)
	return delPlan
}

func findCol(tbl *model.TableInfo, colName *ast.ColumnName) *model.ColumnInfo {
	for _, col := range tbl.Columns {
		if col.Name.L == colName.Name.L {
			return col
		}
	}
	return nil
}

func colInfoToColumn(db model.CIStr, tblName model.CIStr, col *model.ColumnInfo, idx int) *expression.Column {
	return &expression.Column{
		FromID:      1,
		ColName:     col.Name,
		OrigTblName: tblName,
		DBName:      db,
		TblName:     tblName,
		RetType:     &col.FieldType,
		ID:          col.ID,
		Position:    col.Offset,
		Index:       idx,
	}
}
