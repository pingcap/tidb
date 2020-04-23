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

package core

import (
	"bytes"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
)

// PointGetPlan is a fast plan for simple point get.
// When we detect that the statement has a unique equal access condition, this plan is used.
// This plan is much faster to build and to execute because it avoid the optimization and coprocessor cost.
type PointGetPlan struct {
	basePlan
	dbName           string
	schema           *expression.Schema
	TblInfo          *model.TableInfo
	IndexInfo        *model.IndexInfo
	Handle           int64
	HandleParam      *driver.ParamMarkerExpr
	UnsignedHandle   bool
	IndexValues      []types.Datum
	IndexValueParams []*driver.ParamMarkerExpr
	expr             expression.Expression
	ctx              sessionctx.Context
	IsTableDual      bool
	Lock             bool
	IsForUpdate      bool
	LockWaitTime     int64
}

type nameValuePair struct {
	colName string
	value   types.Datum
	param   *driver.ParamMarkerExpr
}

// Schema implements the Plan interface.
func (p *PointGetPlan) Schema() *expression.Schema {
	return p.schema
}

// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (p *PointGetPlan) attach2Task(...task) task {
	return nil
}

// ToPB converts physical plan to tipb executor.
func (p *PointGetPlan) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	return nil, nil
}

// ExplainInfo returns operator information to be explained.
func (p *PointGetPlan) ExplainInfo() string {
	return p.explainInfo(false)
}

// ExplainInfo returns operator information to be explained.
func (p *PointGetPlan) explainInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	tblName := p.TblInfo.Name.O
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.IndexInfo != nil {
		fmt.Fprintf(buffer, ", index:")
		for i, col := range p.IndexInfo.Columns {
			buffer.WriteString(col.Name.O)
			if i < len(p.IndexInfo.Columns)-1 {
				buffer.WriteString(" ")
			}
		}
	} else {
		if normalized {
			fmt.Fprintf(buffer, ", handle:?")
		} else {
			if p.UnsignedHandle {
				fmt.Fprintf(buffer, ", handle:%d", uint64(p.Handle))
			} else {
				fmt.Fprintf(buffer, ", handle:%d", p.Handle)
			}
		}
	}
	if p.Lock {
		fmt.Fprintf(buffer, ", lock")
	}
	return buffer.String()
}

// ExplainNormalizedInfo returns normalized operator information to be explained.
func (p *PointGetPlan) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// GetChildReqProps gets the required property by child index.
func (p *PointGetPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return nil
}

// StatsCount will return the the RowCount of property.StatsInfo for this plan.
func (p *PointGetPlan) StatsCount() float64 {
	return 1
}

// statsInfo will return the the RowCount of property.StatsInfo for this plan.
func (p *PointGetPlan) statsInfo() *property.StatsInfo {
	if p.stats == nil {
		p.stats = &property.StatsInfo{}
	}
	p.stats.RowCount = 1
	return p.stats
}

// Children gets all the children.
func (p *PointGetPlan) Children() []PhysicalPlan {
	return nil
}

// SetChildren sets the children for the plan.
func (p *PointGetPlan) SetChildren(...PhysicalPlan) {}

// SetChild sets a specific child for the plan.
func (p *PointGetPlan) SetChild(i int, child PhysicalPlan) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *PointGetPlan) ResolveIndices() error {
	return nil
}

// TryFastPlan tries to use the PointGetPlan for the query.
func TryFastPlan(ctx sessionctx.Context, node ast.Node) Plan {
	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	switch x := node.(type) {
	case *ast.SelectStmt:
		fp := tryPointGetPlan(ctx, x)
		if fp != nil {
			if checkFastPlanPrivilege(ctx, fp, mysql.SelectPriv) != nil {
				return nil
			}
			if fp.IsTableDual {
				tableDual := PhysicalTableDual{}
				tableDual.SetSchema(fp.Schema())
				return tableDual.Init(ctx, &property.StatsInfo{})
			}
			if x.LockTp == ast.SelectLockForUpdate {
				// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
				// is disabled (either by beginning transaction with START TRANSACTION or by setting
				// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
				// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
				sessVars := ctx.GetSessionVars()
				if !sessVars.IsAutocommit() || sessVars.InTxn() {
					fp.Lock = true
					fp.IsForUpdate = true
					fp.LockWaitTime = sessVars.LockWaitTimeout
				}
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

// tryPointGetPlan determine if the SelectStmt can use a PointGetPlan.
// Returns nil if not applicable.
// To use the PointGetPlan the following rules must be satisfied:
// 1. For the limit clause, the count should at least 1 and the offset is 0.
// 2. It must be a single table select.
// 3. All the columns must be public and generated.
// 4. The condition is an access path that the range is a unique key.
func tryPointGetPlan(ctx sessionctx.Context, selStmt *ast.SelectStmt) *PointGetPlan {
	if selStmt.Having != nil {
		return nil
	} else if selStmt.Limit != nil {
		count, offset, err := extractLimitCountOffset(ctx, selStmt.Limit)
		if err != nil || count == 0 || offset > 0 {
			return nil
		}
	}
	tblName, tblAlias := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.TableInfo
	if tbl == nil {
		return nil
	}
<<<<<<< HEAD
	// Do not handle partitioned table.
	// Table partition implementation translates LogicalPlan from `DataSource` to
	// `Union -> DataSource` in the logical plan optimization pass, since PointGetPlan
	// bypass the logical plan optimization, it can't support partitioned table.
	if tbl.GetPartitionInfo() != nil {
=======
	pi := tbl.GetPartitionInfo()
	if pi != nil && pi.Type != model.PartitionTypeHash {
>>>>>>> e521ea9... *: fix the problem that PointGet returns wrong results in the case of overflow (#14776)
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
	schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}
	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetSessionVars().CurrentDB
	}

	pairs := make([]nameValuePair, 0, 4)
	pairs, isTableDual := getNameValuePairs(ctx.GetSessionVars().StmtCtx, tbl, tblAlias, pairs, selStmt.Where)
	if pairs == nil && !isTableDual {
		return nil
	}
<<<<<<< HEAD
	handlePair, fieldType := findPKHandle(tbl, pairs)
	if handlePair.value.Kind() != types.KindNull && len(pairs) == 1 {
		schema := buildSchemaFromFields(ctx, tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
		if schema == nil {
			return nil
		}
		dbName := tblName.Schema.L
		if dbName == "" {
			dbName = ctx.GetSessionVars().CurrentDB
		}
		p := newPointGetPlan(ctx, dbName, schema, tbl)
		intDatum, err := handlePair.value.ConvertTo(ctx.GetSessionVars().StmtCtx, fieldType)
		if err != nil {
			if terror.ErrorEqual(types.ErrOverflow, err) {
				p.IsTableDual = true
				return p
			}
			// some scenarios cast to int with error, but we may use this value in point get
			if !terror.ErrorEqual(types.ErrTruncatedWrongVal, err) {
				return nil
			}
		}
		cmp, err := intDatum.CompareDatum(ctx.GetSessionVars().StmtCtx, &handlePair.value)
		if err != nil {
			return nil
		} else if cmp != 0 {
=======

	var partitionInfo *model.PartitionDefinition
	var pos int
	if pi != nil {
		partitionInfo, pos = getPartitionInfo(ctx, tbl, pairs)
		if partitionInfo == nil {
			return nil
		}
	}

	handlePair, fieldType := findPKHandle(tbl, pairs)
	if handlePair.value.Kind() != types.KindNull && len(pairs) == 1 {
		if isTableDual {
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
>>>>>>> e521ea9... *: fix the problem that PointGet returns wrong results in the case of overflow (#14776)
			p.IsTableDual = true
			return p
		}

		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.Handle = handlePair.value.GetInt64()
		p.UnsignedHandle = mysql.HasUnsignedFlag(fieldType.Flag)
		p.HandleParam = handlePair.param
		return p
	}

	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique {
			continue
		}
		if idxInfo.State != model.StatePublic {
			continue
		}
		if isTableDual {
			p := newPointGetPlan(ctx, tblName.Schema.O, schema, tbl, names)
			p.IsTableDual = true
			return p
		}

		idxValues, idxValueParams := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			continue
		}
<<<<<<< HEAD
		schema := buildSchemaFromFields(ctx, tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
		if schema == nil {
			return nil
		}
		dbName := tblName.Schema.L
		if dbName == "" {
			dbName = ctx.GetSessionVars().CurrentDB
		}
		p := newPointGetPlan(ctx, dbName, schema, tbl)
=======
		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
>>>>>>> e521ea9... *: fix the problem that PointGet returns wrong results in the case of overflow (#14776)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		p.IndexValueParams = idxValueParams
		return p
	}
	return nil
}

func newPointGetPlan(ctx sessionctx.Context, dbName string, schema *expression.Schema, tbl *model.TableInfo) *PointGetPlan {
	p := &PointGetPlan{
		basePlan:     newBasePlan(ctx, plancodec.TypePointGet),
		dbName:       dbName,
		schema:       schema,
		TblInfo:      tbl,
		LockWaitTime: ctx.GetSessionVars().LockWaitTimeout,
	}
	ctx.GetSessionVars().StmtCtx.Tables = []stmtctx.TableEntry{{DB: dbName, Table: tbl.Name.L}}
	return p
}

func checkFastPlanPrivilege(ctx sessionctx.Context, fastPlan *PointGetPlan, checkTypes ...mysql.PrivilegeType) error {
	pm := privilege.GetPrivilegeManager(ctx)
	if pm == nil {
		return nil
	}
	for _, checkType := range checkTypes {
		if !pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, fastPlan.dbName, fastPlan.TblInfo.Name.L, "", checkType) {
			return errors.New("privilege check fail")
		}
	}
	return nil
}

func buildSchemaFromFields(ctx sessionctx.Context, dbName model.CIStr, tbl *model.TableInfo, tblName model.CIStr, fields []*ast.SelectField) *expression.Schema {
	if dbName.L == "" {
		dbName = model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	}
	columns := make([]*expression.Column, 0, len(tbl.Columns)+1)
	if len(fields) > 0 {
		for _, field := range fields {
			if field.WildCard != nil {
				if field.WildCard.Table.L != "" && field.WildCard.Table.L != tblName.L {
					return nil
				}
				for _, col := range tbl.Columns {
					columns = append(columns, colInfoToColumn(dbName, tbl.Name, tblName, col.Name, col, len(columns)))
				}
				continue
			}
			colNameExpr, ok := field.Expr.(*ast.ColumnNameExpr)
			if !ok {
				return nil
			}
			if colNameExpr.Name.Table.L != "" && colNameExpr.Name.Table.L != tblName.L {
				return nil
			}
			col := findCol(tbl, colNameExpr.Name)
			if col == nil {
				return nil
			}
			asName := col.Name
			if field.AsName.L != "" {
				asName = field.AsName
			}
			columns = append(columns, colInfoToColumn(dbName, tbl.Name, tblName, asName, col, len(columns)))
		}
		return expression.NewSchema(columns...)
	}
	// fields len is 0 for update and delete.
	var handleCol *expression.Column
	for _, col := range tbl.Columns {
		column := colInfoToColumn(dbName, tbl.Name, tblName, col.Name, col, len(columns))
		if tbl.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			handleCol = column
		}
		columns = append(columns, column)
	}
	if handleCol == nil {
		handleCol = colInfoToColumn(dbName, tbl.Name, tblName, model.ExtraHandleName, model.NewExtraHandleColInfo(), len(columns))
		columns = append(columns, handleCol)
	}
	schema := expression.NewSchema(columns...)
	schema.TblID2Handle = make(map[int64][]*expression.Column)
	schema.TblID2Handle[tbl.ID] = []*expression.Column{handleCol}
	return schema
}

// getSingleTableNameAndAlias return the ast node of queried table name and the alias string.
// `tblName` is `nil` if there are multiple tables in the query.
// `tblAlias` will be the real table name if there is no table alias in the query.
func getSingleTableNameAndAlias(tableRefs *ast.TableRefsClause) (tblName *ast.TableName, tblAlias model.CIStr) {
	if tableRefs == nil || tableRefs.TableRefs == nil || tableRefs.TableRefs.Right != nil {
		return nil, tblAlias
	}
	tblSrc, ok := tableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, tblAlias
	}
	tblName, ok = tblSrc.Source.(*ast.TableName)
	if !ok {
		return nil, tblAlias
	}
	tblAlias = tblSrc.AsName
	if tblSrc.AsName.L == "" {
		tblAlias = tblName.Name
	}
	return tblName, tblAlias
}

// getNameValuePairs extracts `column = constant/paramMarker` conditions from expr as name value pairs.
func getNameValuePairs(stmtCtx *stmtctx.StatementContext, tbl *model.TableInfo, tblName model.CIStr, nvPairs []nameValuePair, expr ast.ExprNode) (
	pairs []nameValuePair, isTableDual bool) {
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, false
	}
	if binOp.Op == opcode.LogicAnd {
		nvPairs, isTableDual = getNameValuePairs(stmtCtx, tbl, tblName, nvPairs, binOp.L)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		nvPairs, isTableDual = getNameValuePairs(stmtCtx, tbl, tblName, nvPairs, binOp.R)
		if nvPairs == nil || isTableDual {
			return nil, isTableDual
		}
		return nvPairs, isTableDual
	} else if binOp.Op == opcode.EQ {
		var d types.Datum
		var colName *ast.ColumnNameExpr
		var param *driver.ParamMarkerExpr
		var ok bool
		if colName, ok = binOp.L.(*ast.ColumnNameExpr); ok {
			switch x := binOp.R.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				d = x.Datum
				param = x
			}
		} else if colName, ok = binOp.R.(*ast.ColumnNameExpr); ok {
			switch x := binOp.L.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				d = x.Datum
				param = x
			}
		} else {
			return nil, false
		}
		if d.IsNull() {
			return nil, false
		}
		// Views' columns have no FieldType.
		if tbl.IsView() {
			return nil, false
		}
		if colName.Name.Table.L != "" && colName.Name.Table.L != tblName.L {
			return nil, false
		}
		col := model.FindColumnInfo(tbl.Cols(), colName.Name.Name.L)
		if col == nil || // Handling the case when the column is _tidb_rowid.
			(col.Tp == mysql.TypeString && col.Collate == charset.CollationBin) { // This type we needn't to pad `\0` in here.
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d, param: param}), false
		}
		dVal, err := d.ConvertTo(stmtCtx, &col.FieldType)
		if err != nil {
			if terror.ErrorEqual(types.ErrOverflow, err) {
				return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d, param: param}), true
			}
			// Some scenarios cast to int with error, but we may use this value in point get.
			if !terror.ErrorEqual(types.ErrTruncatedWrongVal, err) {
				return nil, false
			}
		}
		// The converted result must be same as original datum.
		cmp, err := d.CompareDatum(stmtCtx, &dVal)
		if err != nil {
			return nil, false
		} else if cmp != 0 {
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: dVal, param: param}), true
		}

		return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: dVal, param: param}), false
	}
	return nil, false
}

func findPKHandle(tblInfo *model.TableInfo, pairs []nameValuePair) (handlePair nameValuePair, fieldType *types.FieldType) {
	if !tblInfo.PKIsHandle {
		rowIDIdx := findInPairs("_tidb_rowid", pairs)
		if rowIDIdx != -1 {
			return pairs[rowIDIdx], types.NewFieldType(mysql.TypeLonglong)
		}
		return handlePair, nil
	}
	for _, col := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			i := findInPairs(col.Name.L, pairs)
			if i == -1 {
				return handlePair, nil
			}
			return pairs[i], &col.FieldType
		}
	}
	return handlePair, nil
}

func getIndexValues(idxInfo *model.IndexInfo, pairs []nameValuePair) ([]types.Datum, []*driver.ParamMarkerExpr) {
	idxValues := make([]types.Datum, 0, 4)
	idxValueParams := make([]*driver.ParamMarkerExpr, 0, 4)
	if len(idxInfo.Columns) != len(pairs) {
		return nil, nil
	}
	if idxInfo.HasPrefixIndex() {
		return nil, nil
	}
	for _, idxCol := range idxInfo.Columns {
		i := findInPairs(idxCol.Name.L, pairs)
		if i == -1 {
			return nil, nil
		}
		idxValues = append(idxValues, pairs[i].value)
		idxValueParams = append(idxValueParams, pairs[i].param)
	}
	if len(idxValues) > 0 {
		return idxValues, idxValueParams
	}
	return nil, nil
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
	fastSelect := tryPointGetPlan(ctx, selStmt)
	if fastSelect == nil {
		return nil
	}
	if checkFastPlanPrivilege(ctx, fastSelect, mysql.SelectPriv, mysql.UpdatePriv) != nil {
		return nil
	}
	if fastSelect.IsTableDual {
		return PhysicalTableDual{}.Init(ctx, &property.StatsInfo{})
	}
	if ctx.GetSessionVars().TxnCtx.IsPessimistic {
		fastSelect.Lock = true
	}
	orderedList := buildOrderedList(ctx, fastSelect, updateStmt.List)
	if orderedList == nil {
		return nil
	}
	updatePlan := Update{
		SelectPlan:  fastSelect,
		OrderedList: orderedList,
	}.Init(ctx)
	updatePlan.SetSchema(fastSelect.schema)
	return updatePlan
}

func buildOrderedList(ctx sessionctx.Context, fastSelect *PointGetPlan, list []*ast.Assignment) []*expression.Assignment {
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
		expr, err := expression.RewriteSimpleExprWithSchema(ctx, assign.Expr, fastSelect.schema)
		if err != nil {
			return nil
		}
		expr = expression.BuildCastFunction(ctx, expr, col.GetType())
		newAssign.Expr, err = expr.ResolveIndices(fastSelect.schema)
		if err != nil {
			return nil
		}
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
	fastSelect := tryPointGetPlan(ctx, selStmt)
	if fastSelect == nil {
		return nil
	}
	if checkFastPlanPrivilege(ctx, fastSelect, mysql.SelectPriv, mysql.DeletePriv) != nil {
		return nil
	}
	if fastSelect.IsTableDual {
		return PhysicalTableDual{}.Init(ctx, &property.StatsInfo{})
	}
	if ctx.GetSessionVars().TxnCtx.IsPessimistic {
		fastSelect.Lock = true
	}
	delPlan := Delete{
		SelectPlan: fastSelect,
	}.Init(ctx)
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

func colInfoToColumn(db model.CIStr, origTblName model.CIStr, tblName model.CIStr, asName model.CIStr, col *model.ColumnInfo, idx int) *expression.Column {
	return &expression.Column{
		ColName:     asName,
		OrigTblName: origTblName,
		DBName:      db,
		TblName:     tblName,
		RetType:     &col.FieldType,
		ID:          col.ID,
		UniqueID:    int64(col.Offset),
		Index:       idx,
	}
}
