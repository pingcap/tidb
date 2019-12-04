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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
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
	IndexValues      []types.Datum
	IndexValueParams []*driver.ParamMarkerExpr
	expr             expression.Expression
	ctx              sessionctx.Context
	UnsignedHandle   bool
	IsTableDual      bool
	Lock             bool
	IsForUpdate      bool
	outputNames      []*types.FieldName
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

// OutputNames returns the outputting names of each column.
func (p *PointGetPlan) OutputNames() types.NameSlice {
	return p.outputNames
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PointGetPlan) SetOutputNames(names types.NameSlice) {
	p.outputNames = names
}

// BatchPointGetPlan represents a physical plan which contains a bunch of
// keys reference the same table and use the same `unique key`
type BatchPointGetPlan struct {
	baseSchemaProducer

	TblInfo          *model.TableInfo
	IndexInfo        *model.IndexInfo
	Handles          []int64
	HandleParams     []*driver.ParamMarkerExpr
	IndexValues      [][]types.Datum
	IndexValueParams [][]*driver.ParamMarkerExpr
}

// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (p *BatchPointGetPlan) attach2Task(...task) task {
	return nil
}

// ToPB converts physical plan to tipb executor.
func (p *BatchPointGetPlan) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	return nil, nil
}

// ExplainInfo returns operator information to be explained.
func (p *BatchPointGetPlan) ExplainInfo() string {
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
	}
	return buffer.String()
}

// ExplainNormalizedInfo returns normalized operator information to be explained.
func (p *BatchPointGetPlan) ExplainNormalizedInfo() string {
	return p.ExplainInfo()
}

// GetChildReqProps gets the required property by child index.
func (p *BatchPointGetPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return nil
}

// StatsCount will return the the RowCount of property.StatsInfo for this plan.
func (p *BatchPointGetPlan) StatsCount() float64 {
	return p.statsInfo().RowCount
}

// statsInfo will return the the RowCount of property.StatsInfo for this plan.
func (p *BatchPointGetPlan) statsInfo() *property.StatsInfo {
	return p.stats
}

// Children gets all the children.
func (p *BatchPointGetPlan) Children() []PhysicalPlan {
	return nil
}

// SetChildren sets the children for the plan.
func (p *BatchPointGetPlan) SetChildren(...PhysicalPlan) {}

// SetChild sets a specific child for the plan.
func (p *BatchPointGetPlan) SetChild(i int, child PhysicalPlan) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *BatchPointGetPlan) ResolveIndices() error {
	return nil
}

// OutputNames returns the outputting names of each column.
func (p *BatchPointGetPlan) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *BatchPointGetPlan) SetOutputNames(names types.NameSlice) {
	p.names = names
}

// TryFastPlan tries to use the PointGetPlan for the query.
func TryFastPlan(ctx sessionctx.Context, node ast.Node) Plan {
	switch x := node.(type) {
	case *ast.SelectStmt:
		// Try to convert the `SELECT a, b, c FROM t WHERE (a, b, c) in ((1, 2, 4), (1, 3, 5))` to
		// `PhysicalUnionAll` which children are `PointGet` if exists an unique key (a, b, c) in table `t`
		if fp := tryWhereIn2BatchPointGet(ctx, x); fp != nil {
			return fp
		}
		fp := tryPointGetPlan(ctx, x)
		if fp != nil {
			if checkFastPlanPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, mysql.SelectPriv) != nil {
				return nil
			}
			if fp.IsTableDual {
				tableDual := PhysicalTableDual{}
				tableDual.names = fp.outputNames
				tableDual.SetSchema(fp.Schema())
				return tableDual.Init(ctx, &property.StatsInfo{}, 0)
			}
			if x.LockTp == ast.SelectLockForUpdate || x.LockTp == ast.SelectLockForUpdateNoWait {
				// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
				// is disabled (either by beginning transaction with START TRANSACTION or by setting
				// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
				// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
				sessVars := ctx.GetSessionVars()
				if !sessVars.IsAutocommit() || sessVars.InTxn() {
					fp.Lock = true
					fp.IsForUpdate = true
					fp.LockWaitTime = sessVars.LockWaitTimeout
					if x.LockTp == ast.SelectLockForUpdateNoWait {
						fp.LockWaitTime = kv.LockNoWait
					}
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

func newBatchPointGetPlan(
	ctx sessionctx.Context, patternInExpr *ast.PatternInExpr,
	tryHandle bool, fieldType *types.FieldType,
	tbl *model.TableInfo, schema *expression.Schema,
	names []*types.FieldName, whereColNames []string,
) Plan {
	statsInfo := &property.StatsInfo{RowCount: float64(len(patternInExpr.List))}
	if tryHandle && fieldType != nil {
		var handles = make([]int64, len(patternInExpr.List))
		var handleParams = make([]*driver.ParamMarkerExpr, len(patternInExpr.List))
		for i, item := range patternInExpr.List {
			// SELECT * FROM t WHERE (key) in ((1), (2))
			if p, ok := item.(*ast.ParenthesesExpr); ok {
				item = p.Expr
			}
			var d types.Datum
			var param *driver.ParamMarkerExpr
			switch x := item.(type) {
			case *driver.ValueExpr:
				d = x.Datum
			case *driver.ParamMarkerExpr:
				d = x.Datum
				param = x
			default:
				return nil
			}
			if d.IsNull() {
				return nil
			}
			intDatum, err := d.ConvertTo(ctx.GetSessionVars().StmtCtx, fieldType)
			if err != nil {
				return nil
			}
			// The converted result must be same as original datum
			cmp, err := intDatum.CompareDatum(ctx.GetSessionVars().StmtCtx, &d)
			if err != nil || cmp != 0 {
				return nil
			}
			handles[i] = intDatum.GetInt64()
			handleParams[i] = param
		}
		return BatchPointGetPlan{
			TblInfo:      tbl,
			Handles:      handles,
			HandleParams: handleParams,
		}.Init(ctx, statsInfo, schema, names)
	}

	// The columns in where clause should be covered by unique index
	var matchIdxInfo *model.IndexInfo
	permutations := make([]int, len(whereColNames))
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != model.StatePublic {
			continue
		}
		if len(idxInfo.Columns) != len(whereColNames) || idxInfo.HasPrefixIndex() {
			continue
		}
		// TODO: not sure is there any function to reuse
		matched := true
		for whereColIndex, innerCol := range whereColNames {
			var found bool
			for i, col := range idxInfo.Columns {
				if innerCol == col.Name.L {
					permutations[whereColIndex] = i
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			matchIdxInfo = idxInfo
			break
		}
	}
	if matchIdxInfo == nil {
		return nil
	}
	indexValues := make([][]types.Datum, len(patternInExpr.List))
	indexValueParams := make([][]*driver.ParamMarkerExpr, len(patternInExpr.List))
	for i, item := range patternInExpr.List {
		// SELECT * FROM t WHERE (key) in ((1), (2))
		if p, ok := item.(*ast.ParenthesesExpr); ok {
			item = p.Expr
		}
		var values []types.Datum
		var valuesParams []*driver.ParamMarkerExpr
		switch x := item.(type) {
		case *ast.RowExpr:
			// The `len(values) == len(valuesParams)` should be satisfied in this mode
			values = make([]types.Datum, len(x.Values))
			valuesParams = make([]*driver.ParamMarkerExpr, len(x.Values))
			for index, inner := range x.Values {
				permIndex := permutations[index]
				switch innerX := inner.(type) {
				case *driver.ValueExpr:
					values[permIndex] = innerX.Datum
				case *driver.ParamMarkerExpr:
					values[permIndex] = innerX.Datum
					valuesParams[permIndex] = innerX
				default:
					return nil
				}
			}
		case *driver.ValueExpr:
			values = []types.Datum{x.Datum}
		case *driver.ParamMarkerExpr:
			values = []types.Datum{x.Datum}
			valuesParams = []*driver.ParamMarkerExpr{x}
		default:
			return nil
		}
		indexValues[i] = values
		indexValueParams[i] = valuesParams
	}
	return BatchPointGetPlan{
		TblInfo:          tbl,
		IndexInfo:        matchIdxInfo,
		IndexValues:      indexValues,
		IndexValueParams: indexValueParams,
	}.Init(ctx, statsInfo, schema, names)
}

func tryWhereIn2BatchPointGet(ctx sessionctx.Context, selStmt *ast.SelectStmt) Plan {
	if selStmt.OrderBy != nil || selStmt.GroupBy != nil ||
		selStmt.Limit != nil || selStmt.Having != nil ||
		len(selStmt.WindowSpecs) > 0 || selStmt.LockTp != ast.SelectLockNone {
		return nil
	}
	in, ok := selStmt.Where.(*ast.PatternInExpr)
	if !ok || in.Not || len(in.List) < 1 {
		return nil
	}

	tblName, tblAlias := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.TableInfo
	if tbl == nil {
		return nil
	}

	// Do not handle partitioned table.
	// Table partition implementation translates LogicalPlan from `DataSource` to
	// `Union -> DataSource` in the logical plan optimization pass, since BatchPointGetPlan
	// bypass the logical plan optimization, it can't support partitioned table.
	if tbl.GetPartitionInfo() != nil {
		return nil
	}

	for _, col := range tbl.Columns {
		if col.IsGenerated() || col.State != model.StatePublic {
			return nil
		}
	}

	schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}

	var (
		tryHandle     bool
		fieldType     *types.FieldType
		whereColNames []string
	)

	// SELECT * FROM t WHERE (key) in ((1), (2))
	colExpr := in.Expr
	if p, ok := colExpr.(*ast.ParenthesesExpr); ok {
		colExpr = p.Expr
	}
	switch colName := colExpr.(type) {
	case *ast.ColumnNameExpr:
		if name := colName.Name.Table.L; name != "" && name != tblAlias.L {
			return nil
		}
		// Try use handle
		if tbl.PKIsHandle {
			for _, col := range tbl.Columns {
				if mysql.HasPriKeyFlag(col.Flag) {
					tryHandle = col.Name.L == colName.Name.Name.L
					fieldType = &col.FieldType
					whereColNames = append(whereColNames, col.Name.L)
					break
				}
			}
		} else {
			// Downgrade to use unique index
			whereColNames = append(whereColNames, colName.Name.Name.L)
		}

	case *ast.RowExpr:
		for _, col := range colName.Values {
			c, ok := col.(*ast.ColumnNameExpr)
			if !ok {
				return nil
			}
			if name := c.Name.Table.L; name != "" && name != tblAlias.L {
				return nil
			}
			whereColNames = append(whereColNames, c.Name.Name.L)
		}
	default:
		return nil
	}

	p := newBatchPointGetPlan(ctx, in, tryHandle, fieldType, tbl, schema, names, whereColNames)
	if p == nil {
		return nil
	}

	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetSessionVars().CurrentDB
	}
	if checkFastPlanPrivilege(ctx, dbName, tbl.Name.L, mysql.SelectPriv) != nil {
		return nil
	}
	return p
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
	// Do not handle partitioned table.
	// Table partition implementation translates LogicalPlan from `DataSource` to
	// `Union -> DataSource` in the logical plan optimization pass, since PointGetPlan
	// bypass the logical plan optimization, it can't support partitioned table.
	if tbl.GetPartitionInfo() != nil {
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
	pairs = getNameValuePairs(pairs, tblAlias, selStmt.Where)
	if pairs == nil {
		return nil
	}
	handlePair, fieldType := findPKHandle(tbl, pairs)
	if handlePair.value.Kind() != types.KindNull && len(pairs) == 1 {
		schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
		if schema == nil {
			return nil
		}
		dbName := tblName.Schema.L
		if dbName == "" {
			dbName = ctx.GetSessionVars().CurrentDB
		}
		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
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
			p.IsTableDual = true
			return p
		}
		p.Handle = intDatum.GetInt64()
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
		idxValues, idxValueParams := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			continue
		}
		schema, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
		if schema == nil {
			return nil
		}
		dbName := tblName.Schema.L
		if dbName == "" {
			dbName = ctx.GetSessionVars().CurrentDB
		}
		p := newPointGetPlan(ctx, dbName, schema, tbl, names)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		p.IndexValueParams = idxValueParams
		return p
	}
	return nil
}

func newPointGetPlan(ctx sessionctx.Context, dbName string, schema *expression.Schema, tbl *model.TableInfo, names []*types.FieldName) *PointGetPlan {
	p := &PointGetPlan{
		basePlan:     newBasePlan(ctx, plancodec.TypePointGet, 0),
		dbName:       dbName,
		schema:       schema,
		TblInfo:      tbl,
		outputNames:  names,
		LockWaitTime: ctx.GetSessionVars().LockWaitTimeout,
	}
	ctx.GetSessionVars().StmtCtx.Tables = []stmtctx.TableEntry{{DB: ctx.GetSessionVars().CurrentDB, Table: tbl.Name.L}}
	return p
}

func checkFastPlanPrivilege(ctx sessionctx.Context, dbName, tableName string, checkTypes ...mysql.PrivilegeType) error {
	pm := privilege.GetPrivilegeManager(ctx)
	if pm == nil {
		return nil
	}
	for _, checkType := range checkTypes {
		if !pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", checkType) {
			return errors.New("privilege check fail")
		}
	}
	return nil
}

func buildSchemaFromFields(
	dbName model.CIStr,
	tbl *model.TableInfo,
	tblName model.CIStr,
	fields []*ast.SelectField,
) (
	*expression.Schema,
	[]*types.FieldName,
) {
	columns := make([]*expression.Column, 0, len(tbl.Columns)+1)
	names := make([]*types.FieldName, 0, len(tbl.Columns)+1)
	if len(fields) > 0 {
		for _, field := range fields {
			if field.WildCard != nil {
				if field.WildCard.Table.L != "" && field.WildCard.Table.L != tblName.L {
					return nil, nil
				}
				for _, col := range tbl.Columns {
					names = append(names, &types.FieldName{
						DBName:      dbName,
						OrigTblName: tbl.Name,
						TblName:     tblName,
						ColName:     col.Name,
					})
					columns = append(columns, colInfoToColumn(col, len(columns)))
				}
				continue
			}
			colNameExpr, ok := field.Expr.(*ast.ColumnNameExpr)
			if !ok {
				return nil, nil
			}
			if colNameExpr.Name.Table.L != "" && colNameExpr.Name.Table.L != tblName.L {
				return nil, nil
			}
			col := findCol(tbl, colNameExpr.Name)
			if col == nil {
				return nil, nil
			}
			asName := col.Name
			if field.AsName.L != "" {
				asName = field.AsName
			}
			names = append(names, &types.FieldName{
				DBName:      dbName,
				OrigTblName: tbl.Name,
				TblName:     tblName,
				ColName:     asName,
			})
			columns = append(columns, colInfoToColumn(col, len(columns)))
		}
		return expression.NewSchema(columns...), names
	}
	// fields len is 0 for update and delete.
	for _, col := range tbl.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			OrigTblName: tbl.Name,
			TblName:     tblName,
			ColName:     col.Name,
		})
		column := colInfoToColumn(col, len(columns))
		columns = append(columns, column)
	}
	schema := expression.NewSchema(columns...)
	return schema, names
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
func getNameValuePairs(nvPairs []nameValuePair, tblName model.CIStr, expr ast.ExprNode) []nameValuePair {
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		return nil
	}
	if binOp.Op == opcode.LogicAnd {
		nvPairs = getNameValuePairs(nvPairs, tblName, binOp.L)
		if nvPairs == nil {
			return nil
		}
		nvPairs = getNameValuePairs(nvPairs, tblName, binOp.R)
		if nvPairs == nil {
			return nil
		}
		return nvPairs
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
			return nil
		}
		if d.IsNull() {
			return nil
		}
		if colName.Name.Table.L != "" && colName.Name.Table.L != tblName.L {
			return nil
		}
		return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d, param: param})
	}
	return nil
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
	if checkFastPlanPrivilege(ctx, fastSelect.dbName, fastSelect.TblInfo.Name.L, mysql.SelectPriv, mysql.UpdatePriv) != nil {
		return nil
	}
	if fastSelect.IsTableDual {
		return PhysicalTableDual{
			names: fastSelect.outputNames,
		}.Init(ctx, &property.StatsInfo{}, 0)
	}
	if ctx.GetSessionVars().TxnCtx.IsPessimistic {
		fastSelect.Lock = true
	}
	orderedList, allAssignmentsAreConstant := buildOrderedList(ctx, fastSelect, updateStmt.List)
	if orderedList == nil {
		return nil
	}
	handleCol := fastSelect.findHandleCol()
	updatePlan := Update{
		SelectPlan:  fastSelect,
		OrderedList: orderedList,
		TblColPosInfos: TblColPosInfoSlice{
			TblColPosInfo{
				TblID:         fastSelect.TblInfo.ID,
				Start:         0,
				End:           fastSelect.schema.Len(),
				HandleOrdinal: handleCol.Index,
			},
		},
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
	}.Init(ctx)
	updatePlan.names = fastSelect.outputNames
	return updatePlan
}

func buildOrderedList(ctx sessionctx.Context, fastSelect *PointGetPlan, list []*ast.Assignment,
) (orderedList []*expression.Assignment, allAssignmentsAreConstant bool) {
	orderedList = make([]*expression.Assignment, 0, len(list))
	allAssignmentsAreConstant = true
	for _, assign := range list {
		idx, err := expression.FindFieldName(fastSelect.outputNames, assign.Column)
		if idx == -1 || err != nil {
			return nil, true
		}
		col := fastSelect.schema.Columns[idx]
		newAssign := &expression.Assignment{
			Col:     col,
			ColName: fastSelect.OutputNames()[idx].ColName,
		}
		expr, err := expression.RewriteSimpleExprWithNames(ctx, assign.Expr, fastSelect.schema, fastSelect.outputNames)
		if err != nil {
			return nil, true
		}
		expr = expression.BuildCastFunction(ctx, expr, col.GetType())
		if allAssignmentsAreConstant {
			_, isConst := expr.(*expression.Constant)
			allAssignmentsAreConstant = isConst
		}

		newAssign.Expr, err = expr.ResolveIndices(fastSelect.schema)
		if err != nil {
			return nil, true
		}
		orderedList = append(orderedList, newAssign)
	}
	return orderedList, allAssignmentsAreConstant
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
	if checkFastPlanPrivilege(ctx, fastSelect.dbName, fastSelect.TblInfo.Name.L, mysql.SelectPriv, mysql.DeletePriv) != nil {
		return nil
	}
	if fastSelect.IsTableDual {
		return PhysicalTableDual{
			names: fastSelect.outputNames,
		}.Init(ctx, &property.StatsInfo{}, 0)
	}
	if ctx.GetSessionVars().TxnCtx.IsPessimistic {
		fastSelect.Lock = true
	}
	handleCol := fastSelect.findHandleCol()
	delPlan := Delete{
		SelectPlan: fastSelect,
		TblColPosInfos: TblColPosInfoSlice{
			TblColPosInfo{
				TblID:         fastSelect.TblInfo.ID,
				Start:         0,
				End:           fastSelect.schema.Len(),
				HandleOrdinal: handleCol.Index,
			},
		},
	}.Init(ctx)
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

func colInfoToColumn(col *model.ColumnInfo, idx int) *expression.Column {
	return &expression.Column{
		RetType:  &col.FieldType,
		ID:       col.ID,
		UniqueID: int64(col.Offset),
		Index:    idx,
	}
}

func (p *PointGetPlan) findHandleCol() *expression.Column {
	// fields len is 0 for update and delete.
	var handleCol *expression.Column
	tbl := p.TblInfo
	if tbl.PKIsHandle {
		for i, col := range p.TblInfo.Columns {
			if mysql.HasPriKeyFlag(col.Flag) && tbl.PKIsHandle {
				handleCol = p.schema.Columns[i]
			}
		}
	}
	if handleCol == nil {
		handleCol = colInfoToColumn(model.NewExtraHandleColInfo(), p.schema.Len())
		p.schema.Append(handleCol)
	}
	return handleCol
}
