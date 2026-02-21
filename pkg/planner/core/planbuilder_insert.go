// Copyright 2015 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func (b *PlanBuilder) getDefaultValueForInsert(col *table.Column) (*expression.Constant, error) {
	var (
		value types.Datum
		err   error
	)
	if col.DefaultIsExpr && col.DefaultExpr != nil {
		value, err = table.EvalColDefaultExpr(b.ctx.GetExprCtx(), col.ToInfo(), col.DefaultExpr)
	} else {
		if err := table.CheckNoDefaultValueForInsert(b.ctx.GetSessionVars().StmtCtx, col.ToInfo()); err != nil {
			return nil, err
		}
		value, err = table.GetColDefaultValue(b.ctx.GetExprCtx(), col.ToInfo())
	}
	if err != nil {
		return nil, err
	}
	return &expression.Constant{Value: value, RetType: col.FieldType.Clone()}, nil
}

// resolveGeneratedColumns resolves generated columns with their generation expressions respectively.
// If not-nil onDups is passed in, it will be **modified in place** and record columns in on-duplicate list
func (b *PlanBuilder) resolveGeneratedColumns(ctx context.Context, columns []*table.Column, onDups map[string]struct{}, mockPlan base.LogicalPlan) (igc physicalop.InsertGeneratedColumns, err error) {
	for _, column := range columns {
		if !column.IsGenerated() {
			// columns having on-update-now flag should also be considered.
			if onDups != nil && mysql.HasOnUpdateNowFlag(column.GetFlag()) {
				onDups[column.Name.L] = struct{}{}
			}
			continue
		}
		columnName := &ast.ColumnName{Name: column.Name}
		columnName.SetText(nil, column.Name.O)

		idx, err := expression.FindFieldName(mockPlan.OutputNames(), columnName)
		if err != nil {
			return igc, err
		}
		colExpr := mockPlan.Schema().Columns[idx]

		originalVal := b.allowBuildCastArray
		b.allowBuildCastArray = true
		expr, _, err := b.rewrite(ctx, column.GeneratedExpr.Clone(), mockPlan, nil, true)
		b.allowBuildCastArray = originalVal
		if err != nil {
			return igc, err
		}

		igc.Exprs = append(igc.Exprs, expr)
		if onDups == nil {
			continue
		}
		// There may be chain dependencies between columns,
		// so we need to add new columns into onDups.
		for dep := range column.Dependences {
			if _, ok := onDups[dep]; ok {
				assign := &expression.Assignment{Col: colExpr, ColName: column.Name, Expr: expr}
				igc.OnDuplicates = append(igc.OnDuplicates, assign)
				// onDups use lower column name, see Insert.resolveOnDuplicate
				onDups[column.Name.L] = struct{}{}
				break
			}
		}
	}
	return igc, nil
}

func (b *PlanBuilder) buildInsert(ctx context.Context, insert *ast.InsertStmt) (base.Plan, error) {
	ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, infoschema.ErrTableNotExists.FastGenByArgs()
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, infoschema.ErrTableNotExists.FastGenByArgs()
	}
	tnW := b.resolveCtx.GetTableName(tn)
	tableInfo := tnW.TableInfo
	if tableInfo.IsView() {
		err := errors.Errorf("insert into view %s is not supported now", tableInfo.Name.O)
		if insert.IsReplace {
			err = errors.Errorf("replace into view %s is not supported now", tableInfo.Name.O)
		}
		return nil, err
	}
	if tableInfo.IsSequence() {
		err := errors.Errorf("insert into sequence %s is not supported now", tableInfo.Name.O)
		if insert.IsReplace {
			err = errors.Errorf("replace into sequence %s is not supported now", tableInfo.Name.O)
		}
		return nil, err
	}
	// Build Schema with DBName otherwise ColumnRef with DBName cannot match any Column in Schema.
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx.GetExprCtx(), tn.Schema, tableInfo)
	if err != nil {
		return nil, err
	}
	tableInPlan, ok := b.is.TableByID(ctx, tableInfo.ID)
	if !ok {
		return nil, errors.Errorf("Can't get table %s", tableInfo.Name.O)
	}

	insertPlan := physicalop.Insert{
		Table:         tableInPlan,
		Columns:       insert.Columns,
		TableSchema:   schema,
		TableColNames: names,
		IsReplace:     insert.IsReplace,
		IgnoreErr:     insert.IgnoreErr,
	}.Init(b.ctx)

	if tableInfo.GetPartitionInfo() != nil && len(insert.PartitionNames) != 0 {
		givenPartitionSets := make(map[int64]struct{}, len(insert.PartitionNames))
		// check partition by name.
		for _, name := range insert.PartitionNames {
			id, err := tables.FindPartitionByName(tableInfo, name.L)
			if err != nil {
				return nil, err
			}
			givenPartitionSets[id] = struct{}{}
		}
		pt := tableInPlan.(table.PartitionedTable)
		insertPlan.Table = tables.NewPartitionTableWithGivenSets(pt, givenPartitionSets)
	} else if len(insert.PartitionNames) != 0 {
		return nil, plannererrors.ErrPartitionClauseOnNonpartitioned
	}

	user := b.ctx.GetSessionVars().User
	var authErr error
	if user != nil {
		authErr = plannererrors.ErrTableaccessDenied.FastGenByArgs("INSERT", user.AuthUsername, user.AuthHostname, tableInfo.Name.L)
	}

	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, tnW.DBInfo.Name.L,
		tableInfo.Name.L, "", authErr)

	// `REPLACE INTO` requires both INSERT + DELETE privilege
	// `ON DUPLICATE KEY UPDATE` requires both INSERT + UPDATE privilege
	var extraPriv mysql.PrivilegeType
	if insert.IsReplace {
		extraPriv = mysql.DeletePriv
	} else if insert.OnDuplicate != nil {
		extraPriv = mysql.UpdatePriv
	}
	if extraPriv != 0 {
		if user != nil {
			cmd := strings.ToUpper(mysql.Priv2Str[extraPriv])
			authErr = plannererrors.ErrTableaccessDenied.FastGenByArgs(cmd, user.AuthUsername, user.AuthHostname, tableInfo.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, extraPriv, tnW.DBInfo.Name.L, tableInfo.Name.L, "", authErr)
	}

	mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	mockTablePlan.SetSchema(insertPlan.TableSchema)
	mockTablePlan.SetOutputNames(insertPlan.TableColNames)

	checkRefColumn := func(n ast.Node) ast.Node {
		if insertPlan.NeedFillDefaultValue {
			return n
		}
		switch n.(type) {
		case *ast.ColumnName, *ast.ColumnNameExpr:
			insertPlan.NeedFillDefaultValue = true
		}
		return n
	}

	if len(insert.Lists) > 0 {
		// Branch for `INSERT ... VALUES ...`.
		// Branch for `INSERT ... SET ...`.
		err := b.buildValuesListOfInsert(ctx, insert, insertPlan, mockTablePlan, checkRefColumn)
		if err != nil {
			return nil, err
		}
	} else {
		// Branch for `INSERT ... SELECT ...`.
		err := b.buildSelectPlanOfInsert(ctx, insert, insertPlan)
		if err != nil {
			return nil, err
		}
	}

	mockTablePlan.SetSchema(insertPlan.Schema4OnDuplicate)
	mockTablePlan.SetOutputNames(insertPlan.Names4OnDuplicate)

	onDupColSet, err := insertPlan.ResolveOnDuplicate(insert.OnDuplicate, tableInfo, func(node ast.ExprNode) (expression.Expression, error) {
		return b.rewriteInsertOnDuplicateUpdate(ctx, node, mockTablePlan, insertPlan)
	})
	if err != nil {
		return nil, err
	}

	// Calculate generated columns.
	mockTablePlan.SetSchema(insertPlan.TableSchema)
	mockTablePlan.SetOutputNames(insertPlan.TableColNames)
	insertPlan.GenCols, err = b.resolveGeneratedColumns(ctx, insertPlan.Table.Cols(), onDupColSet, mockTablePlan)
	if err != nil {
		return nil, err
	}

	err = insertPlan.ResolveIndices()
	if err != nil {
		return nil, err
	}
	err = insertPlan.BuildOnInsertFKTriggers(b.ctx, b.is, tnW.DBInfo.Name.L)
	return insertPlan, err
}

func (*PlanBuilder) getAffectCols(insertStmt *ast.InsertStmt, insertPlan *physicalop.Insert) (affectedValuesCols []*table.Column, err error) {
	if len(insertStmt.Columns) > 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name (col_name [, col_name] ...) {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name (col_name [, col_name] ...) SELECT ...`.
		// 3. `INSERT INTO tbl_name SET col1=x1, ... colM=xM...`.
		colName := make([]string, 0, len(insertStmt.Columns))
		for _, col := range insertStmt.Columns {
			colName = append(colName, col.Name.L)
		}
		var missingColIdx int
		affectedValuesCols, missingColIdx = table.FindColumns(insertPlan.Table.VisibleCols(), colName, insertPlan.Table.Meta().PKIsHandle)
		if missingColIdx >= 0 {
			return nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(
				insertStmt.Columns[missingColIdx].Name.O, clauseMsg[fieldList])
		}
	} else {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name SELECT ...`.
		affectedValuesCols = insertPlan.Table.VisibleCols()
	}
	return affectedValuesCols, nil
}

func (b PlanBuilder) getInsertColExpr(ctx context.Context, insertPlan *physicalop.Insert, mockTablePlan *logicalop.LogicalTableDual, col *table.Column, expr ast.ExprNode, checkRefColumn func(n ast.Node) ast.Node) (outExpr expression.Expression, err error) {
	if col.Hidden {
		return nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(col.Name, clauseMsg[fieldList])
	}
	switch x := expr.(type) {
	case *ast.DefaultExpr:
		refCol := col
		if x.Name != nil {
			refCol = table.FindColLowerCase(insertPlan.Table.Cols(), x.Name.Name.L)
			if refCol == nil {
				return nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(x.Name.OrigColName(), clauseMsg[fieldList])
			}
			// Cannot use DEFAULT(generated column) except for the same column
			if col != refCol && (col.IsGenerated() || refCol.IsGenerated()) {
				return nil, plannererrors.ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
			} else if col == refCol && col.IsGenerated() {
				return nil, nil
			}
		} else if col.IsGenerated() {
			// See note in the end of the function. Only default for generated columns are OK.
			return nil, nil
		}
		outExpr, err = b.getDefaultValueForInsert(refCol)
	case *driver.ValueExpr:
		outExpr = &expression.Constant{
			Value:   x.Datum,
			RetType: &x.Type,
		}
	case *driver.ParamMarkerExpr:
		outExpr, err = expression.ParamMarkerExpression(b.ctx.GetExprCtx(), x, false)
	default:
		b.curClause = fieldList
		// subquery in insert values should not reference upper scope
		usingPlan := mockTablePlan
		if _, ok := expr.(*ast.SubqueryExpr); ok {
			usingPlan = logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
		}
		var np base.LogicalPlan
		outExpr, np, err = b.rewriteWithPreprocess(ctx, expr, usingPlan, nil, nil, true, checkRefColumn)
		if np != nil {
			if _, ok := np.(*logicalop.LogicalTableDual); !ok {
				// See issue#30626 and the related tests in function TestInsertValuesWithSubQuery for more details.
				// This is a TODO and we will support it later.
				return nil, errors.New("Insert's SET operation or VALUES_LIST doesn't support complex subqueries now")
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if insertPlan.AllAssignmentsAreConstant {
		_, isConstant := outExpr.(*expression.Constant)
		insertPlan.AllAssignmentsAreConstant = isConstant
	}
	// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
	// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
	if col.IsGenerated() {
		return nil, plannererrors.ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
	}
	return outExpr, nil
}

func (b *PlanBuilder) buildValuesListOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *physicalop.Insert, mockTablePlan *logicalop.LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return err
	}

	// If value_list and col_list are empty and we have a generated column, we can still write data to this table.
	// For example, insert into t values(); can be executed successfully if t has a generated column.
	if len(insert.Columns) > 0 || len(insert.Lists[0]) > 0 {
		// If value_list or col_list is not empty, the length of value_list should be the same with that of col_list.
		if len(insert.Lists[0]) != len(affectedValuesCols) {
			return plannererrors.ErrWrongValueCountOnRow.GenWithStackByArgs(1)
		}
	}

	insertPlan.AllAssignmentsAreConstant = true
	for i, valuesItem := range insert.Lists {
		// The length of all the value_list should be the same.
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		if i > 0 && len(insert.Lists[i-1]) != len(insert.Lists[i]) {
			return plannererrors.ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
		}
		exprList := make([]expression.Expression, 0, len(valuesItem))
		for j, valueItem := range valuesItem {
			expr, err := b.getInsertColExpr(ctx, insertPlan, mockTablePlan, affectedValuesCols[j], valueItem, checkRefColumn)
			if err != nil {
				return err
			}
			if expr == nil {
				continue
			}
			exprList = append(exprList, expr)
		}
		insertPlan.Lists = append(insertPlan.Lists, exprList)
	}
	insertPlan.Schema4OnDuplicate = insertPlan.TableSchema
	insertPlan.Names4OnDuplicate = insertPlan.TableColNames
	return nil
}

type colNameInOnDupExtractor struct {
	colNameMap map[types.FieldName]*ast.ColumnNameExpr
}

func (c *colNameInOnDupExtractor) Enter(node ast.Node) (ast.Node, bool) {
	switch x := node.(type) {
	case *ast.ColumnNameExpr:
		fieldName := types.FieldName{
			DBName:  x.Name.Schema,
			TblName: x.Name.Table,
			ColName: x.Name.Name,
		}
		c.colNameMap[fieldName] = x
		return node, true
	// We don't extract the column names from the sub select.
	case *ast.SelectStmt, *ast.SetOprStmt:
		return node, true
	default:
		return node, false
	}
}

func (*colNameInOnDupExtractor) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

func (b *PlanBuilder) buildSelectPlanOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *physicalop.Insert) error {
	b.isForUpdateRead = true
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return err
	}
	actualColLen := -1
	// For MYSQL, it handles the case that the columns in ON DUPLICATE UPDATE is not the project column of the SELECT clause
	// but just in the table occurs in the SELECT CLAUSE.
	//   e.g. insert into a select x from b ON DUPLICATE KEY UPDATE  a.x=b.y; the `y` is not a column of select's output.
	//        MySQL won't throw error and will execute this SQL successfully.
	// To make compatible with this strange feature, we add the variable `actualColLen` and the following IF branch.
	if len(insert.OnDuplicate) > 0 {
		// If the select has aggregation, it cannot see the columns not in the select field.
		//   e.g. insert into a select x from b ON DUPLICATE KEY UPDATE  a.x=b.y; can be executed successfully.
		//        insert into a select x from b group by x ON DUPLICATE KEY UPDATE  a.x=b.y; will report b.y not found.
		if sel, ok := insert.Select.(*ast.SelectStmt); ok && !b.detectSelectAgg(sel) {
			hasWildCard := false
			for _, field := range sel.Fields.Fields {
				if field.WildCard != nil {
					hasWildCard = true
					break
				}
			}
			if !hasWildCard {
				colExtractor := &colNameInOnDupExtractor{colNameMap: make(map[types.FieldName]*ast.ColumnNameExpr)}
				for _, assign := range insert.OnDuplicate {
					assign.Expr.Accept(colExtractor)
				}
				actualColLen = len(sel.Fields.Fields)
				for _, colName := range colExtractor.colNameMap {
					// If we found the name from the INSERT's table columns, we don't try to find it in select field anymore.
					if insertPlan.TableColNames.FindAstColName(colName.Name) {
						continue
					}
					found := false
					for _, field := range sel.Fields.Fields {
						if colField, ok := field.Expr.(*ast.ColumnNameExpr); ok &&
							(colName.Name.Schema.L == "" || colField.Name.Schema.L == "" || colName.Name.Schema.L == colField.Name.Schema.L) &&
							(colName.Name.Table.L == "" || colField.Name.Table.L == "" || colName.Name.Table.L == colField.Name.Table.L) &&
							colName.Name.Name.L == colField.Name.Name.L {
							found = true
							break
						}
					}
					if found {
						continue
					}
					sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{Expr: colName, Offset: len(sel.Fields.Fields)})
				}
				defer func(originSelFieldLen int) {
					// Revert the change for ast. Because when we use the 'prepare' and 'execute' statement it will both build plan which will cause problem.
					// You can see the issue #37187 for more details.
					sel.Fields.Fields = sel.Fields.Fields[:originSelFieldLen]
				}(actualColLen)
			}
		}
	}
	nodeW := resolve.NewNodeWWithCtx(insert.Select, b.resolveCtx)
	selectPlan, err := b.Build(ctx, nodeW)
	if err != nil {
		return err
	}

	// Check to guarantee that the length of the row returned by select is equal to that of affectedValuesCols.
	if (actualColLen == -1 && selectPlan.Schema().Len() != len(affectedValuesCols)) || (actualColLen != -1 && actualColLen != len(affectedValuesCols)) {
		return plannererrors.ErrWrongValueCountOnRow.GenWithStackByArgs(1)
	}

	// Check to guarantee that there's no generated column.
	// This check should be done after the above one to make its behavior compatible with MySQL.
	// For example, table t has two columns, namely a and b, and b is a generated column.
	// "insert into t (b) select * from t" will raise an error that the column count is not matched.
	// "insert into t select * from t" will raise an error that there's a generated column in the column list.
	// If we do this check before the above one, "insert into t (b) select * from t" will raise an error
	// that there's a generated column in the column list.
	for _, col := range affectedValuesCols {
		if col.IsGenerated() {
			return plannererrors.ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
		}
	}

	names := selectPlan.OutputNames()
	insertPlan.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag, selectPlan.(base.LogicalPlan))
	if err != nil {
		return err
	}

	if actualColLen == -1 {
		actualColLen = selectPlan.Schema().Len()
	}
	insertPlan.RowLen = actualColLen
	// schema4NewRow is the schema for the newly created data record based on
	// the result of the select statement.
	schema4NewRow := expression.NewSchema(make([]*expression.Column, len(insertPlan.Table.Cols()))...)
	names4NewRow := make(types.NameSlice, len(insertPlan.Table.Cols()))
	// TODO: don't clone it.
	for i := range actualColLen {
		selCol := insertPlan.SelectPlan.Schema().Columns[i]
		ordinal := affectedValuesCols[i].Offset
		schema4NewRow.Columns[ordinal] = &expression.Column{}
		*schema4NewRow.Columns[ordinal] = *selCol

		schema4NewRow.Columns[ordinal].RetType = &types.FieldType{}
		*schema4NewRow.Columns[ordinal].RetType = affectedValuesCols[i].FieldType

		names4NewRow[ordinal] = names[i]
	}
	for i := range schema4NewRow.Columns {
		if schema4NewRow.Columns[i] == nil {
			schema4NewRow.Columns[i] = &expression.Column{UniqueID: insertPlan.SCtx().GetSessionVars().AllocPlanColumnID()}
			names4NewRow[i] = types.EmptyName
		}
	}
	insertPlan.Schema4OnDuplicate = expression.NewSchema(insertPlan.TableSchema.Columns...)
	insertPlan.Schema4OnDuplicate.Append(insertPlan.SelectPlan.Schema().Columns[actualColLen:]...)
	insertPlan.Schema4OnDuplicate.Append(schema4NewRow.Columns...)
	insertPlan.Names4OnDuplicate = append(insertPlan.TableColNames.Shallow(), names[actualColLen:]...)
	insertPlan.Names4OnDuplicate = append(insertPlan.Names4OnDuplicate, names4NewRow...)
	return nil
}

