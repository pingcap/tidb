// Copyright 2017 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Now TableName in expression only used by sequence function like nextval(seq).
// The function arg should be evaluated as a table name rather than normal column name like mysql does.
func (er *expressionRewriter) toTable(v *ast.TableName) {
	fullName := v.Name.L
	if len(v.Schema.L) != 0 {
		fullName = v.Schema.L + "." + fullName
	}
	val := &expression.Constant{
		Value:   types.NewDatum(fullName),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	er.ctxStackAppend(val, types.EmptyName)
}

func (er *expressionRewriter) toParamMarker(v *driver.ParamMarkerExpr) {
	var value *expression.Constant
	value, er.err = expression.ParamMarkerExpression(er.sctx, v, false)
	if er.err != nil {
		return
	}
	initConstantRepertoire(er.sctx.GetEvalCtx(), value)
	er.adjustUTF8MB4Collation(value.RetType)
	if er.err != nil {
		return
	}
	er.ctxStackAppend(value, types.EmptyName)
}

func (er *expressionRewriter) clause() clauseCode {
	if er.planCtx != nil {
		return er.planCtx.builder.curClause
	}
	return expressionClause
}

func (er *expressionRewriter) toColumn(v *ast.ColumnName) {
	idx, err := expression.FindFieldName(er.names, v)
	if err != nil {
		er.err = plannererrors.ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
		return
	}
	if idx >= 0 {
		column := er.schema.Columns[idx]
		if column.IsHidden {
			er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[er.clause()])
			return
		}
		er.ctxStackAppend(column, er.names[idx])
		return
	} else if er.planCtx == nil && er.sourceTable != nil &&
		(v.Table.L == "" || er.sourceTable.Name.L == v.Table.L) {
		colInfo := er.sourceTable.FindPublicColumnByName(v.Name.L)
		if colInfo == nil || colInfo.Hidden {
			er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[er.clause()])
			return
		}
		er.ctxStackAppend(&expression.Column{
			RetType:  &colInfo.FieldType,
			ID:       colInfo.ID,
			UniqueID: colInfo.ID,
			OrigName: fmt.Sprintf("%s.%s", er.sourceTable.Name.L, colInfo.Name.L),
		}, &types.FieldName{ColName: v.Name})
		return
	}

	planCtx := er.planCtx
	if planCtx == nil {
		er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.String(), clauseMsg[er.clause()])
		return
	}

	col, name, err := findFieldNameFromNaturalUsingJoin(planCtx.plan, v)
	if err != nil {
		er.err = err
		return
	} else if col != nil {
		er.ctxStackAppend(col, name)
		return
	}
	for i := len(planCtx.builder.outerSchemas) - 1; i >= 0; i-- {
		outerSchema, outerName := planCtx.builder.outerSchemas[i], planCtx.builder.outerNames[i]
		idx, err = expression.FindFieldName(outerName, v)
		if idx >= 0 {
			column := outerSchema.Columns[idx]
			er.ctxStackAppend(&expression.CorrelatedColumn{Column: *column, Data: new(types.Datum)}, outerName[idx])
			return
		}
		if err != nil {
			er.err = plannererrors.ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
			return
		}
	}
	if _, ok := planCtx.plan.(*logicalop.LogicalUnionAll); ok && v.Table.O != "" {
		er.err = plannererrors.ErrTablenameNotAllowedHere.GenWithStackByArgs(v.Table.O, "SELECT", clauseMsg[planCtx.builder.curClause])
		return
	}
	if planCtx.builder.curClause == globalOrderByClause {
		planCtx.builder.curClause = orderByClause
	}
	er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.String(), clauseMsg[planCtx.builder.curClause])
}

func findFieldNameFromNaturalUsingJoin(p base.LogicalPlan, v *ast.ColumnName) (col *expression.Column, name *types.FieldName, err error) {
	switch x := p.(type) {
	case *logicalop.LogicalLimit, *logicalop.LogicalSelection, *logicalop.LogicalTopN, *logicalop.LogicalSort, *logicalop.LogicalMaxOneRow:
		return findFieldNameFromNaturalUsingJoin(p.Children()[0], v)
	case *logicalop.LogicalJoin:
		if x.FullSchema != nil {
			idx, err := expression.FindFieldName(x.FullNames, v)
			if err != nil {
				return nil, nil, err
			}
			if idx >= 0 {
				return x.FullSchema.Columns[idx], x.FullNames[idx], nil
			}
		}
	}
	return nil, nil, nil
}

func (er *expressionRewriter) evalDefaultExprForTable(v *ast.DefaultExpr, tbl *model.TableInfo) {
	idx, err := expression.FindFieldName(er.names, v.Name)
	if err != nil {
		er.err = err
		return
	}
	if idx < 0 {
		er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), "field list")
		return
	}
	name := er.names[idx]
	er.evalFieldDefaultValue(name, tbl)
}

func (er *expressionRewriter) evalDefaultExprWithPlanCtx(planCtx *exprRewriterPlanCtx, v *ast.DefaultExpr) {
	intest.AssertNotNil(planCtx)
	var name *types.FieldName
	// Here we will find the corresponding column for default function. At the same time, we need to consider the issue
	// of subquery and name space.
	// For example, we have two tables t1(a int default 1, b int) and t2(a int default -1, c int). Consider the following SQL:
	// 		select a from t1 where a > (select default(a) from t2)
	// Refer to the behavior of MySQL, we need to find column a in table t2. If table t2 does not have column a, then find it
	// in table t1. If there are none, return an error message.
	// Based on the above description, we need to look in er.b.allNames from back to front.
	for i := len(planCtx.builder.allNames) - 1; i >= 0; i-- {
		idx, err := expression.FindFieldName(planCtx.builder.allNames[i], v.Name)
		if err != nil {
			er.err = err
			return
		}
		if idx >= 0 {
			name = planCtx.builder.allNames[i][idx]
			break
		}
	}
	if name == nil {
		idx, err := expression.FindFieldName(er.names, v.Name)
		if err != nil {
			er.err = err
			return
		}
		if idx < 0 {
			er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), "field list")
			return
		}
		name = er.names[idx]
	}

	dbName := name.DBName
	if dbName.O == "" {
		// if database name is not specified, use current database name
		dbName = ast.NewCIStr(planCtx.builder.ctx.GetSessionVars().CurrentDB)
	}
	if name.OrigTblName.O == "" {
		// column is evaluated by some expressions, for example:
		// `select default(c) from (select (a+1) as c from t) as t0`
		// in such case, a 'no default' error is returned
		er.err = table.ErrNoDefaultValue.GenWithStackByArgs(name.ColName)
		return
	}
	var tbl table.Table
	tbl, er.err = planCtx.builder.is.TableByName(context.Background(), dbName, name.OrigTblName)
	if er.err != nil {
		return
	}
	er.evalFieldDefaultValue(name, tbl.Meta())
}

func (er *expressionRewriter) evalFieldDefaultValue(field *types.FieldName, tblInfo *model.TableInfo) {
	colName := field.OrigColName.L
	if colName == "" {
		// in some cases, OrigColName is empty, use ColName instead
		colName = field.ColName.L
	}
	col := tblInfo.FindPublicColumnByName(colName)
	if col == nil {
		er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(colName, "field_list")
		return
	}
	isCurrentTimestamp := hasCurrentDatetimeDefault(col)
	var val *expression.Constant
	switch {
	case isCurrentTimestamp && (col.GetType() == mysql.TypeDatetime || col.GetType() == mysql.TypeTimestamp):
		t, err := expression.GetTimeValue(er.sctx, ast.CurrentTimestamp, col.GetType(), col.GetDecimal(), nil)
		if err != nil {
			return
		}
		val = &expression.Constant{
			Value:   t,
			RetType: types.NewFieldType(col.GetType()),
		}
	default:
		// for other columns, just use what it is
		d, err := table.GetColDefaultValue(er.sctx, col)
		if err != nil {
			er.err = err
			return
		}
		val = &expression.Constant{
			Value:   d,
			RetType: col.FieldType.Clone(),
		}
	}
	if er.err != nil {
		return
	}
	er.ctxStackAppend(val, types.EmptyName)
}

// hasCurrentDatetimeDefault checks if column has current_timestamp default value
func hasCurrentDatetimeDefault(col *model.ColumnInfo) bool {
	x, ok := col.DefaultValue.(string)
	if !ok {
		return false
	}
	return strings.ToLower(x) == ast.CurrentTimestamp
}

// hasLimit checks if the plan already contains a LIMIT operator
func hasLimit(plan base.LogicalPlan) bool {
	if plan == nil {
		return false
	}
	// Check if this is a LogicalLimit
	if _, ok := plan.(*logicalop.LogicalLimit); ok {
		return true
	}
	// Recursively check children
	for _, child := range plan.Children() {
		if hasLimit(child) {
			return true
		}
	}
	return false
}
