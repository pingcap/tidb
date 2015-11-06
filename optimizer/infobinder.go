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
// See the License for the specific language governing permissions and
// limitations under the License.

package optimizer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
)

// InfoBinder binds schema information for table name and column name and set result fields
// for ResetSetNode.
// We need to know which table a table name refers to, which column a column name refers to.
//
// In general, a reference can only refer to information that are available for it.
// So children elements are visited in the order that previous elements make information
// available for following elements.
//
// During visiting, information are collected and stored in binderContext.
// When we enter a sub query, a new binderContext is pushed to the contextStack, so sub query
// information can overwrite outer query information. When we look up for a column reference,
// we look up from top to bottom in the contextStack.
type InfoBinder struct {
	Info          infoschema.InfoSchema
	DefaultSchema model.CIStr
	Err           error

	contextStack []*binderContext
}

// binderContext stores information that table name and column name
// can be bind to.
type binderContext struct {
	/* For Select Statement. */
	// table map to lookup and check table name conflict.
	tableMap map[string]int
	// tableSources collected in from clause.
	tables []*ast.TableSource
	// result fields collected in select field list.
	fieldList []*ast.ResultField
	// result fields collected in group by clause.
	groupBy []*ast.ResultField

	// The join node stack is used by on condition to find out
	// available tables to reference. On condition can only
	// refer to tables involved in current join.
	joinNodeStack []*ast.Join

	// When visiting TableRefs, tables in this context are not available
	// because it is being collected.
	inTableRefs bool
	// When visiting on conditon only tables in current join node are available.
	inOnCondition bool
	// When visiting field list, fieldList in this context are not available.
	inFieldList bool
	// When visiting group by, groupBy fields are not available.
	inGroupBy bool
	// When visiting having, only fieldList and groupBy fields are available.
	inHaving bool
}

// currentContext gets the current binder context.
func (sb *InfoBinder) currentContext() *binderContext {
	stackLen := len(sb.contextStack)
	if stackLen == 0 {
		return nil
	}
	return sb.contextStack[stackLen-1]
}

// pushContext is called when we enter a statement.
func (sb *InfoBinder) pushContext() {
	sb.contextStack = append(sb.contextStack, &binderContext{
		tableMap: map[string]int{},
	})
}

// popContext is called when we leave a statement.
func (sb *InfoBinder) popContext() {
	sb.contextStack = sb.contextStack[:len(sb.contextStack)-1]
}

// pushJoin is called when we enter a join node.
func (sb *InfoBinder) pushJoin(j *ast.Join) {
	ctx := sb.currentContext()
	ctx.joinNodeStack = append(ctx.joinNodeStack, j)
}

// popJoin is called when we leave a join node.
func (sb *InfoBinder) popJoin() {
	ctx := sb.currentContext()
	ctx.joinNodeStack = ctx.joinNodeStack[:len(ctx.joinNodeStack)-1]
}

// Enter implements ast.Visitor interface.
func (sb *InfoBinder) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	switch v := inNode.(type) {
	case *ast.SelectStmt:
		sb.pushContext()
	case *ast.TableRefsClause:
		sb.currentContext().inTableRefs = true
	case *ast.Join:
		sb.pushJoin(v)
	case *ast.OnCondition:
		sb.currentContext().inOnCondition = true
	case *ast.FieldList:
		sb.currentContext().inFieldList = true
	case *ast.GroupByClause:
		sb.currentContext().inGroupBy = true
	case *ast.HavingClause:
		sb.currentContext().inHaving = true
	case *ast.InsertStmt:
		sb.pushContext()
	case *ast.DeleteStmt:
		sb.pushContext()
	case *ast.UpdateStmt:
		sb.pushContext()
	}
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (sb *InfoBinder) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	switch v := inNode.(type) {
	case *ast.TableName:
		sb.handleTableName(v)
	case *ast.ColumnName:
		sb.handleColumnName(v)
	case *ast.TableSource:
		sb.handleTableSource(v)
	case *ast.OnCondition:
		sb.currentContext().inOnCondition = false
	case *ast.Join:
		sb.handleJoin(v)
		sb.popJoin()
	case *ast.TableRefsClause:
		sb.currentContext().inTableRefs = false
	case *ast.FieldList:
		sb.handleFieldList(v)
		sb.currentContext().inFieldList = false
	case *ast.GroupByClause:
		sb.currentContext().inGroupBy = false
	case *ast.HavingClause:
		sb.currentContext().inHaving = false
	case *ast.SelectStmt:
		v.SetResultFields(sb.currentContext().fieldList)
		sb.popContext()
	case *ast.InsertStmt:
		sb.popContext()
	case *ast.DeleteStmt:
		sb.popContext()
	case *ast.UpdateStmt:
		sb.popContext()
	}
	return inNode, sb.Err == nil
}

// handleTableName looks up and bind the schema information for table name
// and set result fields for table name.
func (sb *InfoBinder) handleTableName(tn *ast.TableName) {
	if tn.Schema.L == "" {
		tn.Schema = sb.DefaultSchema
	}
	table, err := sb.Info.TableByName(tn.Schema, tn.Name)
	if err != nil {
		sb.Err = err
		return
	}
	tn.TableInfo = table.Meta()
	dbInfo, _ := sb.Info.SchemaByName(tn.Schema)
	tn.DBInfo = dbInfo

	rfs := make([]*ast.ResultField, len(tn.TableInfo.Columns))
	for i, v := range tn.TableInfo.Columns {
		rfs[i] = &ast.ResultField{
			Column: v,
			Table:  tn.TableInfo,
			DBName: tn.Schema,
		}
	}
	tn.SetResultFields(rfs)
	return
}

// handleTableSources checks name duplication
// and puts the table source in current binderContext.
func (sb *InfoBinder) handleTableSource(ts *ast.TableSource) {
	for _, v := range ts.GetResultFields() {
		v.TableAsName = ts.AsName
	}
	var name string
	if ts.AsName.L != "" {
		name = ts.AsName.L
	} else {
		tableName := ts.Source.(*ast.TableName)
		name = sb.tableUniqueName(tableName.Schema, tableName.Name)
	}
	ctx := sb.currentContext()
	if _, ok := ctx.tableMap[name]; ok {
		sb.Err = errors.Errorf("duplicated table/alias name %s", name)
		return
	}
	ctx.tableMap[name] = len(ctx.tables)
	ctx.tables = append(ctx.tables, ts)
	return
}

// handleJoin sets result fields for join.
func (sb *InfoBinder) handleJoin(j *ast.Join) {
	if j.Right == nil {
		j.SetResultFields(j.Left.GetResultFields())
		return
	}
	leftLen := len(j.Left.GetResultFields())
	rightLen := len(j.Right.GetResultFields())
	rfs := make([]*ast.ResultField, leftLen+rightLen)
	copy(rfs, j.Left.GetResultFields())
	copy(rfs[leftLen:], j.Right.GetResultFields())
	j.SetResultFields(rfs)
}

// handleColumnName looks up and binds schema information to
// the column name.
func (sb *InfoBinder) handleColumnName(cn *ast.ColumnName) {
	ctx := sb.currentContext()
	if ctx.inOnCondition {
		// In on condition, only tables within current join is available.
		sb.bindColumnNameInOnCondition(cn)
		return
	}

	// Try to bind the column name form top to bottom in the context stack.
	for i := len(sb.contextStack) - 1; i >= 0; i-- {
		if sb.bindColumnNameInContext(sb.contextStack[i], cn) {
			// Column is already bound or encountered an error.
			return
		}
	}
	sb.Err = errors.Errorf("Unknown column %s", cn.Name.L)
}

// bindColumnNameInContext looks up and binds schema information for a column with the ctx.
func (sb *InfoBinder) bindColumnNameInContext(ctx *binderContext, cn *ast.ColumnName) (done bool) {
	if cn.Table.L == "" {
		// If qualified table name is not specified in column name, the column name may be ambiguous,
		// We need to iterate over all tables and
	}

	if ctx.inTableRefs {
		// In TableRefsClause, column reference only in join on condition which is handled before.
		return false
	}
	if ctx.inFieldList {
		// only bind column using tables.
		return sb.bindColumnInTableSources(cn, ctx.tables)
	}
	if ctx.inGroupBy {
		// field list first, then tables.
		if sb.bindColumnInResultFields(cn, ctx.fieldList) {
			return true
		}
		return sb.bindColumnInTableSources(cn, ctx.tables)
	}
	// column name in other places can be looked up in the same order.
	if sb.bindColumnInResultFields(cn, ctx.groupBy) {
		return true
	}
	if sb.bindColumnInResultFields(cn, ctx.fieldList) {
		return true
	}

	// tables is not available for having clause.
	if !ctx.inHaving {
		return sb.bindColumnInTableSources(cn, ctx.tables)
	}
	return false
}

// bindColumnNameInOnCondition looks up for column name in current join, and
// binds the schema information.
func (sb *InfoBinder) bindColumnNameInOnCondition(cn *ast.ColumnName) {
	ctx := sb.currentContext()
	join := ctx.joinNodeStack[len(ctx.joinNodeStack)-1]
	tableSources := appendTableSources(nil, join)
	if !sb.bindColumnInTableSources(cn, tableSources) {
		sb.Err = errors.Errorf("unkown column name %s", cn.Name.O)
	}
}

func (sb *InfoBinder) bindColumnInTableSources(cn *ast.ColumnName, tableSources []*ast.TableSource) (done bool) {
	var matchedResultField *ast.ResultField
	if cn.Table.L != "" {
		var matchedTable ast.ResultSetNode
		for _, ts := range tableSources {
			if cn.Table.L == ts.AsName.L {
				// different table name.
				matchedTable = ts
				break
			}
			if tn, ok := ts.Source.(*ast.TableName); ok {
				if cn.Table.L == tn.Name.L {
					matchedTable = ts
				}
			}
		}
		if matchedTable != nil {
			resultFields := matchedTable.GetResultFields()
			for _, rf := range resultFields {
				if rf.ColumnAsName.L == cn.Name.L || rf.Column.Name.L == cn.Name.L {
					// bind column.
					matchedResultField = rf
					break
				}
			}
		}
	} else {
		for _, ts := range tableSources {
			rfs := ts.GetResultFields()
			for _, rf := range rfs {
				matchAsName := rf.ColumnAsName.L != "" && rf.ColumnAsName.L == cn.Name.L
				matchColumnName := rf.ColumnAsName.L == "" && rf.Column.Name.L == cn.Name.L
				if matchAsName || matchColumnName {
					if matchedResultField != nil {
						sb.Err = errors.Errorf("column %s is ambiguous.", cn.Name.O)
						return true
					}
					matchedResultField = rf
				}
			}
		}
	}
	if matchedResultField != nil {
		// bind column.
		cn.ColumnInfo = matchedResultField.Column
		cn.TableInfo = matchedResultField.Table
		return true
	}
	return false
}

func (sb *InfoBinder) bindColumnInResultFields(cn *ast.ColumnName, rfs []*ast.ResultField) bool {
	var matchedResultField *ast.ResultField
	for _, rf := range rfs {
		matchAsName := rf.ColumnAsName.L != "" && rf.ColumnAsName.L == cn.Name.L
		matchColumnName := rf.ColumnAsName.L == "" && rf.Column.Name.L == cn.Name.L
		if matchAsName || matchColumnName {
			if matchedResultField != nil {
				sb.Err = errors.Errorf("column %s is ambiguous.", cn.Name.O)
				return false
			}
			matchedResultField = rf
		}
	}
	if matchedResultField != nil {
		// bind column.
		cn.ColumnInfo = matchedResultField.Column
		cn.TableInfo = matchedResultField.Table
		return true
	}
	return false
}

// handleFieldList expands wild card field and set fieldList in current context.
func (sb *InfoBinder) handleFieldList(fieldList *ast.FieldList) {
	var resultFields []*ast.ResultField
	for _, v := range fieldList.Fields {
		resultFields = append(resultFields, sb.createResultFields(v)...)
	}
	sb.currentContext().fieldList = resultFields
}

// createResultFields creates result field list for a single select field.
func (sb *InfoBinder) createResultFields(field *ast.SelectField) (rfs []*ast.ResultField) {
	ctx := sb.currentContext()
	if field.WildCard != nil {
		if len(ctx.tables) == 0 {
			sb.Err = errors.Errorf("No table used.")
			return
		}
		if field.WildCard.Table.L == "" {
			for _, v := range ctx.tables {
				rfs = append(rfs, v.GetResultFields()...)
			}
		} else {
			name := sb.tableUniqueName(field.WildCard.Schema, field.WildCard.Table)
			tableIdx, ok := ctx.tableMap[name]
			if !ok {
				sb.Err = errors.Errorf("unknown table %s.", field.WildCard.Table.O)
			}
			rfs = ctx.tables[tableIdx].GetResultFields()
		}
		return
	}
	// The column is visited before so it must has been bound already.
	rf := &ast.ResultField{ColumnAsName: field.AsName}
	switch v := field.Expr.(type) {
	case *ast.ColumnNameExpr:
		rf.Column = v.Name.ColumnInfo
		rf.Table = v.Name.TableInfo
		rf.DBName = v.Name.Schema
	default:
		if field.AsName.L == "" {
			rf.ColumnAsName.L = field.Expr.Text()
			rf.ColumnAsName.O = rf.ColumnAsName.L
		}
	}
	rfs = append(rfs, rf)
	return
}

func appendTableSources(in []*ast.TableSource, resultSetNode ast.ResultSetNode) (out []*ast.TableSource) {
	switch v := resultSetNode.(type) {
	case *ast.TableSource:
		out = append(in, v)
	case *ast.Join:
		out = appendTableSources(in, v.Left)
		if v.Right != nil {
			out = appendTableSources(out, v.Right)
		}
	}
	return
}

func (sb *InfoBinder) tableUniqueName(schema, table model.CIStr) string {
	if schema.L != "" && schema.L != sb.DefaultSchema.L {
		return schema.L + "." + table.L
	}
	return table.L
}
