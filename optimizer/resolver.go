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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/db"
)

// ResolveName resolves table name and column name.
// It generates ResultFields for ResultSetNode and resolves ColumnNameExpr to a ResultField.
func ResolveName(node ast.Node, info infoschema.InfoSchema, ctx context.Context) error {
	defaultSchema := db.GetCurrentSchema(ctx)
	resolver := nameResolver{Info: info, Ctx: ctx, DefaultSchema: model.NewCIStr(defaultSchema)}
	node.Accept(&resolver)
	return errors.Trace(resolver.Err)
}

// nameResolver is the visitor to resolve table name and column name.
// In general, a reference can only refer to information that are available for it.
// So children elements are visited in the order that previous elements make information
// available for following elements.
//
// During visiting, information are collected and stored in resolverContext.
// When we enter a subquery, a new resolverContext is pushed to the contextStack, so subquery
// information can overwrite outer query information. When we look up for a column reference,
// we look up from top to bottom in the contextStack.
type nameResolver struct {
	Info          infoschema.InfoSchema
	Ctx           context.Context
	DefaultSchema model.CIStr
	Err           error

	contextStack []*resolverContext
}

// resolverContext stores information in a single level of select statement
// that table name and column name can be resolved.
type resolverContext struct {
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
	// When visiting having, checks if the expr is an aggregate function expr.
	inHavingAgg bool
	// OrderBy clause has different resolving rule than group by.
	inOrderBy bool
	// When visiting column name in ByItem, we should know if the column name is in an expression.
	inByItemExpression bool
}

// currentContext gets the current resolverContext.
func (nr *nameResolver) currentContext() *resolverContext {
	stackLen := len(nr.contextStack)
	if stackLen == 0 {
		return nil
	}
	return nr.contextStack[stackLen-1]
}

// pushContext is called when we enter a statement.
func (nr *nameResolver) pushContext() {
	nr.contextStack = append(nr.contextStack, &resolverContext{
		tableMap: map[string]int{},
	})
}

// popContext is called when we leave a statement.
func (nr *nameResolver) popContext() {
	nr.contextStack = nr.contextStack[:len(nr.contextStack)-1]
}

// pushJoin is called when we enter a join node.
func (nr *nameResolver) pushJoin(j *ast.Join) {
	ctx := nr.currentContext()
	ctx.joinNodeStack = append(ctx.joinNodeStack, j)
}

// popJoin is called when we leave a join node.
func (nr *nameResolver) popJoin() {
	ctx := nr.currentContext()
	ctx.joinNodeStack = ctx.joinNodeStack[:len(ctx.joinNodeStack)-1]
}

// Enter implements ast.Visitor interface.
func (nr *nameResolver) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
		ctx := nr.currentContext()
		if ctx.inHaving {
			ctx.inHavingAgg = true
		}
	case *ast.ByItem:
		if _, ok := v.Expr.(*ast.ColumnNameExpr); !ok {
			// If ByItem is not a single column name expression,
			// the resolving rule is different from order by clause.
			nr.currentContext().inByItemExpression = true
		}
		if nr.currentContext().inGroupBy {
			// make sure item is not aggregate function
			if ast.HasAggFlag(v.Expr) {
				nr.Err = errors.New("group by cannot contain aggregate function")
				return inNode, true
			}
		}
	case *ast.DeleteStmt:
		nr.pushContext()
	case *ast.FieldList:
		nr.currentContext().inFieldList = true
	case *ast.GroupByClause:
		nr.currentContext().inGroupBy = true
	case *ast.HavingClause:
		nr.currentContext().inHaving = true
	case *ast.InsertStmt:
		nr.pushContext()
	case *ast.Join:
		nr.pushJoin(v)
	case *ast.OnCondition:
		nr.currentContext().inOnCondition = true
	case *ast.OrderByClause:
		nr.currentContext().inOrderBy = true
	case *ast.SelectStmt:
		nr.pushContext()
	case *ast.TableRefsClause:
		nr.currentContext().inTableRefs = true
	case *ast.UpdateStmt:
		nr.pushContext()
	}
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (nr *nameResolver) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
		ctx := nr.currentContext()
		if ctx.inHaving {
			ctx.inHavingAgg = false
		}
	case *ast.TableName:
		nr.handleTableName(v)
	case *ast.ColumnNameExpr:
		nr.handleColumnName(v)
	case *ast.TableSource:
		nr.handleTableSource(v)
	case *ast.OnCondition:
		nr.currentContext().inOnCondition = false
	case *ast.Join:
		nr.handleJoin(v)
		nr.popJoin()
	case *ast.TableRefsClause:
		nr.currentContext().inTableRefs = false
	case *ast.FieldList:
		nr.handleFieldList(v)
		nr.currentContext().inFieldList = false
	case *ast.GroupByClause:
		ctx := nr.currentContext()
		ctx.inGroupBy = false
		for _, item := range v.Items {
			switch x := item.Expr.(type) {
			case *ast.ColumnNameExpr:
				ctx.groupBy = append(ctx.groupBy, x.Refer)
			}
		}
	case *ast.HavingClause:
		nr.currentContext().inHaving = false
	case *ast.OrderByClause:
		nr.currentContext().inOrderBy = false
	case *ast.ByItem:
		nr.currentContext().inByItemExpression = false
	case *ast.PositionExpr:
		nr.handlePosition(v)
	case *ast.SelectStmt:
		v.SetResultFields(nr.currentContext().fieldList)
		nr.popContext()
	case *ast.InsertStmt:
		nr.popContext()
	case *ast.DeleteStmt:
		nr.popContext()
	case *ast.UpdateStmt:
		nr.popContext()
	}
	return inNode, nr.Err == nil
}

// handleTableName looks up and sets the schema information and result fields for table name.
func (nr *nameResolver) handleTableName(tn *ast.TableName) {
	if tn.Schema.L == "" {
		tn.Schema = nr.DefaultSchema
	}
	table, err := nr.Info.TableByName(tn.Schema, tn.Name)
	if err != nil {
		nr.Err = err
		return
	}
	tn.TableInfo = table.Meta()
	dbInfo, _ := nr.Info.SchemaByName(tn.Schema)
	tn.DBInfo = dbInfo

	rfs := make([]*ast.ResultField, len(tn.TableInfo.Columns))
	for i, v := range tn.TableInfo.Columns {
		expr := &ast.ValueExpr{}
		expr.SetType(&v.FieldType)
		rfs[i] = &ast.ResultField{
			Column: v,
			Table:  tn.TableInfo,
			DBName: tn.Schema,
			Expr:   expr,
		}
	}
	tn.SetResultFields(rfs)
	return
}

// handleTableSources checks name duplication
// and puts the table source in current resolverContext.
func (nr *nameResolver) handleTableSource(ts *ast.TableSource) {
	for _, v := range ts.GetResultFields() {
		v.TableAsName = ts.AsName
	}
	var name string
	if ts.AsName.L != "" {
		name = ts.AsName.L
	} else {
		tableName := ts.Source.(*ast.TableName)
		name = nr.tableUniqueName(tableName.Schema, tableName.Name)
	}
	ctx := nr.currentContext()
	if _, ok := ctx.tableMap[name]; ok {
		nr.Err = errors.Errorf("duplicated table/alias name %s", name)
		return
	}
	ctx.tableMap[name] = len(ctx.tables)
	ctx.tables = append(ctx.tables, ts)
	return
}

// handleJoin sets result fields for join.
func (nr *nameResolver) handleJoin(j *ast.Join) {
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

// handleColumnName looks up and sets ResultField for
// the column name.
func (nr *nameResolver) handleColumnName(cn *ast.ColumnNameExpr) {
	ctx := nr.currentContext()
	if ctx.inOnCondition {
		// In on condition, only tables within current join is available.
		nr.resolveColumnNameInOnCondition(cn)
		return
	}

	// Try to resolve the column name form top to bottom in the context stack.
	for i := len(nr.contextStack) - 1; i >= 0; i-- {
		if nr.resolveColumnNameInContext(nr.contextStack[i], cn) {
			// Column is already resolved or encountered an error.
			return
		}
	}
	nr.Err = errors.Errorf("unknown column %s", cn.Name.Name.L)
}

// resolveColumnNameInContext looks up and sets ResultField for a column with the ctx.
func (nr *nameResolver) resolveColumnNameInContext(ctx *resolverContext, cn *ast.ColumnNameExpr) bool {
	if ctx.inTableRefs {
		// In TableRefsClause, column reference only in join on condition which is handled before.
		return false
	}
	if ctx.inFieldList {
		// only resolve column using tables.
		return nr.resolveColumnInTableSources(cn, ctx.tables)
	}
	if ctx.inGroupBy {
		// From tables first, then field list.
		// If ctx.InByItemExpression is true, the item is not an identifier.
		// Otherwise it is an identifier.
		if ctx.inByItemExpression {
			// From table first, then field list.
			if nr.resolveColumnInTableSources(cn, ctx.tables) {
				return true
			}
			found := nr.resolveColumnInResultFields(cn, ctx.fieldList)
			if !found || nr.Err != nil {
				return found
			}
			if _, ok := cn.Refer.Expr.(*ast.AggregateFuncExpr); ok {
				nr.Err = errors.New("Groupby identifier can not refer to aggregate function.")
			}
		}
		// Resolve from table first, then from select list.
		found := nr.resolveColumnInTableSources(cn, ctx.tables)
		if nr.Err != nil {
			return found
		}
		// We should copy the refer here.
		// Because if the ByItem is an identifier, we should check if it
		// is ambiguous even it is already resolved from table source.
		// If the ByItem is not an identifier, we do not need the second check.
		r := cn.Refer
		if nr.resolveColumnInResultFields(cn, ctx.fieldList) {
			if nr.Err != nil {
				return true
			}
			if r != nil {
				// It is not ambiguous and already resolved from table source.
				// We should restore its Refer.
				cn.Refer = r
			}
			if _, ok := cn.Refer.Expr.(*ast.AggregateFuncExpr); ok {
				nr.Err = errors.New("Groupby identifier can not refer to aggregate function.")
			}
			return true
		}
		return found
	}
	if ctx.inHaving {
		// First group by, then field list.
		if nr.resolveColumnInResultFields(cn, ctx.groupBy) {
			return true
		}
		found := nr.resolveColumnInResultFields(cn, ctx.fieldList)
		if nr.Err != nil {
			return found
		}
		if found || !ctx.inHavingAgg {
			return found
		}
		return nr.resolveColumnInTableSources(cn, ctx.tables)
	}
	if ctx.inOrderBy {
		if nr.resolveColumnInResultFields(cn, ctx.groupBy) {
			return true
		}
		if ctx.inByItemExpression {
			// From table first, then field list.
			if nr.resolveColumnInTableSources(cn, ctx.tables) {
				return true
			}
			return nr.resolveColumnInResultFields(cn, ctx.fieldList)
		}
		// Field list first, then from table.
		if nr.resolveColumnInResultFields(cn, ctx.fieldList) {
			return true
		}
		return nr.resolveColumnInTableSources(cn, ctx.tables)
	}
	// In where clause.
	return nr.resolveColumnInTableSources(cn, ctx.tables)
}

// resolveColumnNameInOnCondition resolves the column name in current join.
func (nr *nameResolver) resolveColumnNameInOnCondition(cn *ast.ColumnNameExpr) {
	ctx := nr.currentContext()
	join := ctx.joinNodeStack[len(ctx.joinNodeStack)-1]
	tableSources := appendTableSources(nil, join)
	if !nr.resolveColumnInTableSources(cn, tableSources) {
		nr.Err = errors.Errorf("unkown column name %s", cn.Name.Name.O)
	}
}

func (nr *nameResolver) resolveColumnInTableSources(cn *ast.ColumnNameExpr, tableSources []*ast.TableSource) (done bool) {
	var matchedResultField *ast.ResultField
	tableNameL := cn.Name.Table.L
	columnNameL := cn.Name.Name.L
	if tableNameL != "" {
		var matchedTable ast.ResultSetNode
		for _, ts := range tableSources {
			if tableNameL == ts.AsName.L {
				// different table name.
				matchedTable = ts
				break
			}
			if tn, ok := ts.Source.(*ast.TableName); ok {
				if tableNameL == tn.Name.L {
					matchedTable = ts
				}
			}
		}
		if matchedTable != nil {
			resultFields := matchedTable.GetResultFields()
			for _, rf := range resultFields {
				if rf.ColumnAsName.L == columnNameL || rf.Column.Name.L == columnNameL {
					// resolve column.
					matchedResultField = rf
					break
				}
			}
		}
	} else {
		for _, ts := range tableSources {
			rfs := ts.GetResultFields()
			for _, rf := range rfs {
				matchAsName := rf.ColumnAsName.L != "" && rf.ColumnAsName.L == columnNameL
				matchColumnName := rf.ColumnAsName.L == "" && rf.Column.Name.L == columnNameL
				if matchAsName || matchColumnName {
					if matchedResultField != nil {
						nr.Err = errors.Errorf("column %s is ambiguous.", cn.Name.Name.O)
						return true
					}
					matchedResultField = rf
				}
			}
		}
	}
	if matchedResultField != nil {
		// Bind column.
		cn.Refer = matchedResultField
		return true
	}
	return false
}

func (nr *nameResolver) resolveColumnInResultFields(cn *ast.ColumnNameExpr, rfs []*ast.ResultField) bool {
	if cn.Name.Table.L != "" {
		// Skip result fields, resolve the column in table source.
		return false
	}
	var matched *ast.ResultField
	for _, rf := range rfs {
		matchAsName := cn.Name.Name.L == rf.ColumnAsName.L
		matchColumnName := cn.Name.Name.L == rf.Column.Name.L
		if matchAsName || matchColumnName {
			if rf.Column.Name.L == "" {
				// This is not a real table column, resolve it directly.
				cn.Refer = rf
				return true
			}
			if matched == nil {
				matched = rf
			} else {
				sameColumn := matched.Table.Name.L == rf.Table.Name.L && matched.Column.Name.L == rf.Column.Name.L
				if !sameColumn {
					nr.Err = errors.Errorf("column %s is ambiguous.", cn.Name.Name.O)
					return true
				}
			}
		}
	}
	if matched != nil {
		// Bind column.
		cn.Refer = matched
		return true
	}
	return false
}

// handleFieldList expands wild card field and sets fieldList in current context.
func (nr *nameResolver) handleFieldList(fieldList *ast.FieldList) {
	var resultFields []*ast.ResultField
	for _, v := range fieldList.Fields {
		resultFields = append(resultFields, nr.createResultFields(v)...)
	}
	nr.currentContext().fieldList = resultFields
}

func getInnerFromParentheses(expr ast.ExprNode) ast.ExprNode {
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		return getInnerFromParentheses(pexpr.Expr)
	}
	return expr
}

// createResultFields creates result field list for a single select field.
func (nr *nameResolver) createResultFields(field *ast.SelectField) (rfs []*ast.ResultField) {
	ctx := nr.currentContext()
	if field.WildCard != nil {
		if len(ctx.tables) == 0 {
			nr.Err = errors.New("No table used.")
			return
		}
		if field.WildCard.Table.L == "" {
			for _, v := range ctx.tables {
				rfs = append(rfs, v.GetResultFields()...)
			}
		} else {
			name := nr.tableUniqueName(field.WildCard.Schema, field.WildCard.Table)
			tableIdx, ok := ctx.tableMap[name]
			if !ok {
				nr.Err = errors.Errorf("unknown table %s.", field.WildCard.Table.O)
			}
			rfs = ctx.tables[tableIdx].GetResultFields()
		}
		return
	}
	// The column is visited before so it must has been resolved already.
	rf := &ast.ResultField{ColumnAsName: field.AsName}
	innerExpr := getInnerFromParentheses(field.Expr)
	switch v := innerExpr.(type) {
	case *ast.ColumnNameExpr:
		rf.Column = v.Refer.Column
		rf.Table = v.Refer.Table
		rf.DBName = v.Refer.DBName
		rf.Expr = v.Refer.Expr
	default:
		rf.Column = &model.ColumnInfo{} // Empty column info.
		rf.Table = &model.TableInfo{}   // Empty table info.
		rf.Expr = v
	}
	if field.AsName.L == "" {
		switch x := innerExpr.(type) {
		case *ast.ColumnNameExpr:
			rf.ColumnAsName = model.NewCIStr(x.Name.Name.O)
		case *ast.ValueExpr:
			if innerExpr.Text() != "" {
				rf.ColumnAsName = model.NewCIStr(innerExpr.Text())
			} else {
				rf.ColumnAsName = model.NewCIStr(field.Text())
			}
		default:
			rf.ColumnAsName = model.NewCIStr(field.Text())
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

func (nr *nameResolver) tableUniqueName(schema, table model.CIStr) string {
	if schema.L != "" && schema.L != nr.DefaultSchema.L {
		return schema.L + "." + table.L
	}
	return table.L
}

func (nr *nameResolver) handlePosition(pos *ast.PositionExpr) {
	ctx := nr.currentContext()
	if pos.N < 1 || pos.N > len(ctx.fieldList) {
		nr.Err = errors.Errorf("Unknown column '%d'", pos.N)
		return
	}
	pos.Refer = ctx.fieldList[pos.N-1]
	if nr.currentContext().inGroupBy {
		// make sure item is not aggregate function
		if ast.HasAggFlag(pos.Refer.Expr) {
			nr.Err = errors.New("group by cannot contain aggregate function")
		}
	}
}
