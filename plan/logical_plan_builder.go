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
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/util/types"
)

// UseNewPlanner means if use the new planner.
var UseNewPlanner = true

type idAllocator struct {
	id int
}

func (a *idAllocator) allocID() string {
	a.id++
	return fmt.Sprintf("_%d", a.id)
}

func (b *planBuilder) buildAggregation(p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gby []expression.Expression, correlated bool) LogicalPlan {
	agg := &Aggregation{
		AggFuncs:        make([]expression.AggregationFunction, 0, len(aggFuncList)),
		baseLogicalPlan: newBaseLogicalPlan(Agg, b.allocator)}
	agg.initID()
	agg.correlated = p.IsCorrelated() || correlated
	addChild(agg, p)
	schema := make([]*expression.Column, 0, len(aggFuncList))
	for i, aggFunc := range aggFuncList {
		var newArgList []expression.Expression
		for _, arg := range aggFunc.Args {
			newArg, np, correlated, err := b.rewrite(arg, p, nil, true)
			if err != nil {
				b.err = errors.Trace(err)
				return nil
			}
			p = np
			agg.correlated = correlated || agg.correlated
			newArgList = append(newArgList, newArg)
		}
		agg.AggFuncs = append(agg.AggFuncs, expression.NewAggFunction(aggFunc.F, newArgList, aggFunc.Distinct))
		schema = append(schema, &expression.Column{FromID: agg.id,
			ColName:     model.NewCIStr(fmt.Sprintf("%s_col_%d", agg.id, i)),
			Position:    i,
			IsAggOrSubq: true,
			RetType:     aggFunc.GetType()})
	}
	agg.GroupByItems = gby
	agg.SetSchema(schema)
	return agg
}

func (b *planBuilder) buildResultSetNode(node ast.ResultSetNode) LogicalPlan {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildNewJoin(x)
	case *ast.TableSource:
		var p LogicalPlan
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			p = b.buildNewSelect(v)
		case *ast.UnionStmt:
			p = b.buildNewUnion(v)
		case *ast.TableName:
			p = b.buildDataSource(v)
		default:
			b.err = ErrUnsupportedType.Gen("unsupported table source type %T", v)
			return nil
		}
		if b.err != nil {
			return nil
		}
		if v, ok := p.(*DataSource); ok {
			v.TableAsName = &x.AsName
		}
		if x.AsName.L != "" {
			schema := p.GetSchema()
			for _, col := range schema {
				col.TblName = x.AsName
				col.DBName = model.NewCIStr("")
			}
		}
		return p
	case *ast.SelectStmt:
		return b.buildNewSelect(x)
	case *ast.UnionStmt:
		return b.buildNewUnion(x)
	default:
		b.err = ErrUnsupportedType.Gen("unsupported table source type %T", x)
		return nil
	}
}

func extractColumn(expr expression.Expression, cols []*expression.Column, outerCols []*expression.Column) (
	result []*expression.Column, outer []*expression.Column) {
	switch v := expr.(type) {
	case *expression.Column:
		if v.Correlated {
			return cols, append(outerCols, v)
		}
		return append(cols, v), outerCols
	case *expression.ScalarFunction:
		for _, arg := range v.Args {
			cols, outerCols = extractColumn(arg, cols, outerCols)
		}
		return cols, outerCols
	}
	return cols, outerCols
}

func extractOnCondition(conditions []expression.Expression, left LogicalPlan, right LogicalPlan) (
	eqCond []*expression.ScalarFunction, leftCond []expression.Expression, rightCond []expression.Expression,
	otherCond []expression.Expression) {
	for _, expr := range conditions {
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && binop.FuncName.L == ast.EQ {
			ln, lOK := binop.Args[0].(*expression.Column)
			rn, rOK := binop.Args[1].(*expression.Column)
			if lOK && rOK {
				if left.GetSchema().GetIndex(ln) != -1 && right.GetSchema().GetIndex(rn) != -1 {
					eqCond = append(eqCond, binop)
					continue
				}
				if left.GetSchema().GetIndex(rn) != -1 && right.GetSchema().GetIndex(ln) != -1 {
					cond, _ := expression.NewFunction(ast.EQ, types.NewFieldType(mysql.TypeTiny), rn, ln)
					eqCond = append(eqCond, cond.(*expression.ScalarFunction))
					continue
				}
			}
		}
		columns, _ := extractColumn(expr, nil, nil)
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if left.GetSchema().GetIndex(col) == -1 {
				allFromLeft = false
			}
			if right.GetSchema().GetIndex(col) == -1 {
				allFromRight = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// CNF means conjunctive normal form, e.g. a and b and c.
func splitCNFItems(onExpr expression.Expression) []expression.Expression {
	switch v := onExpr.(type) {
	case *expression.ScalarFunction:
		if v.FuncName.L == ast.AndAnd {
			var ret []expression.Expression
			for _, arg := range v.Args {
				ret = append(ret, splitCNFItems(arg)...)
			}
			return ret
		}
	}
	return []expression.Expression{onExpr}
}

func (b *planBuilder) buildNewJoin(join *ast.Join) LogicalPlan {
	if join.Right == nil {
		return b.buildResultSetNode(join.Left)
	}
	leftPlan := b.buildResultSetNode(join.Left)
	rightPlan := b.buildResultSetNode(join.Right)
	newSchema := append(leftPlan.GetSchema().DeepCopy(), rightPlan.GetSchema().DeepCopy()...)
	joinPlan := &Join{baseLogicalPlan: newBaseLogicalPlan(Jn, b.allocator)}
	joinPlan.initID()
	joinPlan.SetSchema(newSchema)
	joinPlan.correlated = leftPlan.IsCorrelated() || rightPlan.IsCorrelated()
	if join.On != nil {
		onExpr, _, correlated, err := b.rewrite(join.On.Expr, joinPlan, nil, false)
		if err != nil {
			b.err = err
			return nil
		}
		if correlated {
			b.err = errors.New("On condition doesn't support subqueries yet.")
		}
		onCondition := splitCNFItems(onExpr)
		eqCond, leftCond, rightCond, otherCond := extractOnCondition(onCondition, leftPlan, rightPlan)
		joinPlan.EqualConditions = eqCond
		joinPlan.LeftConditions = leftCond
		joinPlan.RightConditions = rightCond
		joinPlan.OtherConditions = otherCond
	} else if joinPlan.JoinType == InnerJoin {
		joinPlan.cartesianJoin = true
	}
	if join.Tp == ast.LeftJoin {
		joinPlan.JoinType = LeftOuterJoin
	} else if join.Tp == ast.RightJoin {
		joinPlan.JoinType = RightOuterJoin
	} else {
		joinPlan.JoinType = InnerJoin
	}
	addChild(joinPlan, leftPlan)
	addChild(joinPlan, rightPlan)
	return joinPlan
}

func (b *planBuilder) buildSelection(p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) LogicalPlan {
	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := &Selection{baseLogicalPlan: newBaseLogicalPlan(Sel, b.allocator)}
	selection.initID()
	selection.correlated = p.IsCorrelated()
	for _, cond := range conditions {
		expr, np, correlated, err := b.rewrite(cond, p, AggMapper, false)
		if err != nil {
			b.err = err
			return nil
		}
		p = np
		selection.correlated = selection.correlated || correlated
		if expr != nil {
			expressions = append(expressions, splitCNFItems(expr)...)
		}
	}
	if len(expressions) == 0 {
		return p
	}
	selection.Conditions = expressions
	selection.SetSchema(p.GetSchema().DeepCopy())
	addChild(selection, p)
	return selection
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *planBuilder) buildProjection(p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, int) {
	proj := &Projection{
		Exprs:           make([]expression.Expression, 0, len(fields)),
		baseLogicalPlan: newBaseLogicalPlan(Proj, b.allocator),
	}
	proj.initID()
	proj.correlated = p.IsCorrelated()
	schema := make(expression.Schema, 0, len(fields))
	oldLen := 0
	for _, field := range fields {
		newExpr, np, correlated, err := b.rewrite(field.Expr, p, mapper, true)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, oldLen
		}
		p = np
		proj.correlated = proj.correlated || correlated
		proj.Exprs = append(proj.Exprs, newExpr)
		var tblName, colName model.CIStr
		if field.AsName.L != "" {
			colName = field.AsName
		} else if c, ok := newExpr.(*expression.Column); ok && !c.IsAggOrSubq {
			colName = c.ColName
			tblName = c.TblName
		} else {
			// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
			if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
				if col, ok := agg.Args[0].(*ast.ColumnNameExpr); ok {
					colName = col.Name.Name
				}
			} else {
				colName = model.NewCIStr(field.Text())
			}
		}
		schemaCol := &expression.Column{
			FromID:  proj.id,
			TblName: tblName,
			ColName: colName,
			RetType: newExpr.GetType(),
		}
		if !field.Auxiliary {
			oldLen++
		}
		schema = append(schema, schemaCol)
		schemaCol.Position = len(schema)
	}
	proj.SetSchema(schema)
	addChild(proj, p)
	return proj, oldLen
}

func (b *planBuilder) buildNewDistinct(src LogicalPlan) LogicalPlan {
	d := &Distinct{baseLogicalPlan: newBaseLogicalPlan(Dis, b.allocator)}
	d.initID()
	addChild(d, src)
	d.SetSchema(src.GetSchema())
	d.correlated = src.IsCorrelated()
	return d
}

func (b *planBuilder) buildNewUnion(union *ast.UnionStmt) LogicalPlan {
	u := &NewUnion{baseLogicalPlan: newBaseLogicalPlan(Un, b.allocator)}
	u.initID()
	u.children = make([]Plan, len(union.SelectList.Selects))
	for i, sel := range union.SelectList.Selects {
		u.children[i] = b.buildNewSelect(sel)
		u.correlated = u.correlated || u.children[i].IsCorrelated()
	}
	firstSchema := u.children[0].GetSchema().DeepCopy()
	for _, sel := range u.children {
		if len(firstSchema) != len(sel.GetSchema()) {
			b.err = errors.New("The used SELECT statements have a different number of columns")
			return nil
		}
		for i, col := range sel.GetSchema() {
			/*
			 * The lengths of the columns in the UNION result take into account the values retrieved by all of the SELECT statements
			 * SELECT REPEAT('a',1) UNION SELECT REPEAT('b',10);
			 * +---------------+
			 * | REPEAT('a',1) |
			 * +---------------+
			 * | a             |
			 * | bbbbbbbbbb    |
			 * +---------------+
			 */
			if col.RetType.Flen > firstSchema[i].RetType.Flen {
				firstSchema[i].RetType.Flen = col.RetType.Flen
			}
			// For select nul union select "abc", we should not convert "abc" to nil.
			// And the result field type should be VARCHAR.
			if firstSchema[i].RetType.Tp == 0 || firstSchema[i].RetType.Tp == mysql.TypeNull {
				firstSchema[i].RetType.Tp = col.RetType.Tp
			}
		}
		sel.SetParents(u)
	}
	for _, v := range firstSchema {
		v.FromID = u.id
		v.DBName = model.NewCIStr("")
	}

	u.SetSchema(firstSchema)
	var p LogicalPlan
	p = u
	if union.Distinct {
		p = b.buildNewDistinct(u)
	}
	if union.OrderBy != nil {
		p = b.buildNewSort(p, union.OrderBy.Items, nil)
	}
	if union.Limit != nil {
		p = b.buildNewLimit(p, union.Limit)
	}
	return p
}

// ByItems wraps a "by" item.
type ByItems struct {
	Expr expression.Expression
	Desc bool
}

func (b *planBuilder) buildNewSort(p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int) LogicalPlan {
	var exprs []*ByItems
	sort := &NewSort{baseLogicalPlan: newBaseLogicalPlan(Srt, b.allocator)}
	sort.initID()
	sort.correlated = p.IsCorrelated()
	for _, item := range byItems {
		it, np, correlated, err := b.rewrite(item.Expr, p, aggMapper, true)
		if err != nil {
			b.err = err
			return nil
		}
		p = np
		sort.correlated = sort.correlated || correlated
		exprs = append(exprs, &ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	addChild(sort, p)
	sort.SetSchema(p.GetSchema().DeepCopy())
	return sort
}

func (b *planBuilder) buildNewLimit(src LogicalPlan, limit *ast.Limit) LogicalPlan {
	li := &Limit{
		Offset:          limit.Offset,
		Count:           limit.Count,
		baseLogicalPlan: newBaseLogicalPlan(Lim, b.allocator),
	}
	li.initID()
	li.correlated = src.IsCorrelated()
	addChild(li, src)
	li.SetSchema(src.GetSchema().DeepCopy())
	return li
}

// colMatch(a,b) means that if a match b, e.g. t.a can match test.t.a bug test.t.a can't match t.a.
// Because column a want column from database test exactly.
func colMatch(a *ast.ColumnName, b *ast.ColumnName) bool {
	if a.Schema.L == "" || a.Schema.L == b.Schema.L {
		if a.Table.L == "" || a.Table.L == b.Table.L {
			return a.Name.L == b.Name.L
		}
	}
	return false
}

func matchField(f *ast.SelectField, col *ast.ColumnNameExpr) bool {
	// if col specify a table name, resolve from table source directly.
	if col.Name.Table.L == "" {
		if f.AsName.L == "" {
			if curCol, isCol := f.Expr.(*ast.ColumnNameExpr); isCol {
				return curCol.Name.Name.L == col.Name.Name.L
			}
			// a expression without as name can't be matched.
			return false
		}
		return f.AsName.L == col.Name.Name.L
	}
	return false
}

func resolveFromSelectFields(v *ast.ColumnNameExpr, fields []*ast.SelectField) (index int, err error) {
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		if field.Auxiliary {
			continue
		}
		if matchField(field, v) {
			curCol, isCol := field.Expr.(*ast.ColumnNameExpr)
			if !isCol {
				return i, nil
			}
			if matchedExpr == nil {
				matchedExpr = curCol
				index = i
			} else if !colMatch(matchedExpr.(*ast.ColumnNameExpr).Name, curCol.Name) &&
				!colMatch(curCol.Name, matchedExpr.(*ast.ColumnNameExpr).Name) {
				return -1, errors.Errorf("Column '%s' in field list is ambiguous", curCol.Name.Name.L)
			}
		}
	}
	return
}

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggFunc    bool
	inExpr       bool
	orderBy      bool
	err          error
	p            LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.ColumnNameExpr]int
	gbyItems     []*ast.ByItem
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = true
	case *ast.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.ColumnName:
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		return n, true
	default:
		a.inExpr = true
	}
	return n, false
}

func (a *AggregateFuncExtractor) resolveFromSchema(v *ast.ColumnNameExpr, schema expression.Schema) (int, error) {
	col, err := schema.FindColumn(v.Name)
	if err != nil {
		return -1, errors.Trace(err)
	}
	if col == nil {
		return -1, nil
	}
	newColName := &ast.ColumnName{
		Schema: col.DBName,
		Table:  col.TblName,
		Name:   col.ColName,
	}
	for i, field := range a.selectFields {
		if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && colMatch(newColName, c.Name) {
			return i, nil
		}
	}
	sf := &ast.SelectField{
		Expr:      &ast.ColumnNameExpr{Name: newColName},
		Auxiliary: true,
	}
	sf.Expr.SetType(col.GetType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.ColumnNameExpr:
		resolveFieldsFirst := true
		if a.inAggFunc || (a.orderBy && a.inExpr) {
			resolveFieldsFirst = false
		}
		if !a.inAggFunc && !a.orderBy {
			for _, item := range a.gbyItems {
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok && colMatch(v.Name, col.Name) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		index := -1
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields)
			if a.err != nil {
				return node, false
			}
			if index == -1 {
				index, a.err = a.resolveFromSchema(v, a.p.GetSchema())
			}
		} else {
			index, a.err = a.resolveFromSchema(v, a.p.GetSchema())
			if a.err != nil {
				return node, false
			}
			if index == -1 {
				index, a.err = resolveFromSelectFields(v, a.selectFields)
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			a.err = errors.Errorf("Unknown Column %s", v.Name.Name.L)
			return node, false
		}
		if a.inAggFunc {
			return a.selectFields[index].Expr, true
		}
		a.colMapper[v] = index
	}
	return n, true
}

func (b *planBuilder) resolveHavingAndOrderBy(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
	}
	if sel.GroupBy != nil {
		extractor.gbyItems = sel.GroupBy.Items
	}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			b.err = errors.Trace(extractor.err)
			return nil, nil
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	havingAggMapper := extractor.aggMapper
	extractor.aggMapper = make(map[*ast.AggregateFuncExpr]int)
	extractor.orderBy = true
	extractor.inExpr = false
	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				b.err = errors.Trace(extractor.err)
				return nil, nil
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return havingAggMapper, extractor.aggMapper
}

func (b *planBuilder) extractAggFuncs(fields []*ast.SelectField) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &ast.AggregateFuncExtractor{}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int)

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

// gbyResolver resolves group by items from select fields.
type gbyResolver struct {
	fields []*ast.SelectField
	schema expression.Schema
	err    error
	inExpr bool
}

func (g *gbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch inNode.(type) {
	case *ast.SubqueryExpr, *ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr:
		return inNode, true
	case *ast.ValueExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.ColumnName:
	default:
		g.inExpr = true
	}
	return inNode, false
}

func (g *gbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		if col, err := g.schema.FindColumn(v.Name); err != nil {
			g.err = errors.Trace(err)
		} else if col == nil || !g.inExpr {
			var index = -1
			index, g.err = resolveFromSelectFields(v, g.fields)
			if g.err != nil {
				return inNode, false
			}
			if col != nil {
				return inNode, true
			}
			if index != -1 {
				return g.fields[index].Expr, true
			}
			return inNode, false
		}
	case *ast.PositionExpr:
		if v.N >= 1 && v.N <= len(g.fields) {
			return g.fields[v.N-1].Expr, true
		}
		g.err = errors.Errorf("Unknown column '%d' in 'group statement'", v.N)
		return inNode, false
	}
	return inNode, true
}

func (b *planBuilder) resolveGbyExprs(p LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (LogicalPlan, bool, []expression.Expression) {
	exprs := make([]expression.Expression, 0, len(gby.Items))
	correlated := false
	resolver := &gbyResolver{fields: fields, schema: p.GetSchema()}
	for _, item := range gby.Items {
		resolver.inExpr = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			b.err = errors.Trace(resolver.err)
			return nil, false, nil
		}
		item.Expr = retExpr.(ast.ExprNode)
		expr, np, cor, err := b.rewrite(item.Expr, p, nil, true)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, false, nil
		}
		exprs = append(exprs, expr)
		correlated = correlated || cor
		p = np
	}
	return p, correlated, exprs
}

func (b *planBuilder) unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField) {
	for _, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		dbName := field.WildCard.Schema
		tblName := field.WildCard.Table
		for _, col := range p.GetSchema() {
			if (dbName.L == "" || dbName.L == col.DBName.L) &&
				(tblName.L == "" || tblName.L == col.TblName.L) {
				colName := &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Schema: col.DBName,
						Table:  col.TblName,
						Name:   col.ColName,
					}}
				colName.SetType(col.GetType())
				field := &ast.SelectField{Expr: colName}
				field.SetText(col.ColName.O)
				resultList = append(resultList, field)
			}
		}
	}
	return
}

func (b *planBuilder) buildNewSelect(sel *ast.SelectStmt) LogicalPlan {
	hasAgg := b.detectSelectAgg(sel)
	var (
		p                             LogicalPlan
		correlated                    bool
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		gbyCols                       []expression.Expression
	)
	if sel.From != nil {
		p = b.buildResultSetNode(sel.From.TableRefs)
	} else {
		p = b.buildNewTableDual()
	}
	if b.err != nil {
		return nil
	}
	sel.Fields.Fields = b.unfoldWildStar(p, sel.Fields.Fields)
	if sel.LockTp != ast.SelectLockNone {
		p = b.buildSelectLock(p, sel.LockTp)
	}
	if sel.GroupBy != nil {
		p, correlated, gbyCols = b.resolveGbyExprs(p, sel.GroupBy, sel.Fields.Fields)
		if b.err != nil {
			return nil
		}
	}
	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	havingMap, orderMap = b.resolveHavingAndOrderBy(sel, p)
	if sel.Where != nil {
		p = b.buildSelection(p, sel.Where, nil)
		if b.err != nil {
			return nil
		}
	}
	if hasAgg {
		aggFuncs, totalMap = b.extractAggFuncs(sel.Fields.Fields)
		if b.err != nil {
			return nil
		}
		p = b.buildAggregation(p, aggFuncs, gbyCols, correlated)
		if b.err != nil {
			return nil
		}
	}
	var oldLen int
	p, oldLen = b.buildProjection(p, sel.Fields.Fields, totalMap)
	if b.err != nil {
		return nil
	}
	if sel.Having != nil {
		p = b.buildSelection(p, sel.Having.Expr, havingMap)
		if b.err != nil {
			return nil
		}
	}
	if sel.Distinct {
		p = b.buildNewDistinct(p)
		if b.err != nil {
			return nil
		}
	}
	if sel.OrderBy != nil {
		p = b.buildNewSort(p, sel.OrderBy.Items, orderMap)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		p = b.buildNewLimit(p, sel.Limit)
		if b.err != nil {
			return nil
		}
	}
	if oldLen != len(p.GetSchema()) {
		return b.buildTrim(p, oldLen)
	}
	return p
}

func (b *planBuilder) buildTrim(p LogicalPlan, len int) LogicalPlan {
	trim := &Trim{baseLogicalPlan: newBaseLogicalPlan(Trm, b.allocator)}
	trim.initID()
	addChild(trim, p)
	trim.SetSchema(p.GetSchema().DeepCopy()[:len])
	trim.correlated = p.IsCorrelated()
	return trim
}

func (b *planBuilder) buildNewTableDual() LogicalPlan {
	dual := &NewTableDual{baseLogicalPlan: newBaseLogicalPlan(Dual, b.allocator)}
	dual.initID()
	return dual
}

func (b *planBuilder) getTableStats(table *model.TableInfo) *statistics.Table {
	// TODO: Currently we always return a pseudo table for good performance. We will use a cache in future.
	return statistics.PseudoTable(table)
}

func (b *planBuilder) buildDataSource(tn *ast.TableName) LogicalPlan {
	statisticTable := b.getTableStats(tn.TableInfo)
	if b.err != nil {
		return nil
	}
	p := &DataSource{
		table:           tn,
		Table:           tn.TableInfo,
		baseLogicalPlan: newBaseLogicalPlan(Ts, b.allocator),
		statisticTable:  statisticTable,
	}
	p.initID()
	// Equal condition contains a column from previous joined table.
	rfs := tn.GetResultFields()
	schema := make([]*expression.Column, 0, len(rfs))
	for i, rf := range rfs {
		p.DBName = &rf.DBName
		p.Columns = append(p.Columns, rf.Column)
		schema = append(schema, &expression.Column{
			FromID:   p.id,
			ColName:  rf.Column.Name,
			TblName:  rf.Table.Name,
			DBName:   rf.DBName,
			RetType:  &rf.Column.FieldType,
			Position: i})
	}
	p.SetSchema(schema)
	return p
}

// ApplyConditionChecker checks whether all or any output of apply matches a condition.
type ApplyConditionChecker struct {
	Condition expression.Expression
	All       bool
}

func (b *planBuilder) buildApply(p, inner LogicalPlan, schema expression.Schema, checker *ApplyConditionChecker) LogicalPlan {
	ap := &Apply{
		InnerPlan:       inner,
		OuterSchema:     schema,
		Checker:         checker,
		baseLogicalPlan: newBaseLogicalPlan(App, b.allocator),
	}
	ap.initID()
	addChild(ap, p)
	_, inner, b.err = inner.PredicatePushDown(nil)
	if b.err != nil {
		return nil
	}
	outerColumns, err := inner.PruneColumnsAndResolveIndices(inner.GetSchema())
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	used := make([]bool, len(ap.OuterSchema))
	for _, outerCol := range outerColumns {
		// If the outer column can't be resolved from this outer schema, it should be resolved by outer schema.
		if idx := ap.OuterSchema.GetIndex(outerCol); idx == -1 {
			ap.outerColumns = append(ap.outerColumns, outerCol)
		} else {
			used[idx] = true
		}
	}
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			ap.OuterSchema = append(ap.OuterSchema[:i], ap.OuterSchema[i+1:]...)
		}
	}
	innerSchema := inner.GetSchema().DeepCopy()
	if checker == nil {
		for _, col := range innerSchema {
			col.IsAggOrSubq = true
		}
		ap.SetSchema(append(p.GetSchema().DeepCopy(), innerSchema...))
	} else {
		ap.SetSchema(append(p.GetSchema().DeepCopy(), &expression.Column{
			FromID:      ap.id,
			ColName:     model.NewCIStr("exists_row"),
			RetType:     types.NewFieldType(mysql.TypeTiny),
			IsAggOrSubq: true,
		}))
	}
	ap.correlated = p.IsCorrelated() || len(ap.outerColumns) > 0
	return ap
}

func (b *planBuilder) buildExists(p LogicalPlan) LogicalPlan {
out:
	for {
		switch p.(type) {
		// This can be removed when in exists clause,
		// e.g. exists(select count(*) from t order by a) is equal to exists t.
		case *Trim, *Projection, *NewSort, *Aggregation:
			p = p.GetChildByIndex(0).(LogicalPlan)
			p.SetParents()
		default:
			break out
		}
	}
	exists := &Exists{baseLogicalPlan: newBaseLogicalPlan(Ext, b.allocator)}
	exists.initID()
	addChild(exists, p)
	newCol := &expression.Column{
		FromID:  exists.id,
		RetType: types.NewFieldType(mysql.TypeTiny),
		ColName: model.NewCIStr("exists_col")}
	exists.SetSchema([]*expression.Column{newCol})
	exists.correlated = p.IsCorrelated()
	return exists
}

func (b *planBuilder) buildMaxOneRow(p LogicalPlan) LogicalPlan {
	maxOneRow := &MaxOneRow{baseLogicalPlan: newBaseLogicalPlan(MOR, b.allocator)}
	maxOneRow.initID()
	addChild(maxOneRow, p)
	maxOneRow.SetSchema(p.GetSchema().DeepCopy())
	maxOneRow.correlated = p.IsCorrelated()
	return maxOneRow
}

// tryDecorrelated tries to remove the correlated column that can be found in the outerPlan's schema.
func tryDecorrelated(expr expression.Expression, outerPlan Plan) bool {
	correlated := false
	_, correlatedCols := extractColumn(expr, nil, nil)
	for _, c := range correlatedCols {
		if outerPlan.GetSchema().GetIndex(c) != -1 {
			c.Correlated = false
		} else {
			correlated = true
		}
	}
	return correlated
}

func (b *planBuilder) buildSemiJoin(outerPlan, innerPlan LogicalPlan, onCondition []expression.Expression, asScalar bool, not bool) LogicalPlan {
	joinPlan := &Join{baseLogicalPlan: newBaseLogicalPlan(Jn, b.allocator)}
	joinPlan.initID()
	joinPlan.correlated = outerPlan.IsCorrelated() || innerPlan.IsCorrelated()
	for _, expr := range onCondition {
		joinPlan.correlated = joinPlan.correlated || tryDecorrelated(expr, outerPlan)
	}
	eqCond, leftCond, rightCond, otherCond := extractOnCondition(onCondition, outerPlan, innerPlan)
	joinPlan.EqualConditions = eqCond
	joinPlan.LeftConditions = leftCond
	joinPlan.RightConditions = rightCond
	joinPlan.OtherConditions = otherCond
	if asScalar {
		joinPlan.SetSchema(append(outerPlan.GetSchema().DeepCopy(), &expression.Column{
			FromID:      joinPlan.id,
			ColName:     model.NewCIStr(fmt.Sprintf("%s_aux_0", joinPlan.id)),
			RetType:     types.NewFieldType(mysql.TypeTiny),
			IsAggOrSubq: true,
		}))
		joinPlan.JoinType = SemiJoinWithAux
	} else {
		joinPlan.SetSchema(outerPlan.GetSchema().DeepCopy())
		joinPlan.JoinType = SemiJoin
	}
	joinPlan.anti = not
	joinPlan.SetChildren(outerPlan, innerPlan)
	outerPlan.SetParents(joinPlan)
	innerPlan.SetParents(joinPlan)
	return joinPlan
}
