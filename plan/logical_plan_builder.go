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

type idAllocator struct {
	id int
}

func (a *idAllocator) allocID() string {
	a.id++
	return fmt.Sprintf("_%d", a.id)
}

func (p *Aggregation) collectGroupByColumns() {
	p.groupByCols = p.groupByCols[:0]
	for _, item := range p.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			p.groupByCols = append(p.groupByCols, col)
		}
	}
}

func (b *planBuilder) buildAggregation(p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression) (LogicalPlan, map[int]int) {
	agg := &Aggregation{
		AggFuncs:        make([]expression.AggregationFunction, 0, len(aggFuncList)),
		ctx:             b.ctx,
		baseLogicalPlan: newBaseLogicalPlan(Agg, b.allocator)}
	agg.self = agg
	agg.initID()
	agg.correlated = p.IsCorrelated()
	for _, item := range gbyItems {
		agg.correlated = agg.correlated || item.IsCorrelated()
	}
	addChild(agg, p)
	schema := make(expression.Schema, 0, len(aggFuncList))
	// aggIdxMap maps the old index to new index after applying common aggregation functions elimination.
	aggIndexMap := make(map[int]int)
	for i, aggFunc := range aggFuncList {
		var newArgList []expression.Expression
		for _, arg := range aggFunc.Args {
			newArg, np, err := b.rewrite(arg, p, nil, true)
			if err != nil {
				b.err = errors.Trace(err)
				return nil, nil
			}
			p = np
			agg.correlated = agg.correlated || newArg.IsCorrelated()
			newArgList = append(newArgList, newArg)
		}
		newFunc := expression.NewAggFunction(aggFunc.F, newArgList, aggFunc.Distinct)
		combined := false
		for j, oldFunc := range agg.AggFuncs {
			if oldFunc.Equal(newFunc) {
				aggIndexMap[i] = j
				combined = true
				break
			}
		}
		if !combined {
			position := len(agg.AggFuncs)
			aggIndexMap[i] = position
			agg.AggFuncs = append(agg.AggFuncs, newFunc)
			schema = append(schema, &expression.Column{
				FromID:      agg.id,
				ColName:     model.NewCIStr(fmt.Sprintf("%s_col_%d", agg.id, position)),
				Position:    position,
				IsAggOrSubq: true,
				RetType:     aggFunc.GetType()})
		}
	}
	agg.GroupByItems = gbyItems
	agg.SetSchema(schema)
	agg.collectGroupByColumns()
	return agg, aggIndexMap
}

func (b *planBuilder) buildResultSetNode(node ast.ResultSetNode) LogicalPlan {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(x)
	case *ast.TableSource:
		var p LogicalPlan
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			p = b.buildSelect(v)
		case *ast.UnionStmt:
			p = b.buildUnion(v)
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
		return b.buildSelect(x)
	case *ast.UnionStmt:
		return b.buildUnion(x)
	default:
		b.err = ErrUnsupportedType.Gen("unsupported table source type %T", x)
		return nil
	}
}

func extractColumn(expr expression.Expression, cols []*expression.Column, corCols []*expression.CorrelatedColumn) (
	[]*expression.Column, []*expression.CorrelatedColumn) {
	switch v := expr.(type) {
	case *expression.Column:
		return append(cols, v), corCols
	case *expression.ScalarFunction:
		for _, arg := range v.Args {
			cols, corCols = extractColumn(arg, cols, corCols)
		}
		return cols, corCols
	case *expression.CorrelatedColumn:
		return cols, append(corCols, v)
	}
	return cols, corCols
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

func (b *planBuilder) buildJoin(join *ast.Join) LogicalPlan {
	if join.Right == nil {
		return b.buildResultSetNode(join.Left)
	}
	leftPlan := b.buildResultSetNode(join.Left)
	rightPlan := b.buildResultSetNode(join.Right)
	newSchema := append(leftPlan.GetSchema().Clone(), rightPlan.GetSchema().Clone()...)
	joinPlan := &Join{baseLogicalPlan: newBaseLogicalPlan(Jn, b.allocator)}
	joinPlan.self = joinPlan
	joinPlan.initID()
	joinPlan.SetSchema(newSchema)
	joinPlan.correlated = leftPlan.IsCorrelated() || rightPlan.IsCorrelated()
	if join.On != nil {
		onExpr, _, err := b.rewrite(join.On.Expr, joinPlan, nil, false)
		if err != nil {
			b.err = err
			return nil
		}
		if onExpr.IsCorrelated() {
			b.err = errors.New("ON condition doesn't support subqueries yet")
		}
		onCondition := expression.SplitCNFItems(onExpr)
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
		joinPlan.DefaultValues = make([]types.Datum, len(rightPlan.GetSchema()))
	} else if join.Tp == ast.RightJoin {
		joinPlan.JoinType = RightOuterJoin
		joinPlan.DefaultValues = make([]types.Datum, len(leftPlan.GetSchema()))
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
	selection.self = selection
	selection.initID()
	selection.correlated = p.IsCorrelated()
	for _, cond := range conditions {
		expr, np, err := b.rewrite(cond, p, AggMapper, false)
		if err != nil {
			b.err = err
			return nil
		}
		p = np
		if expr == nil {
			continue
		}
		expressions = append(expressions, expression.SplitCNFItems(expr)...)
		selection.correlated = selection.correlated || expr.IsCorrelated()
	}
	if len(expressions) == 0 {
		return p
	}
	selection.Conditions = expressions
	selection.SetSchema(p.GetSchema().Clone())
	addChild(selection, p)
	return selection
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *planBuilder) buildProjection(p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, int) {
	proj := &Projection{
		Exprs:           make([]expression.Expression, 0, len(fields)),
		baseLogicalPlan: newBaseLogicalPlan(Proj, b.allocator),
	}
	proj.self = proj
	proj.initID()
	proj.correlated = p.IsCorrelated()
	schema := make(expression.Schema, 0, len(fields))
	oldLen := 0
	for _, field := range fields {
		newExpr, np, err := b.rewrite(field.Expr, p, mapper, true)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, oldLen
		}
		p = np
		proj.correlated = proj.correlated || newExpr.IsCorrelated()
		proj.Exprs = append(proj.Exprs, newExpr)
		var tblName, colName model.CIStr
		if field.AsName.L != "" {
			colName = field.AsName
		} else if c, ok := newExpr.(*expression.Column); ok && !c.IsAggOrSubq {
			if astCol, ok := getInnerFromParentheses(field.Expr).(*ast.ColumnNameExpr); ok {
				colName = astCol.Name.Name
				tblName = astCol.Name.Table
			} else {
				colName = c.ColName
				tblName = c.TblName
			}
		} else {
			// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
			if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
				if col, ok := agg.Args[0].(*ast.ColumnNameExpr); ok {
					colName = col.Name.Name
				}
			} else {
				innerExpr := getInnerFromParentheses(field.Expr)
				if _, ok := innerExpr.(*ast.ValueExpr); ok && innerExpr.Text() != "" {
					colName = model.NewCIStr(innerExpr.Text())
				} else {
					colName = model.NewCIStr(field.Text())
				}
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

func (b *planBuilder) buildDistinct(src LogicalPlan) LogicalPlan {
	d := &Distinct{baseLogicalPlan: newBaseLogicalPlan(Dis, b.allocator)}
	d.self = d
	d.initID()
	addChild(d, src)
	d.SetSchema(src.GetSchema())
	d.correlated = src.IsCorrelated()
	return d
}

func (b *planBuilder) buildUnion(union *ast.UnionStmt) LogicalPlan {
	u := &Union{baseLogicalPlan: newBaseLogicalPlan(Un, b.allocator)}
	u.self = u
	u.initID()
	u.children = make([]Plan, len(union.SelectList.Selects))
	for i, sel := range union.SelectList.Selects {
		u.children[i] = b.buildSelect(sel)
		u.correlated = u.correlated || u.children[i].IsCorrelated()
	}
	firstSchema := u.children[0].GetSchema().Clone()
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
		p = b.buildDistinct(u)
	}
	if union.OrderBy != nil {
		p = b.buildSort(p, union.OrderBy.Items, nil)
	}
	if union.Limit != nil {
		p = b.buildLimit(p, union.Limit)
	}
	return p
}

// ByItems wraps a "by" item.
type ByItems struct {
	Expr expression.Expression
	Desc bool
}

// String implements fmt.Stringer interface.
func (by *ByItems) String() string {
	return fmt.Sprintf("(%s, %v)", by.Expr, by.Desc)
}

func (b *planBuilder) buildSort(p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int) LogicalPlan {
	var exprs []*ByItems
	sort := &Sort{baseLogicalPlan: newBaseLogicalPlan(Srt, b.allocator)}
	sort.self = sort
	sort.initID()
	sort.correlated = p.IsCorrelated()
	for _, item := range byItems {
		it, np, err := b.rewrite(item.Expr, p, aggMapper, true)
		if err != nil {
			b.err = err
			return nil
		}
		p = np
		sort.correlated = sort.correlated || it.IsCorrelated()
		exprs = append(exprs, &ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	addChild(sort, p)
	sort.SetSchema(p.GetSchema().Clone())
	return sort
}

func (b *planBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) LogicalPlan {
	li := &Limit{
		Offset:          limit.Offset,
		Count:           limit.Count,
		baseLogicalPlan: newBaseLogicalPlan(Lim, b.allocator),
	}
	li.self = li
	li.initID()
	li.correlated = src.IsCorrelated()
	addChild(li, src)
	li.SetSchema(src.GetSchema().Clone())
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

func matchField(f *ast.SelectField, col *ast.ColumnNameExpr, ignoreAsName bool) bool {
	// if col specify a table name, resolve from table source directly.
	if col.Name.Table.L == "" {
		if f.AsName.L == "" || ignoreAsName {
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

func resolveFromSelectFields(v *ast.ColumnNameExpr, fields []*ast.SelectField, ignoreAsName bool) (index int, err error) {
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		if field.Auxiliary {
			continue
		}
		if matchField(field, v, ignoreAsName) {
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
type havingAndOrderbyExprResolver struct {
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
func (a *havingAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
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

func (a *havingAndOrderbyExprResolver) resolveFromSchema(v *ast.ColumnNameExpr, schema expression.Schema) (int, error) {
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
func (a *havingAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
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
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok &&
					(colMatch(v.Name, col.Name) || colMatch(col.Name, v.Name)) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		index := -1
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				return node, false
			}
			if index == -1 {
				if a.orderBy {
					index, a.err = a.resolveFromSchema(v, a.p.GetSchema())
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			index, a.err = a.resolveFromSchema(v, a.p.GetSchema())
			if a.err != nil {
				return node, false
			}
			if index == -1 {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
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
	extractor := &havingAndOrderbyExprResolver{
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
		col, err := g.schema.FindColumn(v.Name)
		if col == nil || !g.inExpr {
			var index = -1
			index, g.err = resolveFromSelectFields(v, g.fields, false)
			if g.err != nil {
				return inNode, false
			}
			if col != nil {
				return inNode, true
			}
			if index != -1 {
				return g.fields[index].Expr, true
			}
			g.err = errors.Trace(err)
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

func (b *planBuilder) resolveGbyExprs(p LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (LogicalPlan, []expression.Expression) {
	exprs := make([]expression.Expression, 0, len(gby.Items))
	resolver := &gbyResolver{fields: fields, schema: p.GetSchema()}
	for _, item := range gby.Items {
		resolver.inExpr = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			b.err = errors.Trace(resolver.err)
			return nil, nil
		}
		item.Expr = retExpr.(ast.ExprNode)
		expr, np, err := b.rewrite(item.Expr, p, nil, true)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, nil
		}
		exprs = append(exprs, expr)
		p = np
	}
	return p, exprs
}

func (b *planBuilder) unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField) {
	for i, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		if field.WildCard.Table.L == "" && i > 0 {
			b.err = ErrInvalidWildCard
			return
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

func (b *planBuilder) buildSelect(sel *ast.SelectStmt) LogicalPlan {
	hasAgg := b.detectSelectAgg(sel)
	var (
		p                             LogicalPlan
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		gbyCols                       []expression.Expression
	)
	if sel.From != nil {
		p = b.buildResultSetNode(sel.From.TableRefs)
	} else {
		p = b.buildTableDual()
	}
	if b.err != nil {
		return nil
	}
	sel.Fields.Fields = b.unfoldWildStar(p, sel.Fields.Fields)
	if b.err != nil {
		return nil
	}
	if sel.GroupBy != nil {
		p, gbyCols = b.resolveGbyExprs(p, sel.GroupBy, sel.Fields.Fields)
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
	if sel.LockTp != ast.SelectLockNone {
		p = b.buildSelectLock(p, sel.LockTp)
	}
	if hasAgg {
		aggFuncs, totalMap = b.extractAggFuncs(sel.Fields.Fields)
		if b.err != nil {
			return nil
		}
		var aggIndexMap map[int]int
		p, aggIndexMap = b.buildAggregation(p, aggFuncs, gbyCols)
		for k, v := range totalMap {
			totalMap[k] = aggIndexMap[v]
		}
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
		p = b.buildDistinct(p)
		if b.err != nil {
			return nil
		}
	}
	if sel.OrderBy != nil {
		p = b.buildSort(p, sel.OrderBy.Items, orderMap)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		p = b.buildLimit(p, sel.Limit)
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
	trim.self = trim
	trim.initID()
	addChild(trim, p)
	trim.SetSchema(p.GetSchema().Clone()[:len])
	trim.correlated = p.IsCorrelated()
	return trim
}

func (b *planBuilder) buildTableDual() LogicalPlan {
	dual := &TableDual{baseLogicalPlan: newBaseLogicalPlan(Dual, b.allocator)}
	dual.self = dual
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
		ctx:             b.ctx,
		table:           tn,
		Table:           tn.TableInfo,
		baseLogicalPlan: newBaseLogicalPlan(Ts, b.allocator),
		statisticTable:  statisticTable,
	}
	p.self = p
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
			Position: i,
			ID:       rf.Column.ID})
	}
	p.SetSchema(schema)
	return p
}

// ApplyConditionChecker checks whether all or any output of apply matches a condition.
type ApplyConditionChecker struct {
	Condition expression.Expression
	All       bool
}

// buildApply builds apply plan with outerPlan and innerPlan. Everytime we fetch a record from outerPlan and apply it to
// innerPlan. This way is the so-called correlated execution.
func (b *planBuilder) buildApply(outerPlan, innerPlan LogicalPlan, checker *ApplyConditionChecker) LogicalPlan {
	ap := &Apply{
		InnerPlan:        innerPlan,
		Checker:          checker,
		corColsInCurPlan: make([]*expression.CorrelatedColumn, len(outerPlan.GetSchema())),
		baseLogicalPlan:  newBaseLogicalPlan(App, b.allocator),
	}
	ap.self = ap
	ap.initID()
	addChild(ap, outerPlan)
	_, innerPlan, b.err = innerPlan.PredicatePushDown(nil)
	if b.err != nil {
		return nil
	}
	corColumns, err := innerPlan.PruneColumnsAndResolveIndices(innerPlan.GetSchema())
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	for _, corCol := range corColumns {
		// If the outer column can't be resolved from this outer schema, it should be resolved by outer schema.
		if idx := outerPlan.GetSchema().GetIndex(&corCol.Column); idx == -1 {
			ap.corColsInOuterPlan = append(ap.corColsInOuterPlan, corCol)
		} else {
			if ap.corColsInCurPlan[idx] == nil {
				ap.corColsInCurPlan[idx] = &expression.CorrelatedColumn{
					Column: *outerPlan.GetSchema()[idx],
					Data:   &types.Datum{},
				}
			}
			corCol.Data = ap.corColsInCurPlan[idx].Data
		}
	}
	for i := len(ap.corColsInCurPlan) - 1; i >= 0; i-- {
		if ap.corColsInCurPlan[i] == nil {
			ap.corColsInCurPlan = append(ap.corColsInCurPlan[:i], ap.corColsInCurPlan[i+1:]...)
		}
	}
	innerSchema := innerPlan.GetSchema().Clone()
	if checker == nil {
		for _, col := range innerSchema {
			col.IsAggOrSubq = true
		}
		ap.SetSchema(append(outerPlan.GetSchema().Clone(), innerSchema...))
	} else {
		ap.SetSchema(append(outerPlan.GetSchema().Clone(), &expression.Column{
			FromID:      ap.id,
			ColName:     model.NewCIStr("exists_row"),
			RetType:     types.NewFieldType(mysql.TypeTiny),
			IsAggOrSubq: true,
		}))
	}
	ap.correlated = outerPlan.IsCorrelated() || len(ap.corColsInOuterPlan) > 0
	return ap
}

func (b *planBuilder) buildExists(p LogicalPlan) LogicalPlan {
out:
	for {
		switch p.(type) {
		// This can be removed when in exists clause,
		// e.g. exists(select count(*) from t order by a) is equal to exists t.
		case *Trim, *Projection, *Sort, *Aggregation:
			p = p.GetChildByIndex(0).(LogicalPlan)
			p.SetParents()
		default:
			break out
		}
	}
	exists := &Exists{baseLogicalPlan: newBaseLogicalPlan(Ext, b.allocator)}
	exists.self = exists
	exists.initID()
	addChild(exists, p)
	newCol := &expression.Column{
		FromID:  exists.id,
		RetType: types.NewFieldType(mysql.TypeTiny),
		ColName: model.NewCIStr("exists_col")}
	exists.SetSchema(expression.Schema{newCol})
	exists.correlated = p.IsCorrelated()
	return exists
}

func (b *planBuilder) buildMaxOneRow(p LogicalPlan) LogicalPlan {
	maxOneRow := &MaxOneRow{baseLogicalPlan: newBaseLogicalPlan(MOR, b.allocator)}
	maxOneRow.self = maxOneRow
	maxOneRow.initID()
	addChild(maxOneRow, p)
	maxOneRow.SetSchema(p.GetSchema().Clone())
	maxOneRow.correlated = p.IsCorrelated()
	return maxOneRow
}

func (b *planBuilder) buildSemiJoin(outerPlan, innerPlan LogicalPlan, onCondition []expression.Expression, asScalar bool, not bool) LogicalPlan {
	joinPlan := &Join{baseLogicalPlan: newBaseLogicalPlan(Jn, b.allocator)}
	joinPlan.self = joinPlan
	joinPlan.initID()
	joinPlan.correlated = outerPlan.IsCorrelated() || innerPlan.IsCorrelated()
	for i, expr := range onCondition {
		onCondition[i] = expr.Decorrelate(outerPlan.GetSchema())
		joinPlan.correlated = joinPlan.correlated || onCondition[i].IsCorrelated()
	}
	eqCond, leftCond, rightCond, otherCond := extractOnCondition(onCondition, outerPlan, innerPlan)
	joinPlan.EqualConditions = eqCond
	joinPlan.LeftConditions = leftCond
	joinPlan.RightConditions = rightCond
	joinPlan.OtherConditions = otherCond
	if asScalar {
		joinPlan.SetSchema(append(outerPlan.GetSchema().Clone(), &expression.Column{
			FromID:      joinPlan.id,
			ColName:     model.NewCIStr(fmt.Sprintf("%s_aux_0", joinPlan.id)),
			RetType:     types.NewFieldType(mysql.TypeTiny),
			IsAggOrSubq: true,
		}))
		joinPlan.JoinType = SemiJoinWithAux
	} else {
		joinPlan.SetSchema(outerPlan.GetSchema().Clone())
		joinPlan.JoinType = SemiJoin
	}
	joinPlan.anti = not
	joinPlan.SetChildren(outerPlan, innerPlan)
	outerPlan.SetParents(joinPlan)
	innerPlan.SetParents(joinPlan)
	return joinPlan
}

func (b *planBuilder) buildUpdate(update *ast.UpdateStmt) LogicalPlan {
	sel := &ast.SelectStmt{Fields: &ast.FieldList{}, From: update.TableRefs, Where: update.Where, OrderBy: update.Order, Limit: update.Limit}
	p := b.buildResultSetNode(sel.From.TableRefs)
	if b.err != nil {
		return nil
	}
	_, _ = b.resolveHavingAndOrderBy(sel, p)
	if sel.Where != nil {
		p = b.buildSelection(p, sel.Where, nil)
		if b.err != nil {
			return nil
		}
	}
	if sel.OrderBy != nil {
		p = b.buildSort(p, sel.OrderBy.Items, nil)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		p = b.buildLimit(p, sel.Limit)
		if b.err != nil {
			return nil
		}
	}
	orderedList, np := b.buildUpdateLists(update.List, p)
	if b.err != nil {
		return nil
	}
	p = np
	updt := &Update{OrderedList: orderedList, baseLogicalPlan: newBaseLogicalPlan(Up, b.allocator)}
	updt.self = updt
	updt.initID()
	addChild(updt, p)
	updt.SetSchema(p.GetSchema())
	return updt
}

func (b *planBuilder) buildUpdateLists(list []*ast.Assignment, p LogicalPlan) ([]*expression.Assignment, LogicalPlan) {
	schema := p.GetSchema()
	newList := make([]*expression.Assignment, len(schema))
	for _, assign := range list {
		col, err := schema.FindColumn(assign.Column)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, nil
		}
		if col == nil {
			b.err = errors.Trace(errors.Errorf("column %s not found", assign.Column.Name.O))
			return nil, nil
		}
		offset := schema.GetIndex(col)
		if offset == -1 {
			b.err = errors.Trace(errors.Errorf("could not find column %s.%s", col.TblName, col.ColName))
		}
		newExpr, np, err := b.rewrite(assign.Expr, p, nil, false)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, nil
		}
		p = np
		newList[offset] = &expression.Assignment{Col: col, Expr: newExpr}
	}
	return newList, p
}

func (b *planBuilder) buildDelete(delete *ast.DeleteStmt) LogicalPlan {
	sel := &ast.SelectStmt{Fields: &ast.FieldList{}, From: delete.TableRefs, Where: delete.Where, OrderBy: delete.Order, Limit: delete.Limit}
	p := b.buildResultSetNode(sel.From.TableRefs)
	if b.err != nil {
		return nil
	}
	_, _ = b.resolveHavingAndOrderBy(sel, p)
	if sel.Where != nil {
		p = b.buildSelection(p, sel.Where, nil)
		if b.err != nil {
			return nil
		}
	}
	if sel.OrderBy != nil {
		p = b.buildSort(p, sel.OrderBy.Items, nil)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		p = b.buildLimit(p, sel.Limit)
		if b.err != nil {
			return nil
		}
	}
	var tables []*ast.TableName
	if delete.Tables != nil {
		tables = delete.Tables.Tables
	}
	del := &Delete{
		Tables:          tables,
		IsMultiTable:    delete.IsMultiTable,
		baseLogicalPlan: newBaseLogicalPlan(Del, b.allocator),
	}
	del.self = del
	del.initID()
	addChild(del, p)
	return del

}
