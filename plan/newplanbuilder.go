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
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// UseNewPlanner means if use the new planner.
var UseNewPlanner = false

func (b *planBuilder) allocID(p Plan) string {
	b.id++
	return fmt.Sprintf("%T_%d", p, b.id)
}

func (b *planBuilder) buildAggregation(p Plan, aggFuncList []*ast.AggregateFuncExpr, gby []expression.Expression, correlated bool) Plan {
	agg := &Aggregation{AggFuncs: make([]expression.AggregationFunction, 0, len(aggFuncList))}
	agg.id = b.allocID(agg)
	agg.correlated = p.IsCorrelated() || correlated
	addChild(agg, p)
	schema := make([]*expression.Column, 0, len(aggFuncList))
	for i, aggFunc := range aggFuncList {
		var newArgList []expression.Expression
		for _, arg := range aggFunc.Args {
			newArg, np, correlated, err := b.rewrite(arg, p, nil)
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
			ColName:  model.NewCIStr(fmt.Sprintf("%s_col_%d", agg.id, i)),
			Position: i})
	}
	agg.GroupByItems = gby
	agg.SetSchema(schema)
	return agg
}

func (b *planBuilder) buildResultSetNode(node ast.ResultSetNode) Plan {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildNewJoin(x)
	case *ast.TableSource:
		var p Plan
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			p = b.buildNewSelect(v)
		case *ast.UnionStmt:
			p = b.buildNewUnion(v)
		case *ast.TableName:
			// TODO: select physical algorithm during cbo phase.
			p = b.buildNewTableScanPlan(v)
		default:
			b.err = ErrUnsupportedType.Gen("unsupported table source type %T", v)
			return nil
		}
		if b.err != nil {
			return nil
		}
		if v, ok := p.(*NewTableScan); ok {
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

func extractColumn(expr expression.Expression, cols []*expression.Column, outerCols []*expression.Column) (result []*expression.Column, outer []*expression.Column) {
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

func extractOnCondition(conditions []expression.Expression, left Plan, right Plan) (
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
					newEq, _ := expression.NewFunction(opcode.EQ, []expression.Expression{rn, ln}, nil)
					eqCond = append(eqCond, newEq)
					continue
				}
			}
		}
		columns, _ := extractColumn(expr, nil, nil)
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if left.GetSchema().GetIndex(col) != -1 {
				allFromRight = false
			} else {
				allFromLeft = false
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

func (b *planBuilder) buildNewJoin(join *ast.Join) Plan {
	if join.Right == nil {
		return b.buildResultSetNode(join.Left)
	}
	leftPlan := b.buildResultSetNode(join.Left)
	rightPlan := b.buildResultSetNode(join.Right)
	newSchema := append(leftPlan.GetSchema().DeepCopy(), rightPlan.GetSchema().DeepCopy()...)
	joinPlan := &Join{}
	joinPlan.id = b.allocID(joinPlan)
	joinPlan.SetSchema(newSchema)
	joinPlan.correlated = leftPlan.IsCorrelated() || rightPlan.IsCorrelated()
	if join.On != nil {
		onExpr, _, correlated, err := b.rewrite(join.On.Expr, joinPlan, nil)
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

func (b *planBuilder) buildSelection(p Plan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) Plan {
	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := &Selection{}
	selection.correlated = p.IsCorrelated()
	for _, cond := range conditions {
		expr, np, correlated, err := b.rewrite(cond, p, AggMapper)
		if err != nil {
			b.err = err
			return nil
		}
		p = np
		selection.correlated = selection.correlated || correlated
		expressions = append(expressions, expr)
	}
	selection.Conditions = expressions
	selection.id = b.allocID(selection)
	selection.SetSchema(p.GetSchema().DeepCopy())
	addChild(selection, p)
	return selection
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *planBuilder) buildProjection(p Plan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) (Plan, int) {
	proj := &Projection{Exprs: make([]expression.Expression, 0, len(fields))}
	proj.id = b.allocID(proj)
	proj.correlated = p.IsCorrelated()
	schema := make(expression.Schema, 0, len(fields))
	oldLen := 0
	for _, field := range fields {
		newExpr, np, correlated, err := b.rewrite(field.Expr, p, mapper)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, oldLen
		}
		p = np
		proj.correlated = proj.correlated || correlated
		proj.Exprs = append(proj.Exprs, newExpr)
		fromID := proj.id
		var tblName, colName model.CIStr
		if field.AsName.L != "" {
			colName = field.AsName
		} else if c, ok := newExpr.(*expression.Column); ok {
			colName = c.ColName
			tblName = c.TblName
		} else {
			colName = model.NewCIStr(field.Expr.Text())
		}
		schemaCol := &expression.Column{
			FromID:  fromID,
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

func (b *planBuilder) buildNewDistinct(src Plan) Plan {
	d := &Distinct{}
	addChild(d, src)
	d.SetSchema(src.GetSchema())
	return d
}

func (b *planBuilder) buildNewUnion(union *ast.UnionStmt) Plan {
	sels := make([]Plan, len(union.SelectList.Selects))
	for i, sel := range union.SelectList.Selects {
		sels[i] = b.buildNewSelect(sel)
	}
	u := &Union{Selects: sels}
	u.id = b.allocID(u)
	firstSchema := sels[0].GetSchema().DeepCopy()
	for _, sel := range sels {
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
		addChild(u, sel)
	}
	for _, v := range firstSchema {
		v.FromID = u.id
		v.DBName = model.NewCIStr("")
	}

	u.SetSchema(firstSchema)
	if union.Distinct {
		return b.buildNewDistinct(u)
	}
	if union.OrderBy != nil {
		return b.buildNewSort(u, union.OrderBy.Items, nil)
	}
	if union.Limit != nil {
		return b.buildNewLimit(u, union.Limit)
	}
	return u
}

// ByItems wraps a "by" item.
type ByItems struct {
	Expr expression.Expression
	Desc bool
}

// NewSort stands for the order by plan.
type NewSort struct {
	basePlan

	ByItems []ByItems
}

func (b *planBuilder) buildNewSort(p Plan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int) Plan {
	var exprs []ByItems
	sort := &NewSort{}
	for _, item := range byItems {
		it, np, correlated, err := b.rewrite(item.Expr, p, aggMapper)
		if err != nil {
			b.err = err
			return nil
		}
		p = np
		sort.correlated = sort.correlated || correlated
		exprs = append(exprs, ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	addChild(sort, p)
	sort.id = b.allocID(sort)
	sort.SetSchema(p.GetSchema().DeepCopy())
	return sort
}

func (b *planBuilder) buildNewLimit(src Plan, limit *ast.Limit) Plan {
	li := &Limit{
		Offset: limit.Offset,
		Count:  limit.Count,
	}
	if s, ok := src.(*Sort); ok {
		s.ExecLimit = li
		return s
	}
	addChild(li, src)
	li.SetSchema(src.GetSchema().DeepCopy())
	return li
}

func (b *planBuilder) extractAggFunc(sel *ast.SelectStmt) (
	[]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int,
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int) {
	extractor := &ast.AggregateFuncExtractor{AggFuncs: make([]*ast.AggregateFuncExpr, 0)}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			b.err = errors.New("Failed to extract agg expr from having clause")
			return nil, nil, nil, nil
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	havingAggFuncs := extractor.AggFuncs
	extractor.AggFuncs = make([]*ast.AggregateFuncExpr, 0)
	havingMapper := make(map[*ast.AggregateFuncExpr]int)
	for _, agg := range havingAggFuncs {
		havingMapper[agg] = len(sel.Fields.Fields)
		field := &ast.SelectField{Expr: agg,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(sel.Fields.Fields))),
			Auxiliary: true}
		sel.Fields.Fields = append(sel.Fields.Fields, field)
	}

	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			_, ok := item.Expr.Accept(extractor)
			if !ok {
				b.err = errors.New("Failed to extract agg expr from orderby clause")
				return nil, nil, nil, nil
			}
		}
	}
	orderByAggFuncs := extractor.AggFuncs
	extractor.AggFuncs = make([]*ast.AggregateFuncExpr, 0)
	orderByMapper := make(map[*ast.AggregateFuncExpr]int)
	for _, agg := range orderByAggFuncs {
		orderByMapper[agg] = len(sel.Fields.Fields)
		field := &ast.SelectField{Expr: agg,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(sel.Fields.Fields))),
			Auxiliary: true}
		sel.Fields.Fields = append(sel.Fields.Fields, field)
	}

	for i, f := range sel.Fields.Fields {
		n, ok := f.Expr.Accept(extractor)
		if !ok {
			b.err = errors.New("Failed to extract agg expr!")
			return nil, nil, nil, nil
		}
		expr, _ := n.(ast.ExprNode)
		sel.Fields.Fields[i].Expr = expr
	}
	aggList := extractor.AggFuncs
	aggList = append(aggList, havingAggFuncs...)
	aggList = append(aggList, orderByAggFuncs...)
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int)

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, havingMapper, orderByMapper, totalAggMapper
}

// havingAndOrderbyResolver resolves having/orderby's ast expression to expression.Expression
type havingAndOrderbyResolver struct {
	proj    *Projection
	inExpr  bool
	orderBy bool
	err     error
	mapper  map[*ast.ColumnNameExpr]expression.Expression
}

func (e *havingAndOrderbyResolver) addProjectionExpr(v *ast.ColumnNameExpr, projCol *expression.Column) {
	e.proj.Exprs = append(e.proj.Exprs, projCol)
	schemaCols, _ := projCol.DeepCopy().(*expression.Column)
	e.mapper[v] = schemaCols
	e.proj.schema = append(e.proj.schema, schemaCols)
}

func (e *havingAndOrderbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.ValueExpr, *ast.ColumnName, *ast.ParenthesesExpr:
	case *ast.ColumnNameExpr:
		var first, second expression.Schema
		var col *expression.Column
		fromSchema := e.proj.GetChildByIndex(0).GetSchema()
		fromSchemaFirst := e.orderBy && e.inExpr
		if fromSchemaFirst {
			first, second = fromSchema, e.proj.GetSchema()
			col, e.err = first.FindColumn(v.Name)
		} else {
			first, second = e.proj.GetSchema(), fromSchema
			col, e.err = first.FindSelectFieldColumn(v.Name, e.proj.Exprs)
		}
		if e.err != nil {
			return inNode, true
		}
		if col == nil {
			if fromSchemaFirst {
				col, e.err = second.FindSelectFieldColumn(v.Name, e.proj.Exprs)
			} else {
				col, e.err = second.FindColumn(v.Name)
			}

			if e.err != nil {
				return inNode, true
			}
			if col == nil {
				e.err = errors.Errorf("Can't find Column %s", v.Name.Name)
				return inNode, true
			}
			if !fromSchemaFirst {
				e.addProjectionExpr(v, col)
			} else {
				e.mapper[v] = col
			}
		} else if fromSchemaFirst {
			e.addProjectionExpr(v, col)
		} else {
			e.mapper[v] = col
		}
	case *ast.SubqueryExpr, *ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr:
		return inNode, true
	default:
		e.inExpr = true
	}
	return inNode, false
}

func (e *havingAndOrderbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	return inNode, true
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

func (g *gbyResolver) match(a *ast.ColumnName, b *ast.ColumnName) bool {
	if a.Schema.L == "" || a.Schema.L == b.Schema.L {
		if a.Table.L == "" || a.Table.L == b.Table.L {
			return a.Name.L == b.Name.L
		}
	}
	return false
}

func (g *gbyResolver) matchField(f *ast.SelectField, col *ast.ColumnNameExpr) bool {
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

func (g *gbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		if col, err := g.schema.FindColumn(v.Name); err != nil {
			g.err = errors.Trace(err)
		} else if col == nil || !g.inExpr {
			var matchedCol ast.ExprNode
			for _, field := range g.fields {
				if g.matchField(field, v) {
					curCol, isCol := field.Expr.(*ast.ColumnNameExpr)
					if !isCol {
						matchedCol = field.Expr
						break
					}
					if matchedCol == nil {
						matchedCol = curCol
					} else if g.match(matchedCol.(*ast.ColumnNameExpr).Name, curCol.Name) {
						g.err = errors.Errorf("Column '%s' in field list is ambiguous", curCol.Name.Name.L)
						return inNode, false
					}
				}
			}
			if matchedCol != nil {
				if col != nil {
					return inNode, true
				}
				return matchedCol, true
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

func (b *planBuilder) resolveGbyExprs(p Plan, gby *ast.GroupByClause, fields []*ast.SelectField) (Plan, bool, []expression.Expression) {
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
		expr, np, cor, err := b.rewrite(retExpr.(ast.ExprNode), p, nil)
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

func (b *planBuilder) unfoldWildStar(p Plan, selectFields []*ast.SelectField) (resultList []*ast.SelectField) {
	for _, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
		} else {
			dbName := field.WildCard.Schema
			tblName := field.WildCard.Table
			for _, col := range p.GetSchema() {
				if (dbName.L == "" || dbName.L == col.DBName.L) &&
					(tblName.L == "" || tblName.L == col.TblName.L) {
					resultList = append(resultList, &ast.SelectField{
						Expr: &ast.ColumnNameExpr{Name: &ast.ColumnName{
							Schema: col.DBName,
							Table:  col.TblName,
							Name:   col.ColName,
						}}})
				}
			}
		}
	}
	return
}

func (b *planBuilder) buildNewSelect(sel *ast.SelectStmt) Plan {
	hasAgg := b.detectSelectAgg(sel)
	var aggFuncs []*ast.AggregateFuncExpr
	var havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
	var p Plan
	var gbyCols []expression.Expression
	var correlated bool
	if sel.From != nil {
		p = b.buildResultSetNode(sel.From.TableRefs)
	} else {
		p = b.buildNewTableDual()
	}
	if b.err != nil {
		return nil
	}
	if sel.Where != nil {
		p = b.buildSelection(p, sel.Where, nil)
		if b.err != nil {
			return nil
		}
	}
	if sel.LockTp != ast.SelectLockNone {
		p = b.buildSelectLock(p, sel.LockTp)
		if b.err != nil {
			return nil
		}
	}
	sel.Fields.Fields = b.unfoldWildStar(p, sel.Fields.Fields)
	if hasAgg {
		if sel.GroupBy != nil {
			p, correlated, gbyCols = b.resolveGbyExprs(p, sel.GroupBy, sel.Fields.Fields)
			if b.err != nil {
				return nil
			}
		}
		aggFuncs, havingMap, orderMap, totalMap = b.extractAggFunc(sel)
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
	resolver := &havingAndOrderbyResolver{proj: p.(*Projection), mapper: b.colMapper}
	if sel.Having != nil && !hasAgg {
		sel.Having.Expr.Accept(resolver)
		if resolver.err != nil {
			b.err = errors.Trace(resolver.err)
			return nil
		}
	}
	resolver.orderBy = true
	if sel.OrderBy != nil && !hasAgg {
		for _, item := range sel.OrderBy.Items {
			resolver.inExpr = false
			item.Expr.Accept(resolver)
			if resolver.err != nil {
				b.err = errors.Trace(resolver.err)
				return nil
			}
		}
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
	// TODO: implement push order during cbo
	if sel.OrderBy != nil {
		p = b.buildNewSort(p, sel.OrderBy.Items, orderMap)
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

func (b *planBuilder) buildTrim(p Plan, len int) Plan {
	trunc := &Trim{}
	trunc.id = b.allocID(trunc)
	addChild(trunc, p)
	trunc.SetSchema(p.GetSchema().DeepCopy()[:len])
	trunc.correlated = p.IsCorrelated()
	return trunc
}

func (b *planBuilder) buildNewTableDual() Plan {
	dual := new(NewTableDual)
	dual.id = b.allocID(dual)
	schema := []*expression.Column{{FromID: dual.id}}
	dual.SetSchema(schema)
	return dual
}

func (b *planBuilder) buildNewTableScanPlan(tn *ast.TableName) Plan {
	p := &NewTableScan{Table: tn.TableInfo}
	p.id = b.allocID(p)
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

func (b *planBuilder) buildApply(p, inner Plan, schema expression.Schema) Plan {
	ap := &Apply{
		InnerPlan:   inner,
		OuterSchema: schema,
	}
	ap.id = b.allocID(ap)
	addChild(ap, p)
	innerSchema := inner.GetSchema().DeepCopy()
	ap.SetSchema(append(p.GetSchema().DeepCopy(), innerSchema...))
	ap.correlated = p.IsCorrelated()
	return ap
}

func (b *planBuilder) buildExists(p Plan) Plan {
	exists := &Exists{}
	exists.id = b.allocID(exists)
	addChild(exists, p)
	newCol := &expression.Column{
		FromID:  exists.id,
		RetType: types.NewFieldType(mysql.TypeTiny),
		ColName: model.NewCIStr("exists_col")}
	exists.SetSchema([]*expression.Column{newCol})
	exists.correlated = p.IsCorrelated()
	return exists
}

func (b *planBuilder) buildMaxOneRow(p Plan) Plan {
	maxOneRow := &MaxOneRow{}
	maxOneRow.id = b.allocID(maxOneRow)
	addChild(maxOneRow, p)
	maxOneRow.SetSchema(p.GetSchema().DeepCopy())
	maxOneRow.correlated = p.IsCorrelated()
	return maxOneRow
}
