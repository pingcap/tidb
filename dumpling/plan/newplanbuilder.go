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
)

// UseNewPlanner means if use the new planner.
var UseNewPlanner = false

func (b *planBuilder) allocID(p Plan) string {
	b.id++
	return fmt.Sprintf("%T_%d", p, b.id)
}

func (b *planBuilder) buildAggregation(p Plan, aggFuncList []*ast.AggregateFuncExpr, gby *ast.GroupByClause) Plan {
	newAggFuncList := make([]expression.AggregationFunction, 0, len(aggFuncList))
	agg := &Aggregation{}
	agg.id = b.allocID(agg)
	addChild(agg, p)
	schema := make([]*expression.Column, 0, len(aggFuncList))
	for i, aggFunc := range aggFuncList {
		var newArgList []expression.Expression
		for _, arg := range aggFunc.Args {
			newArg, err := b.rewrite(arg, p.GetSchema(), nil)
			if err != nil {
				b.err = errors.Trace(err)
				return nil
			}
			newArgList = append(newArgList, newArg)
		}
		newAggFuncList = append(newAggFuncList, expression.NewAggFunction(aggFunc.F, newArgList, aggFunc.Distinct))
		schema = append(schema, &expression.Column{FromID: agg.id, ColName: model.NewCIStr(fmt.Sprintf("%s_col_%d", agg.id, i))})
	}
	var gbyExprList []expression.Expression
	if gby != nil {
		gbyExprList = make([]expression.Expression, 0, len(gby.Items))
		for _, gbyItem := range gby.Items {
			gbyExpr, err := b.rewrite(gbyItem.Expr, p.GetSchema(), nil)
			if err != nil {
				b.err = errors.Trace(err)
				return nil
			}
			gbyExprList = append(gbyExprList, gbyExpr)
		}
	}
	agg.AggFuncs = newAggFuncList
	agg.GroupByItems = gbyExprList
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
	default:
		b.err = ErrUnsupportedType.Gen("unsupported table source type %T", x)
		return nil
	}
}

func extractColumn(expr expression.Expression, cols []*expression.Column) (result []*expression.Column) {
	switch v := expr.(type) {
	case *expression.Column:
		return append(cols, v)
	case *expression.ScalarFunction:
		for _, arg := range v.Args {
			cols = extractColumn(arg, cols)
		}
		return cols
	}
	return cols
}

func extractOnCondition(conditions []expression.Expression, left Plan, right Plan) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression, rightCond []expression.Expression, otherCond []expression.Expression) {
	for _, expr := range conditions {
		binop, ok := expr.(*expression.ScalarFunction)
		eqStr, _ := opcode.Ops[opcode.EQ]
		if ok && binop.FuncName.L == eqStr {
			ln, lOK := binop.Args[0].(*expression.Column)
			rn, rOK := binop.Args[1].(*expression.Column)
			if lOK && rOK {
				if left.GetSchema().GetIndex(ln) != -1 && right.GetSchema().GetIndex(rn) != -1 {
					eqCond = append(eqCond, binop)
					continue
				} else if left.GetSchema().GetIndex(rn) != -1 && right.GetSchema().GetIndex(ln) != -1 {
					newEq := expression.NewFunction(model.NewCIStr(eqStr), []expression.Expression{rn, ln})
					eqCond = append(eqCond, newEq)
					continue
				}
			}
		}
		columns := extractColumn(expr, make([]*expression.Column, 0))
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
		andandStr, _ := opcode.Ops[opcode.AndAnd]
		if v.FuncName.L == andandStr {
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
	var eqCond []*expression.ScalarFunction
	var leftCond, rightCond, otherCond []expression.Expression
	if join.On != nil {
		onExpr, err := b.rewrite(join.On.Expr, newSchema, nil)
		if err != nil {
			b.err = err
			return nil
		}
		onCondition := splitCNFItems(onExpr)
		eqCond, leftCond, rightCond, otherCond = extractOnCondition(onCondition, leftPlan, rightPlan)
	}
	joinPlan := &Join{EqualConditions: eqCond, LeftConditions: leftCond, RightConditions: rightCond, OtherConditions: otherCond}
	if join.Tp == ast.LeftJoin {
		joinPlan.JoinType = LeftOuterJoin
	} else if join.Tp == ast.RightJoin {
		joinPlan.JoinType = RightOuterJoin
	} else {
		joinPlan.JoinType = InnerJoin
	}
	addChild(joinPlan, leftPlan)
	addChild(joinPlan, rightPlan)
	joinPlan.SetSchema(newSchema)
	return joinPlan
}

func (b *planBuilder) buildSelection(p Plan, where ast.ExprNode, mapper map[*ast.AggregateFuncExpr]int) Plan {
	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	for _, cond := range conditions {
		expr, err := b.rewrite(cond, p.GetSchema(), mapper)
		if err != nil {
			b.err = err
			return nil
		}
		expressions = append(expressions, expr)
	}
	selection := &Selection{Conditions: expressions}
	selection.id = b.allocID(selection)
	selection.SetSchema(p.GetSchema().DeepCopy())
	addChild(selection, p)
	return selection
}

func (b *planBuilder) buildProjection(src Plan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) Plan {
	proj := &Projection{Exprs: make([]expression.Expression, 0, len(fields))}
	proj.id = b.allocID(proj)
	schema := make(expression.Schema, 0, len(fields))
	for _, field := range fields {
		var tblName, colName model.CIStr
		if field.WildCard != nil {
			dbName := field.WildCard.Schema
			colTblName := field.WildCard.Table
			for _, col := range src.GetSchema() {
				if (dbName.L == "" || dbName.L == col.DBName.L) &&
					(colTblName.L == "" || colTblName.L == col.TblName.L) {
					newExpr := col.DeepCopy()
					proj.Exprs = append(proj.Exprs, newExpr)
					tblName = col.TblName
					schemaCol := &expression.Column{FromID: col.FromID, TblName: tblName, ColName: col.ColName, RetType: newExpr.GetType()}
					schema = append(schema, schemaCol)
				}
			}
		} else {
			newExpr, err := b.rewrite(field.Expr, src.GetSchema(), mapper)
			if err != nil {
				b.err = errors.Trace(err)
				return nil
			}
			proj.Exprs = append(proj.Exprs, newExpr)
			var fromID string
			if field.AsName.L != "" {
				colName = field.AsName
				fromID = proj.id
			} else if c, ok := newExpr.(*expression.Column); ok {
				colName = c.ColName
				tblName = c.TblName
				fromID = c.FromID
			} else {
				colName = model.NewCIStr(field.Expr.Text())
				fromID = proj.id
			}
			schemaCol := &expression.Column{FromID: fromID, TblName: tblName, ColName: colName, RetType: newExpr.GetType()}
			schema = append(schema, schemaCol)
		}
	}
	proj.SetSchema(schema)
	addChild(proj, src)
	return proj
}

func (b *planBuilder) buildNewDistinct(src Plan) Plan {
	d := &Distinct{}
	addChild(d, src)
	d.SetSchema(src.GetSchema())
	return d
}

func (b *planBuilder) buildNewUnion(union *ast.UnionStmt) (p Plan) {
	sels := make([]Plan, len(union.SelectList.Selects))
	for i, sel := range union.SelectList.Selects {
		sels[i] = b.buildNewSelect(sel)
	}
	u := &Union{
		Selects: sels,
	}
	u.id = b.allocID(u)
	p = u
	firstSchema := make(expression.Schema, 0, len(sels[0].GetSchema()))
	firstSchema = append(firstSchema, sels[0].GetSchema()...)
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
		addChild(p, sel)
	}
	for _, v := range firstSchema {
		v.FromID = u.id
		v.DBName = model.NewCIStr("")
	}

	p.SetSchema(firstSchema)
	if union.Distinct {
		p = b.buildNewDistinct(p)
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

// NewSort stands for the order by plan.
type NewSort struct {
	basePlan

	ByItems []ByItems
}

func (b *planBuilder) buildNewSort(src Plan, byItems []*ast.ByItem, mapper map[*ast.AggregateFuncExpr]int) Plan {
	var exprs []ByItems
	for _, item := range byItems {
		it, err := b.rewrite(item.Expr, src.GetSchema(), mapper)
		if err != nil {
			b.err = err
		}
		exprs = append(exprs, ByItems{Expr: it, Desc: item.Desc})
	}
	sort := &NewSort{
		ByItems: exprs,
	}
	addChild(sort, src)
	sort.id = b.allocID(sort)
	sort.SetSchema(src.GetSchema().DeepCopy())
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

func (b *planBuilder) extractAggFunc(sel *ast.SelectStmt) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int) {
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
			AsName: model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(sel.Fields.Fields)))}
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
			// TODO: support position expression.
		}
	}
	orderByAggFuncs := extractor.AggFuncs
	extractor.AggFuncs = make([]*ast.AggregateFuncExpr, 0)
	orderByMapper := make(map[*ast.AggregateFuncExpr]int)
	for _, agg := range orderByAggFuncs {
		orderByMapper[agg] = len(sel.Fields.Fields)
		field := &ast.SelectField{Expr: agg,
			AsName: model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(sel.Fields.Fields)))}
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

func (b *planBuilder) buildNewSelect(sel *ast.SelectStmt) Plan {
	oldLen := len(sel.Fields.Fields)
	hasAgg := b.detectSelectAgg(sel)
	var aggFuncs []*ast.AggregateFuncExpr
	var havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
	if hasAgg {
		aggFuncs, havingMap, orderMap, totalMap = b.extractAggFunc(sel)
	}
	// Build subquery
	// Convert subquery to expr with plan
	// TODO: add subquery support.
	//b.buildSubquery(sel)
	var p Plan
	if sel.From != nil {
		p = b.buildResultSetNode(sel.From.TableRefs)
		if b.err != nil {
			return nil
		}
		if sel.Where != nil {
			p = b.buildSelection(p, sel.Where, nil)
		}
		if b.err != nil {
			return nil
		}
		if sel.LockTp != ast.SelectLockNone {
			p = b.buildSelectLock(p, sel.LockTp)
			if b.err != nil {
				return nil
			}
		}
		if hasAgg {
			p = b.buildAggregation(p, aggFuncs, sel.GroupBy)
		}
	} else {
		if sel.Where != nil {
			p = b.buildTableDual(sel)
		}
		if hasAgg {
			p = b.buildAggregation(p, aggFuncs, nil)
		}
	}
	p = b.buildProjection(p, sel.Fields.Fields, totalMap)
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
	if oldLen != len(sel.Fields.Fields) {
		proj := &Projection{}
		proj.id = b.allocID(proj)
		var newSchema expression.Schema
		oldSchema := p.GetSchema()
		proj.Exprs = make([]expression.Expression, 0, oldLen)
		for _, col := range oldSchema[:oldLen] {
			proj.Exprs = append(proj.Exprs, col)
		}
		newSchema = oldSchema[:oldLen]
		newSchema = newSchema.DeepCopy()
		for _, s := range newSchema {
			s.FromID = proj.id
		}
		proj.SetSchema(newSchema)
		addChild(proj, p)
		return proj
	}
	return p
}

func (b *planBuilder) buildNewTableScanPlan(tn *ast.TableName) Plan {
	p := &NewTableScan{
		Table: tn.TableInfo,
	}
	p.id = b.allocID(p)
	// Equal condition contains a column from previous joined table.
	p.RefAccess = false
	rfs := tn.GetResultFields()
	schema := make([]*expression.Column, 0, len(rfs))
	for _, rf := range rfs {
		p.DBName = &rf.DBName
		p.Columns = append(p.Columns, rf.Column)
		schema = append(schema, &expression.Column{
			FromID:  p.id,
			ColName: rf.Column.Name,
			TblName: rf.Table.Name,
			DBName:  rf.DBName,
			RetType: &rf.Column.FieldType})
	}
	p.SetSchema(schema)
	return p
}
