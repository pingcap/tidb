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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/types"
)

const (
	// TiDBMergeJoin is hint enforce merge join.
	TiDBMergeJoin = "tidb_smj"
	// TiDBIndexNestedLoopJoin is hint enforce index nested loop join.
	TiDBIndexNestedLoopJoin = "tidb_inlj"
)

type idAllocator struct {
	id int
}

func (a *idAllocator) allocID() string {
	a.id++
	return fmt.Sprintf("_%d", a.id)
}

func (p *LogicalAggregation) collectGroupByColumns() {
	p.groupByCols = p.groupByCols[:0]
	for _, item := range p.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			p.groupByCols = append(p.groupByCols, col)
		}
	}
}

func (b *planBuilder) buildAggregation(p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression) (LogicalPlan, map[int]int) {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagAggregationOptimize

	agg := LogicalAggregation{AggFuncs: make([]expression.AggregationFunction, 0, len(aggFuncList))}.init(b.allocator, b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncList)+p.Schema().Len())...)
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
			newArgList = append(newArgList, newArg)
		}
		newFunc := expression.NewAggFunction(aggFunc.F, newArgList, aggFunc.Distinct)
		combined := false
		for j, oldFunc := range agg.AggFuncs {
			if oldFunc.Equal(newFunc, b.ctx) {
				aggIndexMap[i] = j
				combined = true
				break
			}
		}
		if !combined {
			position := len(agg.AggFuncs)
			aggIndexMap[i] = position
			agg.AggFuncs = append(agg.AggFuncs, newFunc)
			schema.Append(&expression.Column{
				FromID:      agg.id,
				ColName:     model.NewCIStr(fmt.Sprintf("%s_col_%d", agg.id, position)),
				Position:    position,
				IsAggOrSubq: true,
				RetType:     aggFunc.GetType()})
		}
	}
	for _, col := range p.Schema().Columns {
		newFunc := expression.NewAggFunction(ast.AggFuncFirstRow, []expression.Expression{col.Clone()}, false)
		agg.AggFuncs = append(agg.AggFuncs, newFunc)
		schema.Append(col.Clone().(*expression.Column))
	}
	addChild(agg, p)
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
			for _, col := range p.Schema().Columns {
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

func extractCorColumns(expr expression.Expression) (cols []*expression.CorrelatedColumn) {
	switch v := expr.(type) {
	case *expression.CorrelatedColumn:
		return []*expression.CorrelatedColumn{v}
	case *expression.ScalarFunction:
		for _, arg := range v.GetArgs() {
			cols = append(cols, extractCorColumns(arg)...)
		}
	}
	return
}

func extractOnCondition(conditions []expression.Expression, left LogicalPlan, right LogicalPlan) (
	eqCond []*expression.ScalarFunction, leftCond []expression.Expression, rightCond []expression.Expression,
	otherCond []expression.Expression) {
	for _, expr := range conditions {
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && binop.FuncName.L == ast.EQ {
			ln, lOK := binop.GetArgs()[0].(*expression.Column)
			rn, rOK := binop.GetArgs()[1].(*expression.Column)
			if lOK && rOK {
				if left.Schema().Contains(ln) && right.Schema().Contains(rn) {
					eqCond = append(eqCond, binop)
					continue
				}
				if left.Schema().Contains(rn) && right.Schema().Contains(ln) {
					cond, _ := expression.NewFunction(binop.GetCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), rn, ln)
					eqCond = append(eqCond, cond.(*expression.ScalarFunction))
					continue
				}
			}
		}
		columns := expression.ExtractColumns(expr)
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if !left.Schema().Contains(col) {
				allFromLeft = false
			}
			if !right.Schema().Contains(col) {
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

func extractTableAlias(p LogicalPlan) *model.CIStr {
	if dataSource, ok := p.(*DataSource); ok {
		if dataSource.TableAsName.L != "" {
			return dataSource.TableAsName
		}
		return &dataSource.tableInfo.Name
	} else if len(p.Schema().Columns) > 0 {
		if p.Schema().Columns[0].TblName.L != "" {
			return &(p.Schema().Columns[0].TblName)
		}
	}
	return nil
}

func (b *planBuilder) buildJoin(join *ast.Join) LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown
	if join.Right == nil {
		return b.buildResultSetNode(join.Left)
	}
	leftPlan := b.buildResultSetNode(join.Left)
	rightPlan := b.buildResultSetNode(join.Right)
	leftAlias := extractTableAlias(leftPlan)
	rightAlias := extractTableAlias(rightPlan)

	newSchema := expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema())
	joinPlan := LogicalJoin{}.init(b.allocator, b.ctx)
	addChild(joinPlan, leftPlan)
	addChild(joinPlan, rightPlan)
	joinPlan.SetSchema(newSchema)

	if b.TableHints() != nil {
		joinPlan.preferMergeJoin = b.TableHints().ifPreferMergeJoin(leftAlias, rightAlias)
		if b.TableHints().ifPreferINLJ(leftAlias) {
			joinPlan.preferINLJ = joinPlan.preferINLJ | preferLeftAsOuter
		}
		if b.TableHints().ifPreferINLJ(rightAlias) {
			joinPlan.preferINLJ = joinPlan.preferINLJ | preferRightAsOuter
		}
		if joinPlan.preferMergeJoin && joinPlan.preferINLJ > 0 {
			b.err = errors.New("Optimizer Hints is conflict")
		}
	}

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
		joinPlan.attachOnConds(onCondition)
	} else if joinPlan.JoinType == InnerJoin {
		joinPlan.cartesianJoin = true
	}
	if join.Tp == ast.LeftJoin {
		joinPlan.JoinType = LeftOuterJoin
		joinPlan.DefaultValues = make([]types.Datum, rightPlan.Schema().Len())
	} else if join.Tp == ast.RightJoin {
		joinPlan.JoinType = RightOuterJoin
		joinPlan.DefaultValues = make([]types.Datum, leftPlan.Schema().Len())
	} else {
		joinPlan.JoinType = InnerJoin
	}
	return joinPlan
}

func (b *planBuilder) buildSelection(p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown
	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := Selection{}.init(b.allocator, b.ctx)
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
	}
	if len(expressions) == 0 {
		return p
	}
	selection.Conditions = expressions
	selection.SetSchema(p.Schema().Clone())
	addChild(selection, p)
	return selection
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *planBuilder) buildProjection(p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, int) {
	proj := Projection{Exprs: make([]expression.Expression, 0, len(fields))}.init(b.allocator, b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)
	oldLen := 0
	for _, field := range fields {
		newExpr, np, err := b.rewrite(field.Expr, p, mapper, true)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, oldLen
		}
		p = np
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
		col := &expression.Column{
			FromID:  proj.id,
			TblName: tblName,
			ColName: colName,
			RetType: newExpr.GetType(),
		}
		if !field.Auxiliary {
			oldLen++
		}
		schema.Append(col)
		col.Position = schema.Len()
	}
	proj.SetSchema(schema)
	addChild(proj, p)
	return proj, oldLen
}

func (b *planBuilder) buildDistinct(child LogicalPlan, length int) LogicalPlan {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagAggregationOptimize
	agg := LogicalAggregation{
		AggFuncs:     make([]expression.AggregationFunction, 0, child.Schema().Len()),
		GroupByItems: expression.Column2Exprs(child.Schema().Clone().Columns[:length]),
	}.init(b.allocator, b.ctx)
	agg.collectGroupByColumns()
	for _, col := range child.Schema().Columns {
		agg.AggFuncs = append(agg.AggFuncs, expression.NewAggFunction(ast.AggFuncFirstRow, []expression.Expression{col}, false))
	}
	addChild(agg, child)
	agg.SetSchema(child.Schema().Clone())
	return agg
}

func (b *planBuilder) buildUnion(union *ast.UnionStmt) LogicalPlan {
	u := Union{}.init(b.allocator, b.ctx)
	u.children = make([]Plan, len(union.SelectList.Selects))
	for i, sel := range union.SelectList.Selects {
		u.children[i] = b.buildSelect(sel)
	}
	firstSchema := u.children[0].Schema().Clone()
	for i, sel := range u.children {
		if firstSchema.Len() != sel.Schema().Len() {
			b.err = errors.New("The used SELECT statements have a different number of columns")
			return nil
		}
		if _, ok := sel.(*Projection); !ok {
			proj := Projection{Exprs: expression.Column2Exprs(sel.Schema().Columns)}.init(b.allocator, b.ctx)
			proj.SetSchema(sel.Schema().Clone())
			sel.SetParents(proj)
			proj.SetChildren(sel)
			sel = proj
			u.children[i] = proj
		}
		for i, col := range sel.Schema().Columns {
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
			if col.RetType.Flen > firstSchema.Columns[i].RetType.Flen {
				firstSchema.Columns[i].RetType.Flen = col.RetType.Flen
			}
			// For select nul union select "abc", we should not convert "abc" to nil.
			// And the result field type should be VARCHAR.
			if firstSchema.Columns[i].RetType.Tp == 0 || firstSchema.Columns[i].RetType.Tp == mysql.TypeNull {
				firstSchema.Columns[i].RetType.Tp = col.RetType.Tp
			}
		}
		sel.SetParents(u)
	}
	for _, v := range firstSchema.Columns {
		v.FromID = u.id
		v.DBName = model.NewCIStr("")
	}

	u.SetSchema(firstSchema)
	var p LogicalPlan
	p = u
	if union.Distinct {
		p = b.buildDistinct(u, u.Schema().Len())
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
	sort := Sort{}.init(b.allocator, b.ctx)
	exprs := make([]*ByItems, 0, len(byItems))
	for _, item := range byItems {
		it, np, err := b.rewrite(item.Expr, p, aggMapper, true)
		if err != nil {
			b.err = err
			return nil
		}
		p = np
		exprs = append(exprs, &ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	addChild(sort, p)
	sort.SetSchema(p.Schema().Clone())
	return sort
}

// getUintForLimitOffset gets uint64 value for limit/offset.
// For ordinary statement, limit/offset should be uint64 constant value.
// For prepared statement, limit/offset is string. We should convert it to uint64.
func getUintForLimitOffset(sc *variable.StatementContext, val interface{}) (uint64, error) {
	switch v := val.(type) {
	case uint64:
		return v, nil
	case int64:
		if v >= 0 {
			return uint64(v), nil
		}
	case string:
		uVal, err := types.StrToUint(sc, v)
		return uVal, errors.Trace(err)
	}
	return 0, errors.Errorf("Invalid type %T for Limit/Offset", val)
}

func (b *planBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) LogicalPlan {
	if UseDAGPlanBuilder {
		b.optFlag = b.optFlag | flagPushDownTopN
	}
	var (
		offset, count uint64
		err           error
	)
	sc := b.ctx.GetSessionVars().StmtCtx
	if limit.Offset != nil {
		offset, err = getUintForLimitOffset(sc, limit.Offset.GetValue())
		if err != nil {
			b.err = ErrWrongArguments
			return nil
		}
	}
	if limit.Count != nil {
		count, err = getUintForLimitOffset(sc, limit.Count.GetValue())
		if err != nil {
			b.err = ErrWrongArguments
			return nil
		}
	}

	li := Limit{
		Offset: offset,
		Count:  count,
	}.init(b.allocator, b.ctx)
	addChild(li, src)
	li.SetSchema(src.Schema().Clone())
	return li
}

// colMatch(a,b) means that if a match b, e.g. t.a can match test.t.a but test.t.a can't match t.a.
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
				return -1, ErrAmbiguous.GenByArgs(curCol.Name.Name.L)
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
	outerSchemas []*expression.Schema
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

func (a *havingAndOrderbyExprResolver) resolveFromSchema(v *ast.ColumnNameExpr, schema *expression.Schema) (int, error) {
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
					index, a.err = a.resolveFromSchema(v, a.p.Schema())
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			// We should ignore the err when resolving from schema. Because we could resolve successfully
			// when considering select fields.
			index, _ = a.resolveFromSchema(v, a.p.Schema())
			if index == -1 {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			// If we can't find it any where, it may be a correlated columns.
			for _, schema := range a.outerSchemas {
				if col, _ := schema.FindColumn(v.Name); col != nil {
					return n, true
				}
			}
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

// resolveHavingAndOrderBy will process aggregate functions and resolve the columns that don't exist in select fields.
// If we found some columns that are not in select fields, we will append it to select fields and update the colMapper.
// When we rewrite the order by / having expression, we will find column in map at first.
func (b *planBuilder) resolveHavingAndOrderBy(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int) {
	extractor := &havingAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
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
	extractor := &AggregateFuncExtractor{}
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
	schema *expression.Schema
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
	resolver := &gbyResolver{fields: fields, schema: p.Schema()}
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
		for _, col := range p.Schema().Columns {
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

func (b *planBuilder) pushTableHints(hints []*ast.TableOptimizerHint) bool {
	var sortMergeTables, INLJTables []model.CIStr
	for _, hint := range hints {
		switch hint.HintName.L {
		case TiDBMergeJoin:
			sortMergeTables = append(sortMergeTables, hint.Tables...)
		case TiDBIndexNestedLoopJoin:
			INLJTables = append(INLJTables, hint.Tables...)
		default:
			// ignore hints that not implemented
		}
	}
	if len(sortMergeTables) != 0 || len(INLJTables) != 0 {
		b.tableHintInfo = append(b.tableHintInfo, tableHintInfo{
			sortMergeJoinTables: sortMergeTables,
			INLJTables:          INLJTables,
		})
		return true
	}
	return false
}

func (b *planBuilder) popTableHints() {
	b.tableHintInfo = b.tableHintInfo[:len(b.tableHintInfo)-1]
}

func (b *planBuilder) TableHints() *tableHintInfo {
	if b.tableHintInfo == nil || len(b.tableHintInfo) == 0 {
		return nil
	}
	return &(b.tableHintInfo[len(b.tableHintInfo)-1])
}

func (b *planBuilder) buildSelect(sel *ast.SelectStmt) LogicalPlan {
	if sel.TableHints != nil {
		// table hints without query block support only visible in current SELECT
		if b.pushTableHints(sel.TableHints) {
			defer b.popTableHints()
		}
	}

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
	originalFields := sel.Fields.Fields
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
		p = b.buildDistinct(p, oldLen)
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
	sel.Fields.Fields = originalFields
	if oldLen != p.Schema().Len() {
		proj := Projection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.init(b.allocator, b.ctx)
		addChild(proj, p)
		proj.SetSchema(expression.NewSchema(p.Schema().Columns[:oldLen]...))
		return proj
	}

	return p
}

func (b *planBuilder) buildTableDual() LogicalPlan {
	dual := TableDual{RowCount: 1}.init(b.allocator, b.ctx)
	dual.SetSchema(expression.NewSchema())
	return dual
}

func (b *planBuilder) buildDataSource(tn *ast.TableName) LogicalPlan {
	handle := sessionctx.GetDomain(b.ctx).StatsHandle()
	var statisticTable *statistics.Table
	if handle == nil {
		// When the first session is created, the handle hasn't been initialized.
		statisticTable = statistics.PseudoTable(tn.TableInfo.ID)
	} else {
		statisticTable = handle.GetTableStats(tn.TableInfo.ID)
	}
	if b.err != nil {
		return nil
	}
	schemaName := tn.Schema
	if schemaName.L == "" {
		schemaName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	tbl, err := b.is.TableByName(schemaName, tn.Name)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	tableInfo := tbl.Meta()

	p := DataSource{
		indexHints:     tn.IndexHints,
		tableInfo:      tableInfo,
		statisticTable: statisticTable,
		DBName:         schemaName,
	}.init(b.allocator, b.ctx)

	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, schemaName.L, tableInfo.Name.L, "")

	// Equal condition contains a column from previous joined table.
	schema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.Columns))...)
	for i, col := range tableInfo.Columns {
		if b.inUpdateStmt {
			switch col.State {
			case model.StatePublic, model.StateWriteOnly, model.StateWriteReorganization:
			default:
				continue
			}
		} else if col.State != model.StatePublic {
			continue
		}
		p.Columns = append(p.Columns, col)
		schema.Append(&expression.Column{
			FromID:   p.id,
			ColName:  col.Name,
			TblName:  tableInfo.Name,
			DBName:   schemaName,
			RetType:  &col.FieldType,
			Position: i,
			ID:       col.ID})
	}
	p.SetSchema(schema)
	return p
}

// ApplyConditionChecker checks whether all or any output of apply matches a condition.
type ApplyConditionChecker struct {
	Condition expression.Expression
	All       bool
}

// buildApplyWithJoinType builds apply plan with outerPlan and innerPlan, which apply join with particular join type for
// every row from outerPlan and the whole innerPlan.
func (b *planBuilder) buildApplyWithJoinType(outerPlan, innerPlan LogicalPlan, tp JoinType) LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagDecorrelate
	ap := LogicalApply{LogicalJoin: LogicalJoin{JoinType: tp}}.init(b.allocator, b.ctx)

	addChild(ap, outerPlan)
	addChild(ap, innerPlan)
	ap.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
	for i := outerPlan.Schema().Len(); i < ap.Schema().Len(); i++ {
		ap.schema.Columns[i].IsAggOrSubq = true
	}
	return ap
}

// buildSemiApply builds apply plan with outerPlan and innerPlan, which apply semi-join for every row from outerPlan and the whole innerPlan.
func (b *planBuilder) buildSemiApply(outerPlan, innerPlan LogicalPlan, condition []expression.Expression, asScalar, not bool) LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagDecorrelate
	join := b.buildSemiJoin(outerPlan, innerPlan, condition, asScalar, not)
	ap := &LogicalApply{LogicalJoin: *join}
	ap.tp = TypeApply
	ap.id = ap.tp + ap.allocator.allocID()
	ap.self = ap
	ap.children[0].SetParents(ap)
	ap.children[1].SetParents(ap)
	return ap
}

func (b *planBuilder) buildExists(p LogicalPlan) LogicalPlan {
out:
	for {
		switch plan := p.(type) {
		// This can be removed when in exists clause,
		// e.g. exists(select count(*) from t order by a) is equal to exists t.
		case *Projection, *Sort:
			p = p.Children()[0].(LogicalPlan)
			p.SetParents()
		case *LogicalAggregation:
			if len(plan.GroupByItems) == 0 {
				p = b.buildTableDual()
				break out
			}
			p = p.Children()[0].(LogicalPlan)
			p.SetParents()
		default:
			break out
		}
	}
	exists := Exists{}.init(b.allocator, b.ctx)
	addChild(exists, p)
	newCol := &expression.Column{
		FromID:  exists.id,
		RetType: types.NewFieldType(mysql.TypeTiny),
		ColName: model.NewCIStr("exists_col")}
	exists.SetSchema(expression.NewSchema(newCol))
	return exists
}

func (b *planBuilder) buildMaxOneRow(p LogicalPlan) LogicalPlan {
	maxOneRow := MaxOneRow{}.init(b.allocator, b.ctx)
	addChild(maxOneRow, p)
	maxOneRow.SetSchema(p.Schema().Clone())
	return maxOneRow
}

func (b *planBuilder) buildSemiJoin(outerPlan, innerPlan LogicalPlan, onCondition []expression.Expression, asScalar bool, not bool) *LogicalJoin {
	joinPlan := LogicalJoin{}.init(b.allocator, b.ctx)
	for i, expr := range onCondition {
		onCondition[i] = expr.Decorrelate(outerPlan.Schema())
	}
	joinPlan.SetChildren(outerPlan, innerPlan)
	outerPlan.SetParents(joinPlan)
	innerPlan.SetParents(joinPlan)
	joinPlan.attachOnConds(onCondition)
	if asScalar {
		newSchema := outerPlan.Schema().Clone()
		newSchema.Append(&expression.Column{
			FromID:      joinPlan.id,
			ColName:     model.NewCIStr(fmt.Sprintf("%s_aux_0", joinPlan.id)),
			RetType:     types.NewFieldType(mysql.TypeTiny),
			IsAggOrSubq: true,
		})
		joinPlan.SetSchema(newSchema)
		joinPlan.JoinType = LeftOuterSemiJoin
	} else {
		joinPlan.SetSchema(outerPlan.Schema().Clone())
		joinPlan.JoinType = SemiJoin
	}
	joinPlan.anti = not
	return joinPlan
}

func (b *planBuilder) buildUpdate(update *ast.UpdateStmt) LogicalPlan {
	b.inUpdateStmt = true
	sel := &ast.SelectStmt{Fields: &ast.FieldList{}, From: update.TableRefs, Where: update.Where, OrderBy: update.Order, Limit: update.Limit}
	p := b.buildResultSetNode(sel.From.TableRefs)
	if b.err != nil {
		return nil
	}

	var tableList []*ast.TableName
	tableList = extractTableList(sel.From.TableRefs, tableList)
	for _, t := range tableList {
		dbName := t.Schema.L
		if dbName == "" {
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, dbName, t.Name.L, "")
	}

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
	updt := Update{OrderedList: orderedList}.init(b.allocator, b.ctx)
	addChild(updt, p)
	updt.SetSchema(p.Schema())
	return updt
}

func (b *planBuilder) buildUpdateLists(list []*ast.Assignment, p LogicalPlan) ([]*expression.Assignment, LogicalPlan) {
	schema := p.Schema()
	newList := make([]*expression.Assignment, schema.Len())
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
		offset := schema.ColumnIndex(col)
		if offset == -1 {
			b.err = errors.Trace(errors.Errorf("could not find column %s.%s", col.TblName, col.ColName))
			return nil, nil
		}
		newExpr, np, err := b.rewrite(assign.Expr, p, nil, false)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, nil
		}
		p = np
		newList[offset] = &expression.Assignment{Col: col.Clone().(*expression.Column), Expr: newExpr}
	}
	return newList, p
}

func (b *planBuilder) buildDelete(delete *ast.DeleteStmt) LogicalPlan {
	sel := &ast.SelectStmt{Fields: &ast.FieldList{}, From: delete.TableRefs, Where: delete.Where, OrderBy: delete.Order, Limit: delete.Limit}
	p := b.buildResultSetNode(sel.From.TableRefs)
	if b.err != nil {
		return nil
	}

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

	del := Delete{
		Tables:       tables,
		IsMultiTable: delete.IsMultiTable,
	}.init(b.allocator, b.ctx)
	addChild(del, p)
	del.SetSchema(expression.NewSchema())

	// Collect visitInfo.
	if delete.Tables != nil {
		// Delete a, b from a, b, c, d... add a and b.
		for _, table := range delete.Tables.Tables {
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, table.Schema.L, table.TableInfo.Name.L, "")
		}
	} else {
		// Delete from a, b, c, d.
		var tableList []*ast.TableName
		tableList = extractTableList(delete.TableRefs.TableRefs, tableList)
		for _, v := range tableList {
			dbName := v.Schema.L
			if dbName == "" {
				dbName = b.ctx.GetSessionVars().CurrentDB
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, dbName, v.Name.L, "")
		}
	}

	return del
}

func extractTableList(node ast.ResultSetNode, input []*ast.TableName) []*ast.TableName {
	switch x := node.(type) {
	case *ast.Join:
		input = extractTableList(x.Left, input)
		input = extractTableList(x.Right, input)
	case *ast.TableSource:
		if s, ok := x.Source.(*ast.TableName); ok {
			input = append(input, s)
		}
	}
	return input
}

func appendVisitInfo(vi []visitInfo, priv mysql.PrivilegeType, db, tbl, col string) []visitInfo {
	return append(vi, visitInfo{
		privilege: priv,
		db:        db,
		table:     tbl,
		column:    col,
	})
}
