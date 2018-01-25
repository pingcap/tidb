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
	"math"
	"math/bits"
	"reflect"
	"strings"
	"unicode"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

const (
	// TiDBMergeJoin is hint enforce merge join.
	TiDBMergeJoin = "tidb_smj"
	// TiDBIndexNestedLoopJoin is hint enforce index nested loop join.
	TiDBIndexNestedLoopJoin = "tidb_inlj"
	// TiDBHashJoin is hint enforce hash join.
	TiDBHashJoin = "tidb_hj"
)

const (
	// ErrExprInSelect  is in select fields for the error of ErrFieldNotInGroupBy
	ErrExprInSelect = "SELECT list"
	// ErrExprInOrderBy  is in order by items for the error of ErrFieldNotInGroupBy
	ErrExprInOrderBy = "ORDER BY"
)

func (la *LogicalAggregation) collectGroupByColumns() {
	la.groupByCols = la.groupByCols[:0]
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			la.groupByCols = append(la.groupByCols, col)
		}
	}
}

func (b *planBuilder) buildAggregation(p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression) (LogicalPlan, map[int]int) {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagAggregationOptimize
	// We may apply aggregation eliminate optimization.
	// So we add the flagMaxMinEliminate to try to convert max/min to topn and flagPushDownTopN to handle the newly added topn operator.
	b.optFlag = b.optFlag | flagMaxMinEliminate
	b.optFlag = b.optFlag | flagPushDownTopN
	// when we eliminate the max and min we may add `is not null` filter.
	b.optFlag = b.optFlag | flagPredicatePushDown

	plan4Agg := LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(aggFuncList))}.init(b.ctx)
	schema4Agg := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncList)+p.Schema().Len())...)
	// aggIdxMap maps the old index to new index after applying common aggregation functions elimination.
	aggIndexMap := make(map[int]int)

	for i, aggFunc := range aggFuncList {
		newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
		for _, arg := range aggFunc.Args {
			newArg, np, err := b.rewrite(arg, p, nil, true)
			if err != nil {
				b.err = errors.Trace(err)
				return nil, nil
			}
			p = np
			newArgList = append(newArgList, newArg)
		}
		newFunc := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList, aggFunc.Distinct)
		combined := false
		for j, oldFunc := range plan4Agg.AggFuncs {
			if oldFunc.Equal(b.ctx, newFunc) {
				aggIndexMap[i] = j
				combined = true
				break
			}
		}
		if !combined {
			position := len(plan4Agg.AggFuncs)
			aggIndexMap[i] = position
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			schema4Agg.Append(&expression.Column{
				FromID:      plan4Agg.id,
				ColName:     model.NewCIStr(fmt.Sprintf("%d_col_%d", plan4Agg.id, position)),
				Position:    position,
				IsAggOrSubq: true,
				RetType:     newFunc.RetTp})
		}
	}
	for _, col := range p.Schema().Columns {
		newFunc := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col.Clone()}, false)
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		schema4Agg.Append(col.Clone().(*expression.Column))
	}
	plan4Agg.SetChildren(p)
	plan4Agg.GroupByItems = gbyItems
	plan4Agg.SetSchema(schema4Agg)
	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scala functions.
	// plan4Agg.buildProjectionIfNecessary()
	// b.optFlag = b.optFlag | flagEliminateProjection
	plan4Agg.collectGroupByColumns()
	return plan4Agg, aggIndexMap
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
			b.err = ErrUnsupportedType.GenByArgs(v)
		}
		if b.err != nil {
			return nil
		}
		if v, ok := p.(*DataSource); ok {
			v.TableAsName = &x.AsName
		}
		for _, col := range p.Schema().Columns {
			col.OrigTblName = col.TblName
			if x.AsName.L != "" {
				col.TblName = x.AsName
				col.DBName = model.NewCIStr("")
			}
		}
		// Duplicate column name in one table is not allowed.
		// "select * from (select 1, 1) as a;" is duplicate
		dupNames := make(map[string]struct{}, len(p.Schema().Columns))
		for _, col := range p.Schema().Columns {
			name := col.ColName.O
			if _, ok := dupNames[name]; ok {
				b.err = ErrDupFieldName.GenByArgs(name)
				return nil
			}
			dupNames[name] = struct{}{}
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
					cond := expression.NewFunctionInternal(binop.GetCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), rn, ln)
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
	if p.Schema().Len() > 0 && p.Schema().Columns[0].TblName.L != "" {
		return &(p.Schema().Columns[0].TblName)
	}
	return nil
}

func (b *planBuilder) buildJoin(join *ast.Join) LogicalPlan {
	if join.Right == nil {
		return b.buildResultSetNode(join.Left)
	}
	b.optFlag = b.optFlag | flagPredicatePushDown
	leftPlan := b.buildResultSetNode(join.Left)
	if b.err != nil {
		return nil
	}
	rightPlan := b.buildResultSetNode(join.Right)
	if b.err != nil {
		return nil
	}

	newSchema := expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema())
	joinPlan := LogicalJoin{}.init(b.ctx)
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(newSchema)

	// Merge sub join's redundantSchema into this join plan. When handle query like
	// select t2.a from (t1 join t2 using (a)) join t3 using (a);
	// we can simply search in the top level join plan to find redundant column.
	var lRedundant, rRedundant *expression.Schema
	if left, ok := leftPlan.(*LogicalJoin); ok && left.redundantSchema != nil {
		lRedundant = left.redundantSchema
	}
	if right, ok := rightPlan.(*LogicalJoin); ok && right.redundantSchema != nil {
		rRedundant = right.redundantSchema
	}
	joinPlan.redundantSchema = expression.MergeSchema(lRedundant, rRedundant)

	if b.TableHints() != nil {
		leftAlias := extractTableAlias(leftPlan)
		rightAlias := extractTableAlias(rightPlan)
		if b.TableHints().ifPreferMergeJoin(leftAlias, rightAlias) {
			joinPlan.preferJoinType |= preferMergeJoin
		}
		if b.TableHints().ifPreferHashJoin(leftAlias, rightAlias) {
			joinPlan.preferJoinType |= preferHashJoin
		}
		if b.TableHints().ifPreferINLJ(leftAlias) {
			joinPlan.preferJoinType |= preferLeftAsIndexOuter
		}
		if b.TableHints().ifPreferINLJ(rightAlias) {
			joinPlan.preferJoinType |= preferRightAsIndexOuter
		}
		// If there're multiple join type and one of them is not the index join hints, then is conflict.
		if bits.OnesCount(joinPlan.preferJoinType) > 1 && (joinPlan.preferJoinType^preferRightAsIndexOuter^preferLeftAsIndexOuter) > 0 {
			b.err = errors.New("Join hints are conflict, you can only specify one type of join")
			return nil
		}
	}

	if join.NaturalJoin {
		if err := b.buildNaturalJoin(joinPlan, leftPlan, rightPlan, join); err != nil {
			b.err = err
			return nil
		}
	} else if join.Using != nil {
		if err := b.buildUsingClause(joinPlan, leftPlan, rightPlan, join); err != nil {
			b.err = err
			return nil
		}
	} else if join.On != nil {
		b.curClause = onClause
		onExpr, newPlan, err := b.rewrite(join.On.Expr, joinPlan, nil, false)
		if err != nil {
			b.err = err
			return nil
		}
		if newPlan != joinPlan {
			b.err = errors.New("ON condition doesn't support subqueries yet")
			return nil
		}
		onCondition := expression.SplitCNFItems(onExpr)
		joinPlan.attachOnConds(onCondition)
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
	return joinPlan
}

// buildUsingClause do redundant column elimination and column ordering based on using clause.
// According to standard SQL, producing this display order:
// First, coalesced common columns of the two joined tables, in the order in which they occur in the first table.
// Second, columns unique to the first table, in order in which they occur in that table.
// Third, columns unique to the second table, in order in which they occur in that table.
func (b *planBuilder) buildUsingClause(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	filter := make(map[string]bool, len(join.Using))
	for _, col := range join.Using {
		filter[col.Name.L] = true
	}
	return b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp == ast.RightJoin, filter)
}

// buildNaturalJoin build natural join output schema. It find out all the common columns
// then using the same mechanism as buildUsingClause to eliminate redundant columns and build join conditions.
// According to standard SQL, producing this display order:
// 	All the common columns
// 	Every column in the first (left) table that is not a common column
// 	Every column in the second (right) table that is not a common column
func (b *planBuilder) buildNaturalJoin(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	return b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp == ast.RightJoin, nil)
}

// coalesceCommonColumns is used by buildUsingClause and buildNaturalJoin. The filter is used by buildUsingClause.
func (b *planBuilder) coalesceCommonColumns(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, rightJoin bool, filter map[string]bool) error {
	lsc := leftPlan.Schema().Clone()
	rsc := rightPlan.Schema().Clone()
	lColumns, rColumns := lsc.Columns, rsc.Columns
	if rightJoin {
		lColumns, rColumns = rsc.Columns, lsc.Columns
	}

	// Find out all the common columns and put them ahead.
	commonLen := 0
	for i, lCol := range lColumns {
		for j := commonLen; j < len(rColumns); j++ {
			if lCol.ColName.L != rColumns[j].ColName.L {
				continue
			}

			if len(filter) > 0 {
				if !filter[lCol.ColName.L] {
					break
				}
				// Mark this column exist.
				filter[lCol.ColName.L] = false
			}

			col := lColumns[i]
			copy(lColumns[commonLen+1:i+1], lColumns[commonLen:i])
			lColumns[commonLen] = col

			col = rColumns[j]
			copy(rColumns[commonLen+1:j+1], rColumns[commonLen:j])
			rColumns[commonLen] = col

			commonLen++
			break
		}
	}

	if len(filter) > 0 && len(filter) != commonLen {
		for col, notExist := range filter {
			if notExist {
				return ErrUnknownColumn.GenByArgs(col, "from clause")
			}
		}
	}

	schemaCols := make([]*expression.Column, len(lColumns)+len(rColumns)-commonLen)
	copy(schemaCols[:len(lColumns)], lColumns)
	copy(schemaCols[len(lColumns):], rColumns[commonLen:])

	conds := make([]expression.Expression, 0, commonLen)
	for i := 0; i < commonLen; i++ {
		lc, rc := lsc.Columns[i], rsc.Columns[i]
		cond, err := expression.NewFunction(b.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), lc, rc)
		if err != nil {
			return errors.Trace(err)
		}
		conds = append(conds, cond)
	}

	p.SetSchema(expression.NewSchema(schemaCols...))
	p.redundantSchema = expression.MergeSchema(p.redundantSchema, expression.NewSchema(rColumns[:commonLen]...))
	p.OtherConditions = append(conds, p.OtherConditions...)

	return nil
}

func (b *planBuilder) buildSelection(p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown
	if b.curClause != havingClause {
		b.curClause = whereClause
	}
	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := LogicalSelection{}.init(b.ctx)
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
		cnfItems := expression.SplitCNFItems(expr)
		for _, item := range cnfItems {
			if con, ok := item.(*expression.Constant); ok {
				ret, err := expression.EvalBool(expression.CNFExprs{con}, nil, b.ctx)
				if err != nil || ret {
					continue
				} else {
					// If there is condition which is always false, return dual plan directly.
					dual := LogicalTableDual{}.init(b.ctx)
					dual.SetSchema(p.Schema())
					return dual
				}
			}
			expressions = append(expressions, item)
		}
	}
	if len(expressions) == 0 {
		return p
	}
	selection.Conditions = expressions
	selection.SetChildren(p)
	return selection
}

// buildProjectionFieldNameFromColumns builds the field name, table name and database name when field expression is a column reference.
func (b *planBuilder) buildProjectionFieldNameFromColumns(field *ast.SelectField, c *expression.Column) (colName, tblName, origTblName, dbName model.CIStr) {
	if astCol, ok := getInnerFromParentheses(field.Expr).(*ast.ColumnNameExpr); ok {
		colName, tblName, dbName = astCol.Name.Name, astCol.Name.Table, astCol.Name.Schema
	}
	if field.AsName.L != "" {
		colName = field.AsName
	}
	if tblName.L == "" {
		tblName = c.TblName
	}
	if dbName.L == "" {
		dbName = c.DBName
	}
	return colName, tblName, c.OrigTblName, c.DBName
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *planBuilder) buildProjectionFieldNameFromExpressions(field *ast.SelectField) model.CIStr {
	if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
		// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
		return agg.Args[0].(*ast.ColumnNameExpr).Name.Name
	}

	innerExpr := getInnerFromParentheses(field.Expr)
	valueExpr, isValueExpr := innerExpr.(*ast.ValueExpr)

	// Non-literal: Output as inputed, except that comments need to be removed.
	if !isValueExpr {
		return model.NewCIStr(parser.SpecFieldPattern.ReplaceAllStringFunc(field.Text(), parser.TrimComment))
	}

	// Literal: Need special processing
	switch valueExpr.Kind() {
	case types.KindString:
		projName := valueExpr.GetString()
		projOffset := valueExpr.GetProjectionOffset()
		if projOffset >= 0 {
			projName = projName[:projOffset]
		}
		// See #3686, #3994:
		// For string literals, string content is used as column name. Non-graph initial characters are trimmed.
		fieldName := strings.TrimLeftFunc(projName, func(r rune) bool {
			return !unicode.IsOneOf(mysql.RangeGraph, r)
		})
		return model.NewCIStr(fieldName)
	case types.KindNull:
		// See #4053, #3685
		return model.NewCIStr("NULL")
	default:
		// Keep as it is.
		if innerExpr.Text() != "" {
			return model.NewCIStr(innerExpr.Text())
		}
		return model.NewCIStr(field.Text())
	}
}

// buildProjectionField builds the field object according to SelectField in projection.
func (b *planBuilder) buildProjectionField(id, position int, field *ast.SelectField, expr expression.Expression) *expression.Column {
	var origTblName, tblName, colName, dbName model.CIStr
	if c, ok := expr.(*expression.Column); ok && !c.IsAggOrSubq {
		// Field is a column reference.
		colName, tblName, origTblName, dbName = b.buildProjectionFieldNameFromColumns(field, c)
	} else if field.AsName.L != "" {
		// Field has alias.
		colName = field.AsName
	} else {
		// Other: field is an expression.
		colName = b.buildProjectionFieldNameFromExpressions(field)
	}
	return &expression.Column{
		FromID:      id,
		Position:    position,
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		DBName:      dbName,
		RetType:     expr.GetType(),
	}
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *planBuilder) buildProjection(p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, int) {
	b.optFlag |= flagEliminateProjection
	b.curClause = fieldList
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(fields))}.init(b.ctx)
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

		col := b.buildProjectionField(proj.id, schema.Len()+1, field, newExpr)
		schema.Append(col)

		if !field.Auxiliary {
			oldLen++
		}
	}
	proj.SetSchema(schema)
	proj.SetChildren(p)
	return proj, oldLen
}

func (b *planBuilder) buildDistinct(child LogicalPlan, length int) LogicalPlan {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagAggregationOptimize
	plan4Agg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, child.Schema().Len()),
		GroupByItems: expression.Column2Exprs(child.Schema().Clone().Columns[:length]),
	}.init(b.ctx)
	plan4Agg.collectGroupByColumns()
	for _, col := range child.Schema().Columns {
		aggDesc := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggDesc)
	}
	plan4Agg.SetChildren(child)
	plan4Agg.SetSchema(child.Schema().Clone())
	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scala functions.
	// plan4Agg.buildProjectionIfNecessary()
	// b.optFlag = b.optFlag | flagEliminateProjection
	return plan4Agg
}

// joinFieldType finds the type which can carry the given types.
func joinFieldType(a, b *types.FieldType) *types.FieldType {
	resultTp := types.NewFieldType(types.MergeFieldType(a.Tp, b.Tp))
	resultTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
	// `Flen - Decimal` is the fraction before '.'
	resultTp.Flen = mathutil.Max(a.Flen-a.Decimal, b.Flen-b.Decimal) + resultTp.Decimal
	resultTp.Charset = a.Charset
	resultTp.Collate = a.Collate
	expression.SetBinFlagOrBinStr(b, resultTp)
	return resultTp
}

func (b *planBuilder) buildProjection4Union(u *LogicalUnionAll) {
	unionSchema := u.children[0].Schema().Clone()

	// Infer union result types by its children's schema.
	for i, col := range unionSchema.Columns {
		var resultTp *types.FieldType
		for j, child := range u.children {
			childTp := child.Schema().Columns[i].RetType
			if j == 0 {
				resultTp = childTp
			} else {
				resultTp = joinFieldType(resultTp, childTp)
			}
		}
		col.RetType = resultTp
		col.DBName = model.NewCIStr("")
	}
	// If the types of some child don't match the types of union, we add a projection with cast function.
	for childID, child := range u.children {
		exprs := make([]expression.Expression, len(child.Schema().Columns))
		needProjection := false
		for i, srcCol := range child.Schema().Columns {
			dstType := unionSchema.Columns[i].RetType
			srcType := srcCol.RetType
			if !srcType.Equal(dstType) {
				exprs[i] = expression.BuildCastFunction(b.ctx, srcCol.Clone(), dstType)
				needProjection = true
			} else {
				exprs[i] = srcCol.Clone()
			}
		}
		if _, isProj := child.(*LogicalProjection); needProjection || !isProj {
			b.optFlag |= flagEliminateProjection
			proj := LogicalProjection{Exprs: exprs}.init(b.ctx)
			if childID == 0 {
				for _, col := range unionSchema.Columns {
					col.FromID = proj.ID()
				}
			}
			proj.SetChildren(child)
			u.children[childID] = proj
		}
		u.children[childID].(*LogicalProjection).SetSchema(unionSchema.Clone())
	}
}

func (b *planBuilder) buildUnion(union *ast.UnionStmt) LogicalPlan {
	u := LogicalUnionAll{}.init(b.ctx)
	u.children = make([]LogicalPlan, len(union.SelectList.Selects))
	for i, sel := range union.SelectList.Selects {
		u.children[i] = b.buildSelect(sel)
		if b.err != nil {
			return nil
		}
		if u.children[i].Schema().Len() != u.children[0].Schema().Len() {
			b.err = errors.New("The used SELECT statements have a different number of columns")
			return nil
		}
	}

	b.buildProjection4Union(u)
	var p LogicalPlan = u
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
	if by.Desc {
		return fmt.Sprintf("%s true", by.Expr)
	}
	return by.Expr.String()
}

// Clone makes a copy of ByItems.
func (by *ByItems) Clone() *ByItems {
	return &ByItems{Expr: by.Expr.Clone(), Desc: by.Desc}
}

func (b *planBuilder) buildSort(p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int) LogicalPlan {
	b.curClause = orderByClause
	sort := LogicalSort{}.init(b.ctx)
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
	sort.SetChildren(p)
	return sort
}

// getUintForLimitOffset gets uint64 value for limit/offset.
// For ordinary statement, limit/offset should be uint64 constant value.
// For prepared statement, limit/offset is string. We should convert it to uint64.
func getUintForLimitOffset(sc *stmtctx.StatementContext, val interface{}) (uint64, error) {
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
	return 0, errors.Errorf("Invalid type %T for LogicalLimit/Offset", val)
}

func (b *planBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) LogicalPlan {
	b.optFlag = b.optFlag | flagPushDownTopN
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
	if count > math.MaxUint64-offset {
		count = math.MaxUint64 - offset
	}
	li := LogicalLimit{
		Offset: offset,
		Count:  count,
	}.init(b.ctx)
	li.SetChildren(src)
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
	curClause    clauseCode
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
			var err error
			index, err = a.resolveFromSchema(v, a.p.Schema())
			_ = err
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
				col, err1 := schema.FindColumn(v.Name)
				if err1 != nil {
					a.err = errors.Trace(err1)
					return node, false
				}
				if col != nil {
					return n, true
				}
			}
			a.err = ErrUnknownColumn.GenByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
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
		extractor.curClause = havingClause
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
		extractor.curClause = orderByClause
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
	extractor := &AggregateFuncExtractor{}
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
				ret := g.fields[index].Expr
				ret.Accept(extractor)
				if len(extractor.AggFuncs) != 0 {
					err = ErrIllegalReference.GenByArgs(v.Name.OrigColName(), "reference to group function")
				} else {
					return ret, true
				}
			}
			g.err = errors.Trace(err)
			return inNode, false
		}
	case *ast.PositionExpr:
		if v.N < 1 || v.N > len(g.fields) {
			g.err = errors.Errorf("Unknown column '%d' in 'group statement'", v.N)
			return inNode, false
		}
		ret := g.fields[v.N-1].Expr
		ret.Accept(extractor)
		if len(extractor.AggFuncs) != 0 {
			g.err = ErrWrongGroupField.GenByArgs(g.fields[v.N-1].Text())
			return inNode, false
		}
		return ret, true
	}
	return inNode, true
}

func tblInfoFromCol(from ast.ResultSetNode, col *expression.Column) *model.TableInfo {
	var tableList []*ast.TableName
	tableList = extractTableList(from, tableList)
	for _, field := range tableList {
		if field.Name.L == col.TblName.L {
			return field.TableInfo
		}
		if field.Name.L != col.TblName.L {
			continue
		}
		if field.Schema.L == col.DBName.L {
			return field.TableInfo
		}
	}
	return nil
}

func buildFuncDependCol(p LogicalPlan, cond ast.ExprNode) (*expression.Column, *expression.Column) {
	binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, nil
	}
	if binOpExpr.Op != opcode.EQ {
		return nil, nil
	}
	lColExpr, ok := binOpExpr.L.(*ast.ColumnNameExpr)
	if !ok {
		return nil, nil
	}
	rColExpr, ok := binOpExpr.R.(*ast.ColumnNameExpr)
	if !ok {
		return nil, nil
	}
	lCol, err := p.Schema().FindColumn(lColExpr.Name)
	if err != nil {
		return nil, nil
	}
	rCol, err := p.Schema().FindColumn(rColExpr.Name)
	if err != nil {
		return nil, nil
	}
	return lCol, rCol
}

func buildWhereFuncDepend(p LogicalPlan, where ast.ExprNode) map[*expression.Column]*expression.Column {
	whereConditions := splitWhere(where)
	colDependMap := make(map[*expression.Column]*expression.Column, 2*len(whereConditions))
	for _, cond := range whereConditions {
		lCol, rCol := buildFuncDependCol(p, cond)
		if lCol == nil || rCol == nil {
			continue
		}
		colDependMap[lCol] = rCol
		colDependMap[rCol] = lCol
	}
	return colDependMap
}

func buildJoinFuncDepend(p LogicalPlan, from ast.ResultSetNode) map[*expression.Column]*expression.Column {
	switch x := from.(type) {
	case *ast.Join:
		if x.On == nil {
			return nil
		}
		onConditions := splitWhere(x.On.Expr)
		colDependMap := make(map[*expression.Column]*expression.Column, len(onConditions))
		for _, cond := range onConditions {
			lCol, rCol := buildFuncDependCol(p, cond)
			if lCol == nil || rCol == nil {
				continue
			}
			lTbl := tblInfoFromCol(x.Left, lCol)
			if lTbl == nil {
				lCol, rCol = rCol, lCol
			}
			switch x.Tp {
			case ast.CrossJoin:
				colDependMap[lCol] = rCol
				colDependMap[rCol] = lCol
			case ast.LeftJoin:
				colDependMap[rCol] = lCol
			case ast.RightJoin:
				colDependMap[lCol] = rCol
			}
		}
		return colDependMap
	default:
		return nil
	}
}

func checkColFuncDepend(p LogicalPlan, col *expression.Column, tblInfo *model.TableInfo, gbyCols map[*expression.Column]struct{}, whereDepends, joinDepends map[*expression.Column]*expression.Column) bool {
	for _, index := range tblInfo.Indices {
		if !index.Unique {
			continue
		}
		funcDepend := true
		for _, indexCol := range index.Columns {
			iColInfo := tblInfo.Columns[indexCol.Offset]
			if !mysql.HasNotNullFlag(iColInfo.Flag) {
				funcDepend = false
				break
			}
			cn := &ast.ColumnName{
				Schema: col.DBName,
				Table:  col.TblName,
				Name:   iColInfo.Name,
			}
			iCol, err := p.Schema().FindColumn(cn)
			if err != nil || iCol == nil {
				funcDepend = false
				break
			}
			if _, ok := gbyCols[iCol]; ok {
				continue
			}
			if wCol, ok := whereDepends[iCol]; ok {
				if _, ok = gbyCols[wCol]; ok {
					continue
				}
			}
			if jCol, ok := joinDepends[iCol]; ok {
				if _, ok = gbyCols[jCol]; ok {
					continue
				}
			}
			funcDepend = false
			break
		}
		if funcDepend {
			return true
		}
	}
	primaryFuncDepend := true
	hasPrimaryField := false
	for _, colInfo := range tblInfo.Columns {
		if !mysql.HasPriKeyFlag(colInfo.Flag) {
			continue
		}
		hasPrimaryField = true
		pCol, err := p.Schema().FindColumn(&ast.ColumnName{
			Schema: col.DBName,
			Table:  col.TblName,
			Name:   colInfo.Name,
		})
		if err != nil {
			primaryFuncDepend = false
			break
		}
		if _, ok := gbyCols[pCol]; ok {
			continue
		}
		if wCol, ok := whereDepends[pCol]; ok {
			if _, ok = gbyCols[wCol]; ok {
				continue
			}
		}
		if jCol, ok := joinDepends[pCol]; ok {
			if _, ok = gbyCols[jCol]; ok {
				continue
			}
		}
		primaryFuncDepend = false
		break
	}
	return primaryFuncDepend && hasPrimaryField
}

// ErrExprLoc is for generate the ErrFieldNotInGroupBy error info
type ErrExprLoc struct {
	Offset int
	Loc    string
}

func checkExprInGroupBy(p LogicalPlan, expr ast.ExprNode, offset int, loc string, gbyCols map[*expression.Column]struct{}, gbyExprs []ast.ExprNode, notInGbyCols map[*expression.Column]ErrExprLoc) {
	if _, ok := expr.(*ast.AggregateFuncExpr); ok {
		return
	}
	if _, ok := expr.(*ast.ColumnNameExpr); !ok {
		for _, gbyExpr := range gbyExprs {
			if reflect.DeepEqual(gbyExpr, expr) {
				return
			}
		}
	}
	colMap := make(map[*expression.Column]struct{}, len(p.Schema().Columns))
	allColFromExprNode(p, expr, colMap)
	for col := range colMap {
		if _, ok := gbyCols[col]; !ok {
			notInGbyCols[col] = ErrExprLoc{Offset: offset, Loc: loc}
		}
	}
}

func (b *planBuilder) checkOnlyFullGroupBy(p LogicalPlan, fields []*ast.SelectField, orderBy *ast.OrderByClause, gby *ast.GroupByClause, from ast.ResultSetNode, where ast.ExprNode) {
	gbyCols := make(map[*expression.Column]struct{}, len(fields))
	gbyExprs := make([]ast.ExprNode, 0, len(fields))
	schema := p.Schema()
	for _, byItem := range gby.Items {
		if colExpr, ok := byItem.Expr.(*ast.ColumnNameExpr); ok {
			col, err := schema.FindColumn(colExpr.Name)
			if err != nil || col == nil {
				continue
			}
			gbyCols[col] = struct{}{}
		} else {
			gbyExprs = append(gbyExprs, byItem.Expr)
		}
	}

	notInGbyCols := make(map[*expression.Column]ErrExprLoc, len(fields))
	for offset, field := range fields {
		if field.Auxiliary {
			continue
		}
		checkExprInGroupBy(p, field.Expr, offset, ErrExprInSelect, gbyCols, gbyExprs, notInGbyCols)
	}
	if orderBy != nil {
		for offset, item := range orderBy.Items {
			checkExprInGroupBy(p, item.Expr, offset, ErrExprInOrderBy, gbyCols, gbyExprs, notInGbyCols)
		}
	}
	if len(notInGbyCols) == 0 {
		return
	}

	whereDepends := buildWhereFuncDepend(p, where)
	joinDepends := buildJoinFuncDepend(p, from)
	tblMap := make(map[*model.TableInfo]struct{}, len(notInGbyCols))
	for col, errExprLoc := range notInGbyCols {
		tblInfo := tblInfoFromCol(from, col)
		if tblInfo == nil {
			continue
		}
		if _, ok := tblMap[tblInfo]; ok {
			continue
		}
		if checkColFuncDepend(p, col, tblInfo, gbyCols, whereDepends, joinDepends) {
			tblMap[tblInfo] = struct{}{}
			continue
		}
		switch errExprLoc.Loc {
		case ErrExprInSelect:
			b.err = ErrFieldNotInGroupBy.GenByArgs(errExprLoc.Offset+1, errExprLoc.Loc, fields[errExprLoc.Offset].Text())
		case ErrExprInOrderBy:
			b.err = ErrFieldNotInGroupBy.GenByArgs(errExprLoc.Offset+1, errExprLoc.Loc, orderBy.Items[errExprLoc.Offset].Expr.Text())
		}
		return
	}
}

type colResolver struct {
	p    LogicalPlan
	cols map[*expression.Column]struct{}
}

func (c *colResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch inNode.(type) {
	case *ast.ColumnNameExpr, *ast.SubqueryExpr, *ast.AggregateFuncExpr:
		return inNode, true
	}
	return inNode, false
}

func (c *colResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		col, err := c.p.Schema().FindColumn(v.Name)
		if err == nil && col != nil {
			c.cols[col] = struct{}{}
		}
	}
	return inNode, true
}

func allColFromExprNode(p LogicalPlan, n ast.Node, cols map[*expression.Column]struct{}) {
	extractor := &colResolver{
		p:    p,
		cols: cols,
	}
	n.Accept(extractor)
	return
}

func (b *planBuilder) resolveGbyExprs(p LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (LogicalPlan, []expression.Expression) {
	b.curClause = groupByClause
	exprs := make([]expression.Expression, 0, len(gby.Items))
	resolver := &gbyResolver{
		fields: fields,
		schema: p.Schema(),
	}
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
		findTblNameInSchema := false
		for _, col := range p.Schema().Columns {
			if (dbName.L == "" || dbName.L == col.DBName.L) &&
				(tblName.L == "" || tblName.L == col.TblName.L) &&
				col.ID != model.ExtraHandleID {
				findTblNameInSchema = true
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
		if !findTblNameInSchema {
			b.err = ErrBadTable.GenByArgs(tblName)
		}
	}
	return
}

func (b *planBuilder) pushTableHints(hints []*ast.TableOptimizerHint) bool {
	var sortMergeTables, INLJTables, hashJoinTables []model.CIStr
	for _, hint := range hints {
		switch hint.HintName.L {
		case TiDBMergeJoin:
			sortMergeTables = append(sortMergeTables, hint.Tables...)
		case TiDBIndexNestedLoopJoin:
			INLJTables = append(INLJTables, hint.Tables...)
		case TiDBHashJoin:
			hashJoinTables = append(hashJoinTables, hint.Tables...)
		default:
			// ignore hints that not implemented
		}
	}
	if len(sortMergeTables)+len(INLJTables)+len(hashJoinTables) > 0 {
		b.tableHintInfo = append(b.tableHintInfo, tableHintInfo{
			sortMergeJoinTables:       sortMergeTables,
			indexNestedLoopJoinTables: INLJTables,
			hashJoinTables:            hashJoinTables,
		})
		return true
	}
	return false
}

func (b *planBuilder) popTableHints() {
	b.tableHintInfo = b.tableHintInfo[:len(b.tableHintInfo)-1]
}

// TableHints returns the *tableHintInfo of PlanBuilder.
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
		if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() {
			b.checkOnlyFullGroupBy(p, sel.Fields.Fields, sel.OrderBy, sel.GroupBy, sel.From.TableRefs, sel.Where)
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
	hasAgg := b.detectSelectAgg(sel)
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
		b.curClause = havingClause
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
		proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.init(b.ctx)
		proj.SetChildren(p)
		schema := expression.NewSchema(p.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.FromID = proj.ID()
		}
		proj.SetSchema(schema)
		return proj
	}

	return p
}

func (b *planBuilder) buildTableDual() LogicalPlan {
	dual := LogicalTableDual{RowCount: 1}.init(b.ctx)
	return dual
}

func (ds *DataSource) newExtraHandleSchemaCol() *expression.Column {
	return &expression.Column{
		FromID:   ds.id,
		DBName:   ds.DBName,
		TblName:  ds.tableInfo.Name,
		ColName:  model.ExtraHandleName,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		Position: len(ds.tableInfo.Columns), // set a unique position
		ID:       model.ExtraHandleID,
	}
}

func (b *planBuilder) buildDataSource(tn *ast.TableName) LogicalPlan {
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
	handle := domain.GetDomain(b.ctx).StatsHandle()
	var statisticTable *statistics.Table
	if handle == nil {
		// When the first session is created, the handle hasn't been initialized.
		statisticTable = statistics.PseudoTable(tableInfo.ID)
	} else {
		statisticTable = handle.GetTableStats(tableInfo.ID)
	}
	indices, includeTableScan, err := availableIndices(tn.IndexHints, tableInfo)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	avalableIndices := avalableIndices{indices: indices, includeTableScan: includeTableScan}

	ds := DataSource{
		indexHints:       tn.IndexHints,
		tableInfo:        tableInfo,
		statisticTable:   statisticTable,
		DBName:           schemaName,
		Columns:          make([]*model.ColumnInfo, 0, len(tableInfo.Columns)),
		availableIndices: &avalableIndices,
	}.init(b.ctx)
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, schemaName.L, tableInfo.Name.L, "")

	var columns []*table.Column
	if b.inUpdateStmt {
		columns = tbl.WritableCols()
	} else {
		columns = tbl.Cols()
	}
	var handleCol *expression.Column
	ds.Columns = make([]*model.ColumnInfo, 0, len(columns))
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	for i, col := range columns {
		ds.Columns = append(ds.Columns, col.ToInfo())
		schema.Append(&expression.Column{
			FromID:   ds.id,
			ColName:  col.Name,
			TblName:  tableInfo.Name,
			DBName:   schemaName,
			RetType:  &col.FieldType,
			Position: i,
			ID:       col.ID})
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			handleCol = schema.Columns[schema.Len()-1]
		}
	}
	ds.SetSchema(schema)
	isMemDB := infoschema.IsMemoryDB(ds.DBName.L)
	// We append an extra handle column to the schema when "ds" is not a memory
	// table e.g. table in the "INFORMATION_SCHEMA" database, and the handle
	// column is not the primary key of "ds".
	if !isMemDB && handleCol == nil {
		ds.Columns = append(ds.Columns, model.NewExtraHandleColInfo())
		handleCol = ds.newExtraHandleSchemaCol()
		schema.Append(handleCol)
	}
	if handleCol != nil {
		schema.TblID2Handle[tableInfo.ID] = []*expression.Column{handleCol}
	}
	// make plan as DS -> US -> Proj
	var result LogicalPlan = ds
	if b.ctx.Txn() != nil && !b.ctx.Txn().IsReadOnly() {
		us := LogicalUnionScan{}.init(b.ctx)
		us.SetChildren(result)
		result = us
	}
	proj := b.projectVirtualColumns(ds, columns)
	if proj != nil {
		proj.SetChildren(result)
		result = proj
	}
	return result
}

// projectVirtualColumns is only for DataSource. If some table has virtual generated columns,
// we add a projection on the original DataSource, and calculate those columns in the projection
// so that plans above it can reference generated columns by their name.
func (b *planBuilder) projectVirtualColumns(ds *DataSource, columns []*table.Column) *LogicalProjection {
	var hasVirtualGeneratedColumn = false
	for _, column := range columns {
		if column.IsGenerated() && !column.GeneratedStored {
			hasVirtualGeneratedColumn = true
			break
		}
	}
	if !hasVirtualGeneratedColumn {
		return nil
	}
	var proj = LogicalProjection{
		Exprs:            make([]expression.Expression, 0, len(columns)),
		calculateGenCols: true,
	}.init(b.ctx)
	for i, colExpr := range ds.Schema().Columns {
		var exprIsGen = false
		var expr expression.Expression
		if i < len(columns) {
			var column = columns[i]
			if column.IsGenerated() && !column.GeneratedStored {
				var err error
				expr, _, err = b.rewrite(column.GeneratedExpr, ds, nil, true)
				if err != nil {
					b.err = errors.Trace(err)
					return nil
				}
				// Because the expression maybe return different type from
				// the generated column, we should wrap a CAST on the result.
				expr = expression.BuildCastFunction(b.ctx, expr, colExpr.GetType())
				exprIsGen = true
			}
		}
		if !exprIsGen {
			expr = colExpr.Clone()
		}
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(ds.Schema().Clone())
	return proj
}

// buildApplyWithJoinType builds apply plan with outerPlan and innerPlan, which apply join with particular join type for
// every row from outerPlan and the whole innerPlan.
func (b *planBuilder) buildApplyWithJoinType(outerPlan, innerPlan LogicalPlan, tp JoinType) LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagDecorrelate
	ap := LogicalApply{LogicalJoin: LogicalJoin{JoinType: tp}}.init(b.ctx)
	ap.SetChildren(outerPlan, innerPlan)
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
	ap.self = ap
	return ap
}

func (b *planBuilder) buildExists(p LogicalPlan) LogicalPlan {
out:
	for {
		switch plan := p.(type) {
		// This can be removed when in exists clause,
		// e.g. exists(select count(*) from t order by a) is equal to exists t.
		case *LogicalProjection, *LogicalSort:
			p = p.Children()[0]
		case *LogicalAggregation:
			if len(plan.GroupByItems) == 0 {
				p = b.buildTableDual()
				break out
			}
			p = p.Children()[0]
		default:
			break out
		}
	}
	exists := LogicalExists{}.init(b.ctx)
	exists.SetChildren(p)
	newCol := &expression.Column{
		FromID:  exists.id,
		RetType: types.NewFieldType(mysql.TypeTiny),
		ColName: model.NewCIStr("exists_col")}
	exists.SetSchema(expression.NewSchema(newCol))
	return exists
}

func (b *planBuilder) buildMaxOneRow(p LogicalPlan) LogicalPlan {
	maxOneRow := LogicalMaxOneRow{}.init(b.ctx)
	maxOneRow.SetChildren(p)
	return maxOneRow
}

func (b *planBuilder) buildSemiJoin(outerPlan, innerPlan LogicalPlan, onCondition []expression.Expression, asScalar bool, not bool) *LogicalJoin {
	joinPlan := LogicalJoin{}.init(b.ctx)
	for i, expr := range onCondition {
		onCondition[i] = expr.Decorrelate(outerPlan.Schema())
	}
	joinPlan.SetChildren(outerPlan, innerPlan)
	joinPlan.attachOnConds(onCondition)
	if asScalar {
		newSchema := outerPlan.Schema().Clone()
		newSchema.Append(&expression.Column{
			FromID:      joinPlan.id,
			ColName:     model.NewCIStr(fmt.Sprintf("%d_aux_0", joinPlan.id)),
			RetType:     types.NewFieldType(mysql.TypeTiny),
			IsAggOrSubq: true,
		})
		joinPlan.SetSchema(newSchema)
		if not {
			joinPlan.JoinType = AntiLeftOuterSemiJoin
		} else {
			joinPlan.JoinType = LeftOuterSemiJoin
		}
	} else {
		joinPlan.SetSchema(outerPlan.Schema().Clone())
		if not {
			joinPlan.JoinType = AntiSemiJoin
		} else {
			joinPlan.JoinType = SemiJoin
		}
	}
	// Apply forces to choose hash join currently, so don't worry the hints will take effect if the semi join is in one apply.
	if b.TableHints() != nil {
		outerAlias := extractTableAlias(outerPlan)
		innerAlias := extractTableAlias(innerPlan)
		if b.TableHints().ifPreferMergeJoin(outerAlias, innerAlias) {
			joinPlan.preferJoinType |= preferMergeJoin
		}
		if b.TableHints().ifPreferHashJoin(outerAlias, innerAlias) {
			joinPlan.preferJoinType |= preferHashJoin
		}
		// semi join's outer is always the left side.
		if b.TableHints().ifPreferINLJ(outerAlias) {
			joinPlan.preferJoinType = preferLeftAsIndexOuter
		}
		// If there're multiple join hints, they're conflict.
		if bits.OnesCount(joinPlan.preferJoinType) > 1 {
			b.err = errors.New("Join hints are conflict, you can only specify one type of join")
			return nil
		}
	}
	return joinPlan
}

func (b *planBuilder) buildUpdate(update *ast.UpdateStmt) Plan {
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
	orderedList, np := b.buildUpdateLists(tableList, update.List, p)
	if b.err != nil {
		return nil
	}
	p = np

	updt := Update{
		OrderedList: orderedList,
		IgnoreErr:   update.IgnoreErr,
	}.init(b.ctx)
	updt.SetSchema(p.Schema())
	updt.SelectPlan, b.err = doOptimize(b.optFlag, p)
	updt.ResolveIndices()
	return updt
}

func (b *planBuilder) buildUpdateLists(tableList []*ast.TableName, list []*ast.Assignment, p LogicalPlan) ([]*expression.Assignment, LogicalPlan) {
	b.curClause = fieldList
	modifyColumns := make(map[string]struct{}, p.Schema().Len()) // Which columns are in set list.
	for _, assign := range list {
		col, _, err := p.findColumn(assign.Column)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, nil
		}
		columnFullName := fmt.Sprintf("%s.%s.%s", col.DBName.L, col.TblName.L, col.ColName.L)
		modifyColumns[columnFullName] = struct{}{}
	}

	// If columns in set list contains generated columns, raise error.
	// And, fill virtualAssignments here; that's for generated columns.
	virtualAssignments := make([]*ast.Assignment, 0)
	tableAsName := make(map[*model.TableInfo][]*model.CIStr)
	extractTableAsNameForUpdate(p, tableAsName)

	for _, tn := range tableList {
		tableInfo := tn.TableInfo
		tableVal, found := b.is.TableByID(tableInfo.ID)
		if !found {
			b.err = infoschema.ErrTableNotExists.GenByArgs(tn.DBInfo.Name.O, tableInfo.Name.O)
			return nil, nil
		}
		for i, colInfo := range tableInfo.Columns {
			if !colInfo.IsGenerated() {
				continue
			}
			columnFullName := fmt.Sprintf("%s.%s.%s", tn.Schema.L, tn.Name.L, colInfo.Name.L)
			if _, ok := modifyColumns[columnFullName]; ok {
				b.err = ErrBadGeneratedColumn.GenByArgs(colInfo.Name.O, tableInfo.Name.O)
				return nil, nil
			}
			for _, asName := range tableAsName[tableInfo] {
				virtualAssignments = append(virtualAssignments, &ast.Assignment{
					Column: &ast.ColumnName{Table: *asName, Name: colInfo.Name},
					Expr:   tableVal.Cols()[i].GeneratedExpr,
				})
			}
		}
	}

	newList := make([]*expression.Assignment, 0, p.Schema().Len())
	allAssignments := append(list, virtualAssignments...)
	for i, assign := range allAssignments {
		col, _, err := p.findColumn(assign.Column)
		if err != nil {
			b.err = errors.Trace(err)
			return nil, nil
		}
		var newExpr expression.Expression
		var np LogicalPlan
		if i < len(list) {
			newExpr, np, err = b.rewrite(assign.Expr, p, nil, false)
		} else {
			// rewrite with generation expression
			rewritePreprocess := func(expr ast.Node) ast.Node {
				switch x := expr.(type) {
				case *ast.ColumnName:
					return &ast.ColumnName{
						Schema: assign.Column.Schema,
						Table:  assign.Column.Table,
						Name:   x.Name,
					}
				}
				return expr
			}
			newExpr, np, err = b.rewriteWithPreprocess(assign.Expr, p, nil, false, rewritePreprocess)
		}
		if err != nil {
			b.err = errors.Trace(err)
			return nil, nil
		}
		newExpr = expression.BuildCastFunction(b.ctx, newExpr, col.GetType())
		p = np
		newList = append(newList, &expression.Assignment{Col: col.Clone().(*expression.Column), Expr: newExpr})
	}
	return newList, p
}

// extractTableAsNameForUpdate extracts tables' alias names for update.
func extractTableAsNameForUpdate(p LogicalPlan, asNames map[*model.TableInfo][]*model.CIStr) {
	switch x := p.(type) {
	case *DataSource:
		alias := extractTableAlias(p)
		if alias != nil {
			if _, ok := asNames[x.tableInfo]; !ok {
				asNames[x.tableInfo] = make([]*model.CIStr, 0, 1)
			}
			asNames[x.tableInfo] = append(asNames[x.tableInfo], alias)
		}
	case *LogicalProjection:
		if x.calculateGenCols {
			ds := x.Children()[0].(*DataSource)
			alias := extractTableAlias(x)
			if alias != nil {
				if _, ok := asNames[ds.tableInfo]; !ok {
					asNames[ds.tableInfo] = make([]*model.CIStr, 0, 1)
				}
				asNames[ds.tableInfo] = append(asNames[ds.tableInfo], alias)
			}
		}
	default:
		for _, child := range p.Children() {
			extractTableAsNameForUpdate(child, asNames)
		}
	}
}

func (b *planBuilder) buildDelete(delete *ast.DeleteStmt) Plan {
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
	}.init(b.ctx)
	del.SelectPlan, b.err = doOptimize(b.optFlag, p)
	if b.err != nil {
		return nil
	}
	del.SetSchema(expression.NewSchema())

	var tableList []*ast.TableName
	tableList = extractTableList(delete.TableRefs.TableRefs, tableList)

	// Collect visitInfo.
	if delete.Tables != nil {
		// Delete a, b from a, b, c, d... add a and b.
		for _, tn := range delete.Tables.Tables {
			foundMatch := false
			for _, v := range tableList {
				dbName := v.Schema.L
				if dbName == "" {
					dbName = b.ctx.GetSessionVars().CurrentDB
				}
				if (tn.Schema.L == "" || tn.Schema.L == dbName) && tn.Name.L == v.Name.L {
					tn.Schema.L = dbName
					tn.DBInfo = v.DBInfo
					tn.TableInfo = v.TableInfo
					foundMatch = true
					break
				}
			}
			if !foundMatch {
				var asNameList []string
				asNameList = extractTableSourceAsNames(delete.TableRefs.TableRefs, asNameList)
				for _, asName := range asNameList {
					tblName := tn.Name.L
					if tn.Schema.L != "" {
						tblName = tn.Schema.L + "." + tblName
					}
					if asName == tblName {
						// check sql like: `delete a from (select * from t) as a, t`
						b.err = ErrNonUpdatableTable.GenByArgs(tn.Name.O, "DELETE")
						return nil
					}
				}
				// check sql like: `delete b from (select * from t) as a, t`
				b.err = ErrUnknownTable.GenByArgs(tn.Name.O, "MULTI DELETE")
				return nil
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, tn.Schema.L, tn.TableInfo.Name.L, "")
		}
	} else {
		// Delete from a, b, c, d.
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

// extractTableList extracts all the TableNames from node.
func extractTableList(node ast.ResultSetNode, input []*ast.TableName) []*ast.TableName {
	switch x := node.(type) {
	case *ast.Join:
		input = extractTableList(x.Left, input)
		input = extractTableList(x.Right, input)
	case *ast.TableSource:
		if s, ok := x.Source.(*ast.TableName); ok {
			if x.AsName.L != "" {
				newTableName := *s
				newTableName.Name = x.AsName
				s.Name = x.AsName
				input = append(input, &newTableName)
			} else {
				input = append(input, s)
			}
		}
	}
	return input
}

// extractTableSourceAsNames extracts all the TableSource.AsNames from node.
func extractTableSourceAsNames(node ast.ResultSetNode, input []string) []string {
	switch x := node.(type) {
	case *ast.Join:
		input = extractTableSourceAsNames(x.Left, input)
		input = extractTableSourceAsNames(x.Right, input)
	case *ast.TableSource:
		if s, ok := x.Source.(*ast.TableName); ok {
			if x.AsName.L == "" {
				input = append(input, s.Name.L)
				break
			}
		}
		input = append(input, x.AsName.L)
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

func getInnerFromParentheses(expr ast.ExprNode) ast.ExprNode {
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		return getInnerFromParentheses(pexpr.Expr)
	}
	return expr
}
