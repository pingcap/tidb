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

package core

import (
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"strings"
	"unicode"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
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

func (b *planBuilder) buildAggregation(p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression) (LogicalPlan, map[int]int, error) {
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
				return nil, nil, errors.Trace(err)
			}
			p = np
			newArgList = append(newArgList, newArg)
		}
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList, aggFunc.Distinct)
		if err != nil {
			return nil, nil, err
		}
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
				ColName:     model.NewCIStr(fmt.Sprintf("%d_col_%d", plan4Agg.id, position)),
				UniqueID:    b.ctx.GetSessionVars().AllocPlanColumnID(),
				IsAggOrSubq: true,
				RetType:     newFunc.RetTp,
			})
		}
	}
	for _, col := range p.Schema().Columns {
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		newCol, _ := col.Clone().(*expression.Column)
		newCol.RetType = newFunc.RetTp
		schema4Agg.Append(newCol)
	}
	plan4Agg.SetChildren(p)
	plan4Agg.GroupByItems = gbyItems
	plan4Agg.SetSchema(schema4Agg)
	plan4Agg.collectGroupByColumns()
	return plan4Agg, aggIndexMap, nil
}

func (b *planBuilder) buildResultSetNode(node ast.ResultSetNode) (p LogicalPlan, err error) {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(x)
	case *ast.TableSource:
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			p, err = b.buildSelect(v)
		case *ast.UnionStmt:
			p, err = b.buildUnion(v)
		case *ast.TableName:
			p, err = b.buildDataSource(v)
		default:
			err = ErrUnsupportedType.GenWithStackByArgs(v)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		if v, ok := p.(*DataSource); ok {
			v.TableAsName = &x.AsName
		}
		for _, col := range p.Schema().Columns {
			col.OrigTblName = col.TblName
			if x.AsName.L != "" {
				col.TblName = x.AsName
			}
		}
		// Duplicate column name in one table is not allowed.
		// "select * from (select 1, 1) as a;" is duplicate
		dupNames := make(map[string]struct{}, len(p.Schema().Columns))
		for _, col := range p.Schema().Columns {
			name := col.ColName.O
			if _, ok := dupNames[name]; ok {
				return nil, ErrDupFieldName.GenWithStackByArgs(name)
			}
			dupNames[name] = struct{}{}
		}
		return p, nil
	case *ast.SelectStmt:
		return b.buildSelect(x)
	case *ast.UnionStmt:
		return b.buildUnion(x)
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.ResultSetNode(%T) for buildResultSetNode()", x)
	}
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) ([]expression.Expression, []expression.Expression) {
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case SemiJoin, InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	case AntiSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
		}
		rightCond = append(rightCond, expr)
	}
	return leftCond, rightCond
}

// extractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool, deriveRight bool) (
	eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	left, right := p.children[0], p.children[1]
	for _, expr := range conditions {
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && binop.FuncName.L == ast.EQ {
			ln, lOK := binop.GetArgs()[0].(*expression.Column)
			rn, rOK := binop.GetArgs()[1].(*expression.Column)
			// For quries like `select a in (select a from s where s.b = t.b) from t`,
			// if subquery is empty caused by `s.b = t.b`, the result should always be
			// false even if t.a is null or s.a is null. To make this join "empty aware",
			// we should differentiate `t.a = s.a` from other column equal conditions, so
			// we put it into OtherConditions instead of EqualConditions of join.
			if lOK && rOK && !ln.InOperand && !rn.InOperand {
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
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this kind of constant condition down according to join type.
		if len(columns) == 0 {
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
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
			// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
			// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
			// children as much as possible.
			if deriveLeft {
				leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, left.Schema())
				if leftRelaxedCond != nil {
					leftCond = append(leftCond, leftRelaxedCond)
				}
			}
			if deriveRight {
				rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, right.Schema())
				if rightRelaxedCond != nil {
					rightCond = append(rightCond, rightRelaxedCond)
				}
			}
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// extractTableAlias returns table alias of the LogicalPlan's columns.
// It will return nil when there are multiple table alias, because the alias is only used to check if
// the logicalPlan match some optimizer hints, and hints are not expected to take effect in this case.
func extractTableAlias(p LogicalPlan) *model.CIStr {
	if p.Schema().Len() > 0 && p.Schema().Columns[0].TblName.L != "" {
		tblName := p.Schema().Columns[0].TblName.L
		for _, column := range p.Schema().Columns {
			if column.TblName.L != tblName {
				return nil
			}
		}
		return &(p.Schema().Columns[0].TblName)
	}
	return nil
}

func (p *LogicalJoin) setPreferredJoinType(hintInfo *tableHintInfo) error {
	if hintInfo == nil {
		return nil
	}

	lhsAlias := extractTableAlias(p.children[0])
	rhsAlias := extractTableAlias(p.children[1])
	if hintInfo.ifPreferMergeJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferMergeJoin
	}
	if hintInfo.ifPreferHashJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferHashJoin
	}
	if hintInfo.ifPreferINLJ(lhsAlias) {
		p.preferJoinType |= preferLeftAsIndexInner
	}
	if hintInfo.ifPreferINLJ(rhsAlias) {
		p.preferJoinType |= preferRightAsIndexInner
	}

	// set hintInfo for further usage if this hint info can be used.
	if p.preferJoinType != 0 {
		p.hintInfo = hintInfo
	}

	// If there're multiple join types and one of them is not index join hint,
	// then there is a conflict of join types.
	if bits.OnesCount(p.preferJoinType) > 1 && (p.preferJoinType^preferRightAsIndexInner^preferLeftAsIndexInner) > 0 {
		return errors.New("Join hints are conflict, you can only specify one type of join")
	}
	return nil
}

func (b *planBuilder) buildJoin(joinNode *ast.Join) (LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		return b.buildResultSetNode(joinNode.Left)
	}

	b.optFlag = b.optFlag | flagPredicatePushDown

	leftPlan, err := b.buildResultSetNode(joinNode.Left)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rightPlan, err := b.buildResultSetNode(joinNode.Right)
	if err != nil {
		return nil, errors.Trace(err)
	}

	joinPlan := LogicalJoin{StraightJoin: joinNode.StraightJoin || b.inStraightJoin}.init(b.ctx)
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))

	// Set join type.
	switch joinNode.Tp {
	case ast.LeftJoin:
		joinPlan.JoinType = LeftOuterJoin
	case ast.RightJoin:
		joinPlan.JoinType = RightOuterJoin
	default:
		joinPlan.JoinType = InnerJoin
	}

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

	// Set preferred join algorithm if some join hints is specified by user.
	err = joinPlan.setPreferredJoinType(b.TableHints())
	if err != nil {
		return nil, errors.Trace(err)
	}

	// "NATURAL JOIN" doesn't have "ON" or "USING" conditions.
	//
	// The "NATURAL [LEFT] JOIN" of two tables is defined to be semantically
	// equivalent to an "INNER JOIN" or a "LEFT JOIN" with a "USING" clause
	// that names all columns that exist in both tables.
	//
	// See https://dev.mysql.com/doc/refman/5.7/en/join.html for more detail.
	if joinNode.NaturalJoin {
		err = b.buildNaturalJoin(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else if joinNode.Using != nil {
		err = b.buildUsingClause(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else if joinNode.On != nil {
		b.curClause = onClause
		onExpr, newPlan, err := b.rewrite(joinNode.On.Expr, joinPlan, nil, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if newPlan != joinPlan {
			return nil, errors.New("ON condition doesn't support subqueries yet")
		}
		onCondition := expression.SplitCNFItems(onExpr)
		joinPlan.attachOnConds(onCondition)
	} else if joinPlan.JoinType == InnerJoin {
		// If a inner join without "ON" or "USING" clause, it's a cartesian
		// product over the join tables.
		joinPlan.cartesianJoin = true
	}

	return joinPlan, nil
}

// buildUsingClause eliminate the redundant columns and ordering columns based
// on the "USING" clause.
//
// According to the standard SQL, columns are ordered in the following way:
// 1. coalesced common columns of "leftPlan" and "rightPlan", in the order they
//    appears in "leftPlan".
// 2. the rest columns in "leftPlan", in the order they appears in "leftPlan".
// 3. the rest columns in "rightPlan", in the order they appears in "rightPlan".
func (b *planBuilder) buildUsingClause(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	filter := make(map[string]bool, len(join.Using))
	for _, col := range join.Using {
		filter[col.Name.L] = true
	}
	return b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp == ast.RightJoin, filter)
}

// buildNaturalJoin builds natural join output schema. It finds out all the common columns
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
				return ErrUnknownColumn.GenWithStackByArgs(col, "from clause")
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

func (b *planBuilder) buildSelection(p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, error) {
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
			return nil, errors.Trace(err)
		}
		p = np
		if expr == nil {
			continue
		}
		cnfItems := expression.SplitCNFItems(expr)
		for _, item := range cnfItems {
			if con, ok := item.(*expression.Constant); ok {
				ret, _, err := expression.EvalBool(b.ctx, expression.CNFExprs{con}, chunk.Row{})
				if err != nil || ret {
					continue
				}
				// If there is condition which is always false, return dual plan directly.
				dual := LogicalTableDual{}.init(b.ctx)
				dual.SetSchema(p.Schema())
				return dual, nil
			}
			expressions = append(expressions, item)
		}
	}
	if len(expressions) == 0 {
		return p, nil
	}
	selection.Conditions = expressions
	selection.SetChildren(p)
	return selection, nil
}

// buildProjectionFieldNameFromColumns builds the field name, table name and database name when field expression is a column reference.
<<<<<<< HEAD
func (b *planBuilder) buildProjectionFieldNameFromColumns(origField *ast.SelectField, colNameField *ast.ColumnNameExpr, c *expression.Column) (colName, origColName, tblName, origTblName, dbName model.CIStr) {
	origColName, tblName, dbName = colNameField.Name.Name, colNameField.Name.Table, colNameField.Name.Schema
	if origField.AsName.L != "" {
		colName = origField.AsName
=======
func (b *PlanBuilder) buildProjectionFieldNameFromColumns(field *ast.SelectField, c *expression.Column) (colName, origColName, tblName, origTblName, dbName model.CIStr) {
	if astCol, ok := getInnerFromParenthesesAndUnaryPlus(field.Expr).(*ast.ColumnNameExpr); ok {
		origColName, tblName, dbName = astCol.Name.Name, astCol.Name.Table, astCol.Name.Schema
	}
	if field.AsName.L != "" {
		colName = field.AsName
>>>>>>> 29c17a1... planner/core: correct column name with unary plus sign (#8702)
	} else {
		colName = origColName
	}
	if tblName.L == "" {
		tblName = c.TblName
	}
	if dbName.L == "" {
		dbName = c.DBName
	}
	return colName, origColName, tblName, c.OrigTblName, c.DBName
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *planBuilder) buildProjectionFieldNameFromExpressions(field *ast.SelectField) model.CIStr {
	if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
		// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
		return agg.Args[0].(*ast.ColumnNameExpr).Name.Name
	}

	innerExpr := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	valueExpr, isValueExpr := innerExpr.(*driver.ValueExpr)

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
	var origTblName, tblName, origColName, colName, dbName model.CIStr
	innerNode := getInnerFromParentheses(field.Expr)
	col, isCol := expr.(*expression.Column)
	// Correlated column won't affect the final output names. So we can put it in any of the three logic block.
	// Don't put it into the first block just for simplifying the codes.
	if colNameField, ok := innerNode.(*ast.ColumnNameExpr); ok && isCol {
		// Field is a column reference.
		colName, origColName, tblName, origTblName, dbName = b.buildProjectionFieldNameFromColumns(field, colNameField, col)
	} else if field.AsName.L != "" {
		// Field has alias.
		colName = field.AsName
	} else {
		// Other: field is an expression.
		colName = b.buildProjectionFieldNameFromExpressions(field)
	}
	return &expression.Column{
		UniqueID:    b.ctx.GetSessionVars().AllocPlanColumnID(),
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: origColName,
		DBName:      dbName,
		RetType:     expr.GetType(),
	}
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *planBuilder) buildProjection(p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, int, error) {
	b.optFlag |= flagEliminateProjection
	b.curClause = fieldList
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(fields))}.init(b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)
	oldLen := 0
	for _, field := range fields {
		newExpr, np, err := b.rewrite(field.Expr, p, mapper, true)
		if err != nil {
			return nil, 0, errors.Trace(err)
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
	return proj, oldLen, nil
}

func (b *planBuilder) buildDistinct(child LogicalPlan, length int) (*LogicalAggregation, error) {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagAggregationOptimize
	plan4Agg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, child.Schema().Len()),
		GroupByItems: expression.Column2Exprs(child.Schema().Clone().Columns[:length]),
	}.init(b.ctx)
	plan4Agg.collectGroupByColumns()
	for _, col := range child.Schema().Columns {
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggDesc)
	}
	plan4Agg.SetChildren(child)
	plan4Agg.SetSchema(child.Schema().Clone())
	// Distinct will be rewritten as first_row, we reset the type here since the return type
	// of first_row is not always the same as the column arg of first_row.
	for i, col := range plan4Agg.schema.Columns {
		col.RetType = plan4Agg.AggFuncs[i].RetTp
	}
	return plan4Agg, nil
}

// unionJoinFieldType finds the type which can carry the given types in Union.
func unionJoinFieldType(a, b *types.FieldType) *types.FieldType {
	resultTp := types.NewFieldType(types.MergeFieldType(a.Tp, b.Tp))
	// This logic will be intelligible when it is associated with the buildProjection4Union logic.
	if resultTp.Tp == mysql.TypeNewDecimal {
		// The decimal result type will be unsigned only when all the decimals to be united are unsigned.
		resultTp.Flag &= b.Flag & mysql.UnsignedFlag
	} else {
		// Non-decimal results will be unsigned when the first SQL statement result in the union is unsigned.
		resultTp.Flag |= a.Flag & mysql.UnsignedFlag
	}
	resultTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
	// `Flen - Decimal` is the fraction before '.'
	resultTp.Flen = mathutil.Max(a.Flen-a.Decimal, b.Flen-b.Decimal) + resultTp.Decimal
	if resultTp.EvalType() != types.ETInt && (a.EvalType() == types.ETInt || b.EvalType() == types.ETInt) && resultTp.Flen < mysql.MaxIntWidth {
		resultTp.Flen = mysql.MaxIntWidth
	}
	resultTp.Charset = a.Charset
	resultTp.Collate = a.Collate
	expression.SetBinFlagOrBinStr(b, resultTp)
	return resultTp
}

func (b *planBuilder) buildProjection4Union(u *LogicalUnionAll) {
	unionCols := make([]*expression.Column, 0, u.children[0].Schema().Len())

	// Infer union result types by its children's schema.
	for i, col := range u.children[0].Schema().Columns {
		resultTp := col.RetType
		for j := 1; j < len(u.children); j++ {
			childTp := u.children[j].Schema().Columns[i].RetType
			resultTp = unionJoinFieldType(resultTp, childTp)
		}
		unionCols = append(unionCols, &expression.Column{
			ColName:  col.ColName,
			RetType:  resultTp,
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
	}
	u.schema = expression.NewSchema(unionCols...)
	// Process each child and add a projection above original child.
	// So the schema of `UnionAll` can be the same with its children's.
	for childID, child := range u.children {
		exprs := make([]expression.Expression, len(child.Schema().Columns))
		for i, srcCol := range child.Schema().Columns {
			dstType := unionCols[i].RetType
			srcType := srcCol.RetType
			if !srcType.Equal(dstType) {
				exprs[i] = expression.BuildCastFunction4Union(b.ctx, srcCol, dstType)
			} else {
				exprs[i] = srcCol
			}
		}
		b.optFlag |= flagEliminateProjection
		proj := LogicalProjection{Exprs: exprs, avoidColumnEvaluator: true}.init(b.ctx)
		proj.SetSchema(u.schema.Clone())
		proj.SetChildren(child)
		u.children[childID] = proj
	}
}

func (b *planBuilder) buildUnion(union *ast.UnionStmt) (LogicalPlan, error) {
	distinctSelectPlans, allSelectPlans, err := b.divideUnionSelectPlans(union.SelectList.Selects)
	if err != nil {
		return nil, errors.Trace(err)
	}

	unionDistinctPlan := b.buildUnionAll(distinctSelectPlans)
	if unionDistinctPlan != nil {
		unionDistinctPlan, err = b.buildDistinct(unionDistinctPlan, unionDistinctPlan.Schema().Len())
		if err != nil {
			return nil, err
		}
		if len(allSelectPlans) > 0 {
			// Can't change the statements order in order to get the correct column info.
			allSelectPlans = append([]LogicalPlan{unionDistinctPlan}, allSelectPlans...)
		}
	}

	unionAllPlan := b.buildUnionAll(allSelectPlans)
	unionPlan := unionDistinctPlan
	if unionAllPlan != nil {
		unionPlan = unionAllPlan
	}

	oldLen := unionPlan.Schema().Len()

	if union.OrderBy != nil {
		unionPlan, err = b.buildSort(unionPlan, union.OrderBy.Items, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if union.Limit != nil {
		unionPlan, err = b.buildLimit(unionPlan, union.Limit)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Fix issue #8189 (https://github.com/pingcap/tidb/issues/8189).
	// If there are extra expressions generated from `ORDER BY` clause, generate a `Projection` to remove them.
	if oldLen != unionPlan.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(unionPlan.Schema().Columns[:oldLen])}.init(b.ctx)
		proj.SetChildren(unionPlan)
		schema := expression.NewSchema(unionPlan.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.SetSchema(schema)
		return proj, nil
	}

	return unionPlan, nil
}

// divideUnionSelectPlans resolves union's select stmts to logical plans.
// and divide result plans into "union-distinct" and "union-all" parts.
// divide rule ref: https://dev.mysql.com/doc/refman/5.7/en/union.html
// "Mixed UNION types are treated such that a DISTINCT union overrides any ALL union to its left."
func (b *planBuilder) divideUnionSelectPlans(selects []*ast.SelectStmt) (distinctSelects []LogicalPlan, allSelects []LogicalPlan, err error) {
	firstUnionAllIdx, columnNums := 0, -1
	// The last slot is reserved for appending distinct union outside this function.
	children := make([]LogicalPlan, len(selects), len(selects)+1)
	for i := len(selects) - 1; i >= 0; i-- {
		stmt := selects[i]
		if firstUnionAllIdx == 0 && stmt.IsAfterUnionDistinct {
			firstUnionAllIdx = i + 1
		}

		selectPlan, err := b.buildSelect(stmt)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		if columnNums == -1 {
			columnNums = selectPlan.Schema().Len()
		}
		if selectPlan.Schema().Len() != columnNums {
			return nil, nil, ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
		children[i] = selectPlan
	}
	return children[:firstUnionAllIdx], children[firstUnionAllIdx:], nil
}

func (b *planBuilder) buildUnionAll(subPlan []LogicalPlan) LogicalPlan {
	if len(subPlan) == 0 {
		return nil
	}
	u := LogicalUnionAll{}.init(b.ctx)
	u.children = subPlan
	b.buildProjection4Union(u)
	return u
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

// itemTransformer transforms ParamMarkerExpr to PositionExpr in the context of ByItem
type itemTransformer struct {
}

func (t *itemTransformer) Enter(inNode ast.Node) (ast.Node, bool) {
	switch n := inNode.(type) {
	case *driver.ParamMarkerExpr:
		newNode := expression.ConstructPositionExpr(n)
		return newNode, true
	}
	return inNode, false
}

func (t *itemTransformer) Leave(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

func (b *planBuilder) buildSort(p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int) (*LogicalSort, error) {
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		b.curClause = globalOrderByClause
	} else {
		b.curClause = orderByClause
	}
	sort := LogicalSort{}.init(b.ctx)
	exprs := make([]*ByItems, 0, len(byItems))
	transformer := &itemTransformer{}
	for _, item := range byItems {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewrite(item.Expr, p, aggMapper, true)
		if err != nil {
			return nil, errors.Trace(err)
		}

		p = np
		exprs = append(exprs, &ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	sort.SetChildren(p)
	return sort, nil
}

// getUintForLimitOffset gets uint64 value for limit/offset.
// For ordinary statement, limit/offset should be uint64 constant value.
// For prepared statement, limit/offset is string. We should convert it to uint64.
func getUintForLimitOffset(ctx sessionctx.Context, n ast.Node) (uint64, error) {
	var val interface{}
	switch v := n.(type) {
	case *driver.ValueExpr:
		val = v.GetValue()
	case *driver.ParamMarkerExpr:
		param, err := expression.GetParamExpression(ctx, v)
		if err != nil {
			return 0, errors.Trace(err)
		}
		str, isNull, err := expression.GetStringFromConstant(ctx, param)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if isNull {
			return 0, nil
		}
		val = str
	default:
		return 0, errors.Errorf("Invalid type %T for LogicalLimit/Offset", v)
	}
	switch v := val.(type) {
	case uint64:
		return v, nil
	case int64:
		if v >= 0 {
			return uint64(v), nil
		}
	case string:
		sc := ctx.GetSessionVars().StmtCtx
		uVal, err := types.StrToUint(sc, v)
		return uVal, errors.Trace(err)
	}
	return 0, errors.Errorf("Invalid type %T for LogicalLimit/Offset", val)
}

func extractLimitCountOffset(ctx sessionctx.Context, limit *ast.Limit) (count uint64,
	offset uint64, err error) {
	if limit.Count != nil {
		count, err = getUintForLimitOffset(ctx, limit.Count)
		if err != nil {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	if limit.Offset != nil {
		offset, err = getUintForLimitOffset(ctx, limit.Offset)
		if err != nil {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	return count, offset, nil
}

func (b *planBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) (LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPushDownTopN
	var (
		offset, count uint64
		err           error
	)
	if count, offset, err = extractLimitCountOffset(b.ctx, limit); err != nil {
		return nil, err
	}

	if count > math.MaxUint64-offset {
		count = math.MaxUint64 - offset
	}
	if offset+count == 0 {
		tableDual := LogicalTableDual{RowCount: 0}.init(b.ctx)
		tableDual.schema = src.Schema()
		return tableDual, nil
	}
	li := LogicalLimit{
		Offset: offset,
		Count:  count,
	}.init(b.ctx)
	li.SetChildren(src)
	return li, nil
}

// colMatch means that if a match b, e.g. t.a can match test.t.a but test.t.a can't match t.a.
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
			} else if _, isFunc := f.Expr.(*ast.FuncCallExpr); isFunc {
				// Fix issue 7331
				// If there are some function calls in SelectField, we check if
				// ColumnNameExpr in GroupByClause matches one of these function calls.
				// Example: select concat(k1,k2) from t group by `concat(k1,k2)`,
				// `concat(k1,k2)` matches with function call concat(k1, k2).
				return strings.ToLower(f.Text()) == col.Name.Name.L
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
				return -1, ErrAmbiguous.GenWithStackByArgs(curCol.Name.Name.L, clauseMsg[fieldList])
			}
		}
	}
	return
}

// havingAndOrderbyExprResolver visits Expr tree.
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
	case *driver.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.ColumnName:
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
			a.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
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
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int, error) {
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
			return nil, nil, errors.Trace(extractor.err)
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
				return nil, nil, errors.Trace(extractor.err)
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return havingAggMapper, extractor.aggMapper, nil
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
	ctx     sessionctx.Context
	fields  []*ast.SelectField
	schema  *expression.Schema
	err     error
	inExpr  bool
	isParam bool
}

func (g *gbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch n := inNode.(type) {
	case *ast.SubqueryExpr, *ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr:
		return inNode, true
	case *driver.ParamMarkerExpr:
		newNode := expression.ConstructPositionExpr(n)
		g.isParam = true
		return newNode, true
	case *driver.ValueExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.ColumnName:
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
					err = ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to group function")
				} else {
					return ret, true
				}
			}
			g.err = errors.Trace(err)
			return inNode, false
		}
	case *ast.PositionExpr:
		pos, isNull, err := expression.PosFromPositionExpr(g.ctx, v)
		if err != nil {
			g.err = ErrUnknown.GenWithStackByArgs()
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(g.fields) {
			g.err = errors.Errorf("Unknown column '%d' in 'group statement'", pos)
			return inNode, false
		}
		ret := g.fields[pos-1].Expr
		ret.Accept(extractor)
		if len(extractor.AggFuncs) != 0 {
			g.err = ErrWrongGroupField.GenWithStackByArgs(g.fields[pos-1].Text())
			return inNode, false
		}
		return ret, true
	case *ast.ValuesExpr:
		if v.Column == nil {
			g.err = ErrUnknownColumn.GenWithStackByArgs("", "VALUES() function")
		}
	}
	return inNode, true
}

func tblInfoFromCol(from ast.ResultSetNode, col *expression.Column) *model.TableInfo {
	var tableList []*ast.TableName
	tableList = extractTableList(from, tableList, true)
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
	// Function `any_value` can be used in aggregation, even `ONLY_FULL_GROUP_BY` is set.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value for details
	if f, ok := expr.(*ast.FuncCallExpr); ok {
		if f.FnName.L == ast.AnyValue {
			return
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

func (b *planBuilder) checkOnlyFullGroupBy(p LogicalPlan, sel *ast.SelectStmt) (err error) {
	if sel.GroupBy != nil {
		err = b.checkOnlyFullGroupByWithGroupClause(p, sel)
	} else {
		err = b.checkOnlyFullGroupByWithOutGroupClause(p, sel.Fields.Fields)
	}
	return errors.Trace(err)
}

func (b *planBuilder) checkOnlyFullGroupByWithGroupClause(p LogicalPlan, sel *ast.SelectStmt) error {
	gbyCols := make(map[*expression.Column]struct{}, len(sel.Fields.Fields))
	gbyExprs := make([]ast.ExprNode, 0, len(sel.Fields.Fields))
	schema := p.Schema()
	for _, byItem := range sel.GroupBy.Items {
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

	notInGbyCols := make(map[*expression.Column]ErrExprLoc, len(sel.Fields.Fields))
	for offset, field := range sel.Fields.Fields {
		if field.Auxiliary {
			continue
		}
		checkExprInGroupBy(p, field.Expr, offset, ErrExprInSelect, gbyCols, gbyExprs, notInGbyCols)
	}

	if sel.OrderBy != nil {
		for offset, item := range sel.OrderBy.Items {
			checkExprInGroupBy(p, item.Expr, offset, ErrExprInOrderBy, gbyCols, gbyExprs, notInGbyCols)
		}
	}
	if len(notInGbyCols) == 0 {
		return nil
	}

	whereDepends := buildWhereFuncDepend(p, sel.Where)
	joinDepends := buildJoinFuncDepend(p, sel.From.TableRefs)
	tblMap := make(map[*model.TableInfo]struct{}, len(notInGbyCols))
	for col, errExprLoc := range notInGbyCols {
		tblInfo := tblInfoFromCol(sel.From.TableRefs, col)
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
			return ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, sel.Fields.Fields[errExprLoc.Offset].Text())
		case ErrExprInOrderBy:
			return ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, sel.OrderBy.Items[errExprLoc.Offset].Expr.Text())
		}
		return nil
	}
	return nil
}

func (b *planBuilder) checkOnlyFullGroupByWithOutGroupClause(p LogicalPlan, fields []*ast.SelectField) error {
	resolver := colResolverForOnlyFullGroupBy{}
	for idx, field := range fields {
		resolver.exprIdx = idx
		field.Accept(&resolver)
		err := resolver.Check()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// colResolverForOnlyFullGroupBy visits Expr tree to find out if an Expr tree is an aggregation function.
// If so, find out the first column name that not in an aggregation function.
type colResolverForOnlyFullGroupBy struct {
	firstNonAggCol       *ast.ColumnName
	exprIdx              int
	firstNonAggColIdx    int
	hasAggFuncOrAnyValue bool
}

func (c *colResolverForOnlyFullGroupBy) Enter(node ast.Node) (ast.Node, bool) {
	switch t := node.(type) {
	case *ast.AggregateFuncExpr:
		c.hasAggFuncOrAnyValue = true
		return node, true
	case *ast.FuncCallExpr:
		// enable function `any_value` in aggregation even `ONLY_FULL_GROUP_BY` is set
		if t.FnName.L == ast.AnyValue {
			c.hasAggFuncOrAnyValue = true
			return node, true
		}
	case *ast.ColumnNameExpr:
		if c.firstNonAggCol == nil {
			c.firstNonAggCol, c.firstNonAggColIdx = t.Name, c.exprIdx
		}
		return node, true
	case *ast.SubqueryExpr:
		return node, true
	}
	return node, false
}

func (c *colResolverForOnlyFullGroupBy) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

func (c *colResolverForOnlyFullGroupBy) Check() error {
	if c.hasAggFuncOrAnyValue && c.firstNonAggCol != nil {
		return ErrMixOfGroupFuncAndFields.GenWithStackByArgs(c.firstNonAggColIdx+1, c.firstNonAggCol.Name.O)
	}
	return nil
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

func (b *planBuilder) resolveGbyExprs(p LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (LogicalPlan, []expression.Expression, error) {
	b.curClause = groupByClause
	exprs := make([]expression.Expression, 0, len(gby.Items))
	resolver := &gbyResolver{
		ctx:    b.ctx,
		fields: fields,
		schema: p.Schema(),
	}
	for _, item := range gby.Items {
		resolver.inExpr = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			return nil, nil, errors.Trace(resolver.err)
		}
		if !resolver.isParam {
			item.Expr = retExpr.(ast.ExprNode)
		}

		itemExpr := retExpr.(ast.ExprNode)
		expr, np, err := b.rewrite(itemExpr, p, nil, true)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		exprs = append(exprs, expr)
		p = np
	}
	return p, exprs, nil
}

func (b *planBuilder) unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
	for i, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		if field.WildCard.Table.L == "" && i > 0 {
			return nil, ErrInvalidWildCard
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
			return nil, ErrBadTable.GenWithStackByArgs(tblName)
		}
	}
	return resultList, nil
}

func (b *planBuilder) pushTableHints(hints []*ast.TableOptimizerHint) bool {
	var sortMergeTables, INLJTables, hashJoinTables []hintTableInfo
	for _, hint := range hints {
		switch hint.HintName.L {
		case TiDBMergeJoin:
			sortMergeTables = tableNames2HintTableInfo(hint.Tables)
		case TiDBIndexNestedLoopJoin:
			INLJTables = tableNames2HintTableInfo(hint.Tables)
		case TiDBHashJoin:
			hashJoinTables = tableNames2HintTableInfo(hint.Tables)
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
	hintInfo := b.tableHintInfo[len(b.tableHintInfo)-1]
	b.appendUnmatchedJoinHintWarning(TiDBIndexNestedLoopJoin, hintInfo.indexNestedLoopJoinTables)
	b.appendUnmatchedJoinHintWarning(TiDBMergeJoin, hintInfo.sortMergeJoinTables)
	b.appendUnmatchedJoinHintWarning(TiDBHashJoin, hintInfo.hashJoinTables)
	b.tableHintInfo = b.tableHintInfo[:len(b.tableHintInfo)-1]
}

func (b *planBuilder) appendUnmatchedJoinHintWarning(joinType string, hintTables []hintTableInfo) {
	unMatchedTables := extractUnmatchedTables(hintTables)
	if len(unMatchedTables) == 0 {
		return
	}
	errMsg := fmt.Sprintf("There are no matching table names for (%s) in optimizer hint %s. Maybe you can use the table alias name",
		strings.Join(unMatchedTables, ", "), restore2JoinHint(joinType, hintTables))
	b.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

// TableHints returns the *tableHintInfo of PlanBuilder.
func (b *planBuilder) TableHints() *tableHintInfo {
	if len(b.tableHintInfo) == 0 {
		return nil
	}
	return &(b.tableHintInfo[len(b.tableHintInfo)-1])
}

func (b *planBuilder) buildSelect(sel *ast.SelectStmt) (p LogicalPlan, err error) {
	if b.pushTableHints(sel.TableHints) {
		// table hints are only visible in the current SELECT statement.
		defer b.popTableHints()
	}
	if sel.SelectStmtOpts != nil {
		origin := b.inStraightJoin
		b.inStraightJoin = sel.SelectStmtOpts.StraightJoin
		defer func() { b.inStraightJoin = origin }()
	}

	var (
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		gbyCols                       []expression.Expression
	)

	if sel.From != nil {
		p, err = b.buildResultSetNode(sel.From.TableRefs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		p = b.buildTableDual()
	}

	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if sel.GroupBy != nil {
		p, gbyCols, err = b.resolveGbyExprs(p, sel.GroupBy, sel.Fields.Fields)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() && sel.From != nil {
		err = b.checkOnlyFullGroupBy(p, sel)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	havingMap, orderMap, err = b.resolveHavingAndOrderBy(sel, p)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if sel.Where != nil {
		p, err = b.buildSelection(p, sel.Where, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if sel.LockTp != ast.SelectLockNone {
		p = b.buildSelectLock(p, sel.LockTp)
	}

	hasAgg := b.detectSelectAgg(sel)
	if hasAgg {
		aggFuncs, totalMap = b.extractAggFuncs(sel.Fields.Fields)
		var aggIndexMap map[int]int
		p, aggIndexMap, err = b.buildAggregation(p, aggFuncs, gbyCols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for k, v := range totalMap {
			totalMap[k] = aggIndexMap[v]
		}
	}

	var oldLen int
	p, oldLen, err = b.buildProjection(p, sel.Fields.Fields, totalMap)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if sel.Having != nil {
		b.curClause = havingClause
		p, err = b.buildSelection(p, sel.Having.Expr, havingMap)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if sel.Distinct {
		p, err = b.buildDistinct(p, oldLen)
		if err != nil {
			return nil, err
		}
	}

	if sel.OrderBy != nil {
		p, err = b.buildSort(p, sel.OrderBy.Items, orderMap)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if sel.Limit != nil {
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	sel.Fields.Fields = originalFields
	if oldLen != p.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.init(b.ctx)
		proj.SetChildren(p)
		schema := expression.NewSchema(p.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.SetSchema(schema)
		return proj, nil
	}

	return p, nil
}

func (b *planBuilder) buildTableDual() *LogicalTableDual {
	return LogicalTableDual{RowCount: 1}.init(b.ctx)
}

func (ds *DataSource) newExtraHandleSchemaCol() *expression.Column {
	return &expression.Column{
		DBName:   ds.DBName,
		TblName:  ds.tableInfo.Name,
		ColName:  model.ExtraHandleName,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: ds.ctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
	}
}

// getStatsTable gets statistics information for a table specified by "tableID".
// A pseudo statistics table is returned in any of the following scenario:
// 1. tidb-server started and statistics handle has not been initialized.
// 2. table row count from statistics is zero.
// 3. statistics is outdated.
func getStatsTable(ctx sessionctx.Context, tblInfo *model.TableInfo, pid int64) *statistics.Table {
	statsHandle := domain.GetDomain(ctx).StatsHandle()

	// 1. tidb-server started and statistics handle has not been initialized.
	if statsHandle == nil {
		return statistics.PseudoTable(tblInfo)
	}

	var statsTbl *statistics.Table
	if pid != tblInfo.ID {
		statsTbl = statsHandle.GetPartitionStats(tblInfo, pid)
	} else {
		statsTbl = statsHandle.GetTableStats(tblInfo)
	}

	// 2. table row count from statistics is zero.
	if statsTbl.Count == 0 {
		return statistics.PseudoTable(tblInfo)
	}

	// 3. statistics is outdated.
	if statsTbl.IsOutdated() {
		tbl := *statsTbl
		tbl.Pseudo = true
		statsTbl = &tbl
		metrics.PseudoEstimation.Inc()
	}
	return statsTbl
}

func (b *planBuilder) buildDataSource(tn *ast.TableName) (LogicalPlan, error) {
	dbName := tn.Schema
	if dbName.L == "" {
		dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}

	tbl, err := b.is.TableByName(dbName, tn.Name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableInfo := tbl.Meta()
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName.L, tableInfo.Name.L, "")

	if tableInfo.GetPartitionInfo() != nil {
		b.optFlag = b.optFlag | flagPartitionProcessor
	}

	possiblePaths, err := getPossibleAccessPaths(tn.IndexHints, tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var columns []*table.Column
	if b.inUpdateStmt {
		columns = tbl.WritableCols()
	} else {
		columns = tbl.Cols()
	}
	var statisticTable *statistics.Table
	if _, ok := tbl.(table.PartitionedTable); !ok {
		statisticTable = getStatsTable(b.ctx, tbl.Meta(), tbl.Meta().ID)
	}

	ds := DataSource{
		DBName:              dbName,
		table:               tbl,
		tableInfo:           tableInfo,
		statisticTable:      statisticTable,
		indexHints:          tn.IndexHints,
		possibleAccessPaths: possiblePaths,
		Columns:             make([]*model.ColumnInfo, 0, len(columns)),
	}.init(b.ctx)

	var handleCol *expression.Column
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	for _, col := range columns {
		ds.Columns = append(ds.Columns, col.ToInfo())
		newCol := &expression.Column{
			UniqueID:    b.ctx.GetSessionVars().AllocPlanColumnID(),
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigColName: col.Name,
			ID:          col.ID,
			RetType:     &col.FieldType,
		}

		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			handleCol = newCol
		}
		schema.Append(newCol)
	}
	ds.SetSchema(schema)

	// We append an extra handle column to the schema when "ds" is not a memory
	// table e.g. table in the "INFORMATION_SCHEMA" database, and the handle
	// column is not the primary key of "ds".
	isMemDB := infoschema.IsMemoryDB(ds.DBName.L)
	if !isMemDB && handleCol == nil {
		ds.Columns = append(ds.Columns, model.NewExtraHandleColInfo())
		handleCol = ds.newExtraHandleSchemaCol()
		schema.Append(handleCol)
	}
	if handleCol != nil {
		schema.TblID2Handle[tableInfo.ID] = []*expression.Column{handleCol}
	}

	var result LogicalPlan = ds

	// If this SQL is executed in a non-readonly transaction, we need a
	// "UnionScan" operator to read the modifications of former SQLs, which is
	// buffered in tidb-server memory.
	txn, err := b.ctx.Txn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if txn.Valid() && !txn.IsReadOnly() {
		us := LogicalUnionScan{}.init(b.ctx)
		us.SetChildren(ds)
		result = us
	}

	// If this table contains any virtual generated columns, we need a
	// "Projection" to calculate these columns.
	proj, err := b.projectVirtualColumns(ds, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if proj != nil {
		proj.SetChildren(result)
		result = proj
	}
	return result, nil
}

// projectVirtualColumns is only for DataSource. If some table has virtual generated columns,
// we add a projection on the original DataSource, and calculate those columns in the projection
// so that plans above it can reference generated columns by their name.
func (b *planBuilder) projectVirtualColumns(ds *DataSource, columns []*table.Column) (*LogicalProjection, error) {
	var hasVirtualGeneratedColumn = false
	for _, column := range columns {
		if column.IsGenerated() && !column.GeneratedStored {
			hasVirtualGeneratedColumn = true
			break
		}
	}
	if !hasVirtualGeneratedColumn {
		return nil, nil
	}
	var proj = LogicalProjection{
		Exprs:            make([]expression.Expression, 0, len(columns)),
		calculateGenCols: true,
	}.init(b.ctx)
	for i, colExpr := range ds.Schema().Columns {
		var exprIsGen = false
		var expr expression.Expression
		if i < len(columns) {
			if columns[i].IsGenerated() && !columns[i].GeneratedStored {
				var err error
				expr, _, err = b.rewrite(columns[i].GeneratedExpr, ds, nil, true)
				if err != nil {
					return nil, errors.Trace(err)
				}
				// Because the expression might return different type from
				// the generated column, we should wrap a CAST on the result.
				expr = expression.BuildCastFunction(b.ctx, expr, colExpr.GetType())
				exprIsGen = true
			}
		}
		if !exprIsGen {
			expr = colExpr
		}
		proj.Exprs = append(proj.Exprs, expr)
	}

	// Re-iterate expressions to handle those virtual generated columns that refers to the other generated columns, for
	// example, given:
	//  column a, column b as (a * 2), column c as (b + 1)
	// we'll get:
	//  column a, column b as (a * 2), column c as ((a * 2) + 1)
	// A generated column definition can refer to only generated columns occurring earlier in the table definition, so
	// it's safe to iterate in index-ascending order.
	for i, expr := range proj.Exprs {
		proj.Exprs[i] = expression.ColumnSubstitute(expr, ds.Schema(), proj.Exprs)
	}
	proj.SetSchema(ds.Schema().Clone())
	return proj, nil
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
func (b *planBuilder) buildSemiApply(outerPlan, innerPlan LogicalPlan, condition []expression.Expression, asScalar, not bool) (LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPredicatePushDown
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagDecorrelate

	join, err := b.buildSemiJoin(outerPlan, innerPlan, condition, asScalar, not)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ap := &LogicalApply{LogicalJoin: *join}
	ap.tp = TypeApply
	ap.self = ap
	return ap, nil
}

func (b *planBuilder) buildMaxOneRow(p LogicalPlan) LogicalPlan {
	maxOneRow := LogicalMaxOneRow{}.init(b.ctx)
	maxOneRow.SetChildren(p)
	return maxOneRow
}

func (b *planBuilder) buildSemiJoin(outerPlan, innerPlan LogicalPlan, onCondition []expression.Expression, asScalar bool, not bool) (*LogicalJoin, error) {
	joinPlan := LogicalJoin{}.init(b.ctx)
	for i, expr := range onCondition {
		onCondition[i] = expr.Decorrelate(outerPlan.Schema())
	}
	joinPlan.SetChildren(outerPlan, innerPlan)
	joinPlan.attachOnConds(onCondition)
	if asScalar {
		newSchema := outerPlan.Schema().Clone()
		newSchema.Append(&expression.Column{
			ColName:     model.NewCIStr(fmt.Sprintf("%d_aux_0", joinPlan.id)),
			RetType:     types.NewFieldType(mysql.TypeTiny),
			IsAggOrSubq: true,
			UniqueID:    b.ctx.GetSessionVars().AllocPlanColumnID(),
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
		if b.TableHints().ifPreferINLJ(innerAlias) {
			joinPlan.preferJoinType = preferRightAsIndexInner
		}
		// If there're multiple join hints, they're conflict.
		if bits.OnesCount(joinPlan.preferJoinType) > 1 {
			return nil, errors.New("Join hints are conflict, you can only specify one type of join")
		}
	}
	return joinPlan, nil
}

func (b *planBuilder) buildUpdate(update *ast.UpdateStmt) (Plan, error) {
	if b.pushTableHints(update.TableHints) {
		// table hints are only visible in the current UPDATE statement.
		defer b.popTableHints()
	}

	// update subquery table should be forbidden
	var asNameList []string
	asNameList = extractTableSourceAsNames(update.TableRefs.TableRefs, asNameList, true)
	for _, asName := range asNameList {
		for _, assign := range update.List {
			if assign.Column.Table.L == asName {
				return nil, ErrNonUpdatableTable.GenWithStackByArgs(asName, "UPDATE")
			}
		}
	}

	b.inUpdateStmt = true
	sel := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    update.TableRefs,
		Where:   update.Where,
		OrderBy: update.Order,
		Limit:   update.Limit,
	}

	p, err := b.buildResultSetNode(sel.From.TableRefs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tableList []*ast.TableName
	tableList = extractTableList(sel.From.TableRefs, tableList, false)
	for _, t := range tableList {
		dbName := t.Schema.L
		if dbName == "" {
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName, t.Name.L, "")
	}

	oldSchema := p.Schema().Clone()
	if sel.Where != nil {
		p, err = b.buildSelection(p, sel.Where, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if sel.OrderBy != nil {
		p, err = b.buildSort(p, sel.OrderBy.Items, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if sel.Limit != nil {
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// TODO: expression rewriter should not change the output columns. We should cut the columns here.
	if p.Schema().Len() != oldSchema.Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(oldSchema.Columns)}.init(b.ctx)
		proj.SetSchema(oldSchema)
		proj.SetChildren(p)
		p = proj
	}

	var updateTableList []*ast.TableName
	updateTableList = extractTableList(sel.From.TableRefs, updateTableList, true)
	orderedList, np, err := b.buildUpdateLists(updateTableList, update.List, p)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p = np

	updt := Update{OrderedList: orderedList}.init(b.ctx)
	updt.SetSchema(p.Schema())
	// We cannot apply projection elimination when building the subplan, because
	// columns in orderedList cannot be resolved.
	updt.SelectPlan, err = doOptimize(b.optFlag&^flagEliminateProjection, p)
	if err != nil {
		return nil, errors.Trace(err)
	}
	updt.ResolveIndices()
	return updt, nil
}

func (b *planBuilder) buildUpdateLists(tableList []*ast.TableName, list []*ast.Assignment, p LogicalPlan) ([]*expression.Assignment, LogicalPlan, error) {
	b.curClause = fieldList
	modifyColumns := make(map[string]struct{}, p.Schema().Len()) // Which columns are in set list.
	for _, assign := range list {
		col, _, err := p.findColumn(assign.Column)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		columnFullName := fmt.Sprintf("%s.%s.%s", col.DBName.L, col.TblName.L, col.ColName.L)
		modifyColumns[columnFullName] = struct{}{}
	}

	// If columns in set list contains generated columns, raise error.
	// And, fill virtualAssignments here; that's for generated columns.
	virtualAssignments := make([]*ast.Assignment, 0)

	for _, tn := range tableList {
		tableInfo := tn.TableInfo
		tableVal, found := b.is.TableByID(tableInfo.ID)
		if !found {
			return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tn.DBInfo.Name.O, tableInfo.Name.O)
		}
		for i, colInfo := range tableInfo.Columns {
			if !colInfo.IsGenerated() {
				continue
			}
			columnFullName := fmt.Sprintf("%s.%s.%s", tn.Schema.L, tn.Name.L, colInfo.Name.L)
			if _, ok := modifyColumns[columnFullName]; ok {
				return nil, nil, ErrBadGeneratedColumn.GenWithStackByArgs(colInfo.Name.O, tableInfo.Name.O)
			}
			virtualAssignments = append(virtualAssignments, &ast.Assignment{
				Column: &ast.ColumnName{Schema: tn.Schema, Table: tn.Name, Name: colInfo.Name},
				Expr:   tableVal.Cols()[i].GeneratedExpr,
			})
		}
	}

	newList := make([]*expression.Assignment, 0, p.Schema().Len())
	allAssignments := append(list, virtualAssignments...)
	for i, assign := range allAssignments {
		col, _, err := p.findColumn(assign.Column)
		if err != nil {
			return nil, nil, errors.Trace(err)
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
				default:
					return expr
				}
			}
			newExpr, np, err = b.rewriteWithPreprocess(assign.Expr, p, nil, false, rewritePreprocess)
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		p = np
		newList = append(newList, &expression.Assignment{Col: col, Expr: newExpr})
	}

	tblDbMap := make(map[string]string, len(tableList))
	for _, tbl := range tableList {
		tblDbMap[tbl.Name.L] = tbl.DBInfo.Name.L
	}
	for _, assign := range newList {
		col := assign.Col

		dbName := col.DBName.L
		// To solve issue#10028, we need to get database name by the table alias name.
		if dbNameTmp, ok := tblDbMap[col.TblName.L]; ok {
			dbName = dbNameTmp
		}
		if dbName == "" {
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, dbName, col.OrigTblName.L, "")
	}
	return newList, p, nil
}

func (b *planBuilder) buildDelete(delete *ast.DeleteStmt) (Plan, error) {
	if b.pushTableHints(delete.TableHints) {
		// table hints are only visible in the current DELETE statement.
		defer b.popTableHints()
	}

	sel := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    delete.TableRefs,
		Where:   delete.Where,
		OrderBy: delete.Order,
		Limit:   delete.Limit,
	}
	p, err := b.buildResultSetNode(sel.From.TableRefs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	oldSchema := p.Schema()
	oldLen := oldSchema.Len()

	if sel.Where != nil {
		p, err = b.buildSelection(p, sel.Where, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if sel.OrderBy != nil {
		p, err = b.buildSort(p, sel.OrderBy.Items, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if sel.Limit != nil {
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Add a projection for the following case, otherwise the final schema will be the schema of the join.
	// delete from t where a in (select ...) or b in (select ...)
	if !delete.IsMultiTable && oldLen != p.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.init(b.ctx)
		proj.SetChildren(p)
		proj.SetSchema(oldSchema.Clone())
		p = proj
	}

	var tables []*ast.TableName
	if delete.Tables != nil {
		tables = delete.Tables.Tables
	}

	del := Delete{
		Tables:       tables,
		IsMultiTable: delete.IsMultiTable,
	}.init(b.ctx)

	del.SelectPlan, err = doOptimize(b.optFlag, p)
	if err != nil {
		return nil, errors.Trace(err)
	}

	del.SetSchema(expression.NewSchema())

	var tableList []*ast.TableName
	tableList = extractTableList(delete.TableRefs.TableRefs, tableList, true)

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
				asNameList = extractTableSourceAsNames(delete.TableRefs.TableRefs, asNameList, false)
				for _, asName := range asNameList {
					tblName := tn.Name.L
					if tn.Schema.L != "" {
						tblName = tn.Schema.L + "." + tblName
					}
					if asName == tblName {
						// check sql like: `delete a from (select * from t) as a, t`
						return nil, ErrNonUpdatableTable.GenWithStackByArgs(tn.Name.O, "DELETE")
					}
				}
				// check sql like: `delete b from (select * from t) as a, t`
				return nil, ErrUnknownTable.GenWithStackByArgs(tn.Name.O, "MULTI DELETE")
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

	return del, nil
}

// extractTableList extracts all the TableNames from node.
// If asName is true, extract AsName prior to OrigName.
// Privilege check should use OrigName, while expression may use AsName.
func extractTableList(node ast.ResultSetNode, input []*ast.TableName, asName bool) []*ast.TableName {
	switch x := node.(type) {
	case *ast.Join:
		input = extractTableList(x.Left, input, asName)
		input = extractTableList(x.Right, input, asName)
	case *ast.TableSource:
		if s, ok := x.Source.(*ast.TableName); ok {
			if x.AsName.L != "" && asName {
				newTableName := *s
				newTableName.Name = x.AsName
				newTableName.Schema = model.NewCIStr("")
				input = append(input, &newTableName)
			} else {
				input = append(input, s)
			}
		}
	}
	return input
}

// extractTableSourceAsNames extracts TableSource.AsNames from node.
// if onlySelectStmt is set to be true, only extracts AsNames when TableSource.Source.(type) == *ast.SelectStmt
func extractTableSourceAsNames(node ast.ResultSetNode, input []string, onlySelectStmt bool) []string {
	switch x := node.(type) {
	case *ast.Join:
		input = extractTableSourceAsNames(x.Left, input, onlySelectStmt)
		input = extractTableSourceAsNames(x.Right, input, onlySelectStmt)
	case *ast.TableSource:
		if _, ok := x.Source.(*ast.SelectStmt); !ok && onlySelectStmt {
			break
		}
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

func getInnerFromParenthesesAndUnaryPlus(expr ast.ExprNode) ast.ExprNode {
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		return getInnerFromParenthesesAndUnaryPlus(pexpr.Expr)
	}
	if uexpr, ok := expr.(*ast.UnaryOperationExpr); ok && uexpr.Op == opcode.Plus {
		return getInnerFromParenthesesAndUnaryPlus(uexpr.V)
	}
	return expr
}
