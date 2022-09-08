// Copyright 2022 PingCAP, Inc.
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

package sqlrestorer

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/exp/slices"
)

func (q *QueryBlock) findAndMarkColName(uid int64, useAndMarkProjected bool) (res string, ok bool) {
	if tblCol, ok := q.tableCols[uid]; ok {
		if len(tblCol.TblName) > 0 {
			res += tblCol.TblName + "."
		}
		res += tblCol.ColName
		return res, true
	} else if col, ok := q.projectedCols[uid]; ok {
		if useAndMarkProjected {
			col.Needed = true
			res += col.AsName
		} else {
			res += col.Expr
		}
		return res, true
	}
	return "", false
}

// String print the QueryBlock into a SELECT statement.
// Note: Output columns should be set for the QueryBlock before call this method.
func (q *QueryBlock) String() string {
	var builder strings.Builder
	builder.WriteString("SELECT ")
	first := true

	// Make the order of the select list stable to make the tests easier.
	uids := make([]int64, 0, len(q.outputCol))
	for uid := range q.outputCol {
		uids = append(uids, uid)
	}
	slices.Sort(uids)
	for _, uid := range uids {
		asName := q.outputCol[uid]
		if !first {
			builder.WriteString(", ")
		}
		name, _ := q.findAndMarkColName(uid, false)
		builder.WriteString(name)
		if len(asName) == 0 || asName == name {
			continue
		}
		builder.WriteString(" AS " + asName)
		first = false
	}

	uids = uids[:0]
	for uid := range q.projectedCols {
		uids = append(uids, uid)
	}
	slices.Sort(uids)
	for _, uid := range uids {
		col := q.projectedCols[uid]
		if !col.Needed {
			continue
		}
		if _, ok := q.outputCol[uid]; ok {
			continue
		}
		builder.WriteString(", ")
		builder.WriteString(col.Expr + " AS " + col.AsName)
	}

	builder.WriteString(" FROM ")
	builder.WriteString(q.joinList.String())

	if len(q.WhereConds) > 0 {
		builder.WriteString(" WHERE ")
		first = true
		for _, cond := range q.WhereConds {
			if !first {
				builder.WriteString(" AND ")
			}
			builder.WriteString(cond)
			first = false
		}
	}
	if len(q.GroupByCols) > 0 {
		builder.WriteString(" GROUP BY ")
		first = true
		for _, groupBy := range q.GroupByCols {
			if !first {
				builder.WriteString(", ")
			}
			builder.WriteString(groupBy)
			first = false
		}
	}
	if len(q.HavingConds) > 0 {
		builder.WriteString(" HAVING ")
		first = true
		for _, cond := range q.HavingConds {
			if !first {
				builder.WriteString(" AND ")
			}
			builder.WriteString(cond)
			first = false
		}
	}
	if q.Limit > 0 {
		builder.WriteString(" LIMIT ")
		builder.WriteString(fmt.Sprintf("%d", q.Limit))
		if q.Offset > 0 {
			builder.WriteString(" OFFSET ")
			builder.WriteString(fmt.Sprintf("%d", q.Offset))
		}
	}
	return builder.String()
}

func (jl joinList) String() string {
	res := ""
	first := true
	for _, item := range jl {
		if !first {
			res += " " + item.JoinType + " "
		}
		if len(item.Table) > 0 {
			res += item.Table
		} else if len(item.SubJoinList) > 0 {
			res += "(" + item.SubJoinList.String() + ")"
		} else {
			res += "(" + item.SubQuery.String() + ")"
		}
		if len(item.AsName) > 0 {
			res += " AS " + item.AsName
		}
		if !first && len(item.JoinCond) > 0 {
			res += " ON "
			first2 := true
			for _, cond := range item.JoinCond {
				if !first2 {
					res += " AND "
				}
				res += cond
				first2 = false
			}
		}
		first = false
	}
	return res
}

// ExprToString print an expression as a string.
// useProjectedCol means if projected columns can be used when printing.
//
// Example:
//
//	There is a projected column: a + 10 as n, and n's unique ID is 2.
//	When trying to print a column with unique ID is 2, if useProjectedCol is true, it will be n,
//	 otherwise, it will be a + 10
func (q *QueryBlock) ExprToString(e expression.Expression, useProjectedCol bool) (string, error) {
	switch expr := e.(type) {
	case *expression.ScalarFunction:
		var buffer bytes.Buffer
		buffer.WriteString("`" + expr.FuncName.L + "`(")
		for i, arg := range expr.GetArgs() {
			argStr, err := q.ExprToString(arg, useProjectedCol)
			if err != nil {
				return "", err
			}
			buffer.WriteString(argStr)
			if i+1 != len(expr.GetArgs()) {
				buffer.WriteString(", ")
			}
		}
		// TODO: CAST is not equal to the original expression, but this doesn't affect cardinality estimation.
		buffer.WriteString(")")
		return buffer.String(), nil
	case *expression.Column:
		uid := expr.UniqueID
		name, ok := q.findAndMarkColName(uid, useProjectedCol)
		if ok {
			return name, nil
		}
		return unknownColumnPlaceholder + strconv.FormatInt(uid, 10), nil
	case *expression.CorrelatedColumn:
		uid := expr.Column.UniqueID
		return unknownColumnPlaceholder + strconv.FormatInt(uid, 10), nil
	case *expression.Constant:
		value, err := expr.Eval(chunk.Row{})
		if err != nil {
			return "", err
		}
		valueExpr := driver.ValueExpr{Datum: value}
		var buffer bytes.Buffer
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buffer)
		err = valueExpr.Restore(restoreCtx)
		if err != nil {
			return "", err
		}
		return buffer.String(), nil
	}
	return "", errors.New("unexpected type of Expression")
}

// ByItemToString print ByItems as a string.
func (q *QueryBlock) ByItemToString(items []*util.ByItems, useProjectedCol bool) (string, error) {
	str := ""
	for i, item := range items {
		if i != 0 {
			str += ", "
		}
		s, err := q.ExprToString(item.Expr, useProjectedCol)
		if err != nil {
			return s, err
		}
		str += s
		if item.Desc {
			str += " DESC "
		}
	}
	return str, nil
}

// AggFuncToString print an aggregate function as a string.
func (q *QueryBlock) AggFuncToString(agg *aggregation.AggFuncDesc) (string, error) {
	if agg.Name == "firstrow" {
		// Projected columns can't be used in aggregate functions, so useProjectedCol is false.
		s, err := q.ExprToString(agg.Args[0], false)
		if err != nil {
			return s, err
		}
		return s, nil
	}
	str := agg.Name + "("
	first := true
	for i := 0; i < len(agg.Args)-1; i++ {
		if !first {
			str += ", "
			first = false
		}
		s, err := q.ExprToString(agg.Args[i], false)
		if err != nil {
			return s, err
		}
		str += s
	}
	if agg.Name == "group_concat" {
		if agg.OrderByItems != nil {
			str += " ORDER BY "
			s, err := q.ByItemToString(agg.OrderByItems, false)
			if err != nil {
				return s, err
			}
			str += s
		}
		str += " SEPARATOR "
		s, err := q.ExprToString(agg.Args[len(agg.Args)-1], false)
		if err != nil {
			return s, err
		}
		str += s
	} else {
		if !first {
			str += ", "
		}
		s, err := q.ExprToString(agg.Args[len(agg.Args)-1], false)
		if err != nil {
			return s, err
		}
		str += s
	}
	str += ")"
	return str, nil
}

// SemiJoinToExprString convert a [Anti] [LeftOuter] SemiJoin to an expression.
func (q *QueryBlock) SemiJoinToExprString(isAntiJoin bool, joinConds []expression.Expression,
	leftChildSchema, rightChildSchema *expression.Schema, right *QueryBlock) (string, error) {
	var inCondsLeftCols, inCondsRightCols []*expression.Column
	var otherConds []expression.Expression
	// 1. Split into null-aware conds and non null-aware conditions.
	// (Currently, only eq conds could be null-aware)
	for _, cond := range joinConds {
		if expression.IsEQCondFromIn(cond) {
			cols := expression.ExtractColumns(cond)
			for _, col := range cols {
				if leftChildSchema.Contains(col) {
					inCondsLeftCols = append(inCondsLeftCols, col)
				} else if rightChildSchema.Contains(col) {
					inCondsRightCols = append(inCondsRightCols, col)
				}
			}
		} else {
			otherConds = append(otherConds, cond)
		}
	}
	// 2. Handle the non null-aware conditions by pushing them into subquery as where conditions
	if len(otherConds) > 0 {
		cols := expression.ExtractColumnsFromExpressions(nil, otherConds, nil)
		// We'll add conditions to the WHERE clause of the inner (right) query, so we make sure it's not after StageWhere.
		right = right.GenQBNotAfter(StageWhere)
		for _, expr := range otherConds {
			s, err := right.ExprToString(expr, true)
			if err != nil {
				return "", err
			}
			right.WhereConds = append(right.WhereConds, s)
		}
		for _, col := range cols {
			if leftChildSchema.Contains(col) {
				right.Decorrelate(col.UniqueID, q)
			}
		}
	}
	// 3. Handle the null-aware conditions and generate the expression for the outer query
	if len(inCondsLeftCols) != len(inCondsRightCols) {
		return "", errors.New("unexpected SemiJoin condition")
	}
	expr := ""
	if len(inCondsLeftCols) > 0 {
		// We want the select list of the inner (right) query only contains the columns that we want,
		// so we make sure it's not after StageProjection here.
		right = right.GenQBNotAfter(StageProjection)
		for _, col := range inCondsRightCols {
			right.AddOutputCol(col.UniqueID)
		}
		inExprRightPart := "(" + right.String() + ")"
		var leftCols []string
		for _, col := range inCondsLeftCols {
			s, err := q.ExprToString(col, false)
			if err != nil {
				return "", err
			}
			leftCols = append(leftCols, s)
		}
		inExprLeftPart := ""
		if len(leftCols) == 1 {
			inExprLeftPart = leftCols[0]
		} else {
			inExprLeftPart = "(" + strings.Join(leftCols, ", ") + ")"
		}
		if !isAntiJoin {
			expr = inExprLeftPart + " IN " + inExprRightPart
		} else {
			expr = inExprLeftPart + " NOT IN " + inExprRightPart
		}
	} else {
		for _, col := range rightChildSchema.Columns {
			right.AddOutputCol(col.UniqueID)
		}
		if !isAntiJoin {
			expr = "EXISTS (" + right.String() + ")"
		} else {
			expr = "NOT EXISTS (" + right.String() + ")"
		}
	}
	return expr, nil
}
