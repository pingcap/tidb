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

package plan

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// Refine tries to build index or table range.
func Refine(p Plan) error {
	return refine(p)
}

func refine(in Plan) error {
	for _, c := range in.GetChildren() {
		e := refine(c)
		if e != nil {
			return errors.Trace(e)
		}
	}

	var err error
	switch x := in.(type) {
	case *IndexScan:
		err = buildIndexRange(x)
	case *Limit:
		x.SetLimit(0)
	case *TableScan:
		err = buildTableRange(x)
	case *PhysicalTableScan:
		x.Ranges = []TableRange{{math.MinInt64, math.MaxInt64}}
	case *PhysicalIndexScan:
		rb := rangeBuilder{}
		x.Ranges = rb.buildIndexRanges(fullRange)
	case *Selection:
		err = buildSelection(x)
	case *PhysicalApply:
		err = refine(x.InnerPlan)
	}
	return errors.Trace(err)
}

var fullRange = []rangePoint{
	{start: true},
	{value: types.MaxValueDatum()},
}

func buildIndexRange(p *IndexScan) error {
	rb := rangeBuilder{}
	if p.AccessEqualCount > 0 {
		// Build ranges for equal access conditions.
		point := rb.build(p.AccessConditions[0])
		p.Ranges = rb.buildIndexRanges(point)
		for i := 1; i < p.AccessEqualCount; i++ {
			point = rb.build(p.AccessConditions[i])
			p.Ranges = rb.appendIndexRanges(p.Ranges, point)
		}
	}
	rangePoints := fullRange
	// Build rangePoints for non-equal access condtions.
	for i := p.AccessEqualCount; i < len(p.AccessConditions); i++ {
		rangePoints = rb.intersection(rangePoints, rb.build(p.AccessConditions[i]))
	}
	if p.AccessEqualCount == 0 {
		p.Ranges = rb.buildIndexRanges(rangePoints)
	} else if p.AccessEqualCount < len(p.AccessConditions) {
		p.Ranges = rb.appendIndexRanges(p.Ranges, rangePoints)
	}

	// Take prefix index into consideration.
	if p.Index.HasPrefixIndex() {
		for i := 0; i < len(p.Ranges); i++ {
			refineRange(p.Ranges[i], p.Index)
		}
	}
	return errors.Trace(rb.err)
}

func buildNewIndexRange(p *PhysicalIndexScan) error {
	rb := rangeBuilder{}
	if p.accessEqualCount > 0 {
		// Build ranges for equal access conditions.
		point := rb.newBuild(p.AccessCondition[0])
		p.Ranges = rb.buildIndexRanges(point)
		for i := 1; i < p.accessEqualCount; i++ {
			point = rb.newBuild(p.AccessCondition[i])
			p.Ranges = rb.appendIndexRanges(p.Ranges, point)
		}
	}
	rangePoints := fullRange
	// Build rangePoints for non-equal access condtions.
	for i := p.accessEqualCount; i < len(p.AccessCondition); i++ {
		rangePoints = rb.intersection(rangePoints, rb.newBuild(p.AccessCondition[i]))
	}
	if p.accessEqualCount == 0 {
		p.Ranges = rb.buildIndexRanges(rangePoints)
	} else if p.accessEqualCount < len(p.AccessCondition) {
		p.Ranges = rb.appendIndexRanges(p.Ranges, rangePoints)
	}

	// Take prefix index into consideration.
	if p.Index.HasPrefixIndex() {
		for i := 0; i < len(p.Ranges); i++ {
			refineRange(p.Ranges[i], p.Index)
		}
	}
	return errors.Trace(rb.err)
}

// refineRange changes the IndexRange taking prefix index length into consideration.
func refineRange(v *IndexRange, idxInfo *model.IndexInfo) {
	for i := 0; i < len(v.LowVal); i++ {
		refineRangeDatum(&v.LowVal[i], idxInfo.Columns[i])
		v.LowExclude = false
	}

	for i := 0; i < len(v.HighVal); i++ {
		refineRangeDatum(&v.HighVal[i], idxInfo.Columns[i])
		v.HighExclude = false
	}
}

func refineRangeDatum(v *types.Datum, ic *model.IndexColumn) {
	if ic.Length != types.UnspecifiedLength {
		// if index prefix length is used, change scan range.
		if ic.Length < len(v.GetBytes()) {
			v.SetBytes(v.GetBytes()[:ic.Length])
		}
	}
}

func buildTableRange(p *TableScan) error {
	if len(p.AccessConditions) == 0 {
		p.Ranges = []TableRange{{math.MinInt64, math.MaxInt64}}
		return nil
	}
	rb := rangeBuilder{}
	rangePoints := fullRange
	for _, cond := range p.AccessConditions {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
	}
	p.Ranges = rb.buildTableRanges(rangePoints)
	return errors.Trace(rb.err)
}

func buildSelection(p *Selection) error {
	var err error
	switch v := p.GetChildByIndex(0).(type) {
	case *PhysicalTableScan:
		v.AccessCondition, p.Conditions = detachTableScanConditions(p.Conditions, v.Table)
		err = buildNewTableRange(v)
	case *PhysicalIndexScan:
		v.AccessCondition, p.Conditions = detachIndexScanConditions(p.Conditions, v)
		err = buildNewIndexRange(v)
	}
	return errors.Trace(err)
}

// getEQFunctionOffset judge if the expression is a eq function like A = 1 where a is an index.
// If so, it will return the offset of A in index columns. e.g. for index(C,B,A), A's offset is 2.
func getEQFunctionOffset(expr expression.Expression, cols []*model.IndexColumn) int {
	f, ok := expr.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		return -1
	}
	if c, ok := f.Args[0].(*expression.Column); ok {
		if _, ok := f.Args[1].(*expression.Constant); ok {
			for i, col := range cols {
				if col.Name.L == c.ColName.L {
					return i
				}
			}
		}
	} else if _, ok := f.Args[0].(*expression.Constant); ok {
		if c, ok := f.Args[1].(*expression.Column); ok {
			for i, col := range cols {
				if col.Name.L == c.ColName.L {
					return i
				}
			}
		}
	}
	return -1
}

func detachIndexScanConditions(conditions []expression.Expression, indexScan *PhysicalIndexScan) ([]expression.Expression, []expression.Expression) {
	accessConds := make([]expression.Expression, len(indexScan.Index.Columns))
	var filterConds []expression.Expression
	for _, cond := range conditions {
		offset := getEQFunctionOffset(cond, indexScan.Index.Columns)
		if offset != -1 {
			accessConds[offset] = cond
		}
	}
	for i, cond := range accessConds {
		if cond == nil {
			accessConds = accessConds[:i]
			indexScan.accessEqualCount = i
			break
		}
		if indexScan.Index.Columns[i].Length != types.UnspecifiedLength {
			filterConds = append(filterConds, cond)
		}
		if i == len(accessConds)-1 {
			indexScan.accessEqualCount = len(accessConds)
		}
	}

	checker := &conditionChecker{
		tableName:    indexScan.Table.Name,
		idx:          indexScan.Index,
		columnOffset: indexScan.accessEqualCount,
	}
	for _, cond := range conditions {
		isAccess := false
		for _, acCond := range accessConds {
			if cond == acCond {
				isAccess = true
				break
			}
		}
		if isAccess {
			continue
		}
		if indexScan.accessEqualCount >= len(indexScan.Index.Columns) ||
			!checker.newCheck(cond) {
			filterConds = append(filterConds, cond)
			continue
		}
		accessConds = append(accessConds, cond)
		if indexScan.Index.Columns[indexScan.accessEqualCount].Length != types.UnspecifiedLength ||
			// TODO: it will lead to repeated compution cost.
			checker.isReserved {
			filterConds = append(filterConds, cond)
			checker.isReserved = false
		}
	}
	for i, cond := range accessConds {
		accessConds[i] = pushDownNot(cond, false)
	}
	return accessConds, filterConds
}

// detachTableScanConditions distinguishes between access conditions and filter conditions from conditions.
func detachTableScanConditions(conditions []expression.Expression, table *model.TableInfo) ([]expression.Expression, []expression.Expression) {
	var pkName model.CIStr
	if table.PKIsHandle {
		for _, colInfo := range table.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				pkName = colInfo.Name
				break
			}
		}
	}
	if pkName.L == "" {
		return nil, conditions
	}

	var accessConditions, filterConditions []expression.Expression
	checker := conditionChecker{
		tableName: table.Name,
		pkName:    pkName}
	for _, cond := range conditions {
		if !checker.newCheck(cond) {
			filterConditions = append(filterConditions, cond)
			continue
		}
		accessConditions = append(accessConditions, cond)
		// TODO: it will lead to repeated compution cost.
		if checker.isReserved {
			filterConditions = append(filterConditions, cond)
			checker.isReserved = false
		}
	}
	for i, cond := range accessConditions {
		accessConditions[i] = pushDownNot(cond, false)
	}

	return accessConditions, filterConditions
}

func buildNewTableRange(p *PhysicalTableScan) error {
	if len(p.AccessCondition) == 0 {
		p.Ranges = []TableRange{{math.MinInt64, math.MaxInt64}}
		return nil
	}

	rb := rangeBuilder{}
	rangePoints := fullRange
	for _, cond := range p.AccessCondition {
		rangePoints = rb.intersection(rangePoints, rb.newBuild(cond))
		if rb.err != nil {
			return errors.Trace(rb.err)
		}
	}
	p.Ranges = rb.buildTableRanges(rangePoints)
	return errors.Trace(rb.err)
}

// conditionChecker checks if this condition can be pushed to index plan.
type conditionChecker struct {
	tableName    model.CIStr
	idx          *model.IndexInfo
	columnOffset int // the offset of the indexed column to be checked.
	pkName       model.CIStr
	isReserved   bool // check if a access condition can be reserved to filter conditions.
}

func (c *conditionChecker) check(condition ast.ExprNode) bool {
	switch x := condition.(type) {
	case *ast.BinaryOperationExpr:
		return c.checkBinaryOperation(x)
	case *ast.BetweenExpr:
		if ast.IsPreEvaluable(x.Left) && ast.IsPreEvaluable(x.Right) && c.checkColumnExpr(x.Expr) {
			return true
		}
	case *ast.ColumnNameExpr:
		return c.checkColumnExpr(x)
	case *ast.IsNullExpr:
		if c.checkColumnExpr(x.Expr) {
			return true
		}
	case *ast.IsTruthExpr:
		if c.checkColumnExpr(x.Expr) {
			return true
		}
	case *ast.ParenthesesExpr:
		return c.check(x.Expr)
	case *ast.PatternInExpr:
		if x.Sel != nil || x.Not {
			return false
		}
		if !c.checkColumnExpr(x.Expr) {
			return false
		}
		for _, val := range x.List {
			if !ast.IsPreEvaluable(val) {
				return false
			}
		}
		return true
	case *ast.PatternLikeExpr:
		if x.Not {
			return false
		}
		if !c.checkColumnExpr(x.Expr) {
			return false
		}
		if !ast.IsPreEvaluable(x.Pattern) {
			return false
		}
		patternVal := x.Pattern.GetValue()
		if patternVal == nil {
			return false
		}
		patternStr, err := types.ToString(patternVal)
		if err != nil {
			return false
		}
		if len(patternStr) == 0 {
			return true
		}
		firstChar := patternStr[0]
		return firstChar != '%' && firstChar != '.'
	}
	return false
}

func (c *conditionChecker) checkBinaryOperation(b *ast.BinaryOperationExpr) bool {
	switch b.Op {
	case opcode.OrOr:
		return c.check(b.L) && c.check(b.R)
	case opcode.AndAnd:
		return c.check(b.L) && c.check(b.R)
	case opcode.EQ, opcode.NE, opcode.GE, opcode.GT, opcode.LE, opcode.LT:
		if ast.IsPreEvaluable(b.L) {
			return c.checkColumnExpr(b.R)
		} else if ast.IsPreEvaluable(b.R) {
			return c.checkColumnExpr(b.L)
		}
	}
	return false
}

func (c *conditionChecker) checkColumnExpr(expr ast.ExprNode) bool {
	cn, ok := expr.(*ast.ColumnNameExpr)
	if !ok {
		return false
	}
	if cn.Refer.Table.Name.L != c.tableName.L {
		return false
	}
	if c.pkName.L != "" {
		return c.pkName.L == cn.Refer.Column.Name.L
	}
	if c.idx != nil {
		return cn.Refer.Column.Name.L == c.idx.Columns[c.columnOffset].Name.L
	}
	return true
}

func (c *conditionChecker) newCheck(condition expression.Expression) bool {
	switch x := condition.(type) {
	case *expression.ScalarFunction:
		return c.checkScalarFunction(x)
	case *expression.Column:
		return c.checkColumn(x)
	case *expression.Constant:
		return true
	}
	return false
}

func (c *conditionChecker) checkScalarFunction(scalar *expression.ScalarFunction) bool {
	switch scalar.FuncName.L {
	case ast.OrOr, ast.AndAnd:
		return c.newCheck(scalar.Args[0]) && c.newCheck(scalar.Args[1])
	case ast.EQ, ast.NE, ast.GE, ast.GT, ast.LE, ast.LT:
		if _, ok := scalar.Args[0].(*expression.Constant); ok {
			return c.checkColumn(scalar.Args[1])
		}
		if _, ok := scalar.Args[1].(*expression.Constant); ok {
			return c.checkColumn(scalar.Args[0])
		}
	case ast.IsNull, ast.IsTruth, ast.IsFalsity:
		return c.checkColumn(scalar.Args[0])
	case ast.UnaryNot:
		// Don't support "not like" and "not in" convert to access conditions.
		if s, ok := scalar.Args[0].(*expression.ScalarFunction); ok {
			if s.FuncName.L == ast.In || s.FuncName.L == ast.Like {
				return false
			}
		}
		return c.newCheck(scalar.Args[0])
	case ast.In:
		if !c.checkColumn(scalar.Args[0]) {
			return false
		}
		for _, v := range scalar.Args[1:] {
			if _, ok := v.(*expression.Constant); !ok {
				return false
			}
		}
		return true
	case ast.Like:
		return c.checkLikeFunc(scalar)
	}
	return false
}

func (c *conditionChecker) checkLikeFunc(scalar *expression.ScalarFunction) bool {
	if !c.checkColumn(scalar.Args[0]) {
		return false
	}
	pattern, ok := scalar.Args[1].(*expression.Constant)
	if !ok {
		return false
	}
	if pattern.Value.IsNull() {
		return false
	}
	patternStr, err := pattern.Value.ToString()
	if err != nil {
		return false
	}
	if len(patternStr) == 0 {
		return true
	}
	escape := byte(scalar.Args[2].(*expression.Constant).Value.GetInt64())
	for i := 0; i < len(patternStr); i++ {
		if patternStr[i] == escape {
			i++
			if i < len(patternStr)-1 {
				continue
			}
			break
		}
		if i == 0 && (patternStr[i] == '%' || patternStr[i] == '_') {
			return false
		}
		if patternStr[i] == '%' {
			if i != len(patternStr)-1 {
				c.isReserved = true
			}
			break
		}
		if patternStr[i] == '_' {
			c.isReserved = true
			break
		}
	}
	return true
}

func (c *conditionChecker) checkColumn(expr expression.Expression) bool {
	col, ok := expr.(*expression.Column)
	if !ok {
		return false
	}
	if col.Correlated {
		return false
	}
	if c.pkName.L != "" {
		return c.pkName.L == col.ColName.L
	}
	if c.idx != nil {
		return col.ColName.L == c.idx.Columns[c.columnOffset].Name.L
	}
	return true
}

var oppositeOp = map[string]string{
	ast.LT: ast.GE,
	ast.GE: ast.LT,
	ast.GT: ast.LE,
	ast.LE: ast.GT,
	ast.EQ: ast.NE,
	ast.NE: ast.EQ,
}

func pushDownNot(expr expression.Expression, not bool) expression.Expression {
	if f, ok := expr.(*expression.ScalarFunction); ok {
		switch f.FuncName.L {
		case ast.UnaryNot:
			return pushDownNot(f.Args[0], !not)
		case ast.LT, ast.GE, ast.GT, ast.LE, ast.EQ, ast.NE:
			if not {
				nf, _ := expression.NewFunction(oppositeOp[f.FuncName.L], f.GetType(), f.Args...)
				return nf
			}
			for i, arg := range f.Args {
				f.Args[i] = pushDownNot(arg, false)
			}
			return f
		case ast.AndAnd:
			if not {
				args := f.Args
				for i, a := range args {
					args[i] = pushDownNot(a, true)
				}
				nf, _ := expression.NewFunction(ast.OrOr, f.GetType(), args...)
				return nf
			}
			for i, arg := range f.Args {
				f.Args[i] = pushDownNot(arg, false)
			}
			return f
		case ast.OrOr:
			if not {
				args := f.Args
				for i, a := range args {
					args[i] = pushDownNot(a, true)
				}
				nf, _ := expression.NewFunction(ast.AndAnd, f.GetType(), args...)
				return nf
			}
			for i, arg := range f.Args {
				f.Args[i] = pushDownNot(arg, false)
			}
			return f
		}
	}
	if not {
		expr, _ = expression.NewFunction(ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), expr)
	}
	return expr
}
