// Copyright 2017 PingCAP, Inc.
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

package ranger

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// fullRange is (-∞, +∞).
var fullRange = []point{
	{start: true},
	{value: types.MaxValueDatum()},
}

// FullIntRange is (-∞, +∞) for IntColumnRange.
func FullIntRange() []types.IntColumnRange {
	return []types.IntColumnRange{{LowVal: math.MinInt64, HighVal: math.MaxInt64}}
}

// FullIndexRange is (-∞, +∞) for IndexRange.
func FullIndexRange() []*types.IndexRange {
	return []*types.IndexRange{{LowVal: []types.Datum{{}}, HighVal: []types.Datum{types.MaxValueDatum()}}}
}

// BuildIndexRange will build range of index for PhysicalIndexScan
func BuildIndexRange(sc *variable.StatementContext, tblInfo *model.TableInfo, index *model.IndexInfo,
	accessInAndEqCount int, accessCondition []expression.Expression) ([]*types.IndexRange, error) {
	rb := builder{sc: sc}
	var ranges []*types.IndexRange
	for i := 0; i < accessInAndEqCount; i++ {
		// Build ranges for equal or in access conditions.
		point := rb.build(accessCondition[i])
		colOff := index.Columns[i].Offset
		tp := &tblInfo.Columns[colOff].FieldType
		if i == 0 {
			ranges = rb.buildIndexRanges(point, tp)
		} else {
			ranges = rb.appendIndexRanges(ranges, point, tp)
		}
	}
	rangePoints := fullRange
	// Build rangePoints for non-equal access conditions.
	for i := accessInAndEqCount; i < len(accessCondition); i++ {
		rangePoints = rb.intersection(rangePoints, rb.build(accessCondition[i]))
	}
	if accessInAndEqCount == 0 {
		colOff := index.Columns[0].Offset
		tp := &tblInfo.Columns[colOff].FieldType
		ranges = rb.buildIndexRanges(rangePoints, tp)
	} else if accessInAndEqCount < len(accessCondition) {
		colOff := index.Columns[accessInAndEqCount].Offset
		tp := &tblInfo.Columns[colOff].FieldType
		ranges = rb.appendIndexRanges(ranges, rangePoints, tp)
	}

	// Take prefix index into consideration.
	if index.HasPrefixIndex() {
		for i := 0; i < len(ranges); i++ {
			refineRange(ranges[i], index)
		}
	}

	if len(ranges) > 0 && len(ranges[0].LowVal) < len(index.Columns) {
		for _, ran := range ranges {
			if ran.HighExclude || ran.LowExclude {
				if ran.HighExclude {
					ran.HighVal = append(ran.HighVal, types.NewDatum(nil))
				} else {
					ran.HighVal = append(ran.HighVal, types.MaxValueDatum())
				}
				if ran.LowExclude {
					ran.LowVal = append(ran.LowVal, types.MaxValueDatum())
				} else {
					ran.LowVal = append(ran.LowVal, types.NewDatum(nil))
				}
			}
		}
	}
	return ranges, errors.Trace(rb.err)
}

// refineRange changes the IndexRange taking prefix index length into consideration.
func refineRange(v *types.IndexRange, idxInfo *model.IndexInfo) {
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

// getEQFunctionOffset judge if the expression is a eq function like A = 1 where a is an index.
// If so, it will return the offset of A in index columns. e.g. for index(C,B,A), A's offset is 2.
func getEQFunctionOffset(expr expression.Expression, cols []*model.IndexColumn) int {
	f, ok := expr.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		return -1
	}
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		if _, ok := f.GetArgs()[1].(*expression.Constant); ok {
			for i, col := range cols {
				if col.Name.L == c.ColName.L {
					return i
				}
			}
		}
	} else if _, ok := f.GetArgs()[0].(*expression.Constant); ok {
		if c, ok := f.GetArgs()[1].(*expression.Column); ok {
			for i, col := range cols {
				if col.Name.L == c.ColName.L {
					return i
				}
			}
		}
	}
	return -1
}

func removeAccessConditions(conditions, accessConds []expression.Expression) []expression.Expression {
	for i := len(conditions) - 1; i >= 0; i-- {
		for _, cond := range accessConds {
			if cond == conditions[i] {
				conditions = append(conditions[:i], conditions[i+1:]...)
				break
			}
		}
	}
	return conditions
}

// checkIndexCondition will check whether all columns of condition is index columns or primary key column.
func checkIndexCondition(condition expression.Expression, indexColumns []*model.IndexColumn, pKName model.CIStr) bool {
	cols := expression.ExtractColumns(condition)
	for _, col := range cols {
		if pKName.L == col.ColName.L {
			continue
		}
		isIndexColumn := false
		for _, indCol := range indexColumns {
			if col.ColName.L == indCol.Name.L && indCol.Length == types.UnspecifiedLength {
				isIndexColumn = true
				break
			}
		}
		if !isIndexColumn {
			return false
		}
	}
	return true
}

// DetachIndexFilterConditions will detach the access conditions from other conditions.
func DetachIndexFilterConditions(conditions []expression.Expression, indexColumns []*model.IndexColumn, table *model.TableInfo) ([]expression.Expression, []expression.Expression) {
	var pKName model.CIStr
	if table.PKIsHandle {
		for _, colInfo := range table.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				pKName = colInfo.Name
				break
			}
		}
	}
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		if checkIndexCondition(cond, indexColumns, pKName) {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// DetachIndexScanConditions will detach the index filters from table filters.
func DetachIndexScanConditions(conditions []expression.Expression, index *model.IndexInfo) (accessConds []expression.Expression,
	filterConds []expression.Expression, accessEqualCount int, accessInAndEqCount int) {
	accessConds = make([]expression.Expression, len(index.Columns))
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, cond := range conditions {
		conditions[i] = expression.PushDownNot(cond, false, nil)
	}
	for _, cond := range conditions {
		offset := getEQFunctionOffset(cond, index.Columns)
		if offset != -1 {
			accessConds[offset] = cond
		}
	}
	for i, cond := range accessConds {
		if cond == nil {
			accessConds = accessConds[:i]
			accessEqualCount = i
			break
		}
		if index.Columns[i].Length != types.UnspecifiedLength {
			filterConds = append(filterConds, cond)
		}
		if i == len(accessConds)-1 {
			accessEqualCount = len(accessConds)
		}
	}
	accessInAndEqCount = accessEqualCount
	// We should remove all accessConds, so that they will not be added to filter conditions.
	conditions = removeAccessConditions(conditions, accessConds)
	var curIndex int
	for curIndex = accessEqualCount; curIndex < len(index.Columns); curIndex++ {
		checker := &conditionChecker{
			idx:          index,
			columnOffset: curIndex,
			length:       index.Columns[curIndex].Length,
		}
		// First of all, we should extract all of in/eq expressions from rest conditions for every continuous index column.
		// e.g. For index (a,b,c) and conditions a in (1,2) and b < 1 and c in (3,4), we should only extract column a in (1,2).
		accessIdx := checker.findEqOrInFunc(conditions)
		// If we fail to find any in or eq expression, we should consider all of other conditions for the next column.
		if accessIdx == -1 {
			accessConds, filterConds = checker.extractAccessAndFilterConds(conditions, accessConds, filterConds)
			break
		}
		accessInAndEqCount++
		accessConds = append(accessConds, conditions[accessIdx])
		if index.Columns[curIndex].Length != types.UnspecifiedLength {
			filterConds = append(filterConds, conditions[accessIdx])
		}
		conditions = append(conditions[:accessIdx], conditions[accessIdx+1:]...)
	}
	// If curIndex equals to len of index columns, it means the rest conditions haven't been appended to filter conditions.
	if curIndex == len(index.Columns) {
		filterConds = append(filterConds, conditions...)
	}
	return accessConds, filterConds, accessEqualCount, accessInAndEqCount
}

// DetachColumnConditions distinguishes between access conditions and filter conditions from conditions.
func DetachColumnConditions(conditions []expression.Expression, colName model.CIStr) ([]expression.Expression, []expression.Expression) {
	if colName.L == "" {
		return nil, conditions
	}

	var accessConditions, filterConditions []expression.Expression
	checker := conditionChecker{
		colName: colName,
		length:  types.UnspecifiedLength,
	}
	for _, cond := range conditions {
		cond = expression.PushDownNot(cond, false, nil)
		if !checker.check(cond) {
			filterConditions = append(filterConditions, cond)
			continue
		}
		accessConditions = append(accessConditions, cond)
		// TODO: it will lead to repeated computation cost.
		if checker.shouldReserve {
			filterConditions = append(filterConditions, cond)
			checker.shouldReserve = false
		}
	}

	return accessConditions, filterConditions
}

// BuildTableRange will build range of pk for PhysicalTableScan
func BuildTableRange(accessConditions []expression.Expression, sc *variable.StatementContext) ([]types.IntColumnRange, error) {
	if len(accessConditions) == 0 {
		return FullIntRange(), nil
	}

	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range accessConditions {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	ranges := rb.buildTableRanges(rangePoints)
	if rb.err != nil {
		return nil, errors.Trace(rb.err)
	}
	return ranges, nil
}

// conditionChecker checks if this condition can be pushed to index plan.
type conditionChecker struct {
	idx           *model.IndexInfo
	cols          []*expression.Column
	columnOffset  int // the offset of the indexed column to be checked.
	colName       model.CIStr
	shouldReserve bool // check if a access condition should be reserved in filter conditions.
	length        int
}

func (c *conditionChecker) check(condition expression.Expression) bool {
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

func (c *conditionChecker) extractAccessAndFilterConds(conditions, accessConds, filterConds []expression.Expression) ([]expression.Expression, []expression.Expression) {
	for _, cond := range conditions {
		if !c.check(cond) {
			filterConds = append(filterConds, cond)
			continue
		}
		accessConds = append(accessConds, cond)
		// TODO: It will lead to repeated computation cost.
		if c.length != types.UnspecifiedLength || c.shouldReserve {
			filterConds = append(filterConds, cond)
			c.shouldReserve = false
		}
	}
	return accessConds, filterConds
}

func (c *conditionChecker) findEqOrInFunc(conditions []expression.Expression) int {
	for i, cond := range conditions {
		var offset int
		if c.idx != nil {
			offset = getEQFunctionOffset(cond, c.idx.Columns)
		} else {
			offset = getEQColOffset(cond, c.cols)
		}
		if c.columnOffset == offset {
			return i
		}
	}
	for i, cond := range conditions {
		if in, ok := cond.(*expression.ScalarFunction); ok &&
			in.FuncName.L == ast.In && c.checkScalarFunction(in) {
			return i
		}
	}
	return -1
}

func (c *conditionChecker) checkScalarFunction(scalar *expression.ScalarFunction) bool {
	switch scalar.FuncName.L {
	case ast.LogicOr, ast.LogicAnd:
		return c.check(scalar.GetArgs()[0]) && c.check(scalar.GetArgs()[1])
	case ast.EQ, ast.NE, ast.GE, ast.GT, ast.LE, ast.LT:
		if _, ok := scalar.GetArgs()[0].(*expression.Constant); ok {
			if c.checkColumn(scalar.GetArgs()[1]) {
				return scalar.FuncName.L != ast.NE || c.length == types.UnspecifiedLength
			}
		}
		if _, ok := scalar.GetArgs()[1].(*expression.Constant); ok {
			if c.checkColumn(scalar.GetArgs()[0]) {
				return scalar.FuncName.L != ast.NE || c.length == types.UnspecifiedLength
			}
		}
	case ast.IsNull, ast.IsTruth, ast.IsFalsity:
		return c.checkColumn(scalar.GetArgs()[0])
	case ast.UnaryNot:
		// TODO: support "not like" and "not in" convert to access conditions.
		if s, ok := scalar.GetArgs()[0].(*expression.ScalarFunction); ok {
			if s.FuncName.L == ast.In || s.FuncName.L == ast.Like {
				return false
			}
		} else {
			// "not column" or "not constant" can't lead to a range.
			return false
		}
		return c.check(scalar.GetArgs()[0])
	case ast.In:
		if !c.checkColumn(scalar.GetArgs()[0]) {
			return false
		}
		for _, v := range scalar.GetArgs()[1:] {
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
	if !c.checkColumn(scalar.GetArgs()[0]) {
		return false
	}
	pattern, ok := scalar.GetArgs()[1].(*expression.Constant)
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
	escape := byte(scalar.GetArgs()[2].(*expression.Constant).Value.GetInt64())
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
				c.shouldReserve = true
			}
			break
		}
		if patternStr[i] == '_' {
			c.shouldReserve = true
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
	if c.colName.L != "" {
		return c.colName.L == col.ColName.L
	}
	if c.idx != nil {
		return col.ColName.L == c.idx.Columns[c.columnOffset].Name.L
	}
	if len(c.cols) > 0 {
		return col.Equal(c.cols[c.columnOffset], nil)
	}
	return true
}
