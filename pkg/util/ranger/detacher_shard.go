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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ranger

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

// AddGcColumnCond add the `tidb_shard(x) = xxx` to the condition
// @param[in] cols          the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] accessCond    the conditions relative to the index and arranged by the index column order.
//
//	e.g. the index is uk(tidb_shard(a), a, b) and the where clause is
//	`WHERE b = 1 AND a = 2 AND c = 3`, the param accessCond is {a = 2, b = 1} that is
//	only relative to uk's columns.
//
// @param[in] columnValues  the values of index columns in param accessCond. if accessCond is {a = 2, b = 1},
//
//	columnValues is {2, 1}. if accessCond the "IN" function like `a IN (1, 2)`, columnValues
//	is empty.
//
// @retval -  []expression.Expression   the new conditions after adding `tidb_shard() = xxx` prefix
//
//	error                     if error gernerated, return error
func AddGcColumnCond(sctx *rangerctx.RangerContext,
	cols []*expression.Column,
	accessesCond []expression.Expression,
	columnValues []*valueInfo) ([]expression.Expression, error) {
	if cond := accessesCond[1]; cond != nil {
		if f, ok := cond.(*expression.ScalarFunction); ok {
			switch f.FuncName.L {
			case ast.EQ:
				return AddGcColumn4EqCond(sctx, cols, accessesCond, columnValues)
			case ast.In:
				return AddGcColumn4InCond(sctx, cols, accessesCond)
			}
		}
	}

	return accessesCond, nil
}

// AddGcColumn4InCond add the `tidb_shard(x) = xxx` for `IN` condition
// For param explanation, please refer to the function `AddGcColumnCond`.
// @retval -  []expression.Expression   the new conditions after adding `tidb_shard() = xxx` prefix
//
//	error                     if error gernerated, return error
func AddGcColumn4InCond(sctx *rangerctx.RangerContext,
	cols []*expression.Column,
	accessesCond []expression.Expression) ([]expression.Expression, error) {
	var errRes error
	var newAccessCond []expression.Expression
	record := make([]types.Datum, 1)

	expr := cols[0].VirtualExpr.Clone()
	andType := types.NewFieldType(mysql.TypeTiny)

	sf := accessesCond[1].(*expression.ScalarFunction)
	c := sf.GetArgs()[0].(*expression.Column)
	var andOrExpr expression.Expression
	evalCtx := sctx.ExprCtx.GetEvalCtx()
	for i, arg := range sf.GetArgs()[1:] {
		// get every const value and calculate tidb_shard(val)
		con := arg.(*expression.Constant)
		conVal, err := con.Eval(evalCtx, chunk.Row{})
		if err != nil {
			return accessesCond, err
		}

		record[0] = conVal
		mutRow := chunk.MutRowFromDatums(record)
		exprVal, err := expr.Eval(evalCtx, mutRow.ToRow())
		if err != nil {
			return accessesCond, err
		}

		// tmpArg1 is like `tidb_shard(a) = 8`, tmpArg2 is like `a = 100`
		exprCon := &expression.Constant{Value: exprVal, RetType: cols[0].RetType}
		tmpArg1, err := expression.NewFunction(sctx.ExprCtx, ast.EQ, cols[0].RetType, cols[0], exprCon)
		if err != nil {
			return accessesCond, err
		}
		tmpArg2, err := expression.NewFunction(sctx.ExprCtx, ast.EQ, c.RetType, c.Clone(), arg)
		if err != nil {
			return accessesCond, err
		}

		// make a LogicAnd, e.g. `tidb_shard(a) = 8 AND a = 100`
		andExpr, err := expression.NewFunction(sctx.ExprCtx, ast.LogicAnd, andType, tmpArg1, tmpArg2)
		if err != nil {
			return accessesCond, err
		}

		if i == 0 {
			andOrExpr = andExpr
		} else {
			// if the LogicAnd more than one, make a LogicOr,
			// e.g. `(tidb_shard(a) = 8 AND a = 100) OR (tidb_shard(a) = 161 AND a = 200)`
			andOrExpr, errRes = expression.NewFunction(sctx.ExprCtx, ast.LogicOr, andType, andOrExpr, andExpr)
			if errRes != nil {
				return accessesCond, errRes
			}
		}
	}

	newAccessCond = append(newAccessCond, andOrExpr)

	return newAccessCond, nil
}

// AddGcColumn4EqCond add the `tidb_shard(x) = xxx` prefix for equal condition
// For param explanation, please refer to the function `AddGcColumnCond`.
// @retval -  []expression.Expression   the new conditions after adding `tidb_shard() = xxx` prefix
//
//	[]*valueInfo              the values of every columns in the returned new conditions
//	error                     if error gernerated, return error
func AddGcColumn4EqCond(sctx *rangerctx.RangerContext,
	cols []*expression.Column,
	accessesCond []expression.Expression,
	columnValues []*valueInfo) ([]expression.Expression, error) {
	expr := cols[0].VirtualExpr.Clone()
	record := make([]types.Datum, len(columnValues)-1)

	for i := 1; i < len(columnValues); i++ {
		cv := columnValues[i]
		if cv == nil {
			break
		}
		record[i-1] = *cv.value
	}

	mutRow := chunk.MutRowFromDatums(record)
	exprCtx := sctx.ExprCtx
	evaluated, err := expr.Eval(exprCtx.GetEvalCtx(), mutRow.ToRow())
	if err != nil {
		return accessesCond, err
	}
	vi := &valueInfo{&evaluated, false}
	con := &expression.Constant{Value: evaluated, RetType: cols[0].RetType}
	// make a tidb_shard() function, e.g. `tidb_shard(a) = 8`
	cond, err := expression.NewFunction(exprCtx, ast.EQ, cols[0].RetType, cols[0], con)
	if err != nil {
		return accessesCond, err
	}

	accessesCond[0] = cond
	columnValues[0] = vi
	return accessesCond, nil
}

// AddExpr4EqAndInCondition add the `tidb_shard(x) = xxx` prefix
// Add tidb_shard() for EQ and IN function. e.g. input condition is `WHERE a = 1`,
// output condition is `WHERE tidb_shard(a) = 214 AND a = 1`. e.g. input condition
// is `WHERE a IN (1, 2 ,3)`, output condition is `WHERE (tidb_shard(a) = 214 AND a = 1)
// OR (tidb_shard(a) = 143 AND a = 2) OR (tidb_shard(a) = 156 AND a = 3)`
// @param[in] conditions  the original condition to be processed
// @param[in] cols        the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] lengths     the length for every column of shard index
// @retval - the new condition after adding tidb_shard() prefix
func AddExpr4EqAndInCondition(sctx *rangerctx.RangerContext, conditions []expression.Expression,
	cols []*expression.Column) ([]expression.Expression, error) {
	accesses := make([]expression.Expression, len(cols))
	columnValues := make([]*valueInfo, len(cols))
	offsets := make([]int, len(conditions))
	addGcCond := true

	// the array accesses stores conditions of every column in the index in the definition order
	// e.g. the original condition is `WHERE b = 100 AND a = 200 AND c = 300`, the definition of
	// index is (tidb_shard(a), a, b), then accesses is "[a = 200, b = 100]"
	for i, cond := range conditions {
		offset := getPotentialEqOrInColOffset(sctx, cond, cols)
		offsets[i] = offset
		if offset == -1 {
			continue
		}
		if accesses[offset] == nil {
			accesses[offset] = cond
			continue
		}
		// if the same field appear twice or more, don't add tidb_shard()
		// e.g. `WHERE a > 100 and a < 200`
		addGcCond = false
	}

	for i, cond := range accesses {
		if cond == nil {
			continue
		}
		if !allEqOrIn(cond) {
			addGcCond = false
			break
		}
		columnValues[i] = extractValueInfo(cond)
	}

	if !addGcCond || !NeedAddGcColumn4ShardIndex(cols, accesses, columnValues) {
		return conditions, nil
	}

	// remove the accesses from newConditions
	newConditions := make([]expression.Expression, 0, len(conditions))
	newConditions = append(newConditions, conditions...)
	newConditions = removeConditions(sctx.ExprCtx.GetEvalCtx(), newConditions, accesses)

	// add Gc condition for accesses and return new condition to newAccesses
	newAccesses, err := AddGcColumnCond(sctx, cols, accesses, columnValues)
	if err != nil {
		return conditions, err
	}

	// merge newAccesses and original condition execept accesses
	newConditions = append(newConditions, newAccesses...)

	return newConditions, nil
}

// NeedAddGcColumn4ShardIndex check whether to add `tidb_shard(x) = xxx`
// @param[in] cols          the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] accessCond    the conditions relative to the index and arranged by the index column order.
//
//	e.g. the index is uk(tidb_shard(a), a, b) and the where clause is
//	`WHERE b = 1 AND a = 2 AND c = 3`, the param accessCond is {a = 2, b = 1} that is
//	only relative to uk's columns.
//
// @param[in] columnValues  the values of index columns in param accessCond. if accessCond is {a = 2, b = 1},
//
//	columnValues is {2, 1}. if accessCond the "IN" function like `a IN (1, 2)`, columnValues
//	is empty.
//
// @retval -  return true if it needs to addr tidb_shard() prefix, ohterwise return false
func NeedAddGcColumn4ShardIndex(cols []*expression.Column, accessCond []expression.Expression, columnValues []*valueInfo) bool {
	// the columns of shard index shoude be more than 2, like (tidb_shard(a),a,...)
	// check cols and columnValues in the sub call function
	if len(accessCond) < 2 || len(cols) < 2 {
		return false
	}

	if !IsValidShardIndex(cols) {
		return false
	}

	// accessCond[0] shoudle be nil, because it has no access condition for
	// the prefix tidb_shard() of the shard index
	if cond := accessCond[1]; cond != nil {
		if f, ok := cond.(*expression.ScalarFunction); ok {
			switch f.FuncName.L {
			case ast.EQ:
				return NeedAddColumn4EqCond(cols, accessCond, columnValues)
			case ast.In:
				return NeedAddColumn4InCond(cols, accessCond, f)
			}
		}
	}

	return false
}

// NeedAddColumn4EqCond `tidb_shard(x) = xxx`
// For param explanation, please refer to the function `NeedAddGcColumn4ShardIndex`.
// It checks whether EQ conditions need to be added tidb_shard() prefix.
// (1) columns in accessCond are all columns of the index except the first.
// (2) every column in accessCond has a constan value
func NeedAddColumn4EqCond(cols []*expression.Column,
	accessCond []expression.Expression, columnValues []*valueInfo) bool {
	valCnt := 0
	matchedKeyFldCnt := 0

	// the columns of shard index shoude be more than 2, like (tidb_shard(a),a,...)
	if len(columnValues) < 2 {
		return false
	}

	for _, cond := range accessCond[1:] {
		if cond == nil {
			break
		}

		f, ok := cond.(*expression.ScalarFunction)
		if !ok || f.FuncName.L != ast.EQ {
			return false
		}

		matchedKeyFldCnt++
	}
	for _, val := range columnValues[1:] {
		if val == nil {
			break
		}
		valCnt++
	}

	if matchedKeyFldCnt != len(cols)-1 ||
		valCnt != len(cols)-1 ||
		accessCond[0] != nil ||
		columnValues[0] != nil {
		return false
	}

	return true
}

// NeedAddColumn4InCond `tidb_shard(x) = xxx`
// For param explanation, please refer to the function `NeedAddGcColumn4ShardIndex`.
// It checks whether "IN" conditions need to be added tidb_shard() prefix.
// (1) columns in accessCond are all columns of the index except the first.
// (2) the first param of "IN" function should be a column not a expression like `a + b`
// (3) the rest params of "IN" function all should be constant
// (4) the first param of "IN" function should be the column in the expression of first index field.
//
//	e.g. uk(tidb_shard(a), a). If the conditions is `WHERE b in (1, 2, 3)`, the first param of "IN" function
//	is `b` that's not the column in `tidb_shard(a)`.
//
// @param  sf	"IN" function, e.g. `a IN (1, 2, 3)`
func NeedAddColumn4InCond(cols []*expression.Column, accessCond []expression.Expression, sf *expression.ScalarFunction) bool {
	if len(cols) == 0 || len(accessCond) == 0 || sf == nil {
		return false
	}

	if accessCond[0] != nil {
		return false
	}

	fields := ExtractColumnsFromExpr(cols[0].VirtualExpr.(*expression.ScalarFunction))

	c, ok := sf.GetArgs()[0].(*expression.Column)
	if !ok {
		return false
	}

	for _, arg := range sf.GetArgs()[1:] {
		if _, ok := arg.(*expression.Constant); !ok {
			return false
		}
	}

	if len(fields) != 1 ||
		!fields[0].EqualColumn(c) {
		return false
	}

	return true
}

// ExtractColumnsFromExpr get all fields from input expression virtaulExpr
func ExtractColumnsFromExpr(virtaulExpr *expression.ScalarFunction) []*expression.Column {
	var fields []*expression.Column

	if virtaulExpr == nil {
		return fields
	}

	for _, arg := range virtaulExpr.GetArgs() {
		if sf, ok := arg.(*expression.ScalarFunction); ok {
			fields = append(fields, ExtractColumnsFromExpr(sf)...)
		} else if c, ok := arg.(*expression.Column); ok {
			if !c.InColumnArray(fields) {
				fields = append(fields, c)
			}
		}
	}

	return fields
}

// IsValidShardIndex Check whether the definition of shard index is valid. The form of index
// should like `index(tidb_shard(a), a, ....)`.
// 1) the column count shoudle be >= 2
// 2) the first column should be tidb_shard(xxx)
// 3) the parameter of tidb_shard shoudle be a column that is the second column of index
// @param[in] cols        the columns of shard index, such as [tidb_shard(a), a, ...]
// @retval - if the shard index is valid return true, otherwise return false
func IsValidShardIndex(cols []*expression.Column) bool {
	// definition of index should like the form: index(tidb_shard(a), a, ....)
	if len(cols) < 2 {
		return false
	}

	// the first coulmn of index must be GC column and the expr must be tidb_shard
	if !expression.GcColumnExprIsTidbShard(cols[0].VirtualExpr) {
		return false
	}

	shardFunc, _ := cols[0].VirtualExpr.(*expression.ScalarFunction)

	argCount := len(shardFunc.GetArgs())
	if argCount != 1 {
		return false
	}

	// parameter of tidb_shard must be the second column of the input index columns
	col, ok := shardFunc.GetArgs()[0].(*expression.Column)
	if !ok || !col.EqualColumn(cols[1]) {
		return false
	}

	return true
}
