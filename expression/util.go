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

package expression

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// ExtractColumns extracts all columns from an expression.
func ExtractColumns(expr Expression) (cols []*Column) {
	switch v := expr.(type) {
	case *Column:
		return []*Column{v}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			cols = append(cols, ExtractColumns(arg)...)
		}
	}
	return
}

// ColumnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func ColumnSubstitute(expr Expression, schema Schema, newExprs []Expression) Expression {
	switch v := expr.(type) {
	case *Column:
		id := schema.GetColumnIndex(v)
		if id == -1 {
			return v
		}
		return newExprs[id].Clone()
	case *ScalarFunction:
		if v.FuncName.L == ast.Cast {
			newFunc := v.Clone().(*ScalarFunction)
			newFunc.GetArgs()[0] = ColumnSubstitute(newFunc.GetArgs()[0], schema, newExprs)
			return newFunc
		}
		newArgs := make([]Expression, 0, len(v.GetArgs()))
		for _, arg := range v.GetArgs() {
			newArgs = append(newArgs, ColumnSubstitute(arg, schema, newExprs))
		}
		fun, _ := NewFunction(v.FuncName.L, v.RetType, newArgs...)
		return fun
	}
	return expr
}

// calculateSum adds v to sum.
func calculateSum(sc *variable.StatementContext, sum, v types.Datum) (data types.Datum, err error) {
	// for avg and sum calculation
	// avg and sum use decimal for integer and decimal type, use float for others
	// see https://dev.mysql.com/doc/refman/5.7/en/group-by-functions.html

	switch v.Kind() {
	case types.KindNull:
	case types.KindInt64, types.KindUint64:
		var d *types.MyDecimal
		d, err = v.ToDecimal(sc)
		if err == nil {
			data = types.NewDecimalDatum(d)
		}
	case types.KindMysqlDecimal:
		data = v
	default:
		var f float64
		f, err = v.ToFloat64(sc)
		if err == nil {
			data = types.NewFloat64Datum(f)
		}
	}

	if err != nil {
		return data, errors.Trace(err)
	}
	if data.IsNull() {
		return sum, nil
	}
	switch sum.Kind() {
	case types.KindNull:
		return data, nil
	case types.KindFloat64, types.KindMysqlDecimal:
		return types.ComputePlus(sum, data)
	default:
		return data, errors.Errorf("invalid value %v for aggregate", sum.Kind())
	}
}
