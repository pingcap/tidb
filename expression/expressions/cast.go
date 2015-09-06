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

package expressions

import (
	"fmt"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/util/types"
)

// FunctionCast is the cast function converting value to another type, e.g, cast(expr AS signed).
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_cast
type FunctionCast struct {
	// Expr is the expression to be converted.
	Expr expression.Expression
	// Tp is the conversion type.
	Tp *types.FieldType
}

// Clone implements the Expression Clone interface.
func (f *FunctionCast) Clone() (expression.Expression, error) {
	expr, err := f.Expr.Clone()
	if err != nil {
		return nil, err
	}
	nf := &FunctionCast{
		Expr: expr,
		Tp:   f.Tp,
	}
	return nf, nil
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionCast) IsStatic() bool {
	return f.Expr.IsStatic()
}

// String implements the Expression String interface.
func (f *FunctionCast) String() string {
	tpStr := ""
	if f.Tp.Tp == mysql.TypeLonglong {
		if mysql.HasUnsignedFlag(f.Tp.Flag) {
			tpStr = "UNSIGNED"
		} else {
			tpStr = "SIGNED"
		}
	} else {
		tpStr = f.Tp.String()
	}
	return fmt.Sprintf("CAST(%s AS %s)", f.Expr.String(), tpStr)
}

// Eval implements the Expression Eval interface.
func (f *FunctionCast) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	value, err := f.Expr.Eval(ctx, args)
	if err != nil {
		return nil, err
	}

	// Casting nil to any type returns null
	if value == nil {
		return nil, nil
	}
	nv, err := types.Convert(value, f.Tp)
	if err != nil {
		return nil, err
	}
	return nv, nil
}
