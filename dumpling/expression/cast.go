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

package expression

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// castOperatopr is the operator type for cast function.
type castFunctionType int

const (
	// CastFunction is CAST function.
	CastFunction castFunctionType = iota + 1
	// ConvertFunction is CONVERT function.
	ConvertFunction
	// BinaryOperator is BINARY operator.
	BinaryOperator
)

// FunctionCast is the cast function converting value to another type, e.g, cast(expr AS signed).
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html
type FunctionCast struct {
	// Expr is the expression to be converted.
	Expr Expression
	// Tp is the conversion type.
	Tp *types.FieldType
	// Cast, Convert and Binary share this struct.
	FunctionType castFunctionType
}

// Clone implements the Expression Clone interface.
func (f *FunctionCast) Clone() Expression {
	expr := f.Expr.Clone()
	nf := &FunctionCast{
		Expr:         expr,
		Tp:           f.Tp,
		FunctionType: f.FunctionType,
	}
	return nf
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
	if f.FunctionType == ConvertFunction {
		return fmt.Sprintf("CONVERT(%s, %s)", f.Expr.String(), tpStr)
	} else if f.FunctionType == BinaryOperator {
		return fmt.Sprintf("BINARY %s", f.Expr.String())
	}
	return fmt.Sprintf("CAST(%s AS %s)", f.Expr.String(), tpStr)
}

// Eval implements the Expression Eval interface.
func (f *FunctionCast) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	value, err := f.Expr.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	value = types.RawData(value)
	d := &types.DataItem{Type: f.Tp}
	// Casting nil to any type returns null
	if value == nil {
		d.Data = nil
		return d, nil
	}

	d.Data, err = types.Cast(value, f.Tp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return d, nil
}

// Accept implements Expression Accept interface.
func (f *FunctionCast) Accept(v Visitor) (Expression, error) {
	return v.VisitFunctionCast(f)
}
