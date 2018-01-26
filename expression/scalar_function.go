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
	"bytes"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	FuncName model.CIStr
	// RetType is the type that ScalarFunction returns.
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType  *types.FieldType
	Function builtinFunc
}

// GetArgs gets arguments of function.
func (sf *ScalarFunction) GetArgs() []Expression {
	return sf.Function.getArgs()
}

// GetCtx gets the context of function.
func (sf *ScalarFunction) GetCtx() context.Context {
	return sf.Function.getCtx()
}

// String implements fmt.Stringer interface.
func (sf *ScalarFunction) String() string {
	result := sf.FuncName.L + "("
	for i, arg := range sf.GetArgs() {
		result += arg.String()
		if i+1 != len(sf.GetArgs()) {
			result += ", "
		}
	}
	result += ")"
	return result
}

// MarshalJSON implements json.Marshaler interface.
func (sf *ScalarFunction) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf("\"%s\"", sf))
	return buffer.Bytes(), nil
}

// NewFunction creates a new scalar function or constant.
func NewFunction(ctx context.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	if retType == nil {
		return nil, errors.Errorf("RetType cannot be nil for ScalarFunction.")
	}
	if funcName == ast.Cast {
		return BuildCastFunction(ctx, args[0], retType), nil
	}
	fc, ok := funcs[funcName]
	if !ok {
		return nil, errFunctionNotExists.GenByArgs("FUNCTION", funcName)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if builtinRetTp := f.getRetTp(); builtinRetTp.Tp != mysql.TypeUnspecified || retType.Tp == mysql.TypeUnspecified {
		retType = builtinRetTp
	}
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  retType,
		Function: f,
	}
	return FoldConstant(sf), nil
}

// NewFunctionInternal is similar to NewFunction, but do not returns error, should only be used internally.
func NewFunctionInternal(ctx context.Context, funcName string, retType *types.FieldType, args ...Expression) Expression {
	expr, err := NewFunction(ctx, funcName, retType, args...)
	terror.Log(errors.Trace(err))
	return expr
}

// ScalarFuncs2Exprs converts []*ScalarFunction to []Expression.
func ScalarFuncs2Exprs(funcs []*ScalarFunction) []Expression {
	result := make([]Expression, 0, len(funcs))
	for _, col := range funcs {
		result = append(result, col)
	}
	return result
}

// Clone implements Expression interface.
func (sf *ScalarFunction) Clone() Expression {
	newArgs := make([]Expression, 0, len(sf.GetArgs()))
	for _, arg := range sf.GetArgs() {
		newArgs = append(newArgs, arg.Clone())
	}
	switch sf.FuncName.L {
	case ast.Cast:
		return BuildCastFunction(sf.GetCtx(), sf.GetArgs()[0], sf.GetType())
	case ast.Values:
		var offset int
		switch sf.GetType().EvalType() {
		case types.ETInt:
			offset = sf.Function.(*builtinValuesIntSig).offset
		case types.ETReal:
			offset = sf.Function.(*builtinValuesRealSig).offset
		case types.ETDecimal:
			offset = sf.Function.(*builtinValuesDecimalSig).offset
		case types.ETString:
			offset = sf.Function.(*builtinValuesStringSig).offset
		case types.ETDatetime, types.ETTimestamp:
			offset = sf.Function.(*builtinValuesTimeSig).offset
		case types.ETDuration:
			offset = sf.Function.(*builtinValuesDurationSig).offset
		case types.ETJson:
			offset = sf.Function.(*builtinValuesJSONSig).offset
		}
		return NewValuesFunc(offset, sf.GetType(), sf.GetCtx())
	}
	newFunc := NewFunctionInternal(sf.GetCtx(), sf.FuncName.L, sf.RetType, newArgs...)
	return newFunc
}

// GetType implements Expression interface.
func (sf *ScalarFunction) GetType() *types.FieldType {
	return sf.RetType
}

// Equal implements Expression interface.
func (sf *ScalarFunction) Equal(e Expression, ctx context.Context) bool {
	fun, ok := e.(*ScalarFunction)
	if !ok {
		return false
	}
	if sf.FuncName.L != fun.FuncName.L {
		return false
	}
	return sf.Function.equal(fun.Function)
}

// IsCorrelated implements Expression interface.
func (sf *ScalarFunction) IsCorrelated() bool {
	for _, arg := range sf.GetArgs() {
		if arg.IsCorrelated() {
			return true
		}
	}
	return false
}

// Decorrelate implements Expression interface.
func (sf *ScalarFunction) Decorrelate(schema *Schema) Expression {
	for i, arg := range sf.GetArgs() {
		sf.GetArgs()[i] = arg.Decorrelate(schema)
	}
	return sf
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(row types.Row) (d types.Datum, err error) {
	var (
		res    interface{}
		isNull bool
	)
	switch tp, evalType := sf.GetType(), sf.GetType().EvalType(); evalType {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = sf.EvalInt(sf.GetCtx(), row)
		if mysql.HasUnsignedFlag(tp.Flag) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = sf.EvalReal(sf.GetCtx(), row)
	case types.ETDecimal:
		res, isNull, err = sf.EvalDecimal(sf.GetCtx(), row)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = sf.EvalTime(sf.GetCtx(), row)
	case types.ETDuration:
		res, isNull, err = sf.EvalDuration(sf.GetCtx(), row)
	case types.ETJson:
		res, isNull, err = sf.EvalJSON(sf.GetCtx(), row)
	case types.ETString:
		res, isNull, err = sf.EvalString(sf.GetCtx(), row)
	}

	if isNull || err != nil {
		d.SetValue(nil)
		return d, errors.Trace(err)
	}
	d.SetValue(res)
	return
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(ctx context.Context, row types.Row) (int64, bool, error) {
	return sf.Function.evalInt(row)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(ctx context.Context, row types.Row) (float64, bool, error) {
	return sf.Function.evalReal(row)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(ctx context.Context, row types.Row) (*types.MyDecimal, bool, error) {
	return sf.Function.evalDecimal(row)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(ctx context.Context, row types.Row) (string, bool, error) {
	return sf.Function.evalString(row)
}

// EvalTime implements Expression interface.
func (sf *ScalarFunction) EvalTime(ctx context.Context, row types.Row) (types.Time, bool, error) {
	return sf.Function.evalTime(row)
}

// EvalDuration implements Expression interface.
func (sf *ScalarFunction) EvalDuration(ctx context.Context, row types.Row) (types.Duration, bool, error) {
	return sf.Function.evalDuration(row)
}

// EvalJSON implements Expression interface.
func (sf *ScalarFunction) EvalJSON(ctx context.Context, row types.Row) (json.BinaryJSON, bool, error) {
	return sf.Function.evalJSON(row)
}

// ResolveIndices implements Expression interface.
func (sf *ScalarFunction) ResolveIndices(schema *Schema) {
	for _, arg := range sf.GetArgs() {
		arg.ResolveIndices(schema)
	}
}
