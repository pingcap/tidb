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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/codec"
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
	sc := sf.GetCtx().GetSessionVars().StmtCtx
	var (
		res    interface{}
		isNull bool
	)
	switch tp, evalType := sf.GetType(), sf.GetType().EvalType(); evalType {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = sf.EvalInt(row, sc)
		if mysql.HasUnsignedFlag(tp.Flag) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = sf.EvalReal(row, sc)
	case types.ETDecimal:
		res, isNull, err = sf.EvalDecimal(row, sc)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = sf.EvalTime(row, sc)
	case types.ETDuration:
		res, isNull, err = sf.EvalDuration(row, sc)
	case types.ETJson:
		res, isNull, err = sf.EvalJSON(row, sc)
	case types.ETString:
		res, isNull, err = sf.EvalString(row, sc)
	}

	if isNull || err != nil {
		d.SetValue(nil)
		return d, errors.Trace(err)
	}
	d.SetValue(res)
	return
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(row types.Row, sc *stmtctx.StatementContext) (int64, bool, error) {
	return sf.Function.evalInt(row)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(row types.Row, sc *stmtctx.StatementContext) (float64, bool, error) {
	return sf.Function.evalReal(row)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(row types.Row, sc *stmtctx.StatementContext) (*types.MyDecimal, bool, error) {
	return sf.Function.evalDecimal(row)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(row types.Row, sc *stmtctx.StatementContext) (string, bool, error) {
	return sf.Function.evalString(row)
}

// EvalTime implements Expression interface.
func (sf *ScalarFunction) EvalTime(row types.Row, sc *stmtctx.StatementContext) (types.Time, bool, error) {
	return sf.Function.evalTime(row)
}

// EvalDuration implements Expression interface.
func (sf *ScalarFunction) EvalDuration(row types.Row, sc *stmtctx.StatementContext) (types.Duration, bool, error) {
	return sf.Function.evalDuration(row)
}

// EvalJSON implements Expression interface.
func (sf *ScalarFunction) EvalJSON(row types.Row, sc *stmtctx.StatementContext) (json.BinaryJSON, bool, error) {
	return sf.Function.evalJSON(row)
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode() []byte {
	v := make([]types.Datum, 0, len(sf.GetArgs())+1)
	bytes, err := codec.EncodeValue(nil, types.NewStringDatum(sf.FuncName.L))
	terror.Log(errors.Trace(err))
	v = append(v, types.NewBytesDatum(bytes))
	for _, arg := range sf.GetArgs() {
		v = append(v, types.NewBytesDatum(arg.HashCode()))
	}
	bytes = bytes[:0]
	bytes, err = codec.EncodeValue(bytes, v...)
	terror.Log(errors.Trace(err))
	return bytes
}

// ResolveIndices implements Expression interface.
func (sf *ScalarFunction) ResolveIndices(schema *Schema) {
	for _, arg := range sf.GetArgs() {
		arg.ResolveIndices(schema)
	}
}
