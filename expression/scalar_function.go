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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
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
	fc, ok := funcs[funcName]
	if !ok {
		return nil, errFunctionNotExists.GenByArgs(funcName)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	f, err := fc.getFunction(funcArgs, ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  retType,
		Function: f,
	}, nil
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
	switch v := sf.Function.(type) {
	case *builtinCastSig:
		return NewCastFunc(v.tp, newArgs[0], sf.GetCtx())
	case *builtinValuesSig:
		return NewValuesFunc(v.offset, sf.GetType(), sf.GetCtx())
	}
	newFunc, _ := NewFunction(sf.GetCtx(), sf.FuncName.L, sf.RetType, newArgs...)
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
func (sf *ScalarFunction) Eval(row []types.Datum) (types.Datum, error) {
	return sf.Function.eval(row)
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(row []types.Datum, sc *variable.StatementContext) (int64, bool, error) {
	return sf.Function.evalInt(row)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(row []types.Datum, sc *variable.StatementContext) (float64, bool, error) {
	return sf.Function.evalReal(row)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(row []types.Datum, sc *variable.StatementContext) (*types.MyDecimal, bool, error) {
	return sf.Function.evalDecimal(row)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(row []types.Datum, sc *variable.StatementContext) (string, bool, error) {
	return sf.Function.evalString(row)
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode() []byte {
	var bytes []byte
	v := make([]types.Datum, 0, len(sf.GetArgs())+1)
	bytes, _ = codec.EncodeValue(bytes, types.NewStringDatum(sf.FuncName.L))
	v = append(v, types.NewBytesDatum(bytes))
	for _, arg := range sf.GetArgs() {
		v = append(v, types.NewBytesDatum(arg.HashCode()))
	}
	bytes = bytes[:0]
	bytes, _ = codec.EncodeValue(bytes, v...)
	return bytes
}

// ResolveIndices implements Expression interface.
func (sf *ScalarFunction) ResolveIndices(schema *Schema) {
	for _, arg := range sf.GetArgs() {
		arg.ResolveIndices(schema)
	}
}
