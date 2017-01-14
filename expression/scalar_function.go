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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	args     []Expression
	FuncName model.CIStr
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType   *types.FieldType
	Function  BuiltinFunc
	ArgValues []types.Datum
}

// GetArgs gets arguments of function.
func (sf *ScalarFunction) GetArgs() []Expression {
	return sf.args
}

// String implements fmt.Stringer interface.
func (sf *ScalarFunction) String() string {
	result := sf.FuncName.L + "("
	for i, arg := range sf.GetArgs() {
		result += arg.String()
		if i+1 != len(sf.args) {
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
func NewFunction(funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	f, ok := Funcs[funcName]
	if !ok {
		return nil, errFunctionNotExists.GenByArgs(funcName)
	}
	if len(args) < f.MinArgs || (f.MaxArgs != -1 && len(args) > f.MaxArgs) {
		return nil, errIncorrectParameterCount.GenByArgs(funcName)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	return &ScalarFunction{
		args:      funcArgs,
		FuncName:  model.NewCIStr(funcName),
		RetType:   retType,
		Function:  f.F,
		ArgValues: make([]types.Datum, len(funcArgs))}, nil
}

//ScalarFuncs2Exprs converts []*ScalarFunction to []Expression.
func ScalarFuncs2Exprs(funcs []*ScalarFunction) []Expression {
	result := make([]Expression, 0, len(funcs))
	for _, col := range funcs {
		result = append(result, col)
	}
	return result
}

// Clone implements Expression interface.
func (sf *ScalarFunction) Clone() Expression {
	newFunc := &ScalarFunction{
		FuncName:  sf.FuncName,
		Function:  sf.Function,
		RetType:   sf.RetType,
		ArgValues: make([]types.Datum, len(sf.args))}
	newFunc.args = make([]Expression, 0, len(sf.args))
	for _, arg := range sf.args {
		newFunc.args = append(newFunc.args, arg.Clone())
	}
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
	if len(sf.args) != len(fun.args) {
		return false
	}
	for i, argX := range sf.args {
		if !argX.Equal(fun.args[i], ctx) {
			return false
		}
	}
	return true
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
func (sf *ScalarFunction) Decorrelate(schema Schema) Expression {
	for i, arg := range sf.GetArgs() {
		sf.args[i] = arg.Decorrelate(schema)
	}
	return sf
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(row []types.Datum, ctx context.Context) (types.Datum, error) {
	var err error
	for i, arg := range sf.GetArgs() {
		sf.ArgValues[i], err = arg.Eval(row, ctx)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
	}
	return sf.Function(sf.ArgValues, ctx)
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode() []byte {
	var bytes []byte
	v := make([]types.Datum, 0, len(sf.args)+1)
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
func (sf *ScalarFunction) ResolveIndices(schema Schema) {
	for _, arg := range sf.args {
		arg.ResolveIndices(schema)
	}
}
