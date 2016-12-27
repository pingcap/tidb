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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// ScalarFunction is the function that returns a value.
type ScalarFunction interface {
	Expression

	// GetName gets the name of the function.
	GetName() model.CIStr

	// GetArgs gets the arguments of the function.
	GetArgs() []Expression
}

type baseScalarFunction struct {
	Args     []Expression
	FuncName model.CIStr
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType   *types.FieldType
	Function  BuiltinFunc
	ArgValues []types.Datum
}

// GetName implements ScalarFunction interface.
func (sf *baseScalarFunction) GetName() model.CIStr {
	return sf.FuncName
}

// GetArgs implements ScalarFunction interface.
func (sf *baseScalarFunction) GetArgs() []Expression {
	return sf.Args
}

// String implements fmt.Stringer interface.
func (sf *baseScalarFunction) String() string {
	result := sf.FuncName.L + "("
	for i, arg := range sf.Args {
		result += arg.String()
		if i+1 != len(sf.Args) {
			result += ", "
		}
	}
	result += ")"
	return result
}

// MarshalJSON implements json.Marshaler interface.
func (sf *baseScalarFunction) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf("\"%s\"", sf))
	return buffer.Bytes(), nil
}

// NewFunction creates a new scalar function or constant.
func NewFunction(funcName string, retType *types.FieldType, args ...Expression) (ScalarFunction, error) {
	f, ok := Funcs[funcName]
	if !ok {
		return nil, errors.Errorf("Function %s is not implemented.", funcName)
	}
	if len(args) < f.MinArgs || (f.MaxArgs != -1 && len(args) > f.MaxArgs) {
		return nil, errInvalidOperation.Gen("number of function arguments must in [%d, %d].",
			f.MinArgs, f.MaxArgs)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	return &baseScalarFunction{
		Args:      funcArgs,
		FuncName:  model.NewCIStr(funcName),
		RetType:   retType,
		Function:  f.F,
		ArgValues: make([]types.Datum, len(funcArgs))}, nil
}

//ScalarFuncs2Exprs converts []ScalarFunction to []Expression.
func ScalarFuncs2Exprs(funcs []ScalarFunction) []Expression {
	result := make([]Expression, 0, len(funcs))
	for _, col := range funcs {
		result = append(result, col)
	}
	return result
}

// Clone implements Expression interface.
func (sf *baseScalarFunction) Clone() Expression {
	newFunc := &baseScalarFunction{
		FuncName:  sf.FuncName,
		Function:  sf.Function,
		RetType:   sf.RetType,
		ArgValues: make([]types.Datum, len(sf.Args))}
	newFunc.Args = make([]Expression, 0, len(sf.Args))
	for _, arg := range sf.Args {
		newFunc.Args = append(newFunc.Args, arg.Clone())
	}
	return newFunc
}

// GetType implements Expression interface.
func (sf *baseScalarFunction) GetType() *types.FieldType {
	return sf.RetType
}

// Equal implements Expression interface.
func (sf *baseScalarFunction) Equal(e Expression, ctx context.Context) bool {
	fun, ok := e.(*baseScalarFunction)
	if !ok {
		return false
	}
	if sf.FuncName.L != fun.FuncName.L {
		return false
	}
	if len(sf.Args) != len(fun.Args) {
		return false
	}
	for i, argX := range sf.Args {
		if !argX.Equal(fun.Args[i], ctx) {
			return false
		}
	}
	return true
}

// IsCorrelated implements Expression interface.
func (sf *baseScalarFunction) IsCorrelated() bool {
	for _, arg := range sf.Args {
		if arg.IsCorrelated() {
			return true
		}
	}
	return false
}

// Decorrelate implements Expression interface.
func (sf *baseScalarFunction) Decorrelate(schema Schema) Expression {
	for i, arg := range sf.Args {
		sf.Args[i] = arg.Decorrelate(schema)
	}
	return sf
}

// Eval implements Expression interface.
func (sf *baseScalarFunction) Eval(row []types.Datum, ctx context.Context) (types.Datum, error) {
	var err error
	for i, arg := range sf.Args {
		sf.ArgValues[i], err = arg.Eval(row, ctx)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
	}
	return sf.Function(sf.ArgValues, ctx)
}

// HashCode implements Expression interface.
func (sf *baseScalarFunction) HashCode() []byte {
	var bytes []byte
	v := make([]types.Datum, 0, len(sf.Args)+1)
	bytes, _ = codec.EncodeValue(bytes, types.NewStringDatum(sf.FuncName.L))
	v = append(v, types.NewBytesDatum(bytes))
	for _, arg := range sf.Args {
		v = append(v, types.NewBytesDatum(arg.HashCode()))
	}
	bytes = bytes[:0]
	bytes, _ = codec.EncodeValue(bytes, v...)
	return bytes
}

// ResolveIndices implements Expression interface.
func (sf *baseScalarFunction) ResolveIndices(schema Schema) {
	for _, arg := range sf.Args {
		arg.ResolveIndices(schema)
	}
}

// NewCastFunction creates a new function for cast.
func NewCastFunction(v *ast.FuncCastExpr, arg Expression) (ScalarFunction, error) {
	bt, err := castFuncFactory(v.Tp)
	return &baseScalarFunction{
		Args:      []Expression{arg},
		FuncName:  model.NewCIStr(ast.Cast),
		RetType:   v.Tp,
		Function:  bt,
		ArgValues: make([]types.Datum, 1)}, errors.Trace(err)
}

// NewValuesFunction creates a new function for values.
func NewValuesFunction(v *ast.ValuesExpr) ScalarFunction {
	bt := buildinValuesFactory(v)
	return &baseScalarFunction{
		FuncName: model.NewCIStr(ast.Values),
		RetType:  &v.Type,
		Function: bt,
	}
}
