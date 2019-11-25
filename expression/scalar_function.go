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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
)

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	FuncName model.CIStr
	// RetType is the type that ScalarFunction returns.
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType  *types.FieldType
	Function builtinFunc
	hashcode []byte
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalInt(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return sf.Function.vecEvalInt(input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalReal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return sf.Function.vecEvalReal(input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalString(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return sf.Function.vecEvalString(input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalDecimal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return sf.Function.vecEvalDecimal(input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalTime(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return sf.Function.vecEvalTime(input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalDuration(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return sf.Function.vecEvalDuration(input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalJSON(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return sf.Function.vecEvalJSON(input, result)
}

// ReverseEvalInt evaluates the only one column int value with given function result.
func (sf *ScalarFunction) ReverseEvalInt(res types.Datum, rType RoundingType) (val int64, err error) {
	return sf.Function.reverseEvalInt(res, rType)
}

// ReverseEvalReal evaluates the only one column real value with given function result.
func (sf *ScalarFunction) ReverseEvalReal(res types.Datum, rType RoundingType) (val float64, err error) {
	return sf.Function.reverseEvalReal(res, rType)
}

// ReverseEvalString evaluates the only one column string value with given function result.
func (sf *ScalarFunction) ReverseEvalString(res types.Datum, rType RoundingType) (val string, err error) {
	return sf.Function.reverseEvalString(res, rType)
}

// ReverseEvalDecimal evaluates the only one column decimal value with given function result.
func (sf *ScalarFunction) ReverseEvalDecimal(res types.Datum, rType RoundingType) (val *types.MyDecimal, err error) {
	return sf.Function.reverseEvalDecimal(res, rType)
}

// ReverseEvalTime evaluates the only one column time value with given function result.
func (sf *ScalarFunction) ReverseEvalTime(res types.Datum, rType RoundingType) (val types.Time, err error) {
	return sf.Function.reverseEvalTime(res, rType)
}

// ReverseEvalDuration evaluates the only one column duration value with given function result.
func (sf *ScalarFunction) ReverseEvalDuration(res types.Datum, rType RoundingType) (val types.Duration, err error) {
	return sf.Function.reverseEvalDuration(res, rType)
}

// GetArgs gets arguments of function.
func (sf *ScalarFunction) GetArgs() []Expression {
	return sf.Function.getArgs()
}

// Vectorized returns if this expression supports vectorized evaluation.
func (sf *ScalarFunction) Vectorized() bool {
	return sf.Function.vectorized() && sf.Function.isChildrenVectorized()
}

// SupportReverseEval returns if this expression supports reversed evaluation.
func (sf *ScalarFunction) SupportReverseEval() bool {
	return sf.Function.supportReverseEval() && sf.Function.isChildrenReversed()
}

// ReverseEval evaluates the only one column value with given function result.
func (sf *ScalarFunction) ReverseEval(res types.Datum, rType RoundingType, colType types.EvalType) (val types.Datum, err error) {
	switch colType {
	case types.ETInt:
		ret, err := sf.ReverseEvalInt(res, rType)
		return types.NewIntDatum(ret), err
	case types.ETReal:
		ret, err := sf.ReverseEvalReal(res, rType)
		return types.NewFloat64Datum(ret), err
	case types.ETString:
		ret, err := sf.ReverseEvalString(res, rType)
		return types.NewStringDatum(ret), err
	case types.ETDecimal:
		ret, err := sf.ReverseEvalDecimal(res, rType)
		return types.NewDecimalDatum(ret), err
	case types.ETTimestamp:
		ret, err := sf.ReverseEvalTime(res, rType)
		return types.NewTimeDatum(ret), err
	case types.ETDuration:
		ret, err := sf.ReverseEvalDuration(res, rType)
		return types.NewDurationDatum(ret), err
	}
	return types.Datum{}, errors.Errorf("unknown evaluation type for Column in ReverseEval")
}

// GetCtx gets the context of function.
func (sf *ScalarFunction) GetCtx() sessionctx.Context {
	return sf.Function.getCtx()
}

// String implements fmt.Stringer interface.
func (sf *ScalarFunction) String() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", sf.FuncName.L)
	for i, arg := range sf.GetArgs() {
		buffer.WriteString(arg.String())
		if i+1 != len(sf.GetArgs()) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// MarshalJSON implements json.Marshaler interface.
func (sf *ScalarFunction) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", sf)), nil
}

// newFunctionImpl creates a new scalar function or constant.
func newFunctionImpl(ctx sessionctx.Context, fold bool, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	if retType == nil {
		return nil, errors.Errorf("RetType cannot be nil for ScalarFunction.")
	}
	if funcName == ast.Cast {
		return BuildCastFunction(ctx, args[0], retType), nil
	}
	fc, ok := funcs[funcName]
	if !ok {
		db := ctx.GetSessionVars().CurrentDB
		if db == "" {
			return nil, terror.ClassOptimizer.New(mysql.ErrNoDB, mysql.MySQLErrName[mysql.ErrNoDB])
		}

		return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", db+"."+funcName)
	}
	if !ctx.GetSessionVars().EnableNoopFuncs {
		if _, ok := noopFuncs[funcName]; ok {
			return nil, ErrFunctionsNoopImpl.GenWithStackByArgs(funcName)
		}
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, err
	}
	if builtinRetTp := f.getRetTp(); builtinRetTp.Tp != mysql.TypeUnspecified || retType.Tp == mysql.TypeUnspecified {
		retType = builtinRetTp
	}
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  retType,
		Function: f,
	}
	if fold {
		return FoldConstant(sf), nil
	}
	return sf, nil
}

// NewFunction creates a new scalar function or constant via a constant folding.
func NewFunction(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, true, funcName, retType, args...)
}

// NewFunctionBase creates a new scalar function with no constant folding.
func NewFunctionBase(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, false, funcName, retType, args...)
}

// NewFunctionInternal is similar to NewFunction, but do not returns error, should only be used internally.
func NewFunctionInternal(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) Expression {
	expr, err := NewFunction(ctx, funcName, retType, args...)
	terror.Log(err)
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
	return &ScalarFunction{
		FuncName: sf.FuncName,
		RetType:  sf.RetType,
		Function: sf.Function.Clone(),
		hashcode: sf.hashcode,
	}
}

// GetType implements Expression interface.
func (sf *ScalarFunction) GetType() *types.FieldType {
	return sf.RetType
}

// Equal implements Expression interface.
func (sf *ScalarFunction) Equal(ctx sessionctx.Context, e Expression) bool {
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

// ConstItem implements Expression interface.
func (sf *ScalarFunction) ConstItem() bool {
	// Note: some unfoldable functions are deterministic, we use unFoldableFunctions here for simplification.
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		return false
	}
	for _, arg := range sf.GetArgs() {
		if !arg.ConstItem() {
			return false
		}
	}
	return true
}

// Decorrelate implements Expression interface.
func (sf *ScalarFunction) Decorrelate(schema *Schema) Expression {
	for i, arg := range sf.GetArgs() {
		sf.GetArgs()[i] = arg.Decorrelate(schema)
	}
	return sf
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(row chunk.Row) (d types.Datum, err error) {
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
		return d, err
	}
	d.SetValue(res)
	return
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	return sf.Function.evalInt(row)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	return sf.Function.evalReal(row)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (*types.MyDecimal, bool, error) {
	return sf.Function.evalDecimal(row)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	return sf.Function.evalString(row)
}

// EvalTime implements Expression interface.
func (sf *ScalarFunction) EvalTime(ctx sessionctx.Context, row chunk.Row) (types.Time, bool, error) {
	return sf.Function.evalTime(row)
}

// EvalDuration implements Expression interface.
func (sf *ScalarFunction) EvalDuration(ctx sessionctx.Context, row chunk.Row) (types.Duration, bool, error) {
	return sf.Function.evalDuration(row)
}

// EvalJSON implements Expression interface.
func (sf *ScalarFunction) EvalJSON(ctx sessionctx.Context, row chunk.Row) (json.BinaryJSON, bool, error) {
	return sf.Function.evalJSON(row)
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode(sc *stmtctx.StatementContext) []byte {
	if len(sf.hashcode) > 0 {
		return sf.hashcode
	}
	sf.hashcode = append(sf.hashcode, scalarFunctionFlag)
	sf.hashcode = codec.EncodeCompactBytes(sf.hashcode, hack.Slice(sf.FuncName.L))
	for _, arg := range sf.GetArgs() {
		sf.hashcode = append(sf.hashcode, arg.HashCode(sc)...)
	}
	return sf.hashcode
}

// ResolveIndices implements Expression interface.
func (sf *ScalarFunction) ResolveIndices(schema *Schema) (Expression, error) {
	newSf := sf.Clone()
	err := newSf.resolveIndices(schema)
	return newSf, err
}

func (sf *ScalarFunction) resolveIndices(schema *Schema) error {
	for _, arg := range sf.GetArgs() {
		err := arg.resolveIndices(schema)
		if err != nil {
			return err
		}
	}
	return nil
}
