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

// error definitions.
var (
	ErrNoDB = terror.ClassOptimizer.New(mysql.ErrNoDB, mysql.MySQLErrName[mysql.ErrNoDB])
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
	switch sf.RetType.Tp {
	case mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return sf.Function.supportReverseEval() && sf.Function.isChildrenReversed()
	}
	return false
}

// ReverseEval evaluates the only one column value with given function result.
func (sf *ScalarFunction) ReverseEval(sc *stmtctx.StatementContext, res types.Datum, rType types.RoundingType) (val types.Datum, err error) {
	return sf.Function.reverseEval(sc, res, rType)
}

// GetCtx gets the context of function.
func (sf *ScalarFunction) GetCtx() sessionctx.Context {
	return sf.Function.getCtx()
}

// String implements fmt.Stringer interface.
func (sf *ScalarFunction) String() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", sf.FuncName.L)
	switch sf.FuncName.L {
	case ast.Cast:
		for _, arg := range sf.GetArgs() {
			buffer.WriteString(arg.String())
			buffer.WriteString(", ")
			buffer.WriteString(sf.RetType.String())
		}
	default:
		for i, arg := range sf.GetArgs() {
			buffer.WriteString(arg.String())
			if i+1 != len(sf.GetArgs()) {
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// MarshalJSON implements json.Marshaler interface.
func (sf *ScalarFunction) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", sf)), nil
}

// typeInferForNull infers the NULL constants field type and set the field type
// of NULL constant same as other non-null operands.
func typeInferForNull(args []Expression) {
	if len(args) < 2 {
		return
	}
	var isNull = func(expr Expression) bool {
		cons, ok := expr.(*Constant)
		return ok && cons.RetType.Tp == mysql.TypeNull && cons.Value.IsNull()
	}
	// Infer the actual field type of the NULL constant.
	var retFieldTp *types.FieldType
	var hasNullArg bool
	for _, arg := range args {
		isNullArg := isNull(arg)
		if !isNullArg && retFieldTp == nil {
			retFieldTp = arg.GetType()
		}
		hasNullArg = hasNullArg || isNullArg
		// Break if there are both NULL and non-NULL expression
		if hasNullArg && retFieldTp != nil {
			break
		}
	}
	if !hasNullArg || retFieldTp == nil {
		return
	}
	for _, arg := range args {
		if isNull(arg) {
			*arg.GetType() = *retFieldTp
		}
	}
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
			return nil, errors.Trace(ErrNoDB)
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
	typeInferForNull(funcArgs)
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
	c := &ScalarFunction{
		FuncName: sf.FuncName,
		RetType:  sf.RetType,
		Function: sf.Function.Clone(),
		hashcode: sf.hashcode,
	}
	c.SetCharsetAndCollation(sf.CharsetAndCollation(sf.GetCtx()))
	c.SetCoercibility(sf.Coercibility())
	return c
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
func (sf *ScalarFunction) ConstItem(sc *stmtctx.StatementContext) bool {
	// Note: some unfoldable functions are deterministic, we use unFoldableFunctions here for simplification.
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		return false
	}
	for _, arg := range sf.GetArgs() {
		if !arg.ConstItem(sc) {
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
		d.SetNull()
		return d, err
	}
	d.SetValue(res, sf.RetType)
	return
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	if f, ok := sf.Function.(builtinFuncNew); ok {
		return f.evalIntWithCtx(ctx, row)
	}
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
	if sf.FuncName.L == ast.In {
		args := []Expression{}
		switch inFunc := sf.Function.(type) {
		case *builtinInIntSig:
			args = inFunc.nonConstArgs
		case *builtinInStringSig:
			args = inFunc.nonConstArgs
		case *builtinInTimeSig:
			args = inFunc.nonConstArgs
		case *builtinInDurationSig:
			args = inFunc.nonConstArgs
		case *builtinInRealSig:
			args = inFunc.nonConstArgs
		case *builtinInDecimalSig:
			args = inFunc.nonConstArgs
		}
		for _, arg := range args {
			err := arg.resolveIndices(schema)
			if err != nil {
				return err
			}
		}
	}
	for _, arg := range sf.GetArgs() {
		err := arg.resolveIndices(schema)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetSingleColumn returns (Col, Desc) when the ScalarFunction is equivalent to (Col, Desc)
// when used as a sort key, otherwise returns (nil, false).
//
// Can only handle:
// - ast.Plus
// - ast.Minus
// - ast.UnaryMinus
func (sf *ScalarFunction) GetSingleColumn(reverse bool) (*Column, bool) {
	switch sf.FuncName.String() {
	case ast.Plus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *Column:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp, reverse
		case *ScalarFunction:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp.GetSingleColumn(reverse)
		case *Constant:
			switch rtp := args[1].(type) {
			case *Column:
				return rtp, reverse
			case *ScalarFunction:
				return rtp.GetSingleColumn(reverse)
			}
		}
		return nil, false
	case ast.Minus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *Column:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp, reverse
		case *ScalarFunction:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp.GetSingleColumn(reverse)
		case *Constant:
			switch rtp := args[1].(type) {
			case *Column:
				return rtp, !reverse
			case *ScalarFunction:
				return rtp.GetSingleColumn(!reverse)
			}
		}
		return nil, false
	case ast.UnaryMinus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *Column:
			return tp, !reverse
		case *ScalarFunction:
			return tp.GetSingleColumn(!reverse)
		}
		return nil, false
	}
	return nil, false
}

// Coercibility returns the coercibility value which is used to check collations.
func (sf *ScalarFunction) Coercibility() Coercibility {
	if !sf.Function.HasCoercibility() {
		sf.SetCoercibility(deriveCoercibilityForScarlarFunc(sf))
	}
	return sf.Function.Coercibility()
}

// HasCoercibility ...
func (sf *ScalarFunction) HasCoercibility() bool {
	return sf.Function.HasCoercibility()
}

// SetCoercibility sets a specified coercibility for this expression.
func (sf *ScalarFunction) SetCoercibility(val Coercibility) {
	sf.Function.SetCoercibility(val)
}

// CharsetAndCollation ...
func (sf *ScalarFunction) CharsetAndCollation(ctx sessionctx.Context) (string, string) {
	return sf.Function.CharsetAndCollation(ctx)
}

// SetCharsetAndCollation ...
func (sf *ScalarFunction) SetCharsetAndCollation(chs, coll string) {
	sf.Function.SetCharsetAndCollation(chs, coll)
}
