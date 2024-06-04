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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"fmt"
	"slices"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	FuncName model.CIStr
	// RetType is the type that ScalarFunction returns.
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType           *types.FieldType
	Function          builtinFunc
	hashcode          []byte
	canonicalhashcode []byte
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalInt(ctx, input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalReal(ctx, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalString(ctx, input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalDecimal(ctx, input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalTime(ctx, input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalDuration(ctx, input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalJSON(ctx, input, result)
}

// GetArgs gets arguments of function.
func (sf *ScalarFunction) GetArgs() []Expression {
	return sf.Function.getArgs()
}

// Vectorized returns if this expression supports vectorized evaluation.
func (sf *ScalarFunction) Vectorized() bool {
	return sf.Function.vectorized() && sf.Function.isChildrenVectorized()
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
	return []byte(fmt.Sprintf("%q", sf)), nil
}

// typeInferForNull infers the NULL constants field type and set the field type
// of NULL constant same as other non-null operands.
func typeInferForNull(ctx EvalContext, args []Expression) {
	if len(args) < 2 {
		return
	}
	var isNull = func(expr Expression) bool {
		cons, ok := expr.(*Constant)
		return ok && cons.RetType.GetType() == mysql.TypeNull && cons.Value.IsNull()
	}
	// Infer the actual field type of the NULL constant.
	var retFieldTp *types.FieldType
	var hasNullArg bool
	for i := len(args) - 1; i >= 0; i-- {
		isNullArg := isNull(args[i])
		if !isNullArg && retFieldTp == nil {
			retFieldTp = args[i].GetType(ctx)
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
			*arg.GetType(ctx) = *retFieldTp
			arg.GetType(ctx).DelFlag(mysql.NotNullFlag) // Remove NotNullFlag of NullConst
		}
	}
}

// newFunctionImpl creates a new scalar function or constant.
// fold: 1 means folding constants, while 0 means not,
// -1 means try to fold constants if without errors/warnings, otherwise not.
func newFunctionImpl(ctx BuildContext, fold int, funcName string, retType *types.FieldType, checkOrInit ScalarFunctionCallBack, args ...Expression) (ret Expression, err error) {
	defer func() {
		if err == nil && ret != nil && checkOrInit != nil {
			if sf, ok := ret.(*ScalarFunction); ok {
				ret, err = checkOrInit(sf)
			}
		}
	}()
	if retType == nil {
		return nil, errors.Errorf("RetType cannot be nil for ScalarFunction")
	}
	switch funcName {
	case ast.Cast:
		return BuildCastFunction(ctx, args[0], retType), nil
	case ast.GetVar:
		return BuildGetVarFunction(ctx, args[0], retType)
	case InternalFuncFromBinary:
		return BuildFromBinaryFunction(ctx, args[0], retType, false), nil
	case InternalFuncToBinary:
		return BuildToBinaryFunction(ctx, args[0]), nil
	case ast.Sysdate:
		if ctx.GetSysdateIsNow() {
			funcName = ast.Now
		}
	}
	fc, ok := funcs[funcName]
	if !ok {
		if extFunc, exist := extensionFuncs.Load(funcName); exist {
			fc = extFunc.(functionClass)
			ok = true
		}
	}

	if !ok {
		db := ctx.GetEvalCtx().CurrentDB()
		if db == "" {
			return nil, errors.Trace(plannererrors.ErrNoDB)
		}
		return nil, ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", db+"."+funcName)
	}
	noopFuncsMode := ctx.GetNoopFuncsMode()
	if noopFuncsMode != variable.OnInt {
		if _, ok := noopFuncs[funcName]; ok {
			err := ErrFunctionsNoopImpl.FastGenByArgs(funcName)
			if noopFuncsMode == variable.OffInt {
				return nil, errors.Trace(err)
			}
			// NoopFuncsMode is Warn, append an error
			ctx.GetEvalCtx().AppendWarning(err)
		}
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	switch funcName {
	case ast.If, ast.Ifnull, ast.Nullif:
		// Do nothing. Because it will call InferType4ControlFuncs.
	case ast.RowFunc:
		// Do nothing. Because it shouldn't use ROW's args to infer null type.
		// For example, expression ('abc', 1) = (null, 0). Null's type should be STRING, not INT.
		// The type infer happens when converting the expression to ('abc' = null) and (1 = 0).
	default:
		typeInferForNull(ctx.GetEvalCtx(), funcArgs)
	}

	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, err
	}
	if builtinRetTp := f.getRetTp(); builtinRetTp.GetType() != mysql.TypeUnspecified || retType.GetType() == mysql.TypeUnspecified {
		retType = builtinRetTp
	}
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  retType,
		Function: f,
	}
	if fold == 1 {
		return FoldConstant(ctx, sf), nil
	} else if fold == -1 {
		// try to fold constants, and return the original function if errors/warnings occur
		evalCtx := ctx.GetEvalCtx()
		beforeWarns := evalCtx.WarningCount()
		newSf := FoldConstant(ctx, sf)
		afterWarns := evalCtx.WarningCount()
		if afterWarns > beforeWarns {
			evalCtx.TruncateWarnings(beforeWarns)
			return sf, nil
		}
		return newSf, nil
	}
	return sf, nil
}

// ScalarFunctionCallBack is the definition of callback of calling a newFunction.
type ScalarFunctionCallBack func(function *ScalarFunction) (Expression, error)

func defaultScalarFunctionCheck(function *ScalarFunction) (Expression, error) {
	// todo: more scalar function init actions can be added here, or setting up with customized init callback.
	if function.FuncName.L == ast.Grouping {
		if !function.Function.(*BuiltinGroupingImplSig).isMetaInited {
			return function, errors.Errorf("grouping meta data hasn't been initialized, try use function clone instead")
		}
	}
	return function, nil
}

// NewFunctionWithInit creates a new scalar function with callback init function.
func NewFunctionWithInit(ctx BuildContext, funcName string, retType *types.FieldType, init ScalarFunctionCallBack, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, 1, funcName, retType, init, args...)
}

// NewFunction creates a new scalar function or constant via a constant folding.
func NewFunction(ctx BuildContext, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, 1, funcName, retType, defaultScalarFunctionCheck, args...)
}

// NewFunctionBase creates a new scalar function with no constant folding.
func NewFunctionBase(ctx BuildContext, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, 0, funcName, retType, defaultScalarFunctionCheck, args...)
}

// NewFunctionTryFold creates a new scalar function with trying constant folding.
func NewFunctionTryFold(ctx BuildContext, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, -1, funcName, retType, defaultScalarFunctionCheck, args...)
}

// NewFunctionInternal is similar to NewFunction, but do not return error, should only be used internally.
// Deprecated: use NewFunction instead, old logic here is for the convenience of go linter error check.
// while for the new function creation, some errors can also be thrown out, for example, args verification
// error, collation derivation error, special function with meta doesn't be initialized error and so on.
// only threw the these internal error out, then we can debug and dig it out quickly rather than in a confusion
// of index out of range / nil pointer error / function execution error.
func NewFunctionInternal(ctx BuildContext, funcName string, retType *types.FieldType, args ...Expression) Expression {
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
	c := &ScalarFunction{
		FuncName: sf.FuncName,
		RetType:  sf.RetType,
		Function: sf.Function.Clone(),
		hashcode: sf.hashcode,
	}
	c.SetCharsetAndCollation(sf.CharsetAndCollation())
	c.SetCoercibility(sf.Coercibility())
	c.SetRepertoire(sf.Repertoire())
	return c
}

// GetType implements Expression interface.
func (sf *ScalarFunction) GetType(_ EvalContext) *types.FieldType {
	return sf.GetStaticType()
}

// GetStaticType returns the static type of the scalar function.
func (sf *ScalarFunction) GetStaticType() *types.FieldType {
	return sf.RetType
}

// Equal implements Expression interface.
func (sf *ScalarFunction) Equal(ctx EvalContext, e Expression) bool {
	intest.Assert(ctx != nil)
	fun, ok := e.(*ScalarFunction)
	if !ok {
		return false
	}
	if sf.FuncName.L != fun.FuncName.L {
		return false
	}
	return sf.Function.equal(ctx, fun.Function)
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

// ConstLevel returns the const level for the expression
func (sf *ScalarFunction) ConstLevel() ConstLevel {
	// Note: some unfoldable functions are deterministic, we use unFoldableFunctions here for simplification.
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		return ConstNone
	}

	if _, ok := sf.Function.(*extensionFuncSig); ok {
		// we should return `ConstNone` for extension functions for safety, because it may have a side effect.
		return ConstNone
	}

	level := ConstStrict
	for _, arg := range sf.GetArgs() {
		argLevel := arg.ConstLevel()
		if argLevel == ConstNone {
			return ConstNone
		}

		if argLevel < level {
			level = argLevel
		}
	}

	return level
}

// Decorrelate implements Expression interface.
func (sf *ScalarFunction) Decorrelate(schema *Schema) Expression {
	for i, arg := range sf.GetArgs() {
		sf.GetArgs()[i] = arg.Decorrelate(schema)
	}
	return sf
}

// Traverse implements the TraverseDown interface.
func (sf *ScalarFunction) Traverse(action TraverseAction) Expression {
	return action.Transform(sf)
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(ctx EvalContext, row chunk.Row) (d types.Datum, err error) {
	var (
		res    any
		isNull bool
	)
	intest.AssertNotNil(ctx)
	switch tp, evalType := sf.GetType(ctx), sf.GetType(ctx).EvalType(); evalType {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = sf.EvalInt(ctx, row)
		if mysql.HasUnsignedFlag(tp.GetFlag()) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = sf.EvalReal(ctx, row)
	case types.ETDecimal:
		res, isNull, err = sf.EvalDecimal(ctx, row)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = sf.EvalTime(ctx, row)
	case types.ETDuration:
		res, isNull, err = sf.EvalDuration(ctx, row)
	case types.ETJson:
		res, isNull, err = sf.EvalJSON(ctx, row)
	case types.ETString:
		var str string
		str, isNull, err = sf.EvalString(ctx, row)
		if !isNull && err == nil && tp.GetType() == mysql.TypeEnum {
			res, err = types.ParseEnum(tp.GetElems(), str, tp.GetCollate())
			tc := typeCtx(ctx)
			err = tc.HandleTruncate(err)
		} else {
			res = str
		}
	}

	if isNull || err != nil {
		d.SetNull()
		return d, err
	}
	d.SetValue(res, sf.RetType)
	return
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalInt(ctx, row)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalReal(ctx, row)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalDecimal(ctx, row)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalString(ctx, row)
}

// EvalTime implements Expression interface.
func (sf *ScalarFunction) EvalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalTime(ctx, row)
}

// EvalDuration implements Expression interface.
func (sf *ScalarFunction) EvalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalDuration(ctx, row)
}

// EvalJSON implements Expression interface.
func (sf *ScalarFunction) EvalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	intest.Assert(ctx != nil)
	if intest.InTest {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalJSON(ctx, row)
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode() []byte {
	if len(sf.hashcode) > 0 {
		return sf.hashcode
	}
	ReHashCode(sf)
	return sf.hashcode
}

// CanonicalHashCode implements Expression interface.
func (sf *ScalarFunction) CanonicalHashCode() []byte {
	if len(sf.canonicalhashcode) > 0 {
		return sf.canonicalhashcode
	}
	simpleCanonicalizedHashCode(sf)
	return sf.canonicalhashcode
}

// ExpressionsSemanticEqual is used to judge whether two expression tree is semantic equivalent.
func ExpressionsSemanticEqual(expr1, expr2 Expression) bool {
	return bytes.Equal(expr1.CanonicalHashCode(), expr2.CanonicalHashCode())
}

// simpleCanonicalizedHashCode is used to judge whether two expression is semantically equal.
func simpleCanonicalizedHashCode(sf *ScalarFunction) {
	if sf.canonicalhashcode != nil {
		sf.canonicalhashcode = sf.canonicalhashcode[:0]
	}
	sf.canonicalhashcode = append(sf.canonicalhashcode, scalarFunctionFlag)

	argsHashCode := make([][]byte, 0, len(sf.GetArgs()))
	for _, arg := range sf.GetArgs() {
		argsHashCode = append(argsHashCode, arg.CanonicalHashCode())
	}
	switch sf.FuncName.L {
	case ast.Plus, ast.Mul, ast.EQ, ast.In, ast.LogicOr, ast.LogicAnd:
		// encode original function name.
		sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(sf.FuncName.L))
		// reorder parameters hashcode, eg: a+b and b+a should has the same hashcode here.
		slices.SortFunc(argsHashCode, func(i, j []byte) int {
			return bytes.Compare(i, j)
		})
		for _, argCode := range argsHashCode {
			sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
		}

	case ast.GE, ast.LE: // directed binary OP: a >= b and b <= a should have the same hashcode.
		// encode GE function name.
		sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GE))
		// encode GE function name and switch the args order.
		if sf.FuncName.L == ast.GE {
			for _, argCode := range argsHashCode {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
			}
		} else {
			for i := len(argsHashCode) - 1; i >= 0; i-- {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argsHashCode[i]...)
			}
		}
	case ast.GT, ast.LT:
		sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GT))
		if sf.FuncName.L == ast.GT {
			for _, argCode := range argsHashCode {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
			}
		} else {
			for i := len(argsHashCode) - 1; i >= 0; i-- {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argsHashCode[i]...)
			}
		}
	case ast.UnaryNot:
		child, ok := sf.GetArgs()[0].(*ScalarFunction)
		if !ok {
			// encode original function name.
			sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(sf.FuncName.L))
			// use the origin arg hash code.
			for _, argCode := range argsHashCode {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
			}
		} else {
			childArgsHashCode := make([][]byte, 0, len(child.GetArgs()))
			for _, arg := range child.GetArgs() {
				childArgsHashCode = append(childArgsHashCode, arg.CanonicalHashCode())
			}
			switch child.FuncName.L {
			case ast.GT: // not GT  ==> LE  ==> use GE and switch args
				sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GE))
				for i := len(childArgsHashCode) - 1; i >= 0; i-- {
					sf.canonicalhashcode = append(sf.canonicalhashcode, childArgsHashCode[i]...)
				}
			case ast.LT: // not LT  ==> GE
				sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GE))
				for _, argCode := range childArgsHashCode {
					sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
				}
			case ast.GE: // not GE  ==> LT  ==> use GT and switch args
				sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GT))
				for i := len(childArgsHashCode) - 1; i >= 0; i-- {
					sf.canonicalhashcode = append(sf.canonicalhashcode, childArgsHashCode[i]...)
				}
			case ast.LE: // not LE  ==> GT
				sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GT))
				for _, argCode := range childArgsHashCode {
					sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
				}
			}
		}
	default:
		// encode original function name.
		sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(sf.FuncName.L))
		for _, argCode := range argsHashCode {
			sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
		}
		// Cast is a special case. The RetType should also be considered as an argument.
		// Please see `newFunctionImpl()` for detail.
		if sf.FuncName.L == ast.Cast {
			evalTp := sf.RetType.EvalType()
			sf.canonicalhashcode = append(sf.canonicalhashcode, byte(evalTp))
		}
	}
}

// ReHashCode is used after we change the argument in place.
func ReHashCode(sf *ScalarFunction) {
	sf.hashcode = sf.hashcode[:0]
	sf.hashcode = append(sf.hashcode, scalarFunctionFlag)
	sf.hashcode = codec.EncodeCompactBytes(sf.hashcode, hack.Slice(sf.FuncName.L))
	for _, arg := range sf.GetArgs() {
		sf.hashcode = append(sf.hashcode, arg.HashCode()...)
	}
	// Cast is a special case. The RetType should also be considered as an argument.
	// Please see `newFunctionImpl()` for detail.
	if sf.FuncName.L == ast.Cast {
		evalTp := sf.RetType.EvalType()
		sf.hashcode = append(sf.hashcode, byte(evalTp))
	}
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

// ResolveIndicesByVirtualExpr implements Expression interface.
func (sf *ScalarFunction) ResolveIndicesByVirtualExpr(ctx EvalContext, schema *Schema) (Expression, bool) {
	newSf := sf.Clone()
	isOK := newSf.resolveIndicesByVirtualExpr(ctx, schema)
	return newSf, isOK
}

func (sf *ScalarFunction) resolveIndicesByVirtualExpr(ctx EvalContext, schema *Schema) bool {
	for _, arg := range sf.GetArgs() {
		isOk := arg.resolveIndicesByVirtualExpr(ctx, schema)
		if !isOk {
			return false
		}
	}
	return true
}

// RemapColumn remaps columns with provided mapping and returns new expression
func (sf *ScalarFunction) RemapColumn(m map[int64]*Column) (Expression, error) {
	newSf, ok := sf.Clone().(*ScalarFunction)
	if !ok {
		return nil, errors.New("failed to cast to scalar function")
	}
	for i, arg := range sf.GetArgs() {
		newArg, err := arg.RemapColumn(m)
		if err != nil {
			return nil, err
		}
		newSf.GetArgs()[i] = newArg
	}
	// clear hash code
	newSf.hashcode = nil
	return newSf, nil
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
		sf.SetCoercibility(deriveCoercibilityForScalarFunc(sf))
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

// CharsetAndCollation gets charset and collation.
func (sf *ScalarFunction) CharsetAndCollation() (string, string) {
	return sf.Function.CharsetAndCollation()
}

// SetCharsetAndCollation sets charset and collation.
func (sf *ScalarFunction) SetCharsetAndCollation(chs, coll string) {
	sf.Function.SetCharsetAndCollation(chs, coll)
}

// Repertoire returns the repertoire value which is used to check collations.
func (sf *ScalarFunction) Repertoire() Repertoire {
	return sf.Function.Repertoire()
}

// SetRepertoire sets a specified repertoire for this expression.
func (sf *ScalarFunction) SetRepertoire(r Repertoire) {
	sf.Function.SetRepertoire(r)
}

const emptyScalarFunctionSize = int64(unsafe.Sizeof(ScalarFunction{}))

// MemoryUsage return the memory usage of ScalarFunction
func (sf *ScalarFunction) MemoryUsage() (sum int64) {
	if sf == nil {
		return
	}

	sum = emptyScalarFunctionSize + int64(len(sf.FuncName.L)+len(sf.FuncName.O)) + int64(cap(sf.hashcode))
	if sf.RetType != nil {
		sum += sf.RetType.MemoryUsage()
	}
	if sf.Function != nil {
		sum += sf.Function.MemoryUsage()
	}
	return sum
}
