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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ base.HashEquals = &ScalarFunction{}

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	FuncName ast.CIStr
	// RetType is the type that ScalarFunction returns.
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType           *types.FieldType `plan-cache-clone:"shallow"`
	Function          builtinFunc
	hashcode          []byte
	canonicalhashcode []byte
}

// SafeToShareAcrossSession returns if the function can be shared across different sessions.
func (sf *ScalarFunction) SafeToShareAcrossSession() bool {
	return sf.Function.SafeToShareAcrossSession()
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalInt(ctx, input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalReal(ctx, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalString(ctx, input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalDecimal(ctx, input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalTime(ctx, input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalDuration(ctx, input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.vecEvalJSON(ctx, input, result)
}

// VecEvalVectorFloat32 evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalVectorFloat32(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return sf.Function.vecEvalVectorFloat32(ctx, input, result)
}

// GetArgs gets arguments of function.
func (sf *ScalarFunction) GetArgs() []Expression {
	return sf.Function.getArgs()
}

// Vectorized returns if this expression supports vectorized evaluation.
func (sf *ScalarFunction) Vectorized() bool {
	return sf.Function.vectorized() && sf.Function.isChildrenVectorized()
}

// StringWithCtx implements Expression interface.
func (sf *ScalarFunction) StringWithCtx(ctx ParamValues, redact string) string {
	buffer := bytes.NewBuffer(make([]byte, 0, len(sf.FuncName.L)+8+16*len(sf.GetArgs())))
	buffer.WriteString(sf.FuncName.L)
	buffer.WriteByte('(')
	switch sf.FuncName.L {
	case ast.Cast:
		for _, arg := range sf.GetArgs() {
			buffer.WriteString(arg.StringWithCtx(ctx, redact))
			buffer.WriteString(", ")
			buffer.WriteString(sf.RetType.String())
		}
	default:
		for i, arg := range sf.GetArgs() {
			buffer.WriteString(arg.StringWithCtx(ctx, redact))
			if i+1 != len(sf.GetArgs()) {
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// String returns the string representation of the function
func (sf *ScalarFunction) String() string {
	return sf.StringWithCtx(exprctx.EmptyParamValues, errors.RedactLogDisable)
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
	for i, arg := range args {
		argflags := arg.GetType(ctx)
		if isNull(arg) && !(argflags.Equals(retFieldTp) && mysql.HasNotNullFlag(retFieldTp.GetFlag())) {
			newarg := arg.Clone()
			*newarg.GetType(ctx) = *retFieldTp.Clone()
			newarg.GetType(ctx).DelFlag(mysql.NotNullFlag) // Remove NotNullFlag of NullConst
			args[i] = newarg
		}
	}
}

// newFunctionImpl creates a new scalar function or constant.
// fold: 1 means folding constants, while 0 means not,
// -1 means try to fold constants if without errors/warnings, otherwise not.
func newFunctionImpl(ctx BuildContext, fold int, funcName string, retType *types.FieldType, checkOrInit ScalarFunctionCallBack, args ...Expression) (ret Expression, err error) {
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
	funcArgs := slices.Clone(args)
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
		FuncName: ast.NewCIStr(funcName),
		RetType:  retType,
		Function: f,
	}
	if checkOrInit != nil {
		sf2, err := checkOrInit(sf)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sf = sf2
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
type ScalarFunctionCallBack func(function *ScalarFunction) (*ScalarFunction, error)

func defaultScalarFunctionCheck(function *ScalarFunction) (*ScalarFunction, error) {
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
	}
	// An implicit assumption: ScalarFunc.RetType == ScalarFunc.builtinFunc.RetType
	if sf.canonicalhashcode != nil {
		c.canonicalhashcode = slices.Clone(sf.canonicalhashcode)
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
	// If they are the same object, they must be equal.
	if sf == fun {
		return true
	}
	if sf.FuncName.L != fun.FuncName.L {
		return false
	}
	if !sf.RetType.Equal(fun.RetType) {
		return false
	}
	if sf.hashcode != nil && fun.hashcode != nil {
		if intest.InTest {
			assertCheckHashCode(sf)
			assertCheckHashCode(fun)
		}
		return bytes.Equal(sf.hashcode, fun.hashcode)
	}
	return sf.Function.equal(ctx, fun.Function)
}

func assertCheckHashCode(sf *ScalarFunction) {
	intest.Assert(intest.InTest)
	copyhashcode := make([]byte, len(sf.hashcode))
	copy(copyhashcode, sf.hashcode)
	// avoid data race in the plan cache
	s := sf.Clone().(*ScalarFunction)
	ReHashCode(s)
	intest.Assert(bytes.Equal(s.hashcode, copyhashcode), "HashCode should not change after ReHashCode is called")
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
