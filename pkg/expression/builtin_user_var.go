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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

type setVarFunctionClass struct {
	baseFunctionClass
}

func (c *setVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[1].GetType(ctx.GetEvalCtx()).EvalType()
	if argTp == types.ETTimestamp || argTp == types.ETDuration || argTp == types.ETJson {
		argTp = types.ETString
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, types.ETString, argTp)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlenUnderLimit(args[1].GetType(ctx.GetEvalCtx()).GetFlen())
	switch argTp {
	case types.ETString:
		sig = &builtinSetStringVarSig{baseBuiltinFunc: bf}
	case types.ETReal:
		sig = &builtinSetRealVarSig{baseBuiltinFunc: bf}
	case types.ETDecimal:
		sig = &builtinSetDecimalVarSig{baseBuiltinFunc: bf}
	case types.ETInt:
		sig = &builtinSetIntVarSig{baseBuiltinFunc: bf}
	case types.ETDatetime:
		sig = &builtinSetTimeVarSig{baseBuiltinFunc: bf}
	default:
		return nil, errors.Errorf("unexpected types.EvalType %v", argTp)
	}
	return sig, nil
}

type builtinSetStringVarSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinSetStringVarSig) Clone() builtinFunc {
	newSig := &builtinSetStringVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetStringVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetStringVarSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	var varName string
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}
	varName, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	isNull = datum.IsNull()
	if isNull || err != nil {
		return "", isNull, err
	}
	res, err = datum.ToString()
	if err != nil {
		return "", isNull, err
	}
	sessionVars.SetStringUserVar(varName, stringutil.Copy(res), datum.Collation())
	return res, false, nil
}

type builtinSetRealVarSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinSetRealVarSig) Clone() builtinFunc {
	newSig := &builtinSetRealVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetRealVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetRealVarSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	var varName string
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	varName, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	isNull = datum.IsNull()
	if isNull || err != nil {
		return 0, isNull, err
	}
	res = datum.GetFloat64()
	varName = strings.ToLower(varName)
	sessionVars.SetUserVarVal(varName, datum)
	return res, false, nil
}

type builtinSetDecimalVarSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinSetDecimalVarSig) Clone() builtinFunc {
	newSig := &builtinSetDecimalVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetDecimalVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetDecimalVarSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return nil, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	isNull = datum.IsNull()
	if isNull || err != nil {
		return nil, isNull, err
	}
	res := datum.GetMysqlDecimal()
	varName = strings.ToLower(varName)
	sessionVars.SetUserVarVal(varName, datum)
	return res, false, nil
}

type builtinSetIntVarSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinSetIntVarSig) Clone() builtinFunc {
	newSig := &builtinSetIntVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetIntVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetIntVarSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	isNull = datum.IsNull()
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := datum.GetInt64()
	varName = strings.ToLower(varName)
	sessionVars.SetUserVarVal(varName, datum)
	return res, false, nil
}

type builtinSetTimeVarSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinSetTimeVarSig) Clone() builtinFunc {
	newSig := &builtinSetTimeVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetTimeVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetTimeVarSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	if err != nil || datum.IsNull() {
		return types.ZeroTime, datum.IsNull(), handleInvalidTimeError(ctx, err)
	}
	res := datum.GetMysqlTime()
	varName = strings.ToLower(varName)
	sessionVars.SetUserVarVal(varName, datum)
	return res, false, nil
}

// BuildGetVarFunction builds a GetVar ScalarFunction from the Expression.
func BuildGetVarFunction(ctx BuildContext, expr Expression, retType *types.FieldType) (Expression, error) {
	var fc functionClass
	switch retType.EvalType() {
	case types.ETInt:
		fc = &getIntVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	case types.ETDecimal:
		fc = &getDecimalVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	case types.ETReal:
		fc = &getRealVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	case types.ETDatetime:
		fc = &getTimeVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	default:
		fc = &getStringVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	}
	f, err := fc.getFunction(ctx, []Expression{expr})
	if err != nil {
		return nil, err
	}
	if builtinRetTp := f.getRetTp(); builtinRetTp.GetType() != mysql.TypeUnspecified || retType.GetType() == mysql.TypeUnspecified {
		retType = builtinRetTp
	}
	sf := &ScalarFunction{
		FuncName: ast.NewCIStr(ast.GetVar),
		RetType:  retType,
		Function: f,
	}
	return convertReadonlyVarToConst(ctx, sf), nil
}

// convertReadonlyVarToConst tries to convert the readonly user variables to constants.
func convertReadonlyVarToConst(ctx BuildContext, getVar *ScalarFunction) Expression {
	arg0, isConst := getVar.GetArgs()[0].(*Constant)
	if !isConst || arg0.DeferredExpr != nil {
		return getVar
	}
	varName := arg0.Value.GetString()
	isReadonly := ctx.IsReadonlyUserVar(varName)
	if !isReadonly {
		return getVar
	}
	v, err := getVar.Eval(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		intest.Assert(false, "readonly user variable should not meet error when executing.")
		return getVar
	}
	d, ok := ctx.GetEvalCtx().GetUserVarsReader().GetUserVarVal(varName)
	if ok && d.Kind() == types.KindBinaryLiteral {
		v.SetBinaryLiteral(v.GetBytes())
	}
	return &Constant{
		Value:        v,
		RetType:      getVar.RetType,
		DeferredExpr: nil,
	}
}

type getVarFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

type getStringVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getStringVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.tp.GetFlen())
	if len(c.tp.GetCharset()) > 0 {
		bf.tp.SetCharset(c.tp.GetCharset())
		bf.tp.SetCollate(c.tp.GetCollate())
	}
	sig = &builtinGetStringVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetStringVarSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGetStringVarSig) Clone() builtinFunc {
	newSig := &builtinGetStringVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetStringVarSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := ctx.GetUserVarsReader().GetUserVarVal(varName); ok {
		// We cannot use v.GetString() here, because the datum may be in KindMysqlTime, which
		// stores the data in datum.x.
		// This seems controversial with https://dev.mysql.com/doc/refman/8.0/en/user-variables.html:
		// > User variables can be assigned a value from a limited set of data types: integer, decimal,
		// > floating-point, binary or nonbinary string, or NULL value.
		// However, MySQL actually does support query like `set @p = now()`, so we should not assume the datum stored
		// must be of one of the following types: string, decimal, int, float.
		res, err := v.ToString()
		if err != nil {
			return "", false, err
		}
		return res, false, nil
	}
	return "", true, nil
}

type getIntVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getIntVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.tp.GetFlen())
	bf.tp.SetFlag(c.tp.GetFlag())
	sig = &builtinGetIntVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetIntVarSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGetIntVarSig) Clone() builtinFunc {
	newSig := &builtinGetIntVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetIntVarSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := ctx.GetUserVarsReader().GetUserVarVal(varName); ok {
		return v.GetInt64(), false, nil
	}
	return 0, true, nil
}

type getRealVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getRealVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.tp.GetFlen())
	sig = &builtinGetRealVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetRealVarSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGetRealVarSig) Clone() builtinFunc {
	newSig := &builtinGetRealVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetRealVarSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := ctx.GetUserVarsReader().GetUserVarVal(varName); ok {
		d, err := v.ToFloat64(typeCtx(ctx))
		if err != nil {
			return 0, false, err
		}
		return d, false, nil
	}
	return 0, true, nil
}

type getDecimalVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getDecimalVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlenUnderLimit(c.tp.GetFlen())
	sig = &builtinGetDecimalVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetDecimalVarSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGetDecimalVarSig) Clone() builtinFunc {
	newSig := &builtinGetDecimalVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetDecimalVarSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := ctx.GetUserVarsReader().GetUserVarVal(varName); ok {
		d, err := v.ToDecimal(typeCtx(ctx))
		if err != nil {
			return nil, false, err
		}
		return d, false, nil
	}
	return nil, true, nil
}

type getTimeVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getTimeVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETString)
	if err != nil {
		return nil, err
	}
	if c.tp.GetType() == mysql.TypeDatetime {
		fsp := c.tp.GetFlen() - mysql.MaxDatetimeWidthNoFsp
		if fsp > 0 {
			fsp--
		}
		bf.setDecimalAndFlenForDatetime(fsp)
	} else {
		bf.setDecimalAndFlenForDate()
	}
	sig = &builtinGetTimeVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetTimeVarSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGetTimeVarSig) Clone() builtinFunc {
	newSig := &builtinGetTimeVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetTimeVarSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := ctx.GetUserVarsReader().GetUserVarVal(varName); ok {
		return v.GetMysqlTime(), false, nil
	}
	return types.ZeroTime, true, nil
}
