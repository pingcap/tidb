// Copyright 2017 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type castJSONAsArrayFunctionSig struct {
	baseBuiltinFunc
}

func (b *castJSONAsArrayFunctionSig) Clone() builtinFunc {
	newSig := &castJSONAsArrayFunctionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// fakeSctx is used to ignore the sql mode, `cast as array` should always return error if any.
var fakeSctx = newFakeSctx()

func newFakeSctx() *stmtctx.StatementContext {
	sc := stmtctx.NewStmtCtx()
	sc.SetTypeFlags(types.StrictFlags)
	return sc
}

func (b *castJSONAsArrayFunctionSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if val.TypeCode == types.JSONTypeCodeObject {
		return types.BinaryJSON{}, false, ErrNotSupportedYet.GenWithStackByArgs("CAST-ing JSON OBJECT type to array")
	}

	arrayVals := make([]any, 0, 8)
	ft := b.tp.ArrayType()
	f := convertJSON2Tp(ft.EvalType())
	if f == nil {
		return types.BinaryJSON{}, false, ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("CAS-ing data to array of %s", ft.String()))
	}
	if val.TypeCode != types.JSONTypeCodeArray {
		item, err := f(fakeSctx, val, ft)
		if err != nil {
			return types.BinaryJSON{}, false, err
		}
		arrayVals = append(arrayVals, item)
	} else {
		for i := range val.GetElemCount() {
			item, err := f(fakeSctx, val.ArrayGetElem(i), ft)
			if err != nil {
				return types.BinaryJSON{}, false, err
			}
			arrayVals = append(arrayVals, item)
		}
	}
	return types.CreateBinaryJSON(arrayVals), false, nil
}

// ConvertJSON2Tp converts JSON to the specified type.
func ConvertJSON2Tp(v types.BinaryJSON, targetType *types.FieldType) (any, error) {
	convertFunc := convertJSON2Tp(targetType.EvalType())
	if convertFunc == nil {
		return nil, ErrInvalidJSONForFuncIndex
	}
	return convertFunc(fakeSctx, v, targetType)
}

func convertJSON2Tp(evalType types.EvalType) func(*stmtctx.StatementContext, types.BinaryJSON, *types.FieldType) (any, error) {
	switch evalType {
	case types.ETString:
		return func(sc *stmtctx.StatementContext, item types.BinaryJSON, tp *types.FieldType) (any, error) {
			if item.TypeCode != types.JSONTypeCodeString {
				return nil, ErrInvalidJSONForFuncIndex
			}
			return types.ProduceStrWithSpecifiedTp(string(item.GetString()), tp, sc.TypeCtx(), false)
		}
	case types.ETInt:
		return func(sc *stmtctx.StatementContext, item types.BinaryJSON, tp *types.FieldType) (any, error) {
			if item.TypeCode != types.JSONTypeCodeInt64 && item.TypeCode != types.JSONTypeCodeUint64 {
				return nil, ErrInvalidJSONForFuncIndex
			}
			jsonToInt, err := types.ConvertJSONToInt(sc.TypeCtx(), item, mysql.HasUnsignedFlag(tp.GetFlag()), tp.GetType())
			err = sc.HandleError(err)
			if mysql.HasUnsignedFlag(tp.GetFlag()) {
				return uint64(jsonToInt), err
			}
			return jsonToInt, err
		}
	case types.ETReal:
		return func(sc *stmtctx.StatementContext, item types.BinaryJSON, _ *types.FieldType) (any, error) {
			if item.TypeCode != types.JSONTypeCodeFloat64 && item.TypeCode != types.JSONTypeCodeInt64 && item.TypeCode != types.JSONTypeCodeUint64 {
				return nil, ErrInvalidJSONForFuncIndex
			}
			return types.ConvertJSONToFloat(sc.TypeCtx(), item)
		}
	case types.ETDatetime:
		return func(_ *stmtctx.StatementContext, item types.BinaryJSON, tp *types.FieldType) (any, error) {
			if (tp.GetType() == mysql.TypeDatetime && item.TypeCode != types.JSONTypeCodeDatetime) || (tp.GetType() == mysql.TypeDate && item.TypeCode != types.JSONTypeCodeDate) {
				return nil, ErrInvalidJSONForFuncIndex
			}
			res := item.GetTimeWithFsp(tp.GetDecimal())
			res.SetType(tp.GetType())
			if tp.GetType() == mysql.TypeDate {
				// Truncate hh:mm:ss part if the type is Date.
				res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
			}
			return res, nil
		}
	case types.ETDuration:
		return func(_ *stmtctx.StatementContext, item types.BinaryJSON, _ *types.FieldType) (any, error) {
			if item.TypeCode != types.JSONTypeCodeDuration {
				return nil, ErrInvalidJSONForFuncIndex
			}
			return item.GetDuration(), nil
		}
	default:
		return nil
	}
}

type castAsJSONFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsJSONFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsJson)
	case types.ETReal:
		sig = &builtinCastRealAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsJson)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsJson)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsJson)
	case types.ETDuration:
		sig = &builtinCastDurationAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsJson)
	case types.ETJson:
		sig = &builtinCastJSONAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsJson)
	case types.ETString:
		sig = &builtinCastStringAsJSONSig{bf}
		sig.getRetTp().AddFlag(mysql.ParseToJSONFlag)
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsJson)
	case types.ETVectorFloat32:
		sig = &builtinCastVectorFloat32AsUnsupportedSig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastVectorFloat32AsJson)
	default:
		return nil, errors.Errorf("cannot cast from %s to %s", argTp, "Json")
	}
	return sig, nil
}

type castAsVectorFloat32FunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsVectorFloat32FunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastUnsupportedAsVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastIntAsVectorFloat32)
	case types.ETReal:
		sig = &builtinCastUnsupportedAsVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastRealAsVectorFloat32)
	case types.ETDecimal:
		sig = &builtinCastUnsupportedAsVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsVectorFloat32)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastUnsupportedAsVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsVectorFloat32)
	case types.ETDuration:
		sig = &builtinCastUnsupportedAsVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsVectorFloat32)
	case types.ETJson:
		sig = &builtinCastUnsupportedAsVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsVectorFloat32)
	case types.ETVectorFloat32:
		sig = &builtinCastVectorFloat32AsVectorFloat32Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastVectorFloat32AsVectorFloat32)
	case types.ETString:
		sig = &builtinCastStringAsVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastStringAsVectorFloat32)
	default:
		return nil, errors.Errorf("cannot cast from %s to %s", argTp, "VectorFloat32")
	}
	return sig, nil
}

type builtinCastUnsupportedAsVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastUnsupportedAsVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinCastUnsupportedAsVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastUnsupportedAsVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, _ chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	return types.ZeroVectorFloat32, false, errors.Errorf(
		"cannot cast from %s to vector",
		types.TypeStr(b.args[0].GetType(ctx).GetType()))
}

type builtinCastVectorFloat32AsUnsupportedSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) Clone() builtinFunc {
	newSig := &builtinCastVectorFloat32AsUnsupportedSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) evalInt(_ EvalContext, _ chunk.Row) (int64, bool, error) {
	return 0, false, errors.Errorf(
		"cannot cast from vector to %s",
		types.TypeStr(b.tp.GetType()))
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) evalReal(_ EvalContext, _ chunk.Row) (float64, bool, error) {
	return 0, false, errors.Errorf(
		"cannot cast from vector to %s",
		types.TypeStr(b.tp.GetType()))
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) evalDecimal(_ EvalContext, _ chunk.Row) (*types.MyDecimal, bool, error) {
	return nil, false, errors.Errorf(
		"cannot cast from vector to %s",
		types.TypeStr(b.tp.GetType()))
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) evalString(_ EvalContext, _ chunk.Row) (string, bool, error) {
	return "", false, errors.Errorf(
		"cannot cast from vector to %s",
		types.TypeStr(b.tp.GetType()))
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) evalTime(_ EvalContext, _ chunk.Row) (types.Time, bool, error) {
	return types.ZeroTime, false, errors.Errorf(
		"cannot cast from vector to %s",
		types.TypeStr(b.tp.GetType()))
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) evalDuration(_ EvalContext, _ chunk.Row) (types.Duration, bool, error) {
	return types.ZeroDuration, false, errors.Errorf(
		"cannot cast from vector to %s",
		types.TypeStr(b.tp.GetType()))
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) evalJSON(_ EvalContext, _ chunk.Row) (types.BinaryJSON, bool, error) {
	return types.BinaryJSON{}, false, errors.Errorf(
		"cannot cast from vector to %s",
		types.TypeStr(b.tp.GetType()))
}

type builtinCastStringAsVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastStringAsVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinCastStringAsVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	vec, err := types.ParseVectorFloat32(val)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	if err = vec.CheckDimsFitColumn(b.tp.GetFlen()); err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	return vec, false, nil
}

type builtinCastVectorFloat32AsVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastVectorFloat32AsVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinCastVectorFloat32AsVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastVectorFloat32AsVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	val, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	if err = val.CheckDimsFitColumn(b.tp.GetFlen()); err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	return val, false, nil
}

