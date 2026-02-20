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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type ifFunctionClass struct {
	baseFunctionClass
}

// getFunction see https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	retTp, err := InferType4ControlFuncs(ctx, c.funcName, args[1], args[2])
	if err != nil {
		return nil, err
	}
	evalTps := retTp.EvalType()
	args[0], err = wrapWithIsTrue(ctx, true, args[0], false)
	if err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithFieldTypes(ctx, c.funcName, args, evalTps, args[0].GetType(ctx.GetEvalCtx()).Clone(), retTp.Clone(), retTp.Clone())
	if err != nil {
		return nil, err
	}
	retTp.AddFlag(bf.tp.GetFlag())
	bf.tp = retTp
	switch evalTps {
	case types.ETInt:
		sig = &builtinIfIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfInt)
	case types.ETReal:
		sig = &builtinIfRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfReal)
	case types.ETDecimal:
		sig = &builtinIfDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfDecimal)
	case types.ETString:
		sig = &builtinIfStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinIfTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfTime)
	case types.ETDuration:
		sig = &builtinIfDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfDuration)
	case types.ETJson:
		sig = &builtinIfJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfJson)
	case types.ETVectorFloat32:
		sig = &builtinIfVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_IfVectorFloat32)
	default:
		return nil, errors.Errorf("%s is not supported for IF()", evalTps)
	}
	return sig, nil
}

type builtinIfIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfIntSig) Clone() builtinFunc {
	newSig := &builtinIfIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalInt(ctx, row)
	}
	return b.args[2].EvalInt(ctx, row)
}

type builtinIfRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfRealSig) Clone() builtinFunc {
	newSig := &builtinIfRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfRealSig) evalReal(ctx EvalContext, row chunk.Row) (val float64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalReal(ctx, row)
	}
	return b.args[2].EvalReal(ctx, row)
}

type builtinIfDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfDecimalSig) Clone() builtinFunc {
	newSig := &builtinIfDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (val *types.MyDecimal, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return nil, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalDecimal(ctx, row)
	}
	return b.args[2].EvalDecimal(ctx, row)
}

type builtinIfStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfStringSig) Clone() builtinFunc {
	newSig := &builtinIfStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfStringSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return "", true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalString(ctx, row)
	}
	return b.args[2].EvalString(ctx, row)
}

type builtinIfTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfTimeSig) Clone() builtinFunc {
	newSig := &builtinIfTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfTimeSig) evalTime(ctx EvalContext, row chunk.Row) (ret types.Time, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalTime(ctx, row)
	}
	return b.args[2].EvalTime(ctx, row)
}

type builtinIfDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfDurationSig) Clone() builtinFunc {
	newSig := &builtinIfDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (ret types.Duration, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalDuration(ctx, row)
	}
	return b.args[2].EvalDuration(ctx, row)
}

type builtinIfJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfJSONSig) Clone() builtinFunc {
	newSig := &builtinIfJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (ret types.BinaryJSON, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalJSON(ctx, row)
	}
	return b.args[2].EvalJSON(ctx, row)
}

type builtinIfVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinIfVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (ret types.VectorFloat32, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalVectorFloat32(ctx, row)
	}
	return b.args[2].EvalVectorFloat32(ctx, row)
}

type ifNullFunctionClass struct {
	baseFunctionClass
}

func (c *ifNullFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhs, rhs := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	retTp, err := InferType4ControlFuncs(ctx, c.funcName, args[0], args[1])
	if err != nil {
		return nil, err
	}

	retTp.AddFlag((lhs.GetFlag() & mysql.NotNullFlag) | (rhs.GetFlag() & mysql.NotNullFlag))
	if lhs.GetType() == mysql.TypeNull && rhs.GetType() == mysql.TypeNull {
		retTp.SetType(mysql.TypeNull)
		retTp.SetFlen(0)
		retTp.SetDecimal(0)
		types.SetBinChsClnFlag(retTp)
	}
	evalTps := retTp.EvalType()
	bf, err := newBaseBuiltinFuncWithFieldTypes(ctx, c.funcName, args, evalTps, retTp.Clone(), retTp.Clone())
	if err != nil {
		return nil, err
	}
	bf.tp = retTp
	switch evalTps {
	case types.ETInt:
		sig = &builtinIfNullIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullInt)
	case types.ETReal:
		sig = &builtinIfNullRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullReal)
	case types.ETDecimal:
		sig = &builtinIfNullDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullDecimal)
	case types.ETString:
		sig = &builtinIfNullStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinIfNullTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullTime)
	case types.ETDuration:
		sig = &builtinIfNullDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullDuration)
	case types.ETJson:
		sig = &builtinIfNullJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullJson)
	case types.ETVectorFloat32:
		sig = &builtinIfNullVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_IfNullVectorFloat32)
	default:
		return nil, errors.Errorf("%s is not supported for IFNULL()", evalTps)
	}
	return sig, nil
}

type builtinIfNullIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfNullIntSig) Clone() builtinFunc {
	newSig := &builtinIfNullIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalInt(ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfNullRealSig) Clone() builtinFunc {
	newSig := &builtinIfNullRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	arg0, isNull, err := b.args[0].EvalReal(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalReal(ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfNullDecimalSig) Clone() builtinFunc {
	newSig := &builtinIfNullDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	arg0, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalDecimal(ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfNullStringSig) Clone() builtinFunc {
	newSig := &builtinIfNullStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalString(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalString(ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfNullTimeSig) Clone() builtinFunc {
	newSig := &builtinIfNullTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullTimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalTime(ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfNullDurationSig) Clone() builtinFunc {
	newSig := &builtinIfNullDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfNullJSONSig) Clone() builtinFunc {
	newSig := &builtinIfNullJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	arg0, isNull, err := b.args[0].EvalJSON(ctx, row)
	if !isNull {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalJSON(ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIfNullVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinIfNullVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	arg0, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if !isNull {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalVectorFloat32(ctx, row)
	return arg1, isNull || err != nil, err
}
