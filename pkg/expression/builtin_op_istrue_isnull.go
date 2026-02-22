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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type isTrueOrFalseFunctionClass struct {
	baseFunctionClass
	op opcode.Op

	// keepNull indicates how this function treats a null input parameter.
	// If keepNull is true and the input parameter is null, the function will return null.
	// If keepNull is false, the null input parameter will be cast to 0.
	keepNull bool
}

func (c *isTrueOrFalseFunctionClass) getDisplayName() string {
	var nameBuilder strings.Builder
	c.op.Format(&nameBuilder)
	return nameBuilder.String()
}

func (c *isTrueOrFalseFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	if argTp == types.ETTimestamp || argTp == types.ETDatetime || argTp == types.ETDuration || argTp == types.ETJson || argTp == types.ETString {
		argTp = types.ETReal
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)

	var sig builtinFunc
	switch c.op {
	case opcode.IsTruth:
		switch argTp {
		case types.ETReal:
			sig = &builtinRealIsTrueSig{bf, c.keepNull}
			if c.keepNull {
				sig.setPbCode(tipb.ScalarFuncSig_RealIsTrueWithNull)
			} else {
				sig.setPbCode(tipb.ScalarFuncSig_RealIsTrue)
			}
		case types.ETDecimal:
			sig = &builtinDecimalIsTrueSig{bf, c.keepNull}
			if c.keepNull {
				sig.setPbCode(tipb.ScalarFuncSig_DecimalIsTrueWithNull)
			} else {
				sig.setPbCode(tipb.ScalarFuncSig_DecimalIsTrue)
			}
		case types.ETInt:
			sig = &builtinIntIsTrueSig{bf, c.keepNull}
			if c.keepNull {
				sig.setPbCode(tipb.ScalarFuncSig_IntIsTrueWithNull)
			} else {
				sig.setPbCode(tipb.ScalarFuncSig_IntIsTrue)
			}
		case types.ETVectorFloat32:
			sig = &builtinVectorFloat32IsTrueSig{bf, c.keepNull}
			// if c.keepNull {
			// 	sig.setPbCode(tipb.ScalarFuncSig_VectorFloat32IsTrueWithNull)
			// } else {
			// 	sig.setPbCode(tipb.ScalarFuncSig_VectorFloat32IsTrue)
			// }
		default:
			return nil, errors.Errorf("unexpected types.EvalType %v", argTp)
		}
	case opcode.IsFalsity:
		switch argTp {
		case types.ETReal:
			sig = &builtinRealIsFalseSig{bf, c.keepNull}
			if c.keepNull {
				sig.setPbCode(tipb.ScalarFuncSig_RealIsFalseWithNull)
			} else {
				sig.setPbCode(tipb.ScalarFuncSig_RealIsFalse)
			}
		case types.ETDecimal:
			sig = &builtinDecimalIsFalseSig{bf, c.keepNull}
			if c.keepNull {
				sig.setPbCode(tipb.ScalarFuncSig_DecimalIsFalseWithNull)
			} else {
				sig.setPbCode(tipb.ScalarFuncSig_DecimalIsFalse)
			}
		case types.ETInt:
			sig = &builtinIntIsFalseSig{bf, c.keepNull}
			if c.keepNull {
				sig.setPbCode(tipb.ScalarFuncSig_IntIsFalseWithNull)
			} else {
				sig.setPbCode(tipb.ScalarFuncSig_IntIsFalse)
			}
		case types.ETVectorFloat32:
			sig = &builtinVectorFloat32IsFalseSig{bf, c.keepNull}
			// if c.keepNull {
			// 	sig.setPbCode(tipb.ScalarFuncSig_VectorFloat32IsFalseWithNull)
			// } else {
			// 	sig.setPbCode(tipb.ScalarFuncSig_VectorFloat32IsFalse)
			// }
		default:
			return nil, errors.Errorf("unexpected types.EvalType %v", argTp)
		}
	}
	return sig, nil
}

type builtinRealIsTrueSig struct {
	baseBuiltinFunc
	keepNull bool

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRealIsTrueSig) Clone() builtinFunc {
	newSig := &builtinRealIsTrueSig{keepNull: b.keepNull}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsTrueSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalReal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input == 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinDecimalIsTrueSig struct {
	baseBuiltinFunc
	keepNull bool

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinDecimalIsTrueSig) Clone() builtinFunc {
	newSig := &builtinDecimalIsTrueSig{keepNull: b.keepNull}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinDecimalIsTrueSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input.IsZero() {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinIntIsTrueSig struct {
	baseBuiltinFunc
	keepNull bool

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIntIsTrueSig) Clone() builtinFunc {
	newSig := &builtinIntIsTrueSig{keepNull: b.keepNull}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsTrueSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input == 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinVectorFloat32IsTrueSig struct {
	baseBuiltinFunc
	keepNull bool
}

func (b *builtinVectorFloat32IsTrueSig) Clone() builtinFunc {
	newSig := &builtinVectorFloat32IsTrueSig{keepNull: b.keepNull}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinVectorFloat32IsTrueSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input.IsZeroValue() {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinRealIsFalseSig struct {
	baseBuiltinFunc
	keepNull bool

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRealIsFalseSig) Clone() builtinFunc {
	newSig := &builtinRealIsFalseSig{keepNull: b.keepNull}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsFalseSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalReal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input != 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinDecimalIsFalseSig struct {
	baseBuiltinFunc
	keepNull bool

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinDecimalIsFalseSig) Clone() builtinFunc {
	newSig := &builtinDecimalIsFalseSig{keepNull: b.keepNull}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinDecimalIsFalseSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || !input.IsZero() {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinIntIsFalseSig struct {
	baseBuiltinFunc
	keepNull bool

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinIntIsFalseSig) Clone() builtinFunc {
	newSig := &builtinIntIsFalseSig{keepNull: b.keepNull}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsFalseSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input != 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinVectorFloat32IsFalseSig struct {
	baseBuiltinFunc
	keepNull bool
}

func (b *builtinVectorFloat32IsFalseSig) Clone() builtinFunc {
	newSig := &builtinVectorFloat32IsFalseSig{keepNull: b.keepNull}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinVectorFloat32IsFalseSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || !input.IsZeroValue() {
		return 0, false, nil
	}
	return 1, false, nil
}

type bitNegFunctionClass struct {
	baseFunctionClass
}

func (c *bitNegFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.AddFlag(mysql.UnsignedFlag)
	sig := &builtinBitNegSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitNegSig)
	return sig, nil
}

type builtinBitNegSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinBitNegSig) Clone() builtinFunc {
	newSig := &builtinBitNegSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinBitNegSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return ^arg, false, nil
}

type unaryNotFunctionClass struct {
	baseFunctionClass
}

func (c *unaryNotFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	if argTp == types.ETTimestamp || argTp == types.ETDatetime || argTp == types.ETDuration {
		argTp = types.ETInt
	} else if argTp == types.ETString {
		argTp = types.ETReal
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)

	var sig builtinFunc
	switch argTp {
	case types.ETReal:
		sig = &builtinUnaryNotRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryNotReal)
	case types.ETDecimal:
		sig = &builtinUnaryNotDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryNotDecimal)
	case types.ETInt:
		sig = &builtinUnaryNotIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryNotInt)
	case types.ETJson:
		ctx.GetEvalCtx().AppendWarning(errJSONInBooleanContext)
		sig = &builtinUnaryNotJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryNotJSON)
	default:
		return nil, errors.Errorf("%s is not supported for unary not operator", argTp)
	}
	return sig, nil
}

type builtinUnaryNotRealSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUnaryNotRealSig) Clone() builtinFunc {
	newSig := &builtinUnaryNotRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotRealSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if arg == 0 {
		return 1, false, nil
	}
	return 0, false, nil
}

type builtinUnaryNotDecimalSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUnaryNotDecimalSig) Clone() builtinFunc {
	newSig := &builtinUnaryNotDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if arg.IsZero() {
		return 1, false, nil
	}
	return 0, false, nil
}

type builtinUnaryNotIntSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUnaryNotIntSig) Clone() builtinFunc {
	newSig := &builtinUnaryNotIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if arg == 0 {
		return 1, false, nil
	}
	return 0, false, nil
}

type builtinUnaryNotJSONSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUnaryNotJSONSig) Clone() builtinFunc {
	newSig := &builtinUnaryNotJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotJSONSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	if types.CompareBinaryJSON(arg, types.CreateBinaryJSON(int64(0))) == 0 {
		return 1, false, nil
	}
	return 0, false, nil
}
