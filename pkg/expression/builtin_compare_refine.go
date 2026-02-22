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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

func (c *compareFunctionClass) refineArgs(ctx BuildContext, args []Expression) ([]Expression, error) {
	arg0Type, arg1Type := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	arg0EvalType, arg1EvalType := arg0Type.EvalType(), arg1Type.EvalType()
	arg0IsInt := arg0EvalType == types.ETInt
	arg1IsInt := arg1EvalType == types.ETInt
	arg0, arg0IsCon := args[0].(*Constant)
	arg1, arg1IsCon := args[1].(*Constant)
	isExceptional, finalArg0, finalArg1 := false, args[0], args[1]
	isPositiveInfinite, isNegativeInfinite := false, false

	if !allowCmpArgsRefining4PlanCache(ctx, args) {
		return args, nil
	}
	// We should remove the mutable constant for correctness, because its value may be changed.
	if err := RemoveMutableConst(ctx, args...); err != nil {
		return nil, err
	}

	// Handle comparison between a duration type column and a non-duration type constant.
	if c.op == opcode.NullEQ {
		if result, err := c.handleDurationTypeComparisonForNullEq(ctx, args[0], args[1]); err != nil || result != nil {
			return result, err
		}
	}

	if arg0IsCon && !arg1IsCon && matchRefineRule3Pattern(arg0EvalType, arg1Type) {
		return c.refineNumericConstantCmpDatetime(ctx, args, arg0, 0), nil
	}

	if !arg0IsCon && arg1IsCon && matchRefineRule3Pattern(arg1EvalType, arg0Type) {
		return c.refineNumericConstantCmpDatetime(ctx, args, arg1, 1), nil
	}

	// int non-constant [cmp] non-int constant
	if arg0IsInt && !arg0IsCon && !arg1IsInt && arg1IsCon {
		arg1, isExceptional = RefineComparedConstant(ctx, *arg0Type, arg1, c.op)
		// Why check not null flag
		// eg: int_col > const_val(which is less than min_int32)
		// If int_col got null, compare result cannot be true
		if !isExceptional || (isExceptional && mysql.HasNotNullFlag(arg0Type.GetFlag())) {
			finalArg1 = arg1
		}
		// TODO if the plan doesn't care about whether the result of the function is null or false, we don't need
		// to check the NotNullFlag, then more optimizations can be enabled.
		isExceptional = isExceptional && mysql.HasNotNullFlag(arg0Type.GetFlag())
		if isExceptional && arg1.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
			// Judge it is inf or -inf
			// For int:
			//			inf:  01111111 & 1 == 1
			//		   -inf:  10000000 & 1 == 0
			// For uint:
			//			inf:  11111111 & 1 == 1
			//		   -inf:  00000000 & 1 == 0
			if arg1.Value.GetInt64()&1 == 1 {
				isPositiveInfinite = true
			} else {
				isNegativeInfinite = true
			}
		}
	}
	// non-int constant [cmp] int non-constant
	if arg1IsInt && !arg1IsCon && !arg0IsInt && arg0IsCon {
		arg0, isExceptional = RefineComparedConstant(ctx, *arg1Type, arg0, symmetricOp[c.op])
		if !isExceptional || (isExceptional && mysql.HasNotNullFlag(arg1Type.GetFlag())) {
			finalArg0 = arg0
		}
		// TODO if the plan doesn't care about whether the result of the function is null or false, we don't need
		// to check the NotNullFlag, then more optimizations can be enabled.
		isExceptional = isExceptional && mysql.HasNotNullFlag(arg1Type.GetFlag())
		if isExceptional && arg0.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
			if arg0.Value.GetInt64()&1 == 1 {
				isNegativeInfinite = true
			} else {
				isPositiveInfinite = true
			}
		}
	}

	// int constant [cmp] year type
	if arg0IsCon && arg0IsInt && arg1Type.GetType() == mysql.TypeYear && !arg0.Value.IsNull() {
		adjusted, failed := types.AdjustYear(arg0.Value.GetInt64(), false)
		if failed == nil {
			arg0.Value.SetInt64(adjusted)
			finalArg0 = arg0
		}
	}
	// year type [cmp] int constant
	if arg1IsCon && arg1IsInt && arg0Type.GetType() == mysql.TypeYear && !arg1.Value.IsNull() {
		adjusted, failed := types.AdjustYear(arg1.Value.GetInt64(), false)
		if failed == nil {
			arg1.Value.SetInt64(adjusted)
			finalArg1 = arg1
		}
	}
	if isExceptional && (c.op == opcode.EQ || c.op == opcode.NullEQ) {
		// This will always be false.
		return []Expression{NewZero(), NewOne()}, nil
	}
	if isPositiveInfinite {
		// If the op is opcode.LT, opcode.LE
		// This will always be true.
		// If the op is opcode.GT, opcode.GE
		// This will always be false.
		return []Expression{NewZero(), NewOne()}, nil
	}
	if isNegativeInfinite {
		// If the op is opcode.GT, opcode.GE
		// This will always be true.
		// If the op is opcode.LT, opcode.LE
		// This will always be false.
		return []Expression{NewOne(), NewZero()}, nil
	}

	return c.refineArgsByUnsignedFlag(ctx, []Expression{finalArg0, finalArg1}), nil
}

// see https://github.com/pingcap/tidb/issues/38361 for more details
func (c *compareFunctionClass) refineNumericConstantCmpDatetime(ctx BuildContext, args []Expression, constArg *Constant, constArgIdx int) []Expression {
	evalCtx := ctx.GetEvalCtx()
	dt, err := constArg.Eval(evalCtx, chunk.Row{})
	if err != nil || dt.IsNull() {
		return args
	}
	var datetimeDatum types.Datum
	targetFieldType := types.NewFieldType(mysql.TypeDatetime)
	datetimeDatum, err = dt.ConvertTo(evalCtx.TypeCtx(), targetFieldType)
	if err != nil || datetimeDatum.IsNull() {
		return args
	}
	finalArg := Constant{
		Value:        datetimeDatum,
		RetType:      targetFieldType,
		DeferredExpr: nil,
		ParamMarker:  nil,
	}
	if constArgIdx == 0 {
		return []Expression{&finalArg, args[1]}
	}
	return []Expression{args[0], &finalArg}
}

func (c *compareFunctionClass) refineArgsByUnsignedFlag(ctx BuildContext, args []Expression) []Expression {
	// Only handle int cases, cause MySQL declares that `UNSIGNED` is deprecated for FLOAT, DOUBLE and DECIMAL types,
	// and support for it would be removed in a future version.
	if args[0].GetType(ctx.GetEvalCtx()).EvalType() != types.ETInt || args[1].GetType(ctx.GetEvalCtx()).EvalType() != types.ETInt {
		return args
	}
	colArgs := make([]*Column, 2)
	constArgs := make([]*Constant, 2)
	for i, arg := range args {
		switch x := arg.(type) {
		case *Constant:
			constArgs[i] = x
		case *Column:
			colArgs[i] = x
		case *CorrelatedColumn:
			colArgs[i] = &x.Column
		}
	}
	for i := range 2 {
		if con, col := constArgs[1-i], colArgs[i]; con != nil && col != nil {
			v, isNull, err := con.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil || isNull || v > 0 {
				return args
			}
			if mysql.HasUnsignedFlag(con.RetType.GetFlag()) && !mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
				op := c.op
				if i == 1 {
					op = symmetricOp[c.op]
				}
				if (op == opcode.EQ && mysql.HasNotNullFlag(col.RetType.GetFlag())) || op == opcode.NullEQ {
					if _, err := types.ConvertUintToInt(uint64(v), types.IntegerSignedUpperBound(col.RetType.GetType()), col.RetType.GetType()); err != nil {
						args[i], args[1-i] = NewOne(), NewZero()
						return args
					}
				}
			}
			if mysql.HasUnsignedFlag(col.RetType.GetFlag()) && mysql.HasNotNullFlag(col.RetType.GetFlag()) && !mysql.HasUnsignedFlag(con.RetType.GetFlag()) {
				op := c.op
				if i == 1 {
					op = symmetricOp[c.op]
				}
				if v == 0 && (op == opcode.LE || op == opcode.GT || op == opcode.NullEQ || op == opcode.EQ || op == opcode.NE) {
					return args
				}
				// `unsigned_col < 0` equals to `1 < 0`,
				// `unsigned_col > -1` equals to `1 > 0`,
				// `unsigned_col <= -1` equals to `1 <= 0`,
				// `unsigned_col >= 0` equals to `1 >= 0`,
				// `unsigned_col == -1` equals to `1 == 0`,
				// `unsigned_col != -1` equals to `1 != 0`,
				// `unsigned_col <=> -1` equals to `1 <=> 0`,
				// so we can replace the column argument with `1`, and the other constant argument with `0`.
				args[i], args[1-i] = NewOne(), NewZero()
				return args
			}
		}
	}
	return args
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(ctx BuildContext, rawArgs []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(rawArgs); err != nil {
		return nil, err
	}
	args, err := c.refineArgs(ctx, rawArgs)
	if err != nil {
		return nil, err
	}
	cmpType := GetAccurateCmpType(ctx.GetEvalCtx(), args[0], args[1])
	sig, err = c.generateCmpSigs(ctx, args, cmpType)
	return sig, err
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx BuildContext, args []Expression, tp types.EvalType) (sig builtinFunc, err error) {
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, tp, tp)
	if err != nil {
		return nil, err
	}
	if tp == types.ETJson {
		// In compare, if we cast string to JSON, we shouldn't parse it.
		for i := range args {
			DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
		}
	}
	bf.tp.SetFlen(1)
	switch tp {
	case types.ETInt:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTInt)
		case opcode.LE:
			sig = &builtinLEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEInt)
		case opcode.GT:
			sig = &builtinGTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTInt)
		case opcode.EQ:
			sig = &builtinEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQInt)
		case opcode.GE:
			sig = &builtinGEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEInt)
		case opcode.NE:
			sig = &builtinNEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEInt)
		case opcode.NullEQ:
			sig = &builtinNullEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQInt)
		}
	case types.ETReal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTReal)
		case opcode.LE:
			sig = &builtinLERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEReal)
		case opcode.GT:
			sig = &builtinGTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTReal)
		case opcode.GE:
			sig = &builtinGERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEReal)
		case opcode.EQ:
			sig = &builtinEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQReal)
		case opcode.NE:
			sig = &builtinNERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEReal)
		case opcode.NullEQ:
			sig = &builtinNullEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQReal)
		}
	case types.ETDecimal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDecimal)
		case opcode.LE:
			sig = &builtinLEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDecimal)
		case opcode.GT:
			sig = &builtinGTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDecimal)
		case opcode.GE:
			sig = &builtinGEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDecimal)
		case opcode.EQ:
			sig = &builtinEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDecimal)
		case opcode.NE:
			sig = &builtinNEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDecimal)
		case opcode.NullEQ:
			sig = &builtinNullEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQDecimal)
		}
	case types.ETString:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTString)
		case opcode.LE:
			sig = &builtinLEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEString)
		case opcode.GT:
			sig = &builtinGTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTString)
		case opcode.GE:
			sig = &builtinGEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEString)
		case opcode.EQ:
			sig = &builtinEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQString)
		case opcode.NE:
			sig = &builtinNEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEString)
		case opcode.NullEQ:
			sig = &builtinNullEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQString)
		}
	case types.ETDuration:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDuration)
		case opcode.LE:
			sig = &builtinLEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDuration)
		case opcode.GT:
			sig = &builtinGTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDuration)
		case opcode.GE:
			sig = &builtinGEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDuration)
		case opcode.EQ:
			sig = &builtinEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDuration)
		case opcode.NE:
			sig = &builtinNEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDuration)
		case opcode.NullEQ:
			sig = &builtinNullEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQDuration)
		}
	case types.ETDatetime, types.ETTimestamp:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTTime)
		case opcode.LE:
			sig = &builtinLETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LETime)
		case opcode.GT:
			sig = &builtinGTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTTime)
		case opcode.GE:
			sig = &builtinGETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GETime)
		case opcode.EQ:
			sig = &builtinEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQTime)
		case opcode.NE:
			sig = &builtinNETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NETime)
		case opcode.NullEQ:
			sig = &builtinNullEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQTime)
		}
	case types.ETJson:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTJson)
		case opcode.LE:
			sig = &builtinLEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEJson)
		case opcode.GT:
			sig = &builtinGTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTJson)
		case opcode.GE:
			sig = &builtinGEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEJson)
		case opcode.EQ:
			sig = &builtinEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQJson)
		case opcode.NE:
			sig = &builtinNEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEJson)
		case opcode.NullEQ:
			sig = &builtinNullEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQJson)
		}
	case types.ETVectorFloat32:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTVectorFloat32)
		case opcode.LE:
			sig = &builtinLEVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEVectorFloat32)
		case opcode.GT:
			sig = &builtinGTVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTVectorFloat32)
		case opcode.GE:
			sig = &builtinGEVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEVectorFloat32)
		case opcode.EQ:
			sig = &builtinEQVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQVectorFloat32)
		case opcode.NE:
			sig = &builtinNEVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEVectorFloat32)
		case opcode.NullEQ:
			sig = &builtinNullEQVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQVectorFloat32)
		}
	default:
		return nil, errors.Errorf("operator %s is not supported for %s", c.op, tp)
	}
	return
}
