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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &coalesceFunctionClass{}
	_ functionClass = &greatestFunctionClass{}
	_ functionClass = &leastFunctionClass{}
	_ functionClass = &intervalFunctionClass{}
	_ functionClass = &compareFunctionClass{}
)

var (
	_ builtinFunc = &builtinCoalesceIntSig{}
	_ builtinFunc = &builtinCoalesceRealSig{}
	_ builtinFunc = &builtinCoalesceDecimalSig{}
	_ builtinFunc = &builtinCoalesceStringSig{}
	_ builtinFunc = &builtinCoalesceTimeSig{}
	_ builtinFunc = &builtinCoalesceDurationSig{}

	_ builtinFunc = &builtinGreatestIntSig{}
	_ builtinFunc = &builtinGreatestRealSig{}
	_ builtinFunc = &builtinGreatestDecimalSig{}
	_ builtinFunc = &builtinGreatestStringSig{}
	_ builtinFunc = &builtinGreatestTimeSig{}
	_ builtinFunc = &builtinLeastIntSig{}
	_ builtinFunc = &builtinLeastRealSig{}
	_ builtinFunc = &builtinLeastDecimalSig{}
	_ builtinFunc = &builtinLeastStringSig{}
	_ builtinFunc = &builtinLeastTimeSig{}
	_ builtinFunc = &builtinIntervalIntSig{}
	_ builtinFunc = &builtinIntervalRealSig{}

	_ builtinFunc = &builtinLTIntSig{}
	_ builtinFunc = &builtinLTRealSig{}
	_ builtinFunc = &builtinLTDecimalSig{}
	_ builtinFunc = &builtinLTStringSig{}
	_ builtinFunc = &builtinLTDurationSig{}
	_ builtinFunc = &builtinLTTimeSig{}

	_ builtinFunc = &builtinLEIntSig{}
	_ builtinFunc = &builtinLERealSig{}
	_ builtinFunc = &builtinLEDecimalSig{}
	_ builtinFunc = &builtinLEStringSig{}
	_ builtinFunc = &builtinLEDurationSig{}
	_ builtinFunc = &builtinLETimeSig{}

	_ builtinFunc = &builtinGTIntSig{}
	_ builtinFunc = &builtinGTRealSig{}
	_ builtinFunc = &builtinGTDecimalSig{}
	_ builtinFunc = &builtinGTStringSig{}
	_ builtinFunc = &builtinGTTimeSig{}
	_ builtinFunc = &builtinGTDurationSig{}

	_ builtinFunc = &builtinGEIntSig{}
	_ builtinFunc = &builtinGERealSig{}
	_ builtinFunc = &builtinGEDecimalSig{}
	_ builtinFunc = &builtinGEStringSig{}
	_ builtinFunc = &builtinGETimeSig{}
	_ builtinFunc = &builtinGEDurationSig{}

	_ builtinFunc = &builtinNEIntSig{}
	_ builtinFunc = &builtinNERealSig{}
	_ builtinFunc = &builtinNEDecimalSig{}
	_ builtinFunc = &builtinNEStringSig{}
	_ builtinFunc = &builtinNETimeSig{}
	_ builtinFunc = &builtinNEDurationSig{}

	_ builtinFunc = &builtinNullEQIntSig{}
	_ builtinFunc = &builtinNullEQRealSig{}
	_ builtinFunc = &builtinNullEQDecimalSig{}
	_ builtinFunc = &builtinNullEQStringSig{}
	_ builtinFunc = &builtinNullEQTimeSig{}
	_ builtinFunc = &builtinNullEQDurationSig{}
)

// coalesceFunctionClass returns the first non-NULL value in the list,
// or NULL if there are no non-NULL values.
type coalesceFunctionClass struct {
	baseFunctionClass
}

func (c *coalesceFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	fieldTps := make([]*types.FieldType, 0, len(args))
	for _, arg := range args {
		fieldTps = append(fieldTps, arg.GetType())
	}

	// Use the aggregated field type as retType.
	resultFieldType := types.AggFieldType(fieldTps)
	resultEvalType := types.AggregateEvalType(fieldTps, &resultFieldType.Flag)
	retEvalTp := resultFieldType.EvalType()

	fieldEvalTps := make([]types.EvalType, 0, len(args))
	for range args {
		fieldEvalTps = append(fieldEvalTps, retEvalTp)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, retEvalTp, fieldEvalTps...)

	bf.tp.Flag |= resultFieldType.Flag
	resultFieldType.Flen, resultFieldType.Decimal = 0, types.UnspecifiedLength

	// Set retType to BINARY(0) if all arguments are of type NULL.
	if resultFieldType.Tp == mysql.TypeNull {
		types.SetBinChsClnFlag(bf.tp)
	} else {
		maxIntLen := 0
		maxFlen := 0

		// Find the max length of field in `maxFlen`,
		// and max integer-part length in `maxIntLen`.
		for _, argTp := range fieldTps {
			if argTp.Decimal > resultFieldType.Decimal {
				resultFieldType.Decimal = argTp.Decimal
			}
			argIntLen := argTp.Flen
			if argTp.Decimal > 0 {
				argIntLen -= argTp.Decimal + 1
			}

			// Reduce the sign bit if it is a signed integer/decimal
			if !mysql.HasUnsignedFlag(argTp.Flag) {
				argIntLen--
			}
			if argIntLen > maxIntLen {
				maxIntLen = argIntLen
			}
			if argTp.Flen > maxFlen || argTp.Flen == types.UnspecifiedLength {
				maxFlen = argTp.Flen
			}
		}

		// For integer, field length = maxIntLen + (1/0 for sign bit)
		// For decimal, field length = maxIntLen + maxDecimal + (1/0 for sign bit)
		if resultEvalType == types.ETInt || resultEvalType == types.ETDecimal {
			resultFieldType.Flen = maxIntLen + resultFieldType.Decimal
			if resultFieldType.Decimal > 0 {
				resultFieldType.Flen++
			}
			if !mysql.HasUnsignedFlag(resultFieldType.Flag) {
				resultFieldType.Flen++
			}
			bf.tp = resultFieldType
		} else {
			// Set the field length to maxFlen for other types.
			bf.tp.Flen = maxFlen
		}
	}

	switch retEvalTp {
	case types.ETInt:
		sig = &builtinCoalesceIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceInt)
	case types.ETReal:
		sig = &builtinCoalesceRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceReal)
	case types.ETDecimal:
		sig = &builtinCoalesceDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceDecimal)
	case types.ETString:
		sig = &builtinCoalesceStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCoalesceTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceTime)
	case types.ETDuration:
		sig = &builtinCoalesceDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceDuration)
	}

	return sig, nil
}

// builtinCoalesceIntSig is buitin function coalesce signature which return type int
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalInt(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, errors.Trace(err)
}

// builtinCoalesceRealSig is buitin function coalesce signature which return type real
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceRealSig) evalReal(row types.Row) (res float64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalReal(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, errors.Trace(err)
}

// builtinCoalesceDecimalSig is buitin function coalesce signature which return type Decimal
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceDecimalSig) evalDecimal(row types.Row) (res *types.MyDecimal, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDecimal(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, errors.Trace(err)
}

// builtinCoalesceStringSig is buitin function coalesce signature which return type string
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceStringSig) evalString(row types.Row) (res string, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalString(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, errors.Trace(err)
}

// builtinCoalesceTimeSig is buitin function coalesce signature which return type time
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceTimeSig) evalTime(row types.Row) (res types.Time, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalTime(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, errors.Trace(err)
}

// builtinCoalesceDurationSig is buitin function coalesce signature which return type duration
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceDurationSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDuration(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, errors.Trace(err)
}

// temporalWithDateAsNumEvalType makes DATE, DATETIME, TIMESTAMP pretend to be numbers rather than strings.
func temporalWithDateAsNumEvalType(argTp *types.FieldType) (argEvalType types.EvalType, isStr bool, isTemporalWithDate bool) {
	argEvalType = argTp.EvalType()
	isStr, isTemporalWithDate = argEvalType.IsStringKind(), types.IsTemporalWithDate(argTp.Tp)
	if !isTemporalWithDate {
		return
	}
	if argTp.Decimal > 0 {
		argEvalType = types.ETDecimal
	} else {
		argEvalType = types.ETInt
	}
	return
}

// getCmpTp4MinMax gets compare type for GREATEST and LEAST.
func getCmpTp4MinMax(args []Expression) (argTp types.EvalType) {
	datetimeFound, isAllStr := false, true
	cmpEvalType, isStr, isTemporalWithDate := temporalWithDateAsNumEvalType(args[0].GetType())
	if !isStr {
		isAllStr = false
	}
	if isTemporalWithDate {
		datetimeFound = true
	}
	lft := args[0].GetType()
	for i := range args {
		rft := args[i].GetType()
		var tp types.EvalType
		tp, isStr, isTemporalWithDate = temporalWithDateAsNumEvalType(rft)
		if isTemporalWithDate {
			datetimeFound = true
		}
		if !isStr {
			isAllStr = false
		}
		cmpEvalType = getBaseCmpType(cmpEvalType, tp, lft, rft)
		lft = rft
	}
	argTp = cmpEvalType
	if cmpEvalType.IsStringKind() {
		argTp = types.ETString
	}
	if isAllStr && datetimeFound {
		argTp = types.ETDatetime
	}
	return argTp
}

type greatestFunctionClass struct {
	baseFunctionClass
}

func (c *greatestFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tp, cmpAsDatetime := getCmpTp4MinMax(args), false
	if tp == types.ETDatetime {
		cmpAsDatetime = true
		tp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = tp
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	if cmpAsDatetime {
		tp = types.ETDatetime
	}
	switch tp {
	case types.ETInt:
		sig = &builtinGreatestIntSig{bf}
	case types.ETReal:
		sig = &builtinGreatestRealSig{bf}
	case types.ETDecimal:
		sig = &builtinGreatestDecimalSig{bf}
	case types.ETString:
		sig = &builtinGreatestStringSig{bf}
	case types.ETDatetime:
		sig = &builtinGreatestTimeSig{bf}
	}
	return sig, nil
}

type builtinGreatestIntSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinGreatestIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestIntSig) evalInt(row types.Row) (max int64, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, errors.Trace(err)
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, errors.Trace(err)
		}
		if v > max {
			max = v
		}
	}
	return
}

type builtinGreatestRealSig struct {
	baseBuiltinFunc
}

// evalReal evals a builtinGreatestRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestRealSig) evalReal(row types.Row) (max float64, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, errors.Trace(err)
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, errors.Trace(err)
		}
		if v > max {
			max = v
		}
	}
	return
}

type builtinGreatestDecimalSig struct {
	baseBuiltinFunc
}

// evalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) evalDecimal(row types.Row) (max *types.MyDecimal, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, errors.Trace(err)
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, errors.Trace(err)
		}
		if v.Compare(max) > 0 {
			max = v
		}
	}
	return
}

type builtinGreatestStringSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinGreatestStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestStringSig) evalString(row types.Row) (max string, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, errors.Trace(err)
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, errors.Trace(err)
		}
		if types.CompareString(v, max) > 0 {
			max = v
		}
	}
	return
}

type builtinGreatestTimeSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinGreatestTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestTimeSig) evalString(row types.Row) (_ string, isNull bool, err error) {
	var (
		v string
		t types.Time
	)
	max := types.ZeroDatetime
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.args); i++ {
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return "", true, errors.Trace(err)
		}
		t, err = types.ParseDatetime(sc, v)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return v, true, errors.Trace(err)
			}
			continue
		}
		if t.Compare(max) > 0 {
			max = t
		}
	}
	return max.String(), false, nil
}

type leastFunctionClass struct {
	baseFunctionClass
}

func (c *leastFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tp, cmpAsDatetime := getCmpTp4MinMax(args), false
	if tp == types.ETDatetime {
		cmpAsDatetime = true
		tp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = tp
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	if cmpAsDatetime {
		tp = types.ETDatetime
	}
	switch tp {
	case types.ETInt:
		sig = &builtinLeastIntSig{bf}
	case types.ETReal:
		sig = &builtinLeastRealSig{bf}
	case types.ETDecimal:
		sig = &builtinLeastDecimalSig{bf}
	case types.ETString:
		sig = &builtinLeastStringSig{bf}
	case types.ETDatetime:
		sig = &builtinLeastTimeSig{bf}
	}
	return sig, nil
}

type builtinLeastIntSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinLeastIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastIntSig) evalInt(row types.Row) (min int64, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, errors.Trace(err)
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, errors.Trace(err)
		}
		if v < min {
			min = v
		}
	}
	return
}

type builtinLeastRealSig struct {
	baseBuiltinFunc
}

// evalReal evals a builtinLeastRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastRealSig) evalReal(row types.Row) (min float64, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, errors.Trace(err)
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, errors.Trace(err)
		}
		if v < min {
			min = v
		}
	}
	return
}

type builtinLeastDecimalSig struct {
	baseBuiltinFunc
}

// evalDecimal evals a builtinLeastDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastDecimalSig) evalDecimal(row types.Row) (min *types.MyDecimal, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, errors.Trace(err)
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, errors.Trace(err)
		}
		if v.Compare(min) < 0 {
			min = v
		}
	}
	return
}

type builtinLeastStringSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinLeastStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastStringSig) evalString(row types.Row) (min string, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, errors.Trace(err)
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, errors.Trace(err)
		}
		if types.CompareString(v, min) < 0 {
			min = v
		}
	}
	return
}

type builtinLeastTimeSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinLeastTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastTimeSig) evalString(row types.Row) (res string, isNull bool, err error) {
	var (
		v string
		t types.Time
	)
	min := types.Time{
		Time: types.MaxDatetime,
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}
	findInvalidTime := false
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.args); i++ {
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return "", true, errors.Trace(err)
		}
		t, err = types.ParseDatetime(sc, v)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return v, true, errors.Trace(err)
			} else if !findInvalidTime {
				res = v
				findInvalidTime = true
			}
		}
		if t.Compare(min) < 0 {
			min = t
		}
	}
	if !findInvalidTime {
		res = min.String()
	}
	return res, false, nil
}

type intervalFunctionClass struct {
	baseFunctionClass
}

func (c *intervalFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	allInt := true
	for i := range args {
		if args[i].GetType().EvalType() != types.ETInt {
			allInt = false
		}
	}

	argTps, argTp := make([]types.EvalType, 0, len(args)), types.ETReal
	if allInt {
		argTp = types.ETInt
	}
	for range args {
		argTps = append(argTps, argTp)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	var sig builtinFunc
	if allInt {
		sig = &builtinIntervalIntSig{bf}
	} else {
		sig = &builtinIntervalRealSig{bf}
	}
	return sig, nil
}

type builtinIntervalIntSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinIntervalIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalIntSig) evalInt(row types.Row) (int64, bool, error) {
	args0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull {
		return -1, false, nil
	}
	idx, err := b.binSearch(args0, mysql.HasUnsignedFlag(b.args[0].GetType().Flag), b.args[1:], row)
	return int64(idx), err != nil, errors.Trace(err)
}

// binSearch is a binary search method.
// All arguments are treated as integers.
// It is required that arg[0] < args[1] < args[2] < ... < args[n] for this function to work correctly.
// This is because a binary search is used (very fast).
func (b *builtinIntervalIntSig) binSearch(target int64, isUint1 bool, args []Expression, row types.Row) (_ int, err error) {
	i, j, cmp := 0, len(args), false
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalInt(b.ctx, row)
		if err1 != nil {
			err = err1
			break
		}
		if isNull {
			v = target
		}
		isUint2 := mysql.HasUnsignedFlag(args[mid].GetType().Flag)
		switch {
		case !isUint1 && !isUint2:
			cmp = target < v
		case isUint1 && isUint2:
			cmp = uint64(target) < uint64(v)
		case !isUint1 && isUint2:
			cmp = target < 0 || uint64(target) < uint64(v)
		case isUint1 && !isUint2:
			cmp = v > 0 && uint64(target) < uint64(v)
		}
		if !cmp {
			i = mid + 1
		} else {
			j = mid
		}
	}
	return i, errors.Trace(err)
}

type builtinIntervalRealSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinIntervalRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalRealSig) evalInt(row types.Row) (int64, bool, error) {
	args0, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull {
		return -1, false, nil
	}
	idx, err := b.binSearch(args0, b.args[1:], row)
	return int64(idx), err != nil, errors.Trace(err)
}

func (b *builtinIntervalRealSig) binSearch(target float64, args []Expression, row types.Row) (_ int, err error) {
	i, j := 0, len(args)
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalReal(b.ctx, row)
		if err != nil {
			err = err1
			break
		}
		if isNull {
			i = mid + 1
		} else if cmp := target < v; !cmp {
			i = mid + 1
		} else {
			j = mid
		}
	}
	return i, errors.Trace(err)
}

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	if lft.Tp == mysql.TypeUnspecified || rft.Tp == mysql.TypeUnspecified {
		if lft.Tp == rft.Tp {
			return types.ETString
		}
		if lft.Tp == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() && rhs.IsStringKind() {
		return types.ETString
	} else if (lhs == types.ETInt || lft.Hybrid()) && (rhs == types.ETInt || rft.Hybrid()) {
		return types.ETInt
	} else if ((lhs == types.ETInt || lft.Hybrid()) || lhs == types.ETDecimal) &&
		((rhs == types.ETInt || rft.Hybrid()) || rhs == types.ETDecimal) {
		return types.ETDecimal
	}
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(lhs, rhs Expression) types.EvalType {
	lhsFieldType, rhsFieldType := lhs.GetType(), rhs.GetType()
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	if (lhsEvalType.IsStringKind() && rhsFieldType.Tp == mysql.TypeJSON) ||
		(lhsFieldType.Tp == mysql.TypeJSON && rhsEvalType.IsStringKind()) {
		cmpType = types.ETJson
	} else if cmpType == types.ETString && (types.IsTypeTime(lhsFieldType.Tp) || types.IsTypeTime(rhsFieldType.Tp)) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		if lhsFieldType.Tp == rhsFieldType.Tp {
			cmpType = lhsFieldType.EvalType()
		} else {
			cmpType = types.ETDatetime
		}
	} else if lhsFieldType.Tp == mysql.TypeDuration && rhsFieldType.Tp == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		cmpType = types.ETDuration
	} else if cmpType == types.ETReal || cmpType == types.ETString {
		_, isLHSConst := lhs.(*Constant)
		_, isRHSConst := rhs.(*Constant)
		if (lhsEvalType == types.ETDecimal && !isLHSConst && rhsEvalType.IsStringKind() && isRHSConst) ||
			(rhsEvalType == types.ETDecimal && !isRHSConst && lhsEvalType.IsStringKind() && isLHSConst) {
			/*
				<non-const decimal expression> <cmp> <const string expression>
				or
				<const string expression> <cmp> <non-const decimal expression>

				Do comparison as decimal rather than float, in order not to lose precision.
			)*/
			cmpType = types.ETDecimal
		} else if isTemporalColumn(lhs) && isRHSConst ||
			isTemporalColumn(rhs) && isLHSConst {
			/*
				<temporal column> <cmp> <non-temporal constant>
				or
				<non-temporal constant> <cmp> <temporal column>

				Convert the constant to temporal type.
			*/
			col, isLHSColumn := lhs.(*Column)
			if !isLHSColumn {
				col = rhs.(*Column)
			}
			if col.GetType().Tp == mysql.TypeDuration {
				cmpType = types.ETDuration
			} else {
				cmpType = types.ETDatetime
			}
		}
	}
	return cmpType
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(expr Expression) bool {
	ft := expr.GetType()
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	if !types.IsTypeTime(ft.Tp) && ft.Tp != mysql.TypeDuration {
		return false
	}
	return true
}

// tryToConvertConstantInt tries to convert a constant with other type to a int constant.
func tryToConvertConstantInt(ctx context.Context, con *Constant) *Constant {
	if con.GetType().EvalType() == types.ETInt {
		return con
	}
	dt, err := con.Eval(nil)
	if err != nil {
		return con
	}
	sc := ctx.GetSessionVars().StmtCtx
	i64, err := dt.ToInt64(sc)
	if err != nil {
		return con
	}
	return &Constant{
		Value:        types.NewIntDatum(i64),
		RetType:      types.NewFieldType(mysql.TypeLonglong),
		DeferredExpr: con.DeferredExpr,
	}
}

// RefineConstantArg changes the constant argument to it's ceiling or flooring result by the given op.
func RefineConstantArg(ctx context.Context, con *Constant, op opcode.Op) *Constant {
	dt, err := con.Eval(nil)
	if err != nil {
		return con
	}
	sc := ctx.GetSessionVars().StmtCtx
	i64, err := dt.ToInt64(sc)
	if err != nil {
		return con
	}
	datumInt := types.NewIntDatum(i64)
	c, err := datumInt.CompareDatum(sc, &con.Value)
	if err != nil {
		return con
	}
	if c == 0 {
		return &Constant{
			Value:        datumInt,
			RetType:      types.NewFieldType(mysql.TypeLonglong),
			DeferredExpr: con.DeferredExpr,
		}
	}
	switch op {
	case opcode.LT, opcode.GE:
		resultExpr := NewFunctionInternal(ctx, ast.Ceil, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, resultCon)
		}
	case opcode.LE, opcode.GT:
		resultExpr := NewFunctionInternal(ctx, ast.Floor, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, resultCon)
		}
	}
	// TODO: argInt = 1.1 should be false forever.
	return con
}

// refineArgs rewrite the arguments if one of them is int expression and another one is non-int constant.
// Like a < 1.1 will be changed to a < 2.
func (c *compareFunctionClass) refineArgs(ctx context.Context, args []Expression) []Expression {
	arg0IsInt := args[0].GetType().EvalType() == types.ETInt
	arg1IsInt := args[1].GetType().EvalType() == types.ETInt
	arg0, arg0IsCon := args[0].(*Constant)
	arg1, arg1IsCon := args[1].(*Constant)
	// int non-constant [cmp] non-int constant
	if arg0IsInt && !arg0IsCon && !arg1IsInt && arg1IsCon {
		arg1 = RefineConstantArg(ctx, arg1, c.op)
		return []Expression{args[0], arg1}
	}
	// non-int constant [cmp] int non-constant
	if arg1IsInt && !arg1IsCon && !arg0IsInt && arg0IsCon {
		arg0 = RefineConstantArg(ctx, arg0, symmetricOp[c.op])
		return []Expression{arg0, args[1]}
	}
	return args
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(ctx context.Context, rawArgs []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(rawArgs); err != nil {
		return nil, errors.Trace(err)
	}
	args := c.refineArgs(ctx, rawArgs)
	cmpType := GetAccurateCmpType(args[0], args[1])
	sig, err = c.generateCmpSigs(ctx, args, cmpType)
	return sig, errors.Trace(err)
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx context.Context, args []Expression, tp types.EvalType) (sig builtinFunc, err error) {
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, tp, tp)
	if tp == types.ETJson {
		// In compare, if we cast string to JSON, we shouldn't parse it.
		for i := range args {
			args[i].GetType().Flag &= ^mysql.ParseToJSONFlag
		}
	}
	bf.tp.Flen = 1
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
	}
	return
}

type builtinLTIntSig struct {
	baseBuiltinFunc
}

func (s *builtinLTIntSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLT(compareInt(s.args, row, s.ctx))
}

type builtinLTRealSig struct {
	baseBuiltinFunc
}

func (s *builtinLTRealSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLT(compareReal(s.args, row, s.ctx))
}

type builtinLTDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinLTDecimalSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLT(compareDecimal(s.args, row, s.ctx))
}

type builtinLTStringSig struct {
	baseBuiltinFunc
}

func (s *builtinLTStringSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLT(compareString(s.args, row, s.ctx))
}

type builtinLTDurationSig struct {
	baseBuiltinFunc
}

func (s *builtinLTDurationSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLT(compareDuration(s.args, row, s.ctx))
}

type builtinLTTimeSig struct {
	baseBuiltinFunc
}

func (s *builtinLTTimeSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLT(compareTime(s.args, row, s.ctx))
}

type builtinLTJSONSig struct {
	baseBuiltinFunc
}

func (s *builtinLTJSONSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLT(compareJSON(s.args, row, s.ctx))
}

type builtinLEIntSig struct {
	baseBuiltinFunc
}

func (s *builtinLEIntSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLE(compareInt(s.args, row, s.ctx))
}

type builtinLERealSig struct {
	baseBuiltinFunc
}

func (s *builtinLERealSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLE(compareReal(s.args, row, s.ctx))
}

type builtinLEDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinLEDecimalSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLE(compareDecimal(s.args, row, s.ctx))
}

type builtinLEStringSig struct {
	baseBuiltinFunc
}

func (s *builtinLEStringSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLE(compareString(s.args, row, s.ctx))
}

type builtinLEDurationSig struct {
	baseBuiltinFunc
}

func (s *builtinLEDurationSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLE(compareDuration(s.args, row, s.ctx))
}

type builtinLETimeSig struct {
	baseBuiltinFunc
}

func (s *builtinLETimeSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLE(compareTime(s.args, row, s.ctx))
}

type builtinLEJSONSig struct {
	baseBuiltinFunc
}

func (s *builtinLEJSONSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfLE(compareJSON(s.args, row, s.ctx))
}

type builtinGTIntSig struct {
	baseBuiltinFunc
}

func (s *builtinGTIntSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGT(compareInt(s.args, row, s.ctx))
}

type builtinGTRealSig struct {
	baseBuiltinFunc
}

func (s *builtinGTRealSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGT(compareReal(s.args, row, s.ctx))
}

type builtinGTDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinGTDecimalSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGT(compareDecimal(s.args, row, s.ctx))
}

type builtinGTStringSig struct {
	baseBuiltinFunc
}

func (s *builtinGTStringSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGT(compareString(s.args, row, s.ctx))
}

type builtinGTDurationSig struct {
	baseBuiltinFunc
}

func (s *builtinGTDurationSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGT(compareDuration(s.args, row, s.ctx))
}

type builtinGTTimeSig struct {
	baseBuiltinFunc
}

func (s *builtinGTTimeSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGT(compareTime(s.args, row, s.ctx))
}

type builtinGTJSONSig struct {
	baseBuiltinFunc
}

func (s *builtinGTJSONSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGT(compareJSON(s.args, row, s.ctx))
}

type builtinGEIntSig struct {
	baseBuiltinFunc
}

func (s *builtinGEIntSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGE(compareInt(s.args, row, s.ctx))
}

type builtinGERealSig struct {
	baseBuiltinFunc
}

func (s *builtinGERealSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGE(compareReal(s.args, row, s.ctx))
}

type builtinGEDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinGEDecimalSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGE(compareDecimal(s.args, row, s.ctx))
}

type builtinGEStringSig struct {
	baseBuiltinFunc
}

func (s *builtinGEStringSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGE(compareString(s.args, row, s.ctx))
}

type builtinGEDurationSig struct {
	baseBuiltinFunc
}

func (s *builtinGEDurationSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGE(compareDuration(s.args, row, s.ctx))
}

type builtinGETimeSig struct {
	baseBuiltinFunc
}

func (s *builtinGETimeSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGE(compareTime(s.args, row, s.ctx))
}

type builtinGEJSONSig struct {
	baseBuiltinFunc
}

func (s *builtinGEJSONSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfGE(compareJSON(s.args, row, s.ctx))
}

type builtinEQIntSig struct {
	baseBuiltinFunc
}

func (s *builtinEQIntSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfEQ(compareInt(s.args, row, s.ctx))
}

type builtinEQRealSig struct {
	baseBuiltinFunc
}

func (s *builtinEQRealSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfEQ(compareReal(s.args, row, s.ctx))
}

type builtinEQDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinEQDecimalSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfEQ(compareDecimal(s.args, row, s.ctx))
}

type builtinEQStringSig struct {
	baseBuiltinFunc
}

func (s *builtinEQStringSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfEQ(compareString(s.args, row, s.ctx))
}

type builtinEQDurationSig struct {
	baseBuiltinFunc
}

func (s *builtinEQDurationSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfEQ(compareDuration(s.args, row, s.ctx))
}

type builtinEQTimeSig struct {
	baseBuiltinFunc
}

func (s *builtinEQTimeSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfEQ(compareTime(s.args, row, s.ctx))
}

type builtinEQJSONSig struct {
	baseBuiltinFunc
}

func (s *builtinEQJSONSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfEQ(compareJSON(s.args, row, s.ctx))
}

type builtinNEIntSig struct {
	baseBuiltinFunc
}

func (s *builtinNEIntSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfNE(compareInt(s.args, row, s.ctx))
}

type builtinNERealSig struct {
	baseBuiltinFunc
}

func (s *builtinNERealSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfNE(compareReal(s.args, row, s.ctx))
}

type builtinNEDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinNEDecimalSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfNE(compareDecimal(s.args, row, s.ctx))
}

type builtinNEStringSig struct {
	baseBuiltinFunc
}

func (s *builtinNEStringSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfNE(compareString(s.args, row, s.ctx))
}

type builtinNEDurationSig struct {
	baseBuiltinFunc
}

func (s *builtinNEDurationSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfNE(compareDuration(s.args, row, s.ctx))
}

type builtinNETimeSig struct {
	baseBuiltinFunc
}

func (s *builtinNETimeSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfNE(compareTime(s.args, row, s.ctx))
}

type builtinNEJSONSig struct {
	baseBuiltinFunc
}

func (s *builtinNEJSONSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	return resOfNE(compareJSON(s.args, row, s.ctx))
}

type builtinNullEQIntSig struct {
	baseBuiltinFunc
}

func (s *builtinNullEQIntSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := s.args[0].EvalInt(s.ctx, row)
	if err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalInt(s.ctx, row)
	if err != nil {
		return 0, isNull1, errors.Trace(err)
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(s.args[0].GetType().Flag), mysql.HasUnsignedFlag(s.args[1].GetType().Flag)
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case isUnsigned0 && isUnsigned1 && types.CompareUint64(uint64(arg0), uint64(arg1)) == 0:
		res = 1
	case !isUnsigned0 && !isUnsigned1 && types.CompareInt64(arg0, arg1) == 0:
		res = 1
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || arg0 > math.MaxInt64 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || arg1 > math.MaxInt64 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	}
	return res, false, nil
}

type builtinNullEQRealSig struct {
	baseBuiltinFunc
}

func (s *builtinNullEQRealSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := s.args[0].EvalReal(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalReal(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareFloat64(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinNullEQDecimalSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := s.args[0].EvalDecimal(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalDecimal(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQStringSig struct {
	baseBuiltinFunc
}

func (s *builtinNullEQStringSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := s.args[0].EvalString(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalString(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareString(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDurationSig struct {
	baseBuiltinFunc
}

func (s *builtinNullEQDurationSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := s.args[0].EvalDuration(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalDuration(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQTimeSig struct {
	baseBuiltinFunc
}

func (s *builtinNullEQTimeSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := s.args[0].EvalTime(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalTime(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQJSONSig struct {
	baseBuiltinFunc
}

func (s *builtinNullEQJSONSig) evalInt(row types.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := s.args[0].EvalJSON(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	arg1, isNull1, err := s.args[1].EvalJSON(s.ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	default:
		cmpRes := json.CompareBinary(arg0, arg1)
		if cmpRes == 0 {
			res = 1
		}
	}
	return res, false, nil
}

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val < 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val <= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val > 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val >= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val == 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val != 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func compareInt(args []Expression, row types.Row, ctx context.Context) (val int64, isNull bool, err error) {
	arg0, isNull0, err := args[0].EvalInt(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalInt(ctx, row)
	if isNull1 || err != nil {
		return 0, isNull1, errors.Trace(err)
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(args[0].GetType().Flag), mysql.HasUnsignedFlag(args[1].GetType().Flag)
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		res = types.CompareUint64(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || arg0 > math.MaxInt64 {
			res = 1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || arg1 > math.MaxInt64 {
			res = -1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && !isUnsigned1:
		res = types.CompareInt64(arg0, arg1)
	}
	return int64(res), false, nil
}

func compareString(args []Expression, row types.Row, ctx context.Context) (val int64, isNull bool, err error) {
	arg0, isNull0, err := args[0].EvalString(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalString(ctx, row)
	if isNull1 || err != nil {
		return 0, isNull1, errors.Trace(err)
	}
	return int64(types.CompareString(arg0, arg1)), false, nil
}

func compareReal(args []Expression, row types.Row, ctx context.Context) (val int64, isNull bool, err error) {
	arg0, isNull0, err := args[0].EvalReal(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalReal(ctx, row)
	if isNull1 || err != nil {
		return 0, isNull1, errors.Trace(err)
	}
	return int64(types.CompareFloat64(arg0, arg1)), false, nil
}

func compareDecimal(args []Expression, row types.Row, ctx context.Context) (val int64, isNull bool, err error) {
	arg0, isNull0, err := args[0].EvalDecimal(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalDecimal(ctx, row)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull1 || err != nil {
		return 0, isNull1, errors.Trace(err)
	}
	return int64(arg0.Compare(arg1)), false, nil
}

func compareTime(args []Expression, row types.Row, ctx context.Context) (int64, bool, error) {
	arg0, isNull0, err := args[0].EvalTime(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalTime(ctx, row)
	if isNull1 || err != nil {
		return 0, isNull1, errors.Trace(err)
	}
	return int64(arg0.Compare(arg1)), false, nil
}

func compareDuration(args []Expression, row types.Row, ctx context.Context) (int64, bool, error) {
	arg0, isNull0, err := args[0].EvalDuration(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalDuration(ctx, row)
	if isNull1 || err != nil {
		return 0, isNull1, errors.Trace(err)
	}
	return int64(arg0.Compare(arg1)), false, nil
}

func compareJSON(args []Expression, row types.Row, ctx context.Context) (int64, bool, error) {
	arg0, isNull0, err := args[0].EvalJSON(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	arg1, isNull1, err := args[1].EvalJSON(ctx, row)
	if isNull1 || err != nil {
		return 0, isNull1, errors.Trace(err)
	}
	return int64(json.CompareBinary(arg0, arg1)), false, nil
}
