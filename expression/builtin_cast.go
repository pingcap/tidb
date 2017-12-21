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

// We implement 6 CastAsXXFunctionClass for `cast` built-in functions.
// XX means the return type of the `cast` built-in functions.
// XX contains the following 6 types:
// Int, Decimal, Real, String, Time, Duration.

// We implement 6 CastYYAsXXSig built-in function signatures for every CastAsXXFunctionClass.
// builtinCastXXAsYYSig takes a argument of type XX and returns a value of type YY.

package expression

import (
	"math"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &castAsIntFunctionClass{}
	_ functionClass = &castAsRealFunctionClass{}
	_ functionClass = &castAsStringFunctionClass{}
	_ functionClass = &castAsDecimalFunctionClass{}
	_ functionClass = &castAsTimeFunctionClass{}
	_ functionClass = &castAsDurationFunctionClass{}
	_ functionClass = &castAsJSONFunctionClass{}
)

var (
	_ builtinFunc = &builtinCastIntAsIntSig{}
	_ builtinFunc = &builtinCastIntAsRealSig{}
	_ builtinFunc = &builtinCastIntAsStringSig{}
	_ builtinFunc = &builtinCastIntAsDecimalSig{}
	_ builtinFunc = &builtinCastIntAsTimeSig{}
	_ builtinFunc = &builtinCastIntAsDurationSig{}
	_ builtinFunc = &builtinCastIntAsJSONSig{}

	_ builtinFunc = &builtinCastRealAsIntSig{}
	_ builtinFunc = &builtinCastRealAsRealSig{}
	_ builtinFunc = &builtinCastRealAsStringSig{}
	_ builtinFunc = &builtinCastRealAsDecimalSig{}
	_ builtinFunc = &builtinCastRealAsTimeSig{}
	_ builtinFunc = &builtinCastRealAsDurationSig{}
	_ builtinFunc = &builtinCastRealAsJSONSig{}

	_ builtinFunc = &builtinCastDecimalAsIntSig{}
	_ builtinFunc = &builtinCastDecimalAsRealSig{}
	_ builtinFunc = &builtinCastDecimalAsStringSig{}
	_ builtinFunc = &builtinCastDecimalAsDecimalSig{}
	_ builtinFunc = &builtinCastDecimalAsTimeSig{}
	_ builtinFunc = &builtinCastDecimalAsDurationSig{}
	_ builtinFunc = &builtinCastDecimalAsJSONSig{}

	_ builtinFunc = &builtinCastStringAsIntSig{}
	_ builtinFunc = &builtinCastStringAsRealSig{}
	_ builtinFunc = &builtinCastStringAsStringSig{}
	_ builtinFunc = &builtinCastStringAsDecimalSig{}
	_ builtinFunc = &builtinCastStringAsTimeSig{}
	_ builtinFunc = &builtinCastStringAsDurationSig{}
	_ builtinFunc = &builtinCastStringAsJSONSig{}

	_ builtinFunc = &builtinCastTimeAsIntSig{}
	_ builtinFunc = &builtinCastTimeAsRealSig{}
	_ builtinFunc = &builtinCastTimeAsStringSig{}
	_ builtinFunc = &builtinCastTimeAsDecimalSig{}
	_ builtinFunc = &builtinCastTimeAsTimeSig{}
	_ builtinFunc = &builtinCastTimeAsDurationSig{}
	_ builtinFunc = &builtinCastTimeAsJSONSig{}

	_ builtinFunc = &builtinCastDurationAsIntSig{}
	_ builtinFunc = &builtinCastDurationAsRealSig{}
	_ builtinFunc = &builtinCastDurationAsStringSig{}
	_ builtinFunc = &builtinCastDurationAsDecimalSig{}
	_ builtinFunc = &builtinCastDurationAsTimeSig{}
	_ builtinFunc = &builtinCastDurationAsDurationSig{}
	_ builtinFunc = &builtinCastDurationAsJSONSig{}

	_ builtinFunc = &builtinCastJSONAsIntSig{}
	_ builtinFunc = &builtinCastJSONAsRealSig{}
	_ builtinFunc = &builtinCastJSONAsStringSig{}
	_ builtinFunc = &builtinCastJSONAsDecimalSig{}
	_ builtinFunc = &builtinCastJSONAsTimeSig{}
	_ builtinFunc = &builtinCastJSONAsDurationSig{}
	_ builtinFunc = &builtinCastJSONAsJSONSig{}
)

type castAsIntFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsIntFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastIntAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsInt)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsInt)
	case types.ETReal:
		sig = &builtinCastRealAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsInt)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsInt)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsInt)
	case types.ETDuration:
		sig = &builtinCastDurationAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsInt)
	case types.ETJson:
		sig = &builtinCastJSONAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsInt)
	case types.ETString:
		sig = &builtinCastStringAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsInt)
	default:
		panic("unsupported types.EvalType in castAsIntFunctionClass")
	}
	return sig, nil
}

type castAsRealFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsRealFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastRealAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsReal)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsReal)
	case types.ETReal:
		sig = &builtinCastRealAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsReal)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsReal)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsReal)
	case types.ETDuration:
		sig = &builtinCastDurationAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsReal)
	case types.ETJson:
		sig = &builtinCastJSONAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsReal)
	case types.ETString:
		sig = &builtinCastStringAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsReal)
	default:
		panic("unsupported types.EvalType in castAsRealFunctionClass")
	}
	return sig, nil
}

type castAsDecimalFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsDecimalFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsDecimal)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsDecimal)
	case types.ETReal:
		sig = &builtinCastRealAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsDecimal)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsDecimal)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsDecimal)
	case types.ETDuration:
		sig = &builtinCastDurationAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsDecimal)
	case types.ETJson:
		sig = &builtinCastJSONAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsDecimal)
	case types.ETString:
		sig = &builtinCastStringAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsDecimal)
	default:
		panic("unsupported types.EvalType in castAsDecimalFunctionClass")
	}
	return sig, nil
}

type castAsStringFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsStringFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastStringAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsString)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsString)
	case types.ETReal:
		sig = &builtinCastRealAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsString)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsString)
	case types.ETDuration:
		sig = &builtinCastDurationAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsString)
	case types.ETJson:
		sig = &builtinCastJSONAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsString)
	case types.ETString:
		sig = &builtinCastStringAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsString)
	default:
		panic("unsupported types.EvalType in castAsStringFunctionClass")
	}
	return sig, nil
}

type castAsTimeFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsTimeFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastTimeAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsTime)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsTime)
	case types.ETReal:
		sig = &builtinCastRealAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsTime)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsTime)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsTime)
	case types.ETDuration:
		sig = &builtinCastDurationAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsTime)
	case types.ETJson:
		sig = &builtinCastJSONAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsTime)
	case types.ETString:
		sig = &builtinCastStringAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsTime)
	default:
		panic("unsupported types.EvalType in castAsTimeFunctionClass")
	}
	return sig, nil
}

type castAsDurationFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsDurationFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastDurationAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsDuration)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsDuration)
	case types.ETReal:
		sig = &builtinCastRealAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsDuration)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsDuration)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsDuration)
	case types.ETDuration:
		sig = &builtinCastDurationAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsDuration)
	case types.ETJson:
		sig = &builtinCastJSONAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsDuration)
	case types.ETString:
		sig = &builtinCastStringAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsDuration)
	default:
		panic("unsupported types.EvalType in castAsDurationFunctionClass")
	}
	return sig, nil
}

type castAsJSONFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsJSONFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	if IsHybridType(args[0]) {
		sig = &builtinCastJSONAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsJson)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
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
		sig.getRetTp().Flag |= mysql.ParseToJSONFlag
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsJson)
	default:
		panic("unsupported types.EvalType in castAsJSONFunctionClass")
	}
	return sig, nil
}

type builtinCastIntAsIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	return b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastIntAsRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsRealSig) evalReal(row types.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if !mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = float64(val)
	} else {
		var uVal uint64
		uVal, err = types.ConvertIntToUint(val, types.UnsignedUpperBound[mysql.TypeLonglong], mysql.TypeLonglong)
		res = float64(uVal)
	}
	return res, false, errors.Trace(err)
}

type builtinCastIntAsDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsDecimalSig) evalDecimal(row types.Row) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if !mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = types.NewDecFromInt(val)
	} else {
		var uVal uint64
		uVal, err = types.ConvertIntToUint(val, types.UnsignedUpperBound[mysql.TypeLonglong], mysql.TypeLonglong)
		if err != nil {
			return res, false, errors.Trace(err)
		}
		res = types.NewDecFromUint(uVal)
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, isNull, errors.Trace(err)
}

type builtinCastIntAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsStringSig) evalString(row types.Row) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if !mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = strconv.FormatInt(val, 10)
	} else {
		var uVal uint64
		uVal, err = types.ConvertIntToUint(val, types.UnsignedUpperBound[mysql.TypeLonglong], mysql.TypeLonglong)
		if err != nil {
			return res, false, errors.Trace(err)
		}
		res = strconv.FormatUint(uVal, 10)
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastIntAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsTimeSig) evalTime(row types.Row) (res types.Time, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseTimeFromNum(sc, val, b.tp.Tp, b.tp.Decimal)
	if err != nil {
		return res, true, errors.Trace(err)
	}
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	res.TimeZone = sc.TimeZone
	return res, false, nil
}

type builtinCastIntAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsDurationSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	t, err := types.NumberToDuration(val, b.tp.Decimal)
	if err != nil {
		if types.ErrOverflow.Equal(err) {
			err = sc.HandleOverflow(err, err)
		}
		return res, true, errors.Trace(err)
	}

	res, err = t.ConvertToDuration()
	return res, false, errors.Trace(err)
}

type builtinCastIntAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsJSONSig) evalJSON(row types.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if mysql.HasIsBooleanFlag(b.args[0].GetType().Flag) {
		res = json.CreateBinary(val != 0)
	} else if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = json.CreateBinary(uint64(val))
	} else {
		res = json.CreateBinary(val)
	}
	return res, false, nil
}

type builtinCastRealAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsJSONSig) evalJSON(row types.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	return json.CreateBinary(val), isNull, errors.Trace(err)
}

type builtinCastDecimalAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsJSONSig) evalJSON(row types.Row) (json.BinaryJSON, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return json.BinaryJSON{}, true, errors.Trace(err)
	}
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	f64, err := val.ToFloat64()
	if err != nil {
		return json.BinaryJSON{}, true, errors.Trace(err)
	}
	return json.CreateBinary(f64), isNull, errors.Trace(err)
}

type builtinCastStringAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsJSONSig) evalJSON(row types.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if mysql.HasParseToJSONFlag(b.tp.Flag) {
		res, err = json.ParseBinaryFromString(val)
	} else {
		res = json.CreateBinary(val)
	}
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsJSONSig) evalJSON(row types.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	val.Fsp = types.MaxFsp
	return json.CreateBinary(val.String()), false, nil
}

type builtinCastTimeAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsJSONSig) evalJSON(row types.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	if val.Type == mysql.TypeDatetime || val.Type == mysql.TypeTimestamp {
		val.Fsp = types.MaxFsp
	}
	return json.CreateBinary(val.String()), false, nil
}

type builtinCastRealAsRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsRealSig) evalReal(row types.Row) (res float64, isNull bool, err error) {
	return b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
}

type builtinCastRealAsIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	if !mysql.HasUnsignedFlag(b.tp.Flag) {
		res, err = types.ConvertFloatToInt(sc, val, types.SignedLowerBound[mysql.TypeLonglong], types.SignedUpperBound[mysql.TypeLonglong], mysql.TypeDouble)
	} else {
		var uintVal uint64
		uintVal, err = types.ConvertFloatToUint(sc, val, types.UnsignedUpperBound[mysql.TypeLonglong], mysql.TypeDouble)
		res = int64(uintVal)
	}
	return res, isNull, errors.Trace(err)
}

type builtinCastRealAsDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsDecimalSig) evalDecimal(row types.Row) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res = new(types.MyDecimal)
	err = res.FromFloat64(val)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastRealAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsStringSig) evalString(row types.Row) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(val, 'f', -1, 64), b.tp, sc)
	return res, isNull, errors.Trace(err)
}

type builtinCastRealAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsTimeSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}
	res, err := types.ParseTime(sc, strconv.FormatFloat(val, 'f', -1, 64), b.tp.Tp, b.tp.Decimal)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	res.TimeZone = sc.TimeZone
	return res, false, nil
}

type builtinCastRealAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsDurationSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(strconv.FormatFloat(val, 'f', -1, 64), b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsDecimalSig) evalDecimal(row types.Row) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	res, isNull, err = b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	// Round is needed for both unsigned and signed.
	var to types.MyDecimal
	err = val.Round(&to, 0, types.ModeHalfEven)
	if err != nil {
		return 0, true, errors.Trace(err)
	}

	if mysql.HasUnsignedFlag(b.tp.Flag) {
		var uintRes uint64
		uintRes, err = to.ToUint()
		res = int64(uintRes)
	} else {
		res, err = to.ToInt()
	}

	if types.ErrOverflow.Equal(err) {
		warnErr := types.ErrTruncatedWrongVal.GenByArgs("DECIMAL", val)
		err = sc.HandleOverflow(err, warnErr)
	}

	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsStringSig) evalString(row types.Row) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(string(val.ToString()), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsRealSig) evalReal(row types.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToFloat64()
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsTimeSig) evalTime(row types.Row) (res types.Time, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseTime(sc, string(val.ToString()), b.tp.Tp, b.tp.Decimal)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	res.TimeZone = sc.TimeZone
	return res, false, errors.Trace(err)
}

type builtinCastDecimalAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsDurationSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ParseDuration(string(val.ToString()), b.tp.Decimal)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
		if res == types.ZeroDuration {
			return res, true, errors.Trace(err)
		}
	}
	return res, false, errors.Trace(err)
}

type builtinCastStringAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsStringSig) evalString(row types.Row) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	res, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsIntSig struct {
	baseBuiltinFunc
}

// handleOverflow handles the overflow caused by cast string as int,
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html.
// When an out-of-range value is assigned to an integer column, MySQL stores the value representing the corresponding endpoint of the column data type range. If it is in select statement, it will return the
// endpoint value with a warning.
func (b *builtinCastStringAsIntSig) handleOverflow(origRes int64, origStr string, origErr error, isNegative bool) (res int64, err error) {
	res, err = origRes, origErr
	if err == nil {
		return
	}

	sc := b.getCtx().GetSessionVars().StmtCtx
	if sc.InSelectStmt && types.ErrOverflow.Equal(origErr) {
		if isNegative {
			res = math.MinInt64
		} else {
			uval := uint64(math.MaxUint64)
			res = int64(uval)
		}
		warnErr := types.ErrTruncatedWrongVal.GenByArgs("INTEGER", origStr)
		err = sc.HandleOverflow(origErr, warnErr)
	}
	return
}

func (b *builtinCastStringAsIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	if IsHybridType(b.args[0]) {
		return b.args[0].EvalInt(row, sc)
	}
	val, isNull, err := b.args[0].EvalString(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	val = strings.TrimSpace(val)
	isNegative := false
	if len(val) > 1 && val[0] == '-' { // negative number
		isNegative = true
	}

	var ures uint64
	if isNegative {
		res, err = types.StrToInt(sc, val)
		if err == nil {
			// If overflow, don't append this warnings
			sc.AppendWarning(types.ErrCastNegIntAsUnsigned)
		}
	} else {
		ures, err = types.StrToUint(sc, val)
		res = int64(ures)

		if err == nil && !mysql.HasUnsignedFlag(b.tp.Flag) && ures > uint64(math.MaxInt64) {
			sc.AppendWarning(types.ErrCastAsSignedOverflow)
		}
	}

	res, err = b.handleOverflow(res, val, err, isNegative)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsRealSig) evalReal(row types.Row) (res float64, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	if IsHybridType(b.args[0]) {
		return b.args[0].EvalReal(row, sc)
	}
	val, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.StrToFloat(sc, val)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	res, err = types.ProduceFloatWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsDecimalSig) evalDecimal(row types.Row) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	if IsHybridType(b.args[0]) {
		return b.args[0].EvalDecimal(row, sc)
	}
	val, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res = new(types.MyDecimal)
	err = sc.HandleTruncate(res.FromString([]byte(val)))
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastStringAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsTimeSig) evalTime(row types.Row) (res types.Time, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseTime(sc, val, b.tp.Tp, b.tp.Decimal)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	res.TimeZone = sc.TimeZone
	return res, false, errors.Trace(err)
}

type builtinCastStringAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsDurationSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ParseDuration(val, b.tp.Decimal)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
		if res == types.ZeroDuration {
			return res, true, errors.Trace(err)
		}
	}
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsTimeSig) evalTime(row types.Row) (res types.Time, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	res, isNull, err = b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	if res, err = res.Convert(sc, b.tp.Tp); err != nil {
		return res, true, errors.Trace(err)
	}
	res, err = res.RoundFrac(b.tp.Decimal)
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
		res.Type = b.tp.Tp
	}
	res.TimeZone = sc.TimeZone
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	t, err := val.RoundFrac(types.DefaultFsp)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = t.ToNumber().ToInt()
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsRealSig) evalReal(row types.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsDecimalSig) evalDecimal(row types.Row) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsStringSig) evalString(row types.Row) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastTimeAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsDurationSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ConvertToDuration()
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = res.RoundFrac(b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsDurationSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = res.RoundFrac(b.tp.Decimal)
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	dur, err := val.RoundFrac(types.DefaultFsp)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = dur.ToNumber().ToInt()
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsRealSig) evalReal(row types.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsDecimalSig) evalDecimal(row types.Row) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsStringSig) evalString(row types.Row) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, sc)
	return res, false, errors.Trace(err)
}

type builtinCastDurationAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsTimeSig) evalTime(row types.Row) (res types.Time, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = val.ConvertToTime(b.tp.Tp)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = res.RoundFrac(b.tp.Decimal)
	res.TimeZone = sc.TimeZone
	return res, false, errors.Trace(err)
}

type builtinCastJSONAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsJSONSig) evalJSON(row types.Row) (res json.BinaryJSON, isNull bool, err error) {
	return b.args[0].EvalJSON(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinCastJSONAsIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ConvertJSONToInt(sc, val, mysql.HasUnsignedFlag(b.tp.Flag))
	return
}

type builtinCastJSONAsRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsRealSig) evalReal(row types.Row) (res float64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	res, err = types.ConvertJSONToFloat(sc, val)
	return
}

type builtinCastJSONAsDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsDecimalSig) evalDecimal(row types.Row) (res *types.MyDecimal, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	f64, err := types.ConvertJSONToFloat(sc, val)
	if err == nil {
		res = new(types.MyDecimal)
		err = res.FromFloat64(f64)
	}
	return res, false, errors.Trace(err)
}

type builtinCastJSONAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsStringSig) evalString(row types.Row) (res string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	return val.String(), false, nil
}

type builtinCastJSONAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsTimeSig) evalTime(row types.Row) (res types.Time, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	s, err := val.Unquote()
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ParseTime(sc, s, b.tp.Tp, b.tp.Decimal)
	if err != nil {
		return res, false, errors.Trace(err)
	}
	if b.tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	res.TimeZone = sc.TimeZone
	return
}

type builtinCastJSONAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsDurationSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	s, err := val.Unquote()
	if err != nil {
		return res, false, errors.Trace(err)
	}
	res, err = types.ParseDuration(s, b.tp.Decimal)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return
}

// BuildCastFunction builds a CAST ScalarFunction from the Expression.
func BuildCastFunction(ctx context.Context, expr Expression, tp *types.FieldType) (res Expression) {
	var fc functionClass
	switch tp.EvalType() {
	case types.ETInt:
		fc = &castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDecimal:
		fc = &castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETReal:
		fc = &castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDatetime, types.ETTimestamp:
		fc = &castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDuration:
		fc = &castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETJson:
		fc = &castAsJSONFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETString:
		fc = &castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	}
	f, err := fc.getFunction(ctx, []Expression{expr})
	terror.Log(errors.Trace(err))
	res = &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp,
		Function: f,
	}
	// We do not fold CAST if the eval type of this scalar function is ETJson
	// since we may reset the flag of the field type of CastAsJson later which would
	// affect the evaluation of it.
	if tp.EvalType() != types.ETJson {
		res = FoldConstant(res)
	}
	return res
}

// WrapWithCastAsInt wraps `expr` with `cast` if the return type
// of expr is not type int,
// otherwise, returns `expr` directly.
func WrapWithCastAsInt(ctx context.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETInt {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flen, tp.Decimal = expr.GetType().Flen, 0
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsReal wraps `expr` with `cast` if the return type
// of expr is not type real,
// otherwise, returns `expr` directly.
func WrapWithCastAsReal(ctx context.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETReal {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeDouble)
	tp.Flen, tp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDecimal wraps `expr` with `cast` if the return type
// of expr is not type decimal,
// otherwise, returns `expr` directly.
func WrapWithCastAsDecimal(ctx context.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETDecimal {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeNewDecimal)
	tp.Flen, tp.Decimal = expr.GetType().Flen, types.UnspecifiedLength
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsString wraps `expr` with `cast` if the return type
// of expr is not type string,
// otherwise, returns `expr` directly.
func WrapWithCastAsString(ctx context.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETString {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeVarString)
	tp.Charset, tp.Collate = charset.CharsetUTF8, charset.CollationUTF8
	tp.Flen, tp.Decimal = expr.GetType().Flen, types.UnspecifiedLength
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsTime wraps `expr` with `cast` if the return type
// of expr is not same as type of the specified `tp` ,
// otherwise, returns `expr` directly.
func WrapWithCastAsTime(ctx context.Context, expr Expression, tp *types.FieldType) Expression {
	exprTp := expr.GetType().Tp
	if tp.Tp == exprTp {
		return expr
	} else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.Tp == mysql.TypeDatetime {
		return expr
	}
	switch x := expr.GetType(); x.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeNewDate, mysql.TypeDate, mysql.TypeDuration:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = types.MaxFsp
	}
	switch tp.Tp {
	case mysql.TypeDate:
		tp.Flen = mysql.MaxDateWidth
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		tp.Flen = mysql.MaxDatetimeWidthNoFsp
		if tp.Decimal > 0 {
			tp.Flen = tp.Flen + 1 + tp.Decimal
		}
	}
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDuration wraps `expr` with `cast` if the return type
// of expr is not type duration,
// otherwise, returns `expr` directly.
func WrapWithCastAsDuration(ctx context.Context, expr Expression) Expression {
	if expr.GetType().Tp == mysql.TypeDuration {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeDuration)
	switch x := expr.GetType(); x.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeNewDate, mysql.TypeDate:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = types.MaxFsp
	}
	tp.Flen = mysql.MaxDurationWidthNoFsp
	if tp.Decimal > 0 {
		tp.Flen = tp.Flen + 1 + tp.Decimal
	}
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsJSON wraps `expr` with `cast` if the return type
// of expr is not type json,
// otherwise, returns `expr` directly.
func WrapWithCastAsJSON(ctx context.Context, expr Expression) Expression {
	tp := &types.FieldType{
		Tp:      mysql.TypeJSON,
		Flen:    12582912, // FIXME: Here the Flen is not trusted.
		Decimal: 0,
		Charset: charset.CharsetUTF8,
		Collate: charset.CollationUTF8,
		Flag:    mysql.BinaryFlag,
	}
	return BuildCastFunction(ctx, expr, tp)
}
