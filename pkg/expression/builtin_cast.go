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

// We implement 6 CastAsXXFunctionClass for `cast` built-in functions.
// XX means the return type of the `cast` built-in functions.
// XX contains the following 6 types:
// Int, decimal, Real, String, Time, Duration.

// We implement 6 CastYYAsXXSig built-in function signatures for every CastAsXXFunctionClass.
// builtinCastXXAsYYSig takes a argument of type XX and returns a value of type YY.

package expression

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
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

	_ builtinFunc = &builtinCastStringAsVectorFloat32Sig{}
	_ builtinFunc = &builtinCastVectorFloat32AsStringSig{}
	_ builtinFunc = &builtinCastVectorFloat32AsVectorFloat32Sig{}
	_ builtinFunc = &builtinCastUnsupportedAsVectorFloat32Sig{}
	_ builtinFunc = &builtinCastVectorFloat32AsUnsupportedSig{}
)

const (
	maxTinyBlobSize   = 255
	maxBlobSize       = 65535
	maxMediumBlobSize = 16777215
	maxLongBlobSize   = 4294967295
	// These two are magic numbers to be compatible with MySQL.
	// They are `MaxBlobSize * 4` and `MaxMediumBlobSize * 4`, but multiply by 4 (mblen) is not necessary here. However
	// a bigger number is always safer to avoid truncation, so they are kept as is.
	castBlobFlen       = maxBlobSize * 4
	castMediumBlobFlen = maxMediumBlobSize * 4
)

type castAsIntFunctionClass struct {
	baseFunctionClass

	tp      *types.FieldType
	inUnion bool
}

func (c *castAsIntFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	b, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(b, c.inUnion)
	if args[0].GetType(ctx.GetEvalCtx()).Hybrid() || IsBinaryLiteral(args[0]) {
		sig = &builtinCastIntAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsInt)
		return sig, nil
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
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
	case types.ETVectorFloat32:
		sig = &builtinCastVectorFloat32AsUnsupportedSig{bf.baseBuiltinFunc}
		// sig.setPbCode(tipb.ScalarFuncSig_CastVectorFloat32AsInt)
	default:
		return nil, errors.Errorf("cannot cast from %s to %s", argTp, "Int")
	}
	return sig, nil
}

type castAsRealFunctionClass struct {
	baseFunctionClass

	tp      *types.FieldType
	inUnion bool
}

func (c *castAsRealFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	b, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(b, c.inUnion)
	if IsBinaryLiteral(args[0]) {
		sig = &builtinCastRealAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsReal)
		return sig, nil
	}
	var argTp types.EvalType
	if args[0].GetType(ctx.GetEvalCtx()).Hybrid() {
		argTp = types.ETInt
	} else {
		argTp = args[0].GetType(ctx.GetEvalCtx()).EvalType()
	}
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsReal)
	case types.ETReal:
		sig = &builtinCastRealAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsReal)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsRealSig{bf}
		PropagateType(ctx.GetEvalCtx(), types.ETReal, sig.getArgs()...)
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
	case types.ETVectorFloat32:
		sig = &builtinCastVectorFloat32AsUnsupportedSig{bf.baseBuiltinFunc}
		// sig.setPbCode(tipb.ScalarFuncSig_CastVectorFloat32AsReal)
	default:
		return nil, errors.Errorf("cannot cast from %s to %s", argTp, "Real")
	}
	return sig, nil
}

type castAsDecimalFunctionClass struct {
	baseFunctionClass

	tp      *types.FieldType
	inUnion bool
}

func (c *castAsDecimalFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	b, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(b, c.inUnion)
	if IsBinaryLiteral(args[0]) {
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsDecimal)
		return sig, nil
	}
	var argTp types.EvalType
	if args[0].GetType(ctx.GetEvalCtx()).Hybrid() {
		argTp = types.ETInt
	} else {
		argTp = args[0].GetType(ctx.GetEvalCtx()).EvalType()
	}
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
	case types.ETVectorFloat32:
		sig = &builtinCastVectorFloat32AsUnsupportedSig{bf.baseBuiltinFunc}
		// sig.setPbCode(tipb.ScalarFuncSig_CastVectorFloat32AsDecimal)
	default:
		return nil, errors.Errorf("cannot cast from %s to %s", argTp, "Decimal")
	}
	return sig, nil
}

type castAsStringFunctionClass struct {
	baseFunctionClass

	tp                *types.FieldType
	isExplicitCharset bool
}

func (c *castAsStringFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinCastFunc4String(ctx, c.funcName, args, c.tp, c.isExplicitCharset)
	if err != nil {
		return nil, err
	}
	if ft := args[0].GetType(ctx.GetEvalCtx()); ft.Hybrid() {
		castBitAsUnBinary := ft.GetType() == mysql.TypeBit && c.tp.GetCharset() != charset.CharsetBin
		if !castBitAsUnBinary {
			sig = &builtinCastStringAsStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CastStringAsString)
			return sig, nil
		}
		// for type BIT, it maybe an invalid value for the specified charset, we need to convert it to binary first,
		// and then convert it to the specified charset with `HandleBinaryLiteral` in the following code.
		tp := types.NewFieldType(mysql.TypeString)
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
		tp.AddFlag(mysql.BinaryFlag)
		args[0] = BuildCastFunction(ctx, args[0], tp)
	}
	argFt := args[0].GetType(ctx.GetEvalCtx())
	adjustRetFtForCastString(bf.tp, argFt)

	switch argFt.EvalType() {
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
	case types.ETVectorFloat32:
		sig = &builtinCastVectorFloat32AsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastVectorFloat32AsString)
	case types.ETString:
		// When cast from binary to some other charsets, we should check if the binary is valid or not.
		// so we build a from_binary function to do this check.
		bf.args[0] = HandleBinaryLiteral(ctx, args[0], &ExprCollation{Charset: c.tp.GetCharset(), Collation: c.tp.GetCollate()}, c.funcName, true)
		sig = &builtinCastStringAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsString)
	default:
		return nil, errors.Errorf("cannot cast from %s to %s", argFt.EvalType(), "String")
	}
	return sig, nil
}

func adjustRetFtForCastString(retFt, argFt *types.FieldType) {
	originalFlen := retFt.GetFlen()

	// Only estimate the length for variable length string types, because different length for fixed
	// length string types will have different behaviors and may cause compatibility issues.
	if retFt.GetType() == mysql.TypeString {
		return
	}

	if argFt.GetType() == mysql.TypeNull {
		return
	}

	argTp := argFt.EvalType()
	switch argTp {
	case types.ETInt:
		if originalFlen == types.UnspecifiedLength {
			// check https://github.com/pingcap/tidb/issues/44786
			// set flen from integers may truncate integers, e.g. char(1) can not display -1[int(1)]
			switch argFt.GetType() {
			case mysql.TypeTiny:
				if mysql.HasUnsignedFlag(argFt.GetFlag()) {
					retFt.SetFlen(3)
				} else {
					retFt.SetFlen(4)
				}
			case mysql.TypeShort:
				if mysql.HasUnsignedFlag(argFt.GetFlag()) {
					retFt.SetFlen(5)
				} else {
					retFt.SetFlen(6)
				}
			case mysql.TypeInt24:
				if mysql.HasUnsignedFlag(argFt.GetFlag()) {
					retFt.SetFlen(8)
				} else {
					retFt.SetFlen(9)
				}
			case mysql.TypeLong:
				if mysql.HasUnsignedFlag(argFt.GetFlag()) {
					retFt.SetFlen(10)
				} else {
					retFt.SetFlen(11)
				}
			case mysql.TypeLonglong:
				// the length of BIGINT is always 20 without considering the unsigned flag, because the
				// bigint range from -9223372036854775808 to 9223372036854775807, and unsigned bigint range
				// from 0 to 18446744073709551615, they are all 20 characters long.
				retFt.SetFlen(20)
			case mysql.TypeYear:
				retFt.SetFlen(4)
			case mysql.TypeBit:
				retFt.SetFlen(argFt.GetFlen())
			case mysql.TypeEnum:
				intest.Assert(false, "cast Enum to String should not set mysql.EnumSetAsIntFlag")
				return
			case mysql.TypeSet:
				intest.Assert(false, "cast Set to String should not set mysql.EnumSetAsIntFlag")
				return
			default:
				intest.Assert(false, "unknown type %d for INT", argFt.GetType())
				return
			}
		}
	case types.ETReal:
		// MySQL used 12/22 for float/double, it's because MySQL turns float/double into scientific notation
		// in some situations. TiDB choose to use 'f' format for all the cases, so TiDB needs much longer length
		// for float/double.
		//
		// The largest float/double value is around `3.40e38`/`1.79e308`, and the smallest positive float/double value
		// is around `1.40e-45`/`4.94e-324`. Therefore, we need at least `1 (sign) + 1 (integer) + 1 (dot) + (45 + 39) (decimal) = 87`
		// for float and `1 (sign) + 1 (integer) + 1 (dot) + (324 + 43) (decimal) = 370` for double.
		//
		// Actually, the golang will usually generate a much smaller string. It used ryu algorithm to generate the shortest
		// decimal representation. It's not necessary to keep all decimals. Ref:
		// - https://github.com/ulfjack/ryu
		// - https://dl.acm.org/doi/10.1145/93548.93559
		// So maybe 48/327 is enough for float/double, but we still set 87/370 for safety.
		if originalFlen == types.UnspecifiedLength {
			if argFt.GetType() == mysql.TypeFloat {
				retFt.SetFlen(87)
			} else if argFt.GetType() == mysql.TypeDouble {
				retFt.SetFlen(370)
			}
		}
	case types.ETDecimal:
		if originalFlen == types.UnspecifiedLength {
			retFt.SetFlen(decimalPrecisionToLength(argFt))
		}
	case types.ETDatetime, types.ETTimestamp:
		if originalFlen == types.UnspecifiedLength {
			if argFt.GetType() == mysql.TypeDate {
				retFt.SetFlen(mysql.MaxDateWidth)
			} else {
				retFt.SetFlen(mysql.MaxDatetimeWidthNoFsp)
			}

			// Theoretically, the decimal of `DATE` will never be greater than 0.
			decimal := argFt.GetDecimal()
			if decimal > 0 {
				// If the type is datetime or timestamp with fractional seconds, we need to set the length to
				// accommodate the fractional seconds part.
				retFt.SetFlen(retFt.GetFlen() + 1 + decimal)
			}
		}
	case types.ETDuration:
		if originalFlen == types.UnspecifiedLength {
			retFt.SetFlen(mysql.MaxDurationWidthNoFsp)
			decimal := argFt.GetDecimal()
			if decimal > 0 {
				// If the type is time with fractional seconds, we need to set the length to
				// accommodate the fractional seconds part.
				retFt.SetFlen(retFt.GetFlen() + 1 + decimal)
			}
		}
	case types.ETJson:
		if originalFlen == types.UnspecifiedLength {
			retFt.SetFlen(mysql.MaxLongBlobWidth)
			retFt.SetType(mysql.TypeLongBlob)
		}
	case types.ETVectorFloat32:

	case types.ETString:
		if originalFlen == types.UnspecifiedLength {
			switch argFt.GetType() {
			case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
				if argFt.GetFlen() > 0 {
					retFt.SetFlen(argFt.GetFlen())
				}
			case mysql.TypeTinyBlob:
				retFt.SetFlen(maxTinyBlobSize)
			case mysql.TypeBlob:
				retFt.SetFlen(castBlobFlen)
			case mysql.TypeMediumBlob:
				retFt.SetFlen(castMediumBlobFlen)
			case mysql.TypeLongBlob:
				retFt.SetFlen(maxLongBlobSize)
			default:
				intest.Assert(false, "unknown type %d for String", argFt.GetType())
				return
			}
		}
	}
}

type castAsTimeFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
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
	case types.ETVectorFloat32:
		sig = &builtinCastVectorFloat32AsUnsupportedSig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastVectorFloat32AsTime)
	default:
		return nil, errors.Errorf("cannot cast from %s to %s", argTp, "Datetime")
	}
	return sig, nil
}

type castAsDurationFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsDurationFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
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
	case types.ETVectorFloat32:
		sig = &builtinCastVectorFloat32AsUnsupportedSig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CastVectorFloat32AsDuration)
	default:
		return nil, errors.Errorf("cannot cast from %s to %s", argTp, "Time")
	}
	return sig, nil
}

type castAsArrayFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsArrayFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}

	if args[0].GetType(ctx).EvalType() != types.ETJson {
		return ErrInvalidTypeForJSON.GenWithStackByArgs(1, "cast_as_array")
	}

	return nil
}

func (c *castAsArrayFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	arrayType := c.tp.ArrayType()
	switch arrayType.GetType() {
	case mysql.TypeYear, mysql.TypeJSON, mysql.TypeFloat, mysql.TypeNewDecimal:
		return nil, ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("CAST-ing data to array of %s", arrayType.String()))
	}
	if arrayType.EvalType() == types.ETString && arrayType.GetCharset() != charset.CharsetUTF8MB4 && arrayType.GetCharset() != charset.CharsetBin {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("specifying charset for multi-valued index")
	}
	if arrayType.EvalType() == types.ETString && arrayType.GetFlen() == types.UnspecifiedLength {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("CAST-ing data to array of char/binary BLOBs with unspecified length")
	}

	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	sig = &castJSONAsArrayFunctionSig{bf}
	return sig, nil
}

