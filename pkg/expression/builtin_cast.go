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
	"math"
	"strconv"
	"strings"
	gotime "time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
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
	default:
		panic("unsupported types.EvalType in castAsIntFunctionClass")
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
	default:
		panic("unsupported types.EvalType in castAsRealFunctionClass")
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
	default:
		panic("unsupported types.EvalType in castAsDecimalFunctionClass")
	}
	return sig, nil
}

type castAsStringFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsStringFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	if args[0].GetType(ctx.GetEvalCtx()).Hybrid() {
		sig = &builtinCastStringAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsString)
		return sig, nil
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	switch argTp {
	case types.ETInt:
		if bf.tp.GetFlen() == types.UnspecifiedLength {
			// check https://github.com/pingcap/tidb/issues/44786
			// set flen from integers may truncate integers, e.g. char(1) can not display -1[int(1)]
			switch args[0].GetType(ctx.GetEvalCtx()).GetType() {
			case mysql.TypeTiny:
				bf.tp.SetFlen(4)
			case mysql.TypeShort:
				bf.tp.SetFlen(6)
			case mysql.TypeInt24:
				bf.tp.SetFlen(9)
			case mysql.TypeLong:
				// set it to 11 as mysql
				bf.tp.SetFlen(11)
			default:
				bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
			}
		}
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
		// When cast from binary to some other charsets, we should check if the binary is valid or not.
		// so we build a from_binary function to do this check.
		bf.args[0] = HandleBinaryLiteral(ctx, args[0], &ExprCollation{Charset: c.tp.GetCharset(), Collation: c.tp.GetCollate()}, c.funcName, true)
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
	default:
		panic("unsupported types.EvalType in castAsTimeFunctionClass")
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
	default:
		panic("unsupported types.EvalType in castAsDurationFunctionClass")
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
		for i := 0; i < val.GetElemCount(); i++ {
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
	default:
		panic("unsupported types.EvalType in castAsJSONFunctionClass")
	}
	return sig, nil
}

type builtinCastIntAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && res < 0 {
		res = 0
	}
	return
}

type builtinCastIntAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if unsignedArgs0 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()); !mysql.HasUnsignedFlag(b.tp.GetFlag()) && !unsignedArgs0 {
		res = float64(val)
	} else if b.inUnion && !unsignedArgs0 && val < 0 {
		// Round up to 0 if the value is negative but the expression eval type is unsigned in `UNION` statement
		// NOTE: the following expressions are equal (so choose the more efficient one):
		// `b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && !unsignedArgs0 && val < 0`
		// `b.inUnion && !unsignedArgs0 && val < 0`
		res = 0
	} else {
		// recall that, int to float is different from uint to float
		res = float64(uint64(val))
	}
	return res, false, err
}

type builtinCastIntAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if unsignedArgs0 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()); !mysql.HasUnsignedFlag(b.tp.GetFlag()) && !unsignedArgs0 {
		//revive:disable:empty-lines
		res = types.NewDecFromInt(val)
		// Round up to 0 if the value is negative but the expression eval type is unsigned in `UNION` statement
		// NOTE: the following expressions are equal (so choose the more efficient one):
		// `b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && !unsignedArgs0 && val < 0`
		// `b.inUnion && !unsignedArgs0 && val < 0`
		//revive:enable:empty-lines
	} else if b.inUnion && !unsignedArgs0 && val < 0 {
		res = &types.MyDecimal{}
	} else {
		res = types.NewDecFromUint(uint64(val))
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ProduceDecWithSpecifiedTp(tc, res, b.tp)
	err = ec.HandleError(err)
	return res, isNull, err
}

type builtinCastIntAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tp := b.args[0].GetType(ctx)
	if !mysql.HasUnsignedFlag(tp.GetFlag()) {
		res = strconv.FormatInt(val, 10)
	} else {
		res = strconv.FormatUint(uint64(val), 10)
	}
	if tp.GetType() == mysql.TypeYear && res == "0" {
		res = "0000"
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastIntAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
		res, err = types.ParseTimeFromYear(val)
	} else {
		res, err = types.ParseTimeFromNum(typeCtx(ctx), val, b.tp.GetType(), b.tp.GetDecimal())
	}

	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastIntAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	dur, err := types.NumberToDuration(val, b.tp.GetDecimal())
	if err != nil {
		if types.ErrOverflow.Equal(err) || types.ErrTruncatedWrongVal.Equal(err) {
			ec := errCtx(ctx)
			err = ec.HandleError(err)
		}
		return res, true, err
	}
	return dur, false, err
}

type builtinCastIntAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if mysql.HasIsBooleanFlag(b.args[0].GetType(ctx).GetFlag()) {
		res = types.CreateBinaryJSON(val != 0)
	} else if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) {
		res = types.CreateBinaryJSON(uint64(val))
	} else {
		res = types.CreateBinaryJSON(val)
	}
	return res, false, nil
}

type builtinCastRealAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	return types.CreateBinaryJSON(val), isNull, err
}

type builtinCastDecimalAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return types.BinaryJSON{}, true, err
	}
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	f64, err := val.ToFloat64()
	if err != nil {
		return types.BinaryJSON{}, true, err
	}
	return types.CreateBinaryJSON(f64), isNull, err
}

type builtinCastStringAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	typ := b.args[0].GetType(ctx)
	if types.IsBinaryStr(typ) {
		buf := []byte(val)
		if typ.GetType() == mysql.TypeString && typ.GetFlen() > 0 {
			// the tailing zero should also be in the opaque json
			buf = make([]byte, typ.GetFlen())
			copy(buf, val)
		}

		res := types.CreateBinaryJSON(types.Opaque{
			TypeCode: b.args[0].GetType(ctx).GetType(),
			Buf:      buf,
		})

		return res, false, err
	} else if mysql.HasParseToJSONFlag(b.tp.GetFlag()) {
		res, err = types.ParseBinaryJSONFromString(val)
	} else {
		res = types.CreateBinaryJSON(val)
	}
	return res, false, err
}

type builtinCastDurationAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	val.Fsp = types.MaxFsp
	return types.CreateBinaryJSON(val), false, nil
}

type builtinCastTimeAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Type() == mysql.TypeDatetime || val.Type() == mysql.TypeTimestamp {
		val.SetFsp(types.MaxFsp)
	}
	return types.CreateBinaryJSON(val), false, nil
}

type builtinCastRealAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalReal(ctx, row)
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && res < 0 {
		res = 0
	}
	return
}

type builtinCastRealAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if !mysql.HasUnsignedFlag(b.tp.GetFlag()) {
		res, err = types.ConvertFloatToInt(val, types.IntergerSignedLowerBound(mysql.TypeLonglong), types.IntergerSignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
	} else if b.inUnion && val < 0 {
		res = 0
	} else {
		var uintVal uint64
		tc := typeCtx(ctx)
		uintVal, err = types.ConvertFloatToUint(tc.Flags(), val, types.IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
		res = int64(uintVal)
	}
	if types.ErrOverflow.Equal(err) {
		ec := errCtx(ctx)
		err = ec.HandleError(err)
	}
	return res, isNull, err
}

type builtinCastRealAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = new(types.MyDecimal)
	ec := errCtx(ctx)
	if !b.inUnion || val >= 0 {
		err = res.FromFloat64(val)
		if types.ErrOverflow.Equal(err) {
			warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", b.args[0])
			err = ec.HandleErrorWithAlias(err, err, warnErr)
		} else if types.ErrTruncated.Equal(err) {
			// This behavior is consistent with MySQL.
			err = nil
		}
		if err != nil {
			return res, false, err
		}
	}
	res, err = types.ProduceDecWithSpecifiedTp(typeCtx(ctx), res, b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastRealAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	bits := 64
	if b.args[0].GetType(ctx).GetType() == mysql.TypeFloat {
		// b.args[0].EvalReal() casts the value from float32 to float64, for example:
		// float32(208.867) is cast to float64(208.86700439)
		// If we strconv.FormatFloat the value with 64bits, the result is incorrect!
		bits = 32
	}
	res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(val, 'f', -1, bits), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastRealAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}
	// MySQL compatibility: 0 should not be converted to null, see #11203
	fv := strconv.FormatFloat(val, 'f', -1, 64)
	if fv == "0" {
		return types.ZeroTime, false, nil
	}
	res, err := types.ParseTimeFromFloatString(typeCtx(ctx), fv, b.tp.GetType(), b.tp.GetDecimal())
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastRealAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc := typeCtx(ctx)
	res, _, err = types.ParseDuration(tc, strconv.FormatFloat(val, 'f', -1, 64), b.tp.GetDecimal())
	if err != nil {
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = tc.HandleTruncate(err)
			// ErrTruncatedWrongVal needs to be considered NULL.
			return res, true, err
		}
	}
	return res, false, err
}

type builtinCastDecimalAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	evalDecimal, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = &types.MyDecimal{}
	if !(b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && evalDecimal.IsNegative()) {
		*res = *evalDecimal
	}
	res, err = types.ProduceDecWithSpecifiedTp(typeCtx(ctx), res, b.tp)
	ec := errCtx(ctx)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastDecimalAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// Round is needed for both unsigned and signed.
	var to types.MyDecimal
	err = val.Round(&to, 0, types.ModeHalfUp)
	if err != nil {
		return 0, true, err
	}

	if !mysql.HasUnsignedFlag(b.tp.GetFlag()) {
		res, err = to.ToInt()
	} else if b.inUnion && to.IsNegative() {
		res = 0
	} else {
		var uintRes uint64
		uintRes, err = to.ToUint()
		res = int64(uintRes)
	}

	if types.ErrOverflow.Equal(err) {
		ec := errCtx(ctx)
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", val)
		err = ec.HandleErrorWithAlias(err, err, warnErr)
	}

	return res, false, err
}

type builtinCastDecimalAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(string(val.ToString()), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastDecimalAsRealSig struct {
	baseBuiltinCastFunc
}

func setDataTypeDouble(srcDecimal int) (flen, decimal int) {
	decimal = mysql.NotFixedDec
	flen = floatLength(srcDecimal, decimal)
	return
}

func floatLength(srcDecimal int, decimalPar int) int {
	const dblDIG = 15
	if srcDecimal != mysql.NotFixedDec {
		return dblDIG + 2 + decimalPar
	}
	return dblDIG + 8
}

func (b *builtinCastDecimalAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && val.IsNegative() {
		res = 0
	} else {
		res, err = val.ToFloat64()
	}
	return res, false, err
}

type builtinCastDecimalAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseTimeFromFloatString(typeCtx(ctx), string(val.ToString()), b.tp.GetType(), b.tp.GetDecimal())
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, err
}

type builtinCastDecimalAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return res, true, err
	}
	res, _, err = types.ParseDuration(typeCtx(ctx), string(val.ToString()), b.tp.GetDecimal())
	if types.ErrTruncatedWrongVal.Equal(err) {
		ec := errCtx(ctx)
		err = ec.HandleError(err)
		// ErrTruncatedWrongVal needs to be considered NULL.
		return res, true, err
	}
	return res, false, err
}

type builtinCastStringAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastStringAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

// handleOverflow handles the overflow caused by cast string as int,
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html.
// When an out-of-range value is assigned to an integer column, MySQL stores the value representing the corresponding endpoint of the column data type range. If it is in select statement, it will return the
// endpoint value with a warning.
func (*builtinCastStringAsIntSig) handleOverflow(ctx EvalContext, origRes int64, origStr string, origErr error, isNegative bool) (res int64, err error) {
	res, err = origRes, origErr
	if err == nil {
		return
	}

	ec := errCtx(ctx)
	if types.ErrOverflow.Equal(origErr) {
		if isNegative {
			res = math.MinInt64
		} else {
			uval := uint64(math.MaxUint64)
			res = int64(uval)
		}
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", origStr)
		err = ec.HandleErrorWithAlias(origErr, origErr, warnErr)
	}
	return
}

func (b *builtinCastStringAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	if b.args[0].GetType(ctx).Hybrid() || IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalInt(ctx, row)
	}

	// Take the implicit evalInt path if possible.
	if CanImplicitEvalInt(b.args[0]) {
		return b.args[0].EvalInt(ctx, row)
	}

	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	val = strings.TrimSpace(val)
	isNegative := false
	if len(val) > 1 && val[0] == '-' { // negative number
		isNegative = true
	}

	var ures uint64
	tc := typeCtx(ctx)
	if !isNegative {
		ures, err = types.StrToUint(tc, val, true)
		res = int64(ures)

		if err == nil && !mysql.HasUnsignedFlag(b.tp.GetFlag()) && ures > uint64(math.MaxInt64) {
			tc.AppendWarning(types.ErrCastAsSignedOverflow)
		}
	} else if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) {
		res = 0
	} else {
		res, err = types.StrToInt(tc, val, true)
		if err == nil && mysql.HasUnsignedFlag(b.tp.GetFlag()) {
			// If overflow, don't append this warnings
			tc.AppendWarning(types.ErrCastNegIntAsUnsigned)
		}
	}

	res, err = b.handleOverflow(ctx, res, val, err, isNegative)
	return res, false, err
}

type builtinCastStringAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalReal(ctx, row)
	}

	// Take the implicit evalReal path if possible.
	if CanImplicitEvalReal(b.args[0]) {
		return b.args[0].EvalReal(ctx, row)
	}

	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.StrToFloat(typeCtx(ctx), val, true)
	if err != nil {
		return 0, false, err
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && res < 0 {
		res = 0
	}
	res, err = types.ProduceFloatWithSpecifiedTp(res, b.tp)
	return res, false, err
}

type builtinCastStringAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalDecimal(ctx, row)
	}
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	val = strings.TrimSpace(val)
	isNegative := len(val) > 1 && val[0] == '-'
	res = new(types.MyDecimal)
	ec := errCtx(ctx)
	if !(b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && isNegative) {
		err = res.FromString([]byte(val))
		if err == types.ErrTruncated {
			err = types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", []byte(val))
		}
		err = ec.HandleError(err)
		if err != nil {
			return res, false, err
		}
	}
	res, err = types.ProduceDecWithSpecifiedTp(typeCtx(ctx), res, b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastStringAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseTime(typeCtx(ctx), val, b.tp.GetType(), b.tp.GetDecimal())
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if res.IsZero() && sqlMode(ctx).HasNoZeroDateMode() {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, res.String()))
	}
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastStringAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, isNull, err = types.ParseDuration(typeCtx(ctx), val, b.tp.GetDecimal())
	if types.ErrTruncatedWrongVal.Equal(err) {
		ec := errCtx(ctx)
		err = ec.HandleError(err)
	}
	return res, isNull, err
}

type builtinCastTimeAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	tc := typeCtx(ctx)
	if res, err = res.Convert(tc, b.tp.GetType()); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	res, err = res.RoundFrac(tc, b.tp.GetDecimal())
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		res.SetType(b.tp.GetType())
	}
	return res, false, err
}

type builtinCastTimeAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc := typeCtx(ctx)
	t, err := val.RoundFrac(tc, types.DefaultFsp)
	if err != nil {
		return res, false, err
	}
	res, err = t.ToNumber().ToInt()
	return res, false, err
}

type builtinCastTimeAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastTimeAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ProduceDecWithSpecifiedTp(tc, val.ToNumber(), b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastTimeAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastTimeAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ConvertToDuration()
	if err != nil {
		return res, false, err
	}
	res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
	return res, false, err
}

type builtinCastDurationAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
	return res, false, err
}

type builtinCastDurationAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if b.tp.GetType() == mysql.TypeYear {
		res, err = val.ConvertToYear(typeCtx(ctx))
	} else {
		var dur types.Duration
		dur, err = val.RoundFrac(types.DefaultFsp, location(ctx))
		if err != nil {
			return res, false, err
		}
		res, err = dur.ToNumber().ToInt()
	}
	return res, false, err
}

type builtinCastDurationAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Fsp, err = types.CheckFsp(val.Fsp); err != nil {
		return res, false, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastDurationAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Fsp, err = types.CheckFsp(val.Fsp); err != nil {
		return res, false, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ProduceDecWithSpecifiedTp(tc, val.ToNumber(), b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastDurationAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

func padZeroForBinaryType(s string, tp *types.FieldType, ctx EvalContext) (string, bool, error) {
	flen := tp.GetFlen()
	if tp.GetType() == mysql.TypeString && types.IsBinaryStr(tp) && len(s) < flen {
		maxAllowedPacket := ctx.GetMaxAllowedPacket()
		if uint64(flen) > maxAllowedPacket {
			return "", true, handleAllowedPacketOverflowed(ctx, "cast_as_binary", maxAllowedPacket)
		}
		padding := make([]byte, flen-len(s))
		s = string(append([]byte(s), padding...))
	}
	return s, false, nil
}

type builtinCastDurationAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc := typeCtx(ctx)
	ts, err := getStmtTimestamp(ctx)
	if err != nil {
		ts = gotime.Now()
	}
	res, err = val.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	res, err = res.RoundFrac(tc, b.tp.GetDecimal())
	return res, false, err
}

type builtinCastJSONAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (val types.BinaryJSON, isNull bool, err error) {
	return b.args[0].EvalJSON(ctx, row)
}

type builtinCastJSONAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ConvertJSONToInt64(typeCtx(ctx), val, mysql.HasUnsignedFlag(b.tp.GetFlag()))
	ec := errCtx(ctx)
	err = ec.HandleError(err)
	return
}

type builtinCastJSONAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ConvertJSONToFloat(typeCtx(ctx), val)
	return
}

type builtinCastJSONAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ConvertJSONToDecimal(tc, val)
	if err != nil {
		return res, false, err
	}
	res, err = types.ProduceDecWithSpecifiedTp(tc, res, b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastJSONAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return s, false, nil
}

type builtinCastJSONAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	switch val.TypeCode {
	case types.JSONTypeCodeDate, types.JSONTypeCodeDatetime, types.JSONTypeCodeTimestamp:
		res = val.GetTimeWithFsp(b.tp.GetDecimal())
		res.SetType(b.tp.GetType())
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		}
		return res, isNull, err
	case types.JSONTypeCodeDuration:
		duration := val.GetDuration()

		tc := typeCtx(ctx)
		ts, err := getStmtTimestamp(ctx)
		if err != nil {
			ts = gotime.Now()
		}
		res, err = duration.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)
		if err != nil {
			return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
		}
		res, err = res.RoundFrac(tc, b.tp.GetDecimal())
		return res, isNull, err
	case types.JSONTypeCodeString:
		s, err := val.Unquote()
		if err != nil {
			return res, false, err
		}
		res, err = types.ParseTime(typeCtx(ctx), s, b.tp.GetType(), b.tp.GetDecimal())
		if err != nil {
			return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
		}
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		}
		return res, isNull, err
	default:
		ec := errCtx(ctx)
		err = types.ErrTruncatedWrongVal.GenWithStackByArgs(types.TypeStr(b.tp.GetType()), val.String())
		return res, true, ec.HandleError(err)
	}
}

type builtinCastJSONAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	tc := typeCtx(ctx)

	switch val.TypeCode {
	case types.JSONTypeCodeDate, types.JSONTypeCodeDatetime, types.JSONTypeCodeTimestamp:
		time := val.GetTimeWithFsp(b.tp.GetDecimal())
		res, err = time.ConvertToDuration()
		if err != nil {
			return res, false, err
		}
		res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
		return res, isNull, err
	case types.JSONTypeCodeDuration:
		res = val.GetDuration()
		return res, isNull, err
	case types.JSONTypeCodeString:
		s, err := val.Unquote()
		if err != nil {
			return res, false, err
		}
		res, _, err = types.ParseDuration(tc, s, b.tp.GetDecimal())
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = tc.HandleTruncate(err)
		}
		return res, isNull, err
	default:
		err = types.ErrTruncatedWrongVal.GenWithStackByArgs("TIME", val.String())
		return res, true, tc.HandleTruncate(err)
	}
}

// inCastContext is session key type that indicates whether executing
// in special cast context that negative unsigned num will be zero.
type inCastContext int

func (inCastContext) String() string {
	return "__cast_ctx"
}

// inUnionCastContext is session key value that indicates whether executing in
// union cast context.
// @see BuildCastFunction4Union
const inUnionCastContext inCastContext = 0

// CanImplicitEvalInt represents the builtin functions that have an implicit path to evaluate as integer,
// regardless of the type that type inference decides it to be.
// This is a nasty way to match the weird behavior of MySQL functions like `dayname()` being implicitly evaluated as integer.
// See https://github.com/mysql/mysql-server/blob/ee4455a33b10f1b1886044322e4893f587b319ed/sql/item_timefunc.h#L423 for details.
func CanImplicitEvalInt(expr Expression) bool {
	if f, ok := expr.(*ScalarFunction); ok {
		return f.FuncName.L == ast.DayName
	}
	return false
}

// CanImplicitEvalReal represents the builtin functions that have an implicit path to evaluate as real,
// regardless of the type that type inference decides it to be.
// This is a nasty way to match the weird behavior of MySQL functions like `dayname()` being implicitly evaluated as real.
// See https://github.com/mysql/mysql-server/blob/ee4455a33b10f1b1886044322e4893f587b319ed/sql/item_timefunc.h#L423 for details.
func CanImplicitEvalReal(expr Expression) bool {
	if f, ok := expr.(*ScalarFunction); ok {
		return f.FuncName.L == ast.DayName
	}
	return false
}

// BuildCastFunction4Union build a implicitly CAST ScalarFunction from the Union
// Expression.
func BuildCastFunction4Union(ctx BuildContext, expr Expression, tp *types.FieldType) (res Expression) {
	res, err := BuildCastFunctionWithCheck(ctx, expr, tp, true)
	terror.Log(err)
	return
}

// BuildCastCollationFunction builds a ScalarFunction which casts the collation.
func BuildCastCollationFunction(ctx BuildContext, expr Expression, ec *ExprCollation, enumOrSetRealTypeIsStr bool) Expression {
	if expr.GetType(ctx.GetEvalCtx()).EvalType() != types.ETString {
		return expr
	}
	if expr.GetType(ctx.GetEvalCtx()).GetCollate() == ec.Collation {
		return expr
	}
	tp := expr.GetType(ctx.GetEvalCtx()).Clone()
	if expr.GetType(ctx.GetEvalCtx()).Hybrid() {
		if !enumOrSetRealTypeIsStr {
			return expr
		}
		tp = types.NewFieldType(mysql.TypeVarString)
	} else if ec.Charset == charset.CharsetBin {
		// When cast character string to binary string, if we still use fixed length representation,
		// then 0 padding will be used, which can affect later execution.
		// e.g. https://github.com/pingcap/tidb/issues/34823.
		// On the other hand, we can not directly return origin expr back,
		// since we need binary collation to do string comparison later.
		// e.g. https://github.com/pingcap/tidb/pull/35053#discussion_r894155052
		// Here we use VarString type of cast, i.e `cast(a as binary)`, to avoid this problem.
		tp.SetType(mysql.TypeVarString)
	}
	tp.SetCharset(ec.Charset)
	tp.SetCollate(ec.Collation)
	newExpr := BuildCastFunction(ctx, expr, tp)
	return newExpr
}

// BuildCastFunction builds a CAST ScalarFunction from the Expression.
func BuildCastFunction(ctx BuildContext, expr Expression, tp *types.FieldType) (res Expression) {
	res, err := BuildCastFunctionWithCheck(ctx, expr, tp, false)
	terror.Log(err)
	return
}

// BuildCastFunctionWithCheck builds a CAST ScalarFunction from the Expression and return error if any.
func BuildCastFunctionWithCheck(ctx BuildContext, expr Expression, tp *types.FieldType, inUnion bool) (res Expression, err error) {
	argType := expr.GetType(ctx.GetEvalCtx())
	// If source argument's nullable, then target type should be nullable
	if !mysql.HasNotNullFlag(argType.GetFlag()) {
		tp.DelFlag(mysql.NotNullFlag)
	}
	expr = TryPushCastIntoControlFunctionForHybridType(ctx, expr, tp)
	var fc functionClass
	switch tp.EvalType() {
	case types.ETInt:
		fc = &castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp, inUnion}
	case types.ETDecimal:
		fc = &castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp, inUnion}
	case types.ETReal:
		fc = &castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp, inUnion}
	case types.ETDatetime, types.ETTimestamp:
		fc = &castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDuration:
		fc = &castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETJson:
		if tp.IsArray() {
			fc = &castAsArrayFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		} else {
			fc = &castAsJSONFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		}
	case types.ETString:
		fc = &castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeBit {
			tp.SetFlen((expr.GetType(ctx.GetEvalCtx()).GetFlen() + 7) / 8)
		}
	}
	f, err := fc.getFunction(ctx, []Expression{expr})
	res = &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp,
		Function: f,
	}
	// We do not fold CAST if the eval type of this scalar function is ETJson
	// since we may reset the flag of the field type of CastAsJson later which
	// would affect the evaluation of it.
	if tp.EvalType() != types.ETJson && err == nil {
		res = FoldConstant(ctx, res)
	}
	return res, err
}

// WrapWithCastAsInt wraps `expr` with `cast` if the return type of expr is not
// type int, otherwise, returns `expr` directly.
func WrapWithCastAsInt(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeEnum {
		if col, ok := expr.(*Column); ok {
			col = col.Clone().(*Column)
			col.RetType = col.RetType.Clone()
			expr = col
		}
		expr.GetType(ctx.GetEvalCtx()).AddFlag(mysql.EnumSetAsIntFlag)
	}
	if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlen(expr.GetType(ctx.GetEvalCtx()).GetFlen())
	tp.SetDecimal(0)
	types.SetBinChsClnFlag(tp)
	tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & (mysql.UnsignedFlag | mysql.NotNullFlag))
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsReal wraps `expr` with `cast` if the return type of expr is not
// type real, otherwise, returns `expr` directly.
func WrapWithCastAsReal(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETReal {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeDouble)
	tp.SetFlen(mysql.MaxRealWidth)
	tp.SetDecimal(types.UnspecifiedLength)
	types.SetBinChsClnFlag(tp)
	tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & (mysql.UnsignedFlag | mysql.NotNullFlag))
	return BuildCastFunction(ctx, expr, tp)
}

func minimalDecimalLenForHoldingInteger(tp byte) int {
	switch tp {
	case mysql.TypeTiny:
		return 3
	case mysql.TypeShort:
		return 5
	case mysql.TypeInt24:
		return 8
	case mysql.TypeLong:
		return 10
	case mysql.TypeLonglong:
		return 20
	case mysql.TypeYear:
		return 4
	default:
		return mysql.MaxIntWidth
	}
}

// WrapWithCastAsDecimal wraps `expr` with `cast` if the return type of expr is
// not type decimal, otherwise, returns `expr` directly.
func WrapWithCastAsDecimal(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETDecimal {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeNewDecimal)
	tp.SetFlenUnderLimit(expr.GetType(ctx.GetEvalCtx()).GetFlen())
	tp.SetDecimalUnderLimit(expr.GetType(ctx.GetEvalCtx()).GetDecimal())

	if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		tp.SetFlen(minimalDecimalLenForHoldingInteger(expr.GetType(ctx.GetEvalCtx()).GetType()))
		tp.SetDecimal(0)
	}
	if tp.GetFlen() == types.UnspecifiedLength || tp.GetFlen() > mysql.MaxDecimalWidth {
		tp.SetFlen(mysql.MaxDecimalWidth)
	}
	types.SetBinChsClnFlag(tp)
	tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & (mysql.UnsignedFlag | mysql.NotNullFlag))
	castExpr := BuildCastFunction(ctx, expr, tp)
	// For const item, we can use find-grained precision and scale by the result.
	if castExpr.ConstLevel() == ConstStrict {
		val, isnull, err := castExpr.EvalDecimal(ctx.GetEvalCtx(), chunk.Row{})
		if !isnull && err == nil {
			precision, frac := val.PrecisionAndFrac()
			castTp := castExpr.GetType(ctx.GetEvalCtx())
			castTp.SetDecimalUnderLimit(frac)
			castTp.SetFlenUnderLimit(precision)
		}
	}
	return castExpr
}

// WrapWithCastAsString wraps `expr` with `cast` if the return type of expr is
// not type string, otherwise, returns `expr` directly.
func WrapWithCastAsString(ctx BuildContext, expr Expression) Expression {
	exprTp := expr.GetType(ctx.GetEvalCtx())
	if exprTp.EvalType() == types.ETString {
		return expr
	}
	argLen := exprTp.GetFlen()
	// If expr is decimal, we should take the decimal point ,negative sign and the leading zero(0.xxx)
	// into consideration, so we set `expr.GetType(ctx.GetEvalCtx()).GetFlen() + 3` as the `argLen`.
	// Since the length of float and double is not accurate, we do not handle
	// them.
	if exprTp.GetType() == mysql.TypeNewDecimal && argLen != types.UnspecifiedFsp {
		argLen += 3
	}

	if exprTp.EvalType() == types.ETInt {
		argLen = mysql.MaxIntWidth
		// For TypeBit, castAsString will make length as int(( bit_len + 7 ) / 8) bytes due to
		// TiKV needs the bit's real len during calculating, eg: ascii(bit).
		if exprTp.GetType() == mysql.TypeBit {
			argLen = (exprTp.GetFlen() + 7) / 8
		}
	}

	// Because we can't control the length of cast(float as char) for now, we can't determine the argLen.
	if exprTp.GetType() == mysql.TypeFloat || exprTp.GetType() == mysql.TypeDouble {
		argLen = -1
	}
	tp := types.NewFieldType(mysql.TypeVarString)
	if expr.Coercibility() == CoercibilityExplicit {
		charset, collate := expr.CharsetAndCollation()
		tp.SetCharset(charset)
		tp.SetCollate(collate)
	} else if exprTp.GetType() == mysql.TypeBit {
		// Implicitly casting BIT to string will make it a binary
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
	} else {
		charset, collate := ctx.GetCharsetInfo()
		tp.SetCharset(charset)
		tp.SetCollate(collate)
	}
	tp.SetFlen(argLen)
	tp.SetDecimal(types.UnspecifiedLength)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsTime wraps `expr` with `cast` if the return type of expr is not
// same as type of the specified `tp` , otherwise, returns `expr` directly.
func WrapWithCastAsTime(ctx BuildContext, expr Expression, tp *types.FieldType) Expression {
	exprTp := expr.GetType(ctx.GetEvalCtx()).GetType()
	if tp.GetType() == exprTp {
		return expr
	} else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.GetType() == mysql.TypeDatetime {
		return expr
	}
	switch x := expr.GetType(ctx.GetEvalCtx()).EvalType(); x {
	case types.ETInt:
		tp.SetDecimal(types.MinFsp)
	case types.ETString, types.ETReal, types.ETJson:
		tp.SetDecimal(types.MaxFsp)
	case types.ETDatetime, types.ETTimestamp, types.ETDuration:
		tp.SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
	case types.ETDecimal:
		tp.SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
		if tp.GetDecimal() > types.MaxFsp {
			tp.SetDecimal(types.MaxFsp)
		}
	default:
	}
	switch tp.GetType() {
	case mysql.TypeDate:
		tp.SetFlen(mysql.MaxDateWidth)
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		tp.SetFlen(mysql.MaxDatetimeWidthNoFsp)
		if tp.GetDecimal() > 0 {
			tp.SetFlen(tp.GetFlen() + 1 + tp.GetDecimal())
		}
	}
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDuration wraps `expr` with `cast` if the return type of expr is
// not type duration, otherwise, returns `expr` directly.
func WrapWithCastAsDuration(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeDuration {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeDuration)
	switch x := expr.GetType(ctx.GetEvalCtx()); x.GetType() {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDate:
		tp.SetDecimal(x.GetDecimal())
	default:
		tp.SetDecimal(types.MaxFsp)
	}
	tp.SetFlen(mysql.MaxDurationWidthNoFsp)
	if tp.GetDecimal() > 0 {
		tp.SetFlen(tp.GetFlen() + 1 + tp.GetDecimal())
	}
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsJSON wraps `expr` with `cast` if the return type of expr is not
// type json, otherwise, returns `expr` directly.
func WrapWithCastAsJSON(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeJSON && !mysql.HasParseToJSONFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag()) {
		return expr
	}
	tp := types.NewFieldTypeBuilder().SetType(mysql.TypeJSON).SetFlag(mysql.BinaryFlag).SetFlen(12582912).SetCharset(mysql.DefaultCharset).SetCollate(mysql.DefaultCollationName).BuildP()
	return BuildCastFunction(ctx, expr, tp)
}

// TryPushCastIntoControlFunctionForHybridType try to push cast into control function for Hybrid Type.
// If necessary, it will rebuild control function using changed args.
// When a hybrid type is the output of a control function, the result may be as a numeric type to subsequent calculation
// We should perform the `Cast` operation early to avoid using the wrong type for calculation
// For example, the condition `if(1, e, 'a') = 1`, `if` function will output `e` and compare with `1`.
// If the evaltype is ETString, it will get wrong result. So we can rewrite the condition to
// `IfInt(1, cast(e as int), cast('a' as int)) = 1` to get the correct result.
func TryPushCastIntoControlFunctionForHybridType(ctx BuildContext, expr Expression, tp *types.FieldType) (res Expression) {
	sf, ok := expr.(*ScalarFunction)
	if !ok {
		return expr
	}

	var wrapCastFunc func(ctx BuildContext, expr Expression) Expression
	switch tp.EvalType() {
	case types.ETInt:
		wrapCastFunc = WrapWithCastAsInt
	case types.ETReal:
		wrapCastFunc = WrapWithCastAsReal
	default:
		return expr
	}

	isHybrid := func(ft *types.FieldType) bool {
		// todo: compatible with mysql control function using bit type. issue 24725
		return ft.Hybrid() && ft.GetType() != mysql.TypeBit
	}

	args := sf.GetArgs()
	switch sf.FuncName.L {
	case ast.If:
		if isHybrid(args[1].GetType(ctx.GetEvalCtx())) || isHybrid(args[2].GetType(ctx.GetEvalCtx())) {
			args[1] = wrapCastFunc(ctx, args[1])
			args[2] = wrapCastFunc(ctx, args[2])
			f, err := funcs[ast.If].getFunction(ctx, args)
			if err != nil {
				return expr
			}
			sf.RetType, sf.Function = f.getRetTp(), f
			return sf
		}
	case ast.Case:
		hasHybrid := false
		for i := 0; i < len(args)-1; i += 2 {
			hasHybrid = hasHybrid || isHybrid(args[i+1].GetType(ctx.GetEvalCtx()))
		}
		if len(args)%2 == 1 {
			hasHybrid = hasHybrid || isHybrid(args[len(args)-1].GetType(ctx.GetEvalCtx()))
		}
		if !hasHybrid {
			return expr
		}

		for i := 0; i < len(args)-1; i += 2 {
			args[i+1] = wrapCastFunc(ctx, args[i+1])
		}
		if len(args)%2 == 1 {
			args[len(args)-1] = wrapCastFunc(ctx, args[len(args)-1])
		}
		f, err := funcs[ast.Case].getFunction(ctx, args)
		if err != nil {
			return expr
		}
		sf.RetType, sf.Function = f.getRetTp(), f
		return sf
	case ast.Elt:
		hasHybrid := false
		for i := 1; i < len(args); i++ {
			hasHybrid = hasHybrid || isHybrid(args[i].GetType(ctx.GetEvalCtx()))
		}
		if !hasHybrid {
			return expr
		}

		for i := 1; i < len(args); i++ {
			args[i] = wrapCastFunc(ctx, args[i])
		}
		f, err := funcs[ast.Elt].getFunction(ctx, args)
		if err != nil {
			return expr
		}
		sf.RetType, sf.Function = f.getRetTp(), f
		return sf
	default:
		return expr
	}
	return expr
}
