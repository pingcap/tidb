// Copyright 2015 PingCAP, Inc.
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
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
)

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (s *testUtilSuite) checkPanic(f func()) (ret bool) {
	defer func() {
		if r := recover(); r != nil {
			ret = true
		}
	}()
	f()
	return false
}

func (s *testUtilSuite) TestBaseBuiltin(c *check.C) {
	ctx := mock.NewContext()
	bf, err := newBaseBuiltinFuncWithTp(ctx, "", nil, types.ETTimestamp)
	c.Assert(err, check.IsNil)
	_, _, err = bf.evalInt(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalReal(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalString(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalDecimal(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalTime(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalDuration(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalJSON(chunk.Row{})
	c.Assert(err, check.NotNil)
}

func (s *testUtilSuite) TestClone(c *check.C) {
	builtinFuncs := []builtinFunc{
		&builtinArithmeticPlusRealSig{}, &builtinArithmeticPlusDecimalSig{}, &builtinArithmeticPlusIntSig{}, &builtinArithmeticMinusRealSig{}, &builtinArithmeticMinusDecimalSig{},
		&builtinArithmeticMinusIntSig{}, &builtinArithmeticDivideRealSig{}, &builtinArithmeticDivideDecimalSig{}, &builtinArithmeticMultiplyRealSig{}, &builtinArithmeticMultiplyDecimalSig{},
		&builtinArithmeticMultiplyIntUnsignedSig{}, &builtinArithmeticMultiplyIntSig{}, &builtinArithmeticIntDivideIntSig{}, &builtinArithmeticIntDivideDecimalSig{}, &builtinArithmeticModIntSig{},
		&builtinArithmeticModRealSig{}, &builtinArithmeticModDecimalSig{}, &builtinCastIntAsIntSig{}, &builtinCastIntAsRealSig{}, &builtinCastIntAsStringSig{},
		&builtinCastIntAsDecimalSig{}, &builtinCastIntAsTimeSig{}, &builtinCastIntAsDurationSig{}, &builtinCastIntAsJSONSig{}, &builtinCastRealAsIntSig{},
		&builtinCastRealAsRealSig{}, &builtinCastRealAsStringSig{}, &builtinCastRealAsDecimalSig{}, &builtinCastRealAsTimeSig{}, &builtinCastRealAsDurationSig{},
		&builtinCastRealAsJSONSig{}, &builtinCastDecimalAsIntSig{}, &builtinCastDecimalAsRealSig{}, &builtinCastDecimalAsStringSig{}, &builtinCastDecimalAsDecimalSig{},
		&builtinCastDecimalAsTimeSig{}, &builtinCastDecimalAsDurationSig{}, &builtinCastDecimalAsJSONSig{}, &builtinCastStringAsIntSig{}, &builtinCastStringAsRealSig{},
		&builtinCastStringAsStringSig{}, &builtinCastStringAsDecimalSig{}, &builtinCastStringAsTimeSig{}, &builtinCastStringAsDurationSig{}, &builtinCastStringAsJSONSig{},
		&builtinCastTimeAsIntSig{}, &builtinCastTimeAsRealSig{}, &builtinCastTimeAsStringSig{}, &builtinCastTimeAsDecimalSig{}, &builtinCastTimeAsTimeSig{},
		&builtinCastTimeAsDurationSig{}, &builtinCastTimeAsJSONSig{}, &builtinCastDurationAsIntSig{}, &builtinCastDurationAsRealSig{}, &builtinCastDurationAsStringSig{},
		&builtinCastDurationAsDecimalSig{}, &builtinCastDurationAsTimeSig{}, &builtinCastDurationAsDurationSig{}, &builtinCastDurationAsJSONSig{}, &builtinCastJSONAsIntSig{},
		&builtinCastJSONAsRealSig{}, &builtinCastJSONAsStringSig{}, &builtinCastJSONAsDecimalSig{}, &builtinCastJSONAsTimeSig{}, &builtinCastJSONAsDurationSig{},
		&builtinCastJSONAsJSONSig{}, &builtinCoalesceIntSig{}, &builtinCoalesceRealSig{}, &builtinCoalesceDecimalSig{}, &builtinCoalesceStringSig{},
		&builtinCoalesceTimeSig{}, &builtinCoalesceDurationSig{}, &builtinGreatestIntSig{}, &builtinGreatestRealSig{}, &builtinGreatestDecimalSig{},
		&builtinGreatestStringSig{}, &builtinGreatestTimeSig{}, &builtinLeastIntSig{}, &builtinLeastRealSig{}, &builtinLeastDecimalSig{},
		&builtinLeastStringSig{}, &builtinLeastTimeSig{}, &builtinIntervalIntSig{}, &builtinIntervalRealSig{}, &builtinLTIntSig{},
		&builtinLTRealSig{}, &builtinLTDecimalSig{}, &builtinLTStringSig{}, &builtinLTDurationSig{}, &builtinLTTimeSig{},
		&builtinLEIntSig{}, &builtinLERealSig{}, &builtinLEDecimalSig{}, &builtinLEStringSig{}, &builtinLEDurationSig{},
		&builtinLETimeSig{}, &builtinGTIntSig{}, &builtinGTRealSig{}, &builtinGTDecimalSig{}, &builtinGTStringSig{},
		&builtinGTTimeSig{}, &builtinGTDurationSig{}, &builtinGEIntSig{}, &builtinGERealSig{}, &builtinGEDecimalSig{},
		&builtinGEStringSig{}, &builtinGETimeSig{}, &builtinGEDurationSig{}, &builtinNEIntSig{}, &builtinNERealSig{},
		&builtinNEDecimalSig{}, &builtinNEStringSig{}, &builtinNETimeSig{}, &builtinNEDurationSig{}, &builtinNullEQIntSig{},
		&builtinNullEQRealSig{}, &builtinNullEQDecimalSig{}, &builtinNullEQStringSig{}, &builtinNullEQTimeSig{}, &builtinNullEQDurationSig{},
		&builtinCaseWhenIntSig{}, &builtinCaseWhenRealSig{}, &builtinCaseWhenDecimalSig{}, &builtinCaseWhenStringSig{}, &builtinCaseWhenTimeSig{},
		&builtinCaseWhenDurationSig{}, &builtinIfNullIntSig{}, &builtinIfNullRealSig{}, &builtinIfNullDecimalSig{}, &builtinIfNullStringSig{},
		&builtinIfNullTimeSig{}, &builtinIfNullDurationSig{}, &builtinIfNullJSONSig{}, &builtinIfIntSig{}, &builtinIfRealSig{},
		&builtinIfDecimalSig{}, &builtinIfStringSig{}, &builtinIfTimeSig{}, &builtinIfDurationSig{}, &builtinIfJSONSig{},
		&builtinAesDecryptSig{}, &builtinAesDecryptIVSig{}, &builtinAesEncryptSig{}, &builtinAesEncryptIVSig{}, &builtinCompressSig{},
		&builtinMD5Sig{}, &builtinPasswordSig{}, &builtinRandomBytesSig{}, &builtinSHA1Sig{}, &builtinSHA2Sig{},
		&builtinUncompressSig{}, &builtinUncompressedLengthSig{}, &builtinDatabaseSig{}, &builtinFoundRowsSig{}, &builtinCurrentUserSig{},
		&builtinUserSig{}, &builtinConnectionIDSig{}, &builtinLastInsertIDSig{}, &builtinLastInsertIDWithIDSig{}, &builtinVersionSig{},
		&builtinTiDBVersionSig{}, &builtinRowCountSig{}, &builtinJSONTypeSig{}, &builtinJSONQuoteSig{}, &builtinJSONUnquoteSig{},
		&builtinJSONArraySig{}, &builtinJSONArrayAppendSig{}, &builtinJSONObjectSig{}, &builtinJSONExtractSig{}, &builtinJSONSetSig{},
		&builtinJSONInsertSig{}, &builtinJSONReplaceSig{}, &builtinJSONRemoveSig{}, &builtinJSONMergeSig{}, &builtinJSONContainsSig{},
		&builtinJSONStorageSizeSig{}, &builtinJSONDepthSig{}, &builtinJSONSearchSig{}, &builtinJSONKeysSig{}, &builtinJSONKeys2ArgsSig{}, &builtinJSONLengthSig{},
		&builtinLikeSig{}, &builtinRegexpSig{}, &builtinRegexpUTF8Sig{}, &builtinAbsRealSig{}, &builtinAbsIntSig{},
		&builtinAbsUIntSig{}, &builtinAbsDecSig{}, &builtinRoundRealSig{}, &builtinRoundIntSig{}, &builtinRoundDecSig{},
		&builtinRoundWithFracRealSig{}, &builtinRoundWithFracIntSig{}, &builtinRoundWithFracDecSig{}, &builtinCeilRealSig{}, &builtinCeilIntToDecSig{},
		&builtinCeilIntToIntSig{}, &builtinCeilDecToIntSig{}, &builtinCeilDecToDecSig{}, &builtinFloorRealSig{}, &builtinFloorIntToDecSig{},
		&builtinFloorIntToIntSig{}, &builtinFloorDecToIntSig{}, &builtinFloorDecToDecSig{}, &builtinLog1ArgSig{}, &builtinLog2ArgsSig{},
		&builtinLog2Sig{}, &builtinLog10Sig{}, &builtinRandSig{}, &builtinRandWithSeedFirstGenSig{}, &builtinPowSig{},
		&builtinConvSig{}, &builtinCRC32Sig{}, &builtinSignSig{}, &builtinSqrtSig{}, &builtinAcosSig{},
		&builtinAsinSig{}, &builtinAtan1ArgSig{}, &builtinAtan2ArgsSig{}, &builtinCosSig{}, &builtinCotSig{},
		&builtinDegreesSig{}, &builtinExpSig{}, &builtinPISig{}, &builtinRadiansSig{}, &builtinSinSig{},
		&builtinTanSig{}, &builtinTruncateIntSig{}, &builtinTruncateRealSig{}, &builtinTruncateDecimalSig{}, &builtinTruncateUintSig{},
		&builtinSleepSig{}, &builtinLockSig{}, &builtinReleaseLockSig{}, &builtinDecimalAnyValueSig{}, &builtinDurationAnyValueSig{},
		&builtinIntAnyValueSig{}, &builtinJSONAnyValueSig{}, &builtinRealAnyValueSig{}, &builtinStringAnyValueSig{}, &builtinTimeAnyValueSig{},
		&builtinInetAtonSig{}, &builtinInetNtoaSig{}, &builtinInet6AtonSig{}, &builtinInet6NtoaSig{}, &builtinIsIPv4Sig{},
		&builtinIsIPv4CompatSig{}, &builtinIsIPv4MappedSig{}, &builtinIsIPv6Sig{}, &builtinUUIDSig{}, &builtinNameConstIntSig{},
		&builtinNameConstRealSig{}, &builtinNameConstDecimalSig{}, &builtinNameConstTimeSig{}, &builtinNameConstDurationSig{}, &builtinNameConstStringSig{},
		&builtinNameConstJSONSig{}, &builtinLogicAndSig{}, &builtinLogicOrSig{}, &builtinLogicXorSig{}, &builtinRealIsTrueSig{},
		&builtinDecimalIsTrueSig{}, &builtinIntIsTrueSig{}, &builtinRealIsFalseSig{}, &builtinDecimalIsFalseSig{}, &builtinIntIsFalseSig{},
		&builtinUnaryMinusIntSig{}, &builtinDecimalIsNullSig{}, &builtinDurationIsNullSig{}, &builtinIntIsNullSig{}, &builtinRealIsNullSig{},
		&builtinStringIsNullSig{}, &builtinTimeIsNullSig{}, &builtinUnaryNotRealSig{}, &builtinUnaryNotDecimalSig{}, &builtinUnaryNotIntSig{}, &builtinSleepSig{}, &builtinInIntSig{},
		&builtinInStringSig{}, &builtinInDecimalSig{}, &builtinInRealSig{}, &builtinInTimeSig{}, &builtinInDurationSig{},
		&builtinInJSONSig{}, &builtinRowSig{}, &builtinSetVarSig{}, &builtinGetVarSig{}, &builtinLockSig{},
		&builtinReleaseLockSig{}, &builtinValuesIntSig{}, &builtinValuesRealSig{}, &builtinValuesDecimalSig{}, &builtinValuesStringSig{},
		&builtinValuesTimeSig{}, &builtinValuesDurationSig{}, &builtinValuesJSONSig{}, &builtinBitCountSig{}, &builtinGetParamStringSig{},
		&builtinLengthSig{}, &builtinASCIISig{}, &builtinConcatSig{}, &builtinConcatWSSig{}, &builtinLeftSig{},
		&builtinLeftUTF8Sig{}, &builtinRightSig{}, &builtinRightUTF8Sig{}, &builtinRepeatSig{}, &builtinLowerSig{},
		&builtinReverseUTF8Sig{}, &builtinReverseSig{}, &builtinSpaceSig{}, &builtinUpperSig{}, &builtinStrcmpSig{},
		&builtinReplaceSig{}, &builtinConvertSig{}, &builtinSubstring2ArgsSig{}, &builtinSubstring3ArgsSig{}, &builtinSubstring2ArgsUTF8Sig{},
		&builtinSubstring3ArgsUTF8Sig{}, &builtinSubstringIndexSig{}, &builtinLocate2ArgsUTF8Sig{}, &builtinLocate3ArgsUTF8Sig{}, &builtinLocate2ArgsSig{},
		&builtinLocate3ArgsSig{}, &builtinHexStrArgSig{}, &builtinHexIntArgSig{}, &builtinUnHexSig{}, &builtinTrim1ArgSig{},
		&builtinTrim2ArgsSig{}, &builtinTrim3ArgsSig{}, &builtinLTrimSig{}, &builtinRTrimSig{}, &builtinLpadUTF8Sig{},
		&builtinLpadSig{}, &builtinRpadUTF8Sig{}, &builtinRpadSig{}, &builtinBitLengthSig{}, &builtinCharSig{},
		&builtinCharLengthUTF8Sig{}, &builtinFindInSetSig{}, &builtinMakeSetSig{}, &builtinOctIntSig{}, &builtinOctStringSig{},
		&builtinOrdSig{}, &builtinQuoteSig{}, &builtinBinSig{}, &builtinEltSig{}, &builtinExportSet3ArgSig{},
		&builtinExportSet4ArgSig{}, &builtinExportSet5ArgSig{}, &builtinFormatWithLocaleSig{}, &builtinFormatSig{}, &builtinFromBase64Sig{},
		&builtinToBase64Sig{}, &builtinInsertSig{}, &builtinInsertUTF8Sig{}, &builtinInstrUTF8Sig{}, &builtinInstrSig{},
		&builtinFieldRealSig{}, &builtinFieldIntSig{}, &builtinFieldStringSig{}, &builtinDateSig{}, &builtinDateLiteralSig{},
		&builtinDateDiffSig{}, &builtinNullTimeDiffSig{}, &builtinTimeStringTimeDiffSig{}, &builtinDurationStringTimeDiffSig{}, &builtinDurationDurationTimeDiffSig{},
		&builtinStringTimeTimeDiffSig{}, &builtinStringDurationTimeDiffSig{}, &builtinStringStringTimeDiffSig{}, &builtinTimeTimeTimeDiffSig{}, &builtinDateFormatSig{},
		&builtinHourSig{}, &builtinMinuteSig{}, &builtinSecondSig{}, &builtinMicroSecondSig{}, &builtinMonthSig{},
		&builtinMonthNameSig{}, &builtinNowWithArgSig{}, &builtinNowWithoutArgSig{}, &builtinDayNameSig{}, &builtinDayOfMonthSig{},
		&builtinDayOfWeekSig{}, &builtinDayOfYearSig{}, &builtinWeekWithModeSig{}, &builtinWeekWithoutModeSig{}, &builtinWeekDaySig{},
		&builtinWeekOfYearSig{}, &builtinYearSig{}, &builtinYearWeekWithModeSig{}, &builtinYearWeekWithoutModeSig{}, &builtinGetFormatSig{},
		&builtinSysDateWithFspSig{}, &builtinSysDateWithoutFspSig{}, &builtinCurrentDateSig{}, &builtinCurrentTime0ArgSig{}, &builtinCurrentTime1ArgSig{},
		&builtinTimeSig{}, &builtinTimeLiteralSig{}, &builtinUTCDateSig{}, &builtinUTCTimestampWithArgSig{}, &builtinUTCTimestampWithoutArgSig{},
		&builtinAddDatetimeAndDurationSig{}, &builtinAddDatetimeAndStringSig{}, &builtinAddTimeDateTimeNullSig{}, &builtinAddStringAndDurationSig{}, &builtinAddStringAndStringSig{},
		&builtinAddTimeStringNullSig{}, &builtinAddDurationAndDurationSig{}, &builtinAddDurationAndStringSig{}, &builtinAddTimeDurationNullSig{}, &builtinAddDateAndDurationSig{},
		&builtinAddDateAndStringSig{}, &builtinSubDatetimeAndDurationSig{}, &builtinSubDatetimeAndStringSig{}, &builtinSubTimeDateTimeNullSig{}, &builtinSubStringAndDurationSig{},
		&builtinSubStringAndStringSig{}, &builtinSubTimeStringNullSig{}, &builtinSubDurationAndDurationSig{}, &builtinSubDurationAndStringSig{}, &builtinSubTimeDurationNullSig{},
		&builtinSubDateAndDurationSig{}, &builtinSubDateAndStringSig{}, &builtinUnixTimestampCurrentSig{}, &builtinUnixTimestampIntSig{}, &builtinUnixTimestampDecSig{},
		&builtinConvertTzSig{}, &builtinMakeDateSig{}, &builtinMakeTimeSig{}, &builtinPeriodAddSig{}, &builtinPeriodDiffSig{},
		&builtinQuarterSig{}, &builtinSecToTimeSig{}, &builtinTimeToSecSig{}, &builtinTimestampAddSig{}, &builtinToDaysSig{},
		&builtinToSecondsSig{}, &builtinUTCTimeWithArgSig{}, &builtinUTCTimeWithoutArgSig{}, &builtinTimestamp1ArgSig{}, &builtinTimestamp2ArgsSig{},
		&builtinTimestampLiteralSig{}, &builtinLastDaySig{}, &builtinStrToDateDateSig{}, &builtinStrToDateDatetimeSig{}, &builtinStrToDateDurationSig{},
		&builtinFromUnixTime1ArgSig{}, &builtinFromUnixTime2ArgSig{}, &builtinExtractDatetimeSig{}, &builtinExtractDurationSig{}, &builtinAddDateStringStringSig{},
		&builtinAddDateStringIntSig{}, &builtinAddDateStringRealSig{}, &builtinAddDateStringDecimalSig{}, &builtinAddDateIntStringSig{}, &builtinAddDateIntIntSig{},
		&builtinAddDateIntRealSig{}, &builtinAddDateIntDecimalSig{}, &builtinAddDateDatetimeStringSig{}, &builtinAddDateDatetimeIntSig{}, &builtinAddDateDatetimeRealSig{},
		&builtinAddDateDatetimeDecimalSig{}, &builtinSubDateStringStringSig{}, &builtinSubDateStringIntSig{}, &builtinSubDateStringRealSig{}, &builtinSubDateStringDecimalSig{},
		&builtinSubDateIntStringSig{}, &builtinSubDateIntIntSig{}, &builtinSubDateIntRealSig{}, &builtinSubDateIntDecimalSig{}, &builtinSubDateDatetimeStringSig{},
		&builtinSubDateDatetimeIntSig{}, &builtinSubDateDatetimeRealSig{}, &builtinSubDateDatetimeDecimalSig{},
	}
	for _, f := range builtinFuncs {
		cf := f.Clone()
		c.Assert(reflect.TypeOf(f) == reflect.TypeOf(cf), check.IsTrue)
	}
}

func (s *testUtilSuite) TestGetUint64FromConstant(c *check.C) {
	con := &Constant{
		Value: types.NewDatum(nil),
	}
	_, isNull, ok := GetUint64FromConstant(con)
	c.Assert(ok, check.IsTrue)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{
		Value: types.NewIntDatum(-1),
	}
	_, _, ok = GetUint64FromConstant(con)
	c.Assert(ok, check.IsFalse)

	con.Value = types.NewIntDatum(1)
	num, isNull, ok := GetUint64FromConstant(con)
	c.Assert(ok, check.IsTrue)
	c.Assert(isNull, check.IsFalse)
	c.Assert(num, check.Equals, uint64(1))

	con.Value = types.NewUintDatum(1)
	num, _, _ = GetUint64FromConstant(con)
	c.Assert(num, check.Equals, uint64(1))

	con.DeferredExpr = &Constant{Value: types.NewIntDatum(1)}
	num, _, _ = GetUint64FromConstant(con)
	c.Assert(num, check.Equals, uint64(1))

	ctx := mock.NewContext()
	ctx.GetSessionVars().PreparedParams = []types.Datum{
		types.NewUintDatum(100),
	}
	con.ParamMarker = &ParamMarker{order: 0, ctx: ctx}
	num, _, _ = GetUint64FromConstant(con)
	c.Assert(num, check.Equals, uint64(100))
}

func (s *testUtilSuite) TestSetExprColumnInOperand(c *check.C) {
	col := &Column{RetType: newIntFieldType()}
	c.Assert(setExprColumnInOperand(col).(*Column).InOperand, check.IsTrue)

	f, err := funcs[ast.Abs].getFunction(mock.NewContext(), []Expression{col})
	c.Assert(err, check.IsNil)
	fun := &ScalarFunction{Function: f}
	setExprColumnInOperand(fun)
	c.Assert(f.getArgs()[0].(*Column).InOperand, check.IsTrue)
}

func (s testUtilSuite) TestPopRowFirstArg(c *check.C) {
	c1, c2, c3 := &Column{RetType: newIntFieldType()}, &Column{RetType: newIntFieldType()}, &Column{RetType: newIntFieldType()}
	f, err := funcs[ast.RowFunc].getFunction(mock.NewContext(), []Expression{c1, c2, c3})
	c.Assert(err, check.IsNil)
	fun := &ScalarFunction{Function: f, FuncName: model.NewCIStr(ast.RowFunc), RetType: newIntFieldType()}
	fun2, err := PopRowFirstArg(mock.NewContext(), fun)
	c.Assert(err, check.IsNil)
	c.Assert(len(fun2.(*ScalarFunction).GetArgs()), check.Equals, 2)
}

func (s testUtilSuite) TestGetStrIntFromConstant(c *check.C) {
	col := &Column{}
	_, _, err := GetStringFromConstant(mock.NewContext(), col)
	c.Assert(err, check.NotNil)

	con := &Constant{RetType: &types.FieldType{Tp: mysql.TypeNull}}
	_, isNull, err := GetStringFromConstant(mock.NewContext(), con)
	c.Assert(err, check.IsNil)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newIntFieldType(), Value: types.NewIntDatum(1)}
	ret, _, _ := GetStringFromConstant(mock.NewContext(), con)
	c.Assert(ret, check.Equals, "1")

	con = &Constant{RetType: &types.FieldType{Tp: mysql.TypeNull}}
	_, isNull, _ = GetIntFromConstant(mock.NewContext(), con)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newStringFieldType(), Value: types.NewStringDatum("abc")}
	_, isNull, _ = GetIntFromConstant(mock.NewContext(), con)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newStringFieldType(), Value: types.NewStringDatum("123")}
	num, _, _ := GetIntFromConstant(mock.NewContext(), con)
	c.Assert(num, check.Equals, 123)
}

func (s *testUtilSuite) TestSubstituteCorCol2Constant(c *check.C) {
	ctx := mock.NewContext()
	corCol1 := &CorrelatedColumn{Data: &NewOne().Value}
	corCol1.RetType = types.NewFieldType(mysql.TypeLonglong)
	corCol2 := &CorrelatedColumn{Data: &NewOne().Value}
	corCol2.RetType = types.NewFieldType(mysql.TypeLonglong)
	cast := BuildCastFunction(ctx, corCol1, types.NewFieldType(mysql.TypeLonglong))
	plus := newFunction(ast.Plus, cast, corCol2)
	plus2 := newFunction(ast.Plus, plus, NewOne())
	ans1 := &Constant{Value: types.NewIntDatum(3), RetType: types.NewFieldType(mysql.TypeLonglong)}
	ret, err := SubstituteCorCol2Constant(plus2)
	c.Assert(err, check.IsNil)
	c.Assert(ret.Equal(ctx, ans1), check.IsTrue)
	col1 := &Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	ret, err = SubstituteCorCol2Constant(col1)
	c.Assert(err, check.IsNil)
	ans2 := col1
	c.Assert(ret.Equal(ctx, ans2), check.IsTrue)
	plus3 := newFunction(ast.Plus, plus2, col1)
	ret, err = SubstituteCorCol2Constant(plus3)
	c.Assert(err, check.IsNil)
	ans3 := newFunction(ast.Plus, ans1, col1)
	c.Assert(ret.Equal(ctx, ans3), check.IsTrue)
}

func (s *testUtilSuite) TestPushDownNot(c *check.C) {
	ctx := mock.NewContext()
	col := &Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	// !((a=1||a=1)&&a=1)
	eqFunc := newFunction(ast.EQ, col, NewOne())
	orFunc := newFunction(ast.LogicOr, eqFunc, eqFunc)
	andFunc := newFunction(ast.LogicAnd, orFunc, eqFunc)
	notFunc := newFunction(ast.UnaryNot, andFunc)
	// (a!=1&&a!=1)||a=1
	neFunc := newFunction(ast.NE, col, NewOne())
	andFunc2 := newFunction(ast.LogicAnd, neFunc, neFunc)
	orFunc2 := newFunction(ast.LogicOr, andFunc2, neFunc)
	notFuncCopy := notFunc.Clone()
	ret := PushDownNot(ctx, notFunc)
	c.Assert(ret.Equal(ctx, orFunc2), check.IsTrue)
	c.Assert(notFunc.Equal(ctx, notFuncCopy), check.IsTrue)

	// issue 15725
	// (not not a) should be optimized to (a is true)
	notFunc = newFunction(ast.UnaryNot, col)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	ret = PushDownNot(ctx, notFunc)
	c.Assert(ret.Equal(ctx, newFunction(ast.IsTruth, col)), check.IsTrue)

	// (not not (a+1)) should be optimized to (a+1 is true)
	plusFunc := newFunction(ast.Plus, col, NewOne())
	notFunc = newFunction(ast.UnaryNot, plusFunc)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	ret = PushDownNot(ctx, notFunc)
	c.Assert(ret.Equal(ctx, newFunction(ast.IsTruth, plusFunc)), check.IsTrue)

	// (not not not a) should be optimized to (not (a is true))
	notFunc = newFunction(ast.UnaryNot, col)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	ret = PushDownNot(ctx, notFunc)
	c.Assert(ret.Equal(ctx, newFunction(ast.UnaryNot, newFunction(ast.IsTruth, col))), check.IsTrue)

	// (not not not not a) should be optimized to (a is true)
	notFunc = newFunction(ast.UnaryNot, col)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	ret = PushDownNot(ctx, notFunc)
	c.Assert(ret.Equal(ctx, newFunction(ast.IsTruth, col)), check.IsTrue)
}

func (s *testUtilSuite) TestFilter(c *check.C) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	result := make([]Expression, 0, 5)
	result = Filter(result, conditions, isLogicOrFunction)
	c.Assert(result, check.HasLen, 1)
}

func (s *testUtilSuite) TestFilterOutInPlace(c *check.C) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	remained, filtered := FilterOutInPlace(conditions, isLogicOrFunction)
	c.Assert(len(remained), check.Equals, 2)
	c.Assert(remained[0].(*ScalarFunction).FuncName.L, check.Equals, "eq")
	c.Assert(remained[1].(*ScalarFunction).FuncName.L, check.Equals, "eq")
	c.Assert(len(filtered), check.Equals, 1)
	c.Assert(filtered[0].(*ScalarFunction).FuncName.L, check.Equals, "or")
}

func (s *testUtilSuite) TestHashGroupKey(c *check.C) {
	ctx := mock.NewContext()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	tNames := []string{"int", "real", "decimal", "string", "timestamp", "datetime", "duration"}
	for i := 0; i < len(tNames); i++ {
		ft := eType2FieldType(eTypes[i])
		if eTypes[i] == types.ETDecimal {
			ft.Flen = 0
		}
		colExpr := &Column{Index: 0, RetType: ft}
		input := chunk.New([]*types.FieldType{ft}, 1024, 1024)
		fillColumnWithGener(eTypes[i], input, 0, nil)
		colBuf := chunk.NewColumn(ft, 1024)
		bufs := make([][]byte, 1024)
		for j := 0; j < 1024; j++ {
			bufs[j] = bufs[j][:0]
		}
		var err error
		err = EvalExpr(ctx, colExpr, input, colBuf)
		if err != nil {
			c.Fatal(err)
		}
		if bufs, err = codec.HashGroupKey(sc, 1024, colBuf, bufs, ft); err != nil {
			c.Fatal(err)
		}

		var buf []byte
		for j := 0; j < input.NumRows(); j++ {
			d, err := colExpr.Eval(input.GetRow(j))
			if err != nil {
				c.Fatal(err)
			}
			buf, err = codec.EncodeValue(sc, buf[:0], d)
			if err != nil {
				c.Fatal(err)
			}
			c.Assert(string(bufs[j]), check.Equals, string(buf))
		}
	}
}

func isLogicOrFunction(e Expression) bool {
	if f, ok := e.(*ScalarFunction); ok {
		return f.FuncName.L == ast.LogicOr
	}
	return false
}

func (s *testUtilSuite) TestDisableParseJSONFlag4Expr(c *check.C) {
	var expr Expression
	expr = &Column{RetType: newIntFieldType()}
	ft := expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsTrue)

	expr = &CorrelatedColumn{Column: Column{RetType: newIntFieldType()}}
	ft = expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsTrue)

	expr = &ScalarFunction{RetType: newIntFieldType()}
	ft = expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsFalse)
}

func BenchmarkExtractColumns(b *testing.B) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.EQ, newColumn(2), newColumn(3)),
		newFunction(ast.EQ, newColumn(3), newLonglong(1)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	expr := ComposeCNFCondition(mock.NewContext(), conditions...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractColumns(expr)
	}
	b.ReportAllocs()
}

func BenchmarkExprFromSchema(b *testing.B) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.EQ, newColumn(2), newColumn(3)),
		newFunction(ast.EQ, newColumn(3), newLonglong(1)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	expr := ComposeCNFCondition(mock.NewContext(), conditions...)
	schema := &Schema{Columns: ExtractColumns(expr)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExprFromSchema(expr, schema)
	}
	b.ReportAllocs()
}

// MockExpr is mainly for test.
type MockExpr struct {
	err error
	t   *types.FieldType
	i   interface{}
}

func (m *MockExpr) VecEvalInt(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return nil
}
func (m *MockExpr) VecEvalReal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return nil
}
func (m *MockExpr) VecEvalString(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return nil
}
func (m *MockExpr) VecEvalDecimal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return nil
}
func (m *MockExpr) VecEvalTime(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return nil
}
func (m *MockExpr) VecEvalDuration(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return nil
}
func (m *MockExpr) VecEvalJSON(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return nil
}

func (m *MockExpr) String() string                          { return "" }
func (m *MockExpr) MarshalJSON() ([]byte, error)            { return nil, nil }
func (m *MockExpr) Eval(row chunk.Row) (types.Datum, error) { return types.NewDatum(m.i), m.err }
func (m *MockExpr) EvalInt(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error) {
	if x, ok := m.i.(int64); ok {
		return x, false, m.err
	}
	return 0, m.i == nil, m.err
}
func (m *MockExpr) EvalReal(ctx sessionctx.Context, row chunk.Row) (val float64, isNull bool, err error) {
	if x, ok := m.i.(float64); ok {
		return float64(x), false, m.err
	}
	return 0, m.i == nil, m.err
}
func (m *MockExpr) EvalString(ctx sessionctx.Context, row chunk.Row) (val string, isNull bool, err error) {
	if x, ok := m.i.(string); ok {
		return x, false, m.err
	}
	return "", m.i == nil, m.err
}
func (m *MockExpr) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (val *types.MyDecimal, isNull bool, err error) {
	if x, ok := m.i.(*types.MyDecimal); ok {
		return x, false, m.err
	}
	return nil, m.i == nil, m.err
}
func (m *MockExpr) EvalTime(ctx sessionctx.Context, row chunk.Row) (val types.Time, isNull bool, err error) {
	if x, ok := m.i.(types.Time); ok {
		return x, false, m.err
	}
	return types.ZeroTime, m.i == nil, m.err
}
func (m *MockExpr) EvalDuration(ctx sessionctx.Context, row chunk.Row) (val types.Duration, isNull bool, err error) {
	if x, ok := m.i.(types.Duration); ok {
		return x, false, m.err
	}
	return types.Duration{}, m.i == nil, m.err
}
func (m *MockExpr) EvalJSON(ctx sessionctx.Context, row chunk.Row) (val json.BinaryJSON, isNull bool, err error) {
	if x, ok := m.i.(json.BinaryJSON); ok {
		return x, false, m.err
	}
	return json.BinaryJSON{}, m.i == nil, m.err
}
func (m *MockExpr) ReverseEval(sc *stmtctx.StatementContext, res types.Datum, rType types.RoundingType) (val types.Datum, err error) {
	return types.Datum{}, m.err
}
func (m *MockExpr) GetType() *types.FieldType                         { return m.t }
func (m *MockExpr) Clone() Expression                                 { return nil }
func (m *MockExpr) Equal(ctx sessionctx.Context, e Expression) bool   { return false }
func (m *MockExpr) IsCorrelated() bool                                { return false }
func (m *MockExpr) ConstItem(_ *stmtctx.StatementContext) bool        { return false }
func (m *MockExpr) Decorrelate(schema *Schema) Expression             { return m }
func (m *MockExpr) ResolveIndices(schema *Schema) (Expression, error) { return m, nil }
func (m *MockExpr) resolveIndices(schema *Schema) error               { return nil }
func (m *MockExpr) ExplainInfo() string                               { return "" }
func (m *MockExpr) ExplainNormalizedInfo() string                     { return "" }
func (m *MockExpr) HashCode(sc *stmtctx.StatementContext) []byte      { return nil }
func (m *MockExpr) Vectorized() bool                                  { return false }
func (m *MockExpr) SupportReverseEval() bool                          { return false }
func (m *MockExpr) HasCoercibility() bool                             { return false }
func (m *MockExpr) Coercibility() Coercibility                        { return 0 }
func (m *MockExpr) SetCoercibility(Coercibility)                      {}

func (m *MockExpr) CharsetAndCollation(ctx sessionctx.Context) (string, string) {
	return "", ""
}
func (m *MockExpr) SetCharsetAndCollation(chs, coll string) {}
