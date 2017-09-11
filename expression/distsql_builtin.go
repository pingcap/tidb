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
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var distFuncs = map[tipb.ExprType]string{
	// compare op
	tipb.ExprType_LT:     ast.LT,
	tipb.ExprType_LE:     ast.LE,
	tipb.ExprType_GT:     ast.GT,
	tipb.ExprType_GE:     ast.GE,
	tipb.ExprType_EQ:     ast.EQ,
	tipb.ExprType_NE:     ast.NE,
	tipb.ExprType_NullEQ: ast.NullEQ,

	// bit op
	tipb.ExprType_BitAnd:    ast.And,
	tipb.ExprType_BitOr:     ast.Or,
	tipb.ExprType_BitXor:    ast.Xor,
	tipb.ExprType_RighShift: ast.RightShift,
	tipb.ExprType_LeftShift: ast.LeftShift,
	tipb.ExprType_BitNeg:    ast.BitNeg,

	// logical op
	tipb.ExprType_And: ast.LogicAnd,
	tipb.ExprType_Or:  ast.LogicOr,
	tipb.ExprType_Xor: ast.LogicXor,
	tipb.ExprType_Not: ast.UnaryNot,

	// arithmetic operator
	tipb.ExprType_Plus:   ast.Plus,
	tipb.ExprType_Minus:  ast.Minus,
	tipb.ExprType_Mul:    ast.Mul,
	tipb.ExprType_Div:    ast.Div,
	tipb.ExprType_IntDiv: ast.IntDiv,
	tipb.ExprType_Mod:    ast.Mod,

	// control operator
	tipb.ExprType_Case:   ast.Case,
	tipb.ExprType_If:     ast.If,
	tipb.ExprType_IfNull: ast.Ifnull,
	tipb.ExprType_NullIf: ast.Nullif,

	// other operator
	tipb.ExprType_Like:     ast.Like,
	tipb.ExprType_In:       ast.In,
	tipb.ExprType_IsNull:   ast.IsNull,
	tipb.ExprType_Coalesce: ast.Coalesce,

	// for json functions.
	tipb.ExprType_JsonType:    ast.JSONType,
	tipb.ExprType_JsonExtract: ast.JSONExtract,
	tipb.ExprType_JsonUnquote: ast.JSONUnquote,
	tipb.ExprType_JsonMerge:   ast.JSONMerge,
	tipb.ExprType_JsonSet:     ast.JSONSet,
	tipb.ExprType_JsonInsert:  ast.JSONInsert,
	tipb.ExprType_JsonReplace: ast.JSONReplace,
	tipb.ExprType_JsonRemove:  ast.JSONRemove,
	tipb.ExprType_JsonArray:   ast.JSONArray,
	tipb.ExprType_JsonObject:  ast.JSONObject,
}

func pbTypeToFieldType(tp *tipb.FieldType) *types.FieldType {
	return &types.FieldType{
		Tp:      byte(tp.Tp),
		Flag:    uint(tp.Flag),
		Flen:    int(tp.Flen),
		Decimal: int(tp.Decimal),
		Charset: tp.Charset,
		Collate: mysql.Collations[uint8(tp.Collate)],
	}
}

func getSignatureByPB(ctx context.Context, sigCode tipb.ScalarFuncSig, tp *tipb.FieldType, args []Expression) (f builtinFunc, e error) {
	fieldTp := pbTypeToFieldType(tp)
	base := newBaseBuiltinFunc(args, ctx)
	base.tp = fieldTp
	switch sigCode {
	case tipb.ScalarFuncSig_CastIntAsInt:
		f = &builtinCastIntAsIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastRealAsInt:
		f = &builtinCastRealAsIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDecimalAsInt:
		f = &builtinCastDecimalAsIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDurationAsInt:
		f = &builtinCastDurationAsIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastTimeAsInt:
		f = &builtinCastTimeAsIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastStringAsInt:
		f = &builtinCastStringAsIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastJsonAsInt:
		f = &builtinCastJSONAsIntSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_CastIntAsReal:
		f = &builtinCastIntAsRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastRealAsReal:
		f = &builtinCastRealAsRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDecimalAsReal:
		f = &builtinCastDecimalAsRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDurationAsReal:
		f = &builtinCastDurationAsRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastTimeAsReal:
		f = &builtinCastTimeAsRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastStringAsReal:
		f = &builtinCastStringAsRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastJsonAsReal:
		f = &builtinCastJSONAsRealSig{baseRealBuiltinFunc{base}}

	case tipb.ScalarFuncSig_CastIntAsDecimal:
		f = &builtinCastIntAsDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastRealAsDecimal:
		f = &builtinCastRealAsDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDecimalAsDecimal:
		f = &builtinCastDecimalAsDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDurationAsDecimal:
		f = &builtinCastDurationAsDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastTimeAsDecimal:
		f = &builtinCastTimeAsDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastStringAsDecimal:
		f = &builtinCastStringAsDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastJsonAsDecimal:
		f = &builtinCastJSONAsDecimalSig{baseDecimalBuiltinFunc{base}}

	case tipb.ScalarFuncSig_CastIntAsTime:
		f = &builtinCastIntAsTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastRealAsTime:
		f = &builtinCastRealAsTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDecimalAsTime:
		f = &builtinCastDecimalAsTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDurationAsTime:
		f = &builtinCastDurationAsTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastTimeAsTime:
		f = &builtinCastTimeAsTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastStringAsTime:
		f = &builtinCastStringAsTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastJsonAsTime:
		f = &builtinCastJSONAsTimeSig{baseTimeBuiltinFunc{base}}

	case tipb.ScalarFuncSig_CastIntAsString:
		f = &builtinCastIntAsStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastRealAsString:
		f = &builtinCastRealAsStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDecimalAsString:
		f = &builtinCastDecimalAsStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDurationAsString:
		f = &builtinCastDurationAsStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastTimeAsString:
		f = &builtinCastTimeAsStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastStringAsString:
		f = &builtinCastStringAsStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastJsonAsString:
		f = &builtinCastJSONAsStringSig{baseStringBuiltinFunc{base}}

	case tipb.ScalarFuncSig_CastIntAsDuration:
		f = &builtinCastIntAsDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastRealAsDuration:
		f = &builtinCastRealAsDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDecimalAsDuration:
		f = &builtinCastDecimalAsDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDurationAsDuration:
		f = &builtinCastDurationAsDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastTimeAsDuration:
		f = &builtinCastTimeAsDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastStringAsDuration:
		f = &builtinCastStringAsDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastJsonAsDuration:
		f = &builtinCastJSONAsDurationSig{baseDurationBuiltinFunc{base}}

	case tipb.ScalarFuncSig_CastIntAsJson:
		f = &builtinCastIntAsJSONSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastRealAsJson:
		f = &builtinCastRealAsJSONSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDecimalAsJson:
		f = &builtinCastDecimalAsJSONSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastTimeAsJson:
		f = &builtinCastTimeAsJSONSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastDurationAsJson:
		f = &builtinCastDurationAsJSONSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastStringAsJson:
		f = &builtinCastStringAsJSONSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CastJsonAsJson:
		f = &builtinCastJSONAsJSONSig{baseJSONBuiltinFunc{base}}

	case tipb.ScalarFuncSig_GTInt:
		f = &builtinGTIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_GEInt:
		f = &builtinGEIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LTInt:
		f = &builtinLTIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LEInt:
		f = &builtinLEIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_EQInt:
		f = &builtinEQIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NEInt:
		f = &builtinNEIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NullEQInt:
		f = &builtinNullEQIntSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_GTReal:
		f = &builtinGTRealSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_GEReal:
		f = &builtinGERealSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LTReal:
		f = &builtinLTRealSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LEReal:
		f = &builtinLERealSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_EQReal:
		f = &builtinEQRealSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NEReal:
		f = &builtinNERealSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NullEQReal:
		f = &builtinNullEQRealSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_GTDecimal:
		f = &builtinGTDecimalSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_GEDecimal:
		f = &builtinGEDecimalSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LTDecimal:
		f = &builtinLTDecimalSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LEDecimal:
		f = &builtinLEDecimalSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_EQDecimal:
		f = &builtinEQDecimalSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NEDecimal:
		f = &builtinNEDecimalSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NullEQDecimal:
		f = &builtinNullEQDecimalSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_GTTime:
		f = &builtinGTTimeSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_GETime:
		f = &builtinGETimeSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LTTime:
		f = &builtinLTTimeSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LETime:
		f = &builtinLETimeSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_EQTime:
		f = &builtinEQTimeSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NETime:
		f = &builtinNETimeSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NullEQTime:
		f = &builtinNullEQTimeSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_GTDuration:
		f = &builtinGTDurationSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_GEDuration:
		f = &builtinGEDurationSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LTDuration:
		f = &builtinLTDurationSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LEDuration:
		f = &builtinLEDurationSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_EQDuration:
		f = &builtinEQDurationSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NEDuration:
		f = &builtinNEDurationSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NullEQDuration:
		f = &builtinNullEQDurationSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_GTString:
		f = &builtinGTStringSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_GEString:
		f = &builtinGEStringSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LTString:
		f = &builtinLTStringSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LEString:
		f = &builtinLEStringSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_EQString:
		f = &builtinEQStringSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NEString:
		f = &builtinNEStringSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NullEQString:
		f = &builtinNullEQStringSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_GTJson:
		f = &builtinGTJSONSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_GEJson:
		f = &builtinGEJSONSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LTJson:
		f = &builtinLTJSONSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LEJson:
		f = &builtinLEJSONSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_EQJson:
		f = &builtinEQJSONSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NEJson:
		f = &builtinNEJSONSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_NullEQJson:
		f = &builtinNullEQJSONSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_PlusInt:
		f = &builtinArithmeticPlusIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_PlusDecimal:
		f = &builtinArithmeticPlusDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_PlusReal:
		f = &builtinArithmeticPlusRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_MinusInt:
		f = &builtinArithmeticMinusIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_MinusDecimal:
		f = &builtinArithmeticMinusDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_MinusReal:
		f = &builtinArithmeticMinusRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_MultiplyInt:
		f = &builtinArithmeticMultiplyIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_MultiplyDecimal:
		f = &builtinArithmeticMultiplyDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_MultiplyReal:
		f = &builtinArithmeticMultiplyRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_DivideDecimal:
		f = &builtinArithmeticDivideDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_DivideReal:
		f = &builtinArithmeticDivideRealSig{baseRealBuiltinFunc{base}}

	case tipb.ScalarFuncSig_AbsInt:
		f = &builtinAbsIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_AbsReal:
		f = &builtinAbsRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_AbsDecimal:
		f = &builtinAbsDecSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CeilIntToInt:
		f = &builtinCeilIntToIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CeilIntToDec:
		f = &builtinCeilIntToDecSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CeilDecToInt:
		f = &builtinCeilDecToIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CeilReal:
		f = &builtinCeilRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_FloorIntToInt:
		f = &builtinFloorIntToIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_FloorIntToDec:
		f = &builtinFloorIntToDecSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_FloorDecToInt:
		f = &builtinFloorDecToIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_FloorReal:
		f = &builtinFloorRealSig{baseRealBuiltinFunc{base}}

	case tipb.ScalarFuncSig_LogicalAnd:
		f = &builtinLogicAndSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LogicalOr:
		f = &builtinLogicOrSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_LogicalXor:
		f = &builtinLogicXorSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_BitAndSig:
		f = &builtinBitAndSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_BitOrSig:
		f = &builtinBitOrSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_BitXorSig:
		f = &builtinBitXorSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_BitNegSig:
		f = &builtinBitNegSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_UnaryNot:
		f = &builtinUnaryNotSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_UnaryMinusInt:
		f = &builtinUnaryMinusIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_UnaryMinusReal:
		f = &builtinUnaryMinusRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_UnaryMinusDecimal:
		f = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{base}, false}

	case tipb.ScalarFuncSig_DecimalIsNull:
		f = &builtinDecimalIsNullSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_DurationIsNull:
		f = &builtinDurationIsNullSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_RealIsNull:
		f = &builtinRealIsNullSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_TimeIsNull:
		f = &builtinTimeIsNullSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_StringIsNull:
		f = &builtinStringIsNullSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IntIsNull:
		f = &builtinIntIsNullSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_CoalesceDecimal:
		f = &builtinCoalesceDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CoalesceDuration:
		f = &builtinCoalesceDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CoalesceReal:
		f = &builtinCoalesceRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CoalesceTime:
		f = &builtinCoalesceTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CoalesceString:
		f = &builtinCoalesceStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CoalesceInt:
		f = &builtinCoalesceIntSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_CaseWhenDecimal:
		f = &builtinCaseWhenDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CaseWhenDuration:
		f = &builtinCaseWhenDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CaseWhenReal:
		f = &builtinCaseWhenRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CaseWhenTime:
		f = &builtinCaseWhenTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CaseWhenString:
		f = &builtinCaseWhenStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_CaseWhenInt:
		f = &builtinCaseWhenIntSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_IntIsFalse:
		f = &builtinIntIsFalseSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_RealIsFalse:
		f = &builtinRealIsFalseSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_DecimalIsFalse:
		f = &builtinDecimalIsFalseSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IntIsTrue:
		f = &builtinIntIsTrueSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_RealIsTrue:
		f = &builtinRealIsTrueSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_DecimalIsTrue:
		f = &builtinDecimalIsTrueSig{baseIntBuiltinFunc{base}}

	case tipb.ScalarFuncSig_IfNullReal:
		f = &builtinIfNullRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfNullInt:
		f = &builtinIfNullIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfNullDecimal:
		f = &builtinIfNullDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfNullString:
		f = &builtinIfNullStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfNullTime:
		f = &builtinIfNullTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfNullDuration:
		f = &builtinIfNullTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfNullJson:
		f = &builtinIfNullJSONSig{baseJSONBuiltinFunc{base}}


	case tipb.ScalarFuncSig_IfReal:
		f = &builtinIfRealSig{baseRealBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfInt:
		f = &builtinIfIntSig{baseIntBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfDecimal:
		f = &builtinIfDecimalSig{baseDecimalBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfString:
		f = &builtinIfStringSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfTime:
		f = &builtinIfTimeSig{baseTimeBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfDuration:
		f = &builtinIfDurationSig{baseDurationBuiltinFunc{base}}
	case tipb.ScalarFuncSig_IfJson:
		f = &builtinIfJSONSig{baseJSONBuiltinFunc{base}}

	case tipb.ScalarFuncSig_JsonTypeSig:
		f = &builtinJSONTypeSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonUnquoteSig:
		f = &builtinJSONUnquoteSig{baseStringBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonArraySig:
		f = &builtinJSONArraySig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonObjectSig:
		f = &builtinJSONObjectSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonExtractSig:
		f = &builtinJSONExtractSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonSetSig:
		f = &builtinJSONSetSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonInsertSig:
		f = &builtinJSONInsertSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonReplaceSig:
		f = &builtinJSONReplaceSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonRemoveSig:
		f = &builtinJSONRemoveSig{baseJSONBuiltinFunc{base}}
	case tipb.ScalarFuncSig_JsonMergeSig:
		f = &builtinJSONMergeSig{baseJSONBuiltinFunc{base}}

	case tipb.ScalarFuncSig_LikeSig:
		f = &builtinLikeSig{baseIntBuiltinFunc{base}}

	default:
		e = errFunctionNotExists.GenByArgs(sigCode)
		return nil, errors.Trace(e)
	}
	f.setSelf(f)
	return f, nil
}

func newDistSQLFunctionBySig(sc *variable.StatementContext, sigCode tipb.ScalarFuncSig, tp *tipb.FieldType, args []Expression) (Expression, error) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = sc
	f, err := getSignatureByPB(ctx, sigCode, tp, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ScalarFunction{
		FuncName: model.NewCIStr(fmt.Sprintf("sig_%T", f)),
		Function: f,
		RetType:  f.getRetTp(),
	}, nil
}

// newDistSQLFunction only creates function for mock-tikv.
func newDistSQLFunction(sc *variable.StatementContext, exprType tipb.ExprType, args []Expression) (Expression, error) {
	name, ok := distFuncs[exprType]
	if !ok {
		return nil, errFunctionNotExists.GenByArgs(exprType)
	}
	// TODO: Too ugly...
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = sc
	return NewFunction(ctx, name, types.NewFieldType(mysql.TypeUnspecified), args...)
}

// PBToExpr converts pb structure to expression.
func PBToExpr(expr *tipb.Expr, tps []*types.FieldType, sc *variable.StatementContext) (Expression, error) {
	switch expr.Tp {
	case tipb.ExprType_ColumnRef:
		_, offset, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &Column{Index: int(offset), RetType: tps[offset]}, nil
	case tipb.ExprType_Null:
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}, nil
	case tipb.ExprType_Int64:
		return convertInt(expr.Val)
	case tipb.ExprType_Uint64:
		return convertUint(expr.Val)
	case tipb.ExprType_String:
		return convertString(expr.Val)
	case tipb.ExprType_Bytes:
		return &Constant{Value: types.NewBytesDatum(expr.Val), RetType: types.NewFieldType(mysql.TypeString)}, nil
	case tipb.ExprType_Float32:
		return convertFloat(expr.Val, true)
	case tipb.ExprType_Float64:
		return convertFloat(expr.Val, false)
	case tipb.ExprType_MysqlDecimal:
		return convertDecimal(expr.Val)
	case tipb.ExprType_MysqlDuration:
		return convertDuration(expr.Val)
	case tipb.ExprType_MysqlTime:
		return convertTime(expr.Val, expr.FieldType, sc.TimeZone)
	}
	// Then it must be a scalar function.
	args := make([]Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		if child.Tp == tipb.ExprType_ValueList {
			results, err := decodeValueList(child.Val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(results) == 0 {
				return &Constant{Value: types.NewDatum(false), RetType: types.NewFieldType(mysql.TypeLonglong)}, nil
			}
			args = append(args, results...)
			continue
		}
		arg, err := PBToExpr(child, tps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		args = append(args, arg)
	}
	if expr.Tp == tipb.ExprType_ScalarFunc {
		return newDistSQLFunctionBySig(sc, expr.Sig, expr.FieldType, args)
	}
	return newDistSQLFunction(sc, expr.Tp, args)
}

func fieldTypeFromPB(ft *tipb.FieldType) *types.FieldType {
	return &types.FieldType{
		Tp:      byte(ft.GetTp()),
		Flag:    uint(ft.GetFlag()),
		Flen:    int(ft.GetFlen()),
		Decimal: int(ft.GetDecimal()),
		Collate: mysql.Collations[uint8(ft.GetCollate())],
	}
}

func convertTime(data []byte, ftPB *tipb.FieldType, tz *time.Location) (*Constant, error) {
	ft := fieldTypeFromPB(ftPB)
	_, v, err := codec.DecodeUint(data)
	if err != nil {
		return nil, errors.Trace(nil)
	}
	var t types.Time
	t.Type = ft.Tp
	t.Fsp = ft.Decimal
	err = t.FromPackedUint(v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ft.Tp == mysql.TypeTimestamp && !t.IsZero() {
		t.ConvertTimeZone(time.UTC, tz)
	}
	return &Constant{Value: types.NewTimeDatum(t), RetType: ft}, nil
}

func decodeValueList(data []byte) ([]Expression, error) {
	if len(data) == 0 {
		return nil, nil
	}
	list, err := codec.Decode(data, 1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result := make([]Expression, 0, len(list))
	for _, value := range list {
		result = append(result, &Constant{Value: value})
	}
	return result, nil
}

func convertInt(val []byte) (*Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid int % x", val)
	}
	d.SetInt64(i)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeLonglong)}, nil
}

func convertUint(val []byte) (*Constant, error) {
	var d types.Datum
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid uint % x", val)
	}
	d.SetUint64(u)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeLonglong)}, nil
}

func convertString(val []byte) (*Constant, error) {
	var d types.Datum
	d.SetBytesAsString(val)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeVarString)}, nil
}

func convertFloat(val []byte, f32 bool) (*Constant, error) {
	var d types.Datum
	_, f, err := codec.DecodeFloat(val)
	if err != nil {
		return nil, errors.Errorf("invalid float % x", val)
	}
	if f32 {
		d.SetFloat32(float32(f))
	} else {
		d.SetFloat64(f)
	}
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeDouble)}, nil
}

func convertDecimal(val []byte) (*Constant, error) {
	_, dec, err := codec.DecodeDecimal(val)
	if err != nil {
		return nil, errors.Errorf("invalid decimal % x", val)
	}
	return &Constant{Value: dec, RetType: types.NewFieldType(mysql.TypeNewDecimal)}, nil
}

func convertDuration(val []byte) (*Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid duration %d", i)
	}
	d.SetMysqlDuration(types.Duration{Duration: time.Duration(i), Fsp: types.MaxFsp})
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeDuration)}, nil
}
