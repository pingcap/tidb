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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

// PbTypeToFieldType converts tipb.FieldType to FieldType
func PbTypeToFieldType(tp *tipb.FieldType) *types.FieldType {
	ftb := types.NewFieldTypeBuilder()
	ft := ftb.SetType(byte(tp.Tp)).SetFlag(uint(tp.Flag)).SetFlen(int(tp.Flen)).SetDecimal(int(tp.Decimal)).SetCharset(tp.Charset).SetCollate(collate.ProtoToCollation(tp.Collate)).BuildP()
	ft.SetElems(tp.Elems)
	return ft
}

func getSignatureByPB(ctx BuildContext, sigCode tipb.ScalarFuncSig, tp *tipb.FieldType, args []Expression) (f builtinFunc, e error) {
	fieldTp := PbTypeToFieldType(tp)
	base, err := newBaseBuiltinFuncWithFieldType(fieldTp, args)
	if err != nil {
		return nil, err
	}
	maxAllowedPacket := ctx.GetMaxAllowedPacket()
	switch sigCode {
	case tipb.ScalarFuncSig_CastIntAsInt:
		f = &builtinCastIntAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastIntAsReal:
		f = &builtinCastIntAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastIntAsString:
		f = &builtinCastIntAsStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastIntAsDecimal:
		f = &builtinCastIntAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastIntAsTime:
		f = &builtinCastIntAsTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastIntAsDuration:
		f = &builtinCastIntAsDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastIntAsJson:
		f = &builtinCastIntAsJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastRealAsInt:
		f = &builtinCastRealAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastRealAsReal:
		f = &builtinCastRealAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastRealAsString:
		f = &builtinCastRealAsStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastRealAsDecimal:
		f = &builtinCastRealAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastRealAsTime:
		f = &builtinCastRealAsTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastRealAsDuration:
		f = &builtinCastRealAsDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastRealAsJson:
		f = &builtinCastRealAsJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastDecimalAsInt:
		f = &builtinCastDecimalAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDecimalAsReal:
		f = &builtinCastDecimalAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDecimalAsString:
		f = &builtinCastDecimalAsStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastDecimalAsDecimal:
		f = &builtinCastDecimalAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDecimalAsTime:
		f = &builtinCastDecimalAsTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastDecimalAsDuration:
		f = &builtinCastDecimalAsDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastDecimalAsJson:
		f = &builtinCastDecimalAsJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastStringAsInt:
		f = &builtinCastStringAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastStringAsReal:
		f = &builtinCastStringAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastStringAsString:
		f = &builtinCastStringAsStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastStringAsDecimal:
		f = &builtinCastStringAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastStringAsTime:
		f = &builtinCastStringAsTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastStringAsDuration:
		f = &builtinCastStringAsDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastStringAsJson:
		f = &builtinCastStringAsJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastTimeAsInt:
		f = &builtinCastTimeAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastTimeAsReal:
		f = &builtinCastTimeAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastTimeAsString:
		f = &builtinCastTimeAsStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastTimeAsDecimal:
		f = &builtinCastTimeAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastTimeAsTime:
		f = &builtinCastTimeAsTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastTimeAsDuration:
		f = &builtinCastTimeAsDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastTimeAsJson:
		f = &builtinCastTimeAsJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastDurationAsInt:
		f = &builtinCastDurationAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDurationAsReal:
		f = &builtinCastDurationAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDurationAsString:
		f = &builtinCastDurationAsStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastDurationAsDecimal:
		f = &builtinCastDurationAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDurationAsTime:
		f = &builtinCastDurationAsTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastDurationAsDuration:
		f = &builtinCastDurationAsDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastDurationAsJson:
		f = &builtinCastDurationAsJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastJsonAsInt:
		f = &builtinCastJSONAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastJsonAsReal:
		f = &builtinCastJSONAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastJsonAsString:
		f = &builtinCastJSONAsStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastJsonAsDecimal:
		f = &builtinCastJSONAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastJsonAsTime:
		f = &builtinCastJSONAsTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastJsonAsDuration:
		f = &builtinCastJSONAsDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CastJsonAsJson:
		f = &builtinCastJSONAsJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CoalesceInt:
		f = &builtinCoalesceIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CoalesceReal:
		f = &builtinCoalesceRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CoalesceDecimal:
		f = &builtinCoalesceDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CoalesceString:
		f = &builtinCoalesceStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CoalesceTime:
		f = &builtinCoalesceTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CoalesceDuration:
		f = &builtinCoalesceDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CoalesceJson:
		f = &builtinCoalesceJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LTInt:
		f = &builtinLTIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LTReal:
		f = &builtinLTRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LTDecimal:
		f = &builtinLTDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LTString:
		f = &builtinLTStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LTTime:
		f = &builtinLTTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LTDuration:
		f = &builtinLTDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LTJson:
		f = &builtinLTJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LEInt:
		f = &builtinLEIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LEReal:
		f = &builtinLERealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LEDecimal:
		f = &builtinLEDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LEString:
		f = &builtinLEStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LETime:
		f = &builtinLETimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LEDuration:
		f = &builtinLEDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LEJson:
		f = &builtinLEJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GTInt:
		f = &builtinGTIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GTReal:
		f = &builtinGTRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GTDecimal:
		f = &builtinGTDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GTString:
		f = &builtinGTStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GTTime:
		f = &builtinGTTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GTDuration:
		f = &builtinGTDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GTJson:
		f = &builtinGTJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GreatestInt:
		f = &builtinGreatestIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GreatestReal:
		f = &builtinGreatestRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GreatestDecimal:
		f = &builtinGreatestDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GreatestString:
		f = &builtinGreatestStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GreatestTime:
		f = &builtinGreatestTimeSig{baseBuiltinFunc: base, cmpAsDate: false}
	case tipb.ScalarFuncSig_GreatestDate:
		f = &builtinGreatestTimeSig{baseBuiltinFunc: base, cmpAsDate: true}
	case tipb.ScalarFuncSig_GreatestCmpStringAsTime:
		f = &builtinGreatestCmpStringAsTimeSig{baseBuiltinFunc: base, cmpAsDate: false}
	case tipb.ScalarFuncSig_GreatestCmpStringAsDate:
		f = &builtinGreatestCmpStringAsTimeSig{baseBuiltinFunc: base, cmpAsDate: true}
	case tipb.ScalarFuncSig_GreatestDuration:
		f = &builtinGreatestDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LeastInt:
		f = &builtinLeastIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LeastReal:
		f = &builtinLeastRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LeastDecimal:
		f = &builtinLeastDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LeastString:
		f = &builtinLeastStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LeastTime:
		f = &builtinLeastTimeSig{baseBuiltinFunc: base, cmpAsDate: false}
	case tipb.ScalarFuncSig_LeastDate:
		f = &builtinLeastTimeSig{baseBuiltinFunc: base, cmpAsDate: true}
	case tipb.ScalarFuncSig_LeastCmpStringAsTime:
		f = &builtinLeastCmpStringAsTimeSig{baseBuiltinFunc: base, cmpAsDate: false}
	case tipb.ScalarFuncSig_LeastCmpStringAsDate:
		f = &builtinLeastCmpStringAsTimeSig{baseBuiltinFunc: base, cmpAsDate: true}
	case tipb.ScalarFuncSig_LeastDuration:
		f = &builtinLeastDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IntervalInt:
		f = &builtinIntervalIntSig{baseBuiltinFunc: base, hasNullable: false} // Since interval function won't be pushed down to TiKV, therefore it doesn't matter what value we give to hasNullable
	case tipb.ScalarFuncSig_IntervalReal:
		f = &builtinIntervalRealSig{baseBuiltinFunc: base, hasNullable: false}
	case tipb.ScalarFuncSig_GEInt:
		f = &builtinGEIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GEReal:
		f = &builtinGERealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GEDecimal:
		f = &builtinGEDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GEString:
		f = &builtinGEStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GETime:
		f = &builtinGETimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GEDuration:
		f = &builtinGEDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GEJson:
		f = &builtinGEJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_EQInt:
		f = &builtinEQIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_EQReal:
		f = &builtinEQRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_EQDecimal:
		f = &builtinEQDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_EQString:
		f = &builtinEQStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_EQTime:
		f = &builtinEQTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_EQDuration:
		f = &builtinEQDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_EQJson:
		f = &builtinEQJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NEInt:
		f = &builtinNEIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NEReal:
		f = &builtinNERealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NEDecimal:
		f = &builtinNEDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NEString:
		f = &builtinNEStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NETime:
		f = &builtinNETimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NEDuration:
		f = &builtinNEDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NEJson:
		f = &builtinNEJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NullEQInt:
		f = &builtinNullEQIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NullEQReal:
		f = &builtinNullEQRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NullEQDecimal:
		f = &builtinNullEQDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NullEQString:
		f = &builtinNullEQStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NullEQTime:
		f = &builtinNullEQTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NullEQDuration:
		f = &builtinNullEQDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NullEQJson:
		f = &builtinNullEQJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_PlusReal:
		f = &builtinArithmeticPlusRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_PlusDecimal:
		f = &builtinArithmeticPlusDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_PlusInt:
		f = &builtinArithmeticPlusIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MinusReal:
		f = &builtinArithmeticMinusRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MinusDecimal:
		f = &builtinArithmeticMinusDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MinusInt:
		f = &builtinArithmeticMinusIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MultiplyReal:
		f = &builtinArithmeticMultiplyRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MultiplyDecimal:
		f = &builtinArithmeticMultiplyDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MultiplyInt:
		f = &builtinArithmeticMultiplyIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DivideReal:
		f = &builtinArithmeticDivideRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DivideDecimal:
		f = &builtinArithmeticDivideDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IntDivideInt:
		f = &builtinArithmeticIntDivideIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IntDivideDecimal:
		f = &builtinArithmeticIntDivideDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ModReal:
		f = &builtinArithmeticModRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ModDecimal:
		f = &builtinArithmeticModDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ModIntUnsignedUnsigned:
		f = &builtinArithmeticModIntUnsignedUnsignedSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ModIntUnsignedSigned:
		f = &builtinArithmeticModIntUnsignedSignedSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ModIntSignedUnsigned:
		f = &builtinArithmeticModIntSignedUnsignedSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ModIntSignedSigned:
		f = &builtinArithmeticModIntSignedSignedSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MultiplyIntUnsigned:
		f = &builtinArithmeticMultiplyIntUnsignedSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AbsInt:
		f = &builtinAbsIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AbsUInt:
		f = &builtinAbsUIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AbsReal:
		f = &builtinAbsRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AbsDecimal:
		f = &builtinAbsDecSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CeilIntToDec:
		f = &builtinCeilIntToDecSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CeilIntToInt:
		f = &builtinCeilIntToIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CeilDecToInt:
		f = &builtinCeilDecToIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CeilDecToDec:
		f = &builtinCeilDecToDecSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CeilReal:
		f = &builtinCeilRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FloorIntToDec:
		f = &builtinFloorIntToDecSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FloorIntToInt:
		f = &builtinFloorIntToIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FloorDecToInt:
		f = &builtinFloorDecToIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FloorDecToDec:
		f = &builtinFloorDecToDecSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FloorReal:
		f = &builtinFloorRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RoundReal:
		f = &builtinRoundRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RoundInt:
		f = &builtinRoundIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RoundDec:
		f = &builtinRoundDecSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RoundWithFracReal:
		f = &builtinRoundWithFracRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RoundWithFracInt:
		f = &builtinRoundWithFracIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RoundWithFracDec:
		f = &builtinRoundWithFracDecSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Log1Arg:
		f = &builtinLog1ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Log2Args:
		f = &builtinLog2ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Log2:
		f = &builtinLog2Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Log10:
		f = &builtinLog10Sig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_Rand:
	case tipb.ScalarFuncSig_RandWithSeedFirstGen:
		f = &builtinRandWithSeedFirstGenSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Pow:
		f = &builtinPowSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Conv:
		f = &builtinConvSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CRC32:
		f = &builtinCRC32Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Sign:
		f = &builtinSignSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Sqrt:
		f = &builtinSqrtSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Acos:
		f = &builtinAcosSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Asin:
		f = &builtinAsinSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Atan1Arg:
		f = &builtinAtan1ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Atan2Args:
		f = &builtinAtan2ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Cos:
		f = &builtinCosSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Cot:
		f = &builtinCotSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Degrees:
		f = &builtinDegreesSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Exp:
		f = &builtinExpSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_PI:
		f = &builtinPISig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Radians:
		f = &builtinRadiansSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Sin:
		f = &builtinSinSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Tan:
		f = &builtinTanSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TruncateInt:
		f = &builtinTruncateIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TruncateReal:
		f = &builtinTruncateRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TruncateDecimal:
		f = &builtinTruncateDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TruncateUint:
		f = &builtinTruncateUintSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LogicalAnd:
		f = &builtinLogicAndSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LogicalOr:
		f = &builtinLogicOrSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LogicalXor:
		f = &builtinLogicXorSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnaryNotInt:
		f = &builtinUnaryNotIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnaryNotDecimal:
		f = &builtinUnaryNotDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnaryNotReal:
		f = &builtinUnaryNotRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnaryMinusInt:
		f = &builtinUnaryMinusIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnaryMinusReal:
		f = &builtinUnaryMinusRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnaryMinusDecimal:
		f = &builtinUnaryMinusDecimalSig{baseBuiltinFunc: base, constantArgOverflow: false}
	case tipb.ScalarFuncSig_DecimalIsNull:
		f = &builtinDecimalIsNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DurationIsNull:
		f = &builtinDurationIsNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RealIsNull:
		f = &builtinRealIsNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_StringIsNull:
		f = &builtinStringIsNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TimeIsNull:
		f = &builtinTimeIsNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IntIsNull:
		f = &builtinIntIsNullSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_JsonIsNull:
	case tipb.ScalarFuncSig_BitAndSig:
		f = &builtinBitAndSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_BitOrSig:
		f = &builtinBitOrSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_BitXorSig:
		f = &builtinBitXorSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_BitNegSig:
		f = &builtinBitNegSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IntIsTrue:
		f = &builtinIntIsTrueSig{baseBuiltinFunc: base, keepNull: false}
	case tipb.ScalarFuncSig_RealIsTrue:
		f = &builtinRealIsTrueSig{baseBuiltinFunc: base, keepNull: false}
	case tipb.ScalarFuncSig_DecimalIsTrue:
		f = &builtinDecimalIsTrueSig{baseBuiltinFunc: base, keepNull: false}
	case tipb.ScalarFuncSig_IntIsFalse:
		f = &builtinIntIsFalseSig{baseBuiltinFunc: base, keepNull: false}
	case tipb.ScalarFuncSig_RealIsFalse:
		f = &builtinRealIsFalseSig{baseBuiltinFunc: base, keepNull: false}
	case tipb.ScalarFuncSig_DecimalIsFalse:
		f = &builtinDecimalIsFalseSig{baseBuiltinFunc: base, keepNull: false}
	case tipb.ScalarFuncSig_IntIsTrueWithNull:
		f = &builtinIntIsTrueSig{baseBuiltinFunc: base, keepNull: true}
	case tipb.ScalarFuncSig_RealIsTrueWithNull:
		f = &builtinRealIsTrueSig{baseBuiltinFunc: base, keepNull: true}
	case tipb.ScalarFuncSig_DecimalIsTrueWithNull:
		f = &builtinDecimalIsTrueSig{baseBuiltinFunc: base, keepNull: true}
	case tipb.ScalarFuncSig_IntIsFalseWithNull:
		f = &builtinIntIsFalseSig{baseBuiltinFunc: base, keepNull: true}
	case tipb.ScalarFuncSig_RealIsFalseWithNull:
		f = &builtinRealIsFalseSig{baseBuiltinFunc: base, keepNull: true}
	case tipb.ScalarFuncSig_DecimalIsFalseWithNull:
		f = &builtinDecimalIsFalseSig{baseBuiltinFunc: base, keepNull: true}
	case tipb.ScalarFuncSig_LeftShift:
		f = &builtinLeftShiftSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RightShift:
		f = &builtinRightShiftSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_BitCount:
		f = &builtinBitCountSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GetParamString:
		f = &builtinGetParamStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GetVar:
		f = &builtinGetStringVarSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_RowSig:
	case tipb.ScalarFuncSig_SetVar:
		f = &builtinSetStringVarSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_ValuesDecimal:
	// 	f = &builtinValuesDecimalSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_ValuesDuration:
	// 	f = &builtinValuesDurationSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_ValuesInt:
	// 	f = &builtinValuesIntSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_ValuesJSON:
	// 	f = &builtinValuesJSONSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_ValuesReal:
	// 	f = &builtinValuesRealSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_ValuesString:
	// 	f = &builtinValuesStringSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_ValuesTime:
	// 	f = &builtinValuesTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_InInt:
		f = &builtinInIntSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_InReal:
		f = &builtinInRealSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_InDecimal:
		f = &builtinInDecimalSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_InString:
		f = &builtinInStringSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_InTime:
		f = &builtinInTimeSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_InDuration:
		f = &builtinInDurationSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_InJson:
		f = &builtinInJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfNullInt:
		f = &builtinIfNullIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfNullReal:
		f = &builtinIfNullRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfNullDecimal:
		f = &builtinIfNullDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfNullString:
		f = &builtinIfNullStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfNullTime:
		f = &builtinIfNullTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfNullDuration:
		f = &builtinIfNullDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfInt:
		f = &builtinIfIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfReal:
		f = &builtinIfRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfDecimal:
		f = &builtinIfDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfString:
		f = &builtinIfStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfTime:
		f = &builtinIfTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfDuration:
		f = &builtinIfDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfNullJson:
		f = &builtinIfNullJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IfJson:
		f = &builtinIfJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CaseWhenInt:
		f = &builtinCaseWhenIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CaseWhenReal:
		f = &builtinCaseWhenRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CaseWhenDecimal:
		f = &builtinCaseWhenDecimalSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CaseWhenString:
		f = &builtinCaseWhenStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CaseWhenTime:
		f = &builtinCaseWhenTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CaseWhenDuration:
		f = &builtinCaseWhenDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CaseWhenJson:
		f = &builtinCaseWhenJSONSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AesDecrypt:
	// 	f = &builtinAesDecryptSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AesEncrypt:
	// 	f = &builtinAesEncryptSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Compress:
		f = &builtinCompressSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MD5:
		f = &builtinMD5Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Password:
		f = &builtinPasswordSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RandomBytes:
		f = &builtinRandomBytesSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SHA1:
		f = &builtinSHA1Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SHA2:
		f = &builtinSHA2Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Uncompress:
		f = &builtinUncompressSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UncompressedLength:
		f = &builtinUncompressedLengthSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Database:
		f = &builtinDatabaseSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FoundRows:
		f = &builtinFoundRowsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CurrentUser:
		f = &builtinCurrentUserSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_User:
		f = &builtinUserSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ConnectionID:
		f = &builtinConnectionIDSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LastInsertID:
		f = &builtinLastInsertIDSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LastInsertIDWithID:
		f = &builtinLastInsertIDWithIDSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Version:
		f = &builtinVersionSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TiDBVersion:
		f = &builtinTiDBVersionSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RowCount:
		f = &builtinRowCountSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Sleep:
		f = &builtinSleepSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Lock:
		f = &builtinLockSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ReleaseLock:
		f = &builtinReleaseLockSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DecimalAnyValue:
		f = &builtinDecimalAnyValueSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DurationAnyValue:
		f = &builtinDurationAnyValueSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IntAnyValue:
		f = &builtinIntAnyValueSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JSONAnyValue:
		f = &builtinJSONAnyValueSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RealAnyValue:
		f = &builtinRealAnyValueSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_StringAnyValue:
		f = &builtinStringAnyValueSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TimeAnyValue:
		f = &builtinTimeAnyValueSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_InetAton:
		f = &builtinInetAtonSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_InetNtoa:
		f = &builtinInetNtoaSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Inet6Aton:
		f = &builtinInet6AtonSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Inet6Ntoa:
		f = &builtinInet6NtoaSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IsIPv4:
		f = &builtinIsIPv4Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IsIPv4Compat:
		f = &builtinIsIPv4CompatSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IsIPv4Mapped:
		f = &builtinIsIPv4MappedSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IsIPv6:
		f = &builtinIsIPv6Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UUID:
		f = &builtinUUIDSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LikeSig:
		f = &builtinLikeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_IlikeSig:
		f = &builtinIlikeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RegexpSig:
		f = &builtinRegexpLikeFuncSig{regexpBaseFuncSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_RegexpUTF8Sig:
		f = &builtinRegexpLikeFuncSig{regexpBaseFuncSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_RegexpLikeSig:
		f = &builtinRegexpLikeFuncSig{regexpBaseFuncSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_RegexpSubstrSig:
		f = &builtinRegexpSubstrFuncSig{regexpBaseFuncSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_RegexpInStrSig:
		f = &builtinRegexpInStrFuncSig{regexpBaseFuncSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_RegexpReplaceSig:
		f = &builtinRegexpReplaceFuncSig{regexpBaseFuncSig: regexpBaseFuncSig{baseBuiltinFunc: base}}
	case tipb.ScalarFuncSig_JsonExtractSig:
		f = &builtinJSONExtractSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonUnquoteSig:
		f = &builtinJSONUnquoteSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonTypeSig:
		f = &builtinJSONTypeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonSetSig:
		f = &builtinJSONSetSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonInsertSig:
		f = &builtinJSONInsertSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonReplaceSig:
		f = &builtinJSONReplaceSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonRemoveSig:
		f = &builtinJSONRemoveSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonMergeSig:
		f = &builtinJSONMergeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonObjectSig:
		f = &builtinJSONObjectSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonArraySig:
		f = &builtinJSONArraySig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonValidJsonSig:
		f = &builtinJSONValidJSONSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonContainsSig:
		f = &builtinJSONContainsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonArrayAppendSig:
		f = &builtinJSONArrayAppendSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonArrayInsertSig:
		f = &builtinJSONArrayInsertSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_JsonMergePatchSig:
	case tipb.ScalarFuncSig_JsonMergePreserveSig:
		f = &builtinJSONMergeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonContainsPathSig:
		f = &builtinJSONContainsPathSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_JsonPrettySig:
	case tipb.ScalarFuncSig_JsonQuoteSig:
		f = &builtinJSONQuoteSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonSearchSig:
		f = &builtinJSONSearchSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonStorageSizeSig:
		f = &builtinJSONStorageSizeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonDepthSig:
		f = &builtinJSONDepthSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonKeysSig:
		f = &builtinJSONKeysSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonLengthSig:
		f = &builtinJSONLengthSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonKeys2ArgsSig:
		f = &builtinJSONKeys2ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonValidStringSig:
		f = &builtinJSONValidStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonValidOthersSig:
		f = &builtinJSONValidOthersSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_JsonMemberOfSig:
		f = &builtinJSONMemberOfSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DateFormatSig:
		f = &builtinDateFormatSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_DateLiteral:
	// 	f = &builtinDateLiteralSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DateDiff:
		f = &builtinDateDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NullTimeDiff:
		f = &builtinNullTimeDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TimeStringTimeDiff:
		f = &builtinTimeStringTimeDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DurationStringTimeDiff:
		f = &builtinDurationStringTimeDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DurationDurationTimeDiff:
		f = &builtinDurationDurationTimeDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_StringTimeTimeDiff:
		f = &builtinStringTimeTimeDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_StringDurationTimeDiff:
		f = &builtinStringDurationTimeDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_StringStringTimeDiff:
		f = &builtinStringStringTimeDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TimeTimeTimeDiff:
		f = &builtinTimeTimeTimeDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Date:
		f = &builtinDateSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Hour:
		f = &builtinHourSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Minute:
		f = &builtinMinuteSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Second:
		f = &builtinSecondSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MicroSecond:
		f = &builtinMicroSecondSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Month:
		f = &builtinMonthSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MonthName:
		f = &builtinMonthNameSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NowWithArg:
		f = &builtinNowWithArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_NowWithoutArg:
		f = &builtinNowWithoutArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DayName:
		f = &builtinDayNameSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DayOfMonth:
		f = &builtinDayOfMonthSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DayOfWeek:
		f = &builtinDayOfWeekSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_DayOfYear:
		f = &builtinDayOfYearSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_WeekWithMode:
		f = &builtinWeekWithModeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_WeekWithoutMode:
		f = &builtinWeekWithoutModeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_WeekDay:
		f = &builtinWeekDaySig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_WeekOfYear:
		f = &builtinWeekOfYearSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Year:
		f = &builtinYearSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_YearWeekWithMode:
		f = &builtinYearWeekWithModeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_YearWeekWithoutMode:
		f = &builtinYearWeekWithoutModeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_GetFormat:
		f = &builtinGetFormatSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SysDateWithFsp:
		f = &builtinSysDateWithFspSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SysDateWithoutFsp:
		f = &builtinSysDateWithoutFspSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CurrentDate:
		f = &builtinCurrentDateSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CurrentTime0Arg:
		f = &builtinCurrentTime0ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CurrentTime1Arg:
		f = &builtinCurrentTime1ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Time:
		f = &builtinTimeSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_TimeLiteral:
	// 	f = &builtinTimeLiteralSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UTCDate:
		f = &builtinUTCDateSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UTCTimestampWithArg:
		f = &builtinUTCTimestampWithArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UTCTimestampWithoutArg:
		f = &builtinUTCTimestampWithoutArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddDatetimeAndDuration:
		f = &builtinAddDatetimeAndDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddDatetimeAndString:
		f = &builtinAddDatetimeAndStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddTimeDateTimeNull:
		f = &builtinAddTimeDateTimeNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddStringAndDuration:
		f = &builtinAddStringAndDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddStringAndString:
		f = &builtinAddStringAndStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddTimeStringNull:
		f = &builtinAddTimeStringNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddDurationAndDuration:
		f = &builtinAddDurationAndDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddDurationAndString:
		f = &builtinAddDurationAndStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddTimeDurationNull:
		f = &builtinAddTimeDurationNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddDateAndDuration:
		f = &builtinAddDateAndDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_AddDateAndString:
		f = &builtinAddDateAndStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubDatetimeAndDuration:
		f = &builtinSubDatetimeAndDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubDatetimeAndString:
		f = &builtinSubDatetimeAndStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubTimeDateTimeNull:
		f = &builtinSubTimeDateTimeNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubStringAndDuration:
		f = &builtinSubStringAndDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubStringAndString:
		f = &builtinSubStringAndStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubTimeStringNull:
		f = &builtinSubTimeStringNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubDurationAndDuration:
		f = &builtinSubDurationAndDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubDurationAndString:
		f = &builtinSubDurationAndStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubTimeDurationNull:
		f = &builtinSubTimeDurationNullSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubDateAndDuration:
		f = &builtinSubDateAndDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubDateAndString:
		f = &builtinSubDateAndStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnixTimestampCurrent:
		f = &builtinUnixTimestampCurrentSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnixTimestampInt:
		f = &builtinUnixTimestampIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnixTimestampDec:
		f = &builtinUnixTimestampDecSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_ConvertTz:
	// 	f = &builtinConvertTzSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MakeDate:
		f = &builtinMakeDateSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_MakeTime:
		f = &builtinMakeTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_PeriodAdd:
		f = &builtinPeriodAddSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_PeriodDiff:
		f = &builtinPeriodDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Quarter:
		f = &builtinQuarterSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SecToTime:
		f = &builtinSecToTimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TimeToSec:
		f = &builtinTimeToSecSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TimestampAdd:
		f = &builtinTimestampAddSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ToDays:
		f = &builtinToDaysSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ToSeconds:
		f = &builtinToSecondsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UTCTimeWithArg:
		f = &builtinUTCTimeWithArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UTCTimeWithoutArg:
		f = &builtinUTCTimeWithoutArgSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_Timestamp1Arg:
	// 	f = &builtinTimestamp1ArgSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_Timestamp2Args:
	// 	f = &builtinTimestamp2ArgsSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_TimestampLiteral:
	// 	f = &builtinTimestampLiteralSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LastDay:
		f = &builtinLastDaySig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_StrToDateDate:
		f = &builtinStrToDateDateSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_StrToDateDatetime:
		f = &builtinStrToDateDatetimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_StrToDateDuration:
		f = &builtinStrToDateDurationSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FromUnixTime1Arg:
		f = &builtinFromUnixTime1ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FromUnixTime2Arg:
		f = &builtinFromUnixTime2ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ExtractDatetimeFromString:
		f = &builtinExtractDatetimeFromStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ExtractDatetime:
		f = &builtinExtractDatetimeSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ExtractDuration:
		f = &builtinExtractDurationSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AddDateStringString:
	// 	f = &builtinAddDateStringStringSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AddDateStringInt:
	// 	f = &builtinAddDateStringIntSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AddDateStringDecimal:
	// 	f = &builtinAddDateStringDecimalSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AddDateIntString:
	// 	f = &builtinAddDateIntStringSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AddDateIntInt:
	// 	f = &builtinAddDateIntIntSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AddDateDatetimeString:
	// 	f = &builtinAddDateDatetimeStringSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_AddDateDatetimeInt:
	// 	f = &builtinAddDateDatetimeIntSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_SubDateStringString:
	// 	f = &builtinSubDateStringStringSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_SubDateStringInt:
	// 	f = &builtinSubDateStringIntSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_SubDateStringDecimal:
	// 	f = &builtinSubDateStringDecimalSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_SubDateIntString:
	// 	f = &builtinSubDateIntStringSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_SubDateIntInt:
	// 	f = &builtinSubDateIntIntSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_SubDateDatetimeString:
	// 	f = &builtinSubDateDatetimeStringSig{baseBuiltinFunc: base}
	// case tipb.ScalarFuncSig_SubDateDatetimeInt:
	// 	f = &builtinSubDateDatetimeIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FromDays:
		f = &builtinFromDaysSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TimeFormat:
		f = &builtinTimeFormatSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_TimestampDiff:
		f = &builtinTimestampDiffSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_BitLength:
		f = &builtinBitLengthSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Bin:
		f = &builtinBinSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ASCII:
		f = &builtinASCIISig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Char:
		f = &builtinCharSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CharLengthUTF8:
		f = &builtinCharLengthUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_CharLength:
		f = &builtinCharLengthBinarySig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Concat:
		f = &builtinConcatSig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_ConcatWS:
		f = &builtinConcatWSSig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_Convert:
		f = &builtinConvertSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Elt:
		f = &builtinEltSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ExportSet3Arg:
		f = &builtinExportSet3ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ExportSet4Arg:
		f = &builtinExportSet4ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ExportSet5Arg:
		f = &builtinExportSet5ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FieldInt:
		f = &builtinFieldIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FieldReal:
		f = &builtinFieldRealSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FieldString:
		f = &builtinFieldStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FindInSet:
		f = &builtinFindInSetSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Format:
		f = &builtinFormatSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FormatWithLocale:
		f = &builtinFormatWithLocaleSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FromBase64:
		f = &builtinFromBase64Sig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_HexIntArg:
		f = &builtinHexIntArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_HexStrArg:
		f = &builtinHexStrArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_InsertUTF8:
		f = &builtinInsertUTF8Sig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_Insert:
		f = &builtinInsertSig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_InstrUTF8:
		f = &builtinInstrUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Instr:
		f = &builtinInstrSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LTrim:
		f = &builtinLTrimSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LeftUTF8:
		f = &builtinLeftUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Left:
		f = &builtinLeftSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Length:
		f = &builtinLengthSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Locate2ArgsUTF8:
		f = &builtinLocate2ArgsUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Locate3ArgsUTF8:
		f = &builtinLocate3ArgsUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Locate2Args:
		f = &builtinLocate2ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Locate3Args:
		f = &builtinLocate3ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Lower:
		f = &builtinLowerSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LowerUTF8:
		f = &builtinLowerUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_LpadUTF8:
		f = &builtinLpadUTF8Sig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_Lpad:
		f = &builtinLpadSig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_MakeSet:
		f = &builtinMakeSetSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_OctInt:
		f = &builtinOctIntSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_OctString:
		f = &builtinOctStringSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Ord:
		f = &builtinOrdSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Quote:
		f = &builtinQuoteSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RTrim:
		f = &builtinRTrimSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Repeat:
		f = &builtinRepeatSig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_Replace:
		f = &builtinReplaceSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ReverseUTF8:
		f = &builtinReverseUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Reverse:
		f = &builtinReverseSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RightUTF8:
		f = &builtinRightUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Right:
		f = &builtinRightSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_RpadUTF8:
		f = &builtinRpadUTF8Sig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_Rpad:
		f = &builtinRpadSig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_Space:
		f = &builtinSpaceSig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_Strcmp:
		f = &builtinStrcmpSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Substring2ArgsUTF8:
		f = &builtinSubstring2ArgsUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Substring3ArgsUTF8:
		f = &builtinSubstring3ArgsUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Substring2Args:
		f = &builtinSubstring2ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Substring3Args:
		f = &builtinSubstring3ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_SubstringIndex:
		f = &builtinSubstringIndexSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ToBase64:
		f = &builtinToBase64Sig{baseBuiltinFunc: base, maxAllowedPacket: maxAllowedPacket}
	case tipb.ScalarFuncSig_Trim1Arg:
		f = &builtinTrim1ArgSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Trim2Args:
		f = &builtinTrim2ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Trim3Args:
		f = &builtinTrim3ArgsSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UnHex:
		f = &builtinUnHexSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_Upper:
		f = &builtinUpperSig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_UpperUTF8:
		f = &builtinUpperUTF8Sig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_ToBinary:
		f = &builtinInternalToBinarySig{baseBuiltinFunc: base}
	case tipb.ScalarFuncSig_FromBinary:
		f = &builtinInternalFromBinarySig{baseBuiltinFunc: base}

	default:
		e = ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", sigCode)
		return nil, e
	}
	f.setPbCode(sigCode)
	return f, nil
}

func newDistSQLFunctionBySig(ctx BuildContext, sigCode tipb.ScalarFuncSig, tp *tipb.FieldType, args []Expression) (Expression, error) {
	f, err := getSignatureByPB(ctx, sigCode, tp, args)
	if err != nil {
		return nil, err
	}
	return &ScalarFunction{
		FuncName: model.NewCIStr(fmt.Sprintf("sig_%T", f)),
		Function: f,
		RetType:  f.getRetTp(),
	}, nil
}

// PBToExprs converts pb structures to expressions.
func PBToExprs(ctx BuildContext, pbExprs []*tipb.Expr, fieldTps []*types.FieldType) ([]Expression, error) {
	exprs := make([]Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := PBToExpr(ctx, expr, fieldTps)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if e == nil {
			return nil, errors.Errorf("pb to expression failed, pb expression is %v", expr)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

// PBToExpr converts pb structure to expression.
func PBToExpr(ctx BuildContext, expr *tipb.Expr, tps []*types.FieldType) (Expression, error) {
	sc := ctx.GetSessionVars().StmtCtx
	switch expr.Tp {
	case tipb.ExprType_ColumnRef:
		_, offset, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, err
		}
		return &Column{Index: int(offset), RetType: tps[offset]}, nil
	case tipb.ExprType_Null:
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}, nil
	case tipb.ExprType_Int64:
		return convertInt(expr.Val, expr.FieldType)
	case tipb.ExprType_Uint64:
		return convertUint(expr.Val, expr.FieldType)
	case tipb.ExprType_String:
		return convertString(expr.Val, expr.FieldType)
	case tipb.ExprType_Bytes:
		return &Constant{Value: types.NewBytesDatum(expr.Val), RetType: types.NewFieldType(mysql.TypeString)}, nil
	case tipb.ExprType_MysqlBit:
		return &Constant{Value: types.NewMysqlBitDatum(expr.Val), RetType: types.NewFieldType(mysql.TypeString)}, nil
	case tipb.ExprType_Float32:
		return convertFloat(expr.Val, true)
	case tipb.ExprType_Float64:
		return convertFloat(expr.Val, false)
	case tipb.ExprType_MysqlDecimal:
		return convertDecimal(expr.Val, expr.FieldType)
	case tipb.ExprType_MysqlDuration:
		return convertDuration(expr.Val)
	case tipb.ExprType_MysqlTime:
		return convertTime(expr.Val, expr.FieldType, sc.TimeZone())
	case tipb.ExprType_MysqlJson:
		return convertJSON(expr.Val)
	case tipb.ExprType_MysqlEnum:
		return convertEnum(expr.Val, expr.FieldType)
	}
	if expr.Tp != tipb.ExprType_ScalarFunc {
		panic("should be a tipb.ExprType_ScalarFunc")
	}
	// Then it must be a scalar function.
	args := make([]Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		if child.Tp == tipb.ExprType_ValueList {
			results, err := decodeValueList(child.Val)
			if err != nil {
				return nil, err
			}
			if len(results) == 0 {
				return &Constant{Value: types.NewDatum(false), RetType: types.NewFieldType(mysql.TypeLonglong)}, nil
			}
			args = append(args, results...)
			continue
		}
		arg, err := PBToExpr(ctx, child, tps)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	sf, err := newDistSQLFunctionBySig(ctx, expr.Sig, expr.FieldType, args)
	if err != nil {
		return nil, err
	}

	return sf, nil
}

func convertTime(data []byte, ftPB *tipb.FieldType, tz *time.Location) (*Constant, error) {
	ft := PbTypeToFieldType(ftPB)
	_, v, err := codec.DecodeUint(data)
	if err != nil {
		return nil, err
	}
	var t types.Time
	t.SetType(ft.GetType())
	t.SetFsp(ft.GetDecimal())
	err = t.FromPackedUint(v)
	if err != nil {
		return nil, err
	}
	if ft.GetType() == mysql.TypeTimestamp && tz != time.UTC {
		err = t.ConvertTimeZone(time.UTC, tz)
		if err != nil {
			return nil, err
		}
	}
	return &Constant{Value: types.NewTimeDatum(t), RetType: ft}, nil
}

func decodeValueList(data []byte) ([]Expression, error) {
	if len(data) == 0 {
		return nil, nil
	}
	list, err := codec.Decode(data, 1)
	if err != nil {
		return nil, err
	}
	result := make([]Expression, 0, len(list))
	for _, value := range list {
		result = append(result, &Constant{Value: value})
	}
	return result, nil
}

func convertInt(val []byte, tp *tipb.FieldType) (*Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid int % x", val)
	}
	d.SetInt64(i)
	return &Constant{Value: d, RetType: PbTypeToFieldType(tp)}, nil
}

func convertUint(val []byte, tp *tipb.FieldType) (*Constant, error) {
	var d types.Datum
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid uint % x", val)
	}
	d.SetUint64(u)
	return &Constant{Value: d, RetType: PbTypeToFieldType(tp)}, nil
}

func convertString(val []byte, tp *tipb.FieldType) (*Constant, error) {
	var d types.Datum
	d.SetBytesAsString(val, collate.ProtoToCollation(tp.Collate), uint32(tp.Flen))
	return &Constant{Value: d, RetType: PbTypeToFieldType(tp)}, nil
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

func convertDecimal(val []byte, ftPB *tipb.FieldType) (*Constant, error) {
	ft := PbTypeToFieldType(ftPB)
	_, dec, precision, frac, err := codec.DecodeDecimal(val)
	var d types.Datum
	d.SetMysqlDecimal(dec)
	d.SetLength(precision)
	d.SetFrac(frac)
	if err != nil {
		return nil, errors.Errorf("invalid decimal % x", val)
	}
	return &Constant{Value: d, RetType: ft}, nil
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

func convertJSON(val []byte) (*Constant, error) {
	var d types.Datum
	_, d, err := codec.DecodeOne(val)
	if err != nil {
		return nil, errors.Errorf("invalid json % x", val)
	}
	if d.Kind() != types.KindMysqlJSON {
		return nil, errors.Errorf("invalid Datum.Kind() %d", d.Kind())
	}
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeJSON)}, nil
}

func convertEnum(val []byte, tp *tipb.FieldType) (*Constant, error) {
	_, uVal, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid enum % x", val)
	}
	// If uVal is 0, it should return Enum{}
	var e = types.Enum{}
	if uVal != 0 {
		e, err = types.ParseEnumValue(tp.Elems, uVal)
		if err != nil {
			return nil, err
		}
	}
	d := types.NewMysqlEnumDatum(e)
	return &Constant{Value: d, RetType: FieldTypeFromPB(tp)}, nil
}
