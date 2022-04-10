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
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestPBToExpr(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldType, 1)
	ds := []types.Datum{types.NewIntDatum(1), types.NewUintDatum(1), types.NewFloat64Datum(1),
		types.NewDecimalDatum(newMyDecimal(t, "1")), types.NewDurationDatum(newDuration(time.Second))}

	for _, d := range ds {
		expr := datumExpr(t, d)
		expr.Val = expr.Val[:len(expr.Val)/2]
		_, err := PBToExpr(expr, fieldTps, sc)
		require.Error(t, err)
	}

	expr := &tipb.Expr{
		Tp: tipb.ExprType_ScalarFunc,
		Children: []*tipb.Expr{
			{
				Tp: tipb.ExprType_ValueList,
			},
		},
	}
	_, err := PBToExpr(expr, fieldTps, sc)
	require.NoError(t, err)

	val := make([]byte, 0, 32)
	val = codec.EncodeInt(val, 1)
	expr = &tipb.Expr{
		Tp: tipb.ExprType_ScalarFunc,
		Children: []*tipb.Expr{
			{
				Tp:  tipb.ExprType_ValueList,
				Val: val[:len(val)/2],
			},
		},
	}
	_, err = PBToExpr(expr, fieldTps, sc)
	require.Error(t, err)

	expr = &tipb.Expr{
		Tp: tipb.ExprType_ScalarFunc,
		Children: []*tipb.Expr{
			{
				Tp:  tipb.ExprType_ValueList,
				Val: val,
			},
		},
		Sig:       tipb.ScalarFuncSig_AbsInt,
		FieldType: ToPBFieldType(newIntFieldType()),
	}
	_, err = PBToExpr(expr, fieldTps, sc)
	require.Error(t, err)
}

// TestEval test expr.Eval().
func TestEval(t *testing.T) {
	row := chunk.MutRowFromDatums([]types.Datum{types.NewDatum(100)}).ToRow()
	fieldTps := make([]*types.FieldType, 1)
	fieldTps[0] = types.NewFieldType(mysql.TypeLonglong)
	tests := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		// Datums.
		{
			datumExpr(t, types.NewFloat32Datum(1.1)),
			types.NewFloat32Datum(1.1),
		},
		{
			datumExpr(t, types.NewFloat64Datum(1.1)),
			types.NewFloat64Datum(1.1),
		},
		{
			datumExpr(t, types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			datumExpr(t, types.NewUintDatum(1)),
			types.NewUintDatum(1),
		},
		{
			datumExpr(t, types.NewBytesDatum([]byte("abc"))),
			types.NewBytesDatum([]byte("abc")),
		},
		{
			datumExpr(t, types.NewStringDatum("abc")),
			types.NewStringDatum("abc"),
		},
		{
			datumExpr(t, types.Datum{}),
			types.Datum{},
		},
		{
			datumExpr(t, types.NewDurationDatum(types.Duration{Duration: time.Hour})),
			types.NewDurationDatum(types.Duration{Duration: time.Hour}),
		},
		{
			datumExpr(t, types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1))),
			types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1)),
		},
		// Columns.
		{
			columnExpr(0),
			types.NewIntDatum(100),
		},
		// Scalar Functions.
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType()),
				jsonDatumExpr(t, `true`),
			),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType()),
				jsonDatumExpr(t, `[10, {"a": 20}]`),
			),
			types.NewIntDatum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonStorageSizeSig,
				toPBFieldType(newIntFieldType()),
				jsonDatumExpr(t, `[{"a":{"a":1},"b":2}]`),
			),
			types.NewIntDatum(25),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonSearchSig,
				toPBFieldType(newJSONFieldType()),
				jsonDatumExpr(t, `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`),
				datumExpr(t, types.NewBytesDatum([]byte(`all`))),
				datumExpr(t, types.NewBytesDatum([]byte(`10`))),
				datumExpr(t, types.NewBytesDatum([]byte(`\`))),
				datumExpr(t, types.NewBytesDatum([]byte(`$**.k`))),
			),
			newJSONDatum(t, `"$[1][0].k"`),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(2333))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(2333))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewStringDatum("2333"))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2333")))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewIntDatum(2333))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(2333))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewStringDatum("2333"))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2333")))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsString,
				toPBFieldType(newStringFieldType()), datumExpr(t, types.NewStringDatum("2333"))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsString,
				toPBFieldType(newStringFieldType()), datumExpr(t, types.NewIntDatum(2333))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsString,
				toPBFieldType(newStringFieldType()), datumExpr(t, types.NewFloat64Datum(2333))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsString,
				toPBFieldType(newStringFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2333")))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2333")))),
			types.NewDecimalDatum(newMyDecimal(t, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewIntDatum(2333))),
			types.NewDecimalDatum(newMyDecimal(t, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewFloat64Datum(2333))),
			types.NewDecimalDatum(newMyDecimal(t, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewStringDatum("2333"))),
			types.NewDecimalDatum(newMyDecimal(t, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(2)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEReal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(2)), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEReal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTReal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQReal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEReal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQReal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GTDuration,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQDuration,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQDuration,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEString,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewStringDatum("1")), datumExpr(t, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEString,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewStringDatum("1")), datumExpr(t, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEString,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewStringDatum("2")), datumExpr(t, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQString,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GTJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(t, "[2]"), jsonDatumExpr(t, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(t, "[2]"), jsonDatumExpr(t, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DecimalIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DurationIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_RealIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LeftShift,
				ToPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(1)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(-1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsUInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewUintDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(-1.23))),
			types.NewFloat64Datum(1.23),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "-1.23")))),
			types.NewDecimalDatum(newMyDecimal(t, "1.23")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalAnd,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalOr,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalXor,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitAndSig,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitOrSig,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitXorSig,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitNegSig,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InReal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InString,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewStringDatum("1")), datumExpr(t, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		// {
		// 	scalarFunctionExpr(tipb.ScalarFuncSig_InTime,
		// 		toPBFieldType(newIntFieldType()), datumExpr(t, types.NewTimeDatum(types.ZeroDate)), datumExpr(t, types.NewTimeDatum(types.ZeroDate))),
		// 	types.NewIntDatum(1),
		// },
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InDuration,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewDecimalDatum(newMyDecimal(t, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewDecimalDatum(newMyDecimal(t, "2")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullString,
				toPBFieldType(newStringFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewStringDatum("1"))),
			types.NewStringDatum("1"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfString,
				toPBFieldType(newStringFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewStringDatum("2"))),
			types.NewStringDatum("2"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullDuration,
				toPBFieldType(newDurFieldType()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewDurationDatum(newDuration(time.Second)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfDuration,
				toPBFieldType(newDurFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewDurationDatum(newDuration(time.Second * 2)),
		},

		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(t, types.NewIntDatum(1))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDurationAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*1)))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(t, types.NewStringDatum("1"))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastTimeAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(t, types.NewTimeDatum(newDateTime(t, "2000-01-01")))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(t, types.NewIntDatum(20000101))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(t, types.NewFloat64Datum(20000101))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "20000101")))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(t, types.NewStringDatum("20000101"))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewDecimalDatum(newMyDecimal(t, "3")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewDecimalDatum(newMyDecimal(t, "-1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewDecimalDatum(newMyDecimal(t, "2")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilIntToInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilIntToDec,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewIntDatum(1))),
			types.NewDecimalDatum(newMyDecimal(t, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilDecToInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorIntToInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorIntToDec,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewIntDatum(1))),
			types.NewDecimalDatum(newMyDecimal(t, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorDecToInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceInt,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceReal,
				toPBFieldType(newRealFieldType()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewDecimalDatum(newMyDecimal(t, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceString,
				toPBFieldType(newStringFieldType()), datumExpr(t, types.NewStringDatum("1"))),
			types.NewStringDatum("1"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceDuration,
				toPBFieldType(newDurFieldType()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewDurationDatum(newDuration(time.Second)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceTime,
				toPBFieldType(newDateFieldType()), datumExpr(t, types.NewTimeDatum(newDateTime(t, "2000-01-01")))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenInt,
				toPBFieldType(newIntFieldType())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenReal,
				toPBFieldType(newRealFieldType())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenDecimal,
				toPBFieldType(newDecimalFieldType())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenDuration,
				toPBFieldType(newDurFieldType())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenTime,
				toPBFieldType(newDateFieldType())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenJson,
				toPBFieldType(newJSONFieldType())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_RealIsFalse,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(0),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DecimalIsFalse,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(0),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_RealIsTrue,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DecimalIsTrue,
				toPBFieldType(newIntFieldType()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		expr, err := PBToExpr(tt.expr, fieldTps, sc)
		require.NoError(t, err)
		result, err := expr.Eval(row)
		require.NoError(t, err)
		require.Equal(t, tt.result.Kind(), result.Kind())
		cmp, err := result.Compare(sc, &tt.result, collate.GetCollator(fieldTps[0].Collate))
		require.NoError(t, err)
		require.Equal(t, 0, cmp)
	}
}

func TestPBToExprWithNewCollation(t *testing.T) {
	collate.SetNewCollationEnabledForTest(false)
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldType, 1)

	cases := []struct {
		name    string
		expName string
		id      int32
		pbID    int32
	}{
		{"utf8_general_ci", "utf8_general_ci", 33, 33},
		{"UTF8MB4_BIN", "utf8mb4_bin", 46, 46},
		{"utf8mb4_bin", "utf8mb4_bin", 46, 46},
		{"utf8mb4_general_ci", "utf8mb4_general_ci", 45, 45},
		{"", "utf8mb4_bin", 46, 46},
		{"some_error_collation", "utf8mb4_bin", 46, 46},
		{"utf8_unicode_ci", "utf8_unicode_ci", 192, 192},
		{"utf8mb4_unicode_ci", "utf8mb4_unicode_ci", 224, 224},
		{"utf8mb4_zh_pinyin_tidb_as_cs", "utf8mb4_zh_pinyin_tidb_as_cs", 2048, 2048},
	}

	for _, cs := range cases {
		ft := types.NewFieldType(mysql.TypeString)
		ft.Collate = cs.name
		expr := new(tipb.Expr)
		expr.Tp = tipb.ExprType_String
		expr.FieldType = toPBFieldType(ft)
		require.Equal(t, cs.pbID, expr.FieldType.Collate)

		e, err := PBToExpr(expr, fieldTps, sc)
		require.NoError(t, err)
		cons, ok := e.(*Constant)
		require.True(t, ok)
		require.Equal(t, cs.expName, cons.Value.Collation())
	}

	collate.SetNewCollationEnabledForTest(true)

	for _, cs := range cases {
		ft := types.NewFieldType(mysql.TypeString)
		ft.Collate = cs.name
		expr := new(tipb.Expr)
		expr.Tp = tipb.ExprType_String
		expr.FieldType = toPBFieldType(ft)
		require.Equal(t, -cs.pbID, expr.FieldType.Collate)

		e, err := PBToExpr(expr, fieldTps, sc)
		require.NoError(t, err)
		cons, ok := e.(*Constant)
		require.True(t, ok)
		require.Equal(t, cs.expName, cons.Value.Collation())
	}
}

// Test convert various scalar functions.
func TestPBToScalarFuncExpr(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldType, 1)
	exprs := []*tipb.Expr{
		{
			Tp:        tipb.ExprType_ScalarFunc,
			Sig:       tipb.ScalarFuncSig_RegexpSig,
			FieldType: ToPBFieldType(newStringFieldType()),
		},
		{
			Tp:        tipb.ExprType_ScalarFunc,
			Sig:       tipb.ScalarFuncSig_RegexpUTF8Sig,
			FieldType: ToPBFieldType(newStringFieldType()),
		},
	}
	for _, expr := range exprs {
		_, err := PBToExpr(expr, fieldTps, sc)
		require.NoError(t, err)
	}
}

func datumExpr(t *testing.T, d types.Datum) *tipb.Expr {
	expr := new(tipb.Expr)
	switch d.Kind() {
	case types.KindInt64:
		expr.Tp = tipb.ExprType_Int64
		expr.Val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		expr.Tp = tipb.ExprType_Uint64
		expr.Val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString:
		expr.Tp = tipb.ExprType_String
		expr.FieldType = toPBFieldType(types.NewFieldType(mysql.TypeString))
		expr.Val = d.GetBytes()
	case types.KindBytes:
		expr.Tp = tipb.ExprType_Bytes
		expr.Val = d.GetBytes()
	case types.KindFloat32:
		expr.Tp = tipb.ExprType_Float32
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		expr.Tp = tipb.ExprType_Float64
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		expr.Tp = tipb.ExprType_MysqlDuration
		expr.Val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		expr.Tp = tipb.ExprType_MysqlDecimal
		var err error
		expr.Val, err = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		require.NoError(t, err)
	case types.KindMysqlJSON:
		expr.Tp = tipb.ExprType_MysqlJson
		var err error
		expr.Val = make([]byte, 0, 1024)
		expr.Val, err = codec.EncodeValue(nil, expr.Val, d)
		require.NoError(t, err)
	case types.KindMysqlTime:
		expr.Tp = tipb.ExprType_MysqlTime
		var err error
		expr.Val, err = codec.EncodeMySQLTime(nil, d.GetMysqlTime(), mysql.TypeUnspecified, nil)
		require.NoError(t, err)
		expr.FieldType = ToPBFieldType(newDateFieldType())
	default:
		expr.Tp = tipb.ExprType_Null
	}
	return expr
}

func newJSONDatum(t *testing.T, s string) (d types.Datum) {
	j, err := json.ParseBinaryFromString(s)
	require.NoError(t, err)
	d.SetMysqlJSON(j)
	return d
}

func jsonDatumExpr(t *testing.T, s string) *tipb.Expr {
	return datumExpr(t, newJSONDatum(t, s))
}

func columnExpr(columnID int64) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tipb.ExprType_ColumnRef
	expr.Val = codec.EncodeInt(nil, columnID)
	return expr
}

// toPBFieldType converts *types.FieldType to *tipb.FieldType.
func toPBFieldType(ft *types.FieldType) *tipb.FieldType {
	return &tipb.FieldType{
		Tp:      int32(ft.Tp),
		Flag:    uint32(ft.Flag),
		Flen:    int32(ft.Flen),
		Decimal: int32(ft.Decimal),
		Charset: ft.Charset,
		Collate: collate.CollationToProto(ft.Collate),
		Elems:   ft.Elems,
	}
}

func newMyDecimal(t *testing.T, s string) *types.MyDecimal {
	d := new(types.MyDecimal)
	require.Nil(t, d.FromString([]byte(s)))
	return d
}

func newDuration(dur time.Duration) types.Duration {
	return types.Duration{
		Duration: dur,
		Fsp:      types.DefaultFsp,
	}
}

func newDateTime(t *testing.T, s string) types.Time {
	tt, err := types.ParseDate(nil, s)
	require.NoError(t, err)
	return tt
}

func newDateFieldType() *types.FieldType {
	return &types.FieldType{
		Tp: mysql.TypeDate,
	}
}

func newIntFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flen:    mysql.MaxIntWidth,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
	}
}

func newDurFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeDuration,
		Decimal: types.DefaultFsp,
	}
}

func newStringFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   mysql.TypeVarString,
		Flen: types.UnspecifiedLength,
	}
}

func newRealFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   mysql.TypeFloat,
		Flen: types.UnspecifiedLength,
	}
}

func newDecimalFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   mysql.TypeNewDecimal,
		Flen: types.UnspecifiedLength,
	}
}

func newJSONFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeJSON,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func newFloatFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeFloat,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func newBinaryLiteralFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeBit,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func newBlobFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeBlob,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func newEnumFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeEnum,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func scalarFunctionExpr(sigCode tipb.ScalarFuncSig, retType *tipb.FieldType, args ...*tipb.Expr) *tipb.Expr {
	return &tipb.Expr{
		Tp:        tipb.ExprType_ScalarFunc,
		Sig:       sigCode,
		Children:  args,
		FieldType: retType,
	}
}
