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
	t.Parallel()
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldTypeBuilder, 1)
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
		FieldType: ToPBFieldType(newIntFieldType().Build()),
	}
	_, err = PBToExpr(expr, fieldTps, sc)
	require.Error(t, err)
}

// TestEval test expr.Eval().
func TestEval(t *testing.T) {
	t.Parallel()
	row := chunk.MutRowFromDatums([]types.Datum{types.NewDatum(100)}).ToRow()
	fieldTps := make([]*types.FieldTypeBuilder, 1)
	fieldTps[0] = types.NewFieldTypeBuilder(mysql.TypeLonglong)
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
				toPBFieldType(newIntFieldType().Build()),
				jsonDatumExpr(t, `true`),
			),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType().Build()),
				jsonDatumExpr(t, `[10, {"a": 20}]`),
			),
			types.NewIntDatum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonStorageSizeSig,
				toPBFieldType(newIntFieldType().Build()),
				jsonDatumExpr(t, `[{"a":{"a":1},"b":2}]`),
			),
			types.NewIntDatum(25),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonSearchSig,
				toPBFieldType(newJSONFieldType().Build()),
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
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(2333))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(2333))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewStringDatum("2333"))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2333")))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewIntDatum(2333))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(2333))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewStringDatum("2333"))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2333")))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsString,
				toPBFieldType(newStringFieldType().Build()), datumExpr(t, types.NewStringDatum("2333"))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsString,
				toPBFieldType(newStringFieldType().Build()), datumExpr(t, types.NewIntDatum(2333))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsString,
				toPBFieldType(newStringFieldType().Build()), datumExpr(t, types.NewFloat64Datum(2333))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsString,
				toPBFieldType(newStringFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2333")))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2333")))),
			types.NewDecimalDatum(newMyDecimal(t, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewIntDatum(2333))),
			types.NewDecimalDatum(newMyDecimal(t, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewFloat64Datum(2333))),
			types.NewDecimalDatum(newMyDecimal(t, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewStringDatum("2333"))),
			types.NewDecimalDatum(newMyDecimal(t, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(2)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEReal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(2)), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEReal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTReal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQReal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEReal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQReal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEDecimal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEDecimal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTDecimal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQDecimal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEDecimal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQDecimal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEDuration,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GTDuration,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQDuration,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEDuration,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEDuration,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQDuration,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEString,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewStringDatum("1")), datumExpr(t, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEString,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewStringDatum("1")), datumExpr(t, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEString,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewStringDatum("2")), datumExpr(t, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQString,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GTJson,
				toPBFieldType(newIntFieldType().Build()), jsonDatumExpr(t, "[2]"), jsonDatumExpr(t, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEJson,
				toPBFieldType(newIntFieldType().Build()), jsonDatumExpr(t, "[2]"), jsonDatumExpr(t, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTJson,
				toPBFieldType(newIntFieldType().Build()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEJson,
				toPBFieldType(newIntFieldType().Build()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQJson,
				toPBFieldType(newIntFieldType().Build()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEJson,
				toPBFieldType(newIntFieldType().Build()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQJson,
				toPBFieldType(newIntFieldType().Build()), jsonDatumExpr(t, "[1]"), jsonDatumExpr(t, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DecimalIsNull,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DurationIsNull,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_RealIsNull,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LeftShift,
				ToPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(1)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(-1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsUInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewUintDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(-1.23))),
			types.NewFloat64Datum(1.23),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "-1.23")))),
			types.NewDecimalDatum(newMyDecimal(t, "1.23")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalAnd,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalOr,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalXor,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitAndSig,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitOrSig,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitXorSig,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitNegSig,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(0))),
			types.NewIntDatum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InReal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InDecimal,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InString,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewStringDatum("1")), datumExpr(t, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		// {
		// 	scalarFunctionExpr(tipb.ScalarFuncSig_InTime,
		// 		toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewTimeDatum(types.ZeroDate)), datumExpr(t, types.NewTimeDatum(types.ZeroDate))),
		// 	types.NewIntDatum(1),
		// },
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InDuration,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second))), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewDecimalDatum(newMyDecimal(t, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewDecimalDatum(newMyDecimal(t, "2")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullString,
				toPBFieldType(newStringFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewStringDatum("1"))),
			types.NewStringDatum("1"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfString,
				toPBFieldType(newStringFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewStringDatum("2"))),
			types.NewStringDatum("2"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullDuration,
				toPBFieldType(newDurFieldType().Build()), datumExpr(t, types.NewDatum(nil)), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewDurationDatum(newDuration(time.Second)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfDuration,
				toPBFieldType(newDurFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewDurationDatum(newDuration(time.Second * 2)),
		},

		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsDuration,
				toPBFieldType(newDurFieldType().Build()), datumExpr(t, types.NewIntDatum(1))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsDuration,
				toPBFieldType(newDurFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsDuration,
				toPBFieldType(newDurFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDurationAsDuration,
				toPBFieldType(newDurFieldType().Build()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second*1)))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsDuration,
				toPBFieldType(newDurFieldType().Build()), datumExpr(t, types.NewStringDatum("1"))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastTimeAsTime,
				toPBFieldType(newDateFieldType().Build()), datumExpr(t, types.NewTimeDatum(newDateTime(t, "2000-01-01")))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsTime,
				toPBFieldType(newDateFieldType().Build()), datumExpr(t, types.NewIntDatum(20000101))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsTime,
				toPBFieldType(newDateFieldType().Build()), datumExpr(t, types.NewFloat64Datum(20000101))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsTime,
				toPBFieldType(newDateFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "20000101")))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsTime,
				toPBFieldType(newDateFieldType().Build()), datumExpr(t, types.NewStringDatum("20000101"))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewDecimalDatum(newMyDecimal(t, "3")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewDecimalDatum(newMyDecimal(t, "-1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1)), datumExpr(t, types.NewIntDatum(2))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1"))), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "2")))),
			types.NewDecimalDatum(newMyDecimal(t, "2")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1)), datumExpr(t, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilIntToInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilIntToDec,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewIntDatum(1))),
			types.NewDecimalDatum(newMyDecimal(t, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilDecToInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorIntToInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorIntToDec,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewIntDatum(1))),
			types.NewDecimalDatum(newMyDecimal(t, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorDecToInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceInt,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceReal,
				toPBFieldType(newRealFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceDecimal,
				toPBFieldType(newDecimalFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewDecimalDatum(newMyDecimal(t, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceString,
				toPBFieldType(newStringFieldType().Build()), datumExpr(t, types.NewStringDatum("1"))),
			types.NewStringDatum("1"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceDuration,
				toPBFieldType(newDurFieldType().Build()), datumExpr(t, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewDurationDatum(newDuration(time.Second)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceTime,
				toPBFieldType(newDateFieldType().Build()), datumExpr(t, types.NewTimeDatum(newDateTime(t, "2000-01-01")))),
			types.NewTimeDatum(newDateTime(t, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenInt,
				toPBFieldType(newIntFieldType().Build())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenReal,
				toPBFieldType(newRealFieldType().Build())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenDecimal,
				toPBFieldType(newDecimalFieldType().Build())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenDuration,
				toPBFieldType(newDurFieldType().Build())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenTime,
				toPBFieldType(newDateFieldType().Build())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CaseWhenJson,
				toPBFieldType(newJSONFieldType().Build())),
			types.NewDatum(nil),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_RealIsFalse,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(0),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DecimalIsFalse,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
			types.NewIntDatum(0),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_RealIsTrue,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DecimalIsTrue,
				toPBFieldType(newIntFieldType().Build()), datumExpr(t, types.NewDecimalDatum(newMyDecimal(t, "1")))),
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
		expr.FieldType = ToPBFieldType(newDateFieldType().Build())
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

// toPBFieldType converts *types.FieldTypeBuilder to *tipb.FieldType.
func toPBFieldType(ft *types.FieldType) *tipb.FieldType {
	return &tipb.FieldType{
		Tp:      int32(ft.GetTp()),
		Flag:    uint32(ft.GetFlag()),
		Flen:    int32(ft.GetFlen()),
		Decimal: int32(ft.GetDecimal()),
		Charset: ft.GetCharset(),
		Collate: collationToProto(ft.GetCollate()),
		Elems:   ft.GetElems(),
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

func newDateFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp: mysql.TypeDate,
	}
}

func newIntFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:      mysql.TypeLonglong,
		Flen:    mysql.MaxIntWidth,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
	}
}

func newDurFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:      mysql.TypeDuration,
		Decimal: int(types.DefaultFsp),
	}
}

func newStringFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:   mysql.TypeVarString,
		Flen: types.UnspecifiedLength,
	}
}

func newRealFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:   mysql.TypeFloat,
		Flen: types.UnspecifiedLength,
	}
}

func newDecimalFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:   mysql.TypeNewDecimal,
		Flen: types.UnspecifiedLength,
	}
}

func newJSONFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:      mysql.TypeJSON,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func newFloatFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:      mysql.TypeFloat,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func newBinaryLiteralFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:      mysql.TypeBit,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func newBlobFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
		Tp:      mysql.TypeBlob,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func newEnumFieldType() *types.FieldTypeBuilder {
	return &types.FieldTypeBuilder{
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
