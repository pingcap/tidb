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
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

var _ = Suite(&testEvalSuite{})
var _ = SerialSuites(&testEvalSerialSuite{})

type testEvalSuite struct {
	colID int64
}

type testEvalSerialSuite struct {
}

func (s *testEvalSerialSuite) TestPBToExprWithNewCollation(c *C) {
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
	}

	for _, cs := range cases {
		ft := types.NewFieldType(mysql.TypeString)
		ft.Collate = cs.name
		expr := new(tipb.Expr)
		expr.Tp = tipb.ExprType_String
		expr.FieldType = toPBFieldType(ft)
		c.Assert(expr.FieldType.Collate, Equals, cs.pbID)

		e, err := PBToExpr(expr, fieldTps, sc)
		c.Assert(err, IsNil)
		cons, ok := e.(*Constant)
		c.Assert(ok, IsTrue)
		c.Assert(cons.Value.Collation(), Equals, cs.expName)
	}
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	for _, cs := range cases {
		ft := types.NewFieldType(mysql.TypeString)
		ft.Collate = cs.name
		expr := new(tipb.Expr)
		expr.Tp = tipb.ExprType_String
		expr.FieldType = toPBFieldType(ft)
		c.Assert(expr.FieldType.Collate, Equals, -cs.pbID)

		e, err := PBToExpr(expr, fieldTps, sc)
		c.Assert(err, IsNil)
		cons, ok := e.(*Constant)
		c.Assert(ok, IsTrue)
		c.Assert(cons.Value.Collation(), Equals, cs.expName)
	}
}

func (s *testEvalSuite) SetUpSuite(c *C) {
	s.colID = 0
}

func (s *testEvalSuite) allocColID() int64 {
	s.colID++
	return s.colID
}

func (s *testEvalSuite) TearDownTest(c *C) {
	s.colID = 0
}

func (s *testEvalSuite) TestPBToExpr(c *C) {
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldType, 1)
	ds := []types.Datum{types.NewIntDatum(1), types.NewUintDatum(1), types.NewFloat64Datum(1),
		types.NewDecimalDatum(newMyDecimal(c, "1")), types.NewDurationDatum(newDuration(time.Second))}

	for _, d := range ds {
		expr := datumExpr(c, d)
		expr.Val = expr.Val[:len(expr.Val)/2]
		_, err := PBToExpr(expr, fieldTps, sc)
		c.Assert(err, NotNil)
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
	c.Assert(err, IsNil)

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
	c.Assert(err, NotNil)

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
	c.Assert(err, NotNil)
}

// TestEval test expr.Eval().
func (s *testEvalSuite) TestEval(c *C) {
	row := chunk.MutRowFromDatums([]types.Datum{types.NewDatum(100)}).ToRow()
	fieldTps := make([]*types.FieldType, 1)
	fieldTps[0] = types.NewFieldType(mysql.TypeLonglong)
	tests := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		// Datums.
		{
			datumExpr(c, types.NewFloat32Datum(1.1)),
			types.NewFloat32Datum(1.1),
		},
		{
			datumExpr(c, types.NewFloat64Datum(1.1)),
			types.NewFloat64Datum(1.1),
		},
		{
			datumExpr(c, types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			datumExpr(c, types.NewUintDatum(1)),
			types.NewUintDatum(1),
		},
		{
			datumExpr(c, types.NewBytesDatum([]byte("abc"))),
			types.NewBytesDatum([]byte("abc")),
		},
		{
			datumExpr(c, types.NewStringDatum("abc")),
			types.NewStringDatum("abc"),
		},
		{
			datumExpr(c, types.Datum{}),
			types.Datum{},
		},
		{
			datumExpr(c, types.NewDurationDatum(types.Duration{Duration: time.Hour})),
			types.NewDurationDatum(types.Duration{Duration: time.Hour}),
		},
		{
			datumExpr(c, types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1))),
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
				jsonDatumExpr(c, `true`),
			),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType()),
				jsonDatumExpr(c, `[10, {"a": 20}]`),
			),
			types.NewIntDatum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonStorageSizeSig,
				toPBFieldType(newIntFieldType()),
				jsonDatumExpr(c, `[{"a":{"a":1},"b":2}]`),
			),
			types.NewIntDatum(25),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonSearchSig,
				toPBFieldType(newJSONFieldType()),
				jsonDatumExpr(c, `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`),
				datumExpr(c, types.NewBytesDatum([]byte(`all`))),
				datumExpr(c, types.NewBytesDatum([]byte(`10`))),
				datumExpr(c, types.NewBytesDatum([]byte(`\`))),
				datumExpr(c, types.NewBytesDatum([]byte(`$**.k`))),
			),
			newJSONDatum(c, `"$[1][0].k"`),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastIntAsInt, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(2333))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastRealAsInt, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(2333))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastStringAsInt, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringDatum("2333"))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastDecimalAsInt, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2333")))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastIntAsReal, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewIntDatum(2333))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastRealAsReal, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(2333))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastStringAsReal, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewStringDatum("2333"))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastDecimalAsReal, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2333")))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewStringDatum("2333"))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewIntDatum(2333))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewFloat64Datum(2333))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2333")))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastDecimalAsDecimal, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2333")))),
			types.NewDecimalDatum(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastIntAsDecimal, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntDatum(2333))),
			types.NewDecimalDatum(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastRealAsDecimal, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewFloat64Datum(2333))),
			types.NewDecimalDatum(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_CastStringAsDecimal, &tipb.InUnionMetadata{InUnion: false},
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewStringDatum("2333"))),
			types.NewDecimalDatum(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(2)), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(2)), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*2))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GTDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*2))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringDatum("1")), datumExpr(c, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringDatum("1")), datumExpr(c, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringDatum("2")), datumExpr(c, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GTJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(c, "[2]"), jsonDatumExpr(c, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(c, "[2]"), jsonDatumExpr(c, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LTJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(c, "[1]"), jsonDatumExpr(c, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(c, "[1]"), jsonDatumExpr(c, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_EQJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(c, "[1]"), jsonDatumExpr(c, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(c, "[1]"), jsonDatumExpr(c, "[2]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQJson,
				toPBFieldType(newIntFieldType()), jsonDatumExpr(c, "[1]"), jsonDatumExpr(c, "[1]")),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DecimalIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DurationIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_RealIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LeftShift,
				ToPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(1)), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(-1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsUInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewUintDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(-1.23))),
			types.NewFloat64Datum(1.23),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "-1.23")))),
			types.NewDecimalDatum(newMyDecimal(c, "1.23")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalAnd,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalOr,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LogicalXor,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitAndSig,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitOrSig,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitXorSig,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_BitNegSig,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(0))),
			types.NewIntDatum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringDatum("1")), datumExpr(c, types.NewStringDatum("1"))),
			types.NewIntDatum(1),
		},
		//{
		//	scalarFunctionExpr(tipb.ScalarFuncSig_InTime,
		//		toPBFieldType(newIntFieldType()), datumExpr(c, types.NewTimeDatum(types.ZeroDate)), datumExpr(c, types.NewTimeDatum(types.ZeroDate))),
		//	types.NewIntDatum(1),
		//},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_InDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(2))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewDecimalDatum(newMyDecimal(c, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewDecimalDatum(newMyDecimal(c, "2")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewStringDatum("1"))),
			types.NewStringDatum("1"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewStringDatum("2"))),
			types.NewStringDatum("2"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfNullDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewDurationDatum(newDuration(time.Second)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_IfDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewDurationDatum(newDuration(time.Second * 2)),
		},

		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewIntDatum(1))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDurationAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*1)))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewStringDatum("1"))),
			types.NewDurationDatum(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastTimeAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewTimeDatum(newDateTime(c, "2000-01-01")))),
			types.NewTimeDatum(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewIntDatum(20000101))),
			types.NewTimeDatum(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewFloat64Datum(20000101))),
			types.NewTimeDatum(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "20000101")))),
			types.NewTimeDatum(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewStringDatum("20000101"))),
			types.NewTimeDatum(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(2))),
			types.NewIntDatum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewDecimalDatum(newMyDecimal(c, "3")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_PlusReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(2))),
			types.NewIntDatum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewDecimalDatum(newMyDecimal(c, "-1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MinusReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(-1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(2))),
			types.NewIntDatum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewDecimalDatum(newMyDecimal(c, "2")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_MultiplyReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewFloat64Datum(2),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilIntToInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilIntToDec,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntDatum(1))),
			types.NewDecimalDatum(newMyDecimal(c, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilDecToInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CeilReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorIntToInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorIntToDec,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntDatum(1))),
			types.NewDecimalDatum(newMyDecimal(c, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorDecToInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_FloorReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewFloat64Datum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewDecimalDatum(newMyDecimal(c, "1")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewStringDatum("1"))),
			types.NewStringDatum("1"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewDurationDatum(newDuration(time.Second)),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CoalesceTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewTimeDatum(newDateTime(c, "2000-01-01")))),
			types.NewTimeDatum(newDateTime(c, "2000-01-01")),
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
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_RealIsFalse, &tipb.IsTrueOrFalseMetadata{KeepNull: false},
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewIntDatum(0),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_DecimalIsFalse, &tipb.IsTrueOrFalseMetadata{KeepNull: false},
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewIntDatum(0),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_RealIsTrue, &tipb.IsTrueOrFalseMetadata{KeepNull: false},
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExprWithMetaData(tipb.ScalarFuncSig_DecimalIsTrue, &tipb.IsTrueOrFalseMetadata{KeepNull: false},
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewIntDatum(1),
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		expr, err := PBToExpr(tt.expr, fieldTps, sc)
		c.Assert(err, IsNil)
		result, err := expr.Eval(row)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, tt.result.Kind())
		cmp, err := result.CompareDatum(sc, &tt.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func datumExpr(c *C, d types.Datum) *tipb.Expr {
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
		c.Assert(err, IsNil)
	case types.KindMysqlJSON:
		expr.Tp = tipb.ExprType_MysqlJson
		var err error
		expr.Val = make([]byte, 0, 1024)
		expr.Val, err = codec.EncodeValue(nil, expr.Val, d)
		c.Assert(err, IsNil)
	case types.KindMysqlTime:
		expr.Tp = tipb.ExprType_MysqlTime
		var err error
		expr.Val, err = codec.EncodeMySQLTime(nil, d.GetMysqlTime(), mysql.TypeUnspecified, nil)
		c.Assert(err, IsNil)
		expr.FieldType = ToPBFieldType(newDateFieldType())
	default:
		expr.Tp = tipb.ExprType_Null
	}
	return expr
}

func newJSONDatum(c *C, s string) (d types.Datum) {
	j, err := json.ParseBinaryFromString(s)
	c.Assert(err, IsNil)
	d.SetMysqlJSON(j)
	return d
}

func jsonDatumExpr(c *C, s string) *tipb.Expr {
	return datumExpr(c, newJSONDatum(c, s))
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
		Collate: collationToProto(ft.Collate),
	}
}

func newMyDecimal(c *C, s string) *types.MyDecimal {
	d := new(types.MyDecimal)
	c.Assert(d.FromString([]byte(s)), IsNil)
	return d
}

func newDuration(dur time.Duration) types.Duration {
	return types.Duration{
		Duration: dur,
		Fsp:      types.DefaultFsp,
	}
}

func newDateTime(c *C, s string) types.Time {
	t, err := types.ParseDate(nil, s)
	c.Assert(err, IsNil)
	return t
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
		Decimal: int(types.DefaultFsp),
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

func scalarFunctionExprWithMetaData(sigCode tipb.ScalarFuncSig, metadata proto.Message, retType *tipb.FieldType, args ...*tipb.Expr) *tipb.Expr {
	val, _ := proto.Marshal(metadata)
	return &tipb.Expr{
		Tp:        tipb.ExprType_ScalarFunc,
		Val:       val,
		Sig:       sigCode,
		Children:  args,
		FieldType: retType,
	}
}
