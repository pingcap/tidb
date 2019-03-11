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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
)

var _ = Suite(&testEvalSuite{})

type testEvalSuite struct {
	colID int64
}

func (s *testEvalSuite) SetUpSuite(c *C) {
	s.colID = 0
}

func (s *testEvalSuite) allocColID() int64 {
	s.colID++
	return s.colID
}

// TestEval test expr.Eval().
// TODO: add more tests.
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
			datumExpr(types.NewFloat32Datum(1.1)),
			types.NewFloat32Datum(1.1),
		},
		{
			datumExpr(types.NewFloat64Datum(1.1)),
			types.NewFloat64Datum(1.1),
		},
		{
			datumExpr(types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			datumExpr(types.NewUintDatum(1)),
			types.NewUintDatum(1),
		},
		{
			datumExpr(types.NewBytesDatum([]byte("abc"))),
			types.NewBytesDatum([]byte("abc")),
		},
		{
			datumExpr(types.NewStringDatum("abc")),
			types.NewStringDatum("abc"),
		},
		{
			datumExpr(types.Datum{}),
			types.Datum{},
		},
		{
			datumExpr(types.NewDurationDatum(types.Duration{Duration: time.Hour})),
			types.NewDurationDatum(types.Duration{Duration: time.Hour}),
		},
		{
			datumExpr(types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1))),
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
				jsonDatumExpr(`true`),
			),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType()),
				jsonDatumExpr(`[10, {"a": 20}]`),
			),
			types.NewIntDatum(3),
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

func buildExpr(tp tipb.ExprType, children ...interface{}) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tp
	expr.Children = make([]*tipb.Expr, len(children))
	for i, child := range children {
		switch x := child.(type) {
		case types.Datum:
			expr.Children[i] = datumExpr(x)
		case *tipb.Expr:
			expr.Children[i] = x
		}
	}
	return expr
}

func datumExpr(d types.Datum) *tipb.Expr {
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
		if err != nil {
			log.Warnf("err happened when EncodeDecimal in datumExpr:%s", err.Error())
		}
	case types.KindMysqlJSON:
		expr.Tp = tipb.ExprType_MysqlJson
		var err error
		expr.Val = make([]byte, 0, 1024)
		expr.Val, err = codec.EncodeValue(nil, expr.Val, d)
		if err != nil {
			log.Warnf("err happened when EncodeValue of JSON in datumExpr:%s", err.Error())
		}
	default:
		expr.Tp = tipb.ExprType_Null
	}
	return expr
}

func jsonDatumExpr(s string) *tipb.Expr {
	var d types.Datum
	j, err := json.ParseBinaryFromString(s)
	if err != nil {
		log.Warnf("err happened when json.ParseBinaryFromString in jsonDatumExpr:%s", err.Error())
	}
	d.SetMysqlJSON(j)
	return datumExpr(d)
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

func newIntFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flen:    mysql.MaxIntWidth,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
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
