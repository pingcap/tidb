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
	"encoding/json"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

// mockKvClient is mocked from tikv.CopClient to avoid circular dependency.
type mockKvClient struct {
}

// IsRequestTypeSupported implements the kv.Client interface..
func (c *mockKvClient) IsRequestTypeSupported(reqType, subType int64) bool {
	switch reqType {
	case kv.ReqTypeSelect, kv.ReqTypeIndex:
		switch subType {
		case kv.ReqSubTypeGroupBy, kv.ReqSubTypeBasic, kv.ReqSubTypeTopN:
			return true
		default:
			return c.supportExpr(tipb.ExprType(subType))
		}
	}
	return false
}

// Send implements the kv.Client interface..
func (c *mockKvClient) Send(ctx goctx.Context, req *kv.Request) kv.Response {
	return nil
}

func (c *mockKvClient) supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlTime, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_ColumnRef,
		tipb.ExprType_And, tipb.ExprType_Or,
		tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList,
		tipb.ExprType_Like, tipb.ExprType_Not:
		return true
	case tipb.ExprType_Plus, tipb.ExprType_Div:
		return true
	case tipb.ExprType_Case, tipb.ExprType_If:
		return true
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min, tipb.ExprType_Sum, tipb.ExprType_Avg:
		return true
	case tipb.ExprType_JsonType, tipb.ExprType_JsonExtract, tipb.ExprType_JsonUnquote, tipb.ExprType_JsonValid,
		tipb.ExprType_JsonObject, tipb.ExprType_JsonArray, tipb.ExprType_JsonMerge, tipb.ExprType_JsonSet,
		tipb.ExprType_JsonInsert, tipb.ExprType_JsonReplace, tipb.ExprType_JsonRemove, tipb.ExprType_JsonContains:
		return false
	case kv.ReqSubTypeDesc:
		return true
	default:
		return false
	}
}

type dataGen4Expr2PbTest struct {
}

func (dg *dataGen4Expr2PbTest) genColumn(tp byte, id int64) *Column {
	return &Column{
		RetType: types.NewFieldType(tp),
		ID:      id,
		Index:   int(id),
	}
}

func (s *testEvaluatorSuite) TestConstant2Pb(c *C) {
	var constExprs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)

	// can be transformed
	constValue := new(Constant)
	constValue.Value = types.NewDatum(nil)
	c.Assert(constValue.Value.Kind(), Equals, types.KindNull)
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(int64(100))
	c.Assert(constValue.Value.Kind(), Equals, types.KindInt64)
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(uint64(100))
	c.Assert(constValue.Value.Kind(), Equals, types.KindUint64)
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum("100")
	c.Assert(constValue.Value.Kind(), Equals, types.KindString)
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum([]byte{'1', '2', '4', 'c'})
	c.Assert(constValue.Value.Kind(), Equals, types.KindBytes)
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(types.NewDecFromInt(110))
	c.Assert(constValue.Value.Kind(), Equals, types.KindMysqlDecimal)
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(types.Duration{})
	c.Assert(constValue.Value.Kind(), Equals, types.KindMysqlDuration)
	constExprs = append(constExprs, constValue)

	// can not be transformed
	constValue = new(Constant)
	constValue.Value = types.NewDatum(float32(100))
	c.Assert(constValue.Value.Kind(), Equals, types.KindFloat32)
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(float64(100))
	c.Assert(constValue.Value.Kind(), Equals, types.KindFloat64)
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(types.Enum{Name: "A", Value: 19})
	c.Assert(constValue.Value.Kind(), Equals, types.KindMysqlEnum)
	constExprs = append(constExprs, constValue)

	pbExpr, pushed, remained := ExpressionsToPB(sc, constExprs, client)
	c.Assert(len(pushed), Equals, len(constExprs)-3)
	c.Assert(len(remained), Equals, 3)
	js, err := json.Marshal(pbExpr)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":0},{\"tp\":1,\"val\":\"gAAAAAAAAGQ=\"}]},{\"tp\":2,\"val\":\"AAAAAAAAAGQ=\"}]},{\"tp\":5,\"val\":\"MTAw\"}]},{\"tp\":6,\"val\":\"MTI0Yw==\"}]},{\"tp\":102,\"val\":\"AwCAbg==\"}]},{\"tp\":103,\"val\":\"gAAAAAAAAAA=\"}]}")

	pbExprs := ExpressionsToPBList(sc, constExprs, client)
	jsons := []string{
		"{\"tp\":0}",
		"{\"tp\":1,\"val\":\"gAAAAAAAAGQ=\"}",
		"{\"tp\":2,\"val\":\"AAAAAAAAAGQ=\"}",
		"{\"tp\":5,\"val\":\"MTAw\"}",
		"{\"tp\":6,\"val\":\"MTI0Yw==\"}",
		"{\"tp\":102,\"val\":\"AwCAbg==\"}",
		"{\"tp\":103,\"val\":\"gAAAAAAAAAA=\"}",
	}
	for i, pbExpr := range pbExprs {
		if i+3 < len(pbExprs) {
			js, err := json.Marshal(pbExpr)
			c.Assert(err, IsNil)
			c.Assert(string(js), Equals, jsons[i])
		} else {
			c.Assert(pbExpr, IsNil)
		}
	}
}

func (s *testEvaluatorSuite) TestColumn2Pb(c *C) {
	var colExprs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	colExprs = append(colExprs, dg.genColumn(mysql.TypeBit, 1))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeSet, 2))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeEnum, 3))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeGeometry, 4))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeUnspecified, 5))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeDecimal, 6))

	pbExpr, pushed, remained := ExpressionsToPB(sc, colExprs, client)
	c.Assert(pbExpr, IsNil)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, len(colExprs))

	pbExprs := ExpressionsToPBList(sc, colExprs, client)
	for _, pbExpr := range pbExprs {
		c.Assert(pbExpr, IsNil)
	}

	colExprs = colExprs[:0]
	colExprs = append(colExprs, dg.genColumn(mysql.TypeTiny, 1))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeShort, 2))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeLong, 3))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeFloat, 4))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeDouble, 5))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeNull, 6))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeTimestamp, 7))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeLonglong, 8))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeInt24, 9))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeDate, 10))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeDuration, 11))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeDatetime, 12))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeYear, 13))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeNewDate, 14))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeVarchar, 15))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeJSON, 16))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeNewDecimal, 17))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeTinyBlob, 18))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeMediumBlob, 19))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeLongBlob, 20))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeBlob, 21))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeVarString, 22))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeString, 23))
	pbExpr, pushed, remained = ExpressionsToPB(sc, colExprs, client)
	c.Assert(len(pushed), Equals, len(colExprs))
	c.Assert(len(remained), Equals, 0)
	js, err := json.Marshal(pbExpr)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAU=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAY=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAc=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAg=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAk=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAo=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAs=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAAw=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAA0=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAA4=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAAA8=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAABA=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAABE=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAABI=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAABM=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAABQ=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAABU=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAABY=\"}]},{\"tp\":201,\"val\":\"gAAAAAAAABc=\"}]}")

	pbExprs = ExpressionsToPBList(sc, colExprs, client)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAY=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAc=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAg=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAk=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAo=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAs=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAw=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA0=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA4=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA8=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABA=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABE=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABI=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABM=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABQ=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABU=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABY=\"}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABc=\"}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}

	for _, expr := range colExprs {
		expr.(*Column).ID = 0
		expr.(*Column).Index = 0
	}
	pbExpr, pushed, remained = ExpressionsToPB(sc, colExprs, client)
	c.Assert(pbExpr, IsNil)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, len(colExprs))

	pbExprs = ExpressionsToPBList(sc, colExprs, client)
	for _, pbExpr := range pbExprs {
		c.Assert(pbExpr, IsNil)
	}
}

func (s *testEvaluatorSuite) TestCompareFunc2Pb(c *C) {
	var compareExprs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.LT, ast.LE, ast.GT, ast.GE, ast.EQ, ast.NE, ast.NullEQ}
	for _, funcName := range funcNames {
		fc, err := NewFunction(mock.NewContext(), funcName, types.NewFieldType(mysql.TypeUnspecified), dg.genColumn(mysql.TypeLonglong, 1), dg.genColumn(mysql.TypeLonglong, 2))
		c.Assert(err, IsNil)
		compareExprs = append(compareExprs, fc)
	}

	pbExpr, pushed, remained := ExpressionsToPB(sc, compareExprs, client)
	c.Assert(pbExpr, NotNil)
	c.Assert(len(pushed), Equals, len(compareExprs))
	c.Assert(len(remained), Equals, 0)
	js, err := json.Marshal(pbExpr)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2001,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]},{\"tp\":2002,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}]},{\"tp\":2006,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}]},{\"tp\":2005,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}]},{\"tp\":2003,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}]},{\"tp\":2004,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}]},{\"tp\":2007,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}]}")

	pbExprs := ExpressionsToPBList(sc, compareExprs, client)
	c.Assert(len(pbExprs), Equals, len(compareExprs))
	jsons := []string{
		"{\"tp\":2001,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
		"{\"tp\":2002,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
		"{\"tp\":2006,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
		"{\"tp\":2005,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
		"{\"tp\":2003,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
		"{\"tp\":2004,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
		"{\"tp\":2007,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestInFunc2Pb(c *C) {
	var (
		compareExprs []Expression
		args         []Expression
	)
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	args = append(args, dg.genColumn(mysql.TypeLonglong, 1))
	args = append(args, &Constant{RetType: nil, Value: types.NewDatum(nil)})
	c.Assert(args[len(args)-1].(*Constant).Value.Kind(), Equals, types.KindNull)
	args = append(args, &Constant{RetType: nil, Value: types.NewDatum(uint64(100))})
	c.Assert(args[len(args)-1].(*Constant).Value.Kind(), Equals, types.KindUint64)
	args = append(args, &Constant{RetType: nil, Value: types.NewDatum("100")})
	c.Assert(args[len(args)-1].(*Constant).Value.Kind(), Equals, types.KindString)

	fc, err := NewFunction(mock.NewContext(), ast.In, types.NewFieldType(mysql.TypeUnspecified), args...)
	c.Assert(err, IsNil)
	compareExprs = append(compareExprs, fc)

	pbExpr, pushed, remained := ExpressionsToPB(sc, compareExprs, client)
	c.Assert(pbExpr, IsNil)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, 1)

	pbExprs := ExpressionsToPBList(sc, compareExprs, client)
	c.Assert(len(pbExprs), Equals, 1)
	c.Assert(pbExprs[0], IsNil)

	args = args[:3]
	args = append(args, &Constant{RetType: nil, Value: types.NewDatum(uint64(200))})
	c.Assert(args[len(args)-1].(*Constant).Value.Kind(), Equals, types.KindUint64)

	fc, err = NewFunction(mock.NewContext(), ast.In, types.NewFieldType(mysql.TypeUnspecified), args...)
	c.Assert(err, IsNil)
	compareExprs[0] = fc

	pbExpr, pushed, remained = ExpressionsToPB(sc, compareExprs, client)
	c.Assert(pbExpr, NotNil)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)
	js, err := json.Marshal(pbExpr)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":4001,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":151,\"val\":\"AAlkCcgB\"}]}")
}

func (s *testEvaluatorSuite) TestLikeFunc2Pb(c *C) {
	var likeFuncs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)

	retTp := types.NewFieldType(mysql.TypeString)
	retTp.Charset = charset.CharsetUTF8
	retTp.Collate = charset.CollationUTF8
	args := []Expression{
		&Constant{RetType: retTp, Value: types.NewDatum("string")},
		&Constant{RetType: retTp, Value: types.NewDatum("pattern")},
		&Constant{RetType: retTp, Value: types.NewDatum("%abc%")},
		&Constant{RetType: retTp, Value: types.NewDatum("\\")},
	}
	ctx := mock.NewContext()
	retTp = types.NewFieldType(mysql.TypeUnspecified)
	fc, err := NewFunction(ctx, ast.Like, retTp, args[0], args[1], args[3])
	c.Assert(err, IsNil)
	likeFuncs = append(likeFuncs, fc)

	fc, err = NewFunction(ctx, ast.Like, retTp, args[0], args[2], args[3])
	c.Assert(err, IsNil)
	likeFuncs = append(likeFuncs, fc)

	pbExprs := ExpressionsToPBList(sc, likeFuncs, client)
	for _, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, "null")
	}
}

func (s *testEvaluatorSuite) TestArithmeticalFunc2Pb(c *C) {
	var arithmeticalFuncs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.Mod, ast.IntDiv}
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			dg.genColumn(mysql.TypeLonglong, 1),
			dg.genColumn(mysql.TypeLonglong, 2))
		c.Assert(err, IsNil)
		arithmeticalFuncs = append(arithmeticalFuncs, fc)
	}

	jsons := make(map[string]string)
	jsons[ast.Plus] = "{\"tp\":2201,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}"
	jsons[ast.Div] = "{\"tp\":2204,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}"

	pbExprs := ExpressionsToPBList(sc, arithmeticalFuncs, client)
	for i, pbExpr := range pbExprs {
		switch funcNames[i] {
		case ast.Plus, ast.Div:
			c.Assert(pbExpr, NotNil)
			js, err := json.Marshal(pbExpr)
			c.Assert(err, IsNil)
			c.Assert(string(js), Equals, jsons[funcNames[i]])
		default:
			c.Assert(pbExpr, IsNil)
		}
	}
}

func (s *testEvaluatorSuite) TestLogicalFunc2Pb(c *C) {
	var logicalFuncs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.LogicAnd, ast.LogicOr, ast.LogicXor, ast.UnaryNot}
	for i, funcName := range funcNames {
		args := []Expression{dg.genColumn(mysql.TypeTiny, 1)}
		if i+1 < len(funcNames) {
			args = append(args, dg.genColumn(mysql.TypeTiny, 2))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			args...,
		)
		c.Assert(err, IsNil)
		logicalFuncs = append(logicalFuncs, fc)
	}

	pbExprs := ExpressionsToPBList(sc, logicalFuncs, client)
	jsons := []string{
		"{\"tp\":2301,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
		"{\"tp\":2302,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"}]}",
		"null",
		"{\"tp\":1001,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"}]}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestBitwiseFunc2Pb(c *C) {
	var bitwiseFuncs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.And, ast.Or, ast.Xor, ast.LeftShift, ast.RightShift, ast.BitNeg}
	for i, funcName := range funcNames {
		args := []Expression{dg.genColumn(mysql.TypeLong, 1)}
		if i+1 < len(funcNames) {
			args = append(args, dg.genColumn(mysql.TypeLong, 2))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			args...,
		)
		c.Assert(err, IsNil)
		bitwiseFuncs = append(bitwiseFuncs, fc)
	}

	pbExprs := ExpressionsToPBList(sc, bitwiseFuncs, client)
	for _, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, "null")
	}
}

func (s *testEvaluatorSuite) TestControlFunc2Pb(c *C) {
	var controlFuncs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{
		ast.Case,
		ast.If,
		ast.Ifnull,
		ast.Nullif,
	}
	for i, funcName := range funcNames {
		args := []Expression{dg.genColumn(mysql.TypeLong, 1)}
		args = append(args, dg.genColumn(mysql.TypeLong, 2))
		if i < 2 {
			args = append(args, dg.genColumn(mysql.TypeLong, 3))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			args...,
		)
		c.Assert(err, IsNil)
		controlFuncs = append(controlFuncs, fc)
	}

	pbExprs := ExpressionsToPBList(sc, controlFuncs, client)
	jsons := []string{
		"{\"tp\":4007,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\"}]}",
		"{\"tp\":3301,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\"},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\"}]}",
		"null",
		"null",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestOtherFunc2Pb(c *C) {
	var otherFuncs []Expression
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.Coalesce, ast.IsNull}
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			dg.genColumn(mysql.TypeLong, 1),
		)
		c.Assert(err, IsNil)
		otherFuncs = append(otherFuncs, fc)
	}

	pbExprs := ExpressionsToPBList(sc, otherFuncs, client)
	for _, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, "null")
	}
}

func (s *testEvaluatorSuite) TestGroupByItem2Pb(c *C) {
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genColumn(mysql.TypeDouble, 0)
	pbByItem := GroupByItemToPB(sc, client, item)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "null")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = GroupByItemToPB(sc, client, item)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"},\"desc\":false}")
}

func (s *testEvaluatorSuite) TestSortByItem2Pb(c *C) {
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genColumn(mysql.TypeDouble, 0)
	pbByItem := SortByItemToPB(sc, client, item, false)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "null")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, false)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, true)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestAggFunc2Pb(c *C) {
	sc := new(variable.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncGroupConcat, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow}
	for _, funcName := range funcNames {
		aggFunc := NewAggFunction(
			funcName,
			[]Expression{dg.genColumn(mysql.TypeDouble, 1)},
			true,
		)
		pbExpr := AggFuncToPBExpr(sc, client, aggFunc)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, "null")
	}

	jsons := []string{
		"{\"tp\":3002,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"}]}",
		"{\"tp\":3001,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"}]}",
		"{\"tp\":3003,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"}]}",
		"null",
		"{\"tp\":3005,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"}]}",
		"{\"tp\":3004,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"}]}",
		"{\"tp\":3006,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\"}]}",
	}
	for i, funcName := range funcNames {
		aggFunc := NewAggFunction(
			funcName,
			[]Expression{dg.genColumn(mysql.TypeDouble, 1)},
			false,
		)
		pbExpr := AggFuncToPBExpr(sc, client, aggFunc)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}
