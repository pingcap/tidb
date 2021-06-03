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
	"fmt"
	"strings"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
)

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
	c.Skip("constant pb has changed")
	var constExprs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

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

	pushed, remained := PushDownExprs(sc, constExprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(constExprs)-3)
	c.Assert(len(remained), Equals, 3)

	pbExprs, err := ExpressionsToPBList(sc, constExprs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":0,\"sig\":0}",
		"{\"tp\":1,\"val\":\"gAAAAAAAAGQ=\",\"sig\":0}",
		"{\"tp\":2,\"val\":\"AAAAAAAAAGQ=\",\"sig\":0}",
		"{\"tp\":5,\"val\":\"MTAw\",\"sig\":0}",
		"{\"tp\":6,\"val\":\"MTI0Yw==\",\"sig\":0}",
		"{\"tp\":102,\"val\":\"AwCAbg==\",\"sig\":0}",
		"{\"tp\":103,\"val\":\"gAAAAAAAAAA=\",\"sig\":0}",
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
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	colExprs = append(colExprs, dg.genColumn(mysql.TypeBit, 1))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeSet, 2))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeGeometry, 4))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeUnspecified, 5))

	pushed, remained := PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, len(colExprs))

	for _, col := range colExprs { // cannot be pushed down
		_, err := ExpressionsToPBList(sc, []Expression{col}, client)
		c.Assert(err, NotNil)
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
	colExprs = append(colExprs, dg.genColumn(mysql.TypeVarchar, 15))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeJSON, 16))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeNewDecimal, 17))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeTinyBlob, 18))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeMediumBlob, 19))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeLongBlob, 20))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeBlob, 21))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeVarString, 22))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeString, 23))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeEnum, 24))
	pushed, remained = PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(colExprs))
	c.Assert(len(remained), Equals, 0)

	pbExprs, err := ExpressionsToPBList(sc, colExprs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":2,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":4,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":6,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAc=\",\"sig\":0,\"field_type\":{\"tp\":7,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAg=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAk=\",\"sig\":0,\"field_type\":{\"tp\":9,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAo=\",\"sig\":0,\"field_type\":{\"tp\":10,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAs=\",\"sig\":0,\"field_type\":{\"tp\":11,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAw=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA0=\",\"sig\":0,\"field_type\":{\"tp\":13,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA8=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABA=\",\"sig\":0,\"field_type\":{\"tp\":245,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABE=\",\"sig\":0,\"field_type\":{\"tp\":246,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABI=\",\"sig\":0,\"field_type\":{\"tp\":249,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABM=\",\"sig\":0,\"field_type\":{\"tp\":250,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABQ=\",\"sig\":0,\"field_type\":{\"tp\":251,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABU=\",\"sig\":0,\"field_type\":{\"tp\":252,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABY=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABc=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABg=\",\"sig\":0,\"field_type\":{\"tp\":247,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i], Commentf("%v\n", i))
	}

	for _, expr := range colExprs {
		expr.(*Column).ID = 0
		expr.(*Column).Index = 0
	}

	pushed, remained = PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(colExprs))
	c.Assert(len(remained), Equals, 0)
}

func (s *testEvaluatorSuite) TestCompareFunc2Pb(c *C) {
	var compareExprs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.LT, ast.LE, ast.GT, ast.GE, ast.EQ, ast.NE, ast.NullEQ}
	for _, funcName := range funcNames {
		fc, err := NewFunction(mock.NewContext(), funcName, types.NewFieldType(mysql.TypeUnspecified), dg.genColumn(mysql.TypeLonglong, 1), dg.genColumn(mysql.TypeLonglong, 2))
		c.Assert(err, IsNil)
		compareExprs = append(compareExprs, fc)
	}

	pushed, remained := PushDownExprs(sc, compareExprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(compareExprs))
	c.Assert(len(remained), Equals, 0)

	pbExprs, err := ExpressionsToPBList(sc, compareExprs, client)
	c.Assert(err, IsNil)
	c.Assert(len(pbExprs), Equals, len(compareExprs))
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":100,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":110,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":120,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":130,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":140,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":150,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":160,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestLikeFunc2Pb(c *C) {
	var likeFuncs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	retTp := types.NewFieldType(mysql.TypeString)
	retTp.Flag |= mysql.NotNullFlag
	retTp.Charset = charset.CharsetUTF8
	retTp.Collate = charset.CollationUTF8
	args := []Expression{
		&Constant{RetType: retTp, Value: types.NewDatum("string")},
		&Constant{RetType: retTp, Value: types.NewDatum("pattern")},
		&Constant{RetType: retTp, Value: types.NewDatum(`%abc%`)},
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

	pbExprs, err := ExpressionsToPBList(sc, likeFuncs, client)
	c.Assert(err, IsNil)
	results := []string{
		`{"tp":10000,"children":[{"tp":5,"val":"c3RyaW5n","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"},"has_distinct":false},{"tp":5,"val":"cGF0dGVybg==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"},"has_distinct":false},{"tp":10000,"val":"CAA=","children":[{"tp":5,"val":"XA==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"},"has_distinct":false}],"sig":30,"field_type":{"tp":8,"flag":128,"flen":-1,"decimal":0,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":4310,"field_type":{"tp":8,"flag":524416,"flen":1,"decimal":0,"collate":63,"charset":"binary"},"has_distinct":false}`,
		`{"tp":10000,"children":[{"tp":5,"val":"c3RyaW5n","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"},"has_distinct":false},{"tp":5,"val":"JWFiYyU=","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"},"has_distinct":false},{"tp":10000,"val":"CAA=","children":[{"tp":5,"val":"XA==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"},"has_distinct":false}],"sig":30,"field_type":{"tp":8,"flag":128,"flen":-1,"decimal":0,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":4310,"field_type":{"tp":8,"flag":524416,"flen":1,"decimal":0,"collate":63,"charset":"binary"},"has_distinct":false}`,
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, results[i])
	}
}

func (s *testEvaluatorSuite) TestArithmeticalFunc2Pb(c *C) {
	var arithmeticalFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.Plus, ast.Minus, ast.Mul, ast.Div}
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			dg.genColumn(mysql.TypeDouble, 1),
			dg.genColumn(mysql.TypeDouble, 2))
		c.Assert(err, IsNil)
		arithmeticalFuncs = append(arithmeticalFuncs, fc)
	}

	jsons := make(map[string]string)
	jsons[ast.Plus] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":200,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}"
	jsons[ast.Minus] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":204,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}"
	jsons[ast.Mul] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":208,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}"
	jsons[ast.Div] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":211,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}"

	pbExprs, err := ExpressionsToPBList(sc, arithmeticalFuncs, client)
	c.Assert(err, IsNil)
	for i, pbExpr := range pbExprs {
		c.Assert(pbExpr, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[funcNames[i]], Commentf("%v\n", funcNames[i]))
	}

	funcNames = []string{ast.Mod, ast.IntDiv} // cannot be pushed down
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			dg.genColumn(mysql.TypeDouble, 1),
			dg.genColumn(mysql.TypeDouble, 2))
		c.Assert(err, IsNil)
		_, err = ExpressionsToPBList(sc, []Expression{fc}, client)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestDateFunc2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	fc, err := NewFunction(
		mock.NewContext(),
		ast.DateFormat,
		types.NewFieldType(mysql.TypeUnspecified),
		dg.genColumn(mysql.TypeDatetime, 1),
		dg.genColumn(mysql.TypeString, 2))
	c.Assert(err, IsNil)
	funcs := []Expression{fc}
	pbExprs, err := ExpressionsToPBList(sc, funcs, client)
	c.Assert(err, IsNil)
	c.Assert(pbExprs[0], NotNil)
	js, err := json.Marshal(pbExprs[0])
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":6001,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":0,\"decimal\":-1,\"collate\":46,\"charset\":\"utf8mb4\"},\"has_distinct\":false}")
}

func (s *testEvaluatorSuite) TestLogicalFunc2Pb(c *C) {
	var logicalFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
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

	pbExprs, err := ExpressionsToPBList(sc, logicalFuncs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3102,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3103,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3104,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestBitwiseFunc2Pb(c *C) {
	var bitwiseFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
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

	pbExprs, err := ExpressionsToPBList(sc, bitwiseFuncs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3118,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3119,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3120,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3129,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3130,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3121,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSerialSuites) TestPanicIfPbCodeUnspecified(c *C) {
	dg := new(dataGen4Expr2PbTest)
	args := []Expression{dg.genColumn(mysql.TypeLong, 1), dg.genColumn(mysql.TypeLong, 2)}
	fc, err := NewFunction(
		mock.NewContext(),
		ast.And,
		types.NewFieldType(mysql.TypeUnspecified),
		args...,
	)
	c.Assert(err, IsNil)
	fn := fc.(*ScalarFunction)
	fn.Function.setPbCode(tipb.ScalarFuncSig_Unspecified)
	c.Assert(fn.Function.PbCode(), Equals, tipb.ScalarFuncSig_Unspecified)

	pc := PbConverter{client: new(mock.Client), sc: new(stmtctx.StatementContext)}
	c.Assert(func() { pc.ExprToPB(fn) }, PanicMatches, "unspecified PbCode: .*")
}

func (s *testEvaluatorSerialSuites) TestPushDownSwitcher(c *C) {
	var funcs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	cases := []struct {
		name   string
		sig    tipb.ScalarFuncSig
		enable bool
	}{
		// Note that so far ScalarFuncSigs here are not be pushed down when the failpoint PushDownTestSwitcher
		// is disable, which is the prerequisite to pass this test.
		// Need to be replaced with other non pushed down ScalarFuncSigs if they are pushed down one day.
		{ast.Sin, tipb.ScalarFuncSig_Sin, true},
		{ast.Cos, tipb.ScalarFuncSig_Cos, false},
		{ast.Tan, tipb.ScalarFuncSig_Tan, true},
	}
	var enabled []string
	for _, funcName := range cases {
		args := []Expression{dg.genColumn(mysql.TypeLong, 1)}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName.name,
			types.NewFieldType(mysql.TypeUnspecified),
			args...,
		)
		c.Assert(err, IsNil)
		funcs = append(funcs, fc)
		if funcName.enable {
			enabled = append(enabled, funcName.name)
		}
	}

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/expression/PushDownTestSwitcher", `return("all")`), IsNil)
	defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/expression/PushDownTestSwitcher"), IsNil) }()

	pbExprs, err := ExpressionsToPBList(sc, funcs, client)
	c.Assert(err, IsNil)
	c.Assert(len(pbExprs), Equals, len(cases))
	for i, pbExpr := range pbExprs {
		c.Assert(pbExpr.Sig, Equals, cases[i].sig, Commentf("function: %s, sig: %v", cases[i].name, cases[i].sig))
	}

	// All disabled
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/expression/PushDownTestSwitcher", `return("")`), IsNil)
	pc := PbConverter{client: client, sc: sc}
	for i := range funcs {
		pbExpr := pc.ExprToPB(funcs[i])
		c.Assert(pbExpr, IsNil, Commentf("function: %s, sig: %v", cases[i].name, cases[i].sig))
	}

	// Partial enabled
	fpexpr := fmt.Sprintf(`return("%s")`, strings.Join(enabled, ","))
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/expression/PushDownTestSwitcher", fpexpr), IsNil)
	for i := range funcs {
		pbExpr := pc.ExprToPB(funcs[i])
		if !cases[i].enable {
			c.Assert(pbExpr, IsNil, Commentf("function: %s, sig: %v", cases[i].name, cases[i].sig))
			continue
		}
		c.Assert(pbExpr.Sig, Equals, cases[i].sig, Commentf("function: %s, sig: %v", cases[i].name, cases[i].sig))
	}
}

func (s *testEvaluatorSuite) TestControlFunc2Pb(c *C) {
	var controlFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{
		ast.Case,
		ast.If,
		ast.Ifnull,
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

	pbExprs, err := ExpressionsToPBList(sc, controlFuncs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":4208,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":4107,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":24,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":4101,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":24,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"null",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestOtherFunc2Pb(c *C) {
	var otherFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
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

	pbExprs, err := ExpressionsToPBList(sc, otherFuncs, client)
	c.Assert(err, IsNil)
	jsons := map[string]string{
		ast.Coalesce: "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":4201,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":0,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}",
		ast.IsNull:   "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false}],\"sig\":3116,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[funcNames[i]])
	}
}

func (s *testEvaluatorSuite) TestExprPushDownToFlash(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	exprs := make([]Expression, 0)

	jsonColumn := dg.genColumn(mysql.TypeJSON, 1)
	intColumn := dg.genColumn(mysql.TypeLonglong, 2)
	realColumn := dg.genColumn(mysql.TypeDouble, 3)
	decimalColumn := dg.genColumn(mysql.TypeNewDecimal, 4)
	stringColumn := dg.genColumn(mysql.TypeString, 5)
	datetimeColumn := dg.genColumn(mysql.TypeDatetime, 6)
	binaryStringColumn := dg.genColumn(mysql.TypeString, 7)
	binaryStringColumn.RetType.Collate = charset.CollationBin

	function, err := NewFunction(mock.NewContext(), ast.JSONLength, types.NewFieldType(mysql.TypeLonglong), jsonColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.If, types.NewFieldType(mysql.TypeLonglong), intColumn, intColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.BitNeg, types.NewFieldType(mysql.TypeLonglong), intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Xor, types.NewFieldType(mysql.TypeLonglong), intColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// ExtractDatetime: can be pushed
	function, err = NewFunction(mock.NewContext(), ast.Extract, types.NewFieldType(mysql.TypeLonglong), stringColumn, datetimeColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastIntAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastRealAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), realColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastDecimalAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), decimalColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastStringAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), stringColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastTimeAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), datetimeColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastIntAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeNewDecimal), intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastRealAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeNewDecimal), realColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastDecimalAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeNewDecimal), decimalColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastStringAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeNewDecimal), stringColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastTimeAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeNewDecimal), datetimeColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastIntAsString
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastRealAsString
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), realColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastDecimalAsString
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), decimalColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastStringAsString
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), stringColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastIntAsTime
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDatetime), intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastRealAsTime
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDatetime), realColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastDecimalAsTime
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDatetime), decimalColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// CastTimeAsTime
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDatetime), datetimeColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// Substring2ArgsUTF8
	function, err = NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), stringColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// Substring3ArgsUTF8
	function, err = NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), stringColumn, intColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// ScalarFuncSig_RoundReal
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeDouble), realColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// ScalarFuncSig_RoundInt
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeLonglong), intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// concat
	function, err = NewFunction(mock.NewContext(), ast.Concat, types.NewFieldType(mysql.TypeString), stringColumn, intColumn, realColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// UnixTimestampCurrent
	function, err = NewFunction(mock.NewContext(), ast.UnixTimestamp, types.NewFieldType(mysql.TypeLonglong))
	c.Assert(err, IsNil)
	_, ok := function.(*Constant)
	c.Assert(ok, IsTrue)

	// UnixTimestampInt
	datetimeColumn.RetType.Decimal = 0
	function, err = NewFunction(mock.NewContext(), ast.UnixTimestamp, types.NewFieldType(mysql.TypeLonglong), datetimeColumn)
	c.Assert(err, IsNil)
	c.Assert(function.(*ScalarFunction).Function.PbCode(), Equals, tipb.ScalarFuncSig_UnixTimestampInt)
	exprs = append(exprs, function)

	// UnixTimestampDecimal
	datetimeColumn.RetType.Decimal = types.UnspecifiedLength
	function, err = NewFunction(mock.NewContext(), ast.UnixTimestamp, types.NewFieldType(mysql.TypeNewDecimal), datetimeColumn)
	c.Assert(err, IsNil)
	c.Assert(function.(*ScalarFunction).Function.PbCode(), Equals, tipb.ScalarFuncSig_UnixTimestampDec)
	exprs = append(exprs, function)

	// StrToDateDateTime
	function, err = NewFunction(mock.NewContext(), ast.StrToDate, types.NewFieldType(mysql.TypeDatetime), stringColumn, stringColumn)
	c.Assert(err, IsNil)
	c.Assert(function.(*ScalarFunction).Function.PbCode(), Equals, tipb.ScalarFuncSig_StrToDateDatetime)
	exprs = append(exprs, function)

	canPush := CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	c.Assert(canPush, Equals, true)

	exprs = exprs[:0]

	// Substring2Args: can not be pushed
	function, err = NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), binaryStringColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// Substring3Args: can not be pushed
	function, err = NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), binaryStringColumn, intColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.JSONDepth, types.NewFieldType(mysql.TypeLonglong), jsonColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// ExtractDatetimeFromString: can not be pushed
	function, err = NewFunction(mock.NewContext(), ast.Extract, types.NewFieldType(mysql.TypeLonglong), stringColumn, stringColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	// RoundDecimal: can not be pushed
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeNewDecimal), decimalColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, kv.TiFlash)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, len(exprs))
}

func (s *testEvaluatorSuite) TestExprOnlyPushDownToFlash(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	exprs := make([]Expression, 0)

	//jsonColumn := dg.genColumn(mysql.TypeJSON, 1)
	intColumn := dg.genColumn(mysql.TypeLonglong, 2)
	//realColumn := dg.genColumn(mysql.TypeDouble, 3)
	decimalColumn := dg.genColumn(mysql.TypeNewDecimal, 4)
	stringColumn := dg.genColumn(mysql.TypeString, 5)
	datetimeColumn := dg.genColumn(mysql.TypeDatetime, 6)
	binaryStringColumn := dg.genColumn(mysql.TypeString, 7)
	binaryStringColumn.RetType.Collate = charset.CollationBin

	function, err := NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), stringColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Substring, types.NewFieldType(mysql.TypeString), stringColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.DateAdd, types.NewFieldType(mysql.TypeDatetime), datetimeColumn, intColumn, stringColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.TimestampDiff, types.NewFieldType(mysql.TypeLonglong), stringColumn, datetimeColumn, datetimeColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.FromUnixTime, types.NewFieldType(mysql.TypeDatetime), decimalColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Extract, types.NewFieldType(mysql.TypeLonglong), stringColumn, datetimeColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(exprs))
	c.Assert(len(remained), Equals, 0)

	canPush := CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	c.Assert(canPush, Equals, true)
	canPush = CanExprsPushDown(sc, exprs, client, kv.TiKV)
	c.Assert(canPush, Equals, false)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiFlash)
	c.Assert(len(pushed), Equals, len(exprs))
	c.Assert(len(remained), Equals, 0)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiKV)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, len(exprs))
}

func (s *testEvaluatorSuite) TestExprOnlyPushDownToTiKV(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	function, err := NewFunction(mock.NewContext(), "dayofyear", types.NewFieldType(mysql.TypeLonglong), dg.genColumn(mysql.TypeDatetime, 1))
	c.Assert(err, IsNil)
	var exprs = make([]Expression, 0)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)

	canPush := CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	c.Assert(canPush, Equals, false)
	canPush = CanExprsPushDown(sc, exprs, client, kv.TiKV)
	c.Assert(canPush, Equals, true)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiFlash)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, 1)
	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiKV)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)
}

func (s *testEvaluatorSuite) TestGroupByItem2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genColumn(mysql.TypeDouble, 0)
	pbByItem := GroupByItemToPB(sc, client, item)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},\"desc\":false}")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = GroupByItemToPB(sc, client, item)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},\"desc\":false}")
}

func (s *testEvaluatorSuite) TestSortByItem2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genColumn(mysql.TypeDouble, 0)
	pbByItem := SortByItemToPB(sc, client, item, false)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},\"desc\":false}")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, false)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},\"desc\":false}")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, true)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"},\"has_distinct\":false},\"desc\":true}")
}

func (s *testEvaluatorSerialSuites) TestMetadata(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/expression/PushDownTestSwitcher", `return("all")`), IsNil)
	defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/expression/PushDownTestSwitcher"), IsNil) }()

	pc := PbConverter{client: client, sc: sc}

	metadata := new(tipb.InUnionMetadata)
	var err error
	// InUnion flag is false in `BuildCastFunction` when `ScalarFuncSig_CastStringAsInt`
	cast := BuildCastFunction(mock.NewContext(), dg.genColumn(mysql.TypeString, 1), types.NewFieldType(mysql.TypeLonglong))
	c.Assert(cast.(*ScalarFunction).Function.metadata(), DeepEquals, &tipb.InUnionMetadata{InUnion: false})
	expr := pc.ExprToPB(cast)
	c.Assert(expr.Sig, Equals, tipb.ScalarFuncSig_CastStringAsInt)
	c.Assert(len(expr.Val), Greater, 0)
	err = proto.Unmarshal(expr.Val, metadata)
	c.Assert(err, IsNil)
	c.Assert(metadata.InUnion, Equals, false)

	// InUnion flag is nil in `BuildCastFunction4Union` when `ScalarFuncSig_CastIntAsString`
	castInUnion := BuildCastFunction4Union(mock.NewContext(), dg.genColumn(mysql.TypeLonglong, 1), types.NewFieldType(mysql.TypeString))
	c.Assert(castInUnion.(*ScalarFunction).Function.metadata(), IsNil)
	expr = pc.ExprToPB(castInUnion)
	c.Assert(expr.Sig, Equals, tipb.ScalarFuncSig_CastIntAsString)
	c.Assert(len(expr.Val), Equals, 0)

	// InUnion flag is true in `BuildCastFunction4Union` when `ScalarFuncSig_CastStringAsInt`
	castInUnion = BuildCastFunction4Union(mock.NewContext(), dg.genColumn(mysql.TypeString, 1), types.NewFieldType(mysql.TypeLonglong))
	c.Assert(castInUnion.(*ScalarFunction).Function.metadata(), DeepEquals, &tipb.InUnionMetadata{InUnion: true})
	expr = pc.ExprToPB(castInUnion)
	c.Assert(expr.Sig, Equals, tipb.ScalarFuncSig_CastStringAsInt)
	c.Assert(len(expr.Val), Greater, 0)
	err = proto.Unmarshal(expr.Val, metadata)
	c.Assert(err, IsNil)
	c.Assert(metadata.InUnion, Equals, true)
}

func columnCollation(c *Column, coll string) *Column {
	c.RetType.Collate = coll
	return c
}

func (s *testEvaluatorSerialSuites) TestNewCollationsEnabled(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	var colExprs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	colExprs = colExprs[:0]
	colExprs = append(colExprs, dg.genColumn(mysql.TypeVarchar, 1))
	colExprs = append(colExprs, columnCollation(dg.genColumn(mysql.TypeVarchar, 2), "some_invalid_collation"))
	colExprs = append(colExprs, columnCollation(dg.genColumn(mysql.TypeVarString, 3), "utf8mb4_general_ci"))
	colExprs = append(colExprs, columnCollation(dg.genColumn(mysql.TypeString, 4), "utf8mb4_0900_ai_ci"))
	colExprs = append(colExprs, columnCollation(dg.genColumn(mysql.TypeVarchar, 5), "utf8_bin"))
	colExprs = append(colExprs, columnCollation(dg.genColumn(mysql.TypeVarchar, 6), "utf8_unicode_ci"))
	colExprs = append(colExprs, columnCollation(dg.genColumn(mysql.TypeVarchar, 7), "utf8mb4_zh_pinyin_tidb_as_cs"))
	pushed, _ := PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(colExprs))
	pbExprs, err := ExpressionsToPBList(sc, colExprs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-45,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-255,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-83,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-192,\"charset\":\"\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAc=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-2048,\"charset\":\"\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i], Commentf("%v\n", i))
	}

	item := columnCollation(dg.genColumn(mysql.TypeDouble, 0), "utf8mb4_0900_ai_ci")
	pbByItem := GroupByItemToPB(sc, client, item)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-255,\"charset\":\"\"},\"has_distinct\":false},\"desc\":false}")
}

func (s *testEvalSerialSuite) TestPushCollationDown(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	dg := new(dataGen4Expr2PbTest)
	fc, err := NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeUnspecified), dg.genColumn(mysql.TypeVarchar, 0), dg.genColumn(mysql.TypeVarchar, 1))
	c.Assert(err, IsNil)
	client := new(mock.Client)
	sc := new(stmtctx.StatementContext)

	tps := []*types.FieldType{types.NewFieldType(mysql.TypeVarchar), types.NewFieldType(mysql.TypeVarchar)}
	for _, coll := range []string{charset.CollationBin, charset.CollationLatin1, charset.CollationUTF8, charset.CollationUTF8MB4} {
		fc.SetCharsetAndCollation("binary", coll) // only collation matters
		pbExpr, err := ExpressionsToPBList(sc, []Expression{fc}, client)
		c.Assert(err, IsNil)
		expr, err := PBToExpr(pbExpr[0], tps, sc)
		c.Assert(err, IsNil)
		_, eColl := expr.CharsetAndCollation(nil)
		c.Assert(eColl, Equals, coll)
	}
}
