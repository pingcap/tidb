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
	"github.com/pingcap/errors"
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

func init() {
	fpname := "github.com/pingcap/tidb/expression/PanicIfPbCodeUnspecified"
	err := failpoint.Enable(fpname, "return(true)")
	if err != nil {
		panic(errors.Errorf("enable global failpoint `%s` failed: %v", fpname, err))
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
	colExprs = append(colExprs, dg.genColumn(mysql.TypeEnum, 3))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeGeometry, 4))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeUnspecified, 5))
	colExprs = append(colExprs, dg.genColumn(mysql.TypeDecimal, 6))

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
	pushed, remained = PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(colExprs))
	c.Assert(len(remained), Equals, 0)

	pbExprs, err := ExpressionsToPBList(sc, colExprs, client)
	c.Assert(err, IsNil)
	binaryID := 63
	utf8mb4BinID := 46
	if collate.NewCollationEnabled() {
		binaryID = -63
		utf8mb4BinID = -46
	}

	jsons := []string{
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":2,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":4,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":6,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAc=\",\"sig\":0,\"field_type\":{\"tp\":7,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAg=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAk=\",\"sig\":0,\"field_type\":{\"tp\":9,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAo=\",\"sig\":0,\"field_type\":{\"tp\":10,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAs=\",\"sig\":0,\"field_type\":{\"tp\":11,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAAw=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAA0=\",\"sig\":0,\"field_type\":{\"tp\":13,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAAA8=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", utf8mb4BinID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAABA=\",\"sig\":0,\"field_type\":{\"tp\":245,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAABE=\",\"sig\":0,\"field_type\":{\"tp\":246,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAABI=\",\"sig\":0,\"field_type\":{\"tp\":249,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAABM=\",\"sig\":0,\"field_type\":{\"tp\":250,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAABQ=\",\"sig\":0,\"field_type\":{\"tp\":251,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAABU=\",\"sig\":0,\"field_type\":{\"tp\":252,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", binaryID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAABY=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", utf8mb4BinID),
		fmt.Sprintf("{\"tp\":201,\"val\":\"gAAAAAAAABc=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}", utf8mb4BinID),
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
	collateID := 63
	if collate.NewCollationEnabled() {
		collateID = -63
	}
	jsons := []string{
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":100,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":110,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":120,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":130,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":140,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":150,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":160,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
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
		`{"tp":10000,"children":[{"tp":5,"val":"c3RyaW5n","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"}},{"tp":5,"val":"cGF0dGVybg==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"}},{"tp":10000,"val":"CAA=","children":[{"tp":5,"val":"XA==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"}}],"sig":30,"field_type":{"tp":8,"flag":128,"flen":-1,"decimal":0,"collate":63,"charset":"binary"}}],"sig":4310,"field_type":{"tp":8,"flag":128,"flen":1,"decimal":0,"collate":63,"charset":"binary"}}`,
		`{"tp":10000,"children":[{"tp":5,"val":"c3RyaW5n","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"}},{"tp":5,"val":"JWFiYyU=","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"}},{"tp":10000,"val":"CAA=","children":[{"tp":5,"val":"XA==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":83,"charset":"utf8"}}],"sig":30,"field_type":{"tp":8,"flag":128,"flen":-1,"decimal":0,"collate":63,"charset":"binary"}}],"sig":4310,"field_type":{"tp":8,"flag":128,"flen":1,"decimal":0,"collate":63,"charset":"binary"}}`,
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
	collateID := 63
	if collate.NewCollationEnabled() {
		collateID = -63
	}
	jsons[ast.Plus] = fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":200,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID)
	jsons[ast.Minus] = fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":204,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID)
	jsons[ast.Mul] = fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":208,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID)
	jsons[ast.Div] = fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":211,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID)

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
	if collate.NewCollationEnabled() {
		c.Assert(string(js), Equals, "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"\"}}],\"sig\":6001,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":0,\"decimal\":-1,\"collate\":-46,\"charset\":\"utf8mb4\"}}")
	} else {
		c.Assert(string(js), Equals, "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":6001,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":0,\"decimal\":-1,\"collate\":46,\"charset\":\"utf8mb4\"}}")
	}
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
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}}],\"sig\":3102,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}}],\"sig\":3103,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}}],\"sig\":3104,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
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
	collateID := 63
	if collate.NewCollationEnabled() {
		collateID = -63
	}
	jsons := []string{
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":3118,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":3119,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":3120,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":3129,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":3130,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":3121,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID),
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
		{ast.And, tipb.ScalarFuncSig_BitAndSig, true},
		{ast.Or, tipb.ScalarFuncSig_BitOrSig, false},
		{ast.UnaryNot, tipb.ScalarFuncSig_UnaryNotInt, true},
	}
	var enabled []string
	for i, funcName := range cases {
		args := []Expression{dg.genColumn(mysql.TypeLong, 1)}
		if i+1 < len(cases) {
			args = append(args, dg.genColumn(mysql.TypeLong, 2))
		}
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
	collateID := 63
	if collate.NewCollationEnabled() {
		collateID = -63
	}
	jsons := []string{
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":4208,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"collate\":%d,\"charset\":\"\"}}", collateID, collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":4107,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID, collateID),
		fmt.Sprintf("{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":%d,\"charset\":\"\"}}],\"sig\":4101,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"collate\":%d,\"charset\":\"binary\"}}", collateID, collateID, collateID),
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
		ast.Coalesce: "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}}],\"sig\":4201,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":0,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}}",
		ast.IsNull:   "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}}],\"sig\":3116,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
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

	canPush := CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	c.Assert(canPush, Equals, true)

	function, err = NewFunction(mock.NewContext(), ast.JSONDepth, types.NewFieldType(mysql.TypeLonglong), jsonColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)
	pushed, remained := PushDownExprs(sc, exprs, client, kv.TiFlash)
	c.Assert(len(pushed), Equals, len(exprs)-1)
	c.Assert(len(remained), Equals, 1)
}

func (s *testEvaluatorSuite) TestExprOnlyPushDownToFlash(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	function, err := NewFunction(mock.NewContext(), ast.TimestampDiff, types.NewFieldType(mysql.TypeLonglong),
		dg.genColumn(mysql.TypeString, 1), dg.genColumn(mysql.TypeDatetime, 2), dg.genColumn(mysql.TypeDatetime, 3))
	c.Assert(err, IsNil)
	var exprs = make([]Expression, 0)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)

	canPush := CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	c.Assert(canPush, Equals, true)
	canPush = CanExprsPushDown(sc, exprs, client, kv.TiKV)
	c.Assert(canPush, Equals, false)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiFlash)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiKV)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, 1)
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
	if collate.NewCollationEnabled() {
		c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"\"}},\"desc\":false}")
	} else {
		c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},\"desc\":false}")
	}

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = GroupByItemToPB(sc, client, item)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	if collate.NewCollationEnabled() {
		c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"\"}},\"desc\":false}")
	} else {
		c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},\"desc\":false}")
	}
}

func (s *testEvaluatorSuite) TestSortByItem2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genColumn(mysql.TypeDouble, 0)
	pbByItem := SortByItemToPB(sc, client, item, false)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},\"desc\":false}")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, false)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},\"desc\":false}")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, true)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"\"}},\"desc\":true}")
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
	pushed, _ := PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(colExprs))
	pbExprs, err := ExpressionsToPBList(sc, colExprs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-45,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-255,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-83,\"charset\":\"\"}}",
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
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-255,\"charset\":\"\"}},\"desc\":false}")
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
