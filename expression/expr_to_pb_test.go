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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
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

	pbExpr, pushed, remained := ExpressionsToPB(sc, constExprs, client)
	c.Assert(len(pushed), Equals, len(constExprs)-3)
	c.Assert(len(remained), Equals, 3)
	js, err := json.Marshal(pbExpr)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":2301,\"children\":[{\"tp\":0,\"sig\":0},{\"tp\":1,\"val\":\"gAAAAAAAAGQ=\",\"sig\":0}],\"sig\":0},{\"tp\":2,\"val\":\"AAAAAAAAAGQ=\",\"sig\":0}],\"sig\":0},{\"tp\":5,\"val\":\"MTAw\",\"sig\":0}],\"sig\":0},{\"tp\":6,\"val\":\"MTI0Yw==\",\"sig\":0}],\"sig\":0},{\"tp\":102,\"val\":\"AwCAbg==\",\"sig\":0}],\"sig\":0},{\"tp\":103,\"val\":\"gAAAAAAAAAA=\",\"sig\":0}],\"sig\":0}")

	pbExprs := ExpressionsToPBList(sc, constExprs, client)
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
	c.Assert(string(js), Equals, "{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":2,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":4,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":6,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAc=\",\"sig\":0,\"field_type\":{\"tp\":7,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAg=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAk=\",\"sig\":0,\"field_type\":{\"tp\":9,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAo=\",\"sig\":0,\"field_type\":{\"tp\":10,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAs=\",\"sig\":0,\"field_type\":{\"tp\":11,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAw=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAA0=\",\"sig\":0,\"field_type\":{\"tp\":13,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAA8=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAABA=\",\"sig\":0,\"field_type\":{\"tp\":245,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAABE=\",\"sig\":0,\"field_type\":{\"tp\":246,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAABI=\",\"sig\":0,\"field_type\":{\"tp\":249,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAABM=\",\"sig\":0,\"field_type\":{\"tp\":250,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAABQ=\",\"sig\":0,\"field_type\":{\"tp\":251,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAABU=\",\"sig\":0,\"field_type\":{\"tp\":252,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAABY=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAABc=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}")
	pbExprs = ExpressionsToPBList(sc, colExprs, client)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":2,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":4,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":6,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAc=\",\"sig\":0,\"field_type\":{\"tp\":7,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAg=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAk=\",\"sig\":0,\"field_type\":{\"tp\":9,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAo=\",\"sig\":0,\"field_type\":{\"tp\":10,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAs=\",\"sig\":0,\"field_type\":{\"tp\":11,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAw=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA0=\",\"sig\":0,\"field_type\":{\"tp\":13,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA8=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABA=\",\"sig\":0,\"field_type\":{\"tp\":245,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABE=\",\"sig\":0,\"field_type\":{\"tp\":246,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABI=\",\"sig\":0,\"field_type\":{\"tp\":249,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABM=\",\"sig\":0,\"field_type\":{\"tp\":250,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABQ=\",\"sig\":0,\"field_type\":{\"tp\":251,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABU=\",\"sig\":0,\"field_type\":{\"tp\":252,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABY=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABc=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err = json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i], Commentf("%v\n", i))
	}

	for _, expr := range colExprs {
		expr.(*Column).ID = 0
		expr.(*Column).Index = 0
	}
	pbExpr, pushed, remained = ExpressionsToPB(sc, colExprs, client)
	c.Assert(pbExpr, NotNil)
	js, err = json.Marshal(pbExpr)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":2,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":4,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":6,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":7,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":9,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":10,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":11,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":13,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":245,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":246,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":249,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":250,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":251,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":252,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}")
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

	pbExpr, pushed, remained := ExpressionsToPB(sc, compareExprs, client)
	c.Assert(pbExpr, NotNil)
	c.Assert(len(pushed), Equals, len(compareExprs))
	c.Assert(len(remained), Equals, 0)
	js, err := json.Marshal(pbExpr)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":100,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":110,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":120,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":130,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":140,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":150,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}},{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":160,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}")

	pbExprs := ExpressionsToPBList(sc, compareExprs, client)
	c.Assert(len(pbExprs), Equals, len(compareExprs))
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":100,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":110,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":120,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":130,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":140,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":150,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":160,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
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

	pbExprs := ExpressionsToPBList(sc, likeFuncs, client)
	for _, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, "null")
	}
}

func (s *testEvaluatorSuite) TestArithmeticalFunc2Pb(c *C) {
	var arithmeticalFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.Mod, ast.IntDiv}
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
	jsons[ast.Plus] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":200,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"}}"
	jsons[ast.Minus] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":204,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"}}"
	jsons[ast.Mul] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":208,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"}}"
	jsons[ast.Div] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":211,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"}}"

	pbExprs := ExpressionsToPBList(sc, arithmeticalFuncs, client)
	for i, pbExpr := range pbExprs {
		switch funcNames[i] {
		case ast.Mod, ast.IntDiv:
			c.Assert(pbExpr, IsNil, Commentf("%v\n", funcNames[i]))
		default:
			c.Assert(pbExpr, NotNil)
			js, err := json.Marshal(pbExpr)
			c.Assert(err, IsNil)
			c.Assert(string(js), Equals, jsons[funcNames[i]], Commentf("%v\n", funcNames[i]))
		}
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
	pbExprs := ExpressionsToPBList(sc, funcs, client)
	c.Assert(pbExprs[0], NotNil)
	js, err := json.Marshal(pbExprs[0])
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":6001,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":0,\"decimal\":-1,\"collate\":46,\"charset\":\"utf8mb4\"}}")
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

	pbExprs := ExpressionsToPBList(sc, logicalFuncs, client)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3102,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"null",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3104,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
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

	pbExprs := ExpressionsToPBList(sc, bitwiseFuncs, client)
	for _, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, "null")
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

	pbExprs := ExpressionsToPBList(sc, controlFuncs, client)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":4208,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"collate\":46,\"charset\":\"\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":4107,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":4101,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
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

	pbExprs := ExpressionsToPBList(sc, otherFuncs, client)
	jsons := map[string]string{
		ast.Coalesce: "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":4201,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":0,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}",
		ast.IsNull:   "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}}],\"sig\":3116,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[funcNames[i]])
	}
}

func (s *testEvaluatorSuite) TestGroupByItem2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genColumn(mysql.TypeDouble, 0)
	pbByItem := GroupByItemToPB(sc, client, item)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},\"desc\":false}")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = GroupByItemToPB(sc, client, item)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},\"desc\":false}")
}

func (s *testEvaluatorSuite) TestSortByItem2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genColumn(mysql.TypeDouble, 0)
	pbByItem := SortByItemToPB(sc, client, item, false)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},\"desc\":false}")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, false)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},\"desc\":false}")

	item = dg.genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, true)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":46,\"charset\":\"\"}},\"desc\":true}")
}
