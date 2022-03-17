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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func genColumn(tp byte, id int64) *Column {
	return &Column{
		RetType: types.NewFieldType(tp),
		ID:      id,
		Index:   int(id),
	}
}

func TestConstant2Pb(t *testing.T) {
	t.Skip("constant pb has changed")
	var constExprs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	// can be transformed
	constValue := new(Constant)
	constValue.Value = types.NewDatum(nil)
	require.Equal(t, types.KindNull, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(int64(100))
	require.Equal(t, types.KindInt64, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(uint64(100))
	require.Equal(t, types.KindUint64, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum("100")
	require.Equal(t, types.KindString, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum([]byte{'1', '2', '4', 'c'})
	require.Equal(t, types.KindBytes, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(types.NewDecFromInt(110))
	require.Equal(t, types.KindMysqlDecimal, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(types.Duration{})
	require.Equal(t, types.KindMysqlDuration, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	// can not be transformed
	constValue = new(Constant)
	constValue.Value = types.NewDatum(float32(100))
	require.Equal(t, types.KindFloat32, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(float64(100))
	require.Equal(t, types.KindFloat64, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	constValue = new(Constant)
	constValue.Value = types.NewDatum(types.Enum{Name: "A", Value: 19})
	require.Equal(t, types.KindMysqlEnum, constValue.Value.Kind())
	constExprs = append(constExprs, constValue)

	pushed, remained := PushDownExprs(sc, constExprs, client, kv.UnSpecified)
	require.Len(t, pushed, len(constExprs)-3)
	require.Len(t, remained, 3)

	pbExprs, err := ExpressionsToPBList(sc, constExprs, client)
	require.NoError(t, err)
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
			require.NoError(t, err)
			require.Equal(t, jsons[i], string(js))
		} else {
			require.Nil(t, pbExpr)
		}
	}
}

func TestColumn2Pb(t *testing.T) {
	var colExprs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	colExprs = append(colExprs, genColumn(mysql.TypeSet, 1))
	colExprs = append(colExprs, genColumn(mysql.TypeGeometry, 2))
	colExprs = append(colExprs, genColumn(mysql.TypeUnspecified, 3))

	pushed, remained := PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	require.Len(t, pushed, 0)
	require.Len(t, remained, len(colExprs))

	for _, col := range colExprs { // cannot be pushed down
		_, err := ExpressionsToPBList(sc, []Expression{col}, client)
		require.Error(t, err)
	}

	colExprs = colExprs[:0]
	colExprs = append(colExprs, genColumn(mysql.TypeTiny, 1))
	colExprs = append(colExprs, genColumn(mysql.TypeShort, 2))
	colExprs = append(colExprs, genColumn(mysql.TypeLong, 3))
	colExprs = append(colExprs, genColumn(mysql.TypeFloat, 4))
	colExprs = append(colExprs, genColumn(mysql.TypeDouble, 5))
	colExprs = append(colExprs, genColumn(mysql.TypeNull, 6))
	colExprs = append(colExprs, genColumn(mysql.TypeTimestamp, 7))
	colExprs = append(colExprs, genColumn(mysql.TypeLonglong, 8))
	colExprs = append(colExprs, genColumn(mysql.TypeInt24, 9))
	colExprs = append(colExprs, genColumn(mysql.TypeDate, 10))
	colExprs = append(colExprs, genColumn(mysql.TypeDuration, 11))
	colExprs = append(colExprs, genColumn(mysql.TypeDatetime, 12))
	colExprs = append(colExprs, genColumn(mysql.TypeYear, 13))
	colExprs = append(colExprs, genColumn(mysql.TypeVarchar, 15))
	colExprs = append(colExprs, genColumn(mysql.TypeJSON, 16))
	colExprs = append(colExprs, genColumn(mysql.TypeNewDecimal, 17))
	colExprs = append(colExprs, genColumn(mysql.TypeTinyBlob, 18))
	colExprs = append(colExprs, genColumn(mysql.TypeMediumBlob, 19))
	colExprs = append(colExprs, genColumn(mysql.TypeLongBlob, 20))
	colExprs = append(colExprs, genColumn(mysql.TypeBlob, 21))
	colExprs = append(colExprs, genColumn(mysql.TypeVarString, 22))
	colExprs = append(colExprs, genColumn(mysql.TypeString, 23))
	colExprs = append(colExprs, genColumn(mysql.TypeEnum, 24))
	colExprs = append(colExprs, genColumn(mysql.TypeBit, 25))
	pushed, remained = PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	require.Len(t, pushed, len(colExprs))
	require.Len(t, remained, 0)

	pbExprs, err := ExpressionsToPBList(sc, colExprs, client)
	require.NoError(t, err)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":2,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":4,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":6,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAc=\",\"sig\":0,\"field_type\":{\"tp\":7,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAg=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAk=\",\"sig\":0,\"field_type\":{\"tp\":9,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAo=\",\"sig\":0,\"field_type\":{\"tp\":10,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAs=\",\"sig\":0,\"field_type\":{\"tp\":11,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAw=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA0=\",\"sig\":0,\"field_type\":{\"tp\":13,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA8=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"utf8mb4\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABA=\",\"sig\":0,\"field_type\":{\"tp\":245,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABE=\",\"sig\":0,\"field_type\":{\"tp\":246,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABI=\",\"sig\":0,\"field_type\":{\"tp\":249,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABM=\",\"sig\":0,\"field_type\":{\"tp\":250,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABQ=\",\"sig\":0,\"field_type\":{\"tp\":251,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABU=\",\"sig\":0,\"field_type\":{\"tp\":252,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABY=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"utf8mb4\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABc=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"utf8mb4\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABg=\",\"sig\":0,\"field_type\":{\"tp\":247,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABk=\",\"sig\":0,\"field_type\":{\"tp\":16,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		require.NotNil(t, pbExprs)
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equalf(t, jsons[i], string(js), "%v\n", i)
	}

	for _, expr := range colExprs {
		expr.(*Column).ID = 0
		expr.(*Column).Index = 0
	}

	pushed, remained = PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	require.Len(t, pushed, len(colExprs))
	require.Len(t, remained, 0)
}

func TestCompareFunc2Pb(t *testing.T) {
	var compareExprs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	funcNames := []string{ast.LT, ast.LE, ast.GT, ast.GE, ast.EQ, ast.NE, ast.NullEQ}
	for _, funcName := range funcNames {
		fc, err := NewFunction(mock.NewContext(), funcName, types.NewFieldType(mysql.TypeUnspecified), genColumn(mysql.TypeLonglong, 1), genColumn(mysql.TypeLonglong, 2))
		require.NoError(t, err)
		compareExprs = append(compareExprs, fc)
	}

	pushed, remained := PushDownExprs(sc, compareExprs, client, kv.UnSpecified)
	require.Len(t, pushed, len(compareExprs))
	require.Len(t, remained, 0)

	pbExprs, err := ExpressionsToPBList(sc, compareExprs, client)
	require.NoError(t, err)
	require.Len(t, pbExprs, len(compareExprs))
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":100,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":110,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":120,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":130,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":140,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":150,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":160,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		require.NotNil(t, pbExprs)
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equal(t, jsons[i], string(js))
	}
}

func TestLikeFunc2Pb(t *testing.T) {
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
	require.NoError(t, err)
	likeFuncs = append(likeFuncs, fc)

	fc, err = NewFunction(ctx, ast.Like, retTp, args[0], args[2], args[3])
	require.NoError(t, err)
	likeFuncs = append(likeFuncs, fc)

	pbExprs, err := ExpressionsToPBList(sc, likeFuncs, client)
	require.NoError(t, err)
	results := []string{
		`{"tp":10000,"children":[{"tp":5,"val":"c3RyaW5n","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":-83,"charset":"utf8"},"has_distinct":false},{"tp":5,"val":"cGF0dGVybg==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":-83,"charset":"utf8"},"has_distinct":false},{"tp":10000,"val":"CAA=","children":[{"tp":5,"val":"XA==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":-83,"charset":"utf8"},"has_distinct":false}],"sig":30,"field_type":{"tp":8,"flag":128,"flen":-1,"decimal":0,"collate":-83,"charset":"binary"},"has_distinct":false}],"sig":4310,"field_type":{"tp":8,"flag":524416,"flen":1,"decimal":0,"collate":-83,"charset":"binary"},"has_distinct":false}`,
		`{"tp":10000,"children":[{"tp":5,"val":"c3RyaW5n","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":-83,"charset":"utf8"},"has_distinct":false},{"tp":5,"val":"JWFiYyU=","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":-83,"charset":"utf8"},"has_distinct":false},{"tp":10000,"val":"CAA=","children":[{"tp":5,"val":"XA==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"collate":-83,"charset":"utf8"},"has_distinct":false}],"sig":30,"field_type":{"tp":8,"flag":128,"flen":-1,"decimal":0,"collate":-83,"charset":"binary"},"has_distinct":false}],"sig":4310,"field_type":{"tp":8,"flag":524416,"flen":1,"decimal":0,"collate":-83,"charset":"binary"},"has_distinct":false}`,
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equal(t, results[i], string(js))
	}
}

func TestArithmeticalFunc2Pb(t *testing.T) {
	var arithmeticalFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	funcNames := []string{ast.Plus, ast.Minus, ast.Mul, ast.Div}
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			genColumn(mysql.TypeDouble, 1),
			genColumn(mysql.TypeDouble, 2))
		require.NoError(t, err)
		arithmeticalFuncs = append(arithmeticalFuncs, fc)
	}

	jsons := make(map[string]string)
	jsons[ast.Plus] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":200,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}"
	jsons[ast.Minus] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":204,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}"
	jsons[ast.Mul] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":208,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}"
	jsons[ast.Div] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":211,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}"
	jsons[ast.Mod] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":215,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}"

	pbExprs, err := ExpressionsToPBList(sc, arithmeticalFuncs, client)
	require.NoError(t, err)
	for i, pbExpr := range pbExprs {
		require.NotNil(t, pbExpr)
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equalf(t, jsons[funcNames[i]], string(js), "%v\n", funcNames[i])
	}

	funcNames = []string{ast.IntDiv} // cannot be pushed down
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			genColumn(mysql.TypeDouble, 1),
			genColumn(mysql.TypeDouble, 2))
		require.NoError(t, err)
		_, err = ExpressionsToPBList(sc, []Expression{fc}, client)
		require.Error(t, err)
	}
}

func TestDateFunc2Pb(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	fc, err := NewFunction(
		mock.NewContext(),
		ast.DateFormat,
		types.NewFieldType(mysql.TypeUnspecified),
		genColumn(mysql.TypeDatetime, 1),
		genColumn(mysql.TypeString, 2))
	require.NoError(t, err)
	funcs := []Expression{fc}
	pbExprs, err := ExpressionsToPBList(sc, funcs, client)
	require.NoError(t, err)
	require.NotNil(t, pbExprs[0])
	js, err := json.Marshal(pbExprs[0])
	require.NoError(t, err)
	require.Equal(t, "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"utf8mb4\"},\"has_distinct\":false}],\"sig\":6001,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":0,\"decimal\":-1,\"collate\":-46,\"charset\":\"utf8mb4\"},\"has_distinct\":false}", string(js))
}

func TestLogicalFunc2Pb(t *testing.T) {
	var logicalFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	funcNames := []string{ast.LogicAnd, ast.LogicOr, ast.LogicXor, ast.UnaryNot}
	for i, funcName := range funcNames {
		args := []Expression{genColumn(mysql.TypeTiny, 1)}
		if i+1 < len(funcNames) {
			args = append(args, genColumn(mysql.TypeTiny, 2))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			args...,
		)
		require.NoError(t, err)
		logicalFuncs = append(logicalFuncs, fc)
	}

	pbExprs, err := ExpressionsToPBList(sc, logicalFuncs, client)
	require.NoError(t, err)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3102,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3103,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3104,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equal(t, jsons[i], string(js))
	}
}

func TestBitwiseFunc2Pb(t *testing.T) {
	var bitwiseFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	funcNames := []string{ast.And, ast.Or, ast.Xor, ast.LeftShift, ast.RightShift, ast.BitNeg}
	for i, funcName := range funcNames {
		args := []Expression{genColumn(mysql.TypeLong, 1)}
		if i+1 < len(funcNames) {
			args = append(args, genColumn(mysql.TypeLong, 2))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			args...,
		)
		require.NoError(t, err)
		bitwiseFuncs = append(bitwiseFuncs, fc)
	}

	pbExprs, err := ExpressionsToPBList(sc, bitwiseFuncs, client)
	require.NoError(t, err)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3118,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3119,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3120,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3129,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3130,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3121,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equal(t, jsons[i], string(js))
	}
}

func TestControlFunc2Pb(t *testing.T) {
	var controlFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	funcNames := []string{
		ast.Case,
		ast.If,
		ast.Ifnull,
	}
	for i, funcName := range funcNames {
		args := []Expression{genColumn(mysql.TypeLong, 1)}
		args = append(args, genColumn(mysql.TypeLong, 2))
		if i < 2 {
			args = append(args, genColumn(mysql.TypeLong, 3))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			args...,
		)
		require.NoError(t, err)
		controlFuncs = append(controlFuncs, fc)
	}

	pbExprs, err := ExpressionsToPBList(sc, controlFuncs, client)
	require.NoError(t, err)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":4208,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":4107,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":24,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":4101,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":24,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		"null",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equal(t, jsons[i], string(js))
	}
}

func TestOtherFunc2Pb(t *testing.T) {
	var otherFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	funcNames := []string{ast.Coalesce, ast.IsNull}
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(mysql.TypeUnspecified),
			genColumn(mysql.TypeLong, 1),
		)
		require.NoError(t, err)
		otherFuncs = append(otherFuncs, fc)
	}

	pbExprs, err := ExpressionsToPBList(sc, otherFuncs, client)
	require.NoError(t, err)
	jsons := map[string]string{
		ast.Coalesce: "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":4201,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":0,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
		ast.IsNull:   "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}],\"sig\":3116,\"field_type\":{\"tp\":8,\"flag\":524416,\"flen\":1,\"decimal\":0,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equal(t, jsons[funcNames[i]], string(js))
	}
}

func TestExprPushDownToFlash(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	exprs := make([]Expression, 0)

	jsonColumn := genColumn(mysql.TypeJSON, 1)
	intColumn := genColumn(mysql.TypeLonglong, 2)
	realColumn := genColumn(mysql.TypeDouble, 3)
	decimalColumn := genColumn(mysql.TypeNewDecimal, 4)
	stringColumn := genColumn(mysql.TypeString, 5)
	datetimeColumn := genColumn(mysql.TypeDatetime, 6)
	binaryStringColumn := genColumn(mysql.TypeString, 7)
	binaryStringColumn.RetType.Collate = charset.CollationBin
	int32Column := genColumn(mysql.TypeLong, 8)
	float32Column := genColumn(mysql.TypeFloat, 9)
	enumColumn := genColumn(mysql.TypeEnum, 10)

	function, err := NewFunction(mock.NewContext(), ast.JSONLength, types.NewFieldType(mysql.TypeLonglong), jsonColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// lpad
	function, err = NewFunction(mock.NewContext(), ast.Lpad, types.NewFieldType(mysql.TypeString), stringColumn, int32Column, stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// rpad
	function, err = NewFunction(mock.NewContext(), ast.Rpad, types.NewFieldType(mysql.TypeString), stringColumn, int32Column, stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.If, types.NewFieldType(mysql.TypeLonglong), intColumn, intColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.BitNeg, types.NewFieldType(mysql.TypeLonglong), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Xor, types.NewFieldType(mysql.TypeLonglong), intColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ExtractDatetime: can be pushed
	function, err = NewFunction(mock.NewContext(), ast.Extract, types.NewFieldType(mysql.TypeLonglong), stringColumn, datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastIntAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastRealAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastDecimalAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastStringAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastTimeAsInt
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	validDecimalType := types.NewFieldType(mysql.TypeNewDecimal)
	validDecimalType.Flen = 20
	validDecimalType.Decimal = 2
	// CastIntAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, validDecimalType, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastRealAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, validDecimalType, realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastDecimalAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, validDecimalType, decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastStringAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, validDecimalType, stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastTimeAsDecimal
	function, err = NewFunction(mock.NewContext(), ast.Cast, validDecimalType, datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastIntAsString
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastRealAsString
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastDecimalAsString
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastStringAsString
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastIntAsTime
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDatetime), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastRealAsTime
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDatetime), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastDecimalAsTime
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDatetime), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastTimeAsTime
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDatetime), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// CastStringAsReal
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDouble), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Substring2ArgsUTF8
	function, err = NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), stringColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Substring3ArgsUTF8
	function, err = NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), stringColumn, intColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// sqrt
	function, err = NewFunction(mock.NewContext(), ast.Sqrt, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_CeilReal
	function, err = NewFunction(mock.NewContext(), ast.Ceil, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_CeilIntToInt
	function, err = NewFunction(mock.NewContext(), ast.Ceil, types.NewFieldType(mysql.TypeLonglong), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_CeilDecimalToInt
	function, err = NewFunction(mock.NewContext(), ast.Ceil, types.NewFieldType(mysql.TypeLonglong), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_CeilDecToDec
	function, err = NewFunction(mock.NewContext(), ast.Ceil, types.NewFieldType(mysql.TypeNewDecimal), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_FloorReal
	function, err = NewFunction(mock.NewContext(), ast.Floor, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_FloorIntToInt
	function, err = NewFunction(mock.NewContext(), ast.Floor, types.NewFieldType(mysql.TypeLonglong), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_FloorDecToInt
	function, err = NewFunction(mock.NewContext(), ast.Floor, types.NewFieldType(mysql.TypeLonglong), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_FloorDecToDec
	function, err = NewFunction(mock.NewContext(), ast.Floor, types.NewFieldType(mysql.TypeNewDecimal), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Log1Arg
	function, err = NewFunction(mock.NewContext(), ast.Log, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Log2Args
	function, err = NewFunction(mock.NewContext(), ast.Log, types.NewFieldType(mysql.TypeDouble), realColumn, realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Log2
	function, err = NewFunction(mock.NewContext(), ast.Log2, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Log10
	function, err = NewFunction(mock.NewContext(), ast.Log10, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Exp
	function, err = NewFunction(mock.NewContext(), ast.Exp, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Pow
	function, err = NewFunction(mock.NewContext(), ast.Pow, types.NewFieldType(mysql.TypeDouble), realColumn, realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Radians
	function, err = NewFunction(mock.NewContext(), ast.Radians, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Degrees
	function, err = NewFunction(mock.NewContext(), ast.Degrees, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_CRC32
	function, err = NewFunction(mock.NewContext(), ast.CRC32, types.NewFieldType(mysql.TypeLonglong), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_Conv
	function, err = NewFunction(mock.NewContext(), ast.Conv, types.NewFieldType(mysql.TypeDouble), stringColumn, intColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Replace
	function, err = NewFunction(mock.NewContext(), ast.Replace, types.NewFieldType(mysql.TypeString), stringColumn, stringColumn, stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// InetAton
	function, err = NewFunction(mock.NewContext(), ast.InetAton, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// InetNtoa
	function, err = NewFunction(mock.NewContext(), ast.InetNtoa, types.NewFieldType(mysql.TypeLonglong), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Inet6Aton
	function, err = NewFunction(mock.NewContext(), ast.Inet6Aton, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Inet6Ntoa
	function, err = NewFunction(mock.NewContext(), ast.Inet6Ntoa, types.NewFieldType(mysql.TypeLonglong), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_RoundReal
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeDouble), realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_RoundInt
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeLonglong), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_RoundDecimal
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeNewDecimal), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_RoundWithFracReal
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeDouble), realColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_RoundWithFracInt
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeLonglong), intColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ScalarFuncSig_RoundWithFracDecimal
	function, err = NewFunction(mock.NewContext(), ast.Round, types.NewFieldType(mysql.TypeNewDecimal), decimalColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// concat
	function, err = NewFunction(mock.NewContext(), ast.Concat, types.NewFieldType(mysql.TypeString), stringColumn, intColumn, realColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// UnixTimestampCurrent
	function, err = NewFunction(mock.NewContext(), ast.UnixTimestamp, types.NewFieldType(mysql.TypeLonglong))
	require.NoError(t, err)
	_, ok := function.(*Constant)
	require.True(t, ok)

	// UnixTimestampInt
	datetimeColumn.RetType.Decimal = 0
	function, err = NewFunction(mock.NewContext(), ast.UnixTimestamp, types.NewFieldType(mysql.TypeLonglong), datetimeColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_UnixTimestampInt, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// UnixTimestampDecimal
	datetimeColumn.RetType.Decimal = types.UnspecifiedLength
	function, err = NewFunction(mock.NewContext(), ast.UnixTimestamp, types.NewFieldType(mysql.TypeNewDecimal), datetimeColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_UnixTimestampDec, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// Year
	function, err = NewFunction(mock.NewContext(), ast.Year, types.NewFieldType(mysql.TypeLonglong), datetimeColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_Year, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// Day
	function, err = NewFunction(mock.NewContext(), ast.Day, types.NewFieldType(mysql.TypeLonglong), datetimeColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_DayOfMonth, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// Datediff
	function, err = NewFunction(mock.NewContext(), ast.DateDiff, types.NewFieldType(mysql.TypeLonglong), datetimeColumn, datetimeColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_DateDiff, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// Datesub
	constStringColumn := new(Constant)
	constStringColumn.Value = types.NewStringDatum("day")
	constStringColumn.RetType = types.NewFieldType(mysql.TypeString)
	function, err = NewFunction(mock.NewContext(), ast.DateSub, types.NewFieldType(mysql.TypeDatetime), datetimeColumn, intColumn, constStringColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_SubDateDatetimeInt, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)
	function, err = NewFunction(mock.NewContext(), ast.DateSub, types.NewFieldType(mysql.TypeDatetime), stringColumn, intColumn, constStringColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_SubDateStringInt, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)
	function, err = NewFunction(mock.NewContext(), ast.SubDate, types.NewFieldType(mysql.TypeDatetime), datetimeColumn, intColumn, constStringColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_SubDateDatetimeInt, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// castTimeAsString:
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), datetimeColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_CastTimeAsString, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// concat_ws
	function, err = NewFunction(mock.NewContext(), ast.ConcatWS, types.NewFieldType(mysql.TypeString), stringColumn, stringColumn, stringColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_ConcatWS, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// StrToDateDateTime
	function, err = NewFunction(mock.NewContext(), ast.StrToDate, types.NewFieldType(mysql.TypeDatetime), stringColumn, stringColumn)
	require.NoError(t, err)
	require.Equal(t, tipb.ScalarFuncSig_StrToDateDatetime, function.(*ScalarFunction).Function.PbCode())
	exprs = append(exprs, function)

	// cast Int32 to Int32
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLong), int32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// cast float32 to float32
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeFloat), float32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// upper string
	function, err = NewFunction(mock.NewContext(), ast.Upper, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ucase string
	function, err = NewFunction(mock.NewContext(), ast.Ucase, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// lower string
	function, err = NewFunction(mock.NewContext(), ast.Lower, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// lcase string
	function, err = NewFunction(mock.NewContext(), ast.Lcase, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// sysdate
	function, err = NewFunction(mock.NewContext(), ast.Sysdate, types.NewFieldType(mysql.TypeDatetime), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	canPush := CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	require.Equal(t, true, canPush)

	exprs = exprs[:0]

	// Substring2Args: can not be pushed
	function, err = NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), binaryStringColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Substring3Args: can not be pushed
	function, err = NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), binaryStringColumn, intColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.JSONDepth, types.NewFieldType(mysql.TypeLonglong), jsonColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// ExtractDatetimeFromString: can not be pushed
	function, err = NewFunction(mock.NewContext(), ast.Extract, types.NewFieldType(mysql.TypeLonglong), stringColumn, stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Cast to Int32: not supported
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLong), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Cast to Float: not supported
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeFloat), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// Cast to invalid Decimal Type: not supported
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeNewDecimal), intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// cast Int32 to UInt32
	unsignedInt32Type := types.NewFieldType(mysql.TypeLong)
	unsignedInt32Type.Flag = mysql.UnsignedFlag
	function, err = NewFunction(mock.NewContext(), ast.Cast, unsignedInt32Type, int32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// cast Enum as String : not supported
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeString), enumColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, kv.TiFlash)
	require.Len(t, pushed, 0)
	require.Len(t, remained, len(exprs))

	pushed, remained = PushDownExprsWithExtraInfo(sc, exprs, client, kv.TiFlash, true)
	require.Len(t, pushed, 0)
	require.Len(t, remained, len(exprs))

	exprs = exprs[:0]
	// cast Enum as UInt : supported
	unsignedInt := types.NewFieldType(mysql.TypeLonglong)
	unsignedInt.Flag = mysql.UnsignedFlag
	function, err = NewFunction(mock.NewContext(), ast.Cast, unsignedInt, enumColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// cast Enum as Int : supported
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeLonglong), enumColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// cast Enum as Double : supported
	function, err = NewFunction(mock.NewContext(), ast.Cast, types.NewFieldType(mysql.TypeDouble), enumColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// cast Enum as Decimal : supported
	function, err = NewFunction(mock.NewContext(), ast.Cast, validDecimalType, enumColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// quarter: supported
	function, err = NewFunction(mock.NewContext(), ast.Quarter, types.NewFieldType(mysql.TypeLonglong), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// strcmp
	function, err = NewFunction(mock.NewContext(), ast.Strcmp, types.NewFieldType(mysql.TypeLonglong), stringColumn, stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// DayName: supported
	function, err = NewFunction(mock.NewContext(), ast.DayName, types.NewFieldType(mysql.TypeString), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// MonthName: supported
	function, err = NewFunction(mock.NewContext(), ast.MonthName, types.NewFieldType(mysql.TypeString), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// regexpUTF8: supported
	function, err = NewFunction(mock.NewContext(), ast.Regexp, types.NewFieldType(mysql.TypeLonglong), stringColumn, stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// regexp: supported
	function, err = NewFunction(mock.NewContext(), ast.Regexp, types.NewFieldType(mysql.TypeLonglong), binaryStringColumn, binaryStringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// greatest
	function, err = NewFunction(mock.NewContext(), ast.Greatest, types.NewFieldType(mysql.TypeLonglong), int32Column, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Greatest, types.NewFieldType(mysql.TypeDouble), float32Column, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// least
	function, err = NewFunction(mock.NewContext(), ast.Least, types.NewFieldType(mysql.TypeLonglong), int32Column, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Least, types.NewFieldType(mysql.TypeDouble), float32Column, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)
	// is true
	function, err = NewFunction(mock.NewContext(), ast.IsTruthWithoutNull, types.NewFieldType(mysql.TypeLonglong), int32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)
	function, err = NewFunction(mock.NewContext(), ast.IsTruthWithoutNull, types.NewFieldType(mysql.TypeLonglong), float32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)
	function, err = NewFunction(mock.NewContext(), ast.IsTruthWithoutNull, types.NewFieldType(mysql.TypeLonglong), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)
	// is true with null
	function, err = NewFunction(mock.NewContext(), ast.IsTruthWithNull, types.NewFieldType(mysql.TypeLonglong), int32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)
	function, err = NewFunction(mock.NewContext(), ast.IsTruthWithNull, types.NewFieldType(mysql.TypeLonglong), float32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)
	function, err = NewFunction(mock.NewContext(), ast.IsTruthWithNull, types.NewFieldType(mysql.TypeLonglong), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)
	// is false, note seems there is actually no is_false_with_null, so unable to add ut for it
	function, err = NewFunction(mock.NewContext(), ast.IsFalsity, types.NewFieldType(mysql.TypeLonglong), int32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)
	function, err = NewFunction(mock.NewContext(), ast.IsFalsity, types.NewFieldType(mysql.TypeLonglong), float32Column)
	require.NoError(t, err)
	exprs = append(exprs, function)
	function, err = NewFunction(mock.NewContext(), ast.IsFalsity, types.NewFieldType(mysql.TypeLonglong), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// DayOfWeek
	function, err = NewFunction(mock.NewContext(), ast.DayOfWeek, types.NewFieldType(mysql.TypeDatetime), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// DayOfMonth
	function, err = NewFunction(mock.NewContext(), ast.DayOfMonth, types.NewFieldType(mysql.TypeDatetime), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// DayOfYear
	function, err = NewFunction(mock.NewContext(), ast.DayOfYear, types.NewFieldType(mysql.TypeDatetime), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	// LastDay
	function, err = NewFunction(mock.NewContext(), ast.LastDay, types.NewFieldType(mysql.TypeDatetime), datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiFlash)
	require.Len(t, pushed, len(exprs))
	require.Len(t, remained, 0)

	pushed, remained = PushDownExprsWithExtraInfo(sc, exprs, client, kv.TiFlash, true)
	require.Len(t, pushed, len(exprs))
	require.Len(t, remained, 0)
}

func TestExprOnlyPushDownToFlash(t *testing.T) {
	t.Skip("Skip this unstable test temporarily and bring it back before 2021-07-26")
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	exprs := make([]Expression, 0)

	//jsonColumn := genColumn(mysql.TypeJSON, 1)
	intColumn := genColumn(mysql.TypeLonglong, 2)
	//realColumn := genColumn(mysql.TypeDouble, 3)
	decimalColumn := genColumn(mysql.TypeNewDecimal, 4)
	stringColumn := genColumn(mysql.TypeString, 5)
	datetimeColumn := genColumn(mysql.TypeDatetime, 6)
	binaryStringColumn := genColumn(mysql.TypeString, 7)
	binaryStringColumn.RetType.Collate = charset.CollationBin

	function, err := NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), stringColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Substring, types.NewFieldType(mysql.TypeString), stringColumn, intColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.TimestampDiff, types.NewFieldType(mysql.TypeLonglong), stringColumn, datetimeColumn, datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.FromUnixTime, types.NewFieldType(mysql.TypeDatetime), decimalColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Extract, types.NewFieldType(mysql.TypeLonglong), stringColumn, datetimeColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, kv.UnSpecified)
	require.Len(t, pushed, len(exprs))
	require.Len(t, remained, 0)

	canPush := CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	require.Equal(t, true, canPush)
	canPush = CanExprsPushDown(sc, exprs, client, kv.TiKV)
	require.Equal(t, false, canPush)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiFlash)
	require.Len(t, pushed, len(exprs))
	require.Len(t, remained, 0)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiKV)
	require.Len(t, pushed, 0)
	require.Len(t, remained, len(exprs))
}

func TestExprPushDownToTiKV(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	exprs := make([]Expression, 0)

	//jsonColumn := genColumn(mysql.TypeJSON, 1)
	intColumn := genColumn(mysql.TypeLonglong, 2)
	//realColumn := genColumn(mysql.TypeDouble, 3)
	//decimalColumn := genColumn(mysql.TypeNewDecimal, 4)
	stringColumn := genColumn(mysql.TypeString, 5)
	//datetimeColumn := genColumn(mysql.TypeDatetime, 6)
	binaryStringColumn := genColumn(mysql.TypeString, 7)
	dateColumn := genColumn(mysql.TypeDate, 8)
	binaryStringColumn.RetType.Collate = charset.CollationBin

	// Test exprs that cannot be pushed.
	function, err := NewFunction(mock.NewContext(), ast.InetAton, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.InetNtoa, types.NewFieldType(mysql.TypeLonglong), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Inet6Aton, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Inet6Ntoa, types.NewFieldType(mysql.TypeLonglong), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.IsIPv4, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.IsIPv6, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.IsIPv4Compat, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.IsIPv4Mapped, types.NewFieldType(mysql.TypeString), stringColumn)
	require.NoError(t, err)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, kv.TiKV)
	require.Len(t, pushed, 0)
	require.Len(t, remained, len(exprs))

	// Test exprs that can be pushed.
	exprs = exprs[:0]
	pushed = pushed[:0]
	remained = remained[:0]

	substringRelated := []string{ast.Substr, ast.Substring, ast.Mid}
	for _, exprName := range substringRelated {
		function, err = NewFunction(mock.NewContext(), exprName, types.NewFieldType(mysql.TypeString), stringColumn, intColumn, intColumn)
		require.NoError(t, err)
		exprs = append(exprs, function)
	}

	testcases := []struct {
		functionName string
		retType      *types.FieldType
		args         []Expression
	}{
		{
			functionName: ast.CharLength,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.Right,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn, intColumn},
		},
		{
			functionName: ast.Left,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn, intColumn},
		},
		{
			functionName: ast.Sin,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.Asin,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.Cos,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.Acos,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.Tan,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.Atan,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.Cot,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.Atan2,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn, intColumn},
		},
		{
			functionName: ast.DateFormat,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn, stringColumn},
		},
		{
			functionName: ast.Hour,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.Minute,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.Second,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.Month,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.MicroSecond,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.PI,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{},
		},
		{
			functionName: ast.Round,
			retType:      types.NewFieldType(mysql.TypeDouble),
			args:         []Expression{intColumn},
		},
		//{
		//	functionName: ast.Truncate,
		//	retType:      types.NewFieldType(mysql.TypeDouble),
		//	args:         []Expression{intColumn, intColumn},
		//},
		{
			functionName: ast.Bin,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.Unhex,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.Locate,
			retType:      types.NewFieldType(mysql.TypeInt24),
			args:         []Expression{stringColumn, stringColumn},
		},
		{
			functionName: ast.Ord,
			retType:      types.NewFieldType(mysql.TypeInt24),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.Lpad,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn, intColumn, stringColumn},
		},
		{
			functionName: ast.Rpad,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn, intColumn, stringColumn},
		},
		{
			functionName: ast.Trim,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.FromBase64,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.ToBase64,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.MakeSet,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{intColumn, stringColumn},
		},
		{
			functionName: ast.SubstringIndex,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn, stringColumn, intColumn},
		},
		{
			functionName: ast.Instr,
			retType:      types.NewFieldType(mysql.TypeInt24),
			args:         []Expression{stringColumn, stringColumn},
		},
		{
			functionName: ast.Quote,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.Oct,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{intColumn},
		},
		{
			functionName: ast.FindInSet,
			retType:      types.NewFieldType(mysql.TypeInt24),
			args:         []Expression{stringColumn, stringColumn},
		},
		{
			functionName: ast.Repeat,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn, intColumn},
		},
		{
			functionName: ast.Date,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.Week,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.YearWeek,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.ToSeconds,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn},
		},
		{
			functionName: ast.DateDiff,
			retType:      types.NewFieldType(mysql.TypeDate),
			args:         []Expression{dateColumn, dateColumn},
		},
		{
			functionName: ast.Lower,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.InsertFunc,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn, intColumn, intColumn, stringColumn},
		},
		{
			functionName: ast.Greatest,
			retType:      types.NewFieldType(mysql.TypeInt24),
			args:         []Expression{intColumn, intColumn},
		},
		{
			functionName: ast.Least,
			retType:      types.NewFieldType(mysql.TypeInt24),
			args:         []Expression{intColumn, intColumn},
		},
		{
			functionName: ast.Upper,
			retType:      types.NewFieldType(mysql.TypeString),
			args:         []Expression{stringColumn},
		},
		{
			functionName: ast.Mod,
			retType:      types.NewFieldType(mysql.TypeInt24),
			args:         []Expression{intColumn, intColumn},
		},
	}

	for _, tc := range testcases {
		function, err = NewFunction(mock.NewContext(), tc.functionName, tc.retType, tc.args...)
		require.NoError(t, err)
		exprs = append(exprs, function)
	}

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiKV)
	require.Len(t, pushed, len(exprs))
	require.Len(t, remained, 0)
}

func TestExprOnlyPushDownToTiKV(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	function, err := NewFunction(mock.NewContext(), "weekofyear", types.NewFieldType(mysql.TypeLonglong), genColumn(mysql.TypeDatetime, 1))
	require.NoError(t, err)
	var exprs = make([]Expression, 0)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, kv.UnSpecified)
	require.Len(t, pushed, 1)
	require.Len(t, remained, 0)

	canPush := CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	require.Equal(t, false, canPush)
	canPush = CanExprsPushDown(sc, exprs, client, kv.TiKV)
	require.Equal(t, true, canPush)

	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiFlash)
	require.Len(t, pushed, 0)
	require.Len(t, remained, 1)
	pushed, remained = PushDownExprs(sc, exprs, client, kv.TiKV)
	require.Len(t, pushed, 1)
	require.Len(t, remained, 0)
}

func TestGroupByItem2Pb(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	item := genColumn(mysql.TypeDouble, 0)
	pbByItem := GroupByItemToPB(sc, client, item)
	js, err := json.Marshal(pbByItem)
	require.NoError(t, err)
	require.Equal(t, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},\"desc\":false}", string(js))

	item = genColumn(mysql.TypeDouble, 1)
	pbByItem = GroupByItemToPB(sc, client, item)
	js, err = json.Marshal(pbByItem)
	require.NoError(t, err)
	require.Equal(t, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},\"desc\":false}", string(js))
}

func TestSortByItem2Pb(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	item := genColumn(mysql.TypeDouble, 0)
	pbByItem := SortByItemToPB(sc, client, item, false)
	js, err := json.Marshal(pbByItem)
	require.NoError(t, err)
	require.Equal(t, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},\"desc\":false}", string(js))

	item = genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, false)
	js, err = json.Marshal(pbByItem)
	require.NoError(t, err)
	require.Equal(t, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},\"desc\":false}", string(js))

	item = genColumn(mysql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, true)
	js, err = json.Marshal(pbByItem)
	require.NoError(t, err)
	require.Equal(t, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-63,\"charset\":\"binary\"},\"has_distinct\":false},\"desc\":true}", string(js))
}

func TestPushCollationDown(t *testing.T) {
	fc, err := NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeUnspecified), genColumn(mysql.TypeVarchar, 0), genColumn(mysql.TypeVarchar, 1))
	require.NoError(t, err)
	client := new(mock.Client)
	sc := new(stmtctx.StatementContext)

	tps := []*types.FieldType{types.NewFieldType(mysql.TypeVarchar), types.NewFieldType(mysql.TypeVarchar)}
	for _, coll := range []string{charset.CollationBin, charset.CollationLatin1, charset.CollationUTF8, charset.CollationUTF8MB4} {
		fc.SetCharsetAndCollation("binary", coll) // only collation matters
		pbExpr, err := ExpressionsToPBList(sc, []Expression{fc}, client)
		require.NoError(t, err)
		expr, err := PBToExpr(pbExpr[0], tps, sc)
		require.NoError(t, err)
		_, eColl := expr.CharsetAndCollation()
		require.Equal(t, coll, eColl)
	}
}

func columnCollation(c *Column, chs, coll string) *Column {
	c.RetType.Charset = chs
	c.RetType.Collate = coll
	return c
}

func TestNewCollationsEnabled(t *testing.T) {
	var colExprs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	colExprs = colExprs[:0]
	colExprs = append(colExprs, genColumn(mysql.TypeVarchar, 1))
	colExprs = append(colExprs, columnCollation(genColumn(mysql.TypeVarchar, 2), "some_invalid_charset", "some_invalid_collation"))
	colExprs = append(colExprs, columnCollation(genColumn(mysql.TypeVarString, 3), "utf8mb4", "utf8mb4_general_ci"))
	colExprs = append(colExprs, columnCollation(genColumn(mysql.TypeString, 4), "utf8mb4", "utf8mb4_0900_ai_ci"))
	colExprs = append(colExprs, columnCollation(genColumn(mysql.TypeVarchar, 5), "utf8", "utf8_bin"))
	colExprs = append(colExprs, columnCollation(genColumn(mysql.TypeVarchar, 6), "utf8", "utf8_unicode_ci"))
	colExprs = append(colExprs, columnCollation(genColumn(mysql.TypeVarchar, 7), "utf8mb4", "utf8mb4_zh_pinyin_tidb_as_cs"))
	pushed, _ := PushDownExprs(sc, colExprs, client, kv.UnSpecified)
	require.Equal(t, len(colExprs), len(pushed))
	pbExprs, err := ExpressionsToPBList(sc, colExprs, client)
	require.NoError(t, err)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"utf8mb4\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-46,\"charset\":\"some_invalid_charset\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-45,\"charset\":\"utf8mb4\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-255,\"charset\":\"utf8mb4\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-83,\"charset\":\"utf8\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-192,\"charset\":\"utf8\"},\"has_distinct\":false}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAc=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-2048,\"charset\":\"utf8mb4\"},\"has_distinct\":false}",
	}
	for i, pbExpr := range pbExprs {
		require.NotNil(t, pbExprs)
		js, err := json.Marshal(pbExpr)
		require.NoError(t, err)
		require.Equalf(t, jsons[i], string(js), "%v\n", i)
	}

	item := columnCollation(genColumn(mysql.TypeDouble, 0), "utf8mb4", "utf8mb4_0900_ai_ci")
	pbByItem := GroupByItemToPB(sc, client, item)
	js, err := json.Marshal(pbByItem)
	require.NoError(t, err)
	require.Equal(t, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"collate\":-255,\"charset\":\"utf8mb4\"},\"has_distinct\":false},\"desc\":false}", string(js))
}

func TestMetadata(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/PushDownTestSwitcher", `return("all")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/PushDownTestSwitcher"))
	}()

	pc := PbConverter{client: client, sc: sc}

	metadata := new(tipb.InUnionMetadata)
	var err error
	// InUnion flag is false in `BuildCastFunction` when `ScalarFuncSig_CastStringAsInt`
	cast := BuildCastFunction(mock.NewContext(), genColumn(mysql.TypeString, 1), types.NewFieldType(mysql.TypeLonglong))
	require.Equal(t, &tipb.InUnionMetadata{InUnion: false}, cast.(*ScalarFunction).Function.metadata())
	expr := pc.ExprToPB(cast)
	require.Equal(t, tipb.ScalarFuncSig_CastStringAsInt, expr.Sig)
	require.Greater(t, len(expr.Val), 0)
	err = proto.Unmarshal(expr.Val, metadata)
	require.NoError(t, err)
	require.Equal(t, false, metadata.InUnion)

	// InUnion flag is nil in `BuildCastFunction4Union` when `ScalarFuncSig_CastIntAsString`
	castInUnion := BuildCastFunction4Union(mock.NewContext(), genColumn(mysql.TypeLonglong, 1), types.NewFieldType(mysql.TypeString))
	require.Nil(t, castInUnion.(*ScalarFunction).Function.metadata())
	expr = pc.ExprToPB(castInUnion)
	require.Equal(t, tipb.ScalarFuncSig_CastIntAsString, expr.Sig)
	require.Equal(t, 0, len(expr.Val))

	// InUnion flag is true in `BuildCastFunction4Union` when `ScalarFuncSig_CastStringAsInt`
	castInUnion = BuildCastFunction4Union(mock.NewContext(), genColumn(mysql.TypeString, 1), types.NewFieldType(mysql.TypeLonglong))
	require.Equal(t, &tipb.InUnionMetadata{InUnion: true}, castInUnion.(*ScalarFunction).Function.metadata())
	expr = pc.ExprToPB(castInUnion)
	require.Equal(t, tipb.ScalarFuncSig_CastStringAsInt, expr.Sig)
	require.Greater(t, len(expr.Val), 0)
	err = proto.Unmarshal(expr.Val, metadata)
	require.NoError(t, err)
	require.Equal(t, true, metadata.InUnion)
}

func TestPushDownSwitcher(t *testing.T) {
	var funcs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

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
		args := []Expression{genColumn(mysql.TypeDouble, 1)}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName.name,
			types.NewFieldType(mysql.TypeUnspecified),
			args...,
		)
		require.NoError(t, err)
		funcs = append(funcs, fc)
		if funcName.enable {
			enabled = append(enabled, funcName.name)
		}
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/PushDownTestSwitcher", `return("all")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/PushDownTestSwitcher"))
	}()

	pbExprs, err := ExpressionsToPBList(sc, funcs, client)
	require.NoError(t, err)
	require.Equal(t, len(cases), len(pbExprs))
	for i, pbExpr := range pbExprs {
		require.Equalf(t, cases[i].sig, pbExpr.Sig, "function: %s, sig: %v", cases[i].name, cases[i].sig)
	}

	// All disabled
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/PushDownTestSwitcher", `return("")`))
	pc := PbConverter{client: client, sc: sc}
	for i := range funcs {
		pbExpr := pc.ExprToPB(funcs[i])
		require.Nil(t, pbExpr)
	}

	// Partial enabled
	fpexpr := fmt.Sprintf(`return("%s")`, strings.Join(enabled, ","))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/PushDownTestSwitcher", fpexpr))
	for i := range funcs {
		pbExpr := pc.ExprToPB(funcs[i])
		if !cases[i].enable {
			require.Nil(t, pbExpr)
			continue
		}
		require.Equalf(t, cases[i].sig, pbExpr.Sig, "function: %s, sig: %v", cases[i].name, cases[i].sig)
	}
}

func TestPanicIfPbCodeUnspecified(t *testing.T) {

	args := []Expression{genColumn(mysql.TypeLong, 1), genColumn(mysql.TypeLong, 2)}
	fc, err := NewFunction(
		mock.NewContext(),
		ast.And,
		types.NewFieldType(mysql.TypeUnspecified),
		args...,
	)
	require.NoError(t, err)
	fn := fc.(*ScalarFunction)
	fn.Function.setPbCode(tipb.ScalarFuncSig_Unspecified)
	require.Equal(t, tipb.ScalarFuncSig_Unspecified, fn.Function.PbCode())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/PanicIfPbCodeUnspecified", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/PanicIfPbCodeUnspecified"))
	}()
	pc := PbConverter{client: new(mock.Client), sc: new(stmtctx.StatementContext)}
	require.PanicsWithError(t, "unspecified PbCode: *expression.builtinBitAndSig", func() { pc.ExprToPB(fn) })
}
