// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestPushCollationDown(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
		args := []Expression{genColumn(mysql.TypeLong, 1)}
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
