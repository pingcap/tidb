// Copyright 2020 PingCAP, Inc.
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

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func newExpression(coercibility Coercibility, repertoire Repertoire, chs, coll string) Expression {
	constant := &Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetCharset(chs).SetCollate(coll).BuildP()}
	constant.SetCoercibility(coercibility)
	constant.SetRepertoire(repertoire)
	return constant
}

func TestInferCollation(t *testing.T) {
	tests := []struct {
		exprs []Expression
		err   bool
		ec    *ExprCollation
	}{
		// same charset.
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_general_ci"),
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			false,
			&ExprCollation{CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"},
		},
		{
			[]Expression{
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			false,
			&ExprCollation{CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"},
		},
		{
			[]Expression{
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_general_ci"),
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			true,
			nil,
		},
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_general_ci"),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			false,
			&ExprCollation{CoercibilityNone, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_general_ci"),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
			},
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		// binary charset with non-binary charset.
		{
			[]Expression{
				newExpression(CoercibilityNumeric, UNICODE, charset.CharsetBin, charset.CollationBin),
				newExpression(CoercibilityCoercible, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
			},
			false,
			&ExprCollation{CoercibilityCoercible, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]Expression{
				newExpression(CoercibilityCoercible, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newExpression(CoercibilityNumeric, UNICODE, charset.CharsetBin, charset.CollationBin),
			},
			false,
			&ExprCollation{CoercibilityCoercible, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]Expression{
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetBin, charset.CollationBin),
			},
			false,
			&ExprCollation{CoercibilityExplicit, UNICODE, charset.CharsetBin, charset.CollationBin},
		},
		// different charset, one of them is utf8mb4
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"},
		},
		{
			[]Expression{
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			false,
			&ExprCollation{CoercibilityExplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"},
		},
		{
			[]Expression{
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			true,
			nil,
		},
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
			},
			true,
			nil,
		},
		{
			[]Expression{
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
			},
			true,
			nil,
		},
		{
			[]Expression{
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
			},
			true,
			nil,
		},
		// different charset, one of them is CoercibilityCoercible
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityCoercible, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
			},
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin},
		},
		{
			[]Expression{
				newExpression(CoercibilityCoercible, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
			},
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1},
		},
		// different charset, one of them is ASCII
		{
			[]Expression{
				newExpression(CoercibilityImplicit, ASCII, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
			},
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1},
		},
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, ASCII, charset.CharsetLatin1, charset.CollationLatin1),
			},
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin},
		},
		// 3 expressions.
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetBin, charset.CollationBin),
			},
			true,
			nil,
		},
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
			},
			true,
			nil,
		},
		{
			[]Expression{
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetGBK, charset.CollationGBKBin),
				newExpression(CoercibilityExplicit, UNICODE, charset.CharsetLatin1, charset.CollationLatin1),
				newExpression(CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
			},
			true,
			nil,
		},
	}

	for i, test := range tests {
		ec := inferCollation(test.exprs...)
		if test.err {
			require.Nil(t, ec, i)
		} else {
			require.Equal(t, test.ec, ec, i)
		}
	}
}

func newConstString(s string, coercibility Coercibility, chs, coll string) *Constant {
	repe := ASCII
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			repe = UNICODE
		}
	}
	constant := &Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetCharset(chs).SetCollate(coll).BuildP(), Value: types.NewDatum(s)}
	constant.SetCoercibility(coercibility)
	constant.SetRepertoire(repe)
	return constant
}

func newColString(chs, coll string) *Column {
	ft := types.FieldType{}
	ft.SetType(mysql.TypeString)
	ft.SetCharset(chs)
	ft.SetCollate(coll)
	column := &Column{RetType: &ft}
	column.SetCoercibility(CoercibilityImplicit)
	column.SetRepertoire(UNICODE)
	if chs == charset.CharsetASCII {
		column.SetRepertoire(ASCII)
	}
	return column
}

func newColJSON() *Column {
	ft := types.FieldType{}
	ft.SetType(mysql.TypeJSON)
	ft.SetCharset(charset.CharsetBin)
	ft.SetCollate(charset.CollationBin)
	column := &Column{RetType: &ft}
	return column
}

func newConstInt(coercibility Coercibility) *Constant {
	ft := types.FieldType{}
	ft.SetType(mysql.TypeLong)
	ft.SetCharset(charset.CharsetBin)
	ft.SetCollate(charset.CollationBin)
	constant := &Constant{RetType: &ft, Value: types.NewDatum(1)}
	constant.SetCoercibility(coercibility)
	constant.SetRepertoire(ASCII)
	return constant
}

func newColInt(coercibility Coercibility) *Column {
	ft := types.FieldType{}
	ft.SetType(mysql.TypeLong)
	ft.SetCharset(charset.CharsetBin)
	ft.SetCollate(charset.CollationBin)
	column := &Column{RetType: &ft}
	column.SetCoercibility(coercibility)
	column.SetRepertoire(ASCII)
	return column
}

func TestDeriveCollation(t *testing.T) {
	ctx := mock.NewContext()
	tests := []struct {
		fcs    []string
		args   []Expression
		argTps []types.EvalType
		retTp  types.EvalType

		err bool
		ec  *ExprCollation
	}{
		{
			[]string{ast.Left, ast.Right, ast.Repeat, ast.Substr, ast.Substring, ast.Mid},
			[]Expression{
				newConstString("a", CoercibilityCoercible, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstInt(CoercibilityExplicit),
			},
			[]types.EvalType{types.ETString, types.ETInt},
			types.ETString,
			false,
			&ExprCollation{CoercibilityCoercible, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.Trim, ast.LTrim, ast.RTrim},
			[]Expression{
				newConstString("a", CoercibilityCoercible, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
			},
			[]types.EvalType{types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityCoercible, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.SubstringIndex},
			[]Expression{
				newConstString("a", CoercibilityCoercible, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstString("啊", CoercibilityExplicit, charset.CharsetGBK, charset.CollationGBKBin),
				newConstInt(CoercibilityExplicit),
			},
			[]types.EvalType{types.ETString, types.ETString, types.ETInt},
			types.ETString,
			false,
			&ExprCollation{CoercibilityCoercible, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.Replace, ast.Translate},
			[]Expression{
				newConstString("a", CoercibilityExplicit, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstString("啊", CoercibilityExplicit, charset.CharsetGBK, charset.CollationGBKBin),
				newConstString("ㅂ", CoercibilityExplicit, charset.CharsetBin, charset.CollationBin),
			},
			[]types.EvalType{types.ETString, types.ETString, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityExplicit, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.InsertFunc},
			[]Expression{
				newConstString("a", CoercibilityExplicit, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstInt(CoercibilityExplicit),
				newConstInt(CoercibilityExplicit),
				newConstString("ㅂ", CoercibilityExplicit, charset.CharsetBin, charset.CollationBin),
			},
			[]types.EvalType{types.ETString, types.ETInt, types.ETInt, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityExplicit, UNICODE, charset.CharsetBin, charset.CollationBin},
		},
		{
			[]string{ast.InsertFunc},
			[]Expression{
				newConstString("a", CoercibilityImplicit, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstInt(CoercibilityExplicit),
				newConstInt(CoercibilityExplicit),
				newConstString("啊", CoercibilityImplicit, charset.CharsetGBK, charset.CollationGBKBin),
			},
			[]types.EvalType{types.ETString, types.ETInt, types.ETInt, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.InsertFunc},
			[]Expression{
				newConstString("ㅂ", CoercibilityImplicit, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstInt(CoercibilityExplicit),
				newConstInt(CoercibilityExplicit),
				newConstString("啊", CoercibilityExplicit, charset.CharsetGBK, charset.CollationGBKBin),
			},
			[]types.EvalType{types.ETString, types.ETInt, types.ETInt, types.ETString},
			types.ETString,
			true,
			nil,
		},
		{
			[]string{ast.Lpad, ast.Rpad},
			[]Expression{
				newConstString("ㅂ", CoercibilityImplicit, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstInt(CoercibilityExplicit),
				newConstString("啊", CoercibilityExplicit, charset.CharsetGBK, charset.CollationGBKBin),
			},
			[]types.EvalType{types.ETString, types.ETInt, types.ETString},
			types.ETString,
			true,
			nil,
		},
		{
			[]string{ast.Lpad, ast.Rpad},
			[]Expression{
				newConstString("ㅂ", CoercibilityImplicit, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstInt(CoercibilityExplicit),
				newConstString("啊", CoercibilityImplicit, charset.CharsetGBK, charset.CollationGBKBin),
			},
			[]types.EvalType{types.ETString, types.ETInt, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.FindInSet, ast.Regexp},
			[]Expression{
				newColString(charset.CharsetUTF8MB4, "utf8mb4_general_ci"),
				newColString(charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETString, types.ETString},
			types.ETInt,
			true,
			nil,
		},
		{
			[]string{ast.Field},
			[]Expression{
				newColString(charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newColString(charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETString, types.ETString},
			types.ETInt,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.Field},
			[]Expression{
				newColInt(CoercibilityImplicit),
				newColInt(CoercibilityImplicit),
			},
			[]types.EvalType{types.ETInt, types.ETInt},
			types.ETInt,
			false,
			&ExprCollation{CoercibilityNumeric, ASCII, charset.CharsetBin, charset.CollationBin},
		},
		{
			[]string{ast.Locate, ast.Instr, ast.Position},
			[]Expression{
				newColInt(CoercibilityNumeric),
				newColInt(CoercibilityNumeric),
			},
			[]types.EvalType{types.ETInt, types.ETInt},
			types.ETInt,
			false,
			&ExprCollation{CoercibilityNumeric, ASCII, charset.CharsetBin, charset.CollationBin},
		},
		{
			[]string{ast.Format, ast.SHA2},
			[]Expression{
				newColInt(CoercibilityNumeric),
				newColInt(CoercibilityNumeric),
			},
			[]types.EvalType{types.ETInt, types.ETInt},
			types.ETString,
			false,
			&ExprCollation{CoercibilityCoercible, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.Space, ast.ToBase64, ast.UUID, ast.Hex, ast.MD5, ast.SHA},
			[]Expression{
				newColInt(CoercibilityNumeric),
			},
			[]types.EvalType{types.ETInt},
			types.ETString,
			false,
			&ExprCollation{CoercibilityCoercible, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.GE, ast.LE, ast.GT, ast.LT, ast.EQ, ast.NE, ast.NullEQ, ast.Strcmp},
			[]Expression{
				newColString(charset.CharsetASCII, charset.CollationASCII),
				newColString(charset.CharsetGBK, charset.CollationGBKBin),
			},
			[]types.EvalType{types.ETString, types.ETString},
			types.ETInt,
			false,
			&ExprCollation{CoercibilityNumeric, ASCII, charset.CharsetGBK, charset.CollationGBKBin},
		},
		{
			[]string{ast.GE, ast.LE, ast.GT, ast.LT, ast.EQ, ast.NE, ast.NullEQ, ast.Strcmp},
			[]Expression{
				newColString(charset.CharsetLatin1, charset.CollationLatin1),
				newColString(charset.CharsetGBK, charset.CollationGBKBin),
			},
			[]types.EvalType{types.ETString, types.ETString},
			types.ETInt,
			true,
			nil,
		},
		{
			[]string{ast.Bin, ast.FromBase64, ast.Oct, ast.Unhex, ast.WeightString},
			[]Expression{
				newColString(charset.CharsetLatin1, charset.CollationLatin1),
			},
			[]types.EvalType{types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityCoercible, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{ast.ASCII, ast.BitLength, ast.CharLength, ast.CharacterLength, ast.Length, ast.OctetLength, ast.Ord},
			[]Expression{
				newColString(charset.CharsetLatin1, charset.CollationLatin1),
			},
			[]types.EvalType{types.ETString},
			types.ETInt,
			false,
			&ExprCollation{CoercibilityNumeric, ASCII, charset.CharsetBin, charset.CollationBin},
		},
		{
			[]string{
				ast.ExportSet, ast.Elt, ast.MakeSet,
			},
			[]Expression{
				newColInt(CoercibilityExplicit),
				newColString(charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newColString(charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETInt, types.ETString, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.ExportSet, ast.Elt, ast.MakeSet,
			},
			[]Expression{
				newColInt(CoercibilityExplicit),
				newColJSON(),
				newColString(charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETInt, types.ETJson},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.Concat, ast.ConcatWS, ast.Coalesce, ast.Greatest, ast.Least,
			},
			[]Expression{
				newColString(charset.CharsetGBK, charset.CollationGBKBin),
				newColJSON(),
			},
			[]types.EvalType{types.ETString, types.ETJson},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.Concat, ast.ConcatWS, ast.Coalesce, ast.Greatest, ast.Least,
			},
			[]Expression{
				newColJSON(),
				newColString(charset.CharsetBin, charset.CharsetBin),
			},
			[]types.EvalType{types.ETJson, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetBin, charset.CharsetBin},
		},
		{
			[]string{
				ast.Concat, ast.ConcatWS, ast.Coalesce, ast.In, ast.Greatest, ast.Least,
			},
			[]Expression{
				newConstString("a", CoercibilityCoercible, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newColString(charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newColString(charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETInt, types.ETString, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.Lower, ast.Lcase, ast.Reverse, ast.Upper, ast.Ucase, ast.Quote,
			},
			[]Expression{
				newConstString("a", CoercibilityCoercible, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
			},
			[]types.EvalType{types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityCoercible, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.Lower, ast.Lcase, ast.Reverse, ast.Upper, ast.Ucase, ast.Quote,
			},
			[]Expression{
				newColJSON(),
			},
			[]types.EvalType{types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.If,
			},
			[]Expression{
				newColInt(CoercibilityExplicit),
				newColString(charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newColString(charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETInt, types.ETString, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.Ifnull,
			},
			[]Expression{
				newColString(charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newColString(charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETString, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityImplicit, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.Like,
			},
			[]Expression{
				newColString(charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstString("like", CoercibilityExplicit, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
				newConstString("\\", CoercibilityExplicit, charset.CharsetUTF8MB4, charset.CollationUTF8MB4),
			},
			[]types.EvalType{types.ETString, types.ETString, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityNumeric, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.DateFormat, ast.TimeFormat,
			},
			[]Expression{
				newConstString("2020-02-02", CoercibilityExplicit, charset.CharsetUTF8MB4, "utf8mb4_general_ci"),
				newConstString("%Y %M %D", CoercibilityExplicit, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETDatetime, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityExplicit, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.DateFormat, ast.TimeFormat,
			},
			[]Expression{
				newConstString("2020-02-02", CoercibilityExplicit, charset.CharsetUTF8MB4, "utf8mb4_general_ci"),
				newConstString("%Y %M %D", CoercibilityCoercible, charset.CharsetUTF8MB4, "utf8mb4_unicode_ci"),
			},
			[]types.EvalType{types.ETDatetime, types.ETString},
			types.ETString,
			false,
			&ExprCollation{CoercibilityCoercible, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.Database, ast.User, ast.CurrentUser, ast.Version, ast.CurrentRole, ast.TiDBVersion,
			},
			[]Expression{},
			[]types.EvalType{},
			types.ETString,
			false,
			&ExprCollation{CoercibilitySysconst, UNICODE, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
		{
			[]string{
				ast.Cast,
			},
			[]Expression{
				newColInt(CoercibilityExplicit),
			},
			[]types.EvalType{types.ETInt},
			types.ETString,
			false,
			&ExprCollation{CoercibilityExplicit, ASCII, charset.CharsetUTF8MB4, charset.CollationUTF8MB4},
		},
	}

	for i, test := range tests {
		for _, fc := range test.fcs {
			ec, err := deriveCollation(ctx, fc, test.args, test.retTp, test.argTps...)
			if test.err {
				require.Error(t, err, "Number: %d, function: %s", i, fc)
				require.Nil(t, ec, i)
			} else {
				require.Equal(t, test.ec, ec, "Number: %d, function: %s", i, fc)
			}
		}
	}
}

func TestCompareString(t *testing.T) {
	require.Equal(t, 0, types.CompareString("a", "A", "utf8_general_ci"))
	require.Equal(t, 0, types.CompareString("À", "A", "utf8_general_ci"))
	require.Equal(t, 0, types.CompareString("😜", "😃", "utf8_general_ci"))
	require.Equal(t, 0, types.CompareString("a ", "a  ", "utf8_general_ci"))
	require.Equal(t, 0, types.CompareString("ß", "s", "utf8_general_ci"))
	require.NotEqual(t, 0, types.CompareString("ß", "ss", "utf8_general_ci"))

	require.Equal(t, 0, types.CompareString("a", "A", "utf8_unicode_ci"))
	require.Equal(t, 0, types.CompareString("À", "A", "utf8_unicode_ci"))
	require.Equal(t, 0, types.CompareString("😜", "😃", "utf8_unicode_ci"))
	require.Equal(t, 0, types.CompareString("a ", "a  ", "utf8_unicode_ci"))
	require.NotEqual(t, 0, types.CompareString("ß", "s", "utf8_unicode_ci"))
	require.Equal(t, 0, types.CompareString("ß", "ss", "utf8_unicode_ci"))

	require.NotEqual(t, 0, types.CompareString("a", "A", "binary"))
	require.NotEqual(t, 0, types.CompareString("À", "A", "binary"))
	require.NotEqual(t, 0, types.CompareString("😜", "😃", "binary"))
	require.NotEqual(t, 0, types.CompareString("a ", "a  ", "binary"))

	ctx := mock.NewContext()
	ft := types.NewFieldType(mysql.TypeVarString)
	col1 := &Column{
		RetType: ft,
		Index:   0,
	}
	col2 := &Column{
		RetType: ft,
		Index:   1,
	}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)
	chk.Column(0).AppendString("a")
	chk.Column(1).AppendString("A")
	chk.Column(0).AppendString("À")
	chk.Column(1).AppendString("A")
	chk.Column(0).AppendString("😜")
	chk.Column(1).AppendString("😃")
	chk.Column(0).AppendString("a ")
	chk.Column(1).AppendString("a  ")
	for i := 0; i < 4; i++ {
		v, isNull, err := CompareStringWithCollationInfo(ctx, col1, col2, chk.GetRow(0), chk.GetRow(0), "utf8_general_ci")
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int64(0), v)
	}
}
