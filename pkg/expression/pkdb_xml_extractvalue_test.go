// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestExtractValueAttributePositionPredicates(t *testing.T) {
	ctx := createContext(t)

	cases := []struct {
		xml  string
		path string
		want string
	}{
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[position()=1]`, want: "b1"},
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[position()=2]`, want: "b2"},
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[position()=3]`, want: "b3"},
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[1=position()]`, want: "b1"},
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[2=position()]`, want: "b2"},
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[3=position()]`, want: "b3"},
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[2>=position()]`, want: "b1 b2"},
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[2<=position()]`, want: "b2 b3"},
		{xml: `<a b="b1" b="b2" b="b3"/>`, path: `/a/@b[position()=3 or position()=2]`, want: "b2 b3"},
		{xml: `<a>A<b>B1</b><b>B2</b></a>`, path: `/a/b[count(.)=last()]`, want: "B1 B2"},
		{xml: `<a>A<b>B1</b><b>B2</b></a>`, path: `/a/b[count(.)]`, want: "B2"},
		{xml: `<a>A<b>B1</b><b>B2</b></a>`, path: `/a/b[count(.)-1]`, want: "B1"},
		{xml: `<a>A<b>B1</b><b>B2</b></a>`, path: `/a/b[count(.)=1]`, want: ""},
		{xml: `<a>A<b>B1</b><b>B2</b></a>`, path: `/a/b[count(.)=2]`, want: "B1 B2"},
		{xml: `<a>A<b>B1</b><b>B2</b></a>`, path: `/a/b[count(.)=position()]`, want: "B2"},
	}

	for _, tc := range cases {
		f, err := newFunctionForTest(ctx, ast.ExtractValue, primitiveValsToConstants(ctx, []any{tc.xml, tc.path})...)
		require.NoError(t, err, tc.path)

		res, isNull, err := f.EvalString(ctx, chunk.Row{})
		require.NoError(t, err, tc.path)
		require.False(t, isNull, tc.path)
		require.Equal(t, tc.want, res, tc.path)
	}
}

func TestExtractValueScalarResults(t *testing.T) {
	ctx := createContext(t)

	cases := []struct {
		xml  string
		path string
		want string
	}{
		{xml: `<a>a</a>`, path: `/a | /a/text()`, want: "a"},
		{xml: `<a><b>b1</b><b>b2</b></a>`, path: `string-length("x")`, want: "1"},
		{xml: `<a><b>B</b></a>`, path: `string-length(/a)`, want: "0"},
		{xml: `<ns:element xmlns:ns="myns"/>`, path: `count(ns:element)`, want: "1"},
		{xml: "<a>\n  <b>x</b>\n</a>", path: `/a`, want: ""},
		{xml: "<a>\n  <b>x</b>\n</a>", path: `/a/text()`, want: "\n   \n"},
		{xml: `<a><![CDATA[test]]></a>`, path: `/a/text()`, want: "test"},
	}

	for _, tc := range cases {
		f, err := newFunctionForTest(ctx, ast.ExtractValue, primitiveValsToConstants(ctx, []any{tc.xml, tc.path})...)
		require.NoError(t, err, tc.path)

		res, isNull, err := f.EvalString(ctx, chunk.Row{})
		require.NoError(t, err, tc.path)
		require.False(t, isNull, tc.path)
		require.Equal(t, tc.want, res, tc.path)
	}
}

func TestRewriteExtractValuePredicateCountDot(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{expr: `/a/b[count(.)]`, want: `/a/b[last()]`},
		{expr: `/a/b[count ( . ) = position()]`, want: `/a/b[last() = position()]`},
		{expr: `/a[b[count(.)=1]]`, want: `/a[b[last()=1]]`},
		{expr: `count(.)`, want: `count(.)`},
		{expr: `/a/b[count(/a/b)=2]`, want: `/a/b[count(/a/b)=2]`},
		{expr: `/a/b[contains("count(.)", "x")]`, want: `/a/b[contains("count(.)", "x")]`},
		{expr: `/a/b[ns:count(.)=1]`, want: `/a/b[ns:count(.)=1]`},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, rewriteExtractValuePredicateCountDot(tc.expr), tc.expr)
	}
}
