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

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func parseEmbedTextExpr(t *testing.T, expr string) ast.ExprNode {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt("select "+expr, "", "")
	require.NoError(t, err)
	return stmt.(*ast.SelectStmt).Fields.Fields[0].Expr
}

func TestEmbedTextASTHelpers(t *testing.T) {
	direct := parseEmbedTextExpr(t, "EMBED_TEXT('mock/json', text)")
	nested := parseEmbedTextExpr(t, "vec_dims(embed_text('mock/json', text))")
	other := parseEmbedTextExpr(t, "vec_dims(vec)")

	require.True(t, ContainsEmbedTextFunc(direct))
	require.True(t, ContainsEmbedTextFunc(nested))
	require.False(t, ContainsEmbedTextFunc(other))
	require.False(t, ContainsEmbedTextFunc(nil))
	require.True(t, IsEmbedTextFuncCall(direct))
	require.False(t, IsEmbedTextFuncCall(nested))
	require.False(t, IsEmbedTextFuncCall(other))
}

func TestExtractEmbedTextInfo(t *testing.T) {
	info, err := ExtractEmbedTextInfo(parseEmbedTextExpr(t,
		`embed_text('mock/json', text, '{"plus":0.5}')`))
	require.NoError(t, err)
	require.Equal(t, &EmbedTextInfo{
		ModelNameWithProvider: "mock/json",
		OptsInJSON:            `{"plus":0.5}`,
	}, info)
	require.True(t, info.Equal(&EmbedTextInfo{
		ModelNameWithProvider: "mock/json",
		OptsInJSON:            `{"plus":0.5}`,
	}))
	require.False(t, info.Equal(&EmbedTextInfo{ModelNameWithProvider: "mock/other"}))
	require.False(t, info.Equal(nil))
	require.False(t, (*EmbedTextInfo)(nil).Equal(info))
	require.True(t, (*EmbedTextInfo)(nil).Equal(nil))

	info, err = ExtractEmbedTextInfo(parseEmbedTextExpr(t,
		`embed_text('mock/json', text, '')`))
	require.NoError(t, err)
	require.Equal(t, &EmbedTextInfo{ModelNameWithProvider: "mock/json"}, info)
	info, err = ExtractEmbedTextInfo(parseEmbedTextExpr(t,
		`embed_text('mock/json', text)`))
	require.NoError(t, err)
	require.Equal(t, &EmbedTextInfo{ModelNameWithProvider: "mock/json"}, info)

	tests := []struct {
		expr string
		err  string
	}{
		{"vec_dims(vec)", "only generated columns using EMBED_TEXT() are allowed"},
		{"embed_text('mock/json')", "invalid EMBED_TEXT() usage"},
		{"embed_text('mock/json', text, '{}', 'extra')", "invalid EMBED_TEXT() usage"},
		{"embed_text(model, text)", "model name using string constant"},
		{"embed_text(1, text)", "model name using string constant"},
		{"embed_text('mock/json', text, opts)", "JSON options using string constant"},
		{"embed_text('mock/json', text, 1)", "JSON options using string constant"},
		{"embed_text('mock/json', text, '{invalid}')", "expects options in JSON format"},
		{"embed_text('mock/json', text, '[]')", "expects options in JSON format"},
		{"embed_text('mock/json', text, 'null')", "expects options in JSON format"},
	}
	for _, test := range tests {
		_, err := ExtractEmbedTextInfo(parseEmbedTextExpr(t, test.expr))
		require.ErrorContains(t, err, test.err, test.expr)
	}
}
