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

func parseAutoEmbedExpr(t *testing.T, expr string) ast.ExprNode {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt("select "+expr, "", "")
	require.NoError(t, err)
	return stmt.(*ast.SelectStmt).Fields.Fields[0].Expr
}

func TestAutoEmbedASTHelpers(t *testing.T) {
	direct := parseAutoEmbedExpr(t, "EMBED_TEXT('mock/json', text)")
	nested := parseAutoEmbedExpr(t, "vec_dims(embed_text('mock/json', text))")
	other := parseAutoEmbedExpr(t, "vec_dims(vec)")

	require.True(t, ContainsAutoEmbedFnAST(direct))
	require.True(t, ContainsAutoEmbedFnAST(nested))
	require.False(t, ContainsAutoEmbedFnAST(other))
	require.False(t, ContainsAutoEmbedFnAST(nil))
	require.True(t, IsAutoEmbedFnCallAST(direct))
	require.False(t, IsAutoEmbedFnCallAST(nested))
	require.False(t, IsAutoEmbedFnCallAST(other))
}

func TestExtractAutoEmbedInfoFromAST(t *testing.T) {
	info, err := ExtractAutoEmbedInfoFromAST(parseAutoEmbedExpr(t,
		`embed_text('mock/json', text, '{"plus":0.5}')`))
	require.NoError(t, err)
	require.Equal(t, &AutoEmbedInfo{
		ModelNameWithProvider: "mock/json",
		OptsInJSON:            `{"plus":0.5}`,
	}, info)
	require.True(t, info.Equal(&AutoEmbedInfo{
		ModelNameWithProvider: "mock/json",
		OptsInJSON:            `{"plus":0.5}`,
	}))
	require.False(t, info.Equal(&AutoEmbedInfo{ModelNameWithProvider: "mock/other"}))
	require.True(t, (*AutoEmbedInfo)(nil).Equal(nil))

	tests := []struct {
		expr string
		err  string
	}{
		{"vec_dims(vec)", "only generated columns using EMBED_TEXT() are allowed"},
		{"embed_text('mock/json')", "invalid EMBED_TEXT() usage"},
		{"embed_text(model, text)", "model name using string constant"},
		{"embed_text('mock/json', text, opts)", "JSON options using string constant"},
		{"embed_text('mock/json', text, '{invalid}')", "expects options in JSON format"},
		{"embed_text('mock/json', text, '[]')", "expects options in JSON format"},
		{"embed_text('mock/json', text, 'null')", "expects options in JSON format"},
	}
	for _, test := range tests {
		_, err := ExtractAutoEmbedInfoFromAST(parseAutoEmbedExpr(t, test.expr))
		require.ErrorContains(t, err, test.err, test.expr)
	}
}
