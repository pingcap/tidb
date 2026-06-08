// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

func parseExpr(t *testing.T, expr string) ast.ExprNode {
	p := parser.New()
	st, err := p.ParseOneStmt("select "+expr, "", "")
	require.NoError(t, err)
	stmt := st.(*ast.SelectStmt)
	return stmt.Fields.Fields[0].Expr
}

func TestContainsAutoEmbedFnAST(t *testing.T) {
	// Simple embed_text function
	expr := parseExpr(t, "embed_text('model1', 'text')")
	require.True(t, ContainsAutoEmbedFnAST(expr))

	// Embed_text with JSON options
	expr = parseExpr(t, "embed_text('model1', 'text', '{\"temperature\": 0.5}')")
	require.True(t, ContainsAutoEmbedFnAST(expr))

	// Embed_text nested in expression
	expr = parseExpr(t, "1 + embed_text('model1', 'text')")
	require.True(t, ContainsAutoEmbedFnAST(expr))

	// Embed_text in function call
	expr = parseExpr(t, "cos(embed_text('model1', 'text'))")
	require.True(t, ContainsAutoEmbedFnAST(expr))

	// Case insensitive - EMBED_TEXT
	expr = parseExpr(t, "EMBED_TEXT('model1', 'text')")
	require.True(t, ContainsAutoEmbedFnAST(expr))

	// Case insensitive - Embed_Text
	expr = parseExpr(t, "Embed_Text('model1', 'text')")
	require.True(t, ContainsAutoEmbedFnAST(expr))

	// No embed_text function
	expr = parseExpr(t, "1 + 2")
	require.False(t, ContainsAutoEmbedFnAST(expr))

	// Other function
	expr = parseExpr(t, "cos(1)")
	require.False(t, ContainsAutoEmbedFnAST(expr))

	// String literal containing embed_text
	expr = parseExpr(t, "'embed_text is a function'")
	require.False(t, ContainsAutoEmbedFnAST(expr))

	// Column name embed_text
	expr = parseExpr(t, "embed_text")
	require.False(t, ContainsAutoEmbedFnAST(expr))

	// Complex expression without embed_text
	expr = parseExpr(t, "col1 + col2 * sin(col3)")
	require.False(t, ContainsAutoEmbedFnAST(expr))

	// Multiple embed_text calls
	expr = parseExpr(t, "embed_text('model1', 'text1') + embed_text('model2', 'text2')")
	require.True(t, ContainsAutoEmbedFnAST(expr))

	// Deeply nested embed_text
	expr = parseExpr(t, "if(col1 > 0, embed_text('model1', 'text'), 0)")
	require.True(t, ContainsAutoEmbedFnAST(expr))
}

func TestIsAutoEmbedFnCallAST(t *testing.T) {
	// Direct embed_text function call
	expr := parseExpr(t, "embed_text('model1', 'text')")
	require.True(t, IsAutoEmbedFnCallAST(expr))

	// Direct embed_text with JSON options
	expr = parseExpr(t, "embed_text('model1', 'text', '{\"temperature\": 0.5}')")
	require.True(t, IsAutoEmbedFnCallAST(expr))

	// Case insensitive - EMBED_TEXT
	expr = parseExpr(t, "EMBED_TEXT('model1', 'text')")
	require.True(t, IsAutoEmbedFnCallAST(expr))

	// Case insensitive - Embed_Text
	expr = parseExpr(t, "Embed_Text('model1', 'text')")
	require.True(t, IsAutoEmbedFnCallAST(expr))

	// Embed_text in expression (not direct)
	expr = parseExpr(t, "1 + embed_text('model1', 'text')")
	require.False(t, IsAutoEmbedFnCallAST(expr))

	// Embed_text nested in function (not direct)
	expr = parseExpr(t, "cos(embed_text('model1', 'text'))")
	require.False(t, IsAutoEmbedFnCallAST(expr))

	// Other function
	expr = parseExpr(t, "cos(1)")
	require.False(t, IsAutoEmbedFnCallAST(expr))

	// String literal
	expr = parseExpr(t, "'embed_text'")
	require.False(t, IsAutoEmbedFnCallAST(expr))

	// Column reference
	expr = parseExpr(t, "embed_text")
	require.False(t, IsAutoEmbedFnCallAST(expr))

	// Number literal
	expr = parseExpr(t, "123")
	require.False(t, IsAutoEmbedFnCallAST(expr))

	// Binary operation
	expr = parseExpr(t, "1 + 2")
	require.False(t, IsAutoEmbedFnCallAST(expr))
}

func TestExtractAutoEmbedInfoFromAST(t *testing.T) {
	// Valid embed_text with 2 arguments
	expr := parseExpr(t, "embed_text('openai/gpt-4', 'Hello world')")
	result, err := ExtractAutoEmbedInfoFromAST(expr)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "openai/gpt-4", result.ModelNameWithProvider)
	require.Equal(t, "", result.OptsInJSON)

	// Valid embed_text with 3 arguments
	expr = parseExpr(t, "embed_text('openai/gpt-4', 'Hello world', '{\"temperature\": 0.5}')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "openai/gpt-4", result.ModelNameWithProvider)
	require.Equal(t, "{\"temperature\": 0.5}", result.OptsInJSON)

	// Case insensitive function name
	expr = parseExpr(t, "EMBED_TEXT('model1', 'text')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "model1", result.ModelNameWithProvider)
	require.Equal(t, "", result.OptsInJSON)

	// Not a function call
	expr = parseExpr(t, "1 + 2")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "only generated column using EMBED_TEXT() are allowed")
	require.Nil(t, result)

	// Wrong function name
	expr = parseExpr(t, "cos(1)")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "only generated column using EMBED_TEXT() are allowed")
	require.Nil(t, result)

	// Too few arguments
	expr = parseExpr(t, "embed_text('model1')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid EMBED_TEXT() usage")
	require.Nil(t, result)

	// Too many arguments
	expr = parseExpr(t, "embed_text('model1', 'text', '{}', 'extra')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid EMBED_TEXT() usage")
	require.Nil(t, result)

	// Model name not a string constant
	expr = parseExpr(t, "embed_text(col1, 'text')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "EMBED_TEXT() only accepts model name using string constant")
	require.Nil(t, result)

	// Model name is number
	expr = parseExpr(t, "embed_text(123, 'text')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "EMBED_TEXT() only accepts model name using string constant")
	require.Nil(t, result)

	// Options not a string constant
	expr = parseExpr(t, "embed_text('model1', 'text', col1)")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "EMBED_TEXT() only accepts JSON options using string constant")
	require.Nil(t, result)

	// Options is number
	expr = parseExpr(t, "embed_text('model1', 'text', 123)")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "EMBED_TEXT() only accepts JSON options using string constant")
	require.Nil(t, result)

	// Empty model name
	expr = parseExpr(t, "embed_text('', 'text')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "", result.ModelNameWithProvider)
	require.Equal(t, "", result.OptsInJSON)

	// Empty options
	expr = parseExpr(t, "embed_text('model1', 'text', '')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "model1", result.ModelNameWithProvider)
	require.Equal(t, "", result.OptsInJSON)

	// Special characters in model name
	expr = parseExpr(t, "embed_text('provider/model-name_v1.0', 'text')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "provider/model-name_v1.0", result.ModelNameWithProvider)
	require.Equal(t, "", result.OptsInJSON)

	// Complex JSON options
	expr = parseExpr(t, "embed_text('model1', 'text', '{\"temperature\": 0.5, \"max_tokens\": 100, \"nested\": {\"key\": \"value\"}}')")
	result, err = ExtractAutoEmbedInfoFromAST(expr)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "model1", result.ModelNameWithProvider)
	require.Equal(t, "{\"temperature\": 0.5, \"max_tokens\": 100, \"nested\": {\"key\": \"value\"}}", result.OptsInJSON)
}
