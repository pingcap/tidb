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

package generatedexpr

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/stretchr/testify/require"
)

func TestParseExpression(t *testing.T) {
	node, err := ParseExpression("json_extract(a, '$.a')")
	require.NoError(t, err)
	require.Equal(t, "json_extract", node.(*ast.FuncCallExpr).FnName.L)
}

func TestParseExpressionWithSQLMode(t *testing.T) {
	defaultSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)

	node, err := ParseExpressionWithSQLMode(`1 || 2`, mysql.ModePipesAsConcat)
	require.NoError(t, err)
	require.Equal(t, ast.Concat, node.(*ast.FuncCallExpr).FnName.L)

	node, err = ParseExpressionWithSQLMode(`1 || 2`, defaultSQLMode)
	require.NoError(t, err)
	require.Equal(t, opcode.LogicOr, node.(*ast.BinaryOperationExpr).Op)

	node, err = ParseExpressionWithSQLMode(`'\n'`, defaultSQLMode)
	require.NoError(t, err)
	require.Equal(t, "\n", node.(ast.ValueExpr).GetString())

	node, err = ParseExpressionWithSQLMode(`'\n'`, mysql.ModeNoBackslashEscapes)
	require.NoError(t, err)
	require.Equal(t, `\n`, node.(ast.ValueExpr).GetString())
}
