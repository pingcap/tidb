// Copyright 2022 PingCAP, Inc.
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

package knownbugs

import (
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	tidb_types "github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, sql string) ast.Node {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		t.Fatalf("got %v", err)
	}
	sel := stmtNodes[0].(*ast.SelectStmt)
	return sel
}

func TestCase1(t *testing.T) {
	s := parse(t, "SELECT * FROM t0 WHERE ('a' != t0.c0) AND t2.c")
	sl := s.(*ast.SelectStmt)
	sl.Where.(*ast.BinaryOperationExpr).L.(*ast.ParenthesesExpr).Expr.(*ast.BinaryOperationExpr).R.SetType(tidb_types.NewFieldType(mysql.TypeInt24))
	sl.Where.(*ast.BinaryOperationExpr).R.SetType(tidb_types.NewFieldType(mysql.TypeInt24))
	d := NewDustbin([]ast.Node{sl}, nil)
	require.Equal(t, issue16788(&d), false)
}

func TestCase2(t *testing.T) {
	s := parse(t, "SELECT * FROM t0 WHERE (0.5 = t0.c0) AND (5 > NULL AND t0.c1)")
	sl := s.(*ast.SelectStmt)
	sl.Where.(*ast.BinaryOperationExpr).L.(*ast.ParenthesesExpr).Expr.(*ast.BinaryOperationExpr).R.SetType(tidb_types.NewFieldType(mysql.TypeInt24))
	d := NewDustbin([]ast.Node{sl}, nil)
	require.Equal(t, issue16788(&d), true)
}

func TestCase3(t *testing.T) {
	s := parse(t, "SELECT * FROM t0 LEFT JOIN t1 ON (0.5 != t0.c0) AND ((5 > NULL) AND (t1.c1 = 0.55)) WHERE 1 = 1")
	sl := s.(*ast.SelectStmt)
	sl.From.TableRefs.On.Expr.(*ast.BinaryOperationExpr).R.(*ast.ParenthesesExpr).Expr.(*ast.BinaryOperationExpr).R.(*ast.ParenthesesExpr).Expr.(*ast.BinaryOperationExpr).L.SetType(tidb_types.NewFieldType(mysql.TypeInt24))
	d := NewDustbin([]ast.Node{sl}, nil)
	require.Equal(t, issue16788(&d), true)
}
