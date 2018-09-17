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

package ast_test

import (
	. "github.com/pingcap/check"
	. "github.com/pingcap/tidb/ast"
)

var _ = Suite(&testFunctionsSuite{})

type testFunctionsSuite struct {
}

func (ts *testFunctionsSuite) TestFunctionsVisitorCover(c *C) {
	stmts := []Node{
		&AggregateFuncExpr{Args: []ExprNode{&ValueExpr{}}},
		&FuncCallExpr{Args: []ExprNode{&ValueExpr{}}},
		&FuncCastExpr{Expr: &ValueExpr{}},
	}

	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}
