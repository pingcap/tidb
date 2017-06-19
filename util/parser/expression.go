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

package parser

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

// getDefaultCharsetAndCollate is copyed from ddl/ddl_api.go.
func getDefaultCharsetAndCollate() (string, string) {
	return "utf8", "utf8_bin"
}

// ParseExpression parses an ExprNode from a string.
// Where should we use this?
//   When TiDB bootstraps, it'll load infoschema from TiKV.
//   Because some ColumnInfos have attribute `GeneratedExprString`,
//   we need to parse that string into ast.ExprNode.
func ParseExpression(expr string) (node ast.ExprNode, err error) {
	expr = fmt.Sprintf("select %s", expr)
	charset, collation := getDefaultCharsetAndCollate()
	stmts, err := parser.New().Parse(expr, charset, collation)
	if err == nil {
		sel := stmts[0].(*ast.SelectStmt)
		node = sel.Fields.Fields[0].Expr
	}
	return node, errors.Trace(err)
}
