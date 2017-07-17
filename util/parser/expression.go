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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// getDefaultCharsetAndCollate is copyed from ddl/ddl_api.go.
func getDefaultCharsetAndCollate() (string, string) {
	return "utf8", "utf8_bin"
}

// nameResolver is the visitor to resolve table name and column name.
// it combines TableInfo and ColumnInfo to a generation expression.
type nameResolver struct {
	tableInfo *model.TableInfo
	err       error
}

// Enter implements ast.Visitor interface.
func (nr *nameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (nr *nameResolver) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		for _, col := range nr.tableInfo.Columns {
			if col.Name.L == v.Name.Name.L {
				v.Refer = &ast.ResultField{
					Column: col,
					Table:  nr.tableInfo,
				}
				return inNode, true
			}
		}
		nr.err = errors.Errorf("can't find column %s in %s", v.Name.Name.O, nr.tableInfo.Name.O)
		return inNode, false
	}
	return inNode, true
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
		node = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	return node, errors.Trace(err)
}

// SimpleResolveName resolves all column names in the expression node.
func SimpleResolveName(node ast.ExprNode, tblInfo *model.TableInfo) (ast.ExprNode, error) {
	nr := nameResolver{tblInfo, nil}
	if _, ok := node.Accept(&nr); !ok {
		return nil, errors.Trace(nr.err)
	}
	sc := new(variable.StatementContext) // here we use the default StatementContext.
	err := expression.InferType(sc, node)
	return node, errors.Trace(err)
}
