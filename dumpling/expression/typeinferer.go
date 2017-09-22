// Copyright 2015 PingCAP, Inc.
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

package expression

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// InferType infers result type for ast.ExprNode.
func InferType(sc *variable.StatementContext, node ast.Node) error {
	var inferrer typeInferrer
	inferrer.sc = sc
	// TODO: get the default charset from ctx
	inferrer.defaultCharset = "utf8"
	node.Accept(&inferrer)
	return inferrer.err
}

type typeInferrer struct {
	sc             *variable.StatementContext
	err            error
	defaultCharset string
}

func (v *typeInferrer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch in.(type) {
	case *ast.ColumnOption:
		return in, true
	}
	return in, false
}

func (v *typeInferrer) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.BetweenExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
	case *ast.ColumnNameExpr:
		x.SetType(&x.Refer.Column.FieldType)
	case *ast.CompareSubqueryExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
	case *ast.ExistsSubqueryExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
		types.SetBinChsClnFlag(&x.Type)
	case *ast.ParamMarkerExpr:
		types.DefaultTypeForValue(x.GetValue(), x.GetType())
	case *ast.ParenthesesExpr:
		x.SetType(x.Expr.GetType())
	case *ast.SelectStmt:
		v.selectStmt(x)
	case *ast.VariableExpr:
		x.SetType(types.NewFieldType(mysql.TypeVarString))
		x.Type.Charset = v.defaultCharset
		cln, err := charset.GetDefaultCollation(v.defaultCharset)
		if err != nil {
			v.err = err
		}
		x.Type.Collate = cln
		// TODO: handle all expression types.
	}
	return in, true
}

func (v *typeInferrer) selectStmt(x *ast.SelectStmt) {
	rf := x.GetResultFields()
	for _, val := range rf {
		// column ID is 0 means it is not a real column from table, but a temporary column,
		// so its type is not pre-defined, we need to set it.
		if val.Column.ID == 0 && val.Expr.GetType() != nil {
			val.Column.FieldType = *(val.Expr.GetType())
		}
	}
}
