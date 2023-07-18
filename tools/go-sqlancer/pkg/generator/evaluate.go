// Copyright 2022 PingCAP, Inc.
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

package generator

import (
	"fmt"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/generator/operator"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

// Evaluate is return ValueExpr
// TODO: it seems this function is useless, remove it?
func Evaluate(e ast.Node, genCtx *GenCtx) parser_driver.ValueExpr {
	switch t := e.(type) {
	case *ast.ParenthesesExpr:
		return Evaluate(t.Expr, genCtx)
	case *ast.BinaryOperationExpr:
		res, err := operator.BinaryOps.Eval(t.Op.String(), Evaluate(t.L, genCtx), Evaluate(t.R, genCtx))
		if err != nil {
			panic(fmt.Sprintf("error occurred on eval: %+v", err))
		}
		return res
	case *ast.UnaryOperationExpr:
		res, err := operator.UnaryOps.Eval(t.Op.String(), Evaluate(t.V, genCtx))
		if err != nil {
			panic(fmt.Sprintf("error occurred on eval: %+v", err))
		}
		return res
	case *ast.IsNullExpr:
		subResult := Evaluate(t.Expr, genCtx)
		c := util.ConvertToBoolOrNull(subResult)
		r := parser_driver.ValueExpr{}
		r.SetInt64(0)
		if c == -1 {
			r.SetInt64(1)
		}
		return r
	case *ast.ColumnNameExpr:
		for key, value := range genCtx.unwrapPivotRows {
			originTableName := t.Name.Table.L
			for k, v := range genCtx.TableAlias {
				if v == originTableName {
					originTableName = k
					break
				}
			}
			originColumnName := t.Name.Name.L
			if key == fmt.Sprintf("%s.%s", originTableName, originColumnName) {
				v := parser_driver.ValueExpr{}
				v.SetValue(value)
				if tmpTable, ok := genCtx.TableAlias[t.Name.Table.L]; ok {
					t.Name.Table = model.NewCIStr(tmpTable)
				}
				return v
			}
		}
		panic(fmt.Sprintf("no such col %s in table %s", t.Name, t.Name.Table))
	case ast.ValueExpr:
		v := parser_driver.ValueExpr{}
		v.SetValue(t.GetValue())
		v.SetType(t.GetType())
		return v
	}

	// is useless?
	// if e == nil {
	// 	return trueValueExpr()
	// }

	panic("not reachable")
	//v := parser_driver.ValueExpr{}
	//v.SetNull()
	//return v
}
