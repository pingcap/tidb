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
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
)

var (
	issue16788 = func(d *Dustbin) bool {
		v := NewVisitor()
		mergeOnClause := func(n ast.Node) []ast.ExprNode {
			result := make([]ast.ExprNode, 0)
			v.SetEnter(func(in ast.Node) (ast.Node, bool) {
				switch t := in.(type) {
				case *ast.Join:
					if t.On != nil {
						result = append(result, t.On.Expr)
					}
				default:
					return in, false
				}
				return in, false
			})
			defer func() {
				v.ClearEnter()
			}()
			n.Accept(&v)
			return result
		}

		find := func(n ast.Node) bool {
			result := false
			v.SetEnter(func(in ast.Node) (ast.Node, bool) {
				switch tp := in.(type) {
				case *ast.ColumnNameExpr:
					return in, true
				case ast.ValueExpr:
					return in, true
				case *ast.BinaryOperationExpr:
					if tp.Op != opcode.EQ {
						return in, false
					}
					if lType, ok := tp.L.(ast.ValueExpr); ok {
						if lType.GetType().GetType() != mysql.TypeFloat && lType.GetType().GetType() != mysql.TypeDouble && lType.GetType().GetType() != mysql.TypeNewDecimal {
							return in, false
						}
						rType, ok := tp.R.(*ast.ColumnNameExpr)
						if !ok {
							return in, false
						}
						rTp := rType.GetType().GetType()
						if rTp != mysql.TypeInt24 &&
							rTp != mysql.TypeLong &&
							rTp != mysql.TypeLonglong &&
							rTp != mysql.TypeShort &&
							rTp != mysql.TypeTiny {
							return in, false
						}
						// TODO: select from pivot row and verify it is NULL
						result = true
						return in, true
					}
					if rType, ok := tp.R.(ast.ValueExpr); ok {
						if rType.GetType().GetType() != mysql.TypeFloat && rType.GetType().GetType() != mysql.TypeDouble && rType.GetType().GetType() != mysql.TypeNewDecimal {
							return in, false
						}
						lType, ok := tp.L.(*ast.ColumnNameExpr)
						if !ok {
							return in, false
						}
						lTp := lType.GetType().GetType()
						if lTp != mysql.TypeInt24 &&
							lTp != mysql.TypeLong &&
							lTp != mysql.TypeLonglong &&
							lTp != mysql.TypeShort &&
							lTp != mysql.TypeTiny {
							return in, false
						}
						// TODO: select from pivot row and verify it is NULL

						result = true
						return in, true
					}
					return in, false
				default:
					return in, false
				}
			})
			defer func() {
				v.ClearEnter()
			}()
			n.Accept(&v)
			return result
		}

		stmt := d.Stmts[:len(d.Stmts)][0]
		switch t := stmt.(type) {
		case *ast.SelectStmt:
			exprs := mergeOnClause(t.From.TableRefs)
			exprs = append(exprs, t.Where)
			for _, e := range exprs {
				if find(e) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
)
