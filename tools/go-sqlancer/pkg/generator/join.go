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

package generator

import (
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
)

func (g *Generator) walkTableRefs(node *ast.Join, genCtx *GenCtx) {
	if node.Right == nil {
		if node, ok := node.Left.(*ast.TableSource); ok {
			if tn, ok := node.Source.(*ast.TableName); ok {
				if table := genCtx.findUsedTableByName(tn.Name.L); table != nil {
					genCtx.ResultTables = append(genCtx.ResultTables, *table)
					return
				}
			}
		}
		panic("unreachable")
	}

	if right, ok := node.Right.(*ast.TableSource); ok {
		var (
			leftTables []types.Table
			rightTable types.Table
		)
		switch node := node.Left.(type) {
		case *ast.Join:
			g.walkTableRefs(node, genCtx)
			leftTables = genCtx.ResultTables
		case *ast.TableSource:
			if tn, ok := node.Source.(*ast.TableName); ok {
				if table := genCtx.findUsedTableByName(tn.Name.L); table != nil {
					tmpTable := genCtx.createTmpTable()
					node.AsName = model.NewCIStr(tmpTable)
					leftTables = []types.Table{table.Rename(tmpTable)}
					genCtx.TableAlias[table.Name.String()] = tmpTable
					break
				}
			}
		default:
			panic("unreachable")
		}
		if table := genCtx.findUsedTableByName(right.Source.(*ast.TableName).Name.L); table != nil {
			tmpTable := genCtx.createTmpTable()
			right.AsName = model.NewCIStr(tmpTable)
			rightTable = table.Rename(tmpTable)
			genCtx.TableAlias[table.Name.String()] = tmpTable
		} else {
			panic("unreachable")
		}
		allTables := append(leftTables, rightTable)
		// usedTables := genCtx.UsedTables
		genCtx.ResultTables = allTables
		// genCtx.UsedTables = allTables
		// defer func() {
		// 	genCtx.UsedTables = usedTables
		// }()
		node.On = &ast.OnCondition{}
		// for _, table := range genCtx.ResultTables {
		// 	fmt.Println(table.Name, table.AliasName)
		// }
		node.On.Expr = g.ConditionClause(genCtx, 1)
		return
	}

	panic("unreachable")
}
