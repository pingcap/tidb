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

package transformer

import (
	"github.com/pingcap/tidb/parser/ast"
)

// UnionTrans TODO: implement union Transformer
var UnionTrans Singleton = func(nodes []ast.ResultSetNode) []ast.ResultSetNode {
	//if len(nodes) > 1 {
	//	nodes = append(nodes, union(nodes))
	//}
	return nodes
}

// TODO: implement
func union(_ []ast.ResultSetNode) *ast.SetOprSelectList {
	return nil
	/*
		selects := make([]*ast.SelectStmt, 0)
		for _, node := range nodes {
			switch stmt := node.(type) {
			case *ast.SelectStmt:
				selects = append(selects, stmt)
			case *ast.SetOprSelectList:
				if stmt.SelectList != nil && stmt.SelectList.Selects != nil {
					selects = append(selects, stmt.SelectList.Selects...)
				}
			}
		}
		return &ast.SetOprSelectList{SelectList: &ast.UnionSelectList{Selects: selects}}
	*/
}
