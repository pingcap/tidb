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

package executor

import (
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
)

func createTableStmt() *ast.CreateTableStmt {
	createTableNode := ast.CreateTableStmt{
		Table:       &ast.TableName{},
		Cols:        []*ast.ColumnDef{},
		Constraints: []*ast.Constraint{},
		Options:     []*ast.TableOption{},
	}
	// TODO: config for enable partition
	// partitionStmt is disabled
	if util.Rd(2) == 0 {
		createTableNode.Partition = partitionStmt()
	}

	return &createTableNode
}

func createIndexStmt() *ast.CreateIndexStmt {
	var indexType model.IndexType
	switch util.Rd(2) {
	case 0:
		indexType = model.IndexTypeBtree
	default:
		indexType = model.IndexTypeHash
	}

	node := ast.CreateIndexStmt{
		Table:                   &ast.TableName{},
		IndexPartSpecifications: []*ast.IndexPartSpecification{},
		IndexOption: &ast.IndexOption{
			Tp: indexType,
		},
	}
	return &node
}

func insertStmt() *ast.InsertStmt {
	insertStmtNode := ast.InsertStmt{
		Table: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableName{},
			},
		},
		Lists:   [][]ast.ExprNode{},
		Columns: []*ast.ColumnName{},
	}
	return &insertStmtNode
}

func partitionStmt() *ast.PartitionOptions {
	return &ast.PartitionOptions{
		PartitionMethod: ast.PartitionMethod{
			ColumnNames: []*ast.ColumnName{},
		},
		Definitions: []*ast.PartitionDefinition{},
	}
}
