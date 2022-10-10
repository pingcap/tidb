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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
)

// GenerateDDLCreateTable rand create table statement
func (e *Executor) GenerateDDLCreateTable(index int, colTypes []string) (*types.SQL, error) {
	tree := createTableStmt()

	stmt, table, err := e.walkDDLCreateTable(index, tree, colTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &types.SQL{
		SQLType:  types.SQLTypeDDLCreateTable,
		SQLTable: table,
		SQLStmt:  stmt,
	}, nil
}

// GenerateDDLCreateIndex rand create index statement
func (e *Executor) GenerateDDLCreateIndex() (*types.SQL, error) {
	tree := createIndexStmt()

	stmt, err := e.walkDDLCreateIndex(tree)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &types.SQL{
		SQLType: types.SQLTypeDDLCreateIndex,
		SQLStmt: stmt,
	}, nil
}

// GenerateDMLInsertByTable rand insert statement for specific table
func (e *Executor) GenerateDMLInsertByTable(table string) (*types.SQL, error) {
	tree := insertStmt()
	stmt, err := e.walkInsertStmtForTable(tree, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &types.SQL{
		SQLType:  types.SQLTypeDMLInsert,
		SQLStmt:  stmt,
		SQLTable: table,
	}, nil
}

func makeConstraintPrimaryKey(node *ast.CreateTableStmt, column string) {
	for _, constraint := range node.Constraints {
		if constraint.Tp == ast.ConstraintPrimaryKey {
			constraint.Keys = append(constraint.Keys, &ast.IndexPartSpecification{
				Column: &ast.ColumnName{
					Name: model.NewCIStr(column),
				},
			})
			return
		}
	}
	node.Constraints = append(node.Constraints, &ast.Constraint{
		Tp: ast.ConstraintPrimaryKey,
		Keys: []*ast.IndexPartSpecification{
			{
				Column: &ast.ColumnName{
					Name: model.NewCIStr(column),
				},
			},
		},
	})
}
