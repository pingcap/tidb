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

package converter

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/stmt"
)

// Converter converts ast.Node into an old stmt.Statement.
type Converter struct {
	converter *expressionConverter
}

// Convert converts an ast.Node into an old stmt.Statement.
func (con *Converter) Convert(node ast.Node) (stmt.Statement, error) {
	c := newExpressionConverter()
	con.converter = c
	switch v := node.(type) {
	case *ast.AlterTableStmt:
		return convertAlterTable(c, v)
	case *ast.CreateDatabaseStmt:
		return convertCreateDatabase(c, v)
	case *ast.CreateIndexStmt:
		return convertCreateIndex(c, v)
	case *ast.CreateTableStmt:
		return convertCreateTable(c, v)
	case *ast.DeleteStmt:
		return convertDelete(c, v)
	case *ast.DropDatabaseStmt:
		return convertDropDatabase(c, v)
	case *ast.DropIndexStmt:
		return convertDropIndex(c, v)
	case *ast.DropTableStmt:
		return convertDropTable(c, v)
	case *ast.InsertStmt:
		return convertInsert(c, v)
	case *ast.SelectStmt:
		return convertSelect(c, v)
	case *ast.UnionStmt:
		return convertUnion(c, v)
	case *ast.UpdateStmt:
		return convertUpdate(c, v)
	}
	return nil, nil
}
