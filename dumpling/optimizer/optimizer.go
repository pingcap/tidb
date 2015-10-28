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

package optimizer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/stmt"
)

// Compile compiles a ast.Node into a executable statement.
func Compile(node ast.Node) (stmt.Statement, error) {
	validator := &validator{}
	if _, ok := node.Accept(validator); !ok {
		return nil, errors.Trace(validator.err)
	}

	binder := &InfoBinder{}
	if _, ok := node.Accept(validator); !ok {
		return nil, errors.Trace(binder.Err)
	}

	tpComputer := &typeComputer{}
	if _, ok := node.Accept(tpComputer); !ok {
		return nil, errors.Trace(tpComputer.err)
	}

	switch v := node.(type) {
	case *ast.InsertStmt:
		return convertInsert(v)
	case *ast.DeleteStmt:
		return convertDelete(v)
	case *ast.UpdateStmt:
		return convertUpdate(v)
	case *ast.SelectStmt:
		return convertSelect(v)
	case *ast.UnionStmt:
		return convertUnion(v)
	case *ast.CreateDatabaseStmt:
		return convertCreateDatabase(v)
	case *ast.DropDatabaseStmt:
		return convertDropDatabase(v)
	case *ast.CreateTableStmt:
		return convertCreateTable(v)
	case *ast.DropTableStmt:
		return convertDropTable(v)
	case *ast.CreateIndexStmt:
		return convertCreateIndex(v)
	case *ast.DropIndexStmt:
		return convertDropIndex(v)
	case *ast.AlterTableStmt:
		return convertAlterTable(v)
	case *ast.TruncateTableStmt:
		return convertTruncateTable(v)
	case *ast.ExplainStmt:
		return convertExplain(v)
	case *ast.PrepareStmt:
		return convertPrepare(v)
	case *ast.DeallocateStmt:
		return convertDeallocate(v)
	case *ast.ExecuteStmt:
		return convertExecute(v)
	case *ast.ShowStmt:
		return convertShow(v)
	case *ast.BeginStmt:
		return convertBegin(v)
	case *ast.CommitStmt:
		return convertCommit(v)
	case *ast.RollbackStmt:
		return convertRollback(v)
	case *ast.UseStmt:
		return convertUse(v)
	case *ast.SetStmt:
		return convertSet(v)
	case *ast.SetCharsetStmt:
		return convertSetCharset(v)
	case *ast.SetPwdStmt:
		return convertSetPwd(v)
	case *ast.DoStmt:
		return convertDo(v)
	}
	return nil, nil
}
