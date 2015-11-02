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
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/stmt"
)

// Compiler compiles ast.Node into an executable statement.
type Compiler struct {
	converter *expressionConverter
}

// Compile compiles a ast.Node into an executable statement.
func (com *Compiler) Compile(node ast.Node) (stmt.Statement, error) {
	validator := &validator{}
	if _, ok := node.Accept(validator); !ok {
		return nil, errors.Trace(validator.err)
	}

	//	binder := &InfoBinder{}
	//	if _, ok := node.Accept(validator); !ok {
	//		return nil, errors.Trace(binder.Err)
	//	}

	tpComputer := &typeComputer{}
	if _, ok := node.Accept(tpComputer); !ok {
		return nil, errors.Trace(tpComputer.err)
	}
	c := newExpressionConverter()
	com.converter = c
	switch v := node.(type) {
	case *ast.InsertStmt:
		return convertInsert(c, v)
	case *ast.DeleteStmt:
		return convertDelete(c, v)
	case *ast.UpdateStmt:
		return convertUpdate(c, v)
	case *ast.SelectStmt:
		return convertSelect(c, v)
	case *ast.UnionStmt:
		return convertUnion(c, v)
	case *ast.CreateDatabaseStmt:
		return convertCreateDatabase(c, v)
	case *ast.DropDatabaseStmt:
		return convertDropDatabase(c, v)
	case *ast.CreateTableStmt:
		return convertCreateTable(c, v)
	case *ast.DropTableStmt:
		return convertDropTable(c, v)
	case *ast.CreateIndexStmt:
		return convertCreateIndex(c, v)
	case *ast.DropIndexStmt:
		return convertDropIndex(c, v)
	case *ast.AlterTableStmt:
		return convertAlterTable(c, v)
	case *ast.TruncateTableStmt:
		return convertTruncateTable(c, v)
	case *ast.ExplainStmt:
		return convertExplain(c, v)
	case *ast.PrepareStmt:
		return convertPrepare(c, v)
	case *ast.DeallocateStmt:
		return convertDeallocate(c, v)
	case *ast.ExecuteStmt:
		return convertExecute(c, v)
	case *ast.ShowStmt:
		return convertShow(c, v)
	case *ast.BeginStmt:
		return convertBegin(c, v)
	case *ast.CommitStmt:
		return convertCommit(c, v)
	case *ast.RollbackStmt:
		return convertRollback(c, v)
	case *ast.UseStmt:
		return convertUse(c, v)
	case *ast.SetStmt:
		return convertSet(c, v)
	case *ast.SetCharsetStmt:
		return convertSetCharset(c, v)
	case *ast.SetPwdStmt:
		return convertSetPwd(c, v)
	case *ast.CreateUserStmt:
		return convertCreateUser(c, v)
	case *ast.DoStmt:
		return convertDo(c, v)
	case *ast.GrantStmt:
		return convertGrant(c, v)
	}
	return nil, nil
}

type paramMarkers []*ast.ParamMarkerExpr

func (p paramMarkers) Len() int {
	return len(p)
}

func (p paramMarkers) Less(i, j int) bool {
	return p[i].Offset < p[j].Offset
}

func (p paramMarkers) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// ParamMarkers returns parameter markers for prepared statement.
func (com *Compiler) ParamMarkers() []*expression.ParamMarker {
	c := com.converter
	sort.Sort(c.paramMarkers)
	oldMarkers := make([]*expression.ParamMarker, len(c.paramMarkers))
	for i, val := range c.paramMarkers {
		oldMarkers[i] = c.exprMap[val].(*expression.ParamMarker)
	}
	return oldMarkers
}
