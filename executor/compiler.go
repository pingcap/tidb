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

package executor

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// Compiler compiles an ast.StmtNode to a stmt.Statement.
type Compiler struct {
}

const (
	// IGNORE is a special label to identify the situations we want to ignore.
	IGNORE = "Ignore"
)

func statementLabel(node ast.StmtNode) string {
	switch x := node.(type) {
	case *ast.AlterTableStmt:
		return "AlterTable"
	case *ast.AnalyzeTableStmt:
		return "AnalyzeTable"
	case *ast.BeginStmt:
		return "Begin"
	case *ast.CommitStmt:
		return "Commit"
	case *ast.CreateDatabaseStmt:
		return "CreateDatabase"
	case *ast.CreateIndexStmt:
		return "CreateIndex"
	case *ast.CreateTableStmt:
		return "CreateTable"
	case *ast.CreateUserStmt:
		return "CreateUser"
	case *ast.DeleteStmt:
		return "Delete"
	case *ast.DropDatabaseStmt:
		return "DropDatabase"
	case *ast.DropIndexStmt:
		return "DropIndex"
	case *ast.DropTableStmt:
		return "DropTable"
	case *ast.ExplainStmt:
		return "Explain"
	case *ast.InsertStmt:
		if x.IsReplace {
			return "Replace"
		}
		return "Insert"
	case *ast.LoadDataStmt:
		return "LoadData"
	case *ast.RollbackStmt:
		return "Rollback"
	case *ast.SelectStmt:
		return distinguishSelectStmt(x)
	case *ast.SetStmt, *ast.SetPwdStmt:
		return "Set"
	case *ast.ShowStmt:
		return "Show"
	case *ast.TruncateTableStmt:
		return "TruncateTable"
	case *ast.UpdateStmt:
		return "Update"
	case *ast.DeallocateStmt, *ast.ExecuteStmt, *ast.PrepareStmt, *ast.UseStmt:
		return IGNORE
	}
	return "other"
}

func distinguishSelectStmt(x *ast.SelectStmt) string {
	if x.From == nil {
		return "Select-Simple"
	}
	tableRefs := x.From.TableRefs
	if tableRefs.Right == nil {
		switch ref := tableRefs.Left.(type) {
		case *ast.TableSource:
			switch ref.Source.(type) {
			case *ast.TableName:
				return "Select-Simple"
			}
		}
	}
	return "Select-Complex"
}

// Compile compiles an ast.StmtNode to an ast.Statement.
// After preprocessed and validated, it will be optimized to a plan,
// then wrappped to an adapter *statement as stmt.Statement.
func (c *Compiler) Compile(ctx context.Context, node ast.StmtNode) (ast.Statement, error) {
	stmtCount(node)
	if _, ok := node.(*ast.UpdateStmt); ok {
		sVars := variable.GetSessionVars(ctx)
		sVars.InUpdateStmt = true
		defer func() {
			sVars.InUpdateStmt = false
		}()
	}

	var is infoschema.InfoSchema
	sessVar := variable.GetSessionVars(ctx)
	if snap := sessVar.SnapshotInfoschema; snap != nil {
		is = snap.(infoschema.InfoSchema)
		log.Infof("[%d] use snapshot schema %d", sessVar.ConnectionID, is.SchemaMetaVersion())
	} else {
		is = sessionctx.GetDomain(ctx).InfoSchema()
		binloginfo.SetSchemaVersion(ctx, is.SchemaMetaVersion())
	}
	if err := plan.Preprocess(node, is, ctx); err != nil {
		return nil, errors.Trace(err)
	}
	// Validate should be after NameResolve.
	if err := plan.Validate(node, false); err != nil {
		return nil, errors.Trace(err)
	}
	p, err := plan.Optimize(ctx, node, is)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, isDDL := node.(ast.DDLNode)
	sa := &statement{
		is:    is,
		plan:  p,
		text:  node.Text(),
		isDDL: isDDL,
	}
	return sa, nil
}
