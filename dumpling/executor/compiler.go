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

func statementLabel(node ast.StmtNode) string {
	switch node.(type) {
	case *ast.SelectStmt:
		return "Select"
	case *ast.DeleteStmt:
		return "Delete"
	case *ast.InsertStmt:
		if node.(*ast.InsertStmt).IsReplace {
			return "Replace"
		}
		return "Insert"
	case *ast.UnionStmt:
		return "Union"
	case *ast.UpdateStmt:
		return "Update"
	case *ast.CreateIndexStmt:
		return "CreateIndex"
	case *ast.CreateTableStmt:
		return "CreateTable"
	case *ast.CreateDatabaseStmt:
		return "CreateDatabase"
	case *ast.DropDatabaseStmt:
		return "DropDatabase"
	case *ast.DropTableStmt:
		return "DropTable"
	case *ast.DropIndexStmt:
		return "DropIndex"
	}
	return "unknown"
}

// Compile compiles an ast.StmtNode to an ast.Statement.
// After preprocessed and validated, it will be optimized to a plan,
// then wrappped to an adapter *statement as stmt.Statement.
func (c *Compiler) Compile(ctx context.Context, node ast.StmtNode) (ast.Statement, error) {
	stmtNodeCounter.WithLabelValues(statementLabel(node)).Inc()
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
