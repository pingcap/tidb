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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// Compiler compiles an ast.StmtNode to a stmt.Statement.
type Compiler struct {
}

// Compile compiles an ast.StmtNode to a stmt.Statement.
// If it is supported to use new plan and executer, it optimizes the node to
// a plan, and we wrap the plan in an adapter as stmt.Statement.
// If it is not supported, the node will be converted to old statement.
func (c *Compiler) Compile(ctx context.Context, node ast.StmtNode) (ast.Statement, error) {
	ast.SetFlag(node)
	if _, ok := node.(*ast.UpdateStmt); ok {
		sVars := variable.GetSessionVars(ctx)
		sVars.InUpdateStmt = true
		defer func() {
			sVars.InUpdateStmt = false
		}()
	}

	is := sessionctx.GetDomain(ctx).InfoSchema()
	binloginfo.SetSchemaVersion(ctx, is.SchemaMetaVersion())
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
