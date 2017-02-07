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
)

// Compiler compiles an ast.StmtNode to a stmt.Statement.
type Compiler struct {
}

// Compile compiles an ast.StmtNode to an ast.Statement.
// After preprocessed and validated, it will be optimized to a plan,
// then wrappped to an adapter *statement as stmt.Statement.
func (c *Compiler) Compile(ctx context.Context, node ast.StmtNode) (ast.Statement, error) {
	is := GetInfoSchema(ctx)
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
	stmtCount(node, p)
	sa := &statement{
		is:   is,
		plan: p,
		text: node.Text(),
	}
	return sa, nil
}

// GetInfoSchema gets TxnCtx InfoSchema if snapshot schema is not set,
// Otherwise, snapshot schema is returned.
func GetInfoSchema(ctx context.Context) infoschema.InfoSchema {
	sessVar := ctx.GetSessionVars()
	var is infoschema.InfoSchema
	if snap := sessVar.SnapshotInfoschema; snap != nil {
		is = snap.(infoschema.InfoSchema)
		log.Infof("[%d] use snapshot schema %d", sessVar.ConnectionID, is.SchemaMetaVersion())
	} else {
		is = sessVar.TxnCtx.InfoSchema.(infoschema.InfoSchema)
	}
	return is
}
