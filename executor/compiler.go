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
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/plan"
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (*ExecStmt, error) {
	infoSchema := GetInfoSchema(ctx)
	if err := plan.Preprocess(stmtNode, infoSchema, ctx); err != nil {
		return nil, errors.Trace(err)
	}
	// Validate should be after NameResolve.
	if err := plan.Validate(stmtNode, false); err != nil {
		return nil, errors.Trace(err)
	}

	finalPlan, err := plan.Optimize(ctx, stmtNode, infoSchema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// check whether the stmt is read only
	readOnly := false
	if e, ok := stmtNode.(*ast.ExecuteStmt); ok {
		vars := ctx.GetSessionVars()

		if id, ok := vars.PreparedStmtNameToID[e.Name]; ok {
			v := vars.PreparedStmts[id]
			if v == nil {
				return nil, errors.Trace(ErrStmtNotFound)
			}

			prepared := v.(*Prepared)
			readOnly = ast.IsReadOnly(prepared.Stmt)
		}
	} else {
		readOnly = ast.IsReadOnly(stmtNode)
	}

	return &ExecStmt{
		InfoSchema: infoSchema,
		Plan:       finalPlan,
		Expensive:  stmtCount(stmtNode, finalPlan, ctx.GetSessionVars().InRestrictedSQL),
		Cacheable:  plan.Cacheable(stmtNode),
		Text:       stmtNode.Text(),
		ReadOnly:   readOnly,
		Ctx:        ctx,
		StmtNode:   stmtNode,
	}, nil
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
	ctx.GetSessionVars().StmtCtx.DebugLog += fmt.Sprintf("-------[get infoschema] ver %v;", is.SchemaMetaVersion())
	return is
}
