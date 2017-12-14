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
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/plan"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
	Ctx context.Context
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(goCtx goctx.Context, stmtNode ast.StmtNode) (*ExecStmt, error) {
	if span := opentracing.SpanFromContext(goCtx); span != nil {
		span1 := opentracing.StartSpan("executor.Compile", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	infoSchema := GetInfoSchema(c.Ctx)
	if err := plan.Preprocess(c.Ctx, stmtNode, infoSchema, false); err != nil {
		return nil, errors.Trace(err)
	}

	finalPlan, err := plan.Optimize(c.Ctx, stmtNode, infoSchema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ExecStmt{
		InfoSchema: infoSchema,
		Plan:       finalPlan,
		Expensive:  stmtCount(stmtNode, finalPlan, c.Ctx.GetSessionVars().InRestrictedSQL),
		Cacheable:  plan.Cacheable(stmtNode),
		Text:       stmtNode.Text(),
		StmtNode:   stmtNode,
		Ctx:        c.Ctx,
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
	return is
}
