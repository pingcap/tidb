// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
)

// ProcedurebodyInfo Store stored procedure content read from the table.
type ProcedurebodyInfo struct {
	Name                string
	Procedurebody       string
	SQLMode             string
	CharacterSetClient  string
	CollationConnection string
	ShemaCollation      string
}

// ProcedureParameterVal Store stored procedure parameter.
type ProcedureParameterVal struct {
	DeclName  string
	DeclType  *types.FieldType
	DeclInput ast.ExprNode
	ParamType int
}

// ProcedureBaseBody base store procedure structure.
type ProcedureBaseBody interface {
}

// ProcedureExecPlan tidb procedure execute interface.
type ProcedureExecPlan interface {
	Execute(ctx context.Context, sctx sessionctx.Context, id *uint) error
	GetContext() *variable.ProcedureContext
}

// ProcedureExec store procedure plans.
type ProcedureExec struct {
	ProcedureCommandList []ProcedureExecPlan
}

// ProcedurePlan store call plan.
type ProcedurePlan struct {
	IfNotExists       bool
	ProcedureName     *ast.TableName
	ProcedureParam    []*ProcedureParameterVal
	Procedurebody     ProcedureBaseBody
	ProcedureExecPlan ProcedureExec
}

// buildCreateProcedure Generate create stored procedure plan.
func (b *PlanBuilder) buildCreateProcedure(ctx context.Context, node *ast.ProcedureInfo) (Plan, error) {
	p := &CreateProcedure{ProcedureInfo: node, is: b.is}
	procedurceSchema := node.ProcedureName.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.ProcedureName.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, errors.Trace(ErrNoDB)
	}
	return p, nil
}

// buildDropProcedure Generate drop stored procedure plan.
func (b *PlanBuilder) buildDropProcedure(ctx context.Context, node *ast.DropProcedureStmt) (Plan, error) {
	p := &DropProcedure{Procedure: node, is: b.is}
	procedurceSchema := node.ProcedureName.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.ProcedureName.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, errors.Trace(ErrNoDB)
	}
	return p, nil
}

// buildCallProcedure Generate call command execution plan.
func (b *PlanBuilder) buildCallProcedure(ctx context.Context, node *ast.CallStmt) (outplan Plan, err error) {
	p := &CallStmt{Callstmt: node, Is: b.is}
	// get database name.
	procedurceSchema := node.Procedure.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.Procedure.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, errors.Trace(ErrNoDB)
	}
	// Check if database exists
	_, ok := b.is.SchemaByName(node.Procedure.Schema)
	if !ok {
		return nil, ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}

	return p, nil
}
