package core

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
)

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
