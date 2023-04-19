package core

import (
	"context"

	"github.com/pingcap/tidb/parser/ast"
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
