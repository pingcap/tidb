package ast

import (
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// CreateLoadableFunctionStmt represents the ast of `CREATE FUNCTION ... SONAME ...`
type CreateLoadableFunctionStmt struct {
	ddlNode

	Aggregate   bool
	IfNotExists bool
	Name        *TableName
	ReturnType  types.EvalType
	SoName      string
}

// Restore implements Node interface.
func (n *CreateLoadableFunctionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE")
	if n.Aggregate {
		ctx.WriteKeyWord(" AGGREGATE")
	}
	ctx.WriteKeyWord(" FUNCTION")
	if n.IfNotExists {
		ctx.WriteKeyWord(" IF NOT EXISTS")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return err
	}
	ctx.WriteKeyWord(" RETURNS ")
	ctx.WriteKeyWord(n.ReturnType.String())
	ctx.WriteKeyWord(" SONAME ")
	ctx.WriteString(n.SoName)
	return nil
}

// Accept implements Node interface.
func (n *CreateLoadableFunctionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	n = newNode.(*CreateLoadableFunctionStmt)
	if skipChildren {
		return v.Leave(n)
	}

	if n.Name != nil {
		newName, ok := n.Name.Accept(v)
		if !ok {
			return n, false
		}
		n.Name = newName.(*TableName)
	}
	return v.Leave(n)
}

// DropFunctionStmt represents the ast of `DROP FUNCTION ...`
type DropFunctionStmt struct {
	stmtNode

	IfExists bool
	Name     *TableName
}

// Restore implements Node interface.
func (n *DropFunctionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP FUNCTION ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	err := n.Name.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements Node interface.
func (n *DropFunctionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropFunctionStmt)
	return v.Leave(n)
}
