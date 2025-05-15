package ast

import (
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/types"
)

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
