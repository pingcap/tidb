package ast

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
)

const (
	TICLASSORIGIN       = 0
	TIFIRSTPROPERTY     = TICLASSORIGIN
	TISUBCLASSORIGIN    = 1
	TICONSTRAINTCATALOG = 2
	TICONSTRAINTSCHEMA  = 3
	TICONSTRAINTNAME    = 4
	TICATALOGNAME       = 5
	TISCHEMANAME        = 6
	TITABLENAME         = 7
	TICOLUMNNAME        = 8
	TICURSORNAME        = 9
	TIMESSAGETEXT       = 10
	TIMYSQLERRNO        = 11
	TILASTPROPERTY      = TIMYSQLERRNO
	TIRETURNEDSQLSTATE  = 12
)

const (
	TICURRENT = 0
	TISTACKED = 1
)

const (
	TINUMBER   = 0
	TIROWCOUNT = 1
)

type DiagnosticsInformation interface {
	Node
}

type SignalInfo struct {
	node
	Name  int
	Value ExprNode
}

type Signal struct {
	stmtNode

	ErrorCon   ErrNode
	SignalCons []*SignalInfo
}

func (n *Signal) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SIGNAL ")
	err := n.ErrorCon.Restore(ctx)
	if err != nil {
		return err
	}
	if len(n.SignalCons) != 0 {
		ctx.WriteKeyWord(" SET")
	}
	for i, SignalCon := range n.SignalCons {
		err = SignalCon.Restore(ctx)
		if err != nil {
			return nil
		}
		if i != len(n.SignalCons)-1 {
			ctx.WriteKeyWord(",")
		}
	}
	return nil
}
func GetSignalString(i int) (string, error) {
	switch i {
	case TICLASSORIGIN:
		return "CLASS_ORIGIN", nil
	case TISUBCLASSORIGIN:
		return "SUBCLASS_ORIGIN", nil
	case TICONSTRAINTCATALOG:
		return "CONSTRAINT_CATALOG", nil
	case TICONSTRAINTSCHEMA:
		return "CONSTRAINT_SCHEMA", nil
	case TICONSTRAINTNAME:
		return "CONSTRAINT_NAME", nil
	case TICATALOGNAME:
		return "CATALOG_NAME", nil
	case TISCHEMANAME:
		return "SCHEMA_NAME", nil
	case TITABLENAME:
		return "TABLE_NAME", nil
	case TICOLUMNNAME:
		return "COLUMN_NAME", nil
	case TICURSORNAME:
		return "CURSOR_NAME", nil
	case TIMYSQLERRNO:
		return "MYSQL_ERRNO", nil
	case TIMESSAGETEXT:
		return "MESSAGE_TEXT", nil
	default:
		return "", errors.Errorf("unspport condition_information_item_name")
	}
}
func (n *SignalInfo) Restore(ctx *format.RestoreCtx) error {
	name, err := GetSignalString(n.Name)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(fmt.Sprintf(" %s = ", name))

	err = n.Value.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (n *SignalInfo) Accept(v Visitor) (Node, bool) {
	expr, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = expr.(ExprNode)
	return v.Leave(n)
}

func (n *Signal) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*Signal)
	if n.ErrorCon != nil {
		node, ok := n.ErrorCon.Accept(v)
		if !ok {
			return n, false
		}
		n.ErrorCon = node.(ErrNode)
		for i, SignalCon := range n.SignalCons {
			node, ok := SignalCon.Accept(v)
			if !ok {
				return n, false
			}
			n.SignalCons[i] = node.(*SignalInfo)
		}
	}
	return v.Leave(n)
}

func GetSignalConString(i int) (string, error) {
	switch i {
	case TINUMBER:
		return "NUMBER", nil
	case TIROWCOUNT:
		return "ROW_COUNT", nil
	default:
		return "", errors.Errorf("unspport statement_information_item_name")
	}
}

type ConditionInfoItem struct {
	node
	Name       string
	IsVariable bool
	Condition  int
}

func (n *ConditionInfoItem) Restore(ctx *format.RestoreCtx) error {
	if n.IsVariable {
		ctx.WriteKeyWord("@")
	}
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" = ")
	name, err := GetSignalString(n.Condition)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(name)
	return nil
}

func (n *ConditionInfoItem) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ConditionInfoItem)
	return v.Leave(n)
}

type StatementInfoItem struct {
	node
	Name       string
	IsVariable bool
	Condition  int
}

func (n *StatementInfoItem) Restore(ctx *format.RestoreCtx) error {
	if n.IsVariable {
		ctx.WriteKeyWord("@")
	}
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" = ")
	name, err := GetSignalConString(n.Condition)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(name)
	return nil
}

func (n *StatementInfoItem) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*StatementInfoItem)
	return v.Leave(n)
}

type DiagnosticsConds struct {
	node
	Num   ExprNode
	Conds []*ConditionInfoItem
}

func (n *DiagnosticsConds) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CONDITION ")
	err := n.Num.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(" ")
	for i, cond := range n.Conds {
		err = cond.Restore(ctx)
		if err != nil {
			return err
		}
		if i != len(n.Conds)-1 {
			ctx.WriteKeyWord(", ")
		}
	}
	return nil
}

func (n *DiagnosticsConds) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DiagnosticsConds)
	node, ok := n.Num.Accept(v)
	if !ok {
		return n, false
	}
	n.Num = node.(ExprNode)
	for i, con := range n.Conds {
		node, ok := con.Accept(v)
		if !ok {
			return n, false
		}
		n.Conds[i] = node.(*ConditionInfoItem)
	}
	return v.Leave(n)
}

func GetDiagnosticsArea(i int) (string, error) {
	switch i {
	case TICURRENT:
		return "CURRENT", nil
	case TISTACKED:
		return "STACKED", nil
	default:
		return "", errors.Errorf("unspport Diagnostics Area")
	}
}

type GetDiagnosticsStmt struct {
	stmtNode
	Area   int
	Infors []DiagnosticsInformation
}

func (stmt *GetDiagnosticsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("Get ")
	name, err := GetDiagnosticsArea(stmt.Area)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(name)
	ctx.WriteKeyWord(" DIAGNOSTICS ")
	for i, info := range stmt.Infors {
		err = info.Restore(ctx)
		if err != nil {
			return err
		}
		if i != len(stmt.Infors)-1 {
			ctx.WriteKeyWord(", ")
		}
	}
	return nil
}

func (stmt *GetDiagnosticsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(stmt)
	if skipChildren {
		return v.Leave(newNode)
	}
	stmt = newNode.(*GetDiagnosticsStmt)
	for i, info := range stmt.Infors {
		node, ok := info.Accept(v)
		if !ok {
			return stmt, false
		}
		stmt.Infors[i] = node.(DiagnosticsInformation)
	}
	return v.Leave(stmt)
}
