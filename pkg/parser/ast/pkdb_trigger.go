// Copyright 2025 PingCAP, Inc.

package ast

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
)

// TriggerTiming represents the timing of trigger execution.
type TriggerTiming int8

const (
	// TriggerTimingBefore means the trigger is executed before the triggering event.
	TriggerTimingBefore TriggerTiming = iota + 1
	// TriggerTimingAfter means the trigger is executed after the triggering event.
	TriggerTimingAfter
)

// String returns the string representation of TriggerTiming.
func (t TriggerTiming) String() string {
	switch t {
	case TriggerTimingBefore:
		return "BEFORE"
	case TriggerTimingAfter:
		return "AFTER"
	default:
		return "UNKNOWN"
	}
}

type TriggerEvent int8

const (
	// TriggerEventInsert means the trigger is executed on insert event.
	TriggerEventInsert TriggerEvent = iota + 1
	// TriggerEventUpdate means the trigger is executed on update event.
	TriggerEventUpdate
	// TriggerEventDelete means the trigger is executed on delete event.
	TriggerEventDelete
)

func (e TriggerEvent) String() string {
	switch e {
	case TriggerEventInsert:
		return "INSERT"
	case TriggerEventUpdate:
		return "UPDATE"
	case TriggerEventDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// TriggerOrderType represents the order type of trigger execution.
type TriggerOrderType int8

const (
	// TriggerOrderNone means no specific order is defined for the trigger execution.
	TriggerOrderNone TriggerOrderType = iota
	// TriggerOrderFollows means the trigger is executed after the specified trigger.
	TriggerOrderFollows
	// TriggerOrderPrecedes means the trigger is executed before the specified trigger.
	TriggerOrderPrecedes
)

// String returns the string representation of TriggerOrderType.
func (t TriggerOrderType) String() string {
	switch t {
	case TriggerOrderNone:
		return "NONE"
	case TriggerOrderFollows:
		return "FOLLOWS"
	case TriggerOrderPrecedes:
		return "PRECEDES"
	default:
		return "UNKNOWN"
	}
}

// TriggerOrder represents the order of trigger execution.
type TriggerOrder struct {
	OrderType        TriggerOrderType
	OtherTriggerName model.CIStr
}

// CREATE [DEFINER = user] TRIGGER [IF NOT EXISTS] trigger_name
//     {BEFORE | AFTER} {INSERT | UPDATE | DELETE}
//     ON table_name FOR EACH ROW
//     [FOLLOWS | PRECEDES other_trigger_name]
//     trigger_body

// CreateTriggerStmt represents a CREATE TRIGGER statement.
type CreateTriggerStmt struct {
	ddlNode
	IfNotExists bool
	Definer     *auth.UserIdentity
	TriggerName *TableName
	Timing      TriggerTiming
	Event       TriggerEvent
	TableName   *TableName
	Order       TriggerOrder

	TriggerBody StmtNode
}

// Restore implements CreateTriggerStmt interface.
func (n *CreateTriggerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.Definer != nil {
		ctx.WriteKeyWord("DEFINER = ")
		if err := n.Definer.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore definer")
		}
		ctx.WriteKeyWord(" ")
	}
	ctx.WriteKeyWord("TRIGGER ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.TriggerName.Restore(ctx); err != nil {
		return err
	}
	ctx.WriteKeyWord(" ")
	switch n.Timing {
	case TriggerTimingBefore:
		ctx.WriteKeyWord("BEFORE ")
	case TriggerTimingAfter:
		ctx.WriteKeyWord("AFTER ")
	}
	switch n.Event {
	case TriggerEventInsert:
		ctx.WriteKeyWord("INSERT ")
	case TriggerEventUpdate:
		ctx.WriteKeyWord("UPDATE ")
	case TriggerEventDelete:
		ctx.WriteKeyWord("DELETE ")
	}
	ctx.WriteKeyWord("ON ")
	if err := n.TableName.Restore(ctx); err != nil {
		return err
	}
	ctx.WriteKeyWord(" FOR EACH ROW ")
	if n.Order.OrderType != TriggerOrderNone {
		switch n.Order.OrderType {
		case TriggerOrderFollows:
			ctx.WriteKeyWord("FOLLOWS ")
		case TriggerOrderPrecedes:
			ctx.WriteKeyWord("PRECEDES ")
		}
		ctx.WriteName(n.Order.OtherTriggerName.O)
		ctx.WriteKeyWord(" ")
	}
	return n.TriggerBody.Restore(ctx)
}

// Accept implements CreateTriggerStmt Accept interface.
func (n *CreateTriggerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateTriggerStmt)
	if n.TriggerName != nil {
		node, ok := n.TriggerName.Accept(v)
		if !ok {
			return n, false
		}
		n.TriggerName = node.(*TableName)
	}
	if n.TableName != nil {
		node, ok := n.TableName.Accept(v)
		if !ok {
			return n, false
		}
		n.TableName = node.(*TableName)
	}
	node, ok := n.TriggerBody.Accept(v)
	if !ok {
		return n, false
	}
	n.TriggerBody = node.(StmtNode)
	return v.Leave(n)
}

// DROP TRIGGER [IF EXISTS] trigger_name

// DropTriggerStmt represents a DROP TRIGGER statement.
type DropTriggerStmt struct {
	ddlNode

	IfExists    bool
	TriggerName *TableName
}

// Restore implements DropTriggerStmt interface.
func (n *DropTriggerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP TRIGGER ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	err := n.TriggerName.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements DropTriggerStmt Accept interface.
func (n *DropTriggerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropTriggerStmt)
	if n.TriggerName != nil {
		node, ok := n.TriggerName.Accept(v)
		if !ok {
			return n, false
		}
		n.TriggerName = node.(*TableName)
	}
	return v.Leave(n)
}
