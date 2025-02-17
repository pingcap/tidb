// Copyright 2023 PingCAP, Inc.
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

package ast

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/types"
)

var (
	_ Node = &StoreParameter{}
	_ Node = &ProcedureDecl{}

	_ StmtNode = &ProcedureBlock{}
	_ StmtNode = &ProcedureInfo{}
	_ StmtNode = &DropProcedureStmt{}
	_ StmtNode = &ProcedureElseIfBlock{}
	_ StmtNode = &ProcedureElseBlock{}
	_ StmtNode = &ProcedureIfBlock{}
	_ StmtNode = &SimpleWhenThenStmt{}
	_ StmtNode = &ProcedureIfInfo{}
	_ StmtNode = &ProcedureLabelBlock{}
	_ StmtNode = &ProcedureLabelLoop{}
	_ StmtNode = &ProcedureJump{}

	_ DeclNode = &ProcedureErrorControl{}
	_ DeclNode = &ProcedureCursor{}
	_ DeclNode = &ProcedureDecl{}

	_ LabelInfo = &ProcedureLabelBlock{}
	_ LabelInfo = &ProcedureLabelLoop{}

	_ ErrNode = &ProcedureErrorCon{}
	_ ErrNode = &ProcedureErrorVal{}
)

// procedure param type.
const (
	MODE_IN = iota
	MODE_OUT
	MODE_INOUT
)

// procedure handler operation type.
const (
	PROCEDUR_CONTINUE = iota
	PROCEDUR_EXIT
)

// procedure handler value string.
const (
	PROCEDUR_SQLWARNING = iota
	PROCEDUR_NOT_FOUND
	PROCEDUR_SQLEXCEPTION
	PROCEDUR_END
)

// DeclNode expresses procedure block variable interface(include handler\cursor\sp variable)
type DeclNode interface {
	Node
}

// ErrNode expresses all types of handler condition value.
type ErrNode interface {
	StmtNode
}

// ProcedureDeclInfo is the base node of a procedure variable.
type ProcedureDeclInfo struct {
	node
}

// ProcedureErrorCondition is the base node of a condition value.
type ProcedureErrorCondition struct {
	stmtNode
}

// LabelInfo is the interface of loop and block label.
type LabelInfo interface {
	// GetErrorStatus gets label status, if error, return end label name and true.
	// if normal，The returned string has no meaning and false.
	GetErrorStatus() (string, bool)
	// GetLabelName gets label name.
	GetLabelName() string
	// IsBlock gets type flag, true is block, false is loop.
	IsBlock() bool
	// GetBlock gets block stmtnode
	GetBlock() StmtNode
}

// StoreParameter is the parameter of stored procedure.
type StoreParameter struct {
	node
	Paramstatus int
	ParamType   *types.FieldType
	ParamName   string
}

// Restore implements Node interface.
func (n *StoreParameter) Restore(ctx *format.RestoreCtx) error {
	switch n.Paramstatus {
	case MODE_IN:
		ctx.WriteKeyWord(" IN ")
	case MODE_OUT:
		ctx.WriteKeyWord(" OUT ")
	case MODE_INOUT:
		ctx.WriteKeyWord(" INOUT ")
	}

	ctx.WriteName(n.ParamName)
	ctx.WritePlain(" ")
	ctx.WriteKeyWord(n.ParamType.CompactStr())
	return nil
}

// Accept implements Node Accept interface.
func (n *StoreParameter) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*StoreParameter)
	return v.Leave(n)
}

// ProcedureDecl represents the internal variables of stored procedure .
type ProcedureDecl struct {
	ProcedureDeclInfo
	DeclNames   []string
	DeclType    *types.FieldType
	DeclDefault ExprNode
}

// Restore implements Node interface.
func (n *ProcedureDecl) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	for i, name := range n.DeclNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WriteName(name)
	}
	ctx.WritePlain(" ")
	ctx.WriteKeyWord(n.DeclType.CompactStr())
	if n.DeclDefault != nil {
		ctx.WriteKeyWord(" DEFAULT ")
		if err := n.DeclDefault.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occur while restore expr")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ProcedureDecl) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureDecl)
	if n.DeclDefault != nil {
		node, ok := n.DeclDefault.Accept(v)
		if !ok {
			return n, false
		}
		n.DeclDefault = node.(ExprNode)
	}
	return v.Leave(n)
}

// ProcedureBlock represents a procedure block.
type ProcedureBlock struct {
	stmtNode
	ProcedureVars      []DeclNode // include handler && cursor && variable
	ProcedureProcStmts []StmtNode // procedure statement
}

// Restore implements Node interface.
func (n *ProcedureBlock) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("BEGIN ")
	for _, ProcedureVar := range n.ProcedureVars {
		err := ProcedureVar.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WritePlain(";")
	}

	for _, ProcedureProcStmt := range n.ProcedureProcStmts {
		err := ProcedureProcStmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WritePlain(";")
	}
	ctx.WriteKeyWord(" END")
	return nil
}

// Accept implements Node interface.
func (n *ProcedureBlock) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureBlock)
	for i, ProcedureVar := range n.ProcedureVars {
		node, ok := ProcedureVar.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureVars[i] = node.(DeclNode)
	}
	// Store Procedure doesn't check the justifiability for statements, so don't traverse ProcedureProcStmts.
	return v.Leave(n)
}

// ProcedureInfo stores all procedure information.
type ProcedureInfo struct {
	stmtNode
	IfNotExists       bool
	ProcedureName     *TableName
	ProcedureParam    []*StoreParameter //procedure param
	ProcedureBody     StmtNode          //procedure body statement
	ProcedureParamStr string            //procedure parameter string
}

// Restore implements Node interface.
func (n *ProcedureInfo) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE PROCEDURE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	err := n.ProcedureName.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WritePlain("(")
	for i, ProcedureParam := range n.ProcedureParam {
		if i > 0 {
			ctx.WritePlain(",")
		}
		err := ProcedureParam.Restore(ctx)
		if err != nil {
			return err
		}
	}
	ctx.WritePlain(") ")
	err = (n.ProcedureBody).Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ProcedureInfo) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureInfo)
	for i, ProcedureParam := range n.ProcedureParam {
		node, ok := ProcedureParam.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureParam[i] = node.(*StoreParameter)
	}
	node, ok := n.ProcedureBody.Accept(v)
	if !ok {
		return n, false
	}
	n.ProcedureBody = node.(StmtNode)
	return v.Leave(n)
}

// DropProcedureStmt represents the ast of `drop procedure`
type DropProcedureStmt struct {
	stmtNode

	IfExists      bool
	ProcedureName *TableName
}

// Restore implements DropProcedureStmt interface.
func (n *DropProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP PROCEDURE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	err := n.ProcedureName.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements Node interface.
func (n *DropProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropProcedureStmt)
	return v.Leave(n)
}

// ProcedureIfInfo stores the `if statement` of procedure.
type ProcedureIfInfo struct {
	stmtNode
	IfBody *ProcedureIfBlock
}

// Restore implements Node interface.
func (n *ProcedureIfInfo) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("IF ")
	err := n.IfBody.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord("END IF")
	return nil
}

// Accept implements ProcedureIfInfo Accept interface.
func (n *ProcedureIfInfo) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureIfInfo)
	node, ok := n.IfBody.Accept(v)
	if !ok {
		return n, false
	}
	n.IfBody = node.(*ProcedureIfBlock)
	return v.Leave(n)
}

// ProcedureElseIfBlock stores the `elseif` statement info of procedure.
type ProcedureElseIfBlock struct {
	stmtNode
	ProcedureIfStmt *ProcedureIfBlock
}

// Restore implements Node interface.
func (n *ProcedureElseIfBlock) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ELSEIF ")
	err := n.ProcedureIfStmt.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements ProcedureElseIfBlock Accept interface.
func (n *ProcedureElseIfBlock) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureElseIfBlock)
	node, ok := n.ProcedureIfStmt.Accept(v)
	if !ok {
		return n, false
	}
	n.ProcedureIfStmt = node.(*ProcedureIfBlock)
	return v.Leave(n)
}

// ProcedureElseBlock stores procedure `else` statement info.
type ProcedureElseBlock struct {
	stmtNode
	ProcedureIfStmts []StmtNode
}

// Restore implements ProcedureElseBlock interface.
func (n *ProcedureElseBlock) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ELSE ")
	for _, stmt := range n.ProcedureIfStmts {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord(";")
	}
	return nil
}

// Accept implements ProcedureElseBlock Accept interface.
func (n *ProcedureElseBlock) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureElseBlock)
	return v.Leave(n)
}

// ProcedureIfBlock stores `expr ... else if ... else ...` statement in procedure.
type ProcedureIfBlock struct {
	stmtNode
	IfExpr            ExprNode
	ProcedureIfStmts  []StmtNode
	ProcedureElseStmt StmtNode
}

// Restore implements ProcedureIfBlock interface.
func (n *ProcedureIfBlock) Restore(ctx *format.RestoreCtx) error {
	err := n.IfExpr.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(" THEN ")
	for _, stmt := range n.ProcedureIfStmts {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord(";")
	}
	if n.ProcedureElseStmt != nil {
		err = n.ProcedureElseStmt.Restore(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Accept implements ProcedureIfBlock Accept interface.
func (n *ProcedureIfBlock) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureIfBlock)
	if n.IfExpr != nil {
		node, ok := n.IfExpr.Accept(v)
		if !ok {
			return n, false
		}
		n.IfExpr = node.(ExprNode)
	}

	if n.ProcedureElseStmt != nil {
		node, ok := n.ProcedureElseStmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureElseStmt = node.(StmtNode)
	}
	return v.Leave(n)
}

// SimpleWhenThenStmt stores `case expr then ...` statement.
type SimpleWhenThenStmt struct {
	stmtNode

	Expr           ExprNode
	ProcedureStmts []StmtNode
}

// Restore implements SimpleWhenThenStmt interface.
func (n *SimpleWhenThenStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("WHEN ")
	err := n.Expr.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(" THEN ")
	for _, stmt := range n.ProcedureStmts {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord(";")
	}
	return nil
}

// Accept implements SimpleWhenThenStmt Accept interface.
func (n *SimpleWhenThenStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SimpleWhenThenStmt)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	// Store Procedure do not check sql justifiability, so don't traverse ProcedureStmts.
	return v.Leave(n)
}

// SimpleCaseStmt store WhenCases SimpleWhenThenStmt `case expr SimpleWhenThenStmt else ...` statement.
type SimpleCaseStmt struct {
	stmtNode

	Condition ExprNode
	WhenCases []*SimpleWhenThenStmt
	ElseCases []StmtNode
}

// Restore implements SimpleCaseStmt interface.
func (n *SimpleCaseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CASE ")
	err := n.Condition.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(" ")
	for _, stmt := range n.WhenCases {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
	}

	if n.ElseCases != nil {
		ctx.WriteKeyWord(" ELSE ")
		for _, stmt := range n.ElseCases {
			err := stmt.Restore(ctx)
			if err != nil {
				return err
			}
			ctx.WriteKeyWord(";")
		}
	}
	ctx.WriteKeyWord(" END CASE")
	return nil
}

// Accept implements SimpleCaseStmt Accept interface.
func (n *SimpleCaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SimpleCaseStmt)
	node, ok := n.Condition.Accept(v)
	if !ok {
		return n, false
	}
	n.Condition = node.(ExprNode)

	for i, stmt := range n.WhenCases {
		node, ok := stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.WhenCases[i] = node.(*SimpleWhenThenStmt)
	}

	if n.ElseCases != nil {
		for i, stmt := range n.ElseCases {
			node, ok := stmt.Accept(v)
			if !ok {
				return n, false
			}
			n.ElseCases[i] = node.(StmtNode)
		}
	}
	return v.Leave(n)
}

// SearchWhenThenStmt stores SearchCaseStmt whencase `case expr then ...` statement.
type SearchWhenThenStmt struct {
	stmtNode

	Expr           ExprNode
	ProcedureStmts []StmtNode
}

// Restore implements SearchWhenThenStmt interface.
func (n *SearchWhenThenStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("WHEN ")
	err := n.Expr.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(" THEN ")
	for _, stmt := range n.ProcedureStmts {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord(";")
	}
	return nil
}

// Accept implements SearchWhenThenStmt Accept interface.
func (n *SearchWhenThenStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SearchWhenThenStmt)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	// Store Procedure do not check sql justifiability, so don't traverse ProcedureStmts.
	return v.Leave(n)
}

// SearchCaseStmt store `case SimpleWhenThenStmt else ...` statement.
type SearchCaseStmt struct {
	stmtNode

	WhenCases []*SearchWhenThenStmt
	ElseCases []StmtNode
}

// Restore implements SearchCaseStmt interface.
func (n *SearchCaseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CASE ")

	for _, stmt := range n.WhenCases {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
	}

	if n.ElseCases != nil {
		ctx.WriteKeyWord(" ELSE ")
		for _, stmt := range n.ElseCases {
			err := stmt.Restore(ctx)
			if err != nil {
				return err
			}
			ctx.WriteKeyWord(";")
		}
	}
	ctx.WriteKeyWord(" END CASE")
	return nil
}

// Accept implements SimpleCaseStmt Accept interface.
func (n *SearchCaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SearchCaseStmt)

	for i, stmt := range n.WhenCases {
		node, ok := stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.WhenCases[i] = node.(*SearchWhenThenStmt)
	}
	// Store Procedure do not check sql justifiability, so don't traverse ElseCases.
	return v.Leave(n)
}

// ProcedureRepeatStmt store `repeat ... until expr end repeat` statement.
type ProcedureRepeatStmt struct {
	stmtNode

	Body      []StmtNode
	Condition ExprNode
}

// Restore implements ProcedureRepeatStmt interface.
func (n *ProcedureRepeatStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REPEAT ")
	for _, stmt := range n.Body {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord(";")
	}
	ctx.WriteKeyWord("UNTIL ")
	err := n.Condition.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(" END REPEAT")
	return nil
}

// Accept implements ProcedureRepeatStmt Accept interface.
func (n *ProcedureRepeatStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureRepeatStmt)

	for i, stmt := range n.Body {
		node, ok := stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.Body[i] = node.(StmtNode)
	}

	node, ok := n.Condition.Accept(v)
	if !ok {
		return n, false
	}
	n.Condition = node.(ExprNode)

	return v.Leave(n)
}

// ProcedureWhileStmt stores `while expr do ... end while` statement.
type ProcedureWhileStmt struct {
	stmtNode

	Condition ExprNode
	Body      []StmtNode
}

// Restore implements ProcedureWhileStmt interface.
func (n *ProcedureWhileStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("WHILE ")
	err := n.Condition.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteKeyWord(" DO ")
	for _, stmt := range n.Body {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord(";")
	}
	ctx.WriteKeyWord("END WHILE")
	return nil
}

// Accept implements ProcedureWhileStmt Accept interface.
func (n *ProcedureWhileStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureWhileStmt)

	node, ok := n.Condition.Accept(v)
	if !ok {
		return n, false
	}
	n.Condition = node.(ExprNode)

	for i, stmt := range n.Body {
		node, ok := stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.Body[i] = node.(StmtNode)
	}
	return v.Leave(n)
}

// ProcedureCursor stores procedure cursor statement.
type ProcedureCursor struct {
	ProcedureDeclInfo

	CurName      string
	Selectstring StmtNode
}

// Restore implements ProcedureCursor interface.
func (n *ProcedureCursor) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	ctx.WriteKeyWord(n.CurName)
	ctx.WriteKeyWord(" CURSOR FOR ")
	err := n.Selectstring.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements ProcedureCursor Accept interface.
func (n *ProcedureCursor) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureCursor)
	return v.Leave(n)
}

// ProcedureErrorControl stored procedure handler statement.
type ProcedureErrorControl struct {
	ProcedureDeclInfo

	ControlHandle int       // handler operation (exit\continue).
	ErrorCon      []ErrNode //handler condition value.
	Operate       StmtNode  // handler block.
}

// Restore implements ProcedureErrorControl interface.
func (n *ProcedureErrorControl) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	switch n.ControlHandle {
	case PROCEDUR_CONTINUE:
		ctx.WriteKeyWord("CONTINUE ")
	case PROCEDUR_EXIT:
		ctx.WriteKeyWord("EXIT ")
	}
	ctx.WriteKeyWord("HANDLER FOR ")
	for i, errorInfo := range n.ErrorCon {
		err := errorInfo.Restore(ctx)
		if err != nil {
			return err
		}
		if i+1 != len(n.ErrorCon) {
			ctx.WritePlain(", ")
		}
	}
	ctx.WritePlain(" ")
	err := n.Operate.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements ProcedureErrorControl Accept interface.
func (n *ProcedureErrorControl) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureErrorControl)
	for i, errorInfo := range n.ErrorCon {
		node, ok := errorInfo.Accept(v)
		if !ok {
			return n, false
		}
		n.ErrorCon[i] = node.(ErrNode)
	}
	return v.Leave(n)
}

// ProcedureOpenCur store open cursor statement.
type ProcedureOpenCur struct {
	stmtNode

	CurName string
}

// Restore implements ProcedureOpenCur interface.
func (n *ProcedureOpenCur) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("OPEN ")
	ctx.WriteKeyWord(n.CurName)
	return nil
}

// Accept implements ProcedureOpenCur Accept interface.
func (n *ProcedureOpenCur) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureOpenCur)
	return v.Leave(n)
}

// ProcedureCloseCur store close cursor statement.
type ProcedureCloseCur struct {
	stmtNode

	CurName string
}

// Restore implements ProcedureCloseCur interface.
func (n *ProcedureCloseCur) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CLOSE ")
	ctx.WriteKeyWord(n.CurName)
	return nil
}

// Accept implements ProcedureCloseCur Accept interface.
func (n *ProcedureCloseCur) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureCloseCur)
	return v.Leave(n)
}

// ProcedureFetchInto store cursor read data command.
type ProcedureFetchInto struct {
	stmtNode

	CurName   string
	Variables []string
}

// Restore implements ProcedureFetchInto interface.
func (n *ProcedureFetchInto) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("FETCH ")
	ctx.WriteKeyWord(n.CurName)
	ctx.WriteKeyWord(" INTO ")
	for i, varName := range n.Variables {
		ctx.WriteKeyWord(varName)
		if i+1 < len(n.Variables) {
			ctx.WriteKeyWord(", ")
		}
	}
	return nil
}

// Accept implements ProcedureFetchInto Accept interface.
func (n *ProcedureFetchInto) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureFetchInto)
	return v.Leave(n)
}

// ProcedureErrorVal store procedure handler error code.
type ProcedureErrorVal struct {
	ProcedureErrorCondition

	ErrorNum uint64
}

// Restore implements ProcedureErrorVal interface.
func (n *ProcedureErrorVal) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain(strconv.FormatUint(n.ErrorNum, 10))
	return nil
}

// Accept implements ProcedureErrorVal Accept interface.
func (n *ProcedureErrorVal) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureErrorVal)
	return v.Leave(n)
}

// ProcedureErrorState store procedure handler SQLSTATE string.
type ProcedureErrorState struct {
	ProcedureErrorCondition

	CodeStatus string
}

// Restore implements ProcedureErrorState interface.
func (n *ProcedureErrorState) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SQLSTATE ")
	ctx.WriteString(n.CodeStatus)
	return nil
}

// Accept implements ProcedureErrorState Accept interface.
func (n *ProcedureErrorState) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureErrorState)
	return v.Leave(n)
}

// ProcedureErrorCon stores procedure handler status info.
type ProcedureErrorCon struct {
	ProcedureErrorCondition

	ErrorCon int
}

// Restore implements ProcedureErrorCon interface.
func (n *ProcedureErrorCon) Restore(ctx *format.RestoreCtx) error {
	switch n.ErrorCon {
	case PROCEDUR_SQLWARNING:
		ctx.WriteKeyWord("SQLWARNING")
	case PROCEDUR_NOT_FOUND:
		ctx.WriteKeyWord("NOT FOUND")
	case PROCEDUR_SQLEXCEPTION:
		ctx.WriteKeyWord("SQLEXCEPTION")
	}
	return nil
}

// Accept implements ProcedureErrorCon Accept interface.
func (n *ProcedureErrorCon) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureErrorCon)
	return v.Leave(n)
}

// ProcedureLabelBlock stored procedure block label statement.
type ProcedureLabelBlock struct {
	stmtNode
	LabelName  string
	Block      *ProcedureBlock
	LabelError bool
	LabelEnd   string
}

// Restore implements ProcedureLabelBlock interface.
func (n *ProcedureLabelBlock) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteName(n.LabelName)
	ctx.WriteKeyWord(": ")
	err := n.Block.Restore(ctx)
	if err != nil {
		return err
	}
	if n.LabelError {
		return errors.Errorf("the same label has different names,begin: %s,end: %s", n.LabelName, n.LabelEnd)
	}
	ctx.WriteKeyWord(" ")
	ctx.WriteName(n.LabelName)
	return nil
}

// Accept implements ProcedureLabelBlock Accept interface.
func (n *ProcedureLabelBlock) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureLabelBlock)

	node, ok := n.Block.Accept(v)
	if !ok {
		return n, false
	}
	n.Block = node.(*ProcedureBlock)
	// Store Procedure do not check sql justifiability, so don't traverse 	ProcedureProcStmts.
	return v.Leave(n)
}

// GetErrorStatus gets label error info.
func (n *ProcedureLabelBlock) GetErrorStatus() (string, bool) {
	return n.LabelEnd, n.LabelError
}

// GetLabelName gets label name.
func (n *ProcedureLabelBlock) GetLabelName() string {
	return n.LabelName
}

// IsBlock gets block flag.
func (n *ProcedureLabelBlock) IsBlock() bool {
	return true
}

// GetBlock gets the block stmtnode
func (n *ProcedureLabelBlock) GetBlock() StmtNode {
	return n.Block
}

// ProcedureLabelLoop stores  the labeled loop block info in procedure.
type ProcedureLabelLoop struct {
	stmtNode
	LabelName  string
	Block      StmtNode
	LabelError bool
	LabelEnd   string
}

// Restore implements ProcedureLabelLoop interface.
func (n *ProcedureLabelLoop) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteName(n.LabelName)
	ctx.WriteKeyWord(": ")
	err := n.Block.Restore(ctx)
	if err != nil {
		return err
	}
	if n.LabelError {
		return errors.Errorf("the same label has different names,begin: %s,end: %s", n.LabelName, n.LabelEnd)
	}
	ctx.WriteKeyWord(" ")
	ctx.WriteName(n.LabelName)
	return nil
}

// Accept implements ProcedureLabelBlock Accept interface.
func (n *ProcedureLabelLoop) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureLabelLoop)

	node, ok := n.Block.Accept(v)
	if !ok {
		return n, false
	}
	n.Block = node.(StmtNode)
	// Store Procedure do not check sql justifiability, so don't traverse 	ProcedureProcStmts.
	return v.Leave(n)
}

// GetErrorStatus get label error info.
func (n *ProcedureLabelLoop) GetErrorStatus() (string, bool) {
	return n.LabelEnd, n.LabelError
}

// GetLabelName get label name.
func (n *ProcedureLabelLoop) GetLabelName() string {
	return n.LabelName
}

// IsBlock get block flag.
func (n *ProcedureLabelLoop) IsBlock() bool {
	return false
}

// GetBlock get label stmtnode
func (n *ProcedureLabelLoop) GetBlock() StmtNode {
	return n.Block
}

// ProcedureJump stores the Jump statements(leave and iterate) in procedure.
type ProcedureJump struct {
	stmtNode
	Name    string
	IsLeave bool
}

// Restore implements ProcedureJump interface.
func (n *ProcedureJump) Restore(ctx *format.RestoreCtx) error {
	if n.IsLeave {
		ctx.WriteKeyWord("LEAVE ")
	} else {
		ctx.WriteKeyWord("ITERATE ")
	}

	ctx.WriteString(n.Name)
	return nil
}

// Accept implements ProcedureJump Accept interface.
func (n *ProcedureJump) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureJump)
	return v.Leave(n)
}
