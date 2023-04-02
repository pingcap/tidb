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
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/types"
)

var (
	_ Node = &StoreParameter{}
	_ Node = &ProcedureDecl{}

	_ StmtNode = &ProcedureBlock{}
	_ StmtNode = &ProcedureProc{}
	_ StmtNode = &ProcedureInfo{}
	_ StmtNode = &DropProcedureStmt{}
	_ StmtNode = &ProcedureElseIfBlock{}
	_ StmtNode = &ProcedureElseBlock{}
	_ StmtNode = &ProcedureIfBlock{}
	_ StmtNode = &SimpleWhenCaseStmt{}
	_ StmtNode = &ProcedureIfInfo{}

	_ DeclNode = &ProcedureErrorControl{}
	_ DeclNode = &ProcedureCursor{}
	_ DeclNode = &ProcedureDecl{}

	_ ErrNode = &ProcedureErrorCon{}
	_ ErrNode = &ProcedureErrorVal{}
)

// param info.
const (
	MODE_IN = iota
	MODE_OUT
	MODE_INOUT
)

const (
	PROCEDUR_CONTINUE = iota
	PROCEDUR_EXIT
)

const (
	PROCEDUR_SQLWARNING = iota
	PROCEDUR_NOT_FOUND
	PROCEDUR_SQLEXCEPTION
	PROCEDUR_END
)

type DeclNode interface {
	Node
}

type ErrNode interface {
	StmtNode
}

type ProcedureDeclInfo struct {
	node
}

type ProcedureErrorList struct {
	stmtNode
}

// StoreParameter Stored procedure entry and exit parameters.
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

// ProcedureDecl Stored procedure declares internal variables.
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

// ProcedureProc stored procedure subobject.
type ProcedureProc struct {
	stmtNode
	ProcNodes Node
}

// Restore implements Node interface.
func (n *ProcedureProc) Restore(ctx *format.RestoreCtx) error {
	err := n.ProcNodes.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WritePlain(";")
	return nil
}

// Accept implements ProcedureProc Accept interface.
func (n *ProcedureProc) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureProc)
	if n.ProcNodes != nil {
		_, ok := (n.ProcNodes).Accept(v)
		if !ok {
			return n, false
		}
	}
	return v.Leave(n)
}

// ProcedureBlock stored procedure block.
type ProcedureBlock struct {
	stmtNode
	ProcedureVars      []DeclNode
	ProcedureProcStmts []StmtNode
}

// Restore implements ProcedureBlock interface.
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
	ctx.WriteKeyWord(" END;")
	return nil
}

// Accept implements ProcedureBlock Accept interface.
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
	// Store Procedure do not check sql justifiability, so don't traverse 	ProcedureProcStmts.
	return v.Leave(n)
}

// ProcedureInfo stored procedure object
type ProcedureInfo struct {
	stmtNode
	IfNotExists       bool
	ProcedureName     *TableName
	ProcedureParam    []*StoreParameter
	ProcedureBody     StmtNode
	ProcedureParamStr string
}

// Restore implements ProcedureInfo interface.
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

// Accept implements ProcedureInfo Accept interface.
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

// DropProcedureStmt
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

// Accept implements DropProcedureStmt Accept interface.
func (n *DropProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropProcedureStmt)
	return v.Leave(n)
}

// ProcedureIfInfo  Stored procedure if structure.
type ProcedureIfInfo struct {
	stmtNode
	IfBody *ProcedureIfBlock
}

// Restore implements ProcedureIfInfo interface.
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

// ProcedureElseIfBlock  Stored procedure elseif structure.
type ProcedureElseIfBlock struct {
	stmtNode
	ProcedureIfStmt *ProcedureIfBlock
}

// Restore implements ProcedureIfBlock interface.
func (n *ProcedureElseIfBlock) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ELSEIF ")
	err := n.ProcedureIfStmt.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements ProcedureIfBlock Accept interface.
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

// ProcedureElseBlock Stored procedure else structure.
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

// ProcedureIfBlock Stored procedure if else/ if elseif structure.
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

// SimpleWhenCaseStmt
type SimpleWhenCaseStmt struct {
	stmtNode

	Expr           ExprNode
	ProcedureStmts []StmtNode
}

// Restore implements SimpleWhenCaseStmt interface.
func (n *SimpleWhenCaseStmt) Restore(ctx *format.RestoreCtx) error {
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

// Accept implements SimpleWhenCaseStmt Accept interface.
func (n *SimpleWhenCaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SimpleWhenCaseStmt)
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

// SimpleCaseStmt
type SimpleCaseStmt struct {
	stmtNode

	Condition ExprNode
	WhenCases []*SimpleWhenCaseStmt
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
		n.WhenCases[i] = node.(*SimpleWhenCaseStmt)
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

// SearchCaseStmt
type SearchCaseStmt struct {
	stmtNode

	WhenCases []*SimpleWhenCaseStmt
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
		n.WhenCases[i] = node.(*SimpleWhenCaseStmt)
	}
	// Store Procedure do not check sql justifiability, so don't traverse ElseCases.
	return v.Leave(n)
}

// ProcedureWhileStmt
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

// ProcedureCursor stored procedure cursor info.
type ProcedureCursor struct {
	ProcedureDeclInfo

	CurName      string
	Selectstring StmtNode
}

// Restore implements Node interface.
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

// Accept implements ProcedureProc Accept interface.
func (n *ProcedureCursor) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureCursor)
	return v.Leave(n)
}

// ProcedureErrorControl stored procedure error control info.
type ProcedureErrorControl struct {
	ProcedureDeclInfo

	ControlHandle int
	ErrorCon      []ErrNode
	Operate       StmtNode
}

// Restore implements Node interface.
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

// Accept implements ProcedureProc Accept interface.
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

// ProcedureOpenCur
type ProcedureOpenCur struct {
	stmtNode

	CurName string
}

// Restore implements Node interface.
func (n *ProcedureOpenCur) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("OPEN ")
	ctx.WriteKeyWord(n.CurName)
	return nil
}

// Accept implements ProcedureProc Accept interface.
func (n *ProcedureOpenCur) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureOpenCur)
	return v.Leave(n)
}

// ProcedureCloseCur
type ProcedureCloseCur struct {
	stmtNode

	CurName string
}

// Restore implements Node interface.
func (n *ProcedureCloseCur) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CLOSE ")
	ctx.WriteKeyWord(n.CurName)
	return nil
}

// Accept implements ProcedureProc Accept interface.
func (n *ProcedureCloseCur) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureCloseCur)
	return v.Leave(n)
}

// ProcedureCloseCur
type ProcedureFetchInto struct {
	stmtNode

	CurName   string
	Variables []string
}

// Restore implements Node interface.
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

// Accept implements ProcedureProc Accept interface.
func (n *ProcedureFetchInto) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureFetchInto)
	return v.Leave(n)
}

// ProcedureErrorVal Error control.
type ProcedureErrorVal struct {
	ProcedureErrorList

	ErrorNum uint64
}

// Restore implements Node interface.
func (n *ProcedureErrorVal) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain(strconv.FormatUint(n.ErrorNum, 10))
	return nil
}

// Accept implements ProcedureProc Accept interface.
func (n *ProcedureErrorVal) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureErrorVal)
	return v.Leave(n)
}

// ProcedureErrorState Error control.
type ProcedureErrorState struct {
	ProcedureErrorList

	CodeStatus string
}

// Restore implements Node interface.
func (n *ProcedureErrorState) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SQLSTATE ")
	ctx.WriteString(n.CodeStatus)
	return nil
}

// Accept implements ProcedureProc Accept interface.
func (n *ProcedureErrorState) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureErrorState)
	return v.Leave(n)
}

// ProcedureErrorCon Error control.
type ProcedureErrorCon struct {
	ProcedureErrorList

	ErrorCon int
}

// Restore implements Node interface.
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
