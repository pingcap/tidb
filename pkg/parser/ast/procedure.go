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
// Copyright 2023-2024 PingCAP, Inc.

package ast

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

var (
	_ Node = &StoreParameter{}
	_ Node = &ProcedureDecl{}
	_ Node = &ProcedureVar{}

	_ ExprNode = &ProcedureVar{}

	_ StmtNode = &ProcedureBlock{}
	_ DDLNode  = &CreateProcedureInfo{}
	_ DDLNode  = &DropProcedureStmt{}
	_ DDLNode  = &AlterProcedureStmt{}
	_ StmtNode = &ProcedureElseIfBlock{}
	_ StmtNode = &ProcedureElseBlock{}
	_ StmtNode = &ProcedureIfBlock{}
	_ StmtNode = &SimpleWhenThenStmt{}
	_ StmtNode = &SearchWhenThenStmt{}
	_ StmtNode = &ProcedureIfInfo{}
	_ StmtNode = &ProcedureLabelBlock{}
	_ StmtNode = &ProcedureLabelLoop{}
	_ StmtNode = &ProcedureJump{}
	_ StmtNode = &ProcedureLoopStmt{}
	_ StmtNode = &ProcedureRepeatStmt{}
	_ StmtNode = &ProcedureWhileStmt{}

	_ DeclNode = &ProcedureErrorControl{}
	_ DeclNode = &ProcedureCursor{}
	_ DeclNode = &ProcedureDecl{}

	_ LabelInfo = &ProcedureLabelBlock{}
	_ LabelInfo = &ProcedureLabelLoop{}

	_ ErrNode = &ProcedureErrorCon{}
	_ ErrNode = &ProcedureErrorVal{}

	_ ProcedureCharacteristic = &ProcedureComment{}
	_ ProcedureCharacteristic = &ProcedureSecurity{}
	_ ProcedureCharacteristic = &ProcedureDeterministic{}
	_ ProcedureCharacteristic = &ProcedureSQLDataAccess{}
	_ ErrNode                 = &ProcedureErrorState{}
)

// param info.
const (
	ModeIn = iota
	ModeOut
	ModeInOut
)

const (
	ProcedureContinue = iota
	ProcedureExit
)

const (
	ProcedureSQLWarning = iota
	ProcedureNotFound
	ProcedureSQLException
	ProcedureEnd
)

const (
	ProcedureCommentType = iota
	ProcedureSecurityType
	ProcedureDeterministicType
	ProcedureSQLDataAccessType
)

// RoutineSQLDataAccess represents the SQL data access characteristic of a stored routine.
type RoutineSQLDataAccess string

const (
	RoutineContainsSQL     RoutineSQLDataAccess = "CONTAINS SQL"
	RoutineNoSQL           RoutineSQLDataAccess = "NO SQL"
	RoutineReadsSQLData    RoutineSQLDataAccess = "READS SQL DATA"
	RoutineModifiesSQLData RoutineSQLDataAccess = "MODIFIES SQL DATA"
)

type DeclNode interface {
	Node
}

type ErrNode interface {
	StmtNode
}

type LabelInfo interface {
	GetErrorStatus() (string, bool)
	GetLabelName() string
}

type ProcedureCharacteristic interface {
	Node
}

// ProcedureComment represents stored procedure annotations.
type ProcedureComment struct {
	node
	Type    int
	Comment string
}

// Restore implements Node interface.
func (procedure *ProcedureComment) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("COMMENT ")
	ctx.WriteString(procedure.Comment)
	return nil
}

// Accept implements Node Accept interface.
func (procedure *ProcedureComment) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(procedure)
	if skipChildren {
		return v.Leave(newNode)
	}
	procedure = newNode.(*ProcedureComment)
	return v.Leave(procedure)
}

// ProcedureSecurity represents stored procedure annotations.
type ProcedureSecurity struct {
	node
	Type     int
	Security model.ViewSecurity
}

// Restore implements Node interface.
func (procedure *ProcedureSecurity) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SQL SECURITY ")
	ctx.WriteKeyWord(procedure.Security.String())
	return nil
}

// Accept implements Node Accept interface.
func (procedure *ProcedureSecurity) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(procedure)
	if skipChildren {
		return v.Leave(newNode)
	}
	procedure = newNode.(*ProcedureSecurity)
	return v.Leave(procedure)
}

// ProcedureDeterministic represents the DETERMINISTIC / NOT DETERMINISTIC characteristic.
type ProcedureDeterministic struct {
	node
	Type          int
	Deterministic bool
}

// Restore implements Node interface.
func (procedure *ProcedureDeterministic) Restore(ctx *format.RestoreCtx) error {
	if procedure.Deterministic {
		ctx.WriteKeyWord("DETERMINISTIC")
		return nil
	}
	ctx.WriteKeyWord("NOT DETERMINISTIC")
	return nil
}

// Accept implements Node Accept interface.
func (procedure *ProcedureDeterministic) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(procedure)
	if skipChildren {
		return v.Leave(newNode)
	}
	procedure = newNode.(*ProcedureDeterministic)
	return v.Leave(procedure)
}

// ProcedureSQLDataAccess represents the SQL data access characteristic.
type ProcedureSQLDataAccess struct {
	node
	Type          int
	SQLDataAccess RoutineSQLDataAccess
}

// Restore implements Node interface.
func (procedure *ProcedureSQLDataAccess) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(string(procedure.SQLDataAccess))
	return nil
}

// Accept implements Node Accept interface.
func (procedure *ProcedureSQLDataAccess) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(procedure)
	if skipChildren {
		return v.Leave(newNode)
	}
	procedure = newNode.(*ProcedureSQLDataAccess)
	return v.Leave(procedure)
}

type ProcedureDeclInfo struct {
	node
}

type ProcedureErrorCondition struct {
	stmtNode
}

// StoreParameter Stored procedure entry and exit parameters.
type StoreParameter struct {
	node
	Paramstatus     int
	ParamType       *types.FieldType
	ParamName       string
	OmitParamStatus bool
}

// RestoreRoutineFieldType restores a stored routine field type in a form that can round-trip
// through SHOW CREATE / mysql.routines without reintroducing implicit widths or collations.
func RestoreRoutineFieldType(ctx *format.RestoreCtx, origin *types.FieldType) error {
	ft := origin.Clone()
	switch ft.GetType() {
	case mysql.TypeFloat, mysql.TypeDouble:
		// (p) syntax for float/double is not supported.
		// When decimal is unspecified, it must be default length (see defaultLengthAndDecimal).
		defaultFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
		if ft.GetDecimal() == types.UnspecifiedLength && ft.GetFlen() == defaultFlen {
			ft.SetFlen(types.UnspecifiedLength)
			ft.SetDecimal(types.UnspecifiedLength)
		}
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		// For stored routine parameters, the tiny/medium/long text/blob types never accept a length,
		// and restoring with a default length (e.g. tinytext(255), mediumtext(16777215)) is unparsable.
		ft.SetFlen(types.UnspecifiedLength)
		ft.SetDecimal(types.UnspecifiedLength)
	case mysql.TypeBlob:
		// TEXT/BLOB optionally accept a length (e.g. TEXT(100)), but TiDB fills the implicit default
		// max length when it's unspecified. Restore should omit that default so the output matches
		// MySQL's SHOW CREATE PROCEDURE and remains parseable.
		//
		// Note: when the user explicitly specifies TEXT(65535)/BLOB(65535), the parser keeps
		// decimal as UnspecifiedLength (-1), so we can preserve the explicit length.
		defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
		if ft.GetFlen() == defaultFlen && ft.GetDecimal() == defaultDecimal {
			ft.SetFlen(types.UnspecifiedLength)
			ft.SetDecimal(types.UnspecifiedLength)
		}
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		// (0) is the implicit default for fractional seconds precision (fsp). MySQL omits it in
		// SHOW CREATE PROCEDURE output, so restore should omit it too.
		if ft.GetDecimal() == 0 {
			ft.SetDecimal(types.UnspecifiedLength)
		}
	case mysql.TypeDate:
		// "date(p)" syntax is invalid for procedure parameter.
		ft.SetFlen(types.UnspecifiedLength)
		ft.SetDecimal(types.UnspecifiedLength)
	case mysql.TypeJSON:
		// "json(p)" syntax is invalid for procedure parameter.
		ft.SetFlen(types.UnspecifiedLength)
		ft.SetDecimal(types.UnspecifiedLength)
	}
	// For stored routines we want:
	// - lower-case type names (to match existing routine restore output),
	// - no implicit display width / default length when unspecified (so restore can round-trip).
	flags := ctx.Flags
	flags &^= format.RestoreKeyWordUppercase
	flags &^= format.RestoreKeyWordLowercase
	return ft.Restore(format.NewRestoreCtx(flags, ctx.In))
}

// Restore implements Node interface.
func (n *StoreParameter) Restore(ctx *format.RestoreCtx) error {
	if !n.OmitParamStatus {
		switch n.Paramstatus {
		case ModeIn:
			ctx.WriteKeyWord("IN ")
		case ModeOut:
			ctx.WriteKeyWord("OUT ")
		case ModeInOut:
			ctx.WriteKeyWord("INOUT ")
		}
	}
	ctx.WriteName(n.ParamName)
	ctx.WritePlain(" ")
	return RestoreRoutineFieldType(ctx, n.ParamType)
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
	ft := n.DeclType.Clone()
	if collate := ft.GetCollate(); collate != "" {
		switch {
		case ctx.Flags.HasKeyWordUppercaseFlag():
			ft.SetCollate(strings.ToUpper(collate))
		case ctx.Flags.HasKeyWordLowercaseFlag():
			ft.SetCollate(strings.ToLower(collate))
		}
	}
	if err := ft.Restore(ctx); err != nil {
		return err
	}
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
	ctx.WriteKeyWord(" END")
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
	for i, stmt := range n.ProcedureProcStmts {
		node, ok := stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureProcStmts[i] = node.(StmtNode)
	}
	return v.Leave(n)
}

// ProcedureLabelBlock stored procedure block.
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
		return errors.New("Inconsistent start and end Label")
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
	return v.Leave(n)
}

func (n *ProcedureLabelBlock) GetErrorStatus() (string, bool) {
	return n.LabelEnd, n.LabelError
}

func (n *ProcedureLabelBlock) GetLabelName() string {
	return n.LabelName
}

// ProcedureLabelLoop stored procedure block.
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
		return errors.New("Inconsistent start and end Label")
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
	return v.Leave(n)
}

func (n *ProcedureLabelLoop) GetErrorStatus() (string, bool) {
	return n.LabelEnd, n.LabelError
}

func (n *ProcedureLabelLoop) GetLabelName() string {
	return n.LabelName
}

// StoredFuncName represents a pair of schema and table names.
type StoredFuncName struct {
	Schema string
	Func   string
}

// CreateProcedureInfo stored procedure object
type CreateProcedureInfo struct {
	ddlNode
	IfNotExists     bool
	Definer         *auth.UserIdentity
	ProcedureName   *TableName
	ProcedureParam  []*StoreParameter
	ProcedureBody   StmtNode
	Characteristics []ProcedureCharacteristic
	FunctionInfo    struct {
		RetType            *types.FieldType
		IsLoadable         bool
		Aggregate          bool
		LoadableReturnType types.EvalType
		SoName             string
	}
}

// Restore implements CreateProcedureInfo interface.
func (n *CreateProcedureInfo) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if !n.FunctionInfo.IsLoadable && n.Definer != nil {
		ctx.WriteKeyWord("DEFINER = ")
		if err := n.Definer.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore definer")
		}
		ctx.WriteKeyWord(" ")
	}
	if n.FunctionInfo.IsLoadable {
		if n.FunctionInfo.Aggregate {
			ctx.WriteKeyWord("AGGREGATE ")
		}
		ctx.WriteKeyWord("FUNCTION ")
		if n.IfNotExists {
			ctx.WriteKeyWord("IF NOT EXISTS ")
		}
		ctx.WriteName(n.ProcedureName.Name.String())
		ctx.WriteKeyWord(" RETURNS ")
		switch n.FunctionInfo.LoadableReturnType {
		case types.ETInt:
			ctx.WriteKeyWord("INTEGER")
		case types.ETString:
			ctx.WriteKeyWord("STRING")
		case types.ETReal:
			ctx.WriteKeyWord("REAL")
		case types.ETDecimal:
			ctx.WriteKeyWord("DECIMAL")
		default:
			ctx.WriteKeyWord(n.FunctionInfo.LoadableReturnType.String())
		}
		ctx.WriteKeyWord(" SONAME ")
		ctx.WriteString(n.FunctionInfo.SoName)
		return nil
	}
	if n.FunctionInfo.RetType != nil {
		ctx.WriteKeyWord("FUNCTION ")
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
				ctx.WritePlain(", ")
			}
			err := ProcedureParam.Restore(ctx)
			if err != nil {
				return err
			}
		}
		ctx.WritePlain(") ")
		ctx.WriteKeyWord("RETURNS ")
		err = RestoreRoutineFieldType(ctx, n.FunctionInfo.RetType)
		if err != nil {
			return err
		}
		ctx.WritePlain(" ")
		if n.Characteristics != nil {
			for _, characteristic := range n.Characteristics {
				err = characteristic.Restore(ctx)
				if err != nil {
					return err
				}
				ctx.WriteKeyWord(" ")
			}
		}
		err = (n.ProcedureBody).Restore(ctx)
		if err != nil {
			return err
		}
		return nil
	}
	ctx.WriteKeyWord("PROCEDURE ")
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
			ctx.WritePlain(", ")
		}
		err := ProcedureParam.Restore(ctx)
		if err != nil {
			return err
		}
	}
	ctx.WritePlain(") ")
	if n.Characteristics != nil {
		for _, characteristic := range n.Characteristics {
			err = characteristic.Restore(ctx)
			if err != nil {
				return err
			}
			ctx.WriteKeyWord(" ")
		}
	}
	err = (n.ProcedureBody).Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements CreateProcedureInfo Accept interface.
func (n *CreateProcedureInfo) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateProcedureInfo)
	for i, ProcedureParam := range n.ProcedureParam {
		node, ok := ProcedureParam.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureParam[i] = node.(*StoreParameter)
	}
	for i, Characteristics := range n.Characteristics {
		node, ok := Characteristics.Accept(v)
		if !ok {
			return n, false
		}
		n.Characteristics[i] = node.(ProcedureCharacteristic)
	}
	if n.ProcedureBody != nil {
		node, ok := n.ProcedureBody.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureBody = node.(StmtNode)
	}
	return v.Leave(n)
}

// DropProcedureStmt is the statement to drop a stored procedure or function.
type DropProcedureStmt struct {
	ddlNode

	IfExists   bool
	Name       *TableName
	IsFunction bool
}

// Type returns the type of the routine, PROCEDURE or FUNCTION.
func (n *DropProcedureStmt) Type() string {
	if n.IsFunction {
		return "FUNCTION"
	}
	return "PROCEDURE"
}

// Restore implements DropProcedureStmt interface.
func (n *DropProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	if n.IsFunction {
		ctx.WriteKeyWord("DROP FUNCTION ")
	} else {
		ctx.WriteKeyWord("DROP PROCEDURE ")
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	err := n.Name.Restore(ctx)
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

type ProcedureReturnStmt struct {
	stmtNode
	ReturnExpr ExprNode
}

// Restore implements ProcedureReturnStmt interface.
// TODO(lance6716): generated by AI, need review
func (n *ProcedureReturnStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RETURN ")
	if n.ReturnExpr != nil {
		err := n.ReturnExpr.Restore(ctx)
		if err != nil {
			return errors.Annotate(err, "An error occurred while restore return expr")
		}
	} else {
		ctx.WritePlain("NULL")
	}
	return nil
}

// Accept implements ProcedureReturnStmt Accept interface.
func (n *ProcedureReturnStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureReturnStmt)

	if n.ReturnExpr != nil {
		node, ok := n.ReturnExpr.Accept(v)
		if !ok {
			return n, false
		}
		n.ReturnExpr = node.(ExprNode)
	}
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
	for i, stmt := range n.ProcedureIfStmts {
		node, ok := stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureIfStmts[i] = node.(StmtNode)
	}
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

	for i, stmt := range n.ProcedureIfStmts {
		node, ok := stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureIfStmts[i] = node.(StmtNode)
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

// SimpleWhenThenStmt
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
	for i, procedureStmt := range n.ProcedureStmts {
		node, ok := procedureStmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureStmts[i] = node.(StmtNode)
	}
	return v.Leave(n)
}

// SimpleCaseStmt
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

// SearchWhenThenStmt
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
	for i, procedureStmt := range n.ProcedureStmts {
		node, ok := procedureStmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureStmts[i] = node.(StmtNode)
	}
	return v.Leave(n)
}

// SearchCaseStmt
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
	for i, procedureStmt := range n.ElseCases {
		node, ok := procedureStmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ElseCases[i] = node.(StmtNode)
	}
	return v.Leave(n)
}

// ProcedureRepeatStmt.
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

// ProcedureLoopStmt.
type ProcedureLoopStmt struct {
	stmtNode

	Body []StmtNode
}

// Restore implements ProcedureLoopStmt interface.
func (n *ProcedureLoopStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LOOP ")
	for _, stmt := range n.Body {
		err := stmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord(";")
	}
	ctx.WriteKeyWord(" END LOOP")
	return nil
}

// Accept implements ProcedureLoopStmt Accept interface.
func (n *ProcedureLoopStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureLoopStmt)

	for i, stmt := range n.Body {
		node, ok := stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.Body[i] = node.(StmtNode)
	}
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
	node, ok := n.Selectstring.Accept(v)
	if !ok {
		return n, false
	}
	n.Selectstring = node.(StmtNode)
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
	case ProcedureContinue:
		ctx.WriteKeyWord("CONTINUE ")
	case ProcedureExit:
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
	node, ok := n.Operate.Accept(v)
	if !ok {
		return n, false
	}
	n.Operate = node.(StmtNode)
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
	ProcedureErrorCondition

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
	ProcedureErrorCondition

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
	ProcedureErrorCondition

	ErrorCon int
}

// Restore implements Node interface.
func (n *ProcedureErrorCon) Restore(ctx *format.RestoreCtx) error {
	switch n.ErrorCon {
	case ProcedureSQLWarning:
		ctx.WriteKeyWord("SQLWARNING")
	case ProcedureNotFound:
		ctx.WriteKeyWord("NOT FOUND")
	case ProcedureSQLException:
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

// ProcedureJump stored procedure block.
type ProcedureJump struct {
	stmtNode
	Name    string
	IsLeave bool
}

// Restore implements ProcedureIterate interface.
func (n *ProcedureJump) Restore(ctx *format.RestoreCtx) error {
	if n.IsLeave {
		ctx.WriteKeyWord("LEAVE ")
	} else {
		ctx.WriteKeyWord("ITERATE ")
	}

	ctx.WriteName(n.Name)
	return nil
}

// Accept implements ProcedureIterate Accept interface.
func (n *ProcedureJump) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureJump)
	return v.Leave(n)
}

// AlterProcedureStmt saved change stored procedure Characteristics
type AlterProcedureStmt struct {
	ddlNode
	ProcedureName   *TableName
	Characteristics []ProcedureCharacteristic
	IsFunction      bool
}

// Restore implements Node interface.
func (n *AlterProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER ")
	if !n.IsFunction {
		ctx.WriteKeyWord("PROCEDURE ")
	} else {
		ctx.WriteKeyWord("FUNCTION ")
	}
	err := n.ProcedureName.Restore(ctx)
	if err != nil {
		return err
	}
	if len(n.Characteristics) != 0 {
		ctx.WriteKeyWord(" ")
		for i, characteristic := range n.Characteristics {
			err = characteristic.Restore(ctx)
			if err != nil {
				return err
			}
			if i != len(n.Characteristics)-1 {
				ctx.WriteKeyWord(" ")
			}
		}
	}
	return nil
}

// Accept implements ProcedureErrorCon Accept interface.
func (n *AlterProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterProcedureStmt)
	for i, Characteristics := range n.Characteristics {
		node, ok := Characteristics.Accept(v)
		if !ok {
			return n, false
		}
		n.Characteristics[i] = node.(ProcedureCharacteristic)
	}
	return v.Leave(n)
}

// ProcedureVar saved procedure local variable name.
type ProcedureVar struct {
	node
	Name model.CIStr
}

// Restore implements Node interface.
func (n *ProcedureVar) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain(n.Name.O)
	return nil
}

// Accept implements ProcedureVar Accept interface.
func (n *ProcedureVar) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureVar)
	return v.Leave(n)
}

// Format formats the `ProcedureVar` which is a local variable defined in procedure to a Writer.
func (n *ProcedureVar) Format(w io.Writer) {
	fmt.Fprint(w, n.Name.O)
}

// GetFlag gets the ExprNode flag.
func (n *ProcedureVar) GetFlag() uint64 {
	panic("Not implemented")
}

// SetFlag sets the ExprNode flag.
func (n *ProcedureVar) SetFlag(_ uint64) {
	panic("Not implemented")
}

// GetType gets the ExprNode type.
func (n *ProcedureVar) GetType() *types.FieldType {
	panic("Not implemented")
}

// SetType sets the ExprNode type.
func (n *ProcedureVar) SetType(tp *types.FieldType) {
	panic("Not implemented")
}
