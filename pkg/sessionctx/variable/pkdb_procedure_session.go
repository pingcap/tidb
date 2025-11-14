// Copyright 2023-2023 PingCAP, Inc.

package variable

import (
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	// BLOCKLABEL Indicates that this code block is of type block
	BLOCKLABEL = iota
	// JUMPLABEL Indicates that this code block is of type jump
	JUMPLABEL
	// LOOPLABEL Indicates that this code block is of type cycle
	LOOPLABEL
	// HANDLELABEL Indicates that this code block is of type handler error.
	HANDLELABEL
)

// CursorRes procedure cursor interface.
type CursorRes interface {
	// GetRow read a row of data.
	GetRow() chunk.Row
	// CloseCurs for closing cursor.
	CloseCurs() error
	// NextRow for point to next.
	NextRow()
	// GetName for get cursor name.
	GetName() string
	// GetLevel for cursor context
	GetLevel() uint
	Reset()
	IsOpen() bool
	Clone(contextMap map[*ProcedureContext]*ProcedureContext) CursorRes
}

// procedureHandle procedure handle interface.
type procedureHandle interface {
	// GetType get handle type.
	GetType() int
	Clone(contextMap map[*ProcedureContext]*ProcedureContext) procedureHandle
}

const (
	// EXIT handler error operate type exit
	EXIT = iota
	// CONTINUE handler error operate type continue
	CONTINUE
)

const (
	// CODE handler error use error code
	CODE = iota
	// STATE handler error use error string
	STATE
	// STATUS handler error use error status
	STATUS
)

// ProcedureVars procedure variable struct.
type ProcedureVars struct {
	name  string
	field *types.FieldType
	vars  types.Datum
}

// GetName read variable name.
func (vars *ProcedureVars) GetName() string {
	return vars.name
}

// GetFieldType get var field type.
func (vars *ProcedureVars) GetFieldType() *types.FieldType {
	return vars.field
}

// SetVar set val .
func (vars *ProcedureVars) SetVar(val types.Datum) {
	vars.vars = *val.Clone()
}

// ProcedureHandleBase save handle action and execute pointer.
type ProcedureHandleBase struct {
	Handle        int               // handler type (continue or exit)
	Operate       uint              // handler code block start position
	HandleContext *ProcedureContext // handler operating environment
}

// ProcedureLabel save label status.
type ProcedureLabel struct {
	Name       string
	LabelType  int  // is block or is loop
	LabelBegin uint // procedure label start position in ProcedureCommandList
	LabelEnd   uint // procedure label end position in ProcedureCommandList
}

// ProcedureSetGoTo save procedure jump interface.
type ProcedureSetGoTo interface {
	SetDest(label *ProcedureLabel, dest uint) bool
}

// ProcedureSetErrorTo save procedure error jump interface.
type ProcedureSetErrorTo interface {
	SetErrorDest(label *ProcedureLabel, dest uint) bool
}

// ProcedureHandleCode save handle error code.
type ProcedureHandleCode struct {
	Codes uint64
	ProcedureHandleBase
}

// ProcedureHandleState save handle error state.
type ProcedureHandleState struct {
	State int
	ProcedureHandleBase
}

// ProcedureHandleStatus save handle error status.
type ProcedureHandleStatus struct {
	Status string
	ProcedureHandleBase
}

// GetType return type.
func (p *ProcedureHandleCode) GetType() int {
	return CODE
}

// Clone Copy ProcedureHandleCode structure without data
func (p *ProcedureHandleCode) Clone(contextMap map[*ProcedureContext]*ProcedureContext) procedureHandle {
	base := ProcedureHandleBase{
		Handle:        p.Handle,
		Operate:       p.Operate,
		HandleContext: p.HandleContext.CopyContext(contextMap),
	}
	return &ProcedureHandleCode{Codes: p.Codes, ProcedureHandleBase: base}
}

// Clone Copy ProcedureHandleState structure without data
func (p *ProcedureHandleState) Clone(contextMap map[*ProcedureContext]*ProcedureContext) procedureHandle {
	base := ProcedureHandleBase{
		Handle:        p.Handle,
		Operate:       p.Operate,
		HandleContext: p.HandleContext.CopyContext(contextMap),
	}
	return &ProcedureHandleState{State: p.State, ProcedureHandleBase: base}
}

// Clone Copy ProcedureHandleStatus structure without data
func (p *ProcedureHandleStatus) Clone(contextMap map[*ProcedureContext]*ProcedureContext) procedureHandle {
	base := ProcedureHandleBase{
		Handle:        p.Handle,
		Operate:       p.Operate,
		HandleContext: p.HandleContext.CopyContext(contextMap),
	}
	return &ProcedureHandleStatus{Status: p.Status, ProcedureHandleBase: base}
}

// GetType return type.
func (p *ProcedureHandleState) GetType() int {
	return STATE
}

// GetType return type.
func (p *ProcedureHandleStatus) GetType() int {
	return STATUS
}

// ProcedureContext procedure environment variable.
type ProcedureContext struct {
	root *ProcedureContext
	// depth
	level uint
	// save procedure variable.
	Vars []*ProcedureVars
	// save procedure handles.
	Handles []procedureHandle
	// save procedure cursors.
	Cursors []CursorRes
	// use for set remote dest.
	NeedSet []ProcedureSetGoTo
	// use for set error dest.
	ErrorSet []ProcedureSetErrorTo
	TypeFlag int
	// use to save and output execution pointer.
	ProcedureReturn []uint
	// this level label.
	Label *ProcedureLabel
}

// Getlevel return level
func (pCon *ProcedureContext) Getlevel() uint {
	return pCon.level
}

// CopyContext recursively copys the routine context structure, using cache.
func (pCon *ProcedureContext) CopyContext(contextMap map[*ProcedureContext]*ProcedureContext) *ProcedureContext {
	if pCon == nil {
		return nil
	}
	// Use the cache if it exists.
	if newCon, ok := contextMap[pCon]; ok {
		return newCon
	}
	newCon := &ProcedureContext{}
	newCon.level = pCon.level
	contextMap[pCon] = newCon
	newCon.Vars = make([]*ProcedureVars, 0, len(pCon.Vars))
	for _, pVar := range pCon.Vars {
		newVar := &ProcedureVars{name: pVar.name, field: pVar.field.Clone(), vars: pVar.vars}
		newCon.Vars = append(newCon.Vars, newVar)
	}

	newCon.Handles = make([]procedureHandle, 0, len(pCon.Handles))
	for _, pHanlde := range pCon.Handles {
		newHanlde := pHanlde.Clone(contextMap)
		newCon.Handles = append(newCon.Handles, newHanlde)
	}

	newCon.Cursors = make([]CursorRes, 0, len(pCon.Cursors))
	for _, pCur := range pCon.Cursors {
		newCon.Cursors = append(newCon.Cursors, pCur.Clone(contextMap))
	}
	newCon.ProcedureReturn = make([]uint, 0, len(pCon.ProcedureReturn))
	newCon.TypeFlag = pCon.TypeFlag
	newCon.Label = pCon.Label

	// copy recursively.
	if pCon.root != nil {
		newCon.root = pCon.root.CopyContext(contextMap)
	}
	return newCon
}

// FindLabel find label.
func (pCon *ProcedureContext) FindLabel(name string, isLeave bool) *ProcedureContext {
	if pCon == nil {
		return nil
	}
	if pCon.Label != nil {
		if pCon.Label.Name == name {
			// BLOCKLABEL is only support leave.
			if !isLeave && pCon.Label.LabelType == BLOCKLABEL {
				return nil
			}
			return pCon
		}
	}
	// recursion find label.
	// If this context is HANDLELABEL, break recursion.
	if pCon.root != nil && pCon.TypeFlag != HANDLELABEL {
		return pCon.root.FindLabel(name, isLeave)
	}
	return nil
}

// CheckLabel check if exists the same name label.
func (pCon *ProcedureContext) CheckLabel(name string) bool {
	if pCon.Label != nil {
		if pCon.Label.Name == name {
			return true
		}
	}
	if pCon.root != nil && pCon.TypeFlag != HANDLELABEL {
		return pCon.root.CheckLabel(name)
	}
	return false
}

// ResetCur reset this name cursor on this context.
func (pCon *ProcedureContext) ResetCur(name string) error {
	if pCon == nil {
		return ErrSpCursorMismatch.GenWithStackByArgs(name)
	}
	num := len(pCon.Cursors)
	for i := num - 1; i >= 0; i-- {
		if pCon.Cursors[i].GetName() == name {
			pCon.Cursors[i].Reset()
			return nil
		}
	}

	return ErrSpCursorMismatch.GenWithStackByArgs(name)
}

// FindCurs find cursor base on name.
func (pCon *ProcedureContext) FindCurs(name string) (CursorRes, error) {
	if pCon == nil {
		return nil, ErrSpCursorMismatch.GenWithStackByArgs(name)
	}
	num := len(pCon.Cursors)
	for i := num - 1; i >= 0; i-- {
		if pCon.Cursors[i].GetName() == name {
			return pCon.Cursors[i], nil
		}
	}
	if pCon.root != nil {
		return pCon.root.FindCurs(name)
	}

	return nil, ErrSpCursorMismatch.GenWithStackByArgs(name)
}

// sessionProcedureContext use in session.
type sessionProcedureContext struct {
	// lock is for user defined variables. values and types is read/write protected.
	Lock              sync.RWMutex
	Context           *ProcedureContext
	BackupStmtCtx     []*stmtctx.BackupStmtCtx
	immutableVariable map[string]struct{} //this variable is immutable in current SQL
	changeVariable    map[string]struct{} //this variable will change in the current SQL
}

// NewProcedureContext create procedure context.
func NewProcedureContext(typeInfo int) *ProcedureContext {
	return &ProcedureContext{
		root:            nil,
		Vars:            make([]*ProcedureVars, 0, 10),
		Handles:         make([]procedureHandle, 0, 10),
		Cursors:         make([]CursorRes, 0, 10),
		TypeFlag:        typeInfo,
		ProcedureReturn: make([]uint, 0, 1),
	}
}

// NewProcedureVars create empty value.
func NewProcedureVars(name string, field *types.FieldType) *ProcedureVars {
	name = strings.ToLower(name)
	d := types.NewDatum("")
	d.SetNull()

	return &ProcedureVars{
		name:  name,
		field: field,
		vars:  d,
	}
}

// CheckVarName check if variable exists.
func (pCon *ProcedureContext) CheckVarName(name string) bool {
	if pCon == nil {
		return false
	}
	name = strings.ToLower(name)
	for _, vari := range pCon.Vars {
		if vari.name == name {
			return true
		}
	}
	return false
}

// FindVarName find variable if variable exists.
func (pCon *ProcedureContext) FindVarName(name string) bool {
	if pCon == nil {
		return false
	}
	name = strings.ToLower(name)
	for _, vari := range pCon.Vars {
		if vari.name == name {
			return true
		}
	}
	if pCon.root != nil {
		return pCon.root.FindVarName(name)
	}
	return false
}

// SetRoot set context root.
func (pCon *ProcedureContext) SetRoot(root *ProcedureContext) {
	if root == nil {
		return
	}
	pCon.level = root.level + 1
	pCon.root = root
}

// GetRoot get root context.
func (pCon *ProcedureContext) GetRoot() *ProcedureContext {
	return pCon.root
}

// GetProcedureVariable get value by name from ProcedureContext.
func (pCon *ProcedureContext) GetProcedureVariable(name string) (*types.FieldType, types.Datum, bool) {
	if pCon == nil {
		return nil, types.NewDatum(""), true
	}
	name = strings.ToLower(name)
	for _, procedureVar := range pCon.Vars {
		if procedureVar.name == name {
			return procedureVar.field, procedureVar.vars, false
		}
	}
	if pCon.root != nil {
		return pCon.root.GetProcedureVariable(name)
	}
	return nil, types.NewDatum(""), true
}

// SetProcedureContext set SessionVars context value.
// use this context as environment variable.
func (s *SessionVars) SetProcedureContext(context *ProcedureContext) error {
	if !s.inCallProcedure.inCall {
		return errors.New("This function cannot be used outside a stored procedure")
	}
	s.procedureContext.Context = context
	s.procedureContext.immutableVariable = make(map[string]struct{})
	s.procedureContext.changeVariable = make(map[string]struct{})
	return nil
}

// GetProcedureVariable get value by name from SessionVars.
func (s *SessionVars) GetProcedureVariable(name string) (*types.FieldType, types.Datum, bool) {
	if !s.inCallProcedure.inCall {
		return nil, types.NewDatum(""), true
	}
	s.procedureContext.Lock.Lock()
	defer s.procedureContext.Lock.Unlock()
	return s.procedureContext.Context.GetProcedureVariable(name)
}

// AddUchangableName indicates adding variable names that are not allowed to be modified.
func (s *SessionVars) AddUchangableName(name string) error {
	if !s.inCallProcedure.inCall {
		return nil
	}
	s.procedureContext.Lock.Lock()
	defer s.procedureContext.Lock.Unlock()
	if s.procedureContext.changeVariable != nil {
		_, ok := s.procedureContext.changeVariable[name]
		if ok {
			return errors.Errorf("the limit parameter '%s' is not supported to be modified in the same sql", name)
		}
	}
	if s.procedureContext.immutableVariable != nil {
		s.procedureContext.immutableVariable[name] = struct{}{}
	}
	return nil
}

// AddUpdatableVarName indicates adding variable names that need to be modified.
func (s *SessionVars) AddUpdatableVarName(name string) error {
	if !s.inCallProcedure.inCall {
		return nil
	}
	s.procedureContext.Lock.Lock()
	defer s.procedureContext.Lock.Unlock()
	if s.procedureContext.immutableVariable != nil {
		_, ok := s.procedureContext.immutableVariable[name]
		if ok {
			return errors.Errorf("the limit parameter '%s' is not supported to be modified in the same sql", name)
		}
	}
	if s.procedureContext.changeVariable != nil {
		s.procedureContext.changeVariable[name] = struct{}{}
	}
	return nil
}

// CheckUnchangableVariable indicate check if variable is mutable.
func (s *SessionVars) CheckUnchangableVariable(name string, needLock bool) bool {
	if !s.inCallProcedure.inCall {
		return false
	}
	if needLock {
		s.procedureContext.Lock.Lock()
		defer s.procedureContext.Lock.Unlock()
	}
	if s.procedureContext.immutableVariable != nil {
		_, ok := s.procedureContext.immutableVariable[name]
		if ok {
			return true
		}
	}
	return false
}

// GetLastBackupStmtCtx reads the recorded object.
func (s *SessionVars) GetLastBackupStmtCtx() *stmtctx.BackupStmtCtx {
	if !s.inCallProcedure.inCall {
		return nil
	}
	s.procedureContext.Lock.Lock()
	defer s.procedureContext.Lock.Unlock()
	if len(s.procedureContext.BackupStmtCtx) == 0 {
		return nil
	}
	return s.procedureContext.BackupStmtCtx[len(s.procedureContext.BackupStmtCtx)-1]
}

// AppendBackupStmtCtx backups current stmtctx
func (s *SessionVars) AppendBackupStmtCtx() {
	if !s.inCallProcedure.inCall {
		return
	}
	s.procedureContext.Lock.Lock()
	defer s.procedureContext.Lock.Unlock()
	s.procedureContext.BackupStmtCtx = append(s.procedureContext.BackupStmtCtx, s.StmtCtx.BackupForHandler())
}

// PopBackupStmtCtx deletes last backup stmtctx
func (s *SessionVars) PopBackupStmtCtx() {
	if !s.inCallProcedure.inCall {
		return
	}
	s.procedureContext.Lock.Lock()
	defer s.procedureContext.Lock.Unlock()
	n := len(s.procedureContext.BackupStmtCtx) - 1
	s.procedureContext.BackupStmtCtx[n] = nil
	s.procedureContext.BackupStmtCtx = s.procedureContext.BackupStmtCtx[:n]
}

// OutCallTemp Temporarily prohibit the use of stored procedure internal variables
func (s *SessionVars) OutCallTemp() {
	s.inCallProcedure.inCall = false
}

// RecoveryCall recover OutCallTemp
func (s *SessionVars) RecoveryCall() {
	s.inCallProcedure.inCall = true
}

// TryFindHandle Try to find a solution when encountering an error.
func (pCon *ProcedureContext) TryFindHandle(code int, errStatus string, hasWarning bool) *ProcedureHandleBase {
	if pCon == nil {
		return nil
	}
	for _, handler := range pCon.Handles {
		switch x := handler.(type) {
		case *ProcedureHandleCode:
			if code == int(x.Codes) {
				return &x.ProcedureHandleBase
			}
		case *ProcedureHandleStatus:
			if errStatus == x.Status {
				return &x.ProcedureHandleBase
			}
		}
	}
	for _, handler := range pCon.Handles {
		switch x := handler.(type) {
		case *ProcedureHandleState:
			switch x.State {
			case ast.PROCEDUR_SQLWARNING:
				if errStatus[:2] == "01" || hasWarning {
					return &x.ProcedureHandleBase
				}
			case ast.PROCEDUR_NOT_FOUND:
				if errStatus[:2] == "02" {
					return &x.ProcedureHandleBase
				}
			case ast.PROCEDUR_SQLEXCEPTION:
				if (errStatus[0] != '0' || errStatus[1] > '2') && !hasWarning {
					return &x.ProcedureHandleBase
				}
			}
		}
	}
	pContext := pCon
	for {
		// if this context is HANDLELABEL,skip this context root block.
		if (pContext == nil) || (pContext.TypeFlag != HANDLELABEL) {
			break
		}
		pContext = pContext.GetRoot()
		if pContext == nil {
			return nil
		}
	}
	return pContext.GetRoot().TryFindHandle(code, errStatus, hasWarning)
}
