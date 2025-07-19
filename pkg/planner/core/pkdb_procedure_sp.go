// Copyright 2022-2023 PingCAP, Inc.

package core

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func init() {
	expression.UpdateVariableVar = UpdateVariableVar
}

var (
	_ ProcedureExecPlan = &ResetProcedurceCursor{}
	_ ProcedureExecPlan = &ProcedurceCursorClose{}
	_ ProcedureExecPlan = &ProcedurceFetchInto{}
	_ ProcedureExecPlan = &OpenProcedurceCursor{}
	_ ProcedureExecPlan = &UpdateVariables{}
	_ ProcedureExecPlan = &ProcedureSaveIP{}
	_ ProcedureExecPlan = &ProcedureNoNeedSave{}
	_ ProcedureExecPlan = &ProcedureOutputIP{}
	_ ProcedureExecPlan = &ProcedureClearBlockVar{}
	_ ProcedureExecPlan = &ProcedureGoToStart{}
	_ ProcedureExecPlan = &ProcedureGoToEnd{}
	_ ProcedureExecPlan = &ProcedureIfGo{}
	_ ProcedureExecPlan = &procedureSearchCase{}
	_ ProcedureExecPlan = &procedureSimpleCase{}
	_ ProcedureExecPlan = &executeBaseSQL{}
	_ ProcedureExecPlan = &ProcedureGoToEndWithOutStmt{}

	_ variable.ProcedureSetGoTo    = &ProcedureGoToStart{}
	_ variable.ProcedureSetGoTo    = &ProcedureGoToEnd{}
	_ variable.ProcedureSetGoTo    = &ProcedureGoToEndWithOutStmt{}
	_ variable.ProcedureSetGoTo    = &ProcedureIfGo{}
	_ variable.ProcedureSetErrorTo = &ProcedureIfGo{}

	// NOCASE No instruction has been executed yet in case block.
	NOCASE = -1
)

// ProcedureExecPlan Indicates the stored procedure execution interface.
type ProcedureExecPlan interface {
	Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error)
	Hanlable() bool
	GetContext() *variable.ProcedureContext
	CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan
	GetString(ctx sessionctx.Context, level string) string
}

// ProcedureExec stores procedure plans.
type ProcedureExec struct {
	ProcedureParam       []*ProcedureParameterVal
	ProcedureCommandList []ProcedureExecPlan
	Labels               []*variable.ProcedureLabel
	ProcedureCtx         *variable.ProcedureContext
}

// initProcedureExec init procedureExec lists.
func (p *ProcedureExec) initProcedureExec() {
	p.ProcedureParam = make([]*ProcedureParameterVal, 0, 10)
	p.ProcedureCommandList = make([]ProcedureExecPlan, 0, 10)
	p.Labels = make([]*variable.ProcedureLabel, 0, 10)
}

// procedurceCurInfo stores procedure cursor info.
type procedurceCurInfo struct {
	curName      string
	selectStmt   string // select ast
	context      *variable.ProcedureContext
	reader       chunk.RowContainerReader
	fields       []*resolve.ResultField
	open         bool // tag
	rowContainer *chunk.RowContainer
}

// NeedCloseCur stores need close cursor.
// ID is cur deep level,The following ID cannot be smaller than the previous ID.
// Cursor is CursorRes.
type NeedCloseCur struct {
	ID     uint
	Cursor variable.CursorRes
}

// CacheAst Stores syntax tree
// isInvalid Indicates whether it is invalid
type CacheAst struct {
	sql       string
	isInvalid bool
	stmts     []ast.StmtNode
}

// GetString gets SQL string.
func (node *CacheAst) GetString() string {
	return node.sql
}

// CcahePrepareCheck skip DDL cache.
// DDL is not used frequently in stored procedures
// There is a bug in DDL syntax tree reuse.
// https://github.com/pingcap/tidb/issues/49465
func (node *CacheAst) CcahePrepareCheck() {
	if node == nil || node.isInvalid {
		return
	}
	for _, stmt := range node.stmts {
		switch stmt.(type) {
		case *ast.CreateTableStmt, *ast.AlterTableStmt, *ast.DropTableStmt:
			node.isInvalid = true
			return
		}
	}
}

// Disable disable cached ast.
func (node *CacheAst) Disable() {
	node.isInvalid = true
}

// SetStmts sets ast which need cached
func (node *CacheAst) SetStmts(stmts []ast.StmtNode) {
	node.isInvalid = false
	node.stmts = stmts
}

// GetStmts gets cached stmts
func (node *CacheAst) GetStmts() []ast.StmtNode {
	return node.stmts
}

// IsInvalid gets if ast is isInvalid
func (node *CacheAst) IsInvalid() bool {
	return node.isInvalid
}

func prepareCacheAst(ctx context.Context, astCache ProcedureCacheAst, exec sqlexec.SQLParser) error {
	if astCache.IsInvalid() {
		stmts, _, err := exec.ParseSQL(ctx, astCache.GetString())
		if err != nil {
			return err
		}
		astCache.SetStmts(stmts)
	} else {
		for _, stmt := range astCache.GetStmts() {
			ast.SetFlag(stmt)
		}
	}
	return nil
}

// CacheExpr Stores expr ast tree
// isInvalid Indicates whether it is invalid
// expr Indicates expr string
// stmt Indicates expr ast tree
type CacheExpr struct {
	isInvalid bool
	expr      string
	stmt      ast.StmtNode
}

// GetString gets expr string.
func (node *CacheExpr) GetString() string {
	return fmt.Sprintf("select (%s)", node.expr)
}

// Disable disable cached ast.
func (node *CacheExpr) Disable() {
	node.isInvalid = true
}

// SetStmts sets ast which need cached
func (node *CacheExpr) SetStmts(stmts []ast.StmtNode) {
	node.isInvalid = false
	node.stmt = stmts[0]
}

// GetStmts gets cached stmts
func (node *CacheExpr) GetStmts() []ast.StmtNode {
	return []ast.StmtNode{node.stmt}
}

// IsInvalid gets if ast is isInvalid
func (node *CacheExpr) IsInvalid() bool {
	return node.isInvalid
}

// ProcedureCacheAst Used to unify syntax trees
type ProcedureCacheAst interface {
	GetString() string
	Disable()
	SetStmts([]ast.StmtNode)
	GetStmts() []ast.StmtNode
	IsInvalid() bool
}

// NewCacheExpr new CacheExpr
func NewCacheExpr(isInvalid bool, expr string, stmt ast.StmtNode) *CacheExpr {
	return &CacheExpr{isInvalid, expr, stmt}
}

func deleteCacheCurs(id uint, cache []NeedCloseCur, name string) ([]NeedCloseCur, error) {
	for num := len(cache) - 1; num >= 0; num-- {
		if cache[num].ID > id {
			err := cache[num].Cursor.CloseCurs()
			if err != nil {
				return cache, err
			}
			cache = cache[:num]
			continue
		}
		if cache[num].Cursor.GetName() == name {
			if num == len(cache)-1 {
				return cache[:num], nil
			}
			cache = append(cache[:num], cache[num+1:]...)
			return cache, nil
		}
	}
	return cache, nil
}

// ReleseAll close all Cursor.
func ReleseAll(cache []NeedCloseCur) []NeedCloseCur {
	for _, curs := range cache {
		terror.Log(curs.Cursor.CloseCurs())
	}
	return cache[:0]
}

func addCache(cache []NeedCloseCur, curs variable.CursorRes) []NeedCloseCur {
	newCache := make([]NeedCloseCur, 0, len(cache)+1)
	id := curs.GetLevel()
	for num := len(cache) - 1; num >= 0; num-- {
		if id < cache[num].ID {
			continue
		}
		newCache = append(newCache, cache[:num+1]...)
		newCache = append(newCache, NeedCloseCur{curs.GetLevel(), curs})
		if num+1 < len(cache) {
			newCache = append(newCache, cache[num+1:]...)
		}
		return newCache
	}
	newCache = append(newCache, NeedCloseCur{curs.GetLevel(), curs})
	newCache = append(newCache, cache...)
	return newCache
}

// ExprNodeToString Get the SQL string from ExprNode.
func ExprNodeToString(node ast.ExprNode) string {
	if node != nil {
		if node.Text() == "" {
			var b []byte
			bf := bytes.NewBuffer(b)
			node.Restore(&format.RestoreCtx{
				Flags: format.DefaultRestoreFlags,
				In:    bf,
			})
			node.SetText(nil, bf.String())
			return bf.String()
		}
		return node.Text()
	}
	return ""
}

// NewProcedurceCurInfo create procedurceCurInfo instance only use test.
func NewProcedurceCurInfo(curName, selectStmt string, context *variable.ProcedureContext) *procedurceCurInfo {
	return &procedurceCurInfo{curName: curName, selectStmt: selectStmt, context: context}
}

// ResetProcedurceCursor clear cursor.
type ResetProcedurceCursor struct {
	curName string
	context *variable.ProcedureContext
}

// NewResetProcedurceCursor create ResetProcedurceCursor instance only use test.
func NewResetProcedurceCursor(curName string, context *variable.ProcedureContext) *ResetProcedurceCursor {
	return &ResetProcedurceCursor{
		curName: curName, context: context,
	}
}

// GetString gets the currently executed instruction
func (p *ResetProcedurceCursor) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] init %s value", level, p.curName)
}

// Execute implement clear cursor info.
func (p *ResetProcedurceCursor) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	*id++
	sctx.GetSessionVars().StmtCtx.TruncateWarnings(0)
	var err error
	cache, err = deleteCacheCurs(p.context.Getlevel(), cache, p.curName)
	if err != nil {
		return cache, err
	}
	err = p.context.ResetCur(p.curName)
	if err != nil {
		return cache, err
	}

	return cache, nil
}

// Hanlable indicates whether the command can handle.
func (p *ResetProcedurceCursor) Hanlable() bool {
	return false
}

// GetContext gets base context.
func (p *ResetProcedurceCursor) GetContext() *variable.ProcedureContext {
	return p.context
}

// ProcedurceCursorClose close cursor instance.
type ProcedurceCursorClose struct {
	curName string
	context *variable.ProcedureContext
}

// NewProcedurceCursorClose create ProcedurceCursorClose instance only use test.
func NewProcedurceCursorClose(curName string, context *variable.ProcedureContext) *ProcedurceCursorClose {
	return &ProcedurceCursorClose{
		curName: curName,
		context: context,
	}
}

// GetString gets the currently executed instruction
func (p *ProcedurceCursorClose) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] close cursor %s", level, p.curName)
}

// CloneStructure Clone ResetProcedurceCursor data structure without data.
func (p *ProcedurceCursorClose) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedurceCursorClose{
		curName: p.curName,
		context: p.context.CopyContext(contextMap),
	}
}

// Execute implement close cursor instance.
func (p *ProcedurceCursorClose) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	*id++
	sctx.GetSessionVars().StmtCtx.TruncateWarnings(0)
	var err error
	cache, err = deleteCacheCurs(p.context.Getlevel(), cache, p.curName)
	if err != nil {
		return cache, err
	}
	cursors, err := p.context.FindCurs(p.curName)
	if err != nil {
		return cache, err
	}
	curs, ok := cursors.(*procedurceCurInfo)
	if !ok {
		return cache, errors.Errorf("unspport CursorRes type %T", cursors)
	}
	err = curs.CloseCurs()
	if err != nil {
		return cache, err
	}
	return cache, nil
}

// Hanlable indicates whether the command can handle.
func (p *ProcedurceCursorClose) Hanlable() bool {
	return true
}

// GetContext gets base context.
func (p *ProcedurceCursorClose) GetContext() *variable.ProcedureContext {
	return p.context
}

// CloneStructure Clone ResetProcedurceCursor data structure without data.
func (p *ResetProcedurceCursor) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ResetProcedurceCursor{
		curName: p.curName,
		context: p.context.CopyContext(contextMap),
	}
}

// ProcedurceFetchInto fetch into procedure value.
type ProcedurceFetchInto struct {
	curName string
	context *variable.ProcedureContext
	vars    []string
}

// NewProcedurceFetchInto create ProcedurceFetchInto instance only use test.
func NewProcedurceFetchInto(curName string, context *variable.ProcedureContext, vars []string) *ProcedurceFetchInto {
	return &ProcedurceFetchInto{
		curName: curName,
		context: context,
		vars:    vars,
	}
}

// GetRow gets current row.
func (curInfo *procedurceCurInfo) GetRow() chunk.Row {
	return curInfo.reader.Current()
}

// NextRow switch to next line.
func (curInfo *procedurceCurInfo) NextRow() {
	curInfo.reader.Next()
}

// GetLevel gets current context level.
func (curInfo *procedurceCurInfo) GetLevel() uint {
	return curInfo.context.Getlevel()
}

// needUpdateCache check cache is finished reading.
func (curInfo *procedurceCurInfo) Clone(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) variable.CursorRes {
	return &procedurceCurInfo{
		curName: curInfo.curName, selectStmt: curInfo.selectStmt, context: curInfo.context.CopyContext(contextMap),
	}
}

// Execute implement read data into procedure value.
func (p *ProcedurceFetchInto) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	sctx.GetSessionVars().StmtCtx.TruncateWarnings(0)
	*id++
	cursors, err := p.context.FindCurs(p.curName)
	if err != nil {
		return cache, err
	}
	curs, ok := cursors.(*procedurceCurInfo)
	if !ok {
		return cache, errors.Errorf("unspport CursorRes type %T", cursors)
	}
	if !curs.open {
		return cache, errors.Trace(plannererrors.ErrSpCursorNotOpen)
	}
	if curs.reader.Error() != nil {
		return cache, curs.reader.Error()
	}
	if curs.reader.Current() == curs.reader.End() {
		return cache, errors.Trace(plannererrors.ErrSpFetchNoData)
	}
	defer func() {
		curs.NextRow()
	}()
	// sets data into procedure variable.
	if curs.GetRow().Len() != len(p.vars) {
		return cache, errors.Trace(plannererrors.ErrSpWrongNoOfFetchArgs)
	}
	ResetCallStatus(sctx)
	sctx.GetSessionVars().SetProcedureContext(p.context)
	for i := 0; i < curs.GetRow().Len(); i++ {
		datum := curs.GetRow().GetDatum(i, &curs.fields[i].Column.FieldType)
		err := UpdateVariableVar(p.vars[i], datum, sctx.GetSessionVars())
		if err != nil {
			return cache, err
		}
	}
	return cache, nil
}

// Hanlable indicates whether the command can handle.
func (p *ProcedurceFetchInto) Hanlable() bool {
	return true
}

// GetContext gets base context.
func (p *ProcedurceFetchInto) GetContext() *variable.ProcedureContext {
	return p.context
}

// GetString gets the currently executed instruction
func (p *ProcedurceFetchInto) GetString(_ sessionctx.Context, level string) string {
	str := fmt.Sprintf("[%s] fetch %s into ", level, p.curName)
	for i, varname := range p.vars {
		str += varname
		if i < len(p.vars)-1 {
			str += ","
		}
	}
	return str

}

// CloneStructure gets base context.
func (p *ProcedurceFetchInto) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedurceFetchInto{
		curName: p.curName,
		context: p.context.CopyContext(contextMap),
		vars:    p.vars,
	}
}

// OpenProcedurceCursor open cursor.
type OpenProcedurceCursor struct {
	curName string
	context *variable.ProcedureContext
}

// NewOpenProcedurceCursor create OpenProcedurceCursor instance only use test.
func NewOpenProcedurceCursor(curName string, context *variable.ProcedureContext) *OpenProcedurceCursor {
	return &OpenProcedurceCursor{
		curName: curName,
		context: context,
	}
}

// Execute implement open cursor.
func (p *OpenProcedurceCursor) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	*id++
	sctx.GetSessionVars().StmtCtx.TruncateWarnings(0)
	cursors, err := p.context.FindCurs(p.curName)
	if err != nil {
		return cache, err
	}
	curs, ok := cursors.(*procedurceCurInfo)
	if !ok {
		return cache, errors.Errorf("unspport CursorRes type %T", cursors)
	}
	err = curs.OpenCurs(ctx, sctx)
	if err != nil {
		return cache, err
	}
	cache = addCache(cache, curs)
	return cache, nil
}

// Hanlable indicates whether the command can handle.
func (p *OpenProcedurceCursor) Hanlable() bool {
	return true
}

// GetContext gets base context.
func (p *OpenProcedurceCursor) GetContext() *variable.ProcedureContext {
	return p.context
}

// GetString gets the currently executed instruction
func (p *OpenProcedurceCursor) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] open cursor %s", level, p.curName)
}

// CloneStructure Clone OpenProcedurceCursor data structure without data.
func (p *OpenProcedurceCursor) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &OpenProcedurceCursor{curName: p.curName, context: p.context.CopyContext(contextMap)}
}

// UpdateVariables init procedure variable.
type UpdateVariables struct {
	name     string
	expr     string
	context  *variable.ProcedureContext
	declType *types.FieldType
}

// ProcedureSaveIP save next execute id.
type ProcedureSaveIP struct {
	context *variable.ProcedureContext
	dest    uint
}

// NewProcedureSaveIP create ProcedureSaveIP instance only use test.
func NewProcedureSaveIP(context *variable.ProcedureContext, dest uint) *ProcedureSaveIP {
	return &ProcedureSaveIP{
		context: context,
		dest:    dest,
	}
}

// Execute implement save procedure execute id.
func (p *ProcedureSaveIP) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	p.context.ProcedureReturn = append(p.context.ProcedureReturn, *id)
	*id = p.dest + 1
	sctx.GetSessionVars().AppendBackupStmtCtx()
	return cache, nil
}

// Hanlable indicates whether the command can handle.
// internal command
func (p *ProcedureSaveIP) Hanlable() bool {
	return false
}

// GetContext gets base context.
func (p *ProcedureSaveIP) GetContext() *variable.ProcedureContext {
	return p.context
}

// GetString gets the currently executed instruction
func (p *ProcedureSaveIP) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] internal command", level)
}

// CloneStructure Clone ProcedureSaveIP data structure without data.
func (p *ProcedureSaveIP) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedureSaveIP{context: p.context.CopyContext(contextMap), dest: p.dest}
}

// ProcedureNoNeedSave do nothing.
type ProcedureNoNeedSave struct {
	dest    uint
	context *variable.ProcedureContext
}

// NewProcedureNoNeedSave create ProcedureNoNeedSave instance only use test.
func NewProcedureNoNeedSave(context *variable.ProcedureContext, dest uint) *ProcedureNoNeedSave {
	return &ProcedureNoNeedSave{
		dest:    dest,
		context: context,
	}
}

// Execute backups diagnostics area.
func (p *ProcedureNoNeedSave) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	*id = p.dest + 1
	sctx.GetSessionVars().AppendBackupStmtCtx()
	return cache, nil
}

// Hanlable indicates whether the command can handle.
// internal command
func (p *ProcedureNoNeedSave) Hanlable() bool {
	return false
}

// GetContext gets current conetxt.
func (p *ProcedureNoNeedSave) GetContext() *variable.ProcedureContext {
	return p.context
}

// GetString gets the currently executed instruction
func (p *ProcedureNoNeedSave) GetString(ctx sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] internal command", level)
}

// CloneStructure Clone ProcedureNoNeedSave data structure without data.
func (p *ProcedureNoNeedSave) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedureNoNeedSave{dest: p.dest, context: p.context.CopyContext(contextMap)}
}

// ProcedureOutputIP recover execute ip.
type ProcedureOutputIP struct {
	context *variable.ProcedureContext
}

// NewProcedureOutputIP create ProcedureOutputIP instance only use test.
func NewProcedureOutputIP(context *variable.ProcedureContext) *ProcedureOutputIP {
	return &ProcedureOutputIP{
		context: context,
	}
}

// Execute outputs the execution ip saved and clears the last diagnostics area.
func (p *ProcedureOutputIP) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	if len(p.context.ProcedureReturn) == 0 {
		err := errors.New("Insufficient procedureReturn data")
		return cache, errors.AddStack(err)
	}
	res := p.context.ProcedureReturn[len(p.context.ProcedureReturn)-1]
	p.context.ProcedureReturn = p.context.ProcedureReturn[:len(p.context.ProcedureReturn)-1]
	sctx.GetSessionVars().PopBackupStmtCtx()
	*id = res
	return cache, nil
}

// Hanlable indicates whether the command can handle.
// internal command
func (p *ProcedureOutputIP) Hanlable() bool {
	return false
}

// GetContext gets base context.
func (p *ProcedureOutputIP) GetContext() *variable.ProcedureContext {
	return p.context
}

// GetString gets the currently executed instruction
func (p *ProcedureOutputIP) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] internal command", level)
}

// CloneStructure Clone ProcedureOutputIP data structure without data.
func (p *ProcedureOutputIP) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedureOutputIP{
		context: p.context.CopyContext(contextMap),
	}
}

// GetLen gets num of procedure execute
func (p ProcedureExec) GetLen() uint {
	return uint(len(p.ProcedureCommandList))
}

// ProcedureClearBlockVar clear block variables
type ProcedureClearBlockVar struct {
	context *variable.ProcedureContext
}

// Execute clear this block vars.
func (p *ProcedureClearBlockVar) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	p.context.Vars = p.context.Vars[:0]
	*id = *id + 1
	return cache, nil
}

// Hanlable indicates whether the command can handle.
// internal command
func (p *ProcedureClearBlockVar) Hanlable() bool {
	return false
}

// GetContext gets base context.
func (p *ProcedureClearBlockVar) GetContext() *variable.ProcedureContext {
	return p.context
}

// GetString gets the currently executed instruction
func (p *ProcedureClearBlockVar) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] internal command", level)
}

// CloneStructure Clone ProcedureClearBlockVar data structure without data.
func (p *ProcedureClearBlockVar) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedureClearBlockVar{p.context.CopyContext(contextMap)}
}

// ProcedureGoToStart go to label begin.
type ProcedureGoToStart struct {
	dest  uint
	label *variable.ProcedureLabel
}

// NewProcedureGoToStart create NewProcedureGoToStart instance only use test.
func NewProcedureGoToStart(dest uint, label *variable.ProcedureLabel) *ProcedureGoToStart {
	return &ProcedureGoToStart{
		dest:  dest,
		label: label,
	}
}

// SetDest sets label begin ip.
func (jump *ProcedureGoToStart) SetDest(label *variable.ProcedureLabel, dest uint) bool {
	if label != jump.label {
		return false
	}
	jump.dest = label.LabelBegin
	return true
}

// Execute jump to label begin ip.
func (jump *ProcedureGoToStart) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	*id = jump.dest
	return cache, nil
}

// Hanlable indicates whether the command can handle.
// internal command
func (p *ProcedureGoToStart) Hanlable() bool {
	return false
}

// GetContext gets base context.
func (jump *ProcedureGoToStart) GetContext() *variable.ProcedureContext {
	return nil
}

// GetString gets the currently executed instruction
func (jump *ProcedureGoToStart) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] internal command", level)
}

// CloneStructure Clone ProcedureGoToStart data structure without data.
func (jump *ProcedureGoToStart) CloneStructure(map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedureGoToStart{dest: jump.dest,
		label: jump.label}
}

// ProcedureGoToEnd go to label end.
type ProcedureGoToEnd struct {
	dest  uint
	label *variable.ProcedureLabel
}

// NewProcedureGoToEnd create ProcedureGoToEnd instance. only use in test.
func NewProcedureGoToEnd(label *variable.ProcedureLabel) *ProcedureGoToEnd {
	return &ProcedureGoToEnd{
		label: label,
	}
}

// SetDest sets label end ip.
func (jump *ProcedureGoToEnd) SetDest(label *variable.ProcedureLabel, dest uint) bool {
	if label != jump.label {
		return false
	}
	jump.dest = dest
	return true
}

// Execute jump to label end ip.
func (jump *ProcedureGoToEnd) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	*id = jump.dest
	return cache, nil
}

// Hanlable indicates whether the command can handle.
// internal command
func (p *ProcedureGoToEnd) Hanlable() bool {
	return false
}

// GetContext gets base context.
func (jump *ProcedureGoToEnd) GetContext() *variable.ProcedureContext {
	return nil
}

// GetString gets the currently executed instruction
func (jump *ProcedureGoToEnd) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] internal command", level)
}

// CloneStructure Clone ProcedureGoToEnd data structure without data.
func (jump *ProcedureGoToEnd) CloneStructure(map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedureGoToEnd{
		dest: jump.dest, label: jump.label,
	}
}

// ProcedureGoToEndWithOutStmt goes to label end.
type ProcedureGoToEndWithOutStmt struct {
	dest    uint
	label   *variable.ProcedureLabel
	context *variable.ProcedureContext
}

// NewProcedureGoToEndWithOutStmt creates ProcedureGoToEnd and clear BackupStmtCtx instance. only use in test.
func NewProcedureGoToEndWithOutStmt(label *variable.ProcedureLabel, context *variable.ProcedureContext) *ProcedureGoToEndWithOutStmt {
	return &ProcedureGoToEndWithOutStmt{
		label:   label,
		context: context,
	}
}

// SetDest sets label end ip.
func (jump *ProcedureGoToEndWithOutStmt) SetDest(label *variable.ProcedureLabel, dest uint) bool {
	if label != jump.label {
		return false
	}
	jump.dest = dest
	return true
}

// Execute jumps to label end ip.
func (jump *ProcedureGoToEndWithOutStmt) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	*id = jump.dest
	sctx.GetSessionVars().PopBackupStmtCtx()
	return cache, nil
}

// Hanlable indicates whether the command can handle.
// internal command
func (p *ProcedureGoToEndWithOutStmt) Hanlable() bool {
	return false
}

// GetContext gets base context.
func (jump *ProcedureGoToEndWithOutStmt) GetContext() *variable.ProcedureContext {
	return jump.context
}

// GetString gets the currently executed instruction
func (jump *ProcedureGoToEndWithOutStmt) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] internal command", level)
}

// CloneStructure Clone ProcedureGoToEnd data structure without data.
func (jump *ProcedureGoToEndWithOutStmt) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedureGoToEndWithOutStmt{
		dest: jump.dest, label: jump.label, context: jump.context.CopyContext(contextMap),
	}
}

// Execute create variable and init variable.
func (vars *UpdateVariables) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	*id++
	sctx.GetSessionVars().StmtCtx.TruncateWarnings(0)
	err := sctx.GetSessionVars().SetProcedureContext(vars.context)
	if err != nil {
		return cache, err
	}
	// according to bug14643_1.
	// If the initialization fails, the variable will still be created.
	d, err := GetExprValue(ctx, &CacheExpr{expr: vars.expr, isInvalid: true}, vars.declType, sctx, vars.name)
	vari := variable.NewProcedureVars(vars.name, vars.declType)
	vars.context.Vars = append(vars.context.Vars, vari)
	if err != nil {
		return cache, err
	}
	err = UpdateVariableVar(vars.name, d, sctx.GetSessionVars())
	if err != nil {
		return cache, err
	}
	return cache, nil
}

// Hanlable indicates whether the command can handle.
func (p *UpdateVariables) Hanlable() bool {
	return true
}

// GetContext gets base context.
func (vars *UpdateVariables) GetContext() *variable.ProcedureContext {
	// Use the upper handle to handle errors
	return vars.context.GetRoot()
}

// GetString gets the currently executed instruction
func (vars *UpdateVariables) GetString(_ sessionctx.Context, level string) string {
	return fmt.Sprintf("[%s] Initialize %s using %s", level, vars.name, vars.expr)
}

// CloneStructure Clone UpdateVariables data structure without data.
func (vars *UpdateVariables) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &UpdateVariables{name: vars.name,
		expr:     vars.expr,
		context:  vars.context.CopyContext(contextMap),
		declType: vars.declType}
}

// ProcedureIfGo Logical Judgment Jump Structure.
type ProcedureIfGo struct {
	context   *variable.ProcedureContext
	label     *variable.ProcedureLabel
	expr      *CacheExpr
	dest      uint
	errorDest uint
}

// NewProcedureIfGo create ProcedureIfGo instance only use test.
func NewProcedureIfGo(context *variable.ProcedureContext, label *variable.ProcedureLabel, expr string) *ProcedureIfGo {
	return &ProcedureIfGo{
		context: context,
		label:   label,
		expr:    &CacheExpr{isInvalid: true, expr: expr},
	}
}

// Execute Logical Judgment if true go to next, if false go to dest, if error go to errorDest.
func (pIf *ProcedureIfGo) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) ([]NeedCloseCur, error) {
	var err error
	defer func() {
		if err != nil {
			*id = pIf.errorDest
		}
	}()
	err = sctx.GetSessionVars().SetProcedureContext(pIf.context)
	if err != nil {
		return cache, err
	}
	datum, err := GetExprValue(ctx, pIf.expr, types.NewFieldType(mysql.TypeTiny), sctx, "")
	if err != nil {
		return cache, err
	}
	if !datum.IsNull() {
		isTrue, err := datum.ToBool(sctx.GetSessionVars().StmtCtx.TypeCtx())
		if err != nil {
			return cache, err
		}
		if isTrue == 1 {
			*id++
			return cache, nil
		}
	}

	*id = pIf.dest
	return cache, nil
}

// Hanlable indicates whether the command can handle.
func (p *ProcedureIfGo) Hanlable() bool {
	return true
}

// GetContext gets base context.
func (pIf *ProcedureIfGo) GetContext() *variable.ProcedureContext {
	return pIf.context
}

// GetString gets the currently executed instruction
func (vars *ProcedureIfGo) GetString(ctx sessionctx.Context, level string) string {
	str := fmt.Sprintf("[%s] Using %s in if statements.", level, vars.expr.expr)
	localVars := getlocalVariableFromAst(vars.expr.GetStmts()[0], vars.context, ctx)
	if len(localVars) > 0 {
		var varStr string
		for i, localVar := range localVars {
			varStr += fmt.Sprintf("%s:%s", localVar.name, localVar.data)
			if i < len(localVars)-1 {
				varStr += ","
			}
		}
		str += fmt.Sprintf(" local variables[%s]", varStr)
	}
	return str
}

// CloneStructure Clone ProcedureIfGo data structure without data.
func (pIf *ProcedureIfGo) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &ProcedureIfGo{
		context:   pIf.context.CopyContext(contextMap),
		label:     pIf.label,
		expr:      pIf.expr,
		dest:      pIf.dest,
		errorDest: pIf.errorDest,
	}
}

// SetDest sets false dest.
func (pIf *ProcedureIfGo) SetDest(label *variable.ProcedureLabel, dest uint) bool {
	if label != pIf.label {
		return false
	}
	pIf.dest = dest
	return true
}

// SetErrorDest sets error dest.
func (pIf *ProcedureIfGo) SetErrorDest(label *variable.ProcedureLabel, dest uint) bool {
	if label != pIf.label {
		return false
	}
	pIf.errorDest = dest
	return true
}

// nextRow switch to next line.
func (curInfo *procedurceCurInfo) GetName() string {
	return curInfo.curName
}

func (curInfo *procedurceCurInfo) IsOpen() bool {
	return curInfo.open
}

// nextRow switch to next line.
func (curInfo *procedurceCurInfo) Reset() {
	curInfo.open = false
	if curInfo.reader != nil {
		curInfo.reader.Close()
		curInfo.reader = nil
	}
	curInfo.fields = nil
	if curInfo.rowContainer != nil {
		curInfo.rowContainer.GetMemTracker().Detach()
		curInfo.rowContainer.GetDiskTracker().Detach()
		curInfo.rowContainer.Close()
		curInfo.rowContainer = nil
	}
}

// NewExecuteBaseSQL create executeBaseSQL instance only use test.
func NewExecuteBaseSQL(sql string, context *variable.ProcedureContext) *executeBaseSQL {
	return &executeBaseSQL{
		cacheStmt: &CacheAst{sql: sql, isInvalid: true},
		context:   context,
	}
}

// executeBaseSQL execute base sql.
type executeBaseSQL struct {
	cacheStmt *CacheAst
	context   *variable.ProcedureContext
}

// conditionDest If condition equal jump to dest destination
type conditionDest struct {
	condition *CacheExpr
	dest      uint
}

// NewProcedureSimpleCase create procedureSimpleCase instance only use test.
func NewProcedureSimpleCase(context *variable.ProcedureContext, hasElse bool, condition string, errDest uint, elseDest uint, collate string) *procedureSimpleCase {
	return &procedureSimpleCase{
		hasElse:   hasElse,
		context:   context,
		condition: &CacheExpr{expr: condition, isInvalid: true},
		errDest:   errDest,
		elseDest:  elseDest,
		collate:   collate,
	}
}

// NewConditionDest create conditionDest instance only use test.
func NewConditionDest(condition string, dest uint) *conditionDest {
	return &conditionDest{
		condition: &CacheExpr{expr: condition, isInvalid: true},
		dest:      dest,
	}
}

// AddDest append dest in procedureSimpleCase.
func (procedureCase *procedureSimpleCase) AddDest(con *conditionDest) {
	procedureCase.dests = append(procedureCase.dests, *con)
}

// procedureSimpleCase represent `case expr when ... then ... `
type procedureSimpleCase struct {
	hasElse   bool
	context   *variable.ProcedureContext
	condition *CacheExpr
	dests     []conditionDest
	errDest   uint
	elseDest  uint
	collate   string
	ID        int
}

// Execute implement `case expr when then` to get jump address.
func (procedureCase *procedureSimpleCase) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) (newcache []NeedCloseCur, err error) {
	defer func() {
		if err != nil {
			*id = procedureCase.errDest
		}
	}()
	err = sctx.GetSessionVars().SetProcedureContext(procedureCase.context)
	if err != nil {
		return cache, err
	}
	datum, err := getExprValue(ctx, procedureCase.condition, sctx)
	if err != nil {
		return cache, err
	}
	for i, dest := range procedureCase.dests {
		procedureCase.ID = i
		datum1, err := getExprValue(ctx, dest.condition, sctx)
		if err != nil {
			return cache, err
		}
		// Compare for equality
		v, err := datum.Compare(sctx.GetSessionVars().StmtCtx.TypeCtx(), &datum1, collate.GetCollator(procedureCase.collate))
		if err != nil {
			return cache, err
		}
		if v == 0 {
			*id = dest.dest
			return cache, nil
		}
	}
	// no match find else.
	if procedureCase.hasElse {
		// if exists else
		*id = procedureCase.elseDest
	} else {
		procedureCase.ID = NOCASE
		// if not exists else
		return cache, errors.AddStack(plannererrors.ErrSpCaseNotFound)
	}

	return cache, nil
}

// Hanlable indicates whether the command can handle.
func (p *procedureSimpleCase) Hanlable() bool {
	return true
}

// GetContext gets base context.
func (procedureCase *procedureSimpleCase) GetContext() *variable.ProcedureContext {
	return procedureCase.context
}

// GetString gets the currently executed instruction
func (procedureCase *procedureSimpleCase) GetString(ctx sessionctx.Context, level string) string {
	var str string
	if procedureCase.ID != NOCASE {
		str = fmt.Sprintf("[%s] Using case_expr:%s, when_expr:%s in case statements.", level, procedureCase.condition.expr,
			procedureCase.dests[procedureCase.ID].condition.expr)
	} else {
		str = fmt.Sprintf("[%s] Using case_expr:%s in case statements.", level, procedureCase.condition.expr)
	}

	localVars := getlocalVariableFromAst(procedureCase.condition.GetStmts()[0], procedureCase.context, ctx)
	if len(localVars) > 0 {
		var varStr string
		for i, localVar := range localVars {
			varStr += fmt.Sprintf("%s:%s", localVar.name, localVar.data)
			if i < len(localVars)-1 {
				varStr += ","
			}
		}
		str += fmt.Sprintf(" case local variables[%s]", varStr)
	}
	if procedureCase.ID != NOCASE {
		localVars = getlocalVariableFromAst(procedureCase.dests[procedureCase.ID].condition.GetStmts()[0], procedureCase.context, ctx)
		if len(localVars) > 0 {
			var varStr string
			for i, localVar := range localVars {
				varStr += fmt.Sprintf("%s:%s", localVar.name, localVar.data)
				if i < len(localVars)-1 {
					varStr += ","
				}
			}
			str += fmt.Sprintf(" when local variables[%s]", varStr)
		}
	}
	return str
}

// CloneStructure gets base context.
func (procedureCase *procedureSimpleCase) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &procedureSimpleCase{
		hasElse:   procedureCase.hasElse,
		context:   procedureCase.context.CopyContext(contextMap),
		condition: procedureCase.condition,
		dests:     procedureCase.dests,
		errDest:   procedureCase.errDest,
		elseDest:  procedureCase.elseDest,
		collate:   procedureCase.collate,
	}

}

// NewProcedureSearchCase create procedureSearchCase instance only use test.
func NewProcedureSearchCase(context *variable.ProcedureContext, hasElse bool, errDest uint, elseDest uint, collate string) *procedureSearchCase {
	return &procedureSearchCase{
		hasElse:  hasElse,
		context:  context,
		errDest:  errDest,
		elseDest: elseDest,
		collate:  collate,
	}
}

// AddDest append dest in procedureSearchCase.
func (procedureSearch *procedureSearchCase) AddDest(con *conditionDest) {
	procedureSearch.dests = append(procedureSearch.dests, *con)
}

// procedureSearchCase represent `case when ... then ... `
type procedureSearchCase struct {
	hasElse  bool
	context  *variable.ProcedureContext
	dests    []conditionDest
	errDest  uint
	elseDest uint
	collate  string
	id       int
}

// Execute implement `case when then` to get jump address.
func (procedureSearch *procedureSearchCase) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) (newCache []NeedCloseCur, err error) {
	defer func() {
		if err != nil {
			*id = procedureSearch.errDest
		}
	}()
	err = sctx.GetSessionVars().SetProcedureContext(procedureSearch.context)
	if err != nil {
		return cache, err
	}
	for i, dest := range procedureSearch.dests {
		procedureSearch.id = i
		datum1, err := getExprValue(ctx, dest.condition, sctx)
		if err != nil {
			return cache, err
		}
		if !datum1.IsNull() {
			isTrue, err := datum1.ToBool(sctx.GetSessionVars().StmtCtx.TypeCtx())
			if err != nil {
				return cache, err
			}
			if isTrue == 1 {
				*id = dest.dest
				return cache, nil
			}
		}
	}
	if procedureSearch.hasElse {
		*id = procedureSearch.elseDest
	} else {
		procedureSearch.id = NOCASE
		return cache, errors.AddStack(plannererrors.ErrSpCaseNotFound)
	}

	return cache, nil
}

// Hanlable indicates whether the command can handle.
func (procedureSearch *procedureSearchCase) Hanlable() bool {
	return true
}

// GetContext gets base context.
func (procedureSearch *procedureSearchCase) GetContext() *variable.ProcedureContext {
	return procedureSearch.context
}

// GetString gets the currently executed instruction
func (procedureSearch *procedureSearchCase) GetString(ctx sessionctx.Context, level string) string {
	if procedureSearch.id == NOCASE {
		return fmt.Sprintf("[%s] no case meet operating conditions in case statements.", level)
	}
	str := fmt.Sprintf("[%s] Using search_expr:%s in case statements.", level, procedureSearch.dests[procedureSearch.id].condition.expr)
	localVars := getlocalVariableFromAst(procedureSearch.dests[procedureSearch.id].condition.GetStmts()[0], procedureSearch.context, ctx)
	if len(localVars) > 0 {
		var varStr string
		for i, localVar := range localVars {
			varStr += fmt.Sprintf("%s:%s", localVar.name, localVar.data)
			if i < len(localVars)-1 {
				varStr += ","
			}
		}
		str += fmt.Sprintf(" [local variables[%s]]", varStr)
	}
	return str
}

// CloneStructure gets base context.
func (procedureSearch *procedureSearchCase) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &procedureSearchCase{
		hasElse:  procedureSearch.hasElse,
		context:  procedureSearch.context.CopyContext(contextMap),
		dests:    procedureSearch.dests,
		errDest:  procedureSearch.errDest,
		elseDest: procedureSearch.elseDest,
		collate:  procedureSearch.collate,
	}
}

// executeSelectStmt handling selectstmt with and without result set in procedure.
// if param `v` is not nil, the param `node` is the internal statement of `v`.
// if param `v` is nil, only the param `node` is effective.
func executeSelectStmt(ctx context.Context, sctx sessionctx.Context, node *ast.SelectStmt, v *ast.ExecuteStmt) error {
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	var execNode ast.StmtNode = node
	if v != nil {
		execNode = v
	}
	if node.SelectIntoOpt != nil {
		rows, _, err := exec.ExecRestrictedStmt(ctx, execNode, sqlexec.ExecOptionUseCurSession)
		if err != nil {
			return err
		}
		if len(rows) != 0 {
			var sb strings.Builder
			_ = node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
			return errors.Errorf("sql:%s should be no result set", (sb.String()))
		}
	} else {
		err := sctx.GetSessionExec().MultiHanldeNodeWithResult(ctx, execNode)
		if err != nil {
			return err
		}
	}
	return nil
}

// Execute execute base sql.
func (p *executeBaseSQL) Execute(ctx context.Context, sctx sessionctx.Context, id *uint, cache []NeedCloseCur) (outcache []NeedCloseCur, err error) {
	*id++
	outcache = cache
	defer func() {
		if !variable.TiDBEnableSPAstReuse.Load() {
			p.cacheStmt.isInvalid = true
		}
		if err != nil {
			p.cacheStmt.isInvalid = true
		}
	}()
	err = sctx.GetSessionVars().SetProcedureContext(p.context)
	if err != nil {
		return cache, err
	}
	ResetCallStatus(sctx)
	p.cacheStmt.CcahePrepareCheck()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnProcedure)
	err = prepareCacheAst(ctx, p.cacheStmt, sctx.(sqlexec.SQLParser))
	if err != nil {
		return cache, err
	}
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	for _, stmt := range p.cacheStmt.stmts {
		switch x := stmt.(type) {
		case *ast.SelectStmt:
			// https://github.com/pingcap/tidb/issues/49166
			if x.SelectIntoOpt != nil {
				p.cacheStmt.isInvalid = true
			}
			err := executeSelectStmt(ctx, sctx, x, nil)
			if err != nil {
				return cache, err
			}

		case *ast.ExplainStmt, *ast.SetOprStmt, *ast.ShowStmt:
			err := sctx.GetSessionExec().MultiHanldeNodeWithResult(ctx, x)
			if err != nil {
				return cache, err
			}

		case *ast.PrepareStmt:
			// https://dev.mysql.com/doc/refman/8.0/en/prepare.html#:~:text=A%20statement%20prepared%20in%20stored%20program%20context%20cannot%20refer%20to%20stored
			// According to the official mysql documentation, prepare cannot use stored procedure local variables
			sctx.GetSessionVars().OutCallTemp()
			defer sctx.GetSessionVars().RecoveryCall()
			rows, _, err := exec.ExecRestrictedStmt(ctx, x, sqlexec.ExecOptionUseCurSession)
			if err != nil {
				return cache, err
			}
			if len(rows) != 0 {
				var sb strings.Builder
				_ = x.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
				return cache, errors.Errorf("sql:%s should be no result set", (sb.String()))
			}

		case *ast.ExecuteStmt:
			p.cacheStmt.isInvalid = true
			// https://dev.mysql.com/doc/refman/8.0/en/prepare.html#:~:text=A%20statement%20prepared%20in%20stored%20program%20context%20cannot%20refer%20to%20stored
			// According to the official mysql documentation, prepare cannot use stored procedure local variables
			sctx.GetSessionVars().OutCallTemp()
			defer sctx.GetSessionVars().RecoveryCall()
			// clear cache, use real value
			x.PrepStmt = nil
			stmt, err := GetPreparedStmt(x, sctx.GetSessionVars())
			if err != nil {
				return cache, err
			}
			if stmt.InUse {
				return cache, errors.AddStack(plannererrors.ErrPsNoRecursion)
			}
			// Query the prepare object to determine whether there is a return value.
			switch v := stmt.PreparedAst.Stmt.(type) {
			case *ast.SelectStmt:
				err := executeSelectStmt(ctx, sctx, v, x)
				if err != nil {
					return cache, err
				}

			// What other types have result set??
			case *ast.ExplainStmt, *ast.SetOprStmt, *ast.ShowStmt:
				err := sctx.GetSessionExec().MultiHanldeNodeWithResult(ctx, x)
				if err != nil {
					return cache, err
				}
			default:
				rows, _, err := exec.ExecRestrictedStmt(ctx, x, sqlexec.ExecOptionUseCurSession)
				if err != nil {
					return cache, err
				}
				if len(rows) != 0 {
					var sb strings.Builder
					_ = x.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
					return cache, errors.Errorf("sql:%s should be no result set", (sb.String()))
				}
			}

		default:
			rows, _, err := exec.ExecRestrictedStmt(ctx, x, sqlexec.ExecOptionUseCurSession)
			if err != nil {
				return cache, err
			}
			if len(rows) != 0 {
				var sb strings.Builder
				_ = x.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
				return cache, errors.Errorf("sql:%s should be no result set", (sb.String()))
			}
		}

	}
	return cache, nil
}

// GetContext gets base context.
func (p *executeBaseSQL) GetContext() *variable.ProcedureContext {
	return p.context
}

// GetString gets the currently executed instruction
func (p *executeBaseSQL) GetString(ctx sessionctx.Context, level string) string {
	str := fmt.Sprintf("[%s] execute SQL: %s.", level, p.cacheStmt.sql)
	for _, stmt := range p.cacheStmt.stmts {
		switch x := stmt.(type) {
		case *ast.SelectStmt, *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.CallStmt:
			localVars := getlocalVariableFromAst(x, p.context, ctx)
			if len(localVars) > 0 {
				var varStr string
				for i, localVar := range localVars {
					varStr += fmt.Sprintf("%s:%s", localVar.name, localVar.data)
					if i < len(localVars)-1 {
						varStr += ","
					}
				}
				str += fmt.Sprintf(" [local variables[%s]]", varStr)
			}
		}
	}

	return str
}

// Hanlable indicates whether the command can handle.
func (p *executeBaseSQL) Hanlable() bool {
	if p.cacheStmt.stmts == nil {
		return false
	}
	for _, stmt := range p.cacheStmt.stmts {
		switch v := stmt.(type) {
		case *ast.ShowStmt:
			if v.Tp == ast.ShowWarnings || v.Tp == ast.ShowErrors || v.Tp == ast.ShowSessionStates {
				return false
			}
			// case *ast.GetDiagnosticsStmt:
			// 	return false
		}
	}
	return true
}

// CloneStructure gets base context.
func (p *executeBaseSQL) CloneStructure(contextMap map[*variable.ProcedureContext]*variable.ProcedureContext) ProcedureExecPlan {
	return &executeBaseSQL{
		context:   p.context.CopyContext(contextMap),
		cacheStmt: p.cacheStmt,
	}
}

func getResType(fields []*resolve.ResultField) []*types.FieldType {
	ftypes := make([]*types.FieldType, 0, len(fields))
	for _, field := range fields {
		ftypes = append(ftypes, field.Column.FieldType.Clone())
	}
	return ftypes
}

// OpenCurs gets current row.
func (curInfo *procedurceCurInfo) OpenCurs(ctx context.Context, sctx sessionctx.Context) error {
	if curInfo.open {
		return errors.AddStack(plannererrors.ErrSpCursorAlreadyOpen)
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnProcedure)
	exec := sctx.(sqlexec.SQLExecutor)
	err := sctx.GetSessionVars().SetProcedureContext(curInfo.context)
	if err != nil {
		return err
	}
	rs, err := exec.ExecuteInternal(ctx, curInfo.selectStmt)
	if err != nil {
		return err
	}
	if rs == nil {
		return errors.New(fmt.Sprintf("CURSOR %s without RecordSet", curInfo.selectStmt))
	}
	rowContainer := chunk.NewRowContainer(getResType(rs.Fields()), sctx.GetSessionVars().MaxChunkSize)
	rowContainer.GetMemTracker().AttachTo(sctx.GetSessionVars().MemTracker)
	rowContainer.GetMemTracker().SetLabel(memory.LabelForFetchInto)
	rowContainer.GetDiskTracker().AttachTo(sctx.GetSessionVars().DiskTracker)
	rowContainer.GetDiskTracker().SetLabel(memory.LabelForFetchInto)
	if variable.EnableTmpStorageOnOOM.Load() {
		action := memory.NewActionWithPriority(rowContainer.ActionSpill(), memory.DefCursorFetchSpillPriority)
		sctx.GetSessionVars().MemTracker.FallbackOldAndSetNewAction(action)
	}
	defer func() {
		if err != nil {
			rowContainer.GetMemTracker().Detach()
			rowContainer.GetDiskTracker().Detach()
			errCloseRowContainer := rowContainer.Close()
			if errCloseRowContainer != nil {
				logutil.Logger(ctx).Error("Fail to close rowContainer in error handler. May cause resource leak",
					zap.NamedError("original-error", err), zap.NamedError("close-error", errCloseRowContainer))
			}
		}
		rs.Close()
	}()
	for {
		req := rs.NewChunk(nil)
		if err = rs.Next(ctx, req); err != nil {
			return err
		}
		rowCount := req.NumRows()
		if rowCount == 0 {
			break
		}
		err = rowContainer.Add(req)
		if err != nil {
			return err
		}
	}
	curInfo.reader = chunk.NewRowContainerReader(rowContainer)
	curInfo.fields = rs.Fields()
	curInfo.open = true
	curInfo.rowContainer = rowContainer
	return nil
}

// CloseCurs close cursor instance.
func (curInfo *procedurceCurInfo) CloseCurs() error {
	if !curInfo.open {
		return errors.Trace(plannererrors.ErrSpCursorNotOpen)
	}
	curInfo.open = false
	if curInfo.reader != nil {
		curInfo.reader.Close()
		curInfo.reader = nil
	}
	curInfo.fields = nil
	if curInfo.rowContainer != nil {
		curInfo.rowContainer.GetMemTracker().Detach()
		curInfo.rowContainer.GetDiskTracker().Detach()
		curInfo.rowContainer.Close()
		curInfo.rowContainer = nil
	}
	return nil
}

// buildCallBodyPlan Generate an execution plan based on the stored procedure structure.
func (b *PlanBuilder) buildCallBodyPlan(ctx context.Context, stmtNodes *ast.CreateProcedureInfo, collate string) error {
	b.ctx.GetSessionVars().SetInCallProcedure()
	defer b.ctx.GetSessionVars().OutCallProcedure(false)
	return b.procedureNodePlan(ctx, stmtNodes.ProcedureBody, collate)
}

// analysiStructure Generate an execution plan based on stored procedure.
func (b *PlanBuilder) getCallPlan(ctx context.Context, stmtNodes []ast.StmtNode, node *ast.CallStmt, collate string) (*ProcedurePlan, error) {
	if len(stmtNodes) > 1 {
		return nil, errors.New("Parse procedure error")
	}

	if len(stmtNodes) == 0 {
		return nil, nil
	}
	switch stmtNodes[0].(type) {
	case *ast.CreateProcedureInfo:
		plan := &ProcedurePlan{
			IfNotExists:   stmtNodes[0].(*ast.CreateProcedureInfo).IfNotExists,
			ProcedureName: stmtNodes[0].(*ast.CreateProcedureInfo).ProcedureName,
		}
		if len(node.Procedure.Args) != len(stmtNodes[0].(*ast.CreateProcedureInfo).ProcedureParam) {
			err := plannererrors.ErrSpWrongNoOfArgs.GenWithStackByArgs("PROCEDURE", node.Procedure.Schema.String()+"."+
				node.Procedure.FnName.String(), len(stmtNodes[0].(*ast.CreateProcedureInfo).ProcedureParam),
				len(node.Procedure.Args))
			return nil, err
		}
		// gets procedure params.
		params, err := b.buildCallParamPlan(ctx, stmtNodes[0].(*ast.CreateProcedureInfo), node, collate)
		if err != nil {
			return nil, err
		}
		b.procedurePlan.ProcedureParam = params

		err = b.buildCallBodyPlan(ctx, stmtNodes[0].(*ast.CreateProcedureInfo), collate)
		if err != nil {
			return nil, err
		}
		plan.ProcedureExecPlan = b.procedurePlan
		return plan, nil
	default:
		return nil, errors.Errorf("Call procedure unsupport node %T", stmtNodes)
	}
}

type procedureCheck struct {
	context   *variable.ProcedureContext
	cacheStmt *CacheAst
	err       error
}

func (p *procedureCheck) Enter(inNode ast.Node) (ast.Node, bool) {
	if p.err != nil {
		return inNode, true
	}
	return inNode, false
}

func (p *procedureCheck) Leave(inNode ast.Node) (ast.Node, bool) {
	if p.err != nil {
		return inNode, false
	}
	switch x := inNode.(type) {
	case *ast.VariableAssignment:
		if !x.CanSPVariable {
			sysVar := variable.GetSysVar(x.Name)
			if sysVar == nil {
				if variable.IsRemovedSysVar(x.Name) {
					return inNode, false
				}
				p.err = variable.ErrUnknownSystemVar.GenWithStackByArgs(x.Name)
			}
		}
		return inNode, false
	case *ast.VariableExpr:
		if x.IsSystem {
			sysVar := variable.GetSysVar(x.Name)
			if sysVar == nil {
				if variable.IsRemovedSysVar(x.Name) {
					return inNode, false
				}
				p.err = variable.ErrUnknownSystemVar.GenWithStackByArgs(x.Name)
			}
			return inNode, false
		}
	case *ast.Limit:
		v, ok := x.Count.(*ast.ProcedureVar)
		if ok {
			fieldType, _, notFind := p.context.GetProcedureVariable(v.Name.L)
			if notFind {
				p.err = plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(v.Name.O)
				return inNode, false
			}
			if !types.IsTypeInteger(fieldType.GetType()) {
				p.err = plannererrors.ErrSPvarNonintegerType.GenWithStackByArgs(v.Name.O)
				return inNode, false
			}
		}
		v, ok = x.Offset.(*ast.ProcedureVar)
		if ok {
			fieldType, _, notFind := p.context.GetProcedureVariable(v.Name.L)
			if notFind {
				p.err = plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(v.Name.O)
				return inNode, false
			}
			if !types.IsTypeInteger(fieldType.GetType()) {
				p.err = plannererrors.ErrSPvarNonintegerType.GenWithStackByArgs(v.Name.O)
				return inNode, false
			}
		}
	case *ast.SelectIntoOption:
		//https://github.com/pingcap/tidb/issues/49166
		p.cacheStmt.isInvalid = true
		if len(x.ProcedureVarList) > 0 {
			// check variable if exists.
			for _, spVar := range x.ProcedureVarList {
				spVarName := spVar.(*ast.ColumnNameExpr).Name.Name.O
				_, _, notFind := p.context.GetProcedureVariable(spVarName)
				if notFind {
					p.err = plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(spVarName)
					return inNode, false
				}
			}
		}
	case *ast.StatementInfoItem:
		if !x.IsVariable {
			_, _, notFind := p.context.GetProcedureVariable(x.Name)
			if notFind {
				p.err = plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(x.Name)
				return inNode, false
			}
		}
	case *ast.ConditionInfoItem:
		if !x.IsVariable {
			_, _, notFind := p.context.GetProcedureVariable(x.Name)
			if notFind {
				p.err = plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(x.Name)
				return inNode, false
			}
		}
	}
	return inNode, true
}

// checkProcedureStatus check set variable if exists.
func (p *executeBaseSQL) checkProcedureStatus(ctx context.Context, sctx base.PlanContext) error {
	if p.cacheStmt.isInvalid {
		err := prepareCacheAst(ctx, p.cacheStmt, sctx.(sqlexec.SQLParser))
		if err != nil {
			return err
		}
	}

	for _, stmt := range p.cacheStmt.stmts {
		switch x := stmt.(type) {
		case *ast.SetStmt:
			// check variable if exists.
			for _, vari := range x.Variables {
				if !vari.IsSystem {
					continue
				}
				if vari.CanSPVariable {
					_, _, notFind := p.context.GetProcedureVariable(vari.Name)
					if !notFind {
						if _, ok := vari.Value.(*ast.DefaultExpr); ok {
							return util.SyntaxError(errors.Errorf("%s unsupport default", vari.Name))
						}
						continue
					}
				}

				sysVar := variable.GetSysVar(vari.Name)
				if sysVar == nil {
					if variable.IsRemovedSysVar(vari.Name) {
						continue // removed vars permit parse-but-ignore
					}
					return variable.ErrUnknownSystemVar.GenWithStackByArgs(vari.Name)
				}
			}
		case *ast.ExecuteStmt:
			// do not cache execute,
			p.cacheStmt.isInvalid = true
			stmt, err := GetPreparedStmt(x, sctx.GetSessionVars())
			if err == nil {
				if stmt.InUse {
					return errors.AddStack(plannererrors.ErrPsNoRecursion)
				}
			}
		case *ast.Signal:
			_, err := signalCheck(x)
			if err != nil {
				return err
			}
		default:
			p := &procedureCheck{p.context, p.cacheStmt, nil}
			stmt.Accept(p)
			return p.err
		}
	}
	return nil
}

// procedureNodePlan Generate execution plan for procedure node.
func (b *PlanBuilder) procedureNodePlan(ctx context.Context, node ast.StmtNode, collate string) error {
	switch x := node.(type) {
	// Convert block to clean vars initialize vars (including setting default) and then execute stmt.
	case *ast.ProcedureBlock:
		label := &variable.ProcedureLabel{
			LabelType:  variable.BLOCKLABEL,
			LabelBegin: b.procedurePlan.GetLen(),
		}
		b.procedureGoSet = append(b.procedureGoSet, label)
		procedureCon := variable.NewProcedureContext(variable.BLOCKLABEL)
		procedureCon.SetRoot(b.procedureNowContext)
		b.procedureNowContext = procedureCon
		// clear this block variable
		exec := &ProcedureClearBlockVar{
			context: procedureCon,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
		// create this block variables.
		for _, varN := range x.ProcedureVars {
			err := b.newVariableVars(ctx, collate, procedureCon, varN)
			if err != nil {
				return err
			}
		}

		for _, stmt := range x.ProcedureProcStmts {
			err := b.procedureNodePlan(ctx, stmt, collate)
			if err != nil {
				return err
			}
		}
		label.LabelEnd = b.procedurePlan.GetLen()
		// sets label end for ProcedureSetGoTo
		for _, needSet := range procedureCon.NeedSet {
			if !needSet.SetDest(label, label.LabelEnd) {
				return errors.New("mismatch label ,which go to label end")
			}
		}
		// out this label.
		b.procedureGoSet = b.procedureGoSet[:len(b.procedureGoSet)-1]
		b.procedureNowContext = b.procedureNowContext.GetRoot()
	case *ast.ProcedureLabelBlock:
		// handler block label.
		labelEnd, ok := x.GetErrorStatus()
		if ok {
			return plannererrors.ErrSpLabelMismatch.GenWithStackByArgs(labelEnd)
		}
		procedureCon := variable.NewProcedureContext(variable.BLOCKLABEL)
		procedureCon.SetRoot(b.procedureNowContext)
		b.procedureNowContext = procedureCon
		// Detect that there is no duplicate label.
		if b.procedureNowContext.CheckLabel(x.LabelName) {
			return plannererrors.ErrSpLabelRedefine.GenWithStackByArgs(x.LabelName)
		}
		label := &variable.ProcedureLabel{
			Name:       x.GetLabelName(),
			LabelType:  variable.BLOCKLABEL,
			LabelBegin: b.procedurePlan.GetLen(),
		}
		procedureCon.Label = label
		err := b.procedureNodePlan(ctx, x.Block, collate)
		if err != nil {
			return err
		}
		label.LabelEnd = b.procedurePlan.GetLen()
		// sets label end dest.
		for _, needSet := range procedureCon.NeedSet {
			if !needSet.SetDest(label, b.procedurePlan.GetLen()) {
				return errors.New("mismatch label ,which go to label end")
			}
		}
		b.procedureNowContext = procedureCon.GetRoot()
	case *ast.ProcedureIfInfo:
		// handler `if ....end if`
		label := &variable.ProcedureLabel{
			LabelType:  variable.BLOCKLABEL,
			LabelBegin: b.procedurePlan.GetLen(),
		}
		b.procedureGoSet = append(b.procedureGoSet, label)
		procedureIf := variable.NewProcedureContext(variable.JUMPLABEL)
		procedureIf.SetRoot(b.procedureNowContext)
		b.procedureNowContext = procedureIf
		procedureIf.Label = label
		procedureIf.NeedSet = make([]variable.ProcedureSetGoTo, 0, 10)
		if x.IfBody == nil {
			return errors.New("tidb procdure if block is nil")
		}
		err := b.procedureNodePlan(ctx, x.IfBody, collate)
		if err != nil {
			return err
		}
		for _, needSet := range procedureIf.NeedSet {
			if !needSet.SetDest(label, b.procedurePlan.GetLen()) {
				return errors.New("mismatch label ,which go to label end")
			}
		}
		for _, ErrorSet := range procedureIf.ErrorSet {
			if !ErrorSet.SetErrorDest(label, b.procedurePlan.GetLen()) {
				return errors.New("mismatch label ,which error jump to if block end")
			}
		}
		label.LabelEnd = b.procedurePlan.GetLen()
		b.procedureGoSet = b.procedureGoSet[:len(b.procedureGoSet)-1]
		b.procedureNowContext = procedureIf.GetRoot()
	case *ast.ProcedureIfBlock:
		// handler if body `expr then ... [elseif ... then ...][else ....]`
		// Success id + 1 Failure to jump to the next conditional judgment id.
		// expr ==> x.IfExpr
		// then stmts ==> x.ProcedureIfStmts (exec end)
		// else ==> ast.ProcedureElseBlock / else if ==> ProcedureElseIfBlock
		label := b.procedureGoSet[len(b.procedureGoSet)-1]
		exec := &ProcedureIfGo{
			context: b.procedureNowContext,
			label:   label,
			expr:    &CacheExpr{expr: ExprNodeToString(x.IfExpr), isInvalid: true},
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
		// handler stmts
		for _, stmt := range x.ProcedureIfStmts {
			err := b.procedureNodePlan(ctx, stmt, collate)
			if err != nil {
				return err
			}
		}
		// Execute only one branch that satisfies the condition.
		goEnd := &ProcedureGoToEnd{
			label: label,
		}
		b.procedureNowContext.NeedSet = append(b.procedureNowContext.NeedSet, goEnd)
		b.procedureNowContext.ErrorSet = append(b.procedureNowContext.ErrorSet, exec)
		// handler `else .../ elseif ...`
		if x.ProcedureElseStmt != nil {
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, goEnd)
		}
		exec.dest = b.procedurePlan.GetLen()
		if x.ProcedureElseStmt != nil {
			err := b.procedureNodePlan(ctx, x.ProcedureElseStmt, collate)
			if err != nil {
				return err
			}
		}
	case *ast.ProcedureElseIfBlock:
		err := b.procedureNodePlan(ctx, x.ProcedureIfStmt, collate)
		if err != nil {
			return err
		}
	case *ast.ProcedureElseBlock:
		for _, stmt := range x.ProcedureIfStmts {
			err := b.procedureNodePlan(ctx, stmt, collate)
			if err != nil {
				return err
			}
		}

	case *ast.ProcedureWhileStmt:
		label := &variable.ProcedureLabel{
			LabelType:  variable.LOOPLABEL,
			LabelBegin: b.procedurePlan.GetLen(),
		}
		b.procedureGoSet = append(b.procedureGoSet, label)
		procedureWhile := variable.NewProcedureContext(variable.LOOPLABEL)
		procedureWhile.SetRoot(b.procedureNowContext)
		b.procedureNowContext = procedureWhile
		exec := &ProcedureIfGo{
			context: b.procedureNowContext,
			label:   label,
			expr:    &CacheExpr{expr: ExprNodeToString(x.Condition), isInvalid: true},
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
		procedureWhile.NeedSet = append(procedureWhile.NeedSet, exec)
		procedureWhile.ErrorSet = append(procedureWhile.ErrorSet, exec)
		for _, stmt := range x.Body {
			err := b.procedureNodePlan(ctx, stmt, collate)
			if err != nil {
				return err
			}
		}
		toBegin := &ProcedureGoToStart{
			dest:  label.LabelBegin,
			label: label,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, toBegin)
		label.LabelEnd = b.procedurePlan.GetLen()
		for _, needSet := range procedureWhile.NeedSet {
			if !needSet.SetDest(label, b.procedurePlan.GetLen()) {
				return errors.New("mismatch label ,which go to label end")
			}
		}
		for _, ErrorSet := range procedureWhile.ErrorSet {
			if !ErrorSet.SetErrorDest(label, b.procedurePlan.GetLen()) {
				return errors.New("mismatch label ,which error jump to if block end")
			}
		}
		b.procedureNowContext = procedureWhile.GetRoot()
		b.procedureGoSet = b.procedureGoSet[:len(b.procedureGoSet)-1]
	case *ast.ProcedureRepeatStmt:
		label := &variable.ProcedureLabel{
			LabelType:  variable.LOOPLABEL,
			LabelBegin: b.procedurePlan.GetLen(),
		}
		b.procedureGoSet = append(b.procedureGoSet, label)
		procedureRepeat := variable.NewProcedureContext(variable.LOOPLABEL)
		procedureRepeat.SetRoot(b.procedureNowContext)
		b.procedureNowContext = procedureRepeat

		for _, stmt := range x.Body {
			err := b.procedureNodePlan(ctx, stmt, collate)
			if err != nil {
				return err
			}
		}
		exec := &ProcedureIfGo{
			context: b.procedureNowContext,
			label:   label,
			expr:    &CacheExpr{expr: ExprNodeToString(x.Condition), isInvalid: true},
			dest:    label.LabelBegin,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
		exec.errorDest = uint(len(b.procedurePlan.ProcedureCommandList))
		b.procedureNowContext = procedureRepeat.GetRoot()
		b.procedureGoSet = b.procedureGoSet[:len(b.procedureGoSet)-1]

	case *ast.ProcedureLabelLoop:
		labelEnd, ok := x.GetErrorStatus()
		if ok {
			return plannererrors.ErrSpLabelMismatch.GenWithStackByArgs(labelEnd)
		}
		procedureCon := variable.NewProcedureContext(variable.LOOPLABEL)
		procedureCon.SetRoot(b.procedureNowContext)
		b.procedureNowContext = procedureCon
		if b.procedureNowContext.CheckLabel(x.LabelName) {
			return plannererrors.ErrSpLabelRedefine.GenWithStackByArgs(x.LabelName)
		}
		label := &variable.ProcedureLabel{
			Name:       x.GetLabelName(),
			LabelType:  variable.LOOPLABEL,
			LabelBegin: b.procedurePlan.GetLen(),
		}
		procedureCon.Label = label
		err := b.procedureNodePlan(ctx, x.Block, collate)
		if err != nil {
			return err
		}
		label.LabelEnd = b.procedurePlan.GetLen()
		for _, needSet := range procedureCon.NeedSet {
			if !needSet.SetDest(label, b.procedurePlan.GetLen()) {
				return errors.New("mismatch label ,which go to label end")
			}
		}
		b.procedureNowContext = procedureCon.GetRoot()
	case *ast.ProcedureJump:
		procedureCon := b.procedureNowContext
		toLabel := procedureCon.FindLabel(x.Name, x.IsLeave)
		if toLabel == nil {
			var activeName string
			if x.IsLeave {
				activeName = "LEAVE"
			} else {
				activeName = "ITERATE"
			}
			return plannererrors.ErrSpLilabelMismatch.GenWithStackByArgs(activeName, x.Name)
		}

		if x.IsLeave {
			goDest := &ProcedureGoToEnd{
				label: toLabel.Label,
			}
			toLabel.NeedSet = append(toLabel.NeedSet, goDest)
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, goDest)
		} else {
			goDest := &ProcedureGoToStart{
				label: toLabel.Label,
				dest:  toLabel.Label.LabelBegin,
			}
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, goDest)
		}
	case *ast.ProcedureOpenCur:
		procedureCon := b.procedureNowContext
		_, err := procedureCon.FindCurs(x.CurName)
		if err != nil {
			return err
		}
		exec := &OpenProcedurceCursor{
			curName: x.CurName,
			context: procedureCon,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
	case *ast.ProcedureCloseCur:
		procedureCon := b.procedureNowContext
		_, err := procedureCon.FindCurs(x.CurName)
		if err != nil {
			return err
		}
		exec := &ProcedurceCursorClose{
			curName: x.CurName,
			context: procedureCon,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
	case *ast.ProcedureFetchInto:
		procedureCon := b.procedureNowContext
		_, err := procedureCon.FindCurs(x.CurName)
		if err != nil {
			return err
		}
		vars := make([]string, 0, len(x.Variables))
		for _, variable := range x.Variables {
			variable = strings.ToLower(variable)
			vars = append(vars, strings.ToLower(variable))
			if !procedureCon.FindVarName(variable) {
				return plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(variable)
			}
		}
		exec := &ProcedurceFetchInto{
			curName: x.CurName,
			context: procedureCon,
			vars:    vars,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
	case *ast.SimpleCaseStmt:
		procedureCon := b.procedureNowContext
		simpleCase := &procedureSimpleCase{
			context:   procedureCon,
			condition: &CacheExpr{expr: ExprNodeToString(x.Condition), isInvalid: true},
			collate:   collate,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, simpleCase)
		simpleCase.dests = make([]conditionDest, 0, len(x.WhenCases))
		needSets := make([]variable.ProcedureSetGoTo, 0, len(x.WhenCases))
		for _, caseCon := range x.WhenCases {
			dest := conditionDest{
				condition: &CacheExpr{expr: ExprNodeToString(caseCon.Expr), isInvalid: true},
				dest:      uint(len(b.procedurePlan.ProcedureCommandList)),
			}
			simpleCase.dests = append(simpleCase.dests, dest)
			for _, stmt := range caseCon.ProcedureStmts {
				err := b.procedureNodePlan(ctx, stmt, collate)
				if err != nil {
					return err
				}
			}
			goToEnd := &ProcedureGoToEnd{}
			needSets = append(needSets, goToEnd)
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, goToEnd)
		}
		simpleCase.elseDest = uint(len(b.procedurePlan.ProcedureCommandList))
		if len(x.ElseCases) != 0 {
			simpleCase.hasElse = true
			for _, stmt := range x.ElseCases {
				err := b.procedureNodePlan(ctx, stmt, collate)
				if err != nil {
					return err
				}
			}
		}
		end := uint(len(b.procedurePlan.ProcedureCommandList))
		simpleCase.errDest = end
		for _, needSet := range needSets {
			needSet.SetDest(nil, end)
		}
	case *ast.SearchCaseStmt:
		procedureCon := b.procedureNowContext
		simpleCase := &procedureSearchCase{
			context: procedureCon,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, simpleCase)
		simpleCase.dests = make([]conditionDest, 0, len(x.WhenCases))
		needSets := make([]variable.ProcedureSetGoTo, 0, len(x.WhenCases))
		for _, caseCon := range x.WhenCases {
			dest := conditionDest{
				condition: &CacheExpr{expr: ExprNodeToString(caseCon.Expr), isInvalid: true},
				dest:      uint(len(b.procedurePlan.ProcedureCommandList)),
			}
			simpleCase.dests = append(simpleCase.dests, dest)
			for _, stmt := range caseCon.ProcedureStmts {
				err := b.procedureNodePlan(ctx, stmt, collate)
				if err != nil {
					return err
				}
			}
			goToEnd := &ProcedureGoToEnd{}
			needSets = append(needSets, goToEnd)
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, goToEnd)
		}
		simpleCase.elseDest = uint(len(b.procedurePlan.ProcedureCommandList))
		if len(x.ElseCases) != 0 {
			simpleCase.hasElse = true
			for _, stmt := range x.ElseCases {
				err := b.procedureNodePlan(ctx, stmt, collate)
				if err != nil {
					return err
				}
			}
		}
		end := uint(len(b.procedurePlan.ProcedureCommandList))
		simpleCase.errDest = end
		for _, needSet := range needSets {
			needSet.SetDest(nil, end)
		}
	case *ast.ProcedureLoopStmt:
		label := &variable.ProcedureLabel{
			LabelType:  variable.LOOPLABEL,
			LabelBegin: b.procedurePlan.GetLen(),
		}
		procedureLoop := variable.NewProcedureContext(variable.LOOPLABEL)
		procedureLoop.SetRoot(b.procedureNowContext)
		b.procedureNowContext = procedureLoop
		for _, stmt := range x.Body {
			err := b.procedureNodePlan(ctx, stmt, collate)
			if err != nil {
				return err
			}
		}
		toBegin := &ProcedureGoToStart{
			dest:  label.LabelBegin,
			label: label,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, toBegin)
		b.procedureNowContext = procedureLoop.GetRoot()
	default:
		exec := &executeBaseSQL{
			context:   b.procedureNowContext,
			cacheStmt: &CacheAst{node.Text(), false, []ast.StmtNode{node}},
		}
		err := exec.checkProcedureStatus(ctx, b.ctx)
		if err != nil {
			return err
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
	}
	return nil
}

// newVariableVars Initialize stored procedure local variables（including Cursor, handler, data variables）from the ast tree.
func (b *PlanBuilder) newVariableVars(ctx context.Context, collate string, context *variable.ProcedureContext, decl ast.DeclNode) error {
	switch x := decl.(type) {
	// Handling Stored Procedure cursor definitions.
	case *ast.ProcedureCursor:
		// The definition of handler cannot appear before the definition of Cursor.
		if len(context.Handles) >= 1 {
			return errors.Trace(plannererrors.ErrSpCursorAfterHandler)
		}
		for _, cursor := range context.Cursors {
			// Cursor names in this block cannot be the same.
			if cursor.GetName() == x.CurName {
				return plannererrors.ErrSpDupCurs.GenWithStackByArgs(x.CurName)
			}
		}
		cursor := &procedurceCurInfo{
			curName:    x.CurName,
			selectStmt: x.Selectstring.Text(),
			context:    b.procedureNowContext,
		}
		context.Cursors = append(context.Cursors, cursor)
		exec := &ResetProcedurceCursor{
			curName: x.CurName,
			context: context,
		}
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
	// Handling Stored Procedure data variables.
	case *ast.ProcedureDecl:
		// procedure data variable must at begin.
		if len(context.Cursors) >= 1 || len(context.Handles) >= 1 {
			return errors.Trace(plannererrors.ErrSpVarcondAfterCurshndlr)
		}
		var err error
		// Sets data type default length and collation.
		x.DeclType, err = b.setDefaultLengthAndCharset(x.DeclType, collate)
		if err != nil {
			return err
		}

		for _, name := range x.DeclNames {
			name = strings.ToLower(name)
			if b.procedureNowContext.CheckVarName(name) {
				return plannererrors.ErrSpDupVar.GenWithStackByArgs(name)
			}
			vari := variable.NewProcedureVars(name, x.DeclType)
			context.Vars = append(context.Vars, vari)
			defaultString := ""
			if x.DeclDefault != nil {
				if x.DeclDefault.Text() == "" {
					var b []byte
					bf := bytes.NewBuffer(b)
					x.DeclDefault.Restore(&format.RestoreCtx{
						Flags: format.DefaultRestoreFlags,
						In:    bf,
					})
					x.DeclDefault.SetText(nil, bf.String())
					defaultString = bf.String()
				} else {
					defaultString = x.DeclDefault.Text()
				}
			}

			exec := &UpdateVariables{
				name:     name,
				expr:     defaultString,
				context:  context,
				declType: x.DeclType,
			}
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
		}
	// Handling Stored Procedure handler error variables.
	case *ast.ProcedureErrorControl:
		handleProcedureCon := variable.NewProcedureContext(variable.HANDLELABEL)
		handleProcedureCon.SetRoot(context)

		label := &variable.ProcedureLabel{
			LabelType:  variable.HANDLELABEL,
			LabelBegin: uint(len(b.procedurePlan.ProcedureCommandList)),
		}
		b.procedureGoSet = append(b.procedureGoSet, label)

		// default skip handler command.
		// When there is no error, the handler code is not executed, so skip this code directly.
		goDest := &ProcedureGoToEnd{
			label: label,
		}
		handleProcedureCon.NeedSet = append(handleProcedureCon.NeedSet, goDest)
		b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, goDest)

		base := variable.ProcedureHandleBase{
			Handle:        x.ControlHandle,
			Operate:       uint(len(b.procedurePlan.ProcedureCommandList)),
			HandleContext: handleProcedureCon,
		}
		for _, errorInfo := range x.ErrorCon {
			switch v := errorInfo.(type) {
			// handler error code
			case *ast.ProcedureErrorVal:
				if v.ErrorNum == 0 {
					return plannererrors.ErrWrongValue.GenWithStackByArgs("CONDITION", "0")
				}
				errCon := &variable.ProcedureHandleCode{
					Codes:               v.ErrorNum,
					ProcedureHandleBase: base,
				}
				for _, handle := range context.Handles {
					code, ok := handle.(*variable.ProcedureHandleCode)
					if ok {
						if code.Codes == v.ErrorNum {
							return errors.AddStack(plannererrors.ErrSpDupHandler)
						}
					}
				}
				context.Handles = append(context.Handles, errCon)
			// handler error status (SQLWARNING\NOT FOUND\SQLEXCEPTION)
			case *ast.ProcedureErrorCon:
				errCon := &variable.ProcedureHandleState{
					State:               v.ErrorCon,
					ProcedureHandleBase: base,
				}
				for _, handle := range context.Handles {
					code, ok := handle.(*variable.ProcedureHandleState)
					if ok {
						if code.State == v.ErrorCon {
							return errors.AddStack(plannererrors.ErrSpDupHandler)
						}
					}
				}
				context.Handles = append(context.Handles, errCon)
			// handler SQLSTATE + string
			case *ast.ProcedureErrorState:
				if !IsSqlstateValid(v.CodeStatus) || IsSQLStateCompletion(v.CodeStatus) {
					return plannererrors.ErrSpBadSQLstate.GenWithStackByArgs(v.CodeStatus)
				}
				errCon := &variable.ProcedureHandleStatus{
					Status:              v.CodeStatus,
					ProcedureHandleBase: base,
				}
				for _, handle := range context.Handles {
					code, ok := handle.(*variable.ProcedureHandleStatus)
					if ok {
						if code.Status == v.CodeStatus {
							return errors.AddStack(plannererrors.ErrSpDupHandler)
						}
					}
				}
				context.Handles = append(context.Handles, errCon)
			default:
				return errors.Errorf("Error control unsupport node %T", errorInfo)
			}
		}

		b.procedureNowContext = handleProcedureCon
		if x.ControlHandle == ast.PROCEDUR_CONTINUE {
			// if continue ,need save current execution pointer.
			exec := &ProcedureSaveIP{
				context: handleProcedureCon,
				dest:    uint(len(b.procedurePlan.ProcedureCommandList)),
			}
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
		} else {
			// if exit ,do noting,placeholder only.
			exec := &ProcedureNoNeedSave{
				context: handleProcedureCon,
				dest:    uint(len(b.procedurePlan.ProcedureCommandList)),
			}
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
		}

		err := b.procedureNodePlan(ctx, x.Operate, collate)
		if err != nil {
			return err
		}

		b.procedureGoSet = b.procedureGoSet[:len(b.procedureGoSet)-1]
		if x.ControlHandle == ast.PROCEDUR_CONTINUE {
			// recover execute ,after handler block.
			exec := &ProcedureOutputIP{
				context: handleProcedureCon,
			}
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, exec)
		} else {
			// go to this block end after handler.
			goDest := &ProcedureGoToEndWithOutStmt{
				label:   b.procedureGoSet[len(b.procedureGoSet)-1],
				context: handleProcedureCon,
			}
			b.procedurePlan.ProcedureCommandList = append(b.procedurePlan.ProcedureCommandList, goDest)
			context.NeedSet = append(context.NeedSet, goDest)
		}
		label.LabelEnd = uint(len(b.procedurePlan.ProcedureCommandList))
		// sets this block end,if need.
		for _, needSet := range handleProcedureCon.NeedSet {
			if !needSet.SetDest(label, b.procedurePlan.GetLen()) {
				return errors.New("mismatch label ,which go to label end")
			}
		}
		b.procedureNowContext = context
	}
	return nil
}

// GetExprValue gets expr value by execute select expr.
// (*PlanBuilder).rewrite expr ==> expr.Eval() cannot handle a in (select * from t1)
// To Do: Gets results from logical plan.
func GetExprValue(ctx context.Context, node *CacheExpr, tp *types.FieldType, sctx sessionctx.Context, name string) (types.Datum, error) {
	if node == nil || node.expr == "" {
		// if node is null ,sets default value is NULL.
		datum := types.NewDatum("")
		datum.SetNull()
		return datum.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), tp)
	}
	datum, err := getExprValue(ctx, node, sctx)
	if err != nil {
		return types.NewDatum(""), err
	}
	col := &model.ColumnInfo{Name: pmodel.NewCIStr(name), FieldType: *tp.Clone()}
	return table.CastValue(sctx, *datum.Clone(), col, false, false)
}

func getExprValue(ctx context.Context, node *CacheExpr, sctx sessionctx.Context) (d types.Datum, err error) {
	var rows []chunk.Row
	var fields []*resolve.ResultField
	defer func() {
		if !variable.TiDBEnableSPAstReuse.Load() {
			node.isInvalid = true
		}
		if err != nil {
			node.isInvalid = true
		}
	}()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnProcedure)
	err = prepareCacheAst(ctx, node, sctx.(sqlexec.SQLParser))
	if err != nil {
		return types.NewDatum(nil), err
	}
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	//execute as select expr.
	rows, fields, err = exec.ExecRestrictedStmt(ctx, node.stmt, sqlexec.ExecOptionUseCurSession)
	if err != nil {
		return types.NewDatum(nil), err
	}
	if len(rows) > 1 {
		return types.NewDatum(nil), plannererrors.ErrOperandColumns.GenWithStackByArgs(1)
	}
	// reset error ignore status,ensure that type conversion failures can be reported normally.
	ResetCallStatus(sctx)
	for _, row := range rows {
		if row.Len() > 1 {
			return types.NewDatum(nil), plannererrors.ErrSubqueryMoreThan1Row.GenWithStackByArgs(1)
		}
		if row.Len() == 1 {
			if row.IsNull(0) {
				break
			}
			r := row.GetDatum(0, fields[0].Column.FieldType.Clone())
			return r, nil
		}
	}
	return types.NewDatum(nil), nil
}

// ResetCallStatus reset call execute sc status.
func ResetCallStatus(sctx sessionctx.Context) {
	sc := sctx.GetSessionVars().StmtCtx
	vars := sctx.GetSessionVars()
	ResetFlagBySQLMode(sc, vars)
}

// ResetFlagBySQLMode resets the flag associated with sql-mode.
func ResetFlagBySQLMode(sc *stmtctx.StatementContext, vars *variable.SessionVars) {
	strictSQLMode := vars.SQLMode.HasStrictMode()
	errLevels := sc.ErrLevels()
	errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !strictSQLMode)
	errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(false, !strictSQLMode)

	sc.SetTypeFlags(sc.TypeFlags().
		WithTruncateAsWarning(!strictSQLMode).
		WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
		WithIgnoreTruncateErr(false).
		WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() ||
			!strictSQLMode || vars.SQLMode.HasAllowInvalidDatesMode()))
}

// UpdateVariableVar updates the variable value.
func UpdateVariableVar(name string, val types.Datum, sessVars *variable.SessionVars) error {
	pCon := sessVars.GetProcedureContext()
	if pCon == nil {
		return plannererrors.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	pCon.Lock.Lock()
	defer pCon.Lock.Unlock()
	name = strings.ToLower(name)
	if sessVars.CheckUnchangableVariable(name, false) {
		return errors.Errorf("the limit parameter '%s' is not supported to be modified in the same sql", name)
	}
	pContext := pCon.Context
	col := &model.ColumnInfo{Name: pmodel.NewCIStr(name)}
	for {
		if pContext == nil {
			break
		}
		for _, procedureVar := range pContext.Vars {
			if procedureVar.GetName() == name {
				col.FieldType = *procedureVar.GetFieldType().Clone()
				varVar, err := table.CastValue(sessVars, val, col, false, false)
				if err != nil {
					varVar.SetNull()
				}
				procedureVar.SetVar(varVar)
				return err
			}
		}
		pContext = pContext.GetRoot()
	}

	return plannererrors.ErrUnknownSystemVar.GenWithStackByArgs(name)
}

type varinfo struct {
	name string
	data string
}

type procedureGetLocalVariable struct {
	context *variable.ProcedureContext
	ctx     sessionctx.Context
	vars    []*varinfo
	err     error
}

func (p *procedureGetLocalVariable) Enter(inNode ast.Node) (ast.Node, bool) {
	if p.err != nil {
		return inNode, true
	}
	return inNode, false
}

func (p *procedureGetLocalVariable) Leave(inNode ast.Node) (ast.Node, bool) {
	if p.err != nil {
		return inNode, false
	}
	switch x := inNode.(type) {
	case *ast.ColumnName:
		if x.Table.String() == "" {
			_, d, notFind := p.context.GetProcedureVariable(x.Name.L)
			if notFind {
				return inNode, true
			}
			tp := types.NewFieldType(mysql.TypeBlob)
			dStr, err := d.ConvertTo(p.ctx.GetSessionVars().StmtCtx.TypeCtx(), tp)
			if err != nil {
				logutil.BgLogger().Warn(fmt.Sprintf("local variable %s conver to string fail", x.Name.L), zap.Error(err))
			}
			p.vars = append(p.vars, &varinfo{x.Name.L, dStr.GetBinaryStringEncoded()})
		}
	}
	return inNode, true
}

func getlocalVariableFromAst(stmt ast.StmtNode, context *variable.ProcedureContext, ctx sessionctx.Context) []*varinfo {
	if stmt == nil {
		return nil
	}
	check := &procedureGetLocalVariable{context: context, ctx: ctx}
	stmt.Accept(check)
	return check.vars
}
