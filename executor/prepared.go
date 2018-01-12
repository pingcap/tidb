// Copyright 2015 PingCAP, Inc.
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

package executor

import (
	"math"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	goctx "golang.org/x/net/context"
)

var (
	_ Executor = &DeallocateExec{}
	_ Executor = &ExecuteExec{}
	_ Executor = &PrepareExec{}
)

type paramMarkerSorter struct {
	markers []*ast.ParamMarkerExpr
}

func (p *paramMarkerSorter) Len() int {
	return len(p.markers)
}

func (p *paramMarkerSorter) Less(i, j int) bool {
	return p.markers[i].Offset < p.markers[j].Offset
}

func (p *paramMarkerSorter) Swap(i, j int) {
	p.markers[i], p.markers[j] = p.markers[j], p.markers[i]
}

type paramMarkerExtractor struct {
	markers []*ast.ParamMarkerExpr
}

func (e *paramMarkerExtractor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (e *paramMarkerExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if x, ok := in.(*ast.ParamMarkerExpr); ok {
		e.markers = append(e.markers, x)
	}
	return in, true
}

// PrepareExec represents a PREPARE executor.
type PrepareExec struct {
	baseExecutor

	is      infoschema.InfoSchema
	name    string
	sqlText string

	ID         uint32
	ParamCount int
	Fields     []*ast.ResultField
}

// NewPrepareExec creates a new PrepareExec.
func NewPrepareExec(ctx context.Context, is infoschema.InfoSchema, sqlTxt string) *PrepareExec {
	return &PrepareExec{
		baseExecutor: newBaseExecutor(nil, ctx),
		is:           is,
		sqlText:      sqlTxt,
	}
}

// Next implements the Executor Next interface.
func (e *PrepareExec) Next(goCtx goctx.Context) (Row, error) {
	return nil, errors.Trace(e.DoPrepare())
}

// NextChunk implements the Executor NextChunk interface.
func (e *PrepareExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	return errors.Trace(e.DoPrepare())
}

// DoPrepare prepares the statement, it can be called multiple times without side effect.
func (e *PrepareExec) DoPrepare() error {
	vars := e.ctx.GetSessionVars()
	if e.ID != 0 {
		// Must be the case when we retry a prepare.
		// Make sure it is idempotent.
		_, ok := vars.PreparedStmts[e.ID]
		if ok {
			return nil
		}
	}
	charset, collation := vars.GetCharsetInfo()
	var (
		stmts []ast.StmtNode
		err   error
	)
	if sqlParser, ok := e.ctx.(sqlexec.SQLParser); ok {
		stmts, err = sqlParser.ParseSQL(e.sqlText, charset, collation)
	} else {
		stmts, err = parser.New().Parse(e.sqlText, charset, collation)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if len(stmts) != 1 {
		return ErrPrepareMulti
	}
	stmt := stmts[0]
	if _, ok := stmt.(ast.DDLNode); ok {
		return ErrPrepareDDL
	}
	var extractor paramMarkerExtractor
	stmt.Accept(&extractor)
	err = plan.Preprocess(e.ctx, stmt, e.is, true)
	if err != nil {
		return errors.Trace(err)
	}

	// The parameter markers are appended in visiting order, which may not
	// be the same as the position order in the query string. We need to
	// sort it by position.
	sorter := &paramMarkerSorter{markers: extractor.markers}
	sort.Sort(sorter)
	e.ParamCount = len(sorter.markers)
	for i := 0; i < e.ParamCount; i++ {
		sorter.markers[i].Order = i
	}
	prepared := &plan.Prepared{
		Stmt:          stmt,
		Params:        sorter.markers,
		SchemaVersion: e.is.SchemaMetaVersion(),
	}
	prepared.UseCache = plan.PreparedPlanCacheEnabled && (vars.ImportingData || plan.Cacheable(stmt))

	// We try to build the real statement of preparedStmt.
	for i := range prepared.Params {
		prepared.Params[i].SetDatum(types.NewIntDatum(0))
	}
	var p plan.Plan
	p, err = plan.BuildLogicalPlan(e.ctx, stmt, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := stmt.(*ast.SelectStmt); ok {
		e.Fields = schema2ResultFields(p.Schema(), vars.CurrentDB)
	}
	if e.ID == 0 {
		e.ID = vars.GetNextPreparedStmtID()
	}
	if e.name != "" {
		vars.PreparedStmtNameToID[e.name] = e.ID
	}
	vars.PreparedStmts[e.ID] = prepared
	return nil
}

// ExecuteExec represents an EXECUTE executor.
// It cannot be executed by itself, all it needs to do is to build
// another Executor from a prepared statement.
type ExecuteExec struct {
	baseExecutor

	is        infoschema.InfoSchema
	name      string
	usingVars []expression.Expression
	id        uint32
	stmtExec  Executor
	stmt      ast.StmtNode
	plan      plan.Plan
}

// Next implements the Executor Next interface.
// It will never be called.
func (e *ExecuteExec) Next(goCtx goctx.Context) (Row, error) {
	return nil, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *ExecuteExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	return nil
}

// Build builds a prepared statement into an executor.
// After Build, e.StmtExec will be used to do the real execution.
func (e *ExecuteExec) Build() error {
	var err error
	if IsPointGetWithPKOrUniqueKeyByAutoCommit(e.ctx, e.plan) {
		err = e.ctx.InitTxnWithStartTS(math.MaxUint64)
	} else {
		err = e.ctx.ActivePendingTxn()
	}
	if err != nil {
		return errors.Trace(err)
	}
	b := newExecutorBuilder(e.ctx, e.is, kv.PriorityNormal)
	stmtExec := b.build(e.plan)
	if b.err != nil {
		return errors.Trace(b.err)
	}
	e.stmtExec = stmtExec
	ResetStmtCtx(e.ctx, e.stmt)
	stmtCount(e.stmt, e.plan, e.ctx.GetSessionVars().InRestrictedSQL)
	return nil
}

// DeallocateExec represent a DEALLOCATE executor.
type DeallocateExec struct {
	baseExecutor

	Name string
}

// Next implements the Executor Next interface.
func (e *DeallocateExec) Next(goCtx goctx.Context) (Row, error) {
	return nil, errors.Trace(e.run(goCtx))
}

// NextChunk implements the Executor NextChunk interface.
func (e *DeallocateExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	return errors.Trace(e.run(goCtx))
}

func (e *DeallocateExec) run(goCtx goctx.Context) error {
	vars := e.ctx.GetSessionVars()
	id, ok := vars.PreparedStmtNameToID[e.Name]
	if !ok {
		return errors.Trace(plan.ErrStmtNotFound)
	}
	delete(vars.PreparedStmtNameToID, e.Name)
	delete(vars.PreparedStmts, id)
	return nil
}

// CompileExecutePreparedStmt compiles a session Execute command to a stmt.Statement.
func CompileExecutePreparedStmt(ctx context.Context, ID uint32, args ...interface{}) (ast.Statement, error) {
	execStmt := &ast.ExecuteStmt{ExecID: ID}
	execStmt.UsingVars = make([]ast.ExprNode, len(args))
	for i, val := range args {
		execStmt.UsingVars[i] = ast.NewValueExpr(val)
	}
	is := GetInfoSchema(ctx)
	execPlan, err := plan.Optimize(ctx, execStmt, is)
	if err != nil {
		return nil, errors.Trace(err)
	}

	stmt := &ExecStmt{
		InfoSchema: GetInfoSchema(ctx),
		Plan:       execPlan,
		StmtNode:   execStmt,
		Ctx:        ctx,
	}
	if prepared, ok := ctx.GetSessionVars().PreparedStmts[ID].(*plan.Prepared); ok {
		stmt.Text = prepared.Stmt.Text()
	}
	return stmt, nil
}

// ResetStmtCtx resets the StmtContext.
// Before every execution, we must clear statement context.
func ResetStmtCtx(ctx context.Context, s ast.StmtNode) {
	sessVars := ctx.GetSessionVars()
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = sessVars.GetTimeZone()

	switch stmt := s.(type) {
	case *ast.UpdateStmt:
		sc.IgnoreTruncate = false
		sc.OverflowAsWarning = false
		sc.TruncateAsWarning = !sessVars.StrictSQLMode || stmt.IgnoreErr
		sc.InUpdateOrDeleteStmt = true
		sc.DividedByZeroAsWarning = stmt.IgnoreErr
		sc.IgnoreZeroInDate = !sessVars.StrictSQLMode || stmt.IgnoreErr
	case *ast.DeleteStmt:
		sc.IgnoreTruncate = false
		sc.OverflowAsWarning = false
		sc.TruncateAsWarning = !sessVars.StrictSQLMode || stmt.IgnoreErr
		sc.InUpdateOrDeleteStmt = true
		sc.DividedByZeroAsWarning = stmt.IgnoreErr
		sc.IgnoreZeroInDate = !sessVars.StrictSQLMode || stmt.IgnoreErr
	case *ast.InsertStmt:
		sc.IgnoreTruncate = false
		sc.TruncateAsWarning = !sessVars.StrictSQLMode || stmt.IgnoreErr
		sc.InInsertStmt = true
		sc.DividedByZeroAsWarning = stmt.IgnoreErr
		sc.IgnoreZeroInDate = !sessVars.StrictSQLMode || stmt.IgnoreErr
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		// Make sure the sql_mode is strict when checking column default value.
		sc.IgnoreTruncate = false
		sc.OverflowAsWarning = false
		sc.TruncateAsWarning = false
	case *ast.LoadDataStmt:
		sc.IgnoreTruncate = false
		sc.OverflowAsWarning = false
		sc.TruncateAsWarning = !sessVars.StrictSQLMode
	case *ast.SelectStmt:
		sc.InSelectStmt = true

		// see https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-strict
		// said "For statements such as SELECT that do not change data, invalid values
		// generate a warning in strict mode, not an error."
		// and https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		sc.OverflowAsWarning = true

		// Return warning for truncate error in selection.
		sc.IgnoreTruncate = false
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		if opts := stmt.SelectStmtOpts; opts != nil {
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.SQLCache
		}
		sc.PadCharToFullLength = ctx.GetSessionVars().SQLMode.HasPadCharToFullLengthMode()
	default:
		sc.IgnoreTruncate = true
		sc.OverflowAsWarning = false
		if show, ok := s.(*ast.ShowStmt); ok {
			if show.Tp == ast.ShowWarnings {
				sc.InShowWarning = true
				sc.SetWarnings(sessVars.StmtCtx.GetWarnings())
			}
		}
		sc.IgnoreZeroInDate = true
	}
	if sessVars.LastInsertID > 0 {
		sessVars.PrevLastInsertID = sessVars.LastInsertID
		sessVars.LastInsertID = 0
	}
	sessVars.ResetPrevAffectedRows()
	sessVars.InsertID = 0
	sessVars.StmtCtx = sc
}
