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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"golang.org/x/net/context"
)

var (
	_ Executor = &DeallocateExec{}
	_ Executor = &ExecuteExec{}
	_ Executor = &PrepareExec{}
)

type paramMarkerSorter struct {
	markers []ast.ParamMarkerExpr
}

func (p *paramMarkerSorter) Len() int {
	return len(p.markers)
}

func (p *paramMarkerSorter) Less(i, j int) bool {
	return p.markers[i].(*driver.ParamMarkerExpr).Offset < p.markers[j].(*driver.ParamMarkerExpr).Offset
}

func (p *paramMarkerSorter) Swap(i, j int) {
	p.markers[i], p.markers[j] = p.markers[j], p.markers[i]
}

type paramMarkerExtractor struct {
	markers []ast.ParamMarkerExpr
}

func (e *paramMarkerExtractor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (e *paramMarkerExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if x, ok := in.(*driver.ParamMarkerExpr); ok {
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
func NewPrepareExec(ctx sessionctx.Context, is infoschema.InfoSchema, sqlTxt string) *PrepareExec {
	base := newBaseExecutor(ctx, nil, "PrepareStmt")
	base.initCap = chunk.ZeroCapacity
	return &PrepareExec{
		baseExecutor: base,
		is:           is,
		sqlText:      sqlTxt,
	}
}

// Next implements the Executor Next interface.
func (e *PrepareExec) Next(ctx context.Context, chk *chunk.Chunk) error {
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
	err = ResetContextOfStmt(e.ctx, stmt)
	if err != nil {
		return err
	}
	var extractor paramMarkerExtractor
	stmt.Accept(&extractor)

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	if len(extractor.markers) > math.MaxUint16 {
		return ErrPsManyParam
	}

	err = plannercore.Preprocess(e.ctx, stmt, e.is, true)
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
		sorter.markers[i].SetOrder(i)
	}
	prepared := &ast.Prepared{
		Stmt:          stmt,
		Params:        sorter.markers,
		SchemaVersion: e.is.SchemaMetaVersion(),
	}
	prepared.UseCache = plannercore.PreparedPlanCacheEnabled() && (vars.LightningMode || plannercore.Cacheable(stmt))

	// We try to build the real statement of preparedStmt.
	for i := range prepared.Params {
		prepared.Params[i].(*driver.ParamMarkerExpr).Datum = types.NewIntDatum(0)
	}
	var p plannercore.Plan
	p, err = plannercore.BuildLogicalPlan(e.ctx, stmt, e.is)
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
	return vars.AddPreparedStmt(e.ID, prepared)
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
	plan      plannercore.Plan
}

// Next implements the Executor Next interface.
func (e *ExecuteExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	return nil
}

// Build builds a prepared statement into an executor.
// After Build, e.StmtExec will be used to do the real execution.
func (e *ExecuteExec) Build() error {
	var err error
	if IsPointGetWithPKOrUniqueKeyByAutoCommit(e.ctx, e.plan) {
		err = e.ctx.InitTxnWithStartTS(math.MaxUint64)
	}
	if err != nil {
		return errors.Trace(err)
	}
	b := newExecutorBuilder(e.ctx, e.is)
	stmtExec := b.build(e.plan)
	if b.err != nil {
		return errors.Trace(b.err)
	}
	e.stmtExec = stmtExec
	CountStmtNode(e.stmt, e.ctx.GetSessionVars().InRestrictedSQL)
	logExpensiveQuery(e.stmt, e.plan)
	return nil
}

// DeallocateExec represent a DEALLOCATE executor.
type DeallocateExec struct {
	baseExecutor

	Name string
}

// Next implements the Executor Next interface.
func (e *DeallocateExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	vars := e.ctx.GetSessionVars()
	id, ok := vars.PreparedStmtNameToID[e.Name]
	if !ok {
		return errors.Trace(plannercore.ErrStmtNotFound)
	}
	delete(vars.PreparedStmtNameToID, e.Name)
	if plannercore.PreparedPlanCacheEnabled() {
		e.ctx.PreparedPlanCache().Delete(plannercore.NewPSTMTPlanCacheKey(
			vars, id, vars.PreparedStmts[id].SchemaVersion,
		))
	}
	vars.RemovePreparedStmt(id)
	return nil
}

// CompileExecutePreparedStmt compiles a session Execute command to a stmt.Statement.
func CompileExecutePreparedStmt(ctx sessionctx.Context, ID uint32, args ...interface{}) (sqlexec.Statement, error) {
	execStmt := &ast.ExecuteStmt{ExecID: ID}
	if err := ResetContextOfStmt(ctx, execStmt); err != nil {
		return nil, err
	}
	execStmt.UsingVars = make([]ast.ExprNode, len(args))
	for i, val := range args {
		execStmt.UsingVars[i] = ast.NewValueExpr(val)
	}
	is := GetInfoSchema(ctx)
	execPlan, err := planner.Optimize(ctx, execStmt, is)
	if err != nil {
		return nil, errors.Trace(err)
	}

	stmt := &ExecStmt{
		InfoSchema: GetInfoSchema(ctx),
		Plan:       execPlan,
		StmtNode:   execStmt,
		Ctx:        ctx,
	}
	if prepared, ok := ctx.GetSessionVars().PreparedStmts[ID]; ok {
		stmt.Text = prepared.Stmt.Text()
	}
	return stmt, nil
}

func getPreparedStmt(stmt *ast.ExecuteStmt, vars *variable.SessionVars) (ast.StmtNode, error) {
	execID := stmt.ExecID
	ok := false
	if stmt.Name != "" {
		if execID, ok = vars.PreparedStmtNameToID[stmt.Name]; !ok {
			return nil, plannercore.ErrStmtNotFound
		}
	}
	if prepared, ok := vars.PreparedStmts[execID]; ok {
		return prepared.Stmt, nil
	}
	return nil, plannercore.ErrStmtNotFound
}
