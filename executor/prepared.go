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
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/sqlexec"
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

// Prepared represents a prepared statement.
type Prepared struct {
	Stmt          ast.StmtNode
	Params        []*ast.ParamMarkerExpr
	SchemaVersion int64
}

// PrepareExec represents a PREPARE executor.
type PrepareExec struct {
	IS      infoschema.InfoSchema
	Ctx     context.Context
	Name    string
	SQLText string

	ID         uint32
	ParamCount int
	Err        error
}

// Schema implements the Executor Schema interface.
func (e *PrepareExec) Schema() expression.Schema {
	// Will never be called.
	return expression.NewSchema(nil)
}

// Next implements the Executor Next interface.
func (e *PrepareExec) Next() (*Row, error) {
	e.DoPrepare()
	return nil, e.Err
}

// Close implements plan.Plan Close interface.
func (e *PrepareExec) Close() error {
	return nil
}

// DoPrepare prepares the statement, it can be called multiple times without
// side effect.
func (e *PrepareExec) DoPrepare() {
	vars := e.Ctx.GetSessionVars()
	if e.ID != 0 {
		// Must be the case when we retry a prepare.
		// Make sure it is idempotent.
		_, ok := vars.PreparedStmts[e.ID]
		if ok {
			return
		}
	}
	charset, collation := vars.GetCharsetInfo()
	var (
		stmts []ast.StmtNode
		err   error
	)
	if sqlParser, ok := e.Ctx.(sqlexec.SQLParser); ok {
		stmts, err = sqlParser.ParseSQL(e.SQLText, charset, collation)
	} else {
		stmts, err = parser.New().Parse(e.SQLText, charset, collation)
	}
	if err != nil {
		e.Err = errors.Trace(err)
		return
	}
	if len(stmts) != 1 {
		e.Err = errors.Trace(ErrPrepareMulti)
		return
	}
	stmt := stmts[0]
	if _, ok := stmt.(ast.DDLNode); ok {
		e.Err = errors.Trace(ErrPrepareDDL)
		return
	}
	var extractor paramMarkerExtractor
	stmt.Accept(&extractor)

	// The parameter markers are appended in visiting order, which may not
	// be the same as the position order in the query string. We need to
	// sort it by position.
	sorter := &paramMarkerSorter{markers: extractor.markers}
	sort.Sort(sorter)
	e.ParamCount = len(sorter.markers)
	prepared := &Prepared{
		Stmt:          stmt,
		Params:        sorter.markers,
		SchemaVersion: e.IS.SchemaMetaVersion(),
	}

	err = plan.PrepareStmt(e.IS, e.Ctx, stmt)
	if err != nil {
		e.Err = errors.Trace(err)
		return
	}

	if e.ID == 0 {
		e.ID = vars.GetNextPreparedStmtID()
	}
	if e.Name != "" {
		vars.PreparedStmtNameToID[e.Name] = e.ID
	}
	vars.PreparedStmts[e.ID] = prepared
}

// ExecuteExec represents an EXECUTE executor.
// It cannot be executed by itself, all it needs to do is to build
// another Executor from a prepared statement.
type ExecuteExec struct {
	IS        infoschema.InfoSchema
	Ctx       context.Context
	Name      string
	UsingVars []expression.Expression
	ID        uint32
	StmtExec  Executor
	Stmt      ast.StmtNode
	Plan      plan.Plan
}

// Schema implements the Executor Schema interface.
func (e *ExecuteExec) Schema() expression.Schema {
	// Will never be called.
	return expression.NewSchema(nil)
}

// Next implements the Executor Next interface.
func (e *ExecuteExec) Next() (*Row, error) {
	// Will never be called.
	return nil, nil
}

// Close implements plan.Plan Close interface.
func (e *ExecuteExec) Close() error {
	// Will never be called.
	return nil
}

// Build builds a prepared statement into an executor.
// After Build, e.StmtExec will be used to do the real execution.
func (e *ExecuteExec) Build() error {
	vars := e.Ctx.GetSessionVars()
	if e.Name != "" {
		e.ID = vars.PreparedStmtNameToID[e.Name]
	}
	v := vars.PreparedStmts[e.ID]
	if v == nil {
		return errors.Trace(ErrStmtNotFound)
	}
	prepared := v.(*Prepared)

	if len(prepared.Params) != len(e.UsingVars) {
		return errors.Trace(ErrWrongParamCount)
	}

	for i, usingVar := range e.UsingVars {
		val, err := usingVar.Eval(nil, e.Ctx)
		if err != nil {
			return errors.Trace(err)
		}
		prepared.Params[i].SetDatum(val)
	}

	if prepared.SchemaVersion != e.IS.SchemaMetaVersion() {
		// If the schema version has changed we need to prepare it again,
		// if this time it failed, the real reason for the error is schema changed.
		err := plan.PrepareStmt(e.IS, e.Ctx, prepared.Stmt)
		if err != nil {
			return ErrSchemaChanged.Gen("Schema change caused error: %s", err.Error())
		}
		prepared.SchemaVersion = e.IS.SchemaMetaVersion()
	}
	p, err := plan.Optimize(e.Ctx, prepared.Stmt, e.IS)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.Ctx.ActivePendingTxn()
	if err != nil {
		return errors.Trace(err)
	}
	b := newExecutorBuilder(e.Ctx, e.IS)
	stmtExec := b.build(p)
	if b.err != nil {
		return errors.Trace(b.err)
	}
	e.StmtExec = stmtExec
	e.Stmt = prepared.Stmt
	e.Plan = p
	return nil
}

// DeallocateExec represent a DEALLOCATE executor.
type DeallocateExec struct {
	Name string
	ctx  context.Context
}

// Schema implements the Executor Schema interface.
func (e *DeallocateExec) Schema() expression.Schema {
	// Will never be called.
	return expression.NewSchema(nil)
}

// Next implements the Executor Next interface.
func (e *DeallocateExec) Next() (*Row, error) {
	vars := e.ctx.GetSessionVars()
	id, ok := vars.PreparedStmtNameToID[e.Name]
	if !ok {
		return nil, errors.Trace(ErrStmtNotFound)
	}
	delete(vars.PreparedStmtNameToID, e.Name)
	delete(vars.PreparedStmts, id)
	return nil, nil
}

// Close implements plan.Plan Close interface.
func (e *DeallocateExec) Close() error {
	return nil
}

// CompileExecutePreparedStmt compiles a session Execute command to a stmt.Statement.
func CompileExecutePreparedStmt(ctx context.Context, ID uint32, args ...interface{}) ast.Statement {
	execPlan := &plan.Execute{ID: ID}
	execPlan.UsingVars = make([]expression.Expression, len(args))
	for i, val := range args {
		value := ast.NewValueExpr(val)
		execPlan.UsingVars[i] = &expression.Constant{Value: value.Datum, RetType: &value.Type}
	}
	sa := &statement{
		is:   GetInfoSchema(ctx),
		plan: execPlan,
	}
	return sa
}
