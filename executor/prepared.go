package executor

import (
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/executor/converter"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/optimizer"
	"github.com/pingcap/tidb/optimizer/evaluator"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
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

type PrepareExec struct {
	IS      infoschema.InfoSchema
	Ctx     context.Context
	Name    string
	SQLText string

	ID           uint32
	ResultFields []*field.ResultField
	ParamCount   int
	Err          error
}

func (e *PrepareExec) Fields() []*ast.ResultField {
	// returns nil to indicate prepare will not return Recordset.
	return nil
}

func (e *PrepareExec) Next() (*Row, error) {
	e.DoPrepare()
	return nil, e.Err
}

func (e *PrepareExec) Close() error {
	return nil
}

// DoPrepare prepares prepares the statement, it can be called multiple times without
// side effect.
func (e *PrepareExec) DoPrepare() {
	vars := variable.GetSessionVars(e.Ctx)
	if e.ID != 0 {
		_, ok := vars.PreparedStmts[e.ID]
		if ok {
			return
		}
	}
	l := parser.NewLexer(e.SQLText)
	l.SetCharsetInfo(getCtxCharsetInfo(e.Ctx))
	if parser.YYParse(l) != 0 {
		e.Err = errors.Trace(l.Errors()[0])
		return
	}
	if len(l.Stmts()) != 1 {
		e.Err = ErrPrepareMulti
		return
	}
	stmt := l.Stmts()[0]
	var extractor paramMarkerExtractor
	stmt.Accept(&extractor)
	sorter := &paramMarkerSorter{markers: extractor.markers}
	sort.Sort(sorter)
	e.ParamCount = len(sorter.markers)
	prepared := &Prepared{
		Stmt:          stmt,
		Params:        sorter.markers,
		SchemaVersion: e.IS.SchemaMetaVersion(),
	}

	if optimizer.IsSupported(stmt) {
		err := optimizer.Prepare(e.IS, e.Ctx, stmt)
		if err != nil {
			e.Err = errors.Trace(err)
			return
		}
		if resultSetNode, ok := stmt.(ast.ResultSetNode); ok {
			e.ResultFields = convertResultFields(resultSetNode.GetResultFields())
		}
	}

	if e.ID == 0 {
		e.ID = vars.GetNextPreparedStmtID()
	}
	if e.Name != "" {
		vars.PreparedStmtNameToID[e.Name] = e.ID
	}
	vars.PreparedStmts[e.ID] = prepared
}

type ExecuteExec struct {
	IS        infoschema.InfoSchema
	Ctx       context.Context
	Name      string
	UsingVars []ast.ExprNode
	ID        uint32
	StmtExec  Executor
	OldStmt   stmt.Statement
}

func (e *ExecuteExec) Fields() []*ast.ResultField {
	// Will never be called.
	return nil
}

func (e *ExecuteExec) Next() (*Row, error) {
	// Will never be called.
	return nil, nil
}

func (e *ExecuteExec) Close() error {
	// Will never be called.
	return nil
}

func (e *ExecuteExec) Build() error {
	vars := variable.GetSessionVars(e.Ctx)
	if e.Name != "" {
		e.ID = vars.PreparedStmtNameToID[e.Name]
	}
	v := vars.PreparedStmts[e.ID]
	if v == nil {
		return ErrStmtNotFound
	}
	prepared := v.(*Prepared)
	if prepared.SchemaVersion != e.IS.SchemaMetaVersion() {
		// If the schema version has changed we need to prepare it again,
		// if this time it failed, the real reason for the error is schema changed.
		err := optimizer.Prepare(e.IS, e.Ctx, prepared.Stmt)
		if err != nil {
			return ErrSchemaChanged
		}
		prepared.SchemaVersion = e.IS.SchemaMetaVersion()
	}
	if len(prepared.Params) != len(e.UsingVars) {
		return ErrWrongParamCount
	}

	for i, usingVar := range e.UsingVars {
		val, err := evaluator.Eval(e.Ctx, usingVar)
		if err != nil {
			return errors.Trace(err)
		}
		prepared.Params[i].SetValue(val)
	}

	if optimizer.IsSupported(prepared.Stmt) {
		plan, err := optimizer.Optimize(e.Ctx, prepared.Stmt)
		if err != nil {
			return errors.Trace(err)
		}
		b := newExecutorBuilder(e.Ctx, e.IS)
		stmtExec := b.build(plan)
		if b.err != nil {
			return errors.Trace(b.err)
		}
		e.StmtExec = stmtExec
	} else {
		conv := converter.Converter{}
		oStmt, err := conv.Convert(prepared.Stmt)
		if err != nil {
			return errors.Trace(err)
		}
		e.OldStmt = oStmt
	}
	return nil
}

type DeallocateExec struct {
	Name string
	ctx  context.Context
}

func (e *DeallocateExec) Fields() []*ast.ResultField {
	return nil
}

func (e *DeallocateExec) Next() (*Row, error) {
	vars := variable.GetSessionVars(e.ctx)
	id, ok := vars.PreparedStmtNameToID[e.Name]
	if !ok {
		return nil, ErrStmtNotFound
	}
	delete(vars.PreparedStmtNameToID, e.Name)
	delete(vars.PreparedStmts, id)
	return nil, nil
}

func (e *DeallocateExec) Close() error {
	return nil
}

// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See: https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func getCtxCharsetInfo(ctx context.Context) (string, string) {
	sessionVars := variable.GetSessionVars(ctx)
	charset := sessionVars.Systems["character_set_connection"]
	collation := sessionVars.Systems["collation_connection"]
	return charset, collation
}

func CompileExecutePreparedStmt(ctx context.Context, ID uint32, args ...interface{}) stmt.Statement {
	execPlan := &plan.Execute{ID: ID}
	execPlan.UsingVars = make([]ast.ExprNode, len(args))
	for i, val := range args {
		execPlan.UsingVars[i] = ast.NewValueExpr(val)
	}
	a := &statementAdapter{
		is:   sessionctx.GetDomain(ctx).InfoSchema(),
		plan: execPlan,
	}
	return a
}
