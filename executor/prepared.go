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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/topsql"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"go.uber.org/zap"
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

	name    string
	sqlText string

	ID         uint32
	ParamCount int
	Fields     []*ast.ResultField

	// If it's generated from executing "prepare stmt from '...'", the process is parse -> plan -> executor
	// If it's generated from the prepare protocol, the process is session.PrepareStmt -> NewPrepareExec
	// They both generate a PrepareExec struct, but the second case needs to reset the statement context while the first already do that.
	needReset bool
}

// NewPrepareExec creates a new PrepareExec.
func NewPrepareExec(ctx sessionctx.Context, sqlTxt string) *PrepareExec {
	base := newBaseExecutor(ctx, nil, 0)
	base.initCap = chunk.ZeroCapacity
	return &PrepareExec{
		baseExecutor: base,
		sqlText:      sqlTxt,
		needReset:    true,
	}
}

// Next implements the Executor Next interface.
func (e *PrepareExec) Next(ctx context.Context, req *chunk.Chunk) error {
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
		// FIXME: ok... yet another parse API, may need some api interface clean.
		stmts, _, err = sqlParser.ParseSQL(ctx, e.sqlText,
			parser.CharsetConnection(charset),
			parser.CollationConnection(collation))
	} else {
		p := parser.New()
		p.SetParserConfig(vars.BuildParserConfig())
		var warns []error
		stmts, warns, err = p.ParseSQL(e.sqlText,
			parser.CharsetConnection(charset),
			parser.CollationConnection(collation))
		for _, warn := range warns {
			e.ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
		}
	}
	if err != nil {
		return util.SyntaxError(err)
	}
	if len(stmts) != 1 {
		return ErrPrepareMulti
	}
	stmt := stmts[0]

	if e.needReset {
		err = ResetContextOfStmt(e.ctx, stmt)
		if err != nil {
			return err
		}
	}

	var extractor paramMarkerExtractor
	stmt.Accept(&extractor)

	// DDL Statements can not accept parameters
	if _, ok := stmt.(ast.DDLNode); ok && len(extractor.markers) > 0 {
		return ErrPrepareDDL
	}

	switch stmt.(type) {
	case *ast.LoadDataStmt, *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt, *ast.NonTransactionalDeleteStmt:
		return ErrUnsupportedPs
	}

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	if len(extractor.markers) > math.MaxUint16 {
		return ErrPsManyParam
	}

	ret := &plannercore.PreprocessorReturn{}
	err = plannercore.Preprocess(e.ctx, stmt, plannercore.InPrepare, plannercore.WithPreprocessorReturn(ret))
	if err != nil {
		return err
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
		StmtType:      GetStmtLabel(stmt),
		Params:        sorter.markers,
		SchemaVersion: ret.InfoSchema.SchemaMetaVersion(),
	}
	normalizedSQL, digest := parser.NormalizeDigest(prepared.Stmt.Text())
	if topsqlstate.TopSQLEnabled() {
		e.ctx.GetSessionVars().StmtCtx.IsSQLRegistered.Store(true)
		ctx = topsql.AttachAndRegisterSQLInfo(ctx, normalizedSQL, digest, vars.InRestrictedSQL)
	}

	var (
		normalizedSQL4PC, digest4PC string
		selectStmtNode              ast.StmtNode
	)
	if !plannercore.PreparedPlanCacheEnabled() {
		prepared.UseCache = false
	} else {
		prepared.UseCache = plannercore.CacheableWithCtx(e.ctx, stmt, ret.InfoSchema)
		selectStmtNode, normalizedSQL4PC, digest4PC, err = planner.ExtractSelectAndNormalizeDigest(stmt, e.ctx.GetSessionVars().CurrentDB)
		if err != nil || selectStmtNode == nil {
			normalizedSQL4PC = ""
			digest4PC = ""
		}
	}

	// We try to build the real statement of preparedStmt.
	for i := range prepared.Params {
		param := prepared.Params[i].(*driver.ParamMarkerExpr)
		param.Datum.SetNull()
		param.InExecute = false
	}
	var p plannercore.Plan
	e.ctx.GetSessionVars().PlanID = 0
	e.ctx.GetSessionVars().PlanColumnID = 0
	e.ctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol = nil
	destBuilder, _ := plannercore.NewPlanBuilder().Init(e.ctx, ret.InfoSchema, &hint.BlockHintProcessor{})
	p, err = destBuilder.Build(ctx, stmt)
	if err != nil {
		return err
	}
	// In MySQL prepare protocol, the server need to tell the client how many column the prepared statement would return when executing it.
	// For a query with on result, e.g. an insert statement, there will be no result, so 'e.Fields' is not set.
	// Usually, p.Schema().Len() == 0 means no result. A special case is the 'do' statement, it looks like 'select' but discard the result.
	if !isNoResultPlan(p) {
		e.Fields = colNames2ResultFields(p.Schema(), p.OutputNames(), vars.CurrentDB)
	}
	if e.ID == 0 {
		e.ID = vars.GetNextPreparedStmtID()
	}
	if e.name != "" {
		vars.PreparedStmtNameToID[e.name] = e.ID
	}

	preparedObj := &plannercore.CachedPrepareStmt{
		PreparedAst:         prepared,
		StmtDB:              e.ctx.GetSessionVars().CurrentDB,
		StmtText:            stmt.Text(),
		VisitInfos:          destBuilder.GetVisitInfo(),
		NormalizedSQL:       normalizedSQL,
		SQLDigest:           digest,
		ForUpdateRead:       destBuilder.GetIsForUpdateRead(),
		SnapshotTSEvaluator: ret.SnapshotTSEvaluator,
		NormalizedSQL4PC:    normalizedSQL4PC,
		SQLDigest4PC:        digest4PC,
	}
	return vars.AddPreparedStmt(e.ID, preparedObj)
}

// ExecuteExec represents an EXECUTE executor.
// It cannot be executed by itself, all it needs to do is to build
// another Executor from a prepared statement.
type ExecuteExec struct {
	baseExecutor

	is            infoschema.InfoSchema
	name          string
	usingVars     []expression.Expression
	stmtExec      Executor
	stmt          ast.StmtNode
	plan          plannercore.Plan
	id            uint32
	lowerPriority bool
	outputNames   []*types.FieldName
}

// Next implements the Executor Next interface.
func (e *ExecuteExec) Next(ctx context.Context, req *chunk.Chunk) error {
	return nil
}

// Build builds a prepared statement into an executor.
// After Build, e.StmtExec will be used to do the real execution.
func (e *ExecuteExec) Build(b *executorBuilder) error {
	if snapshotTS := e.ctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		if err := e.ctx.InitTxnWithStartTS(snapshotTS); err != nil {
			return err
		}
	} else {
		ok, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(e.ctx, e.plan)
		if err != nil {
			return err
		}
		if ok {
			err = e.ctx.InitTxnWithStartTS(math.MaxUint64)
			if err != nil {
				return err
			}
		}
	}
	stmtExec := b.build(e.plan)
	if b.err != nil {
		log.Warn("rebuild plan in EXECUTE statement failed", zap.String("labelName of PREPARE statement", e.name))
		return errors.Trace(b.err)
	}
	e.stmtExec = stmtExec
	if e.ctx.GetSessionVars().StmtCtx.Priority == mysql.NoPriority {
		e.lowerPriority = needLowerPriority(e.plan)
	}
	return nil
}

// DeallocateExec represent a DEALLOCATE executor.
type DeallocateExec struct {
	baseExecutor

	Name string
}

// Next implements the Executor Next interface.
func (e *DeallocateExec) Next(ctx context.Context, req *chunk.Chunk) error {
	vars := e.ctx.GetSessionVars()
	id, ok := vars.PreparedStmtNameToID[e.Name]
	if !ok {
		return errors.Trace(plannercore.ErrStmtNotFound)
	}
	preparedPointer := vars.PreparedStmts[id]
	preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
	if !ok {
		return errors.Errorf("invalid CachedPrepareStmt type")
	}
	prepared := preparedObj.PreparedAst
	delete(vars.PreparedStmtNameToID, e.Name)
	if plannercore.PreparedPlanCacheEnabled() {
		cacheKey, err := plannercore.NewPlanCacheKey(vars, preparedObj.StmtText, preparedObj.StmtDB, prepared.SchemaVersion)
		if err != nil {
			return err
		}
		if !vars.IgnorePreparedCacheCloseStmt { // keep the plan in cache
			e.ctx.PreparedPlanCache().Delete(cacheKey)
		}
	}
	vars.RemovePreparedStmt(id)
	return nil
}

// CompileExecutePreparedStmt compiles a session Execute command to a stmt.Statement.
func CompileExecutePreparedStmt(ctx context.Context, sctx sessionctx.Context,
	ID uint32, is infoschema.InfoSchema, snapshotTS uint64, replicaReadScope string, args []types.Datum) (*ExecStmt, bool, bool, error) {
	startTime := time.Now()
	defer func() {
		sctx.GetSessionVars().DurationCompile = time.Since(startTime)
	}()
	execStmt := &ast.ExecuteStmt{ExecID: ID}
	if err := ResetContextOfStmt(sctx, execStmt); err != nil {
		return nil, false, false, err
	}
	isStaleness := snapshotTS != 0
	sctx.GetSessionVars().StmtCtx.IsStaleness = isStaleness
	execStmt.BinaryArgs = args
	execPlan, names, err := planner.Optimize(ctx, sctx, execStmt, is)
	if err != nil {
		return nil, false, false, err
	}

	failpoint.Inject("assertTxnManagerInCompile", func() {
		sessiontxn.RecordAssert(sctx, "assertTxnManagerInCompile", true)
		sessiontxn.AssertTxnManagerInfoSchema(sctx, is)
		staleread.AssertStmtStaleness(sctx, snapshotTS != 0)
		if snapshotTS != 0 {
			sessiontxn.AssertTxnManagerReadTS(sctx, snapshotTS)
		}
	})

	stmt := &ExecStmt{
		GoCtx:            ctx,
		InfoSchema:       is,
		Plan:             execPlan,
		StmtNode:         execStmt,
		Ctx:              sctx,
		OutputNames:      names,
		Ti:               &TelemetryInfo{},
		ReplicaReadScope: replicaReadScope,
	}
	if preparedPointer, ok := sctx.GetSessionVars().PreparedStmts[ID]; ok {
		preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
		if !ok {
			return nil, false, false, errors.Errorf("invalid CachedPrepareStmt type")
		}
		stmtCtx := sctx.GetSessionVars().StmtCtx
		stmt.Text = preparedObj.PreparedAst.Stmt.Text()
		stmtCtx.OriginalSQL = stmt.Text
		stmtCtx.InitSQLDigest(preparedObj.NormalizedSQL, preparedObj.SQLDigest)
	}
	tiFlashPushDown, tiFlashExchangePushDown := plannercore.IsTiFlashContained(stmt.Plan)
	return stmt, tiFlashPushDown, tiFlashExchangePushDown, nil
}
