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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"go.uber.org/zap"
)

var (
	_ exec.Executor = &DeallocateExec{}
	_ exec.Executor = &ExecuteExec{}
	_ exec.Executor = &PrepareExec{}
)

// PrepareExec represents a PREPARE executor.
type PrepareExec struct {
	exec.BaseExecutor

	name    string
	sqlText string

	ID         uint32
	ParamCount int
	Fields     []*ast.ResultField
	Stmt       any

	// If it's generated from executing "prepare stmt from '...'", the process is parse -> plan -> executor
	// If it's generated from the prepare protocol, the process is session.PrepareStmt -> NewPrepareExec
	// They both generate a PrepareExec struct, but the second case needs to reset the statement context while the first already do that.
	needReset bool
}

// NewPrepareExec creates a new PrepareExec.
func NewPrepareExec(ctx sessionctx.Context, sqlTxt string) *PrepareExec {
	base := exec.NewBaseExecutor(ctx, nil, 0)
	base.SetInitCap(chunk.ZeroCapacity)
	return &PrepareExec{
		BaseExecutor: base,
		sqlText:      sqlTxt,
		needReset:    true,
	}
}

// Next implements the Executor Next interface.
func (e *PrepareExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	vars := e.Ctx().GetSessionVars()
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
	if sqlParser, ok := e.Ctx().(sqlexec.SQLParser); ok {
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
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
		}
	}
	if err != nil {
		return util.SyntaxError(err)
	}
	if len(stmts) != 1 {
		return exeerrors.ErrPrepareMulti
	}
	stmt0 := stmts[0]
	if e.needReset {
		err = ResetContextOfStmt(e.Ctx(), stmt0)
		if err != nil {
			return err
		}
	}
	stmt, p, paramCnt, err := plannercore.GeneratePlanCacheStmtWithAST(ctx, e.Ctx(), true, stmt0.Text(), stmt0, sessiontxn.GetTxnManager(e.Ctx()).GetTxnInfoSchema())
	if err != nil {
		return err
	}
	if topsqlstate.TopSQLEnabled() {
		e.Ctx().GetSessionVars().StmtCtx.IsSQLRegistered.Store(true)
		topsql.AttachAndRegisterSQLInfo(ctx, stmt.NormalizedSQL, stmt.SQLDigest, vars.InRestrictedSQL)
	}

	e.Ctx().GetSessionVars().PlanID.Store(0)
	e.Ctx().GetSessionVars().PlanColumnID.Store(0)
	e.Ctx().GetSessionVars().MapHashCode2UniqueID4ExtendedCol = nil
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

	e.ParamCount = paramCnt
	e.Stmt = stmt
	return vars.AddPreparedStmt(e.ID, stmt)
}

// ExecuteExec represents an EXECUTE executor.
// It cannot be executed by itself, all it needs to do is to build
// another Executor from a prepared statement.
type ExecuteExec struct {
	exec.BaseExecutor

	is            infoschema.InfoSchema
	name          string
	usingVars     []expression.Expression
	stmtExec      exec.Executor
	stmt          ast.StmtNode
	plan          base.Plan
	lowerPriority bool
	outputNames   []*types.FieldName
}

// Next implements the Executor Next interface.
func (*ExecuteExec) Next(context.Context, *chunk.Chunk) error {
	return nil
}

// Build builds a prepared statement into an executor.
// After Build, e.StmtExec will be used to do the real execution.
func (e *ExecuteExec) Build(b *executorBuilder) error {
	stmtExec := b.build(e.plan)
	if b.err != nil {
		log.Warn("rebuild plan in EXECUTE statement failed", zap.String("labelName of PREPARE statement", e.name))
		return errors.Trace(b.err)
	}
	e.stmtExec = stmtExec
	if e.Ctx().GetSessionVars().StmtCtx.Priority == mysql.NoPriority {
		e.lowerPriority = needLowerPriority(e.plan)
	}
	return nil
}

// DeallocateExec represent a DEALLOCATE executor.
type DeallocateExec struct {
	exec.BaseExecutor

	Name string
}

// Next implements the Executor Next interface.
func (e *DeallocateExec) Next(context.Context, *chunk.Chunk) error {
	vars := e.Ctx().GetSessionVars()
	id, ok := vars.PreparedStmtNameToID[e.Name]
	if !ok {
		return errors.Trace(plannererrors.ErrStmtNotFound)
	}
	preparedPointer := vars.PreparedStmts[id]
	preparedObj, ok := preparedPointer.(*plannercore.PlanCacheStmt)
	if !ok {
		return errors.Errorf("invalid PlanCacheStmt type")
	}
	delete(vars.PreparedStmtNameToID, e.Name)
	if e.Ctx().GetSessionVars().EnablePreparedPlanCache {
		bindSQL, _ := bindinfo.MatchSQLBindingForPlanCache(e.Ctx(), preparedObj.PreparedAst.Stmt, &preparedObj.BindingInfo)
		cacheKey, err := plannercore.NewPlanCacheKey(vars, preparedObj.StmtText, preparedObj.StmtDB, preparedObj.SchemaVersion,
			0, bindSQL, expression.ExprPushDownBlackListReloadTimeStamp.Load(), preparedObj.RelateVersion)
		if err != nil {
			return err
		}
		if !vars.IgnorePreparedCacheCloseStmt { // keep the plan in cache
			e.Ctx().GetSessionPlanCache().Delete(cacheKey)
		}
	}
	vars.RemovePreparedStmt(id)
	return nil
}
