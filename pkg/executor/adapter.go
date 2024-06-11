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
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/types"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/breakpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// processinfoSetter is the interface use to set current running process info.
type processinfoSetter interface {
	SetProcessInfo(string, time.Time, byte, uint64)
	UpdateProcessInfo()
}

// recordSet wraps an executor, implements sqlexec.RecordSet interface
type recordSet struct {
	fields     []*ast.ResultField
	executor   exec.Executor
	stmt       *ExecStmtRuntime
	lastErrs   []error
	txnStartTS uint64
	once       sync.Once
}

func (a *recordSet) Fields() []*ast.ResultField {
	if len(a.fields) == 0 {
		a.fields = colNames2ResultFields(a.executor.Schema(), a.stmt.OutputNames, a.stmt.Ctx.GetSessionVars().CurrentDB)
	}
	return a.fields
}

func colNames2ResultFields(schema *expression.Schema, names []*types.FieldName, defaultDB string) []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, schema.Len())
	defaultDBCIStr := model.NewCIStr(defaultDB)
	for i := 0; i < schema.Len(); i++ {
		dbName := names[i].DBName
		if dbName.L == "" && names[i].TblName.L != "" {
			dbName = defaultDBCIStr
		}
		origColName := names[i].OrigColName
		emptyOrgName := false
		if origColName.L == "" {
			origColName = names[i].ColName
			emptyOrgName = true
		}
		rf := &ast.ResultField{
			Column:       &model.ColumnInfo{Name: origColName, FieldType: *schema.Columns[i].RetType},
			ColumnAsName: names[i].ColName,
			EmptyOrgName: emptyOrgName,
			Table:        &model.TableInfo{Name: names[i].OrigTblName},
			TableAsName:  names[i].TblName,
			DBName:       dbName,
		}
		// This is for compatibility.
		// See issue https://github.com/pingcap/tidb/issues/10513 .
		if len(rf.ColumnAsName.O) > mysql.MaxAliasIdentifierLen {
			rf.ColumnAsName.O = rf.ColumnAsName.O[:mysql.MaxAliasIdentifierLen]
		}
		// Usually the length of O equals the length of L.
		// Add this len judgement to avoid panic.
		if len(rf.ColumnAsName.L) > mysql.MaxAliasIdentifierLen {
			rf.ColumnAsName.L = rf.ColumnAsName.L[:mysql.MaxAliasIdentifierLen]
		}
		rfs = append(rfs, rf)
	}
	return rfs
}

// Next use uses recordSet's executor to get next available chunk for later usage.
// If chunk does not contain any rows, then we update last query found rows in session variable as current found rows.
// The reason we need update is that chunk with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and chunk with some rows inside, we simply update last query found rows by the number of row in chunk.
func (a *recordSet) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = util2.GetRecoverError(r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.stmt.GetTextToLog(false)), zap.Stack("stack"))
	}()

	err = a.stmt.next(ctx, a.executor, req)
	if err != nil {
		a.lastErrs = append(a.lastErrs, err)
		return err
	}
	numRows := req.NumRows()
	if numRows == 0 {
		if a.stmt != nil {
			a.stmt.Ctx.GetSessionVars().LastFoundRows = a.stmt.Ctx.GetSessionVars().StmtCtx.FoundRows()
		}
		return nil
	}
	if a.stmt != nil {
		a.stmt.Ctx.GetSessionVars().StmtCtx.AddFoundRows(uint64(numRows))
	}
	return nil
}

// NewChunk create a chunk base on top-level executor's exec.NewFirstChunk().
func (a *recordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	if alloc == nil {
		return exec.NewFirstChunk(a.executor)
	}

	return alloc.Alloc(a.executor.RetFieldTypes(), a.executor.InitCap(), a.executor.MaxChunkSize())
}

func (a *recordSet) Finish() error {
	var err error
	a.once.Do(func() {
		err = exec.Close(a.executor)
		cteErr := resetCTEStorageMap(a.stmt.Ctx)
		if cteErr != nil {
			logutil.BgLogger().Error("got error when reset cte storage, should check if the spill disk file deleted or not", zap.Error(cteErr))
		}
		if err == nil {
			err = cteErr
		}
	})
	if err != nil {
		a.lastErrs = append(a.lastErrs, err)
	}
	return err
}

func (a *recordSet) Close() error {
	err := a.Finish()
	if err != nil {
		logutil.BgLogger().Error("close recordSet error", zap.Error(err))
	}
	a.stmt.CloseRecordSet(a.txnStartTS, errors.Join(a.lastErrs...))
	return err
}

// OnFetchReturned implements commandLifeCycle#OnFetchReturned
func (a *recordSet) OnFetchReturned() {
	a.stmt.LogSlowQuery(a.txnStartTS, len(a.lastErrs) == 0, true)
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
type ExecStmt struct {
	ExecStmtRuntime

	// LowerPriority represents whether to lower the execution priority of a query.
	LowerPriority     bool
	isSelectForUpdate bool

	PsStmt *plannercore.PlanCacheStmt
}

// GetStmtNode returns the stmtNode inside Statement
func (a *ExecStmt) GetStmtNode() ast.StmtNode {
	return a.StmtNode
}

// PointGet short path for point exec directly from plan, keep only necessary steps
func (a *ExecStmt) PointGet(ctx context.Context) (*recordSet, error) {
	r, ctx := tracing.StartRegionEx(ctx, "ExecStmt.PointGet")
	defer r.End()
	if r.Span != nil {
		r.Span.LogKV("sql", a.OriginText())
	}

	failpoint.Inject("assertTxnManagerInShortPointGetPlan", func() {
		sessiontxn.RecordAssert(a.Ctx, "assertTxnManagerInShortPointGetPlan", true)
		// stale read should not reach here
		staleread.AssertStmtStaleness(a.Ctx, false)
		sessiontxn.AssertTxnManagerInfoSchema(a.Ctx, a.InfoSchema)
	})

	ctx = a.observeStmtBeginForTopSQL(ctx)
	startTs, err := sessiontxn.GetTxnManager(a.Ctx).GetStmtReadTS()
	if err != nil {
		return nil, err
	}
	a.Ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityHigh

	var executor exec.Executor
	useMaxTS := startTs == math.MaxUint64

	// try to reuse point get executor
	// We should only use the cached the executor when the startTS is MaxUint64
	if a.PsStmt.PointGet.Executor != nil && useMaxTS {
		exec, ok := a.PsStmt.PointGet.Executor.(*PointGetExecutor)
		if !ok {
			logutil.Logger(ctx).Error("invalid executor type, not PointGetExecutor for point get path")
			a.PsStmt.PointGet.Executor = nil
		} else {
			// CachedPlan type is already checked in last step
			pointGetPlan := a.Plan.(*plannercore.PointGetPlan)
			exec.Init(pointGetPlan)
			a.PsStmt.PointGet.Executor = exec
			executor = exec
		}
	}

	if executor == nil {
		b := newExecutorBuilder(a.Ctx, a.InfoSchema)
		executor = b.build(a.Plan)
		if b.err != nil {
			return nil, b.err
		}
		pointExecutor, ok := executor.(*PointGetExecutor)

		// Don't cache the executor for non point-get (table dual) or partitioned tables
		if ok && useMaxTS && pointExecutor.partitionDefIdx == nil {
			a.PsStmt.PointGet.Executor = pointExecutor
		}
	}

	if err = exec.Open(ctx, executor); err != nil {
		terror.Log(exec.Close(executor))
		return nil, err
	}

	sctx := a.Ctx
	cmd32 := atomic.LoadUint32(&sctx.GetSessionVars().CommandValue)
	cmd := byte(cmd32)
	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
		pi = raw
		sql := a.OriginText()
		maxExecutionTime := sctx.GetSessionVars().GetMaxExecutionTime()
		// Update processinfo, ShowProcess() will use it.
		pi.SetProcessInfo(sql, time.Now(), cmd, maxExecutionTime)
		if sctx.GetSessionVars().StmtCtx.StmtType == "" {
			sctx.GetSessionVars().StmtCtx.StmtType = ast.GetStmtLabel(a.StmtNode)
		}
	}

	return &recordSet{
		executor:   executor,
		stmt:       &a.ExecStmtRuntime,
		txnStartTS: startTs,
	}, nil
}

// OriginText returns original statement as a string.
func (a *ExecStmt) OriginText() string {
	return a.Text
}

// IsPrepared returns true if stmt is a prepare statement.
func (a *ExecStmt) IsPrepared() bool {
	return a.isPreparedStmt
}

// IsReadOnly returns true if a statement is read only.
// If current StmtNode is an ExecuteStmt, we can get its prepared stmt,
// then using ast.IsReadOnly function to determine a statement is read only or not.
func (a *ExecStmt) IsReadOnly(vars *variable.SessionVars) bool {
	return planner.IsReadOnly(a.StmtNode, vars)
}

// RebuildPlan rebuilds current execute statement plan.
// It returns the current information schema version that 'a' is using.
func (a *ExecStmt) RebuildPlan(ctx context.Context) (int64, error) {
	ret := &plannercore.PreprocessorReturn{}
	if err := plannercore.Preprocess(ctx, a.Ctx, a.StmtNode, plannercore.InTxnRetry, plannercore.InitTxnContextProvider, plannercore.WithPreprocessorReturn(ret)); err != nil {
		return 0, err
	}

	failpoint.Inject("assertTxnManagerInRebuildPlan", func() {
		if is, ok := a.Ctx.Value(sessiontxn.AssertTxnInfoSchemaAfterRetryKey).(infoschema.InfoSchema); ok {
			a.Ctx.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is)
			a.Ctx.SetValue(sessiontxn.AssertTxnInfoSchemaAfterRetryKey, nil)
		}
		sessiontxn.RecordAssert(a.Ctx, "assertTxnManagerInRebuildPlan", true)
		sessiontxn.AssertTxnManagerInfoSchema(a.Ctx, ret.InfoSchema)
		staleread.AssertStmtStaleness(a.Ctx, ret.IsStaleness)
		if ret.IsStaleness {
			sessiontxn.AssertTxnManagerReadTS(a.Ctx, ret.LastSnapshotTS)
		}
	})

	a.InfoSchema = sessiontxn.GetTxnManager(a.Ctx).GetTxnInfoSchema()
	replicaReadScope := sessiontxn.GetTxnManager(a.Ctx).GetReadReplicaScope()
	if a.Ctx.GetSessionVars().GetReplicaRead().IsClosestRead() && replicaReadScope == kv.GlobalReplicaScope {
		logutil.BgLogger().Warn(fmt.Sprintf("tidb can't read closest replicas due to it haven't %s label", placement.DCLabelKey))
	}
	p, names, err := planner.Optimize(ctx, a.Ctx, a.StmtNode, a.InfoSchema)
	if err != nil {
		return 0, err
	}
	a.OutputNames = names
	a.Plan = p
	a.Ctx.GetSessionVars().StmtCtx.SetPlan(p)
	return a.InfoSchema.SchemaMetaVersion(), nil
}

// IsFastPlan exports for testing.
func IsFastPlan(p base.Plan) bool {
	if proj, ok := p.(*plannercore.PhysicalProjection); ok {
		p = proj.Children()[0]
	}
	switch p.(type) {
	case *plannercore.PointGetPlan:
		return true
	case *plannercore.PhysicalTableDual:
		// Plan of following SQL is PhysicalTableDual:
		// select 1;
		// select @@autocommit;
		return true
	case *plannercore.Set:
		// Plan of following SQL is Set:
		// set @a=1;
		// set @@autocommit=1;
		return true
	}
	return false
}

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function. If the Executor returns
// result, execution is done after this function returns, in the returned sqlexec.RecordSet Next method.
func (a *ExecStmt) Exec(ctx context.Context) (_ sqlexec.RecordSet, err error) {
	defer func() {
		r := recover()
		if r == nil {
			if a.retryCount > 0 {
				metrics.StatementPessimisticRetryCount.Observe(float64(a.retryCount))
			}
			lockKeysCnt := a.Ctx.GetSessionVars().StmtCtx.LockKeysCount
			if lockKeysCnt > 0 {
				metrics.StatementLockKeysCount.Observe(float64(lockKeysCnt))
			}

			execDetails := a.Ctx.GetSessionVars().StmtCtx.GetExecDetails()
			if err == nil && execDetails.LockKeysDetail != nil &&
				(execDetails.LockKeysDetail.AggressiveLockNewCount > 0 || execDetails.LockKeysDetail.AggressiveLockDerivedCount > 0) {
				a.Ctx.GetSessionVars().TxnCtx.FairLockingUsed = true
				// If this statement is finished when some of the keys are locked with conflict in the last retry, or
				// some of the keys are derived from the previous retry, we consider the optimization of fair locking
				// takes effect on this statement.
				if execDetails.LockKeysDetail.LockedWithConflictCount > 0 || execDetails.LockKeysDetail.AggressiveLockDerivedCount > 0 {
					a.Ctx.GetSessionVars().TxnCtx.FairLockingEffective = true
				}
			}
			return
		}
		recoverdErr, ok := r.(error)
		if !ok || !(exeerrors.ErrMemoryExceedForQuery.Equal(recoverdErr) ||
			exeerrors.ErrMemoryExceedForInstance.Equal(recoverdErr) ||
			exeerrors.ErrQueryInterrupted.Equal(recoverdErr) ||
			exeerrors.ErrMaxExecTimeExceeded.Equal(recoverdErr)) {
			panic(r)
		}
		err = recoverdErr
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.GetTextToLog(false)), zap.Stack("stack"))
	}()

	failpoint.Inject("assertStaleTSO", func(val failpoint.Value) {
		if n, ok := val.(int); ok && staleread.IsStmtStaleness(a.Ctx) {
			txnManager := sessiontxn.GetTxnManager(a.Ctx)
			ts, err := txnManager.GetStmtReadTS()
			if err != nil {
				panic(err)
			}
			startTS := oracle.ExtractPhysical(ts) / 1000
			if n != int(startTS) {
				panic(fmt.Sprintf("different tso %d != %d", n, startTS))
			}
		}
	})
	sctx := a.Ctx
	ctx = util.SetSessionID(ctx, sctx.GetSessionVars().ConnectionID)
	if _, ok := a.Plan.(*plannercore.Analyze); ok && sctx.GetSessionVars().InRestrictedSQL {
		oriStats, ok := sctx.GetSessionVars().GetSystemVar(variable.TiDBBuildStatsConcurrency)
		if !ok {
			oriStats = strconv.Itoa(variable.DefBuildStatsConcurrency)
		}
		oriScan := sctx.GetSessionVars().AnalyzeDistSQLScanConcurrency()
		oriIso, ok := sctx.GetSessionVars().GetSystemVar(variable.TxnIsolation)
		if !ok {
			oriIso = "REPEATABLE-READ"
		}
		autoConcurrency, err1 := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, variable.TiDBAutoBuildStatsConcurrency)
		terror.Log(err1)
		if err1 == nil {
			terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, autoConcurrency))
		}
		sVal, err2 := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, variable.TiDBSysProcScanConcurrency)
		terror.Log(err2)
		if err2 == nil {
			concurrency, err3 := strconv.ParseInt(sVal, 10, 64)
			terror.Log(err3)
			if err3 == nil {
				sctx.GetSessionVars().SetAnalyzeDistSQLScanConcurrency(int(concurrency))
			}
		}
		terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TxnIsolation, ast.ReadCommitted))
		defer func() {
			terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, oriStats))
			sctx.GetSessionVars().SetAnalyzeDistSQLScanConcurrency(oriScan)
			terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TxnIsolation, oriIso))
		}()
	}

	if sctx.GetSessionVars().StmtCtx.HasMemQuotaHint {
		sctx.GetSessionVars().MemTracker.SetBytesLimit(sctx.GetSessionVars().StmtCtx.MemQuotaQuery)
	}

	// must set plan according to the `Execute` plan before getting planDigest
	a.inheritContextFromExecuteStmt()
	if variable.EnableResourceControl.Load() && domain.GetDomain(sctx).RunawayManager() != nil {
		stmtCtx := sctx.GetSessionVars().StmtCtx
		_, planDigest := GetPlanDigest(stmtCtx)
		_, sqlDigest := stmtCtx.SQLDigest()
		stmtCtx.RunawayChecker = domain.GetDomain(sctx).RunawayManager().DeriveChecker(sctx.GetSessionVars().StmtCtx.ResourceGroupName, stmtCtx.OriginalSQL, sqlDigest.String(), planDigest.String())
		if err := stmtCtx.RunawayChecker.BeforeExecutor(); err != nil {
			return nil, err
		}
	}
	ctx = a.observeStmtBeginForTopSQL(ctx)

	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}

	cmd32 := atomic.LoadUint32(&sctx.GetSessionVars().CommandValue)
	cmd := byte(cmd32)
	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
		pi = raw
		sql := a.getSQLForProcessInfo()
		maxExecutionTime := sctx.GetSessionVars().GetMaxExecutionTime()
		// Update processinfo, ShowProcess() will use it.
		if a.Ctx.GetSessionVars().StmtCtx.StmtType == "" {
			a.Ctx.GetSessionVars().StmtCtx.StmtType = ast.GetStmtLabel(a.StmtNode)
		}
		// Since maxExecutionTime is used only for query statement, here we limit it affect scope.
		if !a.IsReadOnly(a.Ctx.GetSessionVars()) {
			maxExecutionTime = 0
		}
		pi.SetProcessInfo(sql, time.Now(), cmd, maxExecutionTime)
	}

	breakpoint.Inject(a.Ctx, sessiontxn.BreakPointBeforeExecutorFirstRun)
	if err = a.openExecutor(ctx, e); err != nil {
		terror.Log(exec.Close(e))
		return nil, err
	}

	isPessimistic := sctx.GetSessionVars().TxnCtx.IsPessimistic

	// Special handle for "select for update statement" in pessimistic transaction.
	if isPessimistic && a.isSelectForUpdate {
		return a.handlePessimisticSelectForUpdate(ctx, e)
	}

	a.prepareFKCascadeContext(e)
	if handled, result, err := a.handleNoDelay(ctx, e, isPessimistic); handled || err != nil {
		return result, err
	}

	var txnStartTS uint64
	txn, err := sctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() {
		txnStartTS = txn.StartTS()
	}

	return &recordSet{
		executor:   e,
		stmt:       &a.ExecStmtRuntime,
		txnStartTS: txnStartTS,
	}, nil
}

func (a *ExecStmt) inheritContextFromExecuteStmt() {
	if executePlan, ok := a.Plan.(*plannercore.Execute); ok {
		a.Ctx.SetValue(sessionctx.QueryString, executePlan.Stmt.Text())
		a.OutputNames = executePlan.OutputNames()
		a.isPreparedStmt = true
		a.Plan = executePlan.Plan
		a.Ctx.GetSessionVars().StmtCtx.SetPlan(executePlan.Plan)
	}
}

func (a *ExecStmt) getSQLForProcessInfo() string {
	sql := a.OriginText()
	if simple, ok := a.Plan.(*plannercore.Simple); ok && simple.Statement != nil {
		if ss, ok := simple.Statement.(ast.SensitiveStmtNode); ok {
			// Use SecureText to avoid leak password information.
			sql = ss.SecureText()
		}
	} else if sn, ok2 := a.StmtNode.(ast.SensitiveStmtNode); ok2 {
		// such as import into statement
		sql = sn.SecureText()
	}
	return sql
}

func (a *ExecStmt) handleStmtForeignKeyTrigger(ctx context.Context, e exec.Executor) error {
	stmtCtx := a.Ctx.GetSessionVars().StmtCtx
	if stmtCtx.ForeignKeyTriggerCtx.HasFKCascades {
		// If the ExecStmt has foreign key cascade to be executed, we need call `StmtCommit` to commit the ExecStmt itself
		// change first.
		// Since `UnionScanExec` use `SnapshotIter` and `SnapshotGetter` to read txn mem-buffer, if we don't  do `StmtCommit`,
		// then the fk cascade executor can't read the mem-buffer changed by the ExecStmt.
		a.Ctx.StmtCommit(ctx)
	}
	err := a.handleForeignKeyTrigger(ctx, e, 1)
	if err != nil {
		err1 := a.handleFKTriggerError(stmtCtx)
		if err1 != nil {
			return errors.Errorf("handle foreign key trigger error failed, err: %v, original_err: %v", err1, err)
		}
		return err
	}
	if stmtCtx.ForeignKeyTriggerCtx.SavepointName != "" {
		a.Ctx.GetSessionVars().TxnCtx.ReleaseSavepoint(stmtCtx.ForeignKeyTriggerCtx.SavepointName)
	}
	return nil
}

var maxForeignKeyCascadeDepth = 15

func (a *ExecStmt) handleForeignKeyTrigger(ctx context.Context, e exec.Executor, depth int) error {
	exec, ok := e.(WithForeignKeyTrigger)
	if !ok {
		return nil
	}
	fkChecks := exec.GetFKChecks()
	for _, fkCheck := range fkChecks {
		err := fkCheck.doCheck(ctx)
		if err != nil {
			return err
		}
	}
	fkCascades := exec.GetFKCascades()
	for _, fkCascade := range fkCascades {
		err := a.handleForeignKeyCascade(ctx, fkCascade, depth)
		if err != nil {
			return err
		}
	}
	return nil
}

// handleForeignKeyCascade uses to execute foreign key cascade behaviour, the progress is:
//  1. Build delete/update executor for foreign key on delete/update behaviour.
//     a. Construct delete/update AST. We used to try generated SQL string first and then parse the SQL to get AST,
//     but we need convert Datum to string, there may be some risks here, since assert_eq(datum_a, parse(datum_a.toString())) may be broken.
//     so we chose to construct AST directly.
//     b. Build plan by the delete/update AST.
//     c. Build executor by the delete/update plan.
//  2. Execute the delete/update executor.
//  3. Close the executor.
//  4. `StmtCommit` to commit the kv change to transaction mem-buffer.
//  5. If the foreign key cascade behaviour has more fk value need to be cascaded, go to step 1.
func (a *ExecStmt) handleForeignKeyCascade(ctx context.Context, fkc *FKCascadeExec, depth int) error {
	if a.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		fkc.stats = &FKCascadeRuntimeStats{}
		defer a.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(fkc.plan.ID(), fkc.stats)
	}
	if len(fkc.fkValues) == 0 && len(fkc.fkUpdatedValuesMap) == 0 {
		return nil
	}
	if depth > maxForeignKeyCascadeDepth {
		return exeerrors.ErrForeignKeyCascadeDepthExceeded.GenWithStackByArgs(maxForeignKeyCascadeDepth)
	}
	a.Ctx.GetSessionVars().StmtCtx.InHandleForeignKeyTrigger = true
	defer func() {
		a.Ctx.GetSessionVars().StmtCtx.InHandleForeignKeyTrigger = false
	}()
	if fkc.stats != nil {
		start := time.Now()
		defer func() {
			fkc.stats.Total += time.Since(start)
		}()
	}
	for {
		e, err := fkc.buildExecutor(ctx)
		if err != nil || e == nil {
			return err
		}
		if err := exec.Open(ctx, e); err != nil {
			terror.Log(exec.Close(e))
			return err
		}
		err = exec.Next(ctx, e, exec.NewFirstChunk(e))
		failpoint.Inject("handleForeignKeyCascadeError", func(val failpoint.Value) {
			// Next can recover panic and convert it to error. So we inject error directly here.
			if val.(bool) && err == nil {
				err = errors.New("handleForeignKeyCascadeError")
			}
		})
		closeErr := exec.Close(e)
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		// Call `StmtCommit` uses to flush the fk cascade executor change into txn mem-buffer,
		// then the later fk cascade executors can see the mem-buffer changes.
		a.Ctx.StmtCommit(ctx)
		err = a.handleForeignKeyTrigger(ctx, e, depth+1)
		if err != nil {
			return err
		}
	}
}

// prepareFKCascadeContext records a transaction savepoint for foreign key cascade when this ExecStmt has foreign key
// cascade behaviour and this ExecStmt is in transaction.
func (a *ExecStmt) prepareFKCascadeContext(e exec.Executor) {
	exec, ok := e.(WithForeignKeyTrigger)
	if !ok || !exec.HasFKCascades() {
		return
	}
	sessVar := a.Ctx.GetSessionVars()
	sessVar.StmtCtx.ForeignKeyTriggerCtx.HasFKCascades = true
	if !sessVar.InTxn() {
		return
	}
	txn, err := a.Ctx.Txn(false)
	if err != nil || !txn.Valid() {
		return
	}
	// Record a txn savepoint if ExecStmt in transaction, the savepoint is use to do rollback when handle foreign key
	// cascade failed.
	savepointName := "fk_sp_" + strconv.FormatUint(txn.StartTS(), 10)
	memDBCheckpoint := txn.GetMemDBCheckpoint()
	sessVar.TxnCtx.AddSavepoint(savepointName, memDBCheckpoint)
	sessVar.StmtCtx.ForeignKeyTriggerCtx.SavepointName = savepointName
}

func (a *ExecStmt) handleFKTriggerError(sc *stmtctx.StatementContext) error {
	if sc.ForeignKeyTriggerCtx.SavepointName == "" {
		return nil
	}
	txn, err := a.Ctx.Txn(false)
	if err != nil || !txn.Valid() {
		return err
	}
	savepointRecord := a.Ctx.GetSessionVars().TxnCtx.RollbackToSavepoint(sc.ForeignKeyTriggerCtx.SavepointName)
	if savepointRecord == nil {
		// Normally should never run into here, but just in case, rollback the transaction.
		err = txn.Rollback()
		if err != nil {
			return err
		}
		return errors.Errorf("foreign key cascade savepoint '%s' not found, transaction is rollback, should never happen", sc.ForeignKeyTriggerCtx.SavepointName)
	}
	txn.RollbackMemDBToCheckpoint(savepointRecord.MemDBCheckpoint)
	a.Ctx.GetSessionVars().TxnCtx.ReleaseSavepoint(sc.ForeignKeyTriggerCtx.SavepointName)
	return nil
}

func (a *ExecStmt) handleNoDelay(ctx context.Context, e exec.Executor, isPessimistic bool) (handled bool, rs sqlexec.RecordSet, err error) {
	sc := a.Ctx.GetSessionVars().StmtCtx
	defer func() {
		// If the stmt have no rs like `insert`, The session tracker detachment will be directly
		// done in the `defer` function. If the rs is not nil, the detachment will be done in
		// `rs.Close` in `handleStmt`
		if handled && sc != nil && rs == nil {
			sc.DetachMemDiskTracker()
			cteErr := resetCTEStorageMap(a.Ctx)
			if err == nil {
				// Only overwrite err when it's nil.
				err = cteErr
			}
		}
	}()

	toCheck := e
	isExplainAnalyze := false
	if explain, ok := e.(*ExplainExec); ok {
		if analyze := explain.getAnalyzeExecToExecutedNoDelay(); analyze != nil {
			toCheck = analyze
			isExplainAnalyze = true
			a.Ctx.GetSessionVars().StmtCtx.IsExplainAnalyzeDML = isExplainAnalyze
		}
	}

	// If the executor doesn't return any result to the client, we execute it without delay.
	if toCheck.Schema().Len() == 0 {
		handled = !isExplainAnalyze
		if isPessimistic {
			err := a.handlePessimisticDML(ctx, toCheck)
			return handled, nil, err
		}
		r, err := a.handleNoDelayExecutor(ctx, toCheck)
		return handled, r, err
	} else if proj, ok := toCheck.(*ProjectionExec); ok && proj.calculateNoDelay {
		// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
		// the Projection has two expressions and two columns in the schema, but we should
		// not return the result of the two expressions.
		r, err := a.handleNoDelayExecutor(ctx, e)
		return true, r, err
	}

	return false, nil, nil
}

func isNoResultPlan(p base.Plan) bool {
	if p.Schema().Len() == 0 {
		return true
	}

	// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
	// the Projection has two expressions and two columns in the schema, but we should
	// not return the result of the two expressions.
	switch raw := p.(type) {
	case *plannercore.LogicalProjection:
		if raw.CalculateNoDelay {
			return true
		}
	case *plannercore.PhysicalProjection:
		if raw.CalculateNoDelay {
			return true
		}
	}
	return false
}

type chunkRowRecordSet struct {
	rows     []chunk.Row
	idx      int
	fields   []*ast.ResultField
	e        exec.Executor
	execStmt *ExecStmtRuntime
}

func (c *chunkRowRecordSet) Fields() []*ast.ResultField {
	if c.fields == nil {
		c.fields = colNames2ResultFields(c.e.Schema(), c.execStmt.OutputNames, c.execStmt.Ctx.GetSessionVars().CurrentDB)
	}
	return c.fields
}

func (c *chunkRowRecordSet) Next(_ context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !chk.IsFull() && c.idx < len(c.rows) {
		numToAppend := min(len(c.rows)-c.idx, chk.RequiredRows()-chk.NumRows())
		chk.AppendRows(c.rows[c.idx : c.idx+numToAppend])
		c.idx += numToAppend
	}
	return nil
}

func (c *chunkRowRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	if alloc == nil {
		return exec.NewFirstChunk(c.e)
	}

	return alloc.Alloc(c.e.RetFieldTypes(), c.e.InitCap(), c.e.MaxChunkSize())
}

func (c *chunkRowRecordSet) Close() error {
	c.execStmt.CloseRecordSet(c.execStmt.Ctx.GetSessionVars().TxnCtx.StartTS, nil)
	return nil
}

func (a *ExecStmt) handlePessimisticSelectForUpdate(ctx context.Context, e exec.Executor) (_ sqlexec.RecordSet, retErr error) {
	if snapshotTS := a.Ctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		terror.Log(exec.Close(e))
		return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
	}

	txnManager := sessiontxn.GetTxnManager(a.Ctx)
	err := txnManager.OnPessimisticStmtStart(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		isSuccessful := retErr == nil
		err1 := txnManager.OnPessimisticStmtEnd(ctx, isSuccessful)
		if retErr == nil && err1 != nil {
			retErr = err1
		}
	}()

	isFirstAttempt := true

	for {
		startTime := time.Now()
		rs, err := a.runPessimisticSelectForUpdate(ctx, e)

		if isFirstAttempt {
			executor_metrics.SelectForUpdateFirstAttemptDuration.Observe(time.Since(startTime).Seconds())
			isFirstAttempt = false
		} else {
			executor_metrics.SelectForUpdateRetryDuration.Observe(time.Since(startTime).Seconds())
		}

		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			return nil, err
		}
		if e == nil {
			return rs, nil
		}

		failpoint.Inject("pessimisticSelectForUpdateRetry", nil)
	}
}

func (a *ExecStmt) runPessimisticSelectForUpdate(ctx context.Context, e exec.Executor) (sqlexec.RecordSet, error) {
	defer func() {
		terror.Log(exec.Close(e))
	}()
	var rows []chunk.Row
	var err error
	req := exec.TryNewCacheChunk(e)
	for {
		err = a.next(ctx, e, req)
		if err != nil {
			// Handle 'write conflict' error.
			break
		}
		if req.NumRows() == 0 {
			return &chunkRowRecordSet{rows: rows, e: e, execStmt: &a.ExecStmtRuntime}, nil
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, a.Ctx.GetSessionVars().MaxChunkSize)
	}
	return nil, err
}

func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, e exec.Executor) (sqlexec.RecordSet, error) {
	sctx := a.Ctx
	r, ctx := tracing.StartRegionEx(ctx, "executor.handleNoDelayExecutor")
	defer r.End()

	var err error
	defer func() {
		terror.Log(exec.Close(e))
		a.logAudit()
	}()

	// Check if "tidb_snapshot" is set for the write executors.
	// In history read mode, we can not do write operations.
	switch e.(type) {
	case *DeleteExec, *InsertExec, *UpdateExec, *ReplaceExec, *LoadDataExec, *DDLExec, *ImportIntoExec:
		snapshotTS := sctx.GetSessionVars().SnapshotTS
		if snapshotTS != 0 {
			return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
		}
		lowResolutionTSO := sctx.GetSessionVars().LowResolutionTSO
		if lowResolutionTSO {
			return nil, errors.New("can not execute write statement when 'tidb_low_resolution_tso' is set")
		}
	}

	err = a.next(ctx, e, exec.TryNewCacheChunk(e))
	if err != nil {
		return nil, err
	}
	err = a.handleStmtForeignKeyTrigger(ctx, e)
	return nil, err
}

func (a *ExecStmt) handlePessimisticDML(ctx context.Context, e exec.Executor) (err error) {
	sctx := a.Ctx
	// Do not activate the transaction here.
	// When autocommit = 0 and transaction in pessimistic mode,
	// statements like set xxx = xxx; should not active the transaction.
	txn, err := sctx.Txn(false)
	if err != nil {
		return err
	}
	txnCtx := sctx.GetSessionVars().TxnCtx
	defer func() {
		if err != nil && !sctx.GetSessionVars().ConstraintCheckInPlacePessimistic && sctx.GetSessionVars().InTxn() {
			// If it's not a retryable error, rollback current transaction instead of rolling back current statement like
			// in normal transactions, because we cannot locate and rollback the statement that leads to the lock error.
			// This is too strict, but since the feature is not for everyone, it's the easiest way to guarantee safety.
			stmtText := parser.Normalize(a.OriginText(), sctx.GetSessionVars().EnableRedactLog)
			logutil.Logger(ctx).Info("Transaction abort for the safety of lazy uniqueness check. "+
				"Note this may not be a uniqueness violation.",
				zap.Error(err),
				zap.String("statement", stmtText),
				zap.Uint64("conn", sctx.GetSessionVars().ConnectionID),
				zap.Uint64("txnStartTS", txnCtx.StartTS),
				zap.Uint64("forUpdateTS", txnCtx.GetForUpdateTS()),
			)
			sctx.GetSessionVars().SetInTxn(false)
			err = exeerrors.ErrLazyUniquenessCheckFailure.GenWithStackByArgs(err.Error())
		}
	}()

	txnManager := sessiontxn.GetTxnManager(a.Ctx)
	err = txnManager.OnPessimisticStmtStart(ctx)
	if err != nil {
		return err
	}
	defer func() {
		isSuccessful := err == nil
		err1 := txnManager.OnPessimisticStmtEnd(ctx, isSuccessful)
		if err == nil && err1 != nil {
			err = err1
		}
	}()

	isFirstAttempt := true

	for {
		if !isFirstAttempt {
			failpoint.Inject("pessimisticDMLRetry", nil)
		}

		startTime := time.Now()
		_, err = a.handleNoDelayExecutor(ctx, e)
		if !txn.Valid() {
			return err
		}

		if isFirstAttempt {
			executor_metrics.DmlFirstAttemptDuration.Observe(time.Since(startTime).Seconds())
			isFirstAttempt = false
		} else {
			executor_metrics.DmlRetryDuration.Observe(time.Since(startTime).Seconds())
		}

		if err != nil {
			// It is possible the DML has point get plan that locks the key.
			e, err = a.handlePessimisticLockError(ctx, err)
			if err != nil {
				if exeerrors.ErrDeadlock.Equal(err) {
					metrics.StatementDeadlockDetectDuration.Observe(time.Since(startTime).Seconds())
				}
				return err
			}
			continue
		}
		keys, err1 := txn.(pessimisticTxn).KeysNeedToLock()
		if err1 != nil {
			return err1
		}
		keys = txnCtx.CollectUnchangedKeysForLock(keys)
		if len(keys) == 0 {
			return nil
		}
		keys = filterTemporaryTableKeys(sctx.GetSessionVars(), keys)
		seVars := sctx.GetSessionVars()
		keys = filterLockTableKeys(seVars.StmtCtx, keys)
		lockCtx, err := newLockCtx(sctx, seVars.LockWaitTimeout, len(keys))
		if err != nil {
			return err
		}
		var lockKeyStats *util.LockKeysDetails
		ctx = context.WithValue(ctx, util.LockKeysDetailCtxKey, &lockKeyStats)
		startLocking := time.Now()
		err = txn.LockKeys(ctx, lockCtx, keys...)
		a.phaseLockDurations[0] += time.Since(startLocking)
		if e.RuntimeStats() != nil {
			e.RuntimeStats().Record(time.Since(startLocking), 0)
		}
		if lockKeyStats != nil {
			seVars.StmtCtx.MergeLockKeysExecDetails(lockKeyStats)
		}
		if err == nil {
			return nil
		}
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			// todo: Report deadlock
			if exeerrors.ErrDeadlock.Equal(err) {
				metrics.StatementDeadlockDetectDuration.Observe(time.Since(startLocking).Seconds())
			}
			return err
		}
	}
}

// handlePessimisticLockError updates TS and rebuild executor if the err is write conflict.
func (a *ExecStmt) handlePessimisticLockError(ctx context.Context, lockErr error) (_ exec.Executor, err error) {
	if lockErr == nil {
		return nil, nil
	}
	failpoint.Inject("assertPessimisticLockErr", func() {
		if terror.ErrorEqual(kv.ErrWriteConflict, lockErr) {
			sessiontxn.AddAssertEntranceForLockError(a.Ctx, "errWriteConflict")
		} else if terror.ErrorEqual(kv.ErrKeyExists, lockErr) {
			sessiontxn.AddAssertEntranceForLockError(a.Ctx, "errDuplicateKey")
		}
	})

	defer func() {
		if _, ok := errors.Cause(err).(*tikverr.ErrDeadlock); ok {
			err = exeerrors.ErrDeadlock
		}
	}()

	txnManager := sessiontxn.GetTxnManager(a.Ctx)
	action, err := txnManager.OnStmtErrorForNextAction(ctx, sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	if err != nil {
		return nil, err
	}

	if action != sessiontxn.StmtActionRetryReady {
		return nil, lockErr
	}

	if a.retryCount >= config.GetGlobalConfig().PessimisticTxn.MaxRetryCount {
		return nil, errors.New("pessimistic lock retry limit reached")
	}
	a.retryCount++
	a.retryStartTime = time.Now()

	err = txnManager.OnStmtRetry(ctx)
	if err != nil {
		return nil, err
	}

	// Without this line of code, the result will still be correct. But it can ensure that the update time of for update read
	// is determined which is beneficial for testing.
	if _, err = txnManager.GetStmtForUpdateTS(); err != nil {
		return nil, err
	}

	breakpoint.Inject(a.Ctx, sessiontxn.BreakPointOnStmtRetryAfterLockError)

	a.resetPhaseDurations()

	a.inheritContextFromExecuteStmt()
	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}
	// Rollback the statement change before retry it.
	a.Ctx.StmtRollback(ctx, true)
	a.Ctx.GetSessionVars().StmtCtx.ResetForRetry()
	a.Ctx.GetSessionVars().RetryInfo.ResetOffset()

	failpoint.Inject("assertTxnManagerAfterPessimisticLockErrorRetry", func() {
		sessiontxn.RecordAssert(a.Ctx, "assertTxnManagerAfterPessimisticLockErrorRetry", true)
	})

	if err = a.openExecutor(ctx, e); err != nil {
		return nil, err
	}
	return e, nil
}

type pessimisticTxn interface {
	kv.Transaction
	// KeysNeedToLock returns the keys need to be locked.
	KeysNeedToLock() ([]kv.Key, error)
}

// buildExecutor build an executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor() (exec.Executor, error) {
	defer func(start time.Time) { a.phaseBuildDurations[0] += time.Since(start) }(time.Now())
	ctx := a.Ctx
	stmtCtx := ctx.GetSessionVars().StmtCtx
	if _, ok := a.Plan.(*plannercore.Execute); !ok {
		if stmtCtx.Priority == mysql.NoPriority && a.LowerPriority {
			stmtCtx.Priority = kv.PriorityLow
		}
	}
	if _, ok := a.Plan.(*plannercore.Analyze); ok && ctx.GetSessionVars().InRestrictedSQL {
		ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityLow
	}

	b := newExecutorBuilder(ctx, a.InfoSchema)
	e := b.build(a.Plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	failpoint.Inject("assertTxnManagerAfterBuildExecutor", func() {
		sessiontxn.RecordAssert(a.Ctx, "assertTxnManagerAfterBuildExecutor", true)
		sessiontxn.AssertTxnManagerInfoSchema(b.ctx, b.is)
	})

	// ExecuteExec is not a real Executor, we only use it to build another Executor from a prepared statement.
	if executorExec, ok := e.(*ExecuteExec); ok {
		err := executorExec.Build(b)
		if err != nil {
			return nil, err
		}
		if executorExec.lowerPriority {
			ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityLow
		}
		e = executorExec.stmtExec
	}
	a.isSelectForUpdate = b.hasLock && (!stmtCtx.InDeleteStmt && !stmtCtx.InUpdateStmt && !stmtCtx.InInsertStmt)
	return e, nil
}

func (a *ExecStmt) openExecutor(ctx context.Context, e exec.Executor) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = util2.GetRecoverError(r)
		}
	}()
	start := time.Now()
	err = exec.Open(ctx, e)
	a.phaseOpenDurations[0] += time.Since(start)
	return err
}

func (a *ExecStmt) resetPhaseDurations() {
	a.phaseBuildDurations[1] += a.phaseBuildDurations[0]
	a.phaseBuildDurations[0] = 0
	a.phaseOpenDurations[1] += a.phaseOpenDurations[0]
	a.phaseOpenDurations[0] = 0
	a.phaseNextDurations[1] += a.phaseNextDurations[0]
	a.phaseNextDurations[0] = 0
	a.phaseLockDurations[1] += a.phaseLockDurations[0]
	a.phaseLockDurations[0] = 0
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

// FormatSQL is used to format the original SQL, e.g. truncating long SQL, appending prepared arguments.
func FormatSQL(sql string) stringutil.StringerFunc {
	return func() string {
		length := len(sql)
		maxQueryLen := variable.QueryLogMaxLen.Load()
		if maxQueryLen <= 0 {
			return QueryReplacer.Replace(sql) // no limit
		}
		if int32(length) > maxQueryLen {
			var result strings.Builder
			result.Grow(int(maxQueryLen))
			result.WriteString(sql[:maxQueryLen])
			fmt.Fprintf(&result, "(len:%d)", length)
			return QueryReplacer.Replace(result.String())
		}
		return QueryReplacer.Replace(sql)
	}
}

func getPhaseDurationObserver(phase string, internal bool) prometheus.Observer {
	if internal {
		if ob, found := executor_metrics.PhaseDurationObserverMapInternal[phase]; found {
			return ob
		}
		return executor_metrics.ExecUnknownInternal
	}
	if ob, found := executor_metrics.PhaseDurationObserverMap[phase]; found {
		return ob
	}
	return executor_metrics.ExecUnknown
}

// Clean CTE storage shared by different CTEFullScan executor within a SQL stmt.
// Will return err in two situations:
// 1. Got err when remove disk spill file.
// 2. Some logical error like ref count of CTEStorage is less than 0.
func resetCTEStorageMap(se sessionctx.Context) error {
	tmp := se.GetSessionVars().StmtCtx.CTEStorageMap
	if tmp == nil {
		// Close() is already called, so no need to reset. Such as TraceExec.
		return nil
	}
	storageMap, ok := tmp.(map[int]*CTEStorages)
	if !ok {
		return errors.New("type assertion for CTEStorageMap failed")
	}
	for _, v := range storageMap {
		v.ResTbl.Lock()
		err1 := v.ResTbl.DerefAndClose()
		// Make sure we do not hold the lock for longer than necessary.
		v.ResTbl.Unlock()
		// No need to lock IterInTbl.
		err2 := v.IterInTbl.DerefAndClose()
		if err1 != nil {
			return err1
		}
		if err2 != nil {
			return err2
		}
	}
	se.GetSessionVars().StmtCtx.CTEStorageMap = nil
	return nil
}

func extractMsgFromSQLWarn(sqlWarn *contextutil.SQLWarn) string {
	// Currently, this function is only used in collectWarningsForSlowLog.
	// collectWarningsForSlowLog can make sure SQLWarn is not nil so no need to add a nil check here.
	warn := errors.Cause(sqlWarn.Err)
	if x, ok := warn.(*terror.Error); ok && x != nil {
		sqlErr := terror.ToSQLError(x)
		return sqlErr.Message
	}
	return warn.Error()
}

func collectWarningsForSlowLog(stmtCtx *stmtctx.StatementContext) []variable.JSONSQLWarnForSlowLog {
	warnings := stmtCtx.GetWarnings()
	extraWarnings := stmtCtx.GetExtraWarnings()
	res := make([]variable.JSONSQLWarnForSlowLog, len(warnings)+len(extraWarnings))
	for i := range warnings {
		res[i].Level = warnings[i].Level
		res[i].Message = extractMsgFromSQLWarn(&warnings[i])
	}
	for i := range extraWarnings {
		res[len(warnings)+i].Level = extraWarnings[i].Level
		res[len(warnings)+i].Message = extractMsgFromSQLWarn(&extraWarnings[i])
		res[len(warnings)+i].IsExtra = true
	}
	return res
}

// GetResultRowsCount gets the count of the statement result rows.
func GetResultRowsCount(stmtCtx *stmtctx.StatementContext, p base.Plan) int64 {
	runtimeStatsColl := stmtCtx.RuntimeStatsColl
	if runtimeStatsColl == nil {
		return 0
	}
	rootPlanID := p.ID()
	if !runtimeStatsColl.ExistsRootStats(rootPlanID) {
		return 0
	}
	rootStats := runtimeStatsColl.GetRootStats(rootPlanID)
	return rootStats.GetActRows()
}

// getFlatPlan generates a FlatPhysicalPlan from the plan stored in stmtCtx.plan,
// then stores it in stmtCtx.flatPlan.
func getFlatPlan(stmtCtx *stmtctx.StatementContext) *plannercore.FlatPhysicalPlan {
	pp := stmtCtx.GetPlan()
	if pp == nil {
		return nil
	}
	if flat := stmtCtx.GetFlatPlan(); flat != nil {
		f := flat.(*plannercore.FlatPhysicalPlan)
		return f
	}
	p := pp.(base.Plan)
	flat := plannercore.FlattenPhysicalPlan(p, false)
	if flat != nil {
		stmtCtx.SetFlatPlan(flat)
		return flat
	}
	return nil
}

func getBinaryPlan(sCtx sessionctx.Context) string {
	stmtCtx := sCtx.GetSessionVars().StmtCtx
	binaryPlan := stmtCtx.GetBinaryPlan()
	if len(binaryPlan) > 0 {
		return binaryPlan
	}
	flat := getFlatPlan(stmtCtx)
	binaryPlan = plannercore.BinaryPlanStrFromFlatPlan(sCtx.GetPlanCtx(), flat)
	stmtCtx.SetBinaryPlan(binaryPlan)
	return binaryPlan
}

// getPlanTree will try to get the select plan tree if the plan is select or the select plan of delete/update/insert statement.
func getPlanTree(stmtCtx *stmtctx.StatementContext) string {
	cfg := config.GetGlobalConfig()
	if atomic.LoadUint32(&cfg.Instance.RecordPlanInSlowLog) == 0 {
		return ""
	}
	planTree, _ := getEncodedPlan(stmtCtx, false)
	if len(planTree) == 0 {
		return planTree
	}
	return variable.SlowLogPlanPrefix + planTree + variable.SlowLogPlanSuffix
}

// GetPlanDigest will try to get the select plan tree if the plan is select or the select plan of delete/update/insert statement.
func GetPlanDigest(stmtCtx *stmtctx.StatementContext) (string, *parser.Digest) {
	normalized, planDigest := stmtCtx.GetPlanDigest()
	if len(normalized) > 0 && planDigest != nil {
		return normalized, planDigest
	}
	flat := getFlatPlan(stmtCtx)
	normalized, planDigest = plannercore.NormalizeFlatPlan(flat)
	stmtCtx.SetPlanDigest(normalized, planDigest)
	return normalized, planDigest
}

// GetEncodedPlan returned same as getEncodedPlan
func GetEncodedPlan(stmtCtx *stmtctx.StatementContext, genHint bool) (encodedPlan, hintStr string) {
	return getEncodedPlan(stmtCtx, genHint)
}

// getEncodedPlan gets the encoded plan, and generates the hint string if indicated.
func getEncodedPlan(stmtCtx *stmtctx.StatementContext, genHint bool) (encodedPlan, hintStr string) {
	var hintSet bool
	encodedPlan = stmtCtx.GetEncodedPlan()
	hintStr, hintSet = stmtCtx.GetPlanHint()
	if len(encodedPlan) > 0 && (!genHint || hintSet) {
		return
	}
	flat := getFlatPlan(stmtCtx)
	if len(encodedPlan) == 0 {
		encodedPlan = plannercore.EncodeFlatPlan(flat)
		stmtCtx.SetEncodedPlan(encodedPlan)
	}
	if genHint {
		hints := plannercore.GenHintsFromFlatPlan(flat)
		for _, tableHint := range stmtCtx.OriginalTableHints {
			// some hints like 'memory_quota' cannot be extracted from the PhysicalPlan directly,
			// so we have to iterate all hints from the customer and keep some other necessary hints.
			switch tableHint.HintName.L {
			case hint.HintMemoryQuota, hint.HintUseToja, hint.HintNoIndexMerge,
				hint.HintMaxExecutionTime, hint.HintIgnoreIndex, hint.HintReadFromStorage,
				hint.HintMerge, hint.HintSemiJoinRewrite, hint.HintNoDecorrelate:
				hints = append(hints, tableHint)
			}
		}

		hintStr = hint.RestoreOptimizerHints(hints)
		stmtCtx.SetPlanHint(hintStr)
	}
	return
}

// GetTextToLog return the query text to log.
func (a *ExecStmt) GetTextToLog(keepHint bool) string {
	var sql string
	sessVars := a.Ctx.GetSessionVars()
	rmode := sessVars.EnableRedactLog
	if rmode == errors.RedactLogEnable {
		if keepHint {
			sql = parser.NormalizeKeepHint(sessVars.StmtCtx.OriginalSQL)
		} else {
			sql, _ = sessVars.StmtCtx.SQLDigest()
		}
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		sql = sensitiveStmt.SecureText()
	} else {
		sql = redact.String(rmode, sessVars.StmtCtx.OriginalSQL+sessVars.PlanCacheParams.String())
	}
	return sql
}

func (a *ExecStmt) observeStmtBeginForTopSQL(ctx context.Context) context.Context {
	vars := a.Ctx.GetSessionVars()
	sc := vars.StmtCtx
	normalizedSQL, sqlDigest := sc.SQLDigest()
	normalizedPlan, planDigest := GetPlanDigest(sc)
	var sqlDigestByte, planDigestByte []byte
	if sqlDigest != nil {
		sqlDigestByte = sqlDigest.Bytes()
	}
	if planDigest != nil {
		planDigestByte = planDigest.Bytes()
	}
	stats := a.Ctx.GetStmtStats()
	if !topsqlstate.TopSQLEnabled() {
		// To reduce the performance impact on fast plan.
		// Drop them does not cause notable accuracy issue in TopSQL.
		if IsFastPlan(a.Plan) {
			return ctx
		}
		// Always attach the SQL and plan info uses to catch the running SQL when Top SQL is enabled in execution.
		if stats != nil {
			stats.OnExecutionBegin(sqlDigestByte, planDigestByte)
		}
		return topsql.AttachSQLAndPlanInfo(ctx, sqlDigest, planDigest)
	}

	if stats != nil {
		stats.OnExecutionBegin(sqlDigestByte, planDigestByte)
		// This is a special logic prepared for TiKV's SQLExecCount.
		sc.KvExecCounter = stats.CreateKvExecCounter(sqlDigestByte, planDigestByte)
	}

	isSQLRegistered := sc.IsSQLRegistered.Load()
	if !isSQLRegistered {
		topsql.RegisterSQL(normalizedSQL, sqlDigest, vars.InRestrictedSQL)
	}
	sc.IsSQLAndPlanRegistered.Store(true)
	if len(normalizedPlan) == 0 {
		return ctx
	}
	topsql.RegisterPlan(normalizedPlan, planDigest)
	return topsql.AttachSQLAndPlanInfo(ctx, sqlDigest, planDigest)
}

// only allow select/delete/update/insert/execute stmt captured by continues capture
func checkPlanReplayerContinuesCaptureValidStmt(stmtNode ast.StmtNode) bool {
	switch stmtNode.(type) {
	case *ast.SelectStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt, *ast.ExecuteStmt:
		return true
	default:
		return false
	}
}

func checkPlanReplayerCaptureTask(sctx sessionctx.Context, stmtNode ast.StmtNode, startTS uint64) {
	dom := domain.GetDomain(sctx)
	if dom == nil {
		return
	}
	handle := dom.GetPlanReplayerHandle()
	if handle == nil {
		return
	}
	tasks := handle.GetTasks()
	if len(tasks) == 0 {
		return
	}
	_, sqlDigest := sctx.GetSessionVars().StmtCtx.SQLDigest()
	_, planDigest := sctx.GetSessionVars().StmtCtx.GetPlanDigest()
	if sqlDigest == nil || planDigest == nil {
		return
	}
	key := replayer.PlanReplayerTaskKey{
		SQLDigest:  sqlDigest.String(),
		PlanDigest: planDigest.String(),
	}
	for _, task := range tasks {
		if task.SQLDigest == sqlDigest.String() {
			if task.PlanDigest == "*" || task.PlanDigest == planDigest.String() {
				sendPlanReplayerDumpTask(key, sctx, stmtNode, startTS, false)
				return
			}
		}
	}
}

func checkPlanReplayerContinuesCapture(sctx sessionctx.Context, stmtNode ast.StmtNode, startTS uint64) {
	dom := domain.GetDomain(sctx)
	if dom == nil {
		return
	}
	handle := dom.GetPlanReplayerHandle()
	if handle == nil {
		return
	}
	_, sqlDigest := sctx.GetSessionVars().StmtCtx.SQLDigest()
	_, planDigest := sctx.GetSessionVars().StmtCtx.GetPlanDigest()
	key := replayer.PlanReplayerTaskKey{
		SQLDigest:  sqlDigest.String(),
		PlanDigest: planDigest.String(),
	}
	existed := sctx.GetSessionVars().CheckPlanReplayerFinishedTaskKey(key)
	if existed {
		return
	}
	sendPlanReplayerDumpTask(key, sctx, stmtNode, startTS, true)
	sctx.GetSessionVars().AddPlanReplayerFinishedTaskKey(key)
}

func sendPlanReplayerDumpTask(key replayer.PlanReplayerTaskKey, sctx sessionctx.Context, stmtNode ast.StmtNode,
	startTS uint64, isContinuesCapture bool) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	handle := sctx.Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
	bindings := handle.GetAllSessionBindings()
	dumpTask := &domain.PlanReplayerDumpTask{
		PlanReplayerTaskKey: key,
		StartTS:             startTS,
		TblStats:            stmtCtx.TableStats,
		SessionBindings:     []bindinfo.Bindings{bindings},
		SessionVars:         sctx.GetSessionVars(),
		ExecStmts:           []ast.StmtNode{stmtNode},
		DebugTrace:          []any{stmtCtx.OptimizerDebugTrace},
		Analyze:             false,
		IsCapture:           true,
		IsContinuesCapture:  isContinuesCapture,
	}
	dumpTask.EncodedPlan, _ = GetEncodedPlan(stmtCtx, false)
	if execStmtAst, ok := stmtNode.(*ast.ExecuteStmt); ok {
		planCacheStmt, err := plannercore.GetPreparedStmt(execStmtAst, sctx.GetSessionVars())
		if err != nil {
			logutil.BgLogger().Warn("fail to find prepared ast for dumping plan replayer", zap.String("category", "plan-replayer-capture"),
				zap.String("sqlDigest", key.SQLDigest),
				zap.String("planDigest", key.PlanDigest),
				zap.Error(err))
		} else {
			dumpTask.ExecStmts = []ast.StmtNode{planCacheStmt.PreparedAst.Stmt}
		}
	}
	domain.GetDomain(sctx).GetPlanReplayerHandle().SendTask(dumpTask)
}
