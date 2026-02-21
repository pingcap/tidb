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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/staticrecordset"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/types"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/breakpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util"
	tikvtrace "github.com/tikv/client-go/v2/trace"
	"go.uber.org/zap"
)

// processinfoSetter is the interface use to set current running process info.
type processinfoSetter interface {
	SetProcessInfo(string, time.Time, byte, uint64)
	UpdateProcessInfo()
}

// recordSet wraps an executor, implements sqlexec.RecordSet interface
type recordSet struct {
	fields   []*resolve.ResultField
	executor exec.Executor
	// The `Fields` method may be called after `Close`, and the executor is cleared in the `Close` function.
	// Therefore, we need to store the schema in `recordSet` to avoid a null pointer exception when calling `executor.Schema()`.
	schema     *expression.Schema
	stmt       *ExecStmt
	lastErrs   []error
	txnStartTS uint64
	once       sync.Once
	// traceID stores the trace ID for this statement execution.
	// It's injected into the context during Next() to ensure trace correlation
	// across TiDB -> client-go -> TiKV during lazy execution.
	traceID []byte
}

func (a *recordSet) Fields() []*resolve.ResultField {
	if len(a.fields) == 0 {
		a.fields = colNames2ResultFields(a.schema, a.stmt.OutputNames, a.stmt.Ctx.GetSessionVars().CurrentDB)
	}
	return a.fields
}

func colNames2ResultFields(schema *expression.Schema, names []*types.FieldName, defaultDB string) []*resolve.ResultField {
	rfs := make([]*resolve.ResultField, 0, schema.Len())
	defaultDBCIStr := ast.NewCIStr(defaultDB)
	for i := range schema.Len() {
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
		rf := &resolve.ResultField{
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
		logutil.Logger(ctx).Warn("execute sql panic", zap.String("sql", a.stmt.GetTextToLog(false)), zap.Stack("stack"))
	}()
	if a.stmt != nil {
		if err := a.stmt.Ctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return err
		}
	}

	// Inject trace ID into context for correlation during lazy execution.
	// The trace ID was generated when the statement started executing, but the
	// context passed to Next() doesn't have it (context is not stored in recordSet).
	// We need to re-inject it here so that operations in executors (e.g., TiKV calls)
	// can be correlated with this statement's trace ID.
	if len(a.traceID) > 0 {
		ctx = tikvtrace.ContextWithTraceID(ctx, a.traceID)
	}

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
		a.executor = nil
		if a.stmt != nil {
			status := a.stmt.Ctx.GetSessionVars().SQLKiller.GetKillSignal()
			inWriteResultSet := a.stmt.Ctx.GetSessionVars().SQLKiller.InWriteResultSet.Load()
			if status > 0 && inWriteResultSet {
				logutil.BgLogger().Warn("kill, this SQL might be stuck in the network stack while writing packets to the client.", zap.Uint64("connection ID", a.stmt.Ctx.GetSessionVars().ConnectionID))
			}
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

// TryDetach creates a new `RecordSet` which doesn't depend on the current session context.
func (a *recordSet) TryDetach() (sqlexec.RecordSet, bool, error) {
	e, ok := Detach(a.executor)
	if !ok {
		return nil, false, nil
	}
	return staticrecordset.New(a.Fields(), e, a.stmt.GetTextToLog(false)), true, nil
}

// GetExecutor4Test exports the internal executor for test purpose.
func (a *recordSet) GetExecutor4Test() any {
	return a.executor
}

// TelemetryInfo records some telemetry information during execution.
type TelemetryInfo struct {
	UseNonRecursive       bool
	UseRecursive          bool
	UseMultiSchemaChange  bool
	UseExchangePartition  bool
	UseFlashbackToCluster bool
	PartitionTelemetry    *PartitionTelemetryInfo
	AccountLockTelemetry  *AccountLockTelemetryInfo
	UseIndexMerge         bool
	UseTableLookUp        atomic.Bool
}

// PartitionTelemetryInfo records table partition telemetry information during execution.
type PartitionTelemetryInfo struct {
	UseTablePartition                bool
	UseTablePartitionList            bool
	UseTablePartitionRange           bool
	UseTablePartitionHash            bool
	UseTablePartitionRangeColumns    bool
	UseTablePartitionRangeColumnsGt1 bool
	UseTablePartitionRangeColumnsGt2 bool
	UseTablePartitionRangeColumnsGt3 bool
	UseTablePartitionListColumns     bool
	TablePartitionMaxPartitionsNum   uint64
	UseCreateIntervalPartition       bool
	UseAddIntervalPartition          bool
	UseDropIntervalPartition         bool
	UseCompactTablePartition         bool
	UseReorganizePartition           bool
}

// AccountLockTelemetryInfo records account lock/unlock information during execution
type AccountLockTelemetryInfo struct {
	// The number of CREATE/ALTER USER statements that lock the user
	LockUser int64
	// The number of CREATE/ALTER USER statements that unlock the user
	UnlockUser int64
	// The number of CREATE/ALTER USER statements
	CreateOrAlterUser int64
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
type ExecStmt struct {
	// GoCtx stores parent go context.Context for a stmt.
	GoCtx context.Context
	// InfoSchema stores a reference to the schema information.
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	Plan base.Plan

	StmtNode ast.StmtNode

	Ctx sessionctx.Context

	// LowerPriority represents whether to lower the execution priority of a query.
	LowerPriority     bool
	isPreparedStmt    bool
	isSelectForUpdate bool
	retryCount        uint
	retryStartTime    time.Time

	// Phase durations are splited into two parts: 1. trying to lock keys (but
	// failed); 2. the final iteration of the retry loop. Here we use
	// [2]time.Duration to record such info for each phase. The first duration
	// is increased only within the current iteration. When we meet a
	// pessimistic lock error and decide to retry, we add the first duration to
	// the second and reset the first to 0 by calling `resetPhaseDurations`.
	phaseBuildDurations [2]time.Duration
	phaseOpenDurations  [2]time.Duration
	phaseNextDurations  [2]time.Duration
	phaseLockDurations  [2]time.Duration

	// OutputNames will be set if using cached plan
	OutputNames []*types.FieldName
	PsStmt      *plannercore.PlanCacheStmt
	Ti          *TelemetryInfo
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
		r.Span.LogKV("sql", a.Text())
	}

	failpoint.Inject("assertTxnManagerInShortPointGetPlan", func() {
		sessiontxn.RecordAssert(a.Ctx, "assertTxnManagerInShortPointGetPlan", true)
		// stale read should not reach here
		staleread.AssertStmtStaleness(a.Ctx, false)
		sessiontxn.AssertTxnManagerInfoSchema(a.Ctx, a.InfoSchema)
	})

	ctx = a.observeStmtBeginForTopProfiling(ctx)
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
			pointGetPlan := a.Plan.(*physicalop.PointGetPlan)
			exec.Recreated(pointGetPlan, a.Ctx)
			a.PsStmt.PointGet.Executor = exec
			executor = exec
			// If reuses the executor, the executor build phase is skipped, and the txn will not be activated that
			// caused `TxnCtx.StartTS` to be 0.
			// So we should set the `TxnCtx.StartTS` manually here to make sure it is not 0
			// to provide the right value for `@@tidb_last_txn_info` or other variables.
			a.Ctx.GetSessionVars().TxnCtx.StartTS = startTs
		}
	}

	if executor == nil {
		b := newExecutorBuilder(a.Ctx, a.InfoSchema, a.Ti)
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
		sql := a.Text()
		maxExecutionTime := sctx.GetSessionVars().GetMaxExecutionTime()
		// Update processinfo, ShowProcess() will use it.
		pi.SetProcessInfo(sql, time.Now(), cmd, maxExecutionTime)
		if sctx.GetSessionVars().StmtCtx.StmtType == "" {
			sctx.GetSessionVars().StmtCtx.StmtType = stmtctx.GetStmtLabel(ctx, a.StmtNode)
		}
	}

	// Extract trace ID from context to store in recordSet for lazy execution
	traceID := tikvtrace.TraceIDFromContext(ctx)

	return &recordSet{
		executor:   executor,
		schema:     executor.Schema(),
		stmt:       a,
		txnStartTS: startTs,
		traceID:    traceID,
	}, nil
}

// OriginText returns original statement as a string.
func (a *ExecStmt) OriginText() string {
	return a.StmtNode.OriginalText()
}

// Text returns utf8 encoded statement as a string.
func (a *ExecStmt) Text() string {
	return a.StmtNode.Text()
}

// IsPrepared returns true if stmt is a prepare statement.
func (a *ExecStmt) IsPrepared() bool {
	return a.isPreparedStmt
}

// IsReadOnly returns true if a statement is read only.
// If current StmtNode is an ExecuteStmt, we can get its prepared stmt,
// then using ast.IsReadOnly function to determine a statement is read only or not.
func (a *ExecStmt) IsReadOnly(vars *variable.SessionVars) bool {
	return plannercore.IsReadOnly(a.StmtNode, vars)
}

// RebuildPlan rebuilds current execute statement plan.
// It returns the current information schema version that 'a' is using.
func (a *ExecStmt) RebuildPlan(ctx context.Context) (int64, error) {
	ret := &plannercore.PreprocessorReturn{}
	nodeW := resolve.NewNodeW(a.StmtNode)
	if err := plannercore.Preprocess(ctx, a.Ctx, nodeW, plannercore.InTxnRetry, plannercore.InitTxnContextProvider, plannercore.WithPreprocessorReturn(ret)); err != nil {
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
	p, names, err := planner.Optimize(ctx, a.Ctx, nodeW, a.InfoSchema)
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
	if proj, ok := p.(*physicalop.PhysicalProjection); ok {
		p = proj.Children()[0]
	}
	switch p.(type) {
	case *physicalop.PointGetPlan:
		return true
	case *physicalop.PhysicalTableDual:
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
			execDetails := a.Ctx.GetSessionVars().StmtCtx.GetExecDetails()
			if execDetails.LockKeysDetail != nil {
				if execDetails.LockKeysDetail.LockKeys > 0 {
					metrics.StatementLockKeysCount.Observe(float64(execDetails.LockKeysDetail.LockKeys))
				}
				if a.Ctx.GetSessionVars().StmtCtx.PessimisticLockStarted() && execDetails.LockKeysDetail.TotalTime > 0 {
					metrics.PessimisticLockKeysDuration.Observe(execDetails.LockKeysDetail.TotalTime.Seconds())
				}
			}
			if execDetails.SharedLockKeysDetail != nil {
				if execDetails.SharedLockKeysDetail.LockKeys > 0 {
					metrics.StatementSharedLockKeysCount.Observe(float64(execDetails.SharedLockKeysDetail.LockKeys))
				}
			}

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
		logutil.Logger(ctx).Warn("execute sql panic", zap.String("sql", a.GetTextToLog(false)), zap.Stack("stack"))
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
		oriStats, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBBuildStatsConcurrency)
		if !ok {
			oriStats = strconv.Itoa(vardef.DefBuildStatsConcurrency)
		}
		oriScan := sctx.GetSessionVars().AnalyzeDistSQLScanConcurrency()
		oriIso, ok := sctx.GetSessionVars().GetSystemVar(vardef.TxnIsolation)
		if !ok {
			oriIso = "REPEATABLE-READ"
		}
		autoConcurrency, err1 := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, vardef.TiDBAutoBuildStatsConcurrency)
		terror.Log(err1)
		if err1 == nil {
			terror.Log(sctx.GetSessionVars().SetSystemVar(vardef.TiDBBuildStatsConcurrency, autoConcurrency))
		}
		sVal, err2 := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, vardef.TiDBSysProcScanConcurrency)
		terror.Log(err2)
		if err2 == nil {
			concurrency, err3 := strconv.ParseInt(sVal, 10, 64)
			terror.Log(err3)
			if err3 == nil {
				sctx.GetSessionVars().SetAnalyzeDistSQLScanConcurrency(int(concurrency))
			}
		}
		terror.Log(sctx.GetSessionVars().SetSystemVar(vardef.TxnIsolation, ast.ReadCommitted))
		defer func() {
			terror.Log(sctx.GetSessionVars().SetSystemVar(vardef.TiDBBuildStatsConcurrency, oriStats))
			sctx.GetSessionVars().SetAnalyzeDistSQLScanConcurrency(oriScan)
			terror.Log(sctx.GetSessionVars().SetSystemVar(vardef.TxnIsolation, oriIso))
		}()
	}

	if sctx.GetSessionVars().StmtCtx.HasMemQuotaHint {
		sctx.GetSessionVars().MemTracker.SetBytesLimit(sctx.GetSessionVars().StmtCtx.MemQuotaQuery)
	}

	// must set plan according to the `Execute` plan before getting planDigest
	a.inheritContextFromExecuteStmt()
	var rm *runaway.Manager
	dom := domain.GetDomain(sctx)
	if dom != nil {
		rm = dom.RunawayManager()
	}
	if vardef.EnableResourceControl.Load() && rm != nil {
		sessionVars := sctx.GetSessionVars()
		stmtCtx := sessionVars.StmtCtx
		_, planDigest := GetPlanDigest(stmtCtx)
		_, sqlDigest := stmtCtx.SQLDigest()
		stmtCtx.RunawayChecker = rm.DeriveChecker(stmtCtx.ResourceGroupName, stmtCtx.OriginalSQL, sqlDigest.String(), planDigest.String(), sessionVars.StartTime)
		switchGroupName, err := stmtCtx.RunawayChecker.BeforeExecutor()
		if err != nil {
			return nil, err
		}
		if len(switchGroupName) > 0 {
			stmtCtx.ResourceGroupName = switchGroupName
		}
	}
	ctx = a.observeStmtBeginForTopProfiling(ctx)

	// Record start time before buildExecutor() to include TSO waiting time in maxExecutionTime timeout.
	// buildExecutor() may block waiting for TSO, so we should start the timer earlier.
	cmd32 := atomic.LoadUint32(&sctx.GetSessionVars().CommandValue)
	cmd := byte(cmd32)
	var execStartTime time.Time
	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
		pi = raw
		execStartTime = time.Now()
	}

	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}

	if pi != nil {
		sql := a.getSQLForProcessInfo()
		maxExecutionTime := sctx.GetSessionVars().GetMaxExecutionTime()
		// Update processinfo, ShowProcess() will use it.
		if a.Ctx.GetSessionVars().StmtCtx.StmtType == "" {
			a.Ctx.GetSessionVars().StmtCtx.StmtType = stmtctx.GetStmtLabel(ctx, a.StmtNode)
		}
		// Since maxExecutionTime is used only for SELECT statements, here we limit its scope.
		if !a.Ctx.GetSessionVars().StmtCtx.InSelectStmt {
			maxExecutionTime = 0
		}
		pi.SetProcessInfo(sql, execStartTime, cmd, maxExecutionTime)
	}

	breakpoint.Inject(a.Ctx, sessiontxn.BreakPointBeforeExecutorFirstRun)
	if err = a.openExecutor(ctx, e); err != nil {
		terror.Log(exec.Close(e))
		return nil, err
	}

	isPessimistic := sctx.GetSessionVars().TxnCtx.IsPessimistic

	if a.isSelectForUpdate {
		if sctx.GetSessionVars().UseLowResolutionTSO() {
			terror.Log(exec.Close(e))
			return nil, errors.New("can not execute select for update statement when 'tidb_low_resolution_tso' is set")
		}
		// Special handle for "select for update statement" in pessimistic transaction.
		if isPessimistic {
			return a.handlePessimisticSelectForUpdate(ctx, e)
		}
	}

	a.prepareFKCascadeContext(e)
	if handled, result, err := a.handleNoDelay(ctx, e, isPessimistic); handled || err != nil {
		return result, err
	}

	var txnStartTS uint64
	txn, err := sctx.Txn(false)
	if err != nil {
		terror.Log(exec.Close(e))
		return nil, err
	}
	if txn.Valid() {
		txnStartTS = txn.StartTS()
	}

	// Extract trace ID from context to store in recordSet for lazy execution
	traceID := tikvtrace.TraceIDFromContext(ctx)

	return &recordSet{
		executor:   e,
		schema:     e.Schema(),
		stmt:       a,
		txnStartTS: txnStartTS,
		traceID:    traceID,
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
	sql := a.Text()
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
		// Since `UnionScanExec` use `SnapshotIter` and `SnapshotGetter` to read txn mem-buffer, if we don't do `StmtCommit`,
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

