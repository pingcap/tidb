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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stmtsummary"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// processinfoSetter is the interface use to set current running process info.
type processinfoSetter interface {
	SetProcessInfo(string, time.Time, byte, uint64)
}

// recordSet wraps an executor, implements sqlexec.RecordSet interface
type recordSet struct {
	fields     []*ast.ResultField
	executor   Executor
	stmt       *ExecStmt
	lastErr    error
	txnStartTS uint64
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
		if origColName.L == "" {
			origColName = names[i].ColName
		}
		rf := &ast.ResultField{
			Column:       &model.ColumnInfo{Name: origColName, FieldType: *schema.Columns[i].RetType},
			ColumnAsName: names[i].ColName,
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
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.stmt.Text), zap.Stack("stack"))
	}()

	err = Next(ctx, a.executor, req)
	if err != nil {
		a.lastErr = err
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

// NewChunk create a chunk base on top-level executor's newFirstChunk().
func (a *recordSet) NewChunk() *chunk.Chunk {
	return newFirstChunk(a.executor)
}

func (a *recordSet) Close() error {
	err := a.executor.Close()
	// `LowSlowQuery` and `SummaryStmt` must be called before recording `PrevStmt`.
	a.stmt.LogSlowQuery(a.txnStartTS, a.lastErr == nil, false)
	a.stmt.SummaryStmt()
	sessVars := a.stmt.Ctx.GetSessionVars()
	pps := types.CloneRow(sessVars.PreparedParams)
	sessVars.PrevStmt = FormatSQL(a.stmt.OriginText(), pps)
	a.stmt.logAudit()
	// Detach the disk tracker from GlobalDiskUsageTracker after every execution
	if stmtCtx := a.stmt.Ctx.GetSessionVars().StmtCtx; stmtCtx != nil && stmtCtx.DiskTracker != nil {
		a.stmt.Ctx.GetSessionVars().StmtCtx.DiskTracker.Detach()
	}
	return err
}

// OnFetchReturned implements commandLifeCycle#OnFetchReturned
func (a *recordSet) OnFetchReturned() {
	a.stmt.LogSlowQuery(a.txnStartTS, a.lastErr == nil, true)
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
type ExecStmt struct {
	// InfoSchema stores a reference to the schema information.
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	Plan plannercore.Plan
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx sessionctx.Context

	// LowerPriority represents whether to lower the execution priority of a query.
	LowerPriority     bool
	isPreparedStmt    bool
	isSelectForUpdate bool
	retryCount        uint

	// OutputNames will be set if using cached plan
	OutputNames []*types.FieldName
	PsStmt      *plannercore.CachedPrepareStmt
}

// PointGet short path for point exec directly from plan, keep only necessary steps
func (a *ExecStmt) PointGet(ctx context.Context, is infoschema.InfoSchema) (*recordSet, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("ExecStmt.PointGet", opentracing.ChildOf(span.Context()))
		span1.LogKV("sql", a.OriginText())
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	startTs := uint64(math.MaxUint64)
	err := a.Ctx.InitTxnWithStartTS(startTs)
	if err != nil {
		return nil, err
	}
	a.Ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityHigh

	// try to reuse point get executor
	if a.PsStmt.Executor != nil {
		exec, ok := a.PsStmt.Executor.(*PointGetExecutor)
		if !ok {
			logutil.Logger(ctx).Error("invalid executor type, not PointGetExecutor for point get path")
			a.PsStmt.Executor = nil
		} else {
			// CachedPlan type is already checked in last step
			pointGetPlan := a.PsStmt.PreparedAst.CachedPlan.(*plannercore.PointGetPlan)
			exec.Init(pointGetPlan, startTs)
			a.PsStmt.Executor = exec
		}
	}
	if a.PsStmt.Executor == nil {
		b := newExecutorBuilder(a.Ctx, is)
		newExecutor := b.build(a.Plan)
		if b.err != nil {
			return nil, b.err
		}
		a.PsStmt.Executor = newExecutor
	}
	stmtNodeCounterSelect.Inc()
	pointExecutor := a.PsStmt.Executor.(*PointGetExecutor)
	if err = pointExecutor.Open(ctx); err != nil {
		terror.Call(pointExecutor.Close)
		return nil, err
	}
	return &recordSet{
		executor:   pointExecutor,
		stmt:       a,
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
	if execStmt, ok := a.StmtNode.(*ast.ExecuteStmt); ok {
		s, err := getPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Error("getPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(s)
	}
	return ast.IsReadOnly(a.StmtNode)
}

// RebuildPlan rebuilds current execute statement plan.
// It returns the current information schema version that 'a' is using.
func (a *ExecStmt) RebuildPlan(ctx context.Context) (int64, error) {
	is := infoschema.GetInfoSchema(a.Ctx)
	a.InfoSchema = is
	if err := plannercore.Preprocess(a.Ctx, a.StmtNode, is, plannercore.InTxnRetry); err != nil {
		return 0, err
	}
	p, names, err := planner.Optimize(ctx, a.Ctx, a.StmtNode, is)
	if err != nil {
		return 0, err
	}
	a.OutputNames = names
	a.Plan = p
	return is.SchemaMetaVersion(), nil
}

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
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
			return
		}
		if str, ok := r.(string); !ok || !strings.HasPrefix(str, memory.PanicMemoryExceed) {
			panic(r)
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.Text), zap.Stack("stack"))
	}()

	sctx := a.Ctx
	if _, ok := a.Plan.(*plannercore.Analyze); ok && sctx.GetSessionVars().InRestrictedSQL {
		oriStats, _ := sctx.GetSessionVars().GetSystemVar(variable.TiDBBuildStatsConcurrency)
		oriScan := sctx.GetSessionVars().DistSQLScanConcurrency
		oriIndex := sctx.GetSessionVars().IndexSerialScanConcurrency
		oriIso, _ := sctx.GetSessionVars().GetSystemVar(variable.TxnIsolation)
		terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "1"))
		sctx.GetSessionVars().DistSQLScanConcurrency = 1
		sctx.GetSessionVars().IndexSerialScanConcurrency = 1
		terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TxnIsolation, ast.ReadCommitted))
		defer func() {
			terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, oriStats))
			sctx.GetSessionVars().DistSQLScanConcurrency = oriScan
			sctx.GetSessionVars().IndexSerialScanConcurrency = oriIndex
			terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TxnIsolation, oriIso))
		}()
	}

	if sctx.GetSessionVars().StmtCtx.HasMemQuotaHint {
		sctx.GetSessionVars().StmtCtx.MemTracker.SetBytesLimit(sctx.GetSessionVars().StmtCtx.MemQuotaQuery)
	}

	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}

	if err = e.Open(ctx); err != nil {
		terror.Call(e.Close)
		return nil, err
	}

	cmd32 := atomic.LoadUint32(&sctx.GetSessionVars().CommandValue)
	cmd := byte(cmd32)
	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
		pi = raw
		sql := a.OriginText()
		if simple, ok := a.Plan.(*plannercore.Simple); ok && simple.Statement != nil {
			if ss, ok := simple.Statement.(ast.SensitiveStmtNode); ok {
				// Use SecureText to avoid leak password information.
				sql = ss.SecureText()
			}
		}
		maxExecutionTime := getMaxExecutionTime(sctx)
		// Update processinfo, ShowProcess() will use it.
		pi.SetProcessInfo(sql, time.Now(), cmd, maxExecutionTime)
		if a.Ctx.GetSessionVars().StmtCtx.StmtType == "" {
			a.Ctx.GetSessionVars().StmtCtx.StmtType = GetStmtLabel(a.StmtNode)
		}
	}

	isPessimistic := sctx.GetSessionVars().TxnCtx.IsPessimistic

	// Special handle for "select for update statement" in pessimistic transaction.
	if isPessimistic && a.isSelectForUpdate {
		return a.handlePessimisticSelectForUpdate(ctx, e)
	}

	if handled, result, err := a.handleNoDelay(ctx, e, isPessimistic); handled {
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
		stmt:       a,
		txnStartTS: txnStartTS,
	}, nil
}

func (a *ExecStmt) handleNoDelay(ctx context.Context, e Executor, isPessimistic bool) (bool, sqlexec.RecordSet, error) {
	toCheck := e
	if explain, ok := e.(*ExplainExec); ok {
		if explain.analyzeExec != nil {
			toCheck = explain.analyzeExec
		}
	}

	// If the executor doesn't return any result to the client, we execute it without delay.
	if toCheck.Schema().Len() == 0 {
		if isPessimistic {
			return true, nil, a.handlePessimisticDML(ctx, e)
		}
		r, err := a.handleNoDelayExecutor(ctx, e)
		return true, r, err
	} else if proj, ok := toCheck.(*ProjectionExec); ok && proj.calculateNoDelay {
		// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
		// the Projection has two expressions and two columns in the schema, but we should
		// not return the result of the two expressions.
		r, err := a.handleNoDelayExecutor(ctx, e)
		return true, r, err
	}

	return false, nil, nil
}

// getMaxExecutionTime get the max execution timeout value.
func getMaxExecutionTime(sctx sessionctx.Context) uint64 {
	if sctx.GetSessionVars().StmtCtx.HasMaxExecutionTime {
		return sctx.GetSessionVars().StmtCtx.MaxExecutionTime
	}
	return sctx.GetSessionVars().MaxExecutionTime
}

type chunkRowRecordSet struct {
	rows   []chunk.Row
	idx    int
	fields []*ast.ResultField
	e      Executor
}

func (c *chunkRowRecordSet) Fields() []*ast.ResultField {
	return c.fields
}

func (c *chunkRowRecordSet) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for !chk.IsFull() && c.idx < len(c.rows) {
		chk.AppendRow(c.rows[c.idx])
		c.idx++
	}
	return nil
}

func (c *chunkRowRecordSet) NewChunk() *chunk.Chunk {
	return newFirstChunk(c.e)
}

func (c *chunkRowRecordSet) Close() error {
	return nil
}

func (a *ExecStmt) handlePessimisticSelectForUpdate(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	for {
		rs, err := a.runPessimisticSelectForUpdate(ctx, e)
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			return nil, err
		}
		if e == nil {
			return rs, nil
		}
	}
}

func (a *ExecStmt) runPessimisticSelectForUpdate(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	defer func() {
		terror.Log(e.Close())
	}()
	var rows []chunk.Row
	var err error
	req := newFirstChunk(e)
	for {
		err = Next(ctx, e, req)
		if err != nil {
			// Handle 'write conflict' error.
			break
		}
		if req.NumRows() == 0 {
			fields := colNames2ResultFields(e.Schema(), a.OutputNames, a.Ctx.GetSessionVars().CurrentDB)
			return &chunkRowRecordSet{rows: rows, fields: fields, e: e}, nil
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, a.Ctx.GetSessionVars().MaxChunkSize)
	}
	return nil, err
}

func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	sctx := a.Ctx
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("executor.handleNoDelayExecutor", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Check if "tidb_snapshot" is set for the write executors.
	// In history read mode, we can not do write operations.
	switch e.(type) {
	case *DeleteExec, *InsertExec, *UpdateExec, *ReplaceExec, *LoadDataExec, *DDLExec:
		snapshotTS := sctx.GetSessionVars().SnapshotTS
		if snapshotTS != 0 {
			return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
		}
		lowResolutionTSO := sctx.GetSessionVars().LowResolutionTSO
		if lowResolutionTSO {
			return nil, errors.New("can not execute write statement when 'tidb_low_resolution_tso' is set")
		}
	}

	var err error
	defer func() {
		terror.Log(e.Close())
		a.logAudit()
	}()

	err = Next(ctx, e, newFirstChunk(e))
	if err != nil {
		return nil, err
	}
	return nil, err
}

func (a *ExecStmt) handlePessimisticDML(ctx context.Context, e Executor) error {
	sctx := a.Ctx
	// Do not active the transaction here.
	// When autocommit = 0 and transaction in pessimistic mode,
	// statements like set xxx = xxx; should not active the transaction.
	txn, err := sctx.Txn(false)
	if err != nil {
		return err
	}
	txnCtx := sctx.GetSessionVars().TxnCtx
	for {
		startPointGetLocking := time.Now()
		_, err = a.handleNoDelayExecutor(ctx, e)
		if !txn.Valid() {
			return err
		}
		if err != nil {
			// It is possible the DML has point get plan that locks the key.
			e, err = a.handlePessimisticLockError(ctx, err)
			if err != nil {
				if ErrDeadlock.Equal(err) {
					metrics.StatementDeadlockDetectDuration.Observe(time.Since(startPointGetLocking).Seconds())
				}
				return err
			}
			continue
		}
		keys, err1 := txn.(pessimisticTxn).KeysNeedToLock()
		if err1 != nil {
			return err1
		}
		keys = txnCtx.CollectUnchangedRowKeys(keys)
		if len(keys) == 0 {
			return nil
		}
		seVars := sctx.GetSessionVars()
		lockCtx := newLockCtx(seVars, seVars.LockWaitTimeout)
		startLocking := time.Now()
		err = txn.LockKeys(ctx, lockCtx, keys...)
		if err == nil {
			return nil
		}
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			if ErrDeadlock.Equal(err) {
				metrics.StatementDeadlockDetectDuration.Observe(time.Since(startLocking).Seconds())
			}
			return err
		}
	}
}

// UpdateForUpdateTS updates the ForUpdateTS, if newForUpdateTS is 0, it obtain a new TS from PD.
func UpdateForUpdateTS(seCtx sessionctx.Context, newForUpdateTS uint64) error {
	txn, err := seCtx.Txn(false)
	if err != nil {
		return err
	}
	if !txn.Valid() {
		return errors.Trace(kv.ErrInvalidTxn)
	}
	if newForUpdateTS == 0 {
		version, err := seCtx.GetStore().CurrentVersion()
		if err != nil {
			return err
		}
		newForUpdateTS = version.Ver
	}
	seCtx.GetSessionVars().TxnCtx.SetForUpdateTS(newForUpdateTS)
	txn.SetOption(kv.SnapshotTS, seCtx.GetSessionVars().TxnCtx.GetForUpdateTS())
	return nil
}

// handlePessimisticLockError updates TS and rebuild executor if the err is write conflict.
func (a *ExecStmt) handlePessimisticLockError(ctx context.Context, err error) (Executor, error) {
	txnCtx := a.Ctx.GetSessionVars().TxnCtx
	var newForUpdateTS uint64
	if deadlock, ok := errors.Cause(err).(*tikv.ErrDeadlock); ok {
		if !deadlock.IsRetryable {
			return nil, ErrDeadlock
		}
		logutil.Logger(ctx).Info("single statement deadlock, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("lockTS", deadlock.LockTs),
			zap.Stringer("lockKey", kv.Key(deadlock.LockKey)),
			zap.Uint64("deadlockKeyHash", deadlock.DeadlockKeyHash))
	} else if terror.ErrorEqual(kv.ErrWriteConflict, err) {
		errStr := err.Error()
		conflictCommitTS := extractConflictCommitTS(errStr)
		forUpdateTS := txnCtx.GetForUpdateTS()
		logutil.Logger(ctx).Info("pessimistic write conflict, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("forUpdateTS", forUpdateTS),
			zap.String("err", errStr))
		if conflictCommitTS > forUpdateTS {
			newForUpdateTS = conflictCommitTS
		}
	} else {
		// this branch if err not nil, always update forUpdateTS to avoid problem described below
		// for nowait, when ErrLock happened, ErrLockAcquireFailAndNoWaitSet will be returned, and in the same txn
		// the select for updateTs must be updated, otherwise there maybe rollback problem.
		// begin;  select for update key1(here ErrLocked or other errors(or max_execution_time like util),
		//         key1 lock not get and async rollback key1 is raised)
		//         select for update key1 again(this time lock succ(maybe lock released by others))
		//         the async rollback operation rollbacked the lock just acquired
		if err != nil {
			tsErr := UpdateForUpdateTS(a.Ctx, 0)
			if tsErr != nil {
				logutil.Logger(ctx).Warn("UpdateForUpdateTS failed", zap.Error(tsErr))
			}
		}
		return nil, err
	}
	if a.retryCount >= config.GetGlobalConfig().PessimisticTxn.MaxRetryCount {
		return nil, errors.New("pessimistic lock retry limit reached")
	}
	a.retryCount++
	err = UpdateForUpdateTS(a.Ctx, newForUpdateTS)
	if err != nil {
		return nil, err
	}
	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}
	// Rollback the statement change before retry it.
	a.Ctx.StmtRollback()
	a.Ctx.GetSessionVars().StmtCtx.ResetForRetry()

	if err = e.Open(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

func extractConflictCommitTS(errStr string) uint64 {
	strs := strings.Split(errStr, "conflictCommitTS=")
	if len(strs) != 2 {
		return 0
	}
	tsPart := strs[1]
	length := strings.IndexByte(tsPart, ',')
	if length < 0 {
		return 0
	}
	tsStr := tsPart[:length]
	ts, err := strconv.ParseUint(tsStr, 10, 64)
	if err != nil {
		return 0
	}
	return ts
}

type pessimisticTxn interface {
	kv.Transaction
	// KeysNeedToLock returns the keys need to be locked.
	KeysNeedToLock() ([]kv.Key, error)
}

// buildExecutor build a executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor() (Executor, error) {
	ctx := a.Ctx
	stmtCtx := ctx.GetSessionVars().StmtCtx
	if _, ok := a.Plan.(*plannercore.Execute); !ok {
		// Do not sync transaction for Execute statement, because the real optimization work is done in
		// "ExecuteExec.Build".
		useMaxTS, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, a.Plan)
		if err != nil {
			return nil, err
		}
		if useMaxTS {
			logutil.BgLogger().Debug("init txnStartTS with MaxUint64", zap.Uint64("conn", ctx.GetSessionVars().ConnectionID), zap.String("text", a.Text))
			err = ctx.InitTxnWithStartTS(math.MaxUint64)
		} else if ctx.GetSessionVars().SnapshotTS != 0 {
			if _, ok := a.Plan.(*plannercore.CheckTable); ok {
				err = ctx.InitTxnWithStartTS(ctx.GetSessionVars().SnapshotTS)
			}
		}
		if err != nil {
			return nil, err
		}

		if stmtPri := stmtCtx.Priority; stmtPri == mysql.NoPriority {
			switch {
			case useMaxTS:
				stmtCtx.Priority = kv.PriorityHigh
			case a.LowerPriority:
				stmtCtx.Priority = kv.PriorityLow
			}
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

	// ExecuteExec is not a real Executor, we only use it to build another Executor from a prepared statement.
	if executorExec, ok := e.(*ExecuteExec); ok {
		err := executorExec.Build(b)
		if err != nil {
			return nil, err
		}
		a.OutputNames = executorExec.outputNames
		a.isPreparedStmt = true
		a.Plan = executorExec.plan
		if executorExec.lowerPriority {
			ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityLow
		}
		e = executorExec.stmtExec
	}
	a.isSelectForUpdate = b.hasLock && (!stmtCtx.InDeleteStmt && !stmtCtx.InUpdateStmt)
	return e, nil
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

func (a *ExecStmt) logAudit() {
	sessVars := a.Ctx.GetSessionVars()
	if sessVars.InRestrictedSQL {
		return
	}
	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		audit := plugin.DeclareAuditManifest(p.Manifest)
		if audit.OnGeneralEvent != nil {
			cmd := mysql.Command2Str[byte(atomic.LoadUint32(&a.Ctx.GetSessionVars().CommandValue))]
			ctx := context.WithValue(context.Background(), plugin.ExecStartTimeCtxKey, a.Ctx.GetSessionVars().StartTime)
			audit.OnGeneralEvent(ctx, sessVars, plugin.Log, cmd)
		}
		return nil
	})
	if err != nil {
		log.Error("log audit log failure", zap.Error(err))
	}
}

// FormatSQL is used to format the original SQL, e.g. truncating long SQL, appending prepared arguments.
func FormatSQL(sql string, pps variable.PreparedParams) stringutil.StringerFunc {
	return func() string {
		cfg := config.GetGlobalConfig()
		length := len(sql)
		if maxQueryLen := atomic.LoadUint64(&cfg.Log.QueryLogMaxLen); uint64(length) > maxQueryLen {
			sql = fmt.Sprintf("%.*q(len:%d)", maxQueryLen, sql, length)
		}
		return QueryReplacer.Replace(sql) + pps.String()
	}
}

// LogSlowQuery is used to print the slow query in the log files.
func (a *ExecStmt) LogSlowQuery(txnTS uint64, succ bool, hasMoreResults bool) {
	sessVars := a.Ctx.GetSessionVars()
	level := log.GetLevel()
	cfg := config.GetGlobalConfig()
	costTime := time.Since(sessVars.StartTime) + sessVars.DurationParse
	threshold := time.Duration(cfg.Log.SlowThreshold) * time.Millisecond
	enable := cfg.Log.EnableSlowLog
	// if the level is Debug, print slow logs anyway
	if (!enable || costTime < threshold) && level > zapcore.DebugLevel {
		return
	}
	sql := FormatSQL(a.Text, sessVars.PreparedParams)

	var tableIDs, indexNames string
	if len(sessVars.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.Replace(fmt.Sprintf("%v", sessVars.StmtCtx.TableIDs), " ", ",", -1)
	}
	if len(sessVars.StmtCtx.IndexNames) > 0 {
		indexNames = strings.Replace(fmt.Sprintf("%v", sessVars.StmtCtx.IndexNames), " ", ",", -1)
	}
	execDetail := sessVars.StmtCtx.GetExecDetails()
	copTaskInfo := sessVars.StmtCtx.CopTasksDetails()
	statsInfos := plannercore.GetStatsInfo(a.Plan)
	memMax := sessVars.StmtCtx.MemTracker.MaxConsumed()
	_, digest := sessVars.StmtCtx.SQLDigest()
	_, planDigest := getPlanDigest(a.Ctx, a.Plan)
	slowItems := &variable.SlowQueryLogItems{
		TxnTS:          txnTS,
		SQL:            sql.String(),
		Digest:         digest,
		TimeTotal:      costTime,
		TimeParse:      sessVars.DurationParse,
		TimeCompile:    sessVars.DurationCompile,
		IndexNames:     indexNames,
		StatsInfos:     statsInfos,
		CopTasks:       copTaskInfo,
		ExecDetail:     execDetail,
		MemMax:         memMax,
		Succ:           succ,
		Plan:           getPlanTree(a.Plan),
		PlanDigest:     planDigest,
		Prepared:       a.isPreparedStmt,
		HasMoreResults: hasMoreResults,
	}
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok {
		slowItems.PrevStmt = sessVars.PrevStmt.String()
	}
	if costTime < threshold {
		logutil.SlowQueryLogger.Debug(sessVars.SlowLogFormat(slowItems))
	} else {
		logutil.SlowQueryLogger.Warn(sessVars.SlowLogFormat(slowItems))
		metrics.TotalQueryProcHistogram.Observe(costTime.Seconds())
		metrics.TotalCopProcHistogram.Observe(execDetail.ProcessTime.Seconds())
		metrics.TotalCopWaitHistogram.Observe(execDetail.WaitTime.Seconds())
		var userString string
		if sessVars.User != nil {
			userString = sessVars.User.String()
		}
		domain.GetDomain(a.Ctx).LogSlowQuery(&domain.SlowQueryInfo{
			SQL:        sql.String(),
			Digest:     digest,
			Start:      sessVars.StartTime,
			Duration:   costTime,
			Detail:     sessVars.StmtCtx.GetExecDetails(),
			Succ:       succ,
			ConnID:     sessVars.ConnectionID,
			TxnTS:      txnTS,
			User:       userString,
			DB:         sessVars.CurrentDB,
			TableIDs:   tableIDs,
			IndexNames: indexNames,
			Internal:   sessVars.InRestrictedSQL,
		})
	}
}

// getPlanTree will try to get the select plan tree if the plan is select or the select plan of delete/update/insert statement.
func getPlanTree(p plannercore.Plan) string {
	cfg := config.GetGlobalConfig()
	if atomic.LoadUint32(&cfg.Log.RecordPlanInSlowLog) == 0 {
		return ""
	}
	planTree := plannercore.EncodePlan(p)
	if len(planTree) == 0 {
		return planTree
	}
	return variable.SlowLogPlanPrefix + planTree + variable.SlowLogPlanSuffix
}

// getPlanDigest will try to get the select plan tree if the plan is select or the select plan of delete/update/insert statement.
func getPlanDigest(sctx sessionctx.Context, p plannercore.Plan) (normalized, planDigest string) {
	normalized, planDigest = sctx.GetSessionVars().StmtCtx.GetPlanDigest()
	if len(normalized) > 0 {
		return
	}
	normalized, planDigest = plannercore.NormalizePlan(p)
	sctx.GetSessionVars().StmtCtx.SetPlanDigest(normalized, planDigest)
	return
}

// SummaryStmt collects statements for performance_schema.events_statements_summary_by_digest
func (a *ExecStmt) SummaryStmt() {
	sessVars := a.Ctx.GetSessionVars()
	// Internal SQLs must also be recorded to keep the consistency of `PrevStmt` and `PrevStmtDigest`.
	if !stmtsummary.StmtSummaryByDigestMap.Enabled() || (sessVars.InRestrictedSQL && !stmtsummary.StmtSummaryByDigestMap.EnabledInternal()) {
		sessVars.SetPrevStmtDigest("")
		return
	}
	stmtCtx := sessVars.StmtCtx
	normalizedSQL, digest := stmtCtx.SQLDigest()
	costTime := time.Since(sessVars.StartTime)

	var prevSQL, prevSQLDigest string
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok {
		// If prevSQLDigest is not recorded, it means this `commit` is the first SQL once stmt summary is enabled,
		// so it's OK just to ignore it.
		if prevSQLDigest = sessVars.GetPrevStmtDigest(); len(prevSQLDigest) == 0 {
			return
		}
		prevSQL = sessVars.PrevStmt.String()
	}
	sessVars.SetPrevStmtDigest(digest)

	// No need to encode every time, so encode lazily.
	planGenerator := func() string {
		return plannercore.EncodePlan(a.Plan)
	}
	// Generating plan digest is slow, only generate it once if it's 'Point_Get'.
	// If it's a point get, different SQLs leads to different plans, so SQL digest
	// is enough to distinguish different plans in this case.
	var planDigest string
	var planDigestGen func() string
	if a.Plan.TP() == plancodec.TypePointGet {
		planDigestGen = func() string {
			_, planDigest := getPlanDigest(a.Ctx, a.Plan)
			return planDigest
		}
	} else {
		_, planDigest = getPlanDigest(a.Ctx, a.Plan)
	}

	execDetail := stmtCtx.GetExecDetails()
	copTaskInfo := stmtCtx.CopTasksDetails()
	memMax := stmtCtx.MemTracker.MaxConsumed()
	var userString string
	if sessVars.User != nil {
		userString = sessVars.User.Username
	}

	stmtsummary.StmtSummaryByDigestMap.AddStatement(&stmtsummary.StmtExecInfo{
		SchemaName:     strings.ToLower(sessVars.CurrentDB),
		OriginalSQL:    a.Text,
		NormalizedSQL:  normalizedSQL,
		Digest:         digest,
		PrevSQL:        prevSQL,
		PrevSQLDigest:  prevSQLDigest,
		PlanGenerator:  planGenerator,
		PlanDigest:     planDigest,
		PlanDigestGen:  planDigestGen,
		User:           userString,
		TotalLatency:   costTime,
		ParseLatency:   sessVars.DurationParse,
		CompileLatency: sessVars.DurationCompile,
		StmtCtx:        stmtCtx,
		CopTasks:       copTaskInfo,
		ExecDetail:     &execDetail,
		MemMax:         memMax,
		StartTime:      sessVars.StartTime,
		IsInternal:     sessVars.InRestrictedSQL,
	})
}
