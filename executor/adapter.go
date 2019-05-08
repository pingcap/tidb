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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// processinfoSetter is the interface use to set current running process info.
type processinfoSetter interface {
	SetProcessInfo(string, time.Time, byte)
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
		a.fields = schema2ResultFields(a.executor.Schema(), a.stmt.Ctx.GetSessionVars().CurrentDB)
	}
	return a.fields
}

func schema2ResultFields(schema *expression.Schema, defaultDB string) (rfs []*ast.ResultField) {
	rfs = make([]*ast.ResultField, 0, schema.Len())
	for _, col := range schema.Columns {
		dbName := col.DBName.O
		if dbName == "" && col.TblName.L != "" {
			dbName = defaultDB
		}
		origColName := col.OrigColName
		if origColName.L == "" {
			origColName = col.ColName
		}
		rf := &ast.ResultField{
			ColumnAsName: col.ColName,
			TableAsName:  col.TblName,
			DBName:       model.NewCIStr(dbName),
			Table:        &model.TableInfo{Name: col.OrigTblName},
			Column: &model.ColumnInfo{
				FieldType: *col.RetType,
				Name:      origColName,
			},
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
func (a *recordSet) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("recordSet.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	err := a.executor.Next(ctx, req)
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

// NewRecordBatch create a recordBatch base on top-level executor's newFirstChunk().
func (a *recordSet) NewRecordBatch() *chunk.RecordBatch {
	return chunk.NewRecordBatch(a.executor.newFirstChunk())
}

func (a *recordSet) Close() error {
	err := a.executor.Close()
	a.stmt.LogSlowQuery(a.txnStartTS, a.lastErr == nil)
	a.stmt.logAudit()
	return err
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
type ExecStmt struct {
	// InfoSchema stores a reference to the schema information.
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	Plan plannercore.Plan
	// Expensive represents whether this query is an expensive one.
	Expensive bool
	// Cacheable represents whether the physical plan can be cached.
	Cacheable bool
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx sessionctx.Context
	// StartTime stands for the starting time when executing the statement.
	StartTime      time.Time
	isPreparedStmt bool
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
			logutil.Logger(context.Background()).Error("getPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(s)
	}
	return ast.IsReadOnly(a.StmtNode)
}

// RebuildPlan rebuilds current execute statement plan.
// It returns the current information schema version that 'a' is using.
func (a *ExecStmt) RebuildPlan() (int64, error) {
	is := GetInfoSchema(a.Ctx)
	a.InfoSchema = is
	if err := plannercore.Preprocess(a.Ctx, a.StmtNode, is, plannercore.InTxnRetry); err != nil {
		return 0, err
	}
	p, err := planner.Optimize(a.Ctx, a.StmtNode, is)
	if err != nil {
		return 0, err
	}
	a.Plan = p
	return is.SchemaMetaVersion(), nil
}

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned sqlexec.RecordSet Next method.
func (a *ExecStmt) Exec(ctx context.Context) (sqlexec.RecordSet, error) {
	a.StartTime = time.Now()
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

	e, err := a.buildExecutor(sctx)
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
		// Update processinfo, ShowProcess() will use it.
		pi.SetProcessInfo(sql, time.Now(), cmd)
		a.Ctx.GetSessionVars().StmtCtx.StmtType = GetStmtLabel(a.StmtNode)
	}

	// If the executor doesn't return any result to the client, we execute it without delay.
	if e.Schema().Len() == 0 {
		return a.handleNoDelayExecutor(ctx, sctx, e)
	} else if proj, ok := e.(*ProjectionExec); ok && proj.calculateNoDelay {
		// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
		// the Projection has two expressions and two columns in the schema, but we should
		// not return the result of the two expressions.
		return a.handleNoDelayExecutor(ctx, sctx, e)
	}

	var txnStartTS uint64
	txn, err1 := sctx.Txn(false)
	if err1 != nil {
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

func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, sctx sessionctx.Context, e Executor) (sqlexec.RecordSet, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("executor.handleNoDelayExecutor", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	// Check if "tidb_snapshot" is set for the write executors.
	// In history read mode, we can not do write operations.
	switch e.(type) {
	case *DeleteExec, *InsertExec, *UpdateExec, *ReplaceExec, *LoadDataExec, *DDLExec:
		snapshotTS := sctx.GetSessionVars().SnapshotTS
		if snapshotTS != 0 {
			return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
		}
	}

	var err error
	defer func() {
		terror.Log(e.Close())
		a.logAudit()
	}()

	err = e.Next(ctx, chunk.NewRecordBatch(e.newFirstChunk()))
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// buildExecutor build a executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor(ctx sessionctx.Context) (Executor, error) {
	if _, ok := a.Plan.(*plannercore.Execute); !ok {
		// Do not sync transaction for Execute statement, because the real optimization work is done in
		// "ExecuteExec.Build".
		useMaxTS, err := IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, a.Plan)
		if err != nil {
			return nil, err
		}
		if useMaxTS {
			logutil.Logger(context.Background()).Debug("init txnStartTS with MaxUint64", zap.Uint64("conn", ctx.GetSessionVars().ConnectionID), zap.String("text", a.Text))
			err = ctx.InitTxnWithStartTS(math.MaxUint64)
		} else if ctx.GetSessionVars().SnapshotTS != 0 {
			if _, ok := a.Plan.(*plannercore.CheckTable); ok {
				err = ctx.InitTxnWithStartTS(ctx.GetSessionVars().SnapshotTS)
			}
		}
		if err != nil {
			return nil, err
		}

		stmtCtx := ctx.GetSessionVars().StmtCtx
		if stmtPri := stmtCtx.Priority; stmtPri == mysql.NoPriority {
			switch {
			case useMaxTS:
				stmtCtx.Priority = kv.PriorityHigh
			case a.Expensive:
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
		err := executorExec.Build()
		if err != nil {
			return nil, err
		}
		a.isPreparedStmt = true
		a.Plan = executorExec.plan
		e = executorExec.stmtExec
	}
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
			audit.OnGeneralEvent(context.Background(), sessVars, plugin.Log, cmd)
		}
		return nil
	})
	if err != nil {
		log.Error("log audit log failure", zap.Error(err))
	}
}

// LogSlowQuery is used to print the slow query in the log files.
func (a *ExecStmt) LogSlowQuery(txnTS uint64, succ bool) {
	sessVars := a.Ctx.GetSessionVars()
	level := log.GetLevel()
	if level > zapcore.WarnLevel {
		return
	}
	cfg := config.GetGlobalConfig()
	costTime := time.Since(a.StartTime)
	threshold := time.Duration(atomic.LoadUint64(&cfg.Log.SlowThreshold)) * time.Millisecond
	if costTime < threshold && level > zapcore.DebugLevel {
		return
	}
	sql := a.Text
	if maxQueryLen := atomic.LoadUint64(&cfg.Log.QueryLogMaxLen); uint64(len(sql)) > maxQueryLen {
		sql = fmt.Sprintf("%.*q(len:%d)", maxQueryLen, sql, len(a.Text))
	}
	sql = QueryReplacer.Replace(sql) + sessVars.GetExecuteArgumentsInfo()

	var tableIDs, indexIDs string
	if len(sessVars.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.Replace(fmt.Sprintf("%v", a.Ctx.GetSessionVars().StmtCtx.TableIDs), " ", ",", -1)
	}
	if len(sessVars.StmtCtx.IndexIDs) > 0 {
		indexIDs = strings.Replace(fmt.Sprintf("%v", a.Ctx.GetSessionVars().StmtCtx.IndexIDs), " ", ",", -1)
	}
	execDetail := sessVars.StmtCtx.GetExecDetails()
	copTaskInfo := sessVars.StmtCtx.CopTasksDetails()
	statsInfos := a.getStatsInfo()
	memMax := sessVars.StmtCtx.MemTracker.MaxConsumed()
	if costTime < threshold {
		_, digest := sessVars.StmtCtx.SQLDigest()
		logutil.SlowQueryLogger.Debug(sessVars.SlowLogFormat(txnTS, costTime, execDetail, indexIDs, digest, statsInfos, copTaskInfo, memMax, sql))
	} else {
		_, digest := sessVars.StmtCtx.SQLDigest()
		logutil.SlowQueryLogger.Warn(sessVars.SlowLogFormat(txnTS, costTime, execDetail, indexIDs, digest, statsInfos, copTaskInfo, memMax, sql))
		metrics.TotalQueryProcHistogram.Observe(costTime.Seconds())
		metrics.TotalCopProcHistogram.Observe(execDetail.ProcessTime.Seconds())
		metrics.TotalCopWaitHistogram.Observe(execDetail.WaitTime.Seconds())
		var userString string
		if sessVars.User != nil {
			userString = sessVars.User.String()
		}
		domain.GetDomain(a.Ctx).LogSlowQuery(&domain.SlowQueryInfo{
			SQL:      sql,
			Digest:   digest,
			Start:    a.StartTime,
			Duration: costTime,
			Detail:   sessVars.StmtCtx.GetExecDetails(),
			Succ:     succ,
			ConnID:   sessVars.ConnectionID,
			TxnTS:    txnTS,
			User:     userString,
			DB:       sessVars.CurrentDB,
			TableIDs: tableIDs,
			IndexIDs: indexIDs,
			Internal: sessVars.InRestrictedSQL,
		})
	}
}

func (a *ExecStmt) getStatsInfo() map[string]uint64 {
	var physicalPlan plannercore.PhysicalPlan
	switch p := a.Plan.(type) {
	case *plannercore.Insert:
		physicalPlan = p.SelectPlan
	case *plannercore.Update:
		physicalPlan = p.SelectPlan
	case *plannercore.Delete:
		physicalPlan = p.SelectPlan
	case plannercore.PhysicalPlan:
		physicalPlan = p
	}

	if physicalPlan == nil {
		return nil
	}

	statsInfos := make(map[string]uint64)
	statsInfos = plannercore.CollectPlanStatsVersion(physicalPlan, statsInfos)
	return statsInfos
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. txn is not valid
//  2. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx sessionctx.Context, p plannercore.Plan) (bool, error) {
	// check auto commit
	if !ctx.GetSessionVars().IsAutocommit() {
		return false, nil
	}

	// check txn
	txn, err := ctx.Txn(false)
	if err != nil {
		return false, err
	}
	if txn.Valid() {
		return false, nil
	}

	// check plan
	if proj, ok := p.(*plannercore.PhysicalProjection); ok {
		if len(proj.Children()) != 1 {
			return false, nil
		}
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *plannercore.PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx.GetSessionVars().StmtCtx), nil
	case *plannercore.PhysicalTableReader:
		tableScan := v.TablePlans[0].(*plannercore.PhysicalTableScan)
		return len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPoint(ctx.GetSessionVars().StmtCtx), nil
	case *plannercore.PointGetPlan:
		// If the PointGetPlan needs to read data using unique index (double read), we
		// can't use max uint64, because using math.MaxUint64 can't guarantee repeatable-read
		// and the data and index would be inconsistent!
		return v.IndexInfo == nil, nil
	default:
		return false, nil
	}
}
