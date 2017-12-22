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
	"fmt"
	"math"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

type processinfoSetter interface {
	SetProcessInfo(string)
}

// recordSet wraps an executor, implements ast.RecordSet interface
type recordSet struct {
	fields      []*ast.ResultField
	executor    Executor
	stmt        *ExecStmt
	processinfo processinfoSetter
	lastErr     error
	txnStartTS  uint64
}

func (a *recordSet) Fields() []*ast.ResultField {
	if len(a.fields) == 0 {
		for _, col := range a.executor.Schema().Columns {
			dbName := col.DBName.O
			if dbName == "" && col.TblName.L != "" {
				dbName = a.stmt.Ctx.GetSessionVars().CurrentDB
			}
			rf := &ast.ResultField{
				ColumnAsName: col.ColName,
				TableAsName:  col.TblName,
				DBName:       model.NewCIStr(dbName),
				Table:        &model.TableInfo{Name: col.OrigTblName},
				Column: &model.ColumnInfo{
					FieldType: *col.RetType,
					Name:      col.ColName,
				},
			}
			a.fields = append(a.fields, rf)
		}
	}
	return a.fields
}

func (a *recordSet) Next(goCtx goctx.Context) (types.Row, error) {
	row, err := a.executor.Next(goCtx)
	if err != nil {
		a.lastErr = err
		return nil, errors.Trace(err)
	}
	if row == nil {
		if a.stmt != nil {
			a.stmt.Ctx.GetSessionVars().LastFoundRows = a.stmt.Ctx.GetSessionVars().StmtCtx.FoundRows()
		}
		return nil, nil
	}

	if a.stmt != nil {
		a.stmt.Ctx.GetSessionVars().StmtCtx.AddFoundRows(1)
	}
	return row, nil
}

func (a *recordSet) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	err := a.executor.NextChunk(goCtx, chk)
	if err != nil {
		a.lastErr = err
		return errors.Trace(err)
	}
	numRows := chk.NumRows()
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

func (a *recordSet) NewChunk() *chunk.Chunk {
	return chunk.NewChunk(a.executor.Schema().GetTypes())
}

func (a *recordSet) SupportChunk() bool {
	return a.executor.supportChunk()
}

func (a *recordSet) Close() error {
	err := a.executor.Close()
	a.stmt.logSlowQuery(a.txnStartTS, a.lastErr == nil)
	if a.processinfo != nil {
		a.processinfo.SetProcessInfo("")
	}
	return errors.Trace(err)
}

// ExecStmt implements the ast.Statement interface, it builds a plan.Plan to an ast.Statement.
type ExecStmt struct {
	// InfoSchema stores a reference to the schema information.
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	Plan plan.Plan
	// Expensive represents whether this query is an expensive one.
	Expensive bool
	// Cacheable represents whether the physical plan can be cached.
	Cacheable bool
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx            context.Context
	startTime      time.Time
	isPreparedStmt bool
}

// OriginText implements ast.Statement interface.
func (a *ExecStmt) OriginText() string {
	return a.Text
}

// IsPrepared implements ast.Statement interface.
func (a *ExecStmt) IsPrepared() bool {
	return a.isPreparedStmt
}

// IsReadOnly implements ast.Statement interface.
func (a *ExecStmt) IsReadOnly() bool {
	readOnlyCheckStmt := a.StmtNode
	if checkPlan, ok := a.Plan.(*plan.Execute); ok {
		readOnlyCheckStmt = checkPlan.Stmt
	}
	return ast.IsReadOnly(readOnlyCheckStmt)
}

// RebuildPlan implements ast.Statement interface.
func (a *ExecStmt) RebuildPlan() error {
	is := GetInfoSchema(a.Ctx)
	a.InfoSchema = is
	if err := plan.Preprocess(a.Ctx, a.StmtNode, is, false); err != nil {
		return errors.Trace(err)
	}
	p, err := plan.Optimize(a.Ctx, a.StmtNode, is)
	if err != nil {
		return errors.Trace(err)
	}
	a.Plan = p
	return nil
}

// Exec implements the ast.Statement Exec interface.
// This function builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned ast.RecordSet Next method.
func (a *ExecStmt) Exec(goCtx goctx.Context) (ast.RecordSet, error) {
	a.startTime = time.Now()
	ctx := a.Ctx
	if _, ok := a.Plan.(*plan.Analyze); ok && ctx.GetSessionVars().InRestrictedSQL {
		oriStats := ctx.GetSessionVars().Systems[variable.TiDBBuildStatsConcurrency]
		oriScan := ctx.GetSessionVars().DistSQLScanConcurrency
		oriIndex := ctx.GetSessionVars().IndexSerialScanConcurrency
		oriIso := ctx.GetSessionVars().Systems[variable.TxnIsolation]
		ctx.GetSessionVars().Systems[variable.TiDBBuildStatsConcurrency] = "1"
		ctx.GetSessionVars().DistSQLScanConcurrency = 1
		ctx.GetSessionVars().IndexSerialScanConcurrency = 1
		ctx.GetSessionVars().Systems[variable.TxnIsolation] = ast.ReadCommitted
		defer func() {
			ctx.GetSessionVars().Systems[variable.TiDBBuildStatsConcurrency] = oriStats
			ctx.GetSessionVars().DistSQLScanConcurrency = oriScan
			ctx.GetSessionVars().IndexSerialScanConcurrency = oriIndex
			ctx.GetSessionVars().Systems[variable.TxnIsolation] = oriIso
		}()
	}

	e, err := a.buildExecutor(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := e.Open(goCtx); err != nil {
		terror.Call(e.Close)
		return nil, errors.Trace(err)
	}

	var pi processinfoSetter
	if raw, ok := ctx.(processinfoSetter); ok {
		pi = raw
		sql := a.OriginText()
		if simple, ok := a.Plan.(*plan.Simple); ok && simple.Statement != nil {
			if ss, ok := simple.Statement.(ast.SensitiveStmtNode); ok {
				// Use SecureText to avoid leak password information.
				sql = ss.SecureText()
			}
		}
		// Update processinfo, ShowProcess() will use it.
		pi.SetProcessInfo(sql)
	}
	// If the executor doesn't return any result to the client, we execute it without delay.
	if e.Schema().Len() == 0 {
		return a.handleNoDelayExecutor(goCtx, e, ctx, pi)
	} else if proj, ok := e.(*ProjectionExec); ok && proj.calculateNoDelay {
		// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
		// the Projection has two expressions and two columns in the schema, but we should
		// not return the result of the two expressions.
		return a.handleNoDelayExecutor(goCtx, e, ctx, pi)
	}

	return &recordSet{
		executor:    e,
		stmt:        a,
		processinfo: pi,
		txnStartTS:  ctx.Txn().StartTS(),
	}, nil
}

func (a *ExecStmt) handleNoDelayExecutor(goCtx goctx.Context, e Executor, ctx context.Context, pi processinfoSetter) (ast.RecordSet, error) {
	// Check if "tidb_snapshot" is set for the write executors.
	// In history read mode, we can not do write operations.
	switch e.(type) {
	case *DeleteExec, *InsertExec, *UpdateExec, *ReplaceExec, *LoadData, *DDLExec:
		snapshotTS := ctx.GetSessionVars().SnapshotTS
		if snapshotTS != 0 {
			return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
		}
	}

	var err error
	defer func() {
		if pi != nil {
			pi.SetProcessInfo("")
		}
		terror.Log(errors.Trace(e.Close()))
		txnTS := uint64(0)
		if ctx.Txn() != nil {
			txnTS = ctx.Txn().StartTS()
		}
		a.logSlowQuery(txnTS, err == nil)
	}()

	if ctx.GetSessionVars().EnableChunk && e.supportChunk() {
		err = e.NextChunk(goCtx, e.newChunk())
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		for {
			var row Row
			row, err = e.Next(goCtx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// Even though there isn't any result set, the row is still used to indicate if there is
			// more work to do.
			// For example, the UPDATE statement updates a single row on a Next call, we keep calling Next until
			// There is no more rows to update.
			if row == nil {
				return nil, nil
			}
		}
	}

	return nil, nil
}

// buildExecutor build a executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor(ctx context.Context) (Executor, error) {
	priority := kv.PriorityNormal
	if _, ok := a.Plan.(*plan.Execute); !ok {
		// Do not sync transaction for Execute statement, because the real optimization work is done in
		// "ExecuteExec.Build".
		var err error
		isPointGet := IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, a.Plan)
		if isPointGet {
			log.Debugf("[%d][InitTxnWithStartTS] %s", ctx.GetSessionVars().ConnectionID, a.Text)
			err = ctx.InitTxnWithStartTS(math.MaxUint64)
		} else {
			log.Debugf("[%d][ActivePendingTxn] %s", ctx.GetSessionVars().ConnectionID, a.Text)
			err = ctx.ActivePendingTxn()
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		if stmtPri := ctx.GetSessionVars().StmtCtx.Priority; stmtPri != mysql.NoPriority {
			priority = int(stmtPri)
		} else {
			switch {
			case isPointGet:
				priority = kv.PriorityHigh
			case a.Expensive:
				priority = kv.PriorityLow
			}
		}
	}
	if _, ok := a.Plan.(*plan.Analyze); ok && ctx.GetSessionVars().InRestrictedSQL {
		priority = kv.PriorityLow
	}

	b := newExecutorBuilder(ctx, a.InfoSchema, priority)
	e := b.build(a.Plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	// ExecuteExec is not a real Executor, we only use it to build another Executor from a prepared statement.
	if executorExec, ok := e.(*ExecuteExec); ok {
		err := executorExec.Build()
		if err != nil {
			return nil, errors.Trace(err)
		}
		a.Text = executorExec.Stmt.Text()
		a.isPreparedStmt = true
		a.Plan = executorExec.Plan
		e = executorExec.StmtExec
	}
	return e, nil
}

func (a *ExecStmt) logSlowQuery(txnTS uint64, succ bool) {
	cfg := config.GetGlobalConfig()
	costTime := time.Since(a.startTime)
	sql := a.Text
	if len(sql) > cfg.Log.QueryLogMaxLen {
		sql = fmt.Sprintf("%.*q(len:%d)", cfg.Log.QueryLogMaxLen, sql, len(a.Text))
	}
	connID := a.Ctx.GetSessionVars().ConnectionID
	currentDB := a.Ctx.GetSessionVars().CurrentDB
	logEntry := log.NewEntry(logutil.SlowQueryLogger)
	logEntry.Data = log.Fields{
		"connectionId": connID,
		"costTime":     costTime,
		"database":     currentDB,
		"sql":          sql,
		"txnStartTS":   txnTS,
	}
	if costTime < time.Duration(cfg.Log.SlowThreshold)*time.Millisecond {
		logEntry.WithField("type", "query").WithField("succ", succ).Debugf("query")
	} else {
		logEntry.WithField("type", "slow-query").WithField("succ", succ).Warnf("slow-query")
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. txn is nil
//  2. plan is point get by pk or unique key
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx context.Context, p plan.Plan) bool {
	// check auto commit
	if !ctx.GetSessionVars().IsAutocommit() {
		return false
	}

	// check txn
	if ctx.Txn() != nil {
		return false
	}

	// check plan
	if proj, ok := p.(*plan.PhysicalProjection); ok {
		if len(proj.Children()) != 1 {
			return false
		}
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *plan.PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*plan.PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx.GetSessionVars().StmtCtx)
	case *plan.PhysicalIndexLookUpReader:
		indexScan := v.IndexPlans[0].(*plan.PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx.GetSessionVars().StmtCtx)
	case *plan.PhysicalTableReader:
		tableScan := v.TablePlans[0].(*plan.PhysicalTableScan)
		return len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPoint()
	default:
		return false
	}
}
