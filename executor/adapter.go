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
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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
		rfs = append(rfs, rf)
	}
	return rfs
}

// Next use uses recordSet's executor to get next available chunk for later usage.
// If chunk does not contain any rows, then we update last query found rows in session variable as current found rows.
// The reason we need update is that chunk with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and chunk with some rows inside, we simply update last query found rows by the number of row in chunk.
func (a *recordSet) Next(ctx context.Context, chk *chunk.Chunk) error {
	err := a.executor.Next(ctx, chk)
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

// NewChunk create a new chunk using NewChunk function in chunk package.
func (a *recordSet) NewChunk() *chunk.Chunk {
	return a.executor.newChunk()
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

	Ctx            sessionctx.Context
	startTime      time.Time
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
// It will update readOnlyCheckStmt if current ExecStmt can be conveted to
// a plan.Execute. Last step is using ast.IsReadOnly function to determine
// a statement is read only or not.
func (a *ExecStmt) IsReadOnly() bool {
	readOnlyCheckStmt := a.StmtNode
	if checkPlan, ok := a.Plan.(*plan.Execute); ok {
		readOnlyCheckStmt = checkPlan.Stmt
	}
	return ast.IsReadOnly(readOnlyCheckStmt)
}

// RebuildPlan rebuilds current execute statement plan.
// It returns the current information schema version that 'a' is using.
func (a *ExecStmt) RebuildPlan() (int64, error) {
	is := GetInfoSchema(a.Ctx)
	a.InfoSchema = is
	if err := plan.Preprocess(a.Ctx, a.StmtNode, is, false); err != nil {
		return 0, errors.Trace(err)
	}
	p, err := plan.Optimize(a.Ctx, a.StmtNode, is)
	if err != nil {
		return 0, errors.Trace(err)
	}
	a.Plan = p
	return is.SchemaMetaVersion(), nil
}

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned ast.RecordSet Next method.
func (a *ExecStmt) Exec(ctx context.Context) (ast.RecordSet, error) {
	a.startTime = time.Now()
	sctx := a.Ctx
	if _, ok := a.Plan.(*plan.Analyze); ok && sctx.GetSessionVars().InRestrictedSQL {
		oriStats, _ := sctx.GetSessionVars().GetSystemVar(variable.TiDBBuildStatsConcurrency)
		oriScan := sctx.GetSessionVars().DistSQLScanConcurrency
		oriIndex := sctx.GetSessionVars().IndexSerialScanConcurrency
		oriIso, _ := sctx.GetSessionVars().GetSystemVar(variable.TxnIsolation)
		terror.Log(errors.Trace(sctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "1")))
		sctx.GetSessionVars().DistSQLScanConcurrency = 1
		sctx.GetSessionVars().IndexSerialScanConcurrency = 1
		terror.Log(errors.Trace(sctx.GetSessionVars().SetSystemVar(variable.TxnIsolation, ast.ReadCommitted)))
		defer func() {
			terror.Log(errors.Trace(sctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, oriStats)))
			sctx.GetSessionVars().DistSQLScanConcurrency = oriScan
			sctx.GetSessionVars().IndexSerialScanConcurrency = oriIndex
			terror.Log(errors.Trace(sctx.GetSessionVars().SetSystemVar(variable.TxnIsolation, oriIso)))
		}()
	}

	e, err := a.buildExecutor(sctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := e.Open(ctx); err != nil {
		terror.Call(e.Close)
		return nil, errors.Trace(err)
	}

	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
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
		return a.handleNoDelayExecutor(ctx, sctx, e, pi)
	} else if proj, ok := e.(*ProjectionExec); ok && proj.calculateNoDelay {
		// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
		// the Projection has two expressions and two columns in the schema, but we should
		// not return the result of the two expressions.
		return a.handleNoDelayExecutor(ctx, sctx, e, pi)
	}

	return &recordSet{
		executor:    e,
		stmt:        a,
		processinfo: pi,
		txnStartTS:  sctx.Txn().StartTS(),
	}, nil
}

func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, sctx sessionctx.Context, e Executor, pi processinfoSetter) (ast.RecordSet, error) {
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
		if pi != nil {
			pi.SetProcessInfo("")
		}
		terror.Log(errors.Trace(e.Close()))
		txnTS := uint64(0)
		if sctx.Txn() != nil {
			txnTS = sctx.Txn().StartTS()
		}
		a.logSlowQuery(txnTS, err == nil)
	}()

	err = e.Next(ctx, e.newChunk())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return nil, nil
}

// buildExecutor build a executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor(ctx sessionctx.Context) (Executor, error) {
	if _, ok := a.Plan.(*plan.Execute); !ok {
		// Do not sync transaction for Execute statement, because the real optimization work is done in
		// "ExecuteExec.Build".
		var err error
		isPointGet := IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, a.Plan)
		if isPointGet {
			log.Debugf("con:%d InitTxnWithStartTS %s", ctx.GetSessionVars().ConnectionID, a.Text)
			err = ctx.InitTxnWithStartTS(math.MaxUint64)
		} else {
			log.Debugf("con:%d ActivePendingTxn %s", ctx.GetSessionVars().ConnectionID, a.Text)
			err = ctx.ActivePendingTxn()
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		stmtCtx := ctx.GetSessionVars().StmtCtx
		if stmtPri := stmtCtx.Priority; stmtPri == mysql.NoPriority {
			switch {
			case isPointGet:
				stmtCtx.Priority = kv.PriorityHigh
			case a.Expensive:
				stmtCtx.Priority = kv.PriorityLow
			}
		}
	}
	if _, ok := a.Plan.(*plan.Analyze); ok && ctx.GetSessionVars().InRestrictedSQL {
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
			return nil, errors.Trace(err)
		}
		a.Text = executorExec.stmt.Text()
		a.isPreparedStmt = true
		a.Plan = executorExec.plan
		e = executorExec.stmtExec
	}
	return e, nil
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

func (a *ExecStmt) logSlowQuery(txnTS uint64, succ bool) {
	level := log.GetLevel()
	if level < log.WarnLevel {
		return
	}
	cfg := config.GetGlobalConfig()
	costTime := time.Since(a.startTime)
	threshold := time.Duration(cfg.Log.SlowThreshold) * time.Millisecond
	if costTime < threshold && level < log.DebugLevel {
		return
	}
	sql := a.Text
	if len(sql) > int(cfg.Log.QueryLogMaxLen) {
		sql = fmt.Sprintf("%.*q(len:%d)", cfg.Log.QueryLogMaxLen, sql, len(a.Text))
	}
	sql = QueryReplacer.Replace(sql)

	sessVars := a.Ctx.GetSessionVars()
	connID := sessVars.ConnectionID
	currentDB := sessVars.CurrentDB
	var tableIDs, indexIDs string
	if len(sessVars.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.Replace(fmt.Sprintf("table_ids:%v ", a.Ctx.GetSessionVars().StmtCtx.TableIDs), " ", ",", -1)
	}
	if len(sessVars.StmtCtx.IndexIDs) > 0 {
		indexIDs = strings.Replace(fmt.Sprintf("index_ids:%v ", a.Ctx.GetSessionVars().StmtCtx.IndexIDs), " ", ",", -1)
	}
	user := a.Ctx.GetSessionVars().User
	if costTime < threshold {
		logutil.SlowQueryLogger.Debugf(
			"[QUERY] cost_time:%v %s succ:%v con:%v user:%s txn_start_ts:%v database:%v %v%vsql:%v",
			costTime, sessVars.StmtCtx.GetExecDetails(), succ, connID, user, txnTS, currentDB, tableIDs, indexIDs, sql)
	} else {
		logutil.SlowQueryLogger.Warnf(
			"[SLOW_QUERY] cost_time:%v %s succ:%v con:%v user:%s txn_start_ts:%v database:%v %v%vsql:%v",
			costTime, sessVars.StmtCtx.GetExecDetails(), succ, connID, user, txnTS, currentDB, tableIDs, indexIDs, sql)
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. txn is nil
//  2. plan is point get by pk or unique key
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx sessionctx.Context, p plan.Plan) bool {
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
		return len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPoint(ctx.GetSessionVars().StmtCtx)
	case *plan.PointGetPlan:
		return true
	default:
		return false
	}
}
