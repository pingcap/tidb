// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package session

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

type domainMap struct {
	domains map[string]*domain.Domain
	mu      sync.Mutex
}

func (dm *domainMap) Get(store kv.Storage) (d *domain.Domain, err error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// If this is the only domain instance, and the caller doesn't provide store.
	if len(dm.domains) == 1 && store == nil {
		for _, r := range dm.domains {
			return r, nil
		}
	}

	key := store.UUID()
	d = dm.domains[key]
	if d != nil {
		return
	}

	ddlLease := time.Duration(atomic.LoadInt64(&schemaLease))
	statisticLease := time.Duration(atomic.LoadInt64(&statsLease))
	err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
		logutil.BgLogger().Info("new domain",
			zap.String("store", store.UUID()),
			zap.Stringer("ddl lease", ddlLease),
			zap.Stringer("stats lease", statisticLease))
		factory := createSessionFunc(store)
		sysFactory := createSessionWithDomainFunc(store)
		d = domain.NewDomain(store, ddlLease, statisticLease, factory)
		err1 = d.Init(ddlLease, sysFactory)
		if err1 != nil {
			// If we don't clean it, there are some dirty data when retrying the function of Init.
			d.Close()
			logutil.BgLogger().Error("[ddl] init domain failed",
				zap.Error(err1))
		}
		return true, err1
	})
	if err != nil {
		return nil, err
	}
	dm.domains[key] = d

	return
}

func (dm *domainMap) Delete(store kv.Storage) {
	dm.mu.Lock()
	delete(dm.domains, store.UUID())
	dm.mu.Unlock()
}

var (
	domap = &domainMap{
		domains: map[string]*domain.Domain{},
	}
	// store.UUID()-> IfBootstrapped
	storeBootstrapped     = make(map[string]bool)
	storeBootstrappedLock sync.Mutex

	// schemaLease is the time for re-updating remote schema.
	// In online DDL, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schema lease, like 300s+.
	schemaLease = int64(1 * time.Second)

	// statsLease is the time for reload stats table.
	statsLease = int64(3 * time.Second)
)

// ResetStoreForWithTiKVTest is only used in the test code.
// TODO: Remove domap and storeBootstrapped. Use store.SetOption() to do it.
func ResetStoreForWithTiKVTest(store kv.Storage) {
	domap.Delete(store)
	unsetStoreBootstrapped(store.UUID())
}

func setStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	storeBootstrapped[storeUUID] = true
}

// unsetStoreBootstrapped delete store uuid from stored bootstrapped map.
// currently this function only used for test.
func unsetStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	delete(storeBootstrapped, storeUUID)
}

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	atomic.StoreInt64(&schemaLease, int64(lease))
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	atomic.StoreInt64(&statsLease, int64(lease))
}

// DisableStats4Test disables the stats for tests.
func DisableStats4Test() {
	SetStatsLease(-1)
}

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx sessionctx.Context, src string) ([]ast.StmtNode, error) {
	logutil.BgLogger().Debug("compiling", zap.String("source", src))
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.EnableWindowFunc(ctx.GetSessionVars().EnableWindowFunction)
	p.SetSQLMode(ctx.GetSessionVars().SQLMode)
	stmts, warns, err := p.Parse(src, charset, collation)
	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
	}
	if err != nil {
		logutil.BgLogger().Warn("compiling",
			zap.String("source", src),
			zap.Error(err))
		return nil, err
	}
	return stmts, nil
}

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, sctx sessionctx.Context, stmtNode ast.StmtNode) (sqlexec.Statement, error) {
	compiler := executor.Compiler{Ctx: sctx}
	stmt, err := compiler.Compile(ctx, stmtNode)
	return stmt, err
}

func recordAbortTxnDuration(sessVars *variable.SessionVars) {
	duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
	if sessVars.TxnCtx.IsPessimistic {
		transactionDurationPessimisticAbort.Observe(duration)
	} else {
		transactionDurationOptimisticAbort.Observe(duration)
	}
}

func finishStmt(ctx context.Context, sctx sessionctx.Context, se *session, sessVars *variable.SessionVars,
	meetsErr error, sql sqlexec.Statement) error {
	if meetsErr != nil {
		if !sessVars.InTxn() {
			logutil.BgLogger().Info("rollbackTxn for ddl/autocommit failed")
			se.RollbackTxn(ctx)
			recordAbortTxnDuration(sessVars)
		} else if se.txn.Valid() && se.txn.IsPessimistic() && executor.ErrDeadlock.Equal(meetsErr) {
			logutil.BgLogger().Info("rollbackTxn for deadlock", zap.Uint64("txn", se.txn.StartTS()))
			se.RollbackTxn(ctx)
			recordAbortTxnDuration(sessVars)
		}
		return meetsErr
	}

	if !sessVars.InTxn() {
		if err := se.CommitTxn(ctx); err != nil {
			if _, ok := sql.(*executor.ExecStmt).StmtNode.(*ast.CommitStmt); ok {
				err = errors.Annotatef(err, "previous statement: %s", se.GetSessionVars().PrevStmt)
			}
			return err
		}
		return nil
	}

	return checkStmtLimit(ctx, sctx, se)
}

func checkStmtLimit(ctx context.Context, sctx sessionctx.Context, se *session) error {
	// If the user insert, insert, insert ... but never commit, TiDB would OOM.
	// So we limit the statement count in a transaction here.
	var err error
	sessVars := se.GetSessionVars()
	history := GetHistory(sctx)
	if history.Count() > int(config.GetGlobalConfig().Performance.StmtCountLimit) {
		if !sessVars.BatchCommit {
			se.RollbackTxn(ctx)
			return errors.Errorf("statement count %d exceeds the transaction limitation, autocommit = %t",
				history.Count(), sctx.GetSessionVars().IsAutocommit())
		}
		err = se.NewTxn(ctx)
		// The transaction does not committed yet, we need to keep it in transaction.
		// The last history could not be "commit"/"rollback" statement.
		// It means it is impossible to start a new transaction at the end of the transaction.
		// Because after the server executed "commit"/"rollback" statement, the session is out of the transaction.
		sessVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
	}
	return err
}

// runStmt executes the sqlexec.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, sctx sessionctx.Context, s sqlexec.Statement) (rs sqlexec.RecordSet, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.runStmt", opentracing.ChildOf(span.Context()))
		span1.LogKV("sql", s.OriginText())
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	sctx.SetValue(sessionctx.QueryString, s.OriginText())
	if _, ok := s.(*executor.ExecStmt).StmtNode.(ast.DDLNode); ok {
		sctx.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		sctx.ClearValue(sessionctx.LastExecuteDDL)
	}

	se := sctx.(*session)
	sessVars := se.GetSessionVars()
	// Save origTxnCtx here to avoid it reset in the transaction retry.
	origTxnCtx := sessVars.TxnCtx
	defer func() {
		// If it is not a select statement, we record its slow log here,
		// then it could include the transaction commit time.
		if rs == nil {
			s.(*executor.ExecStmt).FinishExecuteStmt(origTxnCtx.StartTS, err == nil, false)
		}
	}()

	err = se.checkTxnAborted(s)
	if err != nil {
		return nil, err
	}
	rs, err = s.Exec(ctx)
	sessVars.TxnCtx.StatementCount++
	if !s.IsReadOnly(sessVars) {
		// All the history should be added here.
		if err == nil && sessVars.TxnCtx.CouldRetry {
			GetHistory(sctx).Add(s, sessVars.StmtCtx)
		}

		// Handle the stmt commit/rollback.
		if txn, err1 := sctx.Txn(false); err1 == nil {
			if txn.Valid() {
				if err != nil {
					sctx.StmtRollback()
				} else {
					err = sctx.StmtCommit(sctx.GetSessionVars().StmtCtx.MemTracker)
				}
			}
		} else {
			logutil.BgLogger().Error("get txn failed", zap.Error(err1))
		}
	}
	err = finishStmt(ctx, sctx, se, sessVars, err, s)

	if se.txn.pending() {
		// After run statement finish, txn state is still pending means the
		// statement never need a Txn(), such as:
		//
		// set @@tidb_general_log = 1
		// set @@autocommit = 0
		// select 1
		//
		// Reset txn state to invalid to dispose the pending start ts.
		se.txn.changeToInvalid()
	}
	return rs, err
}

// GetHistory get all stmtHistory in current txn. Exported only for test.
func GetHistory(ctx sessionctx.Context) *StmtHistory {
	hist, ok := ctx.GetSessionVars().TxnCtx.History.(*StmtHistory)
	if ok {
		return hist
	}
	hist = new(StmtHistory)
	ctx.GetSessionVars().TxnCtx.History = hist
	return hist
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func GetRows4Test(ctx context.Context, sctx sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk()
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		err := rs.Next(ctx, req)
		if err != nil {
			return nil, err
		}
		if req.NumRows() == 0 {
			break
		}

		iter := chunk.NewIterator4Chunk(req.CopyConstruct())
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// ResultSetToStringSlice changes the RecordSet to [][]string.
func ResultSetToStringSlice(ctx context.Context, s Session, rs sqlexec.RecordSet) ([][]string, error) {
	rows, err := GetRows4Test(ctx, s, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

// Session errors.
var (
	ErrForUpdateCantRetry = terror.ClassSession.New(errno.ErrForUpdateCantRetry, errno.MySQLErrName[errno.ErrForUpdateCantRetry])
)
