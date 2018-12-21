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
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
)

type domainMap struct {
	domains map[string]*domain.Domain
	mu      sync.Mutex
}

func (dm *domainMap) Get(store kv.Storage) (d *domain.Domain, err error) {
	key := store.UUID()
	dm.mu.Lock()
	defer dm.mu.Unlock()
	d = dm.domains[key]
	if d != nil {
		return
	}

	ddlLease := time.Duration(0)
	statisticLease := time.Duration(0)
	ddlLease = schemaLease
	statisticLease = statsLease
	err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
		log.Infof("store %v new domain, ddl lease %v, stats lease %d", store.UUID(), ddlLease, statisticLease)
		factory := createSessionFunc(store)
		sysFactory := createSessionWithDomainFunc(store)
		d = domain.NewDomain(store, ddlLease, statisticLease, factory)
		err1 = d.Init(ddlLease, sysFactory)
		if err1 != nil {
			// If we don't clean it, there are some dirty data when retrying the function of Init.
			d.Close()
			log.Errorf("[ddl] init domain failed %v", errors.ErrorStack(errors.Trace(err1)))
		}
		return true, errors.Trace(err1)
	})
	if err != nil {
		return nil, errors.Trace(err)
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
	schemaLease = 1 * time.Second

	// statsLease is the time for reload stats table.
	statsLease = 3 * time.Second
)

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	schemaLease = lease
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	statsLease = lease
}

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx sessionctx.Context, src string) ([]ast.StmtNode, error) {
	log.Debug("compiling", src)
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.EnableWindowFunc(ctx.GetSessionVars().EnableWindowFunction)
	p.SetSQLMode(ctx.GetSessionVars().SQLMode)
	stmts, err := p.Parse(src, charset, collation)
	if err != nil {
		log.Warnf("compiling %s, error: %v", src, err)
		return nil, errors.Trace(err)
	}
	return stmts, nil
}

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, sctx sessionctx.Context, stmtNode ast.StmtNode) (sqlexec.Statement, error) {
	compiler := executor.Compiler{Ctx: sctx}
	stmt, err := compiler.Compile(ctx, stmtNode)
	return stmt, errors.Trace(err)
}

func finishStmt(ctx context.Context, sctx sessionctx.Context, se *session, sessVars *variable.SessionVars, meetsErr error) error {
	if meetsErr != nil {
		if !sessVars.InTxn() {
			log.Info("RollbackTxn for ddl/autocommit error.")
			terror.Log(se.RollbackTxn(ctx))
		}
		return meetsErr
	}

	if !sessVars.InTxn() {
		return se.CommitTxn(ctx)
	}

	return checkStmtLimit(ctx, sctx, se, sessVars)
}

func checkStmtLimit(ctx context.Context, sctx sessionctx.Context, se *session, sessVars *variable.SessionVars) error {
	// If the user insert, insert, insert ... but never commit, TiDB would OOM.
	// So we limit the statement count in a transaction here.
	var err error
	history := GetHistory(sctx)
	if history.Count() > int(config.GetGlobalConfig().Performance.StmtCountLimit) {
		if !sessVars.BatchCommit {
			err1 := se.RollbackTxn(ctx)
			terror.Log(errors.Trace(err1))
			return errors.Errorf("statement count %d exceeds the transaction limitation, autocommit = %t",
				history.Count(), sctx.GetSessionVars().IsAutocommit())
		}
		err = se.NewTxn(ctx)
		// The transaction does not committed yet, we need to keep it in transaction.
		// The last history could not be "commit"/"rollback" statement.
		// It means it is impossible to start a new transaction at the end of the transaction.
		// Because after the server executed "commit"/"rollback" statement, the session is out of the transaction.
		se.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
	}
	return err
}

// runStmt executes the sqlexec.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, sctx sessionctx.Context, s sqlexec.Statement) (sqlexec.RecordSet, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.runStmt", opentracing.ChildOf(span.Context()))
		span1.LogKV("sql", s.OriginText())
		defer span1.Finish()
	}

	var err error
	var rs sqlexec.RecordSet
	se := sctx.(*session)
	rs, err = s.Exec(ctx)
	sessVars := se.GetSessionVars()
	// All the history should be added here.
	sessVars.TxnCtx.StatementCount++
	if !s.IsReadOnly() {
		if err == nil {
			GetHistory(sctx).Add(0, s, se.sessionVars.StmtCtx)
		}
		if txn, err1 := sctx.Txn(false); err1 == nil {
			if txn.Valid() {
				if err != nil {
					sctx.StmtRollback()
				} else {
					err = sctx.StmtCommit()
				}
			}
		} else {
			log.Error(err1)
		}
	}

	err = finishStmt(ctx, sctx, se, sessVars, err)
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
	return rs, errors.Trace(err)
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
	chk := rs.NewChunk()
	for {
		// Since we collect all the rows, we can not reuse the chunk.
		iter := chunk.NewIterator4Chunk(chk)

		err := rs.Next(ctx, chk)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}

		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
		chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
	}
	return rows, nil
}

var queryStmtTable = []string{"explain", "select", "show", "execute", "describe", "desc", "admin"}

func trimSQL(sql string) string {
	// Trim space.
	sql = strings.TrimSpace(sql)
	// Trim leading /*comment*/
	// There may be multiple comments
	for strings.HasPrefix(sql, "/*") {
		i := strings.Index(sql, "*/")
		if i != -1 && i < len(sql)+1 {
			sql = sql[i+2:]
			sql = strings.TrimSpace(sql)
			continue
		}
		break
	}
	// Trim leading '('. For `(select 1);` is also a query.
	return strings.TrimLeft(sql, "( ")
}

// IsQuery checks if a sql statement is a query statement.
func IsQuery(sql string) bool {
	sqlText := strings.ToLower(trimSQL(sql))
	for _, key := range queryStmtTable {
		if strings.HasPrefix(sqlText, key) {
			return true
		}
	}

	return false
}

var (
	errForUpdateCantRetry = terror.ClassSession.New(codeForUpdateCantRetry,
		mysql.MySQLErrName[mysql.ErrForUpdateCantRetry])
)

const (
	codeForUpdateCantRetry terror.ErrCode = mysql.ErrForUpdateCantRetry
)

func init() {
	sessionMySQLErrCodes := map[terror.ErrCode]uint16{
		codeForUpdateCantRetry: mysql.ErrForUpdateCantRetry,
	}
	terror.ErrClassToMySQLCodes[terror.ClassSession] = sessionMySQLErrCodes
}
