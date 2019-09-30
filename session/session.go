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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
<<<<<<< HEAD
	"golang.org/x/net/context"
=======
)

var (
	statementPerTransactionInternalOK    = metrics.StatementPerTransaction.WithLabelValues(metrics.LblInternal, "ok")
	statementPerTransactionInternalError = metrics.StatementPerTransaction.WithLabelValues(metrics.LblInternal, "error")
	statementPerTransactionGeneralOK     = metrics.StatementPerTransaction.WithLabelValues(metrics.LblGeneral, "ok")
	statementPerTransactionGeneralError  = metrics.StatementPerTransaction.WithLabelValues(metrics.LblGeneral, "error")
	transactionDurationInternalOK        = metrics.TransactionDuration.WithLabelValues(metrics.LblInternal, "ok")
	transactionDurationInternalError     = metrics.TransactionDuration.WithLabelValues(metrics.LblInternal, "error")
	transactionDurationGeneralOK         = metrics.TransactionDuration.WithLabelValues(metrics.LblGeneral, "ok")
	transactionDurationGeneralError      = metrics.TransactionDuration.WithLabelValues(metrics.LblGeneral, "error")

	transactionCounterInternalOK             = metrics.TransactionCounter.WithLabelValues(metrics.LblInternal, metrics.LblOK)
	transactionCounterInternalErr            = metrics.TransactionCounter.WithLabelValues(metrics.LblInternal, metrics.LblError)
	transactionCounterGeneralOK              = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	transactionCounterGeneralErr             = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblError)
	transactionCounterInternalCommitRollback = metrics.TransactionCounter.WithLabelValues(metrics.LblInternal, metrics.LblComRol)
	transactionCounterGeneralCommitRollback  = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblComRol)
	transactionRollbackCounterInternal       = metrics.TransactionCounter.WithLabelValues(metrics.LblInternal, metrics.LblRollback)
	transactionRollbackCounterGeneral        = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblRollback)

	sessionExecuteRunDurationInternal = metrics.SessionExecuteRunDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteRunDurationGeneral  = metrics.SessionExecuteRunDuration.WithLabelValues(metrics.LblGeneral)

	sessionExecuteCompileDurationInternal = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteCompileDurationGeneral  = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblGeneral)
	sessionExecuteParseDurationInternal   = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteParseDurationGeneral    = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblGeneral)
>>>>>>> ea6d00b... *: add a new way to calculate TPS (#12411)
)

// Session context, it is consistent with the lifecycle of a client connection.
type Session interface {
	sessionctx.Context
	Status() uint16                                               // Flag of current status, such as autocommit.
	LastInsertID() uint64                                         // LastInsertID is the last inserted auto_increment ID.
	AffectedRows() uint64                                         // Affected rows by latest executed stmt.
	Execute(context.Context, string) ([]sqlexec.RecordSet, error) // Execute a sql statement.
	String() string                                               // String is used to debug.
	CommitTxn(context.Context) error
	RollbackTxn(context.Context) error
	// PrepareStmt executes prepare statement in binary protocol.
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// ExecutePreparedStmt executes a prepared statement.
	ExecutePreparedStmt(ctx context.Context, stmtID uint32, param ...interface{}) (sqlexec.RecordSet, error)
	DropPreparedStmt(stmtID uint32) error
	SetClientCapability(uint32) // Set client capability flags.
	SetConnectionID(uint64)
	SetCommandValue(byte)
	SetProcessInfo(string, time.Time, byte, uint64)
	SetTLSState(*tls.ConnectionState)
	SetCollation(coID int) error
	SetSessionManager(util.SessionManager)
	Close()
	Auth(user *auth.UserIdentity, auth []byte, salt []byte) bool
	ShowProcess() *util.ProcessInfo
	// PrePareTxnCtx is exported for test.
	PrepareTxnCtx(context.Context)
	// FieldList returns fields list of a table.
	FieldList(tableName string) (fields []*ast.ResultField, err error)
}

var (
	_ Session = (*session)(nil)
)

type stmtRecord struct {
	stmtID  uint32
	st      sqlexec.Statement
	stmtCtx *stmtctx.StatementContext
	params  []interface{}
}

// StmtHistory holds all histories of statements in a txn.
type StmtHistory struct {
	history []*stmtRecord
}

// Add appends a stmt to history list.
func (h *StmtHistory) Add(stmtID uint32, st sqlexec.Statement, stmtCtx *stmtctx.StatementContext, params ...interface{}) {
	s := &stmtRecord{
		stmtID:  stmtID,
		st:      st,
		stmtCtx: stmtCtx,
		params:  append(([]interface{})(nil), params...),
	}
	h.history = append(h.history, s)
}

// Count returns the count of the history.
func (h *StmtHistory) Count() int {
	return len(h.history)
}

type session struct {
	// processInfo is used by ShowProcess(), and should be modified atomically.
	processInfo atomic.Value
	txn         TxnState

	mu struct {
		sync.RWMutex
		values map[fmt.Stringer]interface{}
	}

	store kv.Storage

	parser *parser.Parser

	preparedPlanCache *kvcache.SimpleLRUCache

	sessionVars    *variable.SessionVars
	sessionManager util.SessionManager

	statsCollector *statistics.SessionStatsCollector
	// ddlOwnerChecker is used in `select tidb_is_ddl_owner()` statement;
	ddlOwnerChecker owner.DDLOwnerChecker
}

// DDLOwnerChecker returns s.ddlOwnerChecker.
func (s *session) DDLOwnerChecker() owner.DDLOwnerChecker {
	return s.ddlOwnerChecker
}

func (s *session) getMembufCap() int {
	if s.sessionVars.LightningMode {
		return kv.ImportingTxnMembufCap
	}

	return kv.DefaultTxnMembufCap
}

func (s *session) cleanRetryInfo() {
	if !s.sessionVars.RetryInfo.Retrying {
		retryInfo := s.sessionVars.RetryInfo
		for _, stmtID := range retryInfo.DroppedPreparedStmtIDs {
			delete(s.sessionVars.PreparedStmts, stmtID)
		}
		retryInfo.Clean()
	}
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status
}

func (s *session) LastInsertID() uint64 {
	if s.sessionVars.StmtCtx.LastInsertID > 0 {
		return s.sessionVars.StmtCtx.LastInsertID
	}
	return s.sessionVars.StmtCtx.InsertID
}

func (s *session) AffectedRows() uint64 {
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *session) SetClientCapability(capability uint32) {
	s.sessionVars.ClientCapability = capability
}

func (s *session) SetConnectionID(connectionID uint64) {
	s.sessionVars.ConnectionID = connectionID
}

func (s *session) SetTLSState(tlsState *tls.ConnectionState) {
	// If user is not connected via TLS, then tlsState == nil.
	if tlsState != nil {
		s.sessionVars.TLSConnectionState = tlsState
	}
}

func (s *session) SetCommandValue(command byte) {
	atomic.StoreUint32(&s.sessionVars.CommandValue, uint32(command))
}

func (s *session) GetTLSState() *tls.ConnectionState {
	return s.sessionVars.TLSConnectionState
}

func (s *session) SetCollation(coID int) error {
	cs, co, err := charset.GetCharsetInfoByID(coID)
	if err != nil {
		return errors.Trace(err)
	}
	for _, v := range variable.SetNamesVariables {
		terror.Log(errors.Trace(s.sessionVars.SetSystemVar(v, cs)))
	}
	terror.Log(errors.Trace(s.sessionVars.SetSystemVar(variable.CollationConnection, co)))
	return nil
}

func (s *session) PreparedPlanCache() *kvcache.SimpleLRUCache {
	return s.preparedPlanCache
}

func (s *session) SetSessionManager(sm util.SessionManager) {
	s.sessionManager = sm
}

func (s *session) GetSessionManager() util.SessionManager {
	return s.sessionManager
}

func (s *session) StoreQueryFeedback(feedback interface{}) {
	if s.statsCollector != nil {
		do, err := GetDomain(s.store)
		if err != nil {
			logutil.Logger(context.Background()).Debug("domain not found", zap.Error(err))
			metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
			return
		}
		err = s.statsCollector.StoreQueryFeedback(feedback, do.StatsHandle())
		if err != nil {
			logutil.Logger(context.Background()).Debug("store query feedback", zap.Error(err))
			metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
			return
		}
		metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblOK).Inc()
	}
}

// FieldList returns fields list of a table.
func (s *session) FieldList(tableName string) ([]*ast.ResultField, error) {
	is := executor.GetInfoSchema(s)
	dbName := model.NewCIStr(s.GetSessionVars().CurrentDB)
	tName := model.NewCIStr(tableName)
	table, err := is.TableByName(dbName, tName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cols := table.Cols()
	fields := make([]*ast.ResultField, 0, len(cols))
	for _, col := range table.Cols() {
		rf := &ast.ResultField{
			ColumnAsName: col.Name,
			TableAsName:  tName,
			DBName:       dbName,
			Table:        table.Meta(),
			Column:       col.ColumnInfo,
		}
		fields = append(fields, rf)
	}
	return fields, nil
}

func (s *session) doCommit(ctx context.Context) error {
	if !s.txn.Valid() {
		return nil
	}
	defer func() {
		s.txn.changeToInvalid()
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()
	if s.txn.IsReadOnly() {
		return nil
	}
	if s.sessionVars.BinlogClient != nil {
		prewriteValue := binloginfo.GetPrewriteValue(s, false)
		if prewriteValue != nil {
			prewriteData, err := prewriteValue.Marshal()
			if err != nil {
				return errors.Trace(err)
			}
			info := &binloginfo.BinlogInfo{
				Data: &binlog.Binlog{
					Tp:            binlog.BinlogType_Prewrite,
					PrewriteValue: prewriteData,
				},
				Client: s.sessionVars.BinlogClient,
			}
			s.txn.SetOption(kv.BinlogInfo, info)
		}
	}

	// Get the related table IDs.
	relatedTables := s.GetSessionVars().TxnCtx.TableDeltaMap
	tableIDs := make([]int64, 0, len(relatedTables))
	for id := range relatedTables {
		tableIDs = append(tableIDs, id)
	}
	// Set this option for 2 phase commit to validate schema lease.
	s.txn.SetOption(kv.SchemaChecker, domain.NewSchemaChecker(domain.GetDomain(s), s.sessionVars.TxnCtx.SchemaVersion, tableIDs))
	if s.sessionVars.TxnCtx.ForUpdate {
		s.txn.SetOption(kv.BypassLatch, true)
	}

	if err := s.txn.Commit(sessionctx.SetCommitCtx(ctx, s)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *session) doCommitWithRetry(ctx context.Context) error {
	var txnSize int
	if s.txn.Valid() {
		txnSize = s.txn.Size()
	}
	err := s.doCommit(ctx)
	if err != nil {
		commitRetryLimit := s.sessionVars.RetryLimit
		if s.sessionVars.DisableTxnAutoRetry && !s.sessionVars.InRestrictedSQL {
			// Do not retry non-autocommit transactions.
			// For autocommit single statement transactions, the history count is always 1.
			// For explicit transactions, the statement count is more than 1.
			history := GetHistory(s)
			if history.Count() > 1 {
				commitRetryLimit = 0
			}
		}
		// If the transaction is batched, it should not retry any more.
		isBatched := s.GetSessionVars().TxnCtx.IsBatched
		// Don't retry in BatchInsert mode. As a counter-example, insert into t1 select * from t2,
		// BatchInsert already commit the first batch 1000 rows, then it commit 1000-2000 and retry the statement,
		// Finally t1 will have more data than t2, with no errors return to user!
		if s.isRetryableError(err) && !s.sessionVars.BatchInsert && !isBatched && commitRetryLimit > 0 {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", s.getSQLLabel()),
				zap.Error(err),
				zap.String("txn", s.txn.GoString()))
			// Transactions will retry 2 ~ commitRetryLimit times.
			// We make larger transactions retry less times to prevent cluster resource outage.
			txnSizeRate := float64(txnSize) / float64(kv.TxnTotalSizeLimit)
			maxRetryCount := commitRetryLimit - int64(float64(commitRetryLimit-1)*txnSizeRate)
			err = s.retry(ctx, uint(maxRetryCount))
		} else {
			logutil.Logger(ctx).Warn("can not retry txn",
				zap.String("label", s.getSQLLabel()),
				zap.Error(err),
				zap.Bool("IsBatchInsert", s.sessionVars.BatchInsert),
				zap.Bool("InRestrictedSQL", s.sessionVars.InRestrictedSQL),
				zap.Int64("tidb_retry_limit", s.sessionVars.RetryLimit),
				zap.Bool("tidb_disable_txn_auto_retry", s.sessionVars.DisableTxnAutoRetry))
		}

	}
	label := s.getSQLLabel()
	counter := s.sessionVars.TxnCtx.StatementCount
	duration := time.Since(s.GetSessionVars().TxnCtx.CreateTime).Seconds()
	metrics.StatementPerTransaction.WithLabelValues(label, metrics.RetLabel(err)).Observe(float64(counter))
	metrics.TransactionDuration.WithLabelValues(label, metrics.RetLabel(err)).Observe(float64(duration))
	s.cleanRetryInfo()

	if isoLevelOneShot := &s.sessionVars.TxnIsolationLevelOneShot; isoLevelOneShot.State != 0 {
		switch isoLevelOneShot.State {
		case 1:
			isoLevelOneShot.State = 2
		case 2:
			isoLevelOneShot.State = 0
			isoLevelOneShot.Value = ""
		}
	}

	if err != nil {
		logutil.Logger(ctx).Warn("commit failed",
			zap.String("finished txn", s.txn.GoString()),
			zap.Error(err))
		return errors.Trace(err)
	}
	mapper := s.GetSessionVars().TxnCtx.TableDeltaMap
	if s.statsCollector != nil && mapper != nil {
		for id, item := range mapper {
			s.statsCollector.Update(id, item.Delta, item.Count, &item.ColSize)
		}
	}
	return nil
}

func (s *session) CommitTxn(ctx context.Context) error {
	var commitDetail *execdetails.CommitDetails
	ctx = context.WithValue(ctx, execdetails.CommitDetailCtxKey, &commitDetail)
	err := s.doCommitWithRetry(ctx)
	if commitDetail != nil {
		s.sessionVars.StmtCtx.MergeExecDetails(nil, commitDetail)
	}
	label := metrics.LblOK
	if err != nil {
		label = metrics.LblError
	}
	s.sessionVars.TxnCtx.Cleanup()
<<<<<<< HEAD
	metrics.TransactionCounter.WithLabelValues(s.getSQLLabel(), label).Inc()
	return errors.Trace(err)
=======
	s.recordTransactionCounter(nil, err)
	return err
>>>>>>> ea6d00b... *: add a new way to calculate TPS (#12411)
}

func (s *session) RollbackTxn(ctx context.Context) error {
	var err error
	if s.txn.Valid() {
		terror.Log(s.txn.Rollback())
		metrics.TransactionCounter.WithLabelValues(s.getSQLLabel(), metrics.LblRollback).Inc()
	}
	s.cleanRetryInfo()
	s.txn.changeToInvalid()
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	return errors.Trace(err)
}

func (s *session) GetClient() kv.Client {
	return s.store.GetClient()
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]interface{}{
		"id":         sessVars.ConnectionID,
		"user":       sessVars.User,
		"currDBName": sessVars.CurrentDB,
		"status":     sessVars.Status,
		"strictMode": sessVars.StrictSQLMode,
	}
	if s.txn.Valid() {
		// if txn is committed or rolled back, txn is nil.
		data["txn"] = s.txn.String()
	}
	if sessVars.SnapshotTS != 0 {
		data["snapshotTS"] = sessVars.SnapshotTS
	}
	if sessVars.StmtCtx.LastInsertID > 0 {
		data["lastInsertID"] = sessVars.StmtCtx.LastInsertID
	}
	if len(sessVars.PreparedStmts) > 0 {
		data["preparedStmtCount"] = len(sessVars.PreparedStmts)
	}
	b, err := json.MarshalIndent(data, "", "  ")
	terror.Log(errors.Trace(err))
	return string(b)
}

const sqlLogMaxLen = 1024

// SchemaChangedWithoutRetry is used for testing.
var SchemaChangedWithoutRetry bool

func (s *session) getSQLLabel() string {
	if s.sessionVars.InRestrictedSQL {
		return metrics.LblInternal
	}
	return metrics.LblGeneral
}

func (s *session) isRetryableError(err error) bool {
	if SchemaChangedWithoutRetry {
		return kv.IsRetryableError(err)
	}
	return kv.IsRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

func (s *session) checkTxnAborted(stmt sqlexec.Statement) error {
	if s.txn.doNotCommit == nil {
		return nil
	}
	// If the transaction is aborted, the following statements do not need to execute, except `commit` and `rollback`,
	// because they are used to finish the aborted transaction.
	if _, ok := stmt.(*executor.ExecStmt).StmtNode.(*ast.CommitStmt); ok {
		return nil
	}
	if _, ok := stmt.(*executor.ExecStmt).StmtNode.(*ast.RollbackStmt); ok {
		return nil
	}
	return errors.New("current transaction is aborted, commands ignored until end of transaction block")
}

func (s *session) retry(ctx context.Context, maxCnt uint) (err error) {
	var retryCnt uint
	defer func() {
		s.sessionVars.RetryInfo.Retrying = false
		// retryCnt only increments on retryable error, so +1 here.
		metrics.SessionRetry.Observe(float64(retryCnt + 1))
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
		if err != nil {
			s.rollbackOnError(ctx)
		}
		s.txn.changeToInvalid()
	}()

	connID := s.sessionVars.ConnectionID
	s.sessionVars.RetryInfo.Retrying = true
	if s.sessionVars.TxnCtx.ForUpdate {
		err = errForUpdateCantRetry.GenWithStackByArgs(connID)
		return err
	}

	nh := GetHistory(s)
	var schemaVersion int64
	sessVars := s.GetSessionVars()
	orgStartTS := sessVars.TxnCtx.StartTS
	label := s.getSQLLabel()
	for {
		s.PrepareTxnCtx(ctx)
		s.sessionVars.RetryInfo.ResetOffset()
		for i, sr := range nh.history {
			st := sr.st
			s.sessionVars.StmtCtx = sr.stmtCtx
			s.sessionVars.StartTime = time.Now()
			s.sessionVars.DurationCompile = time.Duration(0)
			s.sessionVars.DurationParse = time.Duration(0)
			s.sessionVars.StmtCtx.ResetForRetry()
			s.sessionVars.PreparedParams = s.sessionVars.PreparedParams[:0]
			schemaVersion, err = st.RebuildPlan()
			if err != nil {
				return errors.Trace(err)
			}

			if retryCnt == 0 {
				// We do not have to log the query every time.
				// We print the queries at the first try only.
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i),
					zap.String("sql", sqlForLog(st.OriginText())+sessVars.PreparedParams.String()))
			} else {
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i))
			}
			_, err = st.Exec(ctx)
			if err != nil {
				s.StmtRollback()
				break
			}
			err = s.StmtCommit()
			if err != nil {
				return errors.Trace(err)
			}
		}
		logutil.Logger(ctx).Warn("transaction association",
			zap.Uint64("retrying txnStartTS", s.GetSessionVars().TxnCtx.StartTS),
			zap.Uint64("original txnStartTS", orgStartTS))
		if hook := ctx.Value("preCommitHook"); hook != nil {
			// For testing purpose.
			hook.(func())()
		}
		if err == nil {
			err = s.doCommit(ctx)
			if err == nil {
				break
			}
		}
		if !s.isRetryableError(err) {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Stringer("session", s),
				zap.Error(err))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblUnretryable)
			return errors.Trace(err)
		}
		retryCnt++
		if retryCnt >= maxCnt {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Uint("retry reached max count", retryCnt))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblReachMax)
			return errors.Trace(err)
		}
		logutil.Logger(ctx).Warn("sql",
			zap.String("label", label),
			zap.Error(err),
			zap.String("txn", s.txn.GoString()))
		kv.BackOff(retryCnt)
		s.txn.changeToInvalid()
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}
	return err
}

func sqlForLog(sql string) string {
	if len(sql) > sqlLogMaxLen {
		sql = sql[:sqlLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	return executor.QueryReplacer.Replace(sql)
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

func (s *session) sysSessionPool() sessionPool {
	return domain.GetDomain(s).SysSessionPool()
}

// ExecRestrictedSQL implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements, usually executed during a normal statement execution.
// Unlike normal Exec, it doesn't reset statement status, doesn't commit or rollback the current transaction
// and doesn't write binlog.
func (s *session) ExecRestrictedSQL(sctx sessionctx.Context, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	ctx := context.TODO()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	se := tmp.(*session)
	defer s.sysSessionPool().Put(tmp)
	metrics.SessionRestrictedSQLCounter.Inc()

	startTime := time.Now()
	recordSets, err := se.Execute(ctx, sql)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var (
		rows   []chunk.Row
		fields []*ast.ResultField
	)
	// Execute all recordset, take out the first one as result.
	for i, rs := range recordSets {
		tmp, err := drainRecordSet(ctx, se, rs)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if err = rs.Close(); err != nil {
			return nil, nil, errors.Trace(err)
		}

		if i == 0 {
			rows = tmp
			fields = rs.Fields()
		}
	}
	metrics.QueryDurationHistogram.WithLabelValues(metrics.LblInternal).Observe(time.Since(startTime).Seconds())
	return rows, fields, nil
}

func createSessionFunc(store kv.Storage) pools.Factory {
	return func() (pools.Resource, error) {
		se, err := createSession(store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.AutocommitVar, types.NewStringDatum("1"))
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.MaxExecutionTime, types.NewUintDatum(0))
		if err != nil {
			return nil, errors.Trace(err)
		}
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		return se, nil
	}
}

func createSessionWithDomainFunc(store kv.Storage) func(*domain.Domain) (pools.Resource, error) {
	return func(dom *domain.Domain) (pools.Resource, error) {
		se, err := createSessionWithDomain(store, dom)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.AutocommitVar, types.NewStringDatum("1"))
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.MaxExecutionTime, types.NewUintDatum(0))
		if err != nil {
			return nil, errors.Trace(err)
		}
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		return se, nil
	}
}

func drainRecordSet(ctx context.Context, se *session, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	var rows []chunk.Row
	chk := rs.NewChunk()
	for {
		err := rs.Next(ctx, chk)
		if err != nil || chk.NumRows() == 0 {
			return rows, errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(chk)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		chk = chunk.Renew(chk, se.sessionVars.MaxChunkSize)
	}
}

// getExecRet executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getExecRet(ctx sessionctx.Context, sql string) (string, error) {
	rows, fields, err := s.ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(rows) == 0 {
		return "", executor.ErrResultIsEmpty
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	value, err := d.ToString()
	if err != nil {
		return "", err
	}
	return value, nil
}

// GetAllSysVars implements GlobalVarAccessor.GetAllSysVars interface.
func (s *session) GetAllSysVars() (map[string]string, error) {
	if s.Value(sessionctx.Initing) != nil {
		return nil, nil
	}
	sql := `SELECT VARIABLE_NAME, VARIABLE_VALUE FROM %s.%s;`
	sql = fmt.Sprintf(sql, mysql.SystemDB, mysql.GlobalVariablesTable)
	rows, _, err := s.ExecRestrictedSQL(s, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := make(map[string]string)
	for _, r := range rows {
		k, v := r.GetString(0), r.GetString(1)
		ret[k] = v
	}
	return ret, nil
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (s *session) GetGlobalSysVar(name string) (string, error) {
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name)
	sysVar, err := s.getExecRet(s, sql)
	if err != nil {
		if executor.ErrResultIsEmpty.Equal(err) {
			if sv, ok := variable.SysVars[name]; ok {
				return sv.Value, nil
			}
			return "", variable.UnknownSystemVar.GenWithStackByArgs(name)
		}
		return "", errors.Trace(err)
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(name, value string) error {
	if name == variable.SQLModeVar {
		value = mysql.FormatSQLModeStr(value)
		if _, err := mysql.GetSQLMode(value); err != nil {
			return errors.Trace(err)
		}
	}
	var sVal string
	var err error
	sVal, err = variable.ValidateSetSystemVar(s.sessionVars, name, value)
	if err != nil {
		return errors.Trace(err)
	}
	name = strings.ToLower(name)
	sql := fmt.Sprintf(`REPLACE %s.%s VALUES ('%s', '%s');`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name, sVal)
	_, _, err = s.ExecRestrictedSQL(s, sql)
	return errors.Trace(err)
}

func (s *session) ParseSQL(ctx context.Context, sql, charset, collation string) ([]ast.StmtNode, []error, error) {
	s.parser.SetSQLMode(s.sessionVars.SQLMode)
	return s.parser.Parse(sql, charset, collation)
}

func (s *session) SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64) {
	var db interface{}
	if len(s.sessionVars.CurrentDB) > 0 {
		db = s.sessionVars.CurrentDB
	}

	var info interface{}
	if len(sql) > 0 {
		info = sql
	}
	pi := util.ProcessInfo{
		ID:      s.sessionVars.ConnectionID,
		DB:      db,
		Command: command,
		Time:    t,
		State:   s.Status(),
		Info:    info,
		StmtCtx: s.sessionVars.StmtCtx,

		MaxExecutionTime: maxExecutionTime,
	}
	if s.sessionVars.User != nil {
		pi.User = s.sessionVars.User.Username
		pi.Host = s.sessionVars.User.Hostname
	}
	s.processInfo.Store(&pi)
}

func (s *session) executeStatement(ctx context.Context, connID uint64, stmtNode ast.StmtNode, stmt sqlexec.Statement, recordSets []sqlexec.RecordSet) ([]sqlexec.RecordSet, error) {
	s.SetValue(sessionctx.QueryString, stmt.OriginText())
	if _, ok := stmtNode.(ast.DDLNode); ok {
		s.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		s.ClearValue(sessionctx.LastExecuteDDL)
	}
	logStmt(stmtNode, s.sessionVars)
	startTime := time.Now()
	recordSet, err := runStmt(ctx, s, stmt)
	if err != nil {
		if !kv.ErrKeyExists.Equal(err) {
			logutil.Logger(ctx).Warn("run statement error",
				zap.Int64("schemaVersion", s.sessionVars.TxnCtx.SchemaVersion),
				zap.Error(err),
				zap.String("session", s.String()))
		}
<<<<<<< HEAD
		return nil, errors.Trace(err)
=======
		return nil, err
	}
	s.recordTransactionCounter(stmtNode, err)
	if s.isInternal() {
		sessionExecuteRunDurationInternal.Observe(time.Since(startTime).Seconds())
	} else {
		sessionExecuteRunDurationGeneral.Observe(time.Since(startTime).Seconds())
	}

	if inMulitQuery && recordSet == nil {
		recordSet = &multiQueryNoDelayRecordSet{
			affectedRows: s.AffectedRows(),
			lastMessage:  s.LastMessage(),
			warnCount:    s.sessionVars.StmtCtx.WarningCount(),
			lastInsertID: s.sessionVars.StmtCtx.LastInsertID,
			status:       s.sessionVars.Status,
		}
>>>>>>> ea6d00b... *: add a new way to calculate TPS (#12411)
	}
	metrics.SessionExecuteRunDuration.WithLabelValues(s.getSQLLabel()).Observe(time.Since(startTime).Seconds())

	if recordSet != nil {
		recordSets = append(recordSets, recordSet)
	}
	return recordSets, nil
}

func (s *session) Execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	if recordSets, err = s.execute(ctx, sql); err != nil {
		err = errors.Trace(err)
		s.sessionVars.StmtCtx.AppendError(err)
	}
	return
}

func (s *session) execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	s.PrepareTxnCtx(ctx)
	connID := s.sessionVars.ConnectionID
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return nil, err
	}

	charsetInfo, collation := s.sessionVars.GetCharsetInfo()

	// Step1: Compile query string to abstract syntax trees(ASTs).
	startTS := time.Now()
	s.GetSessionVars().StartTime = startTS
	stmtNodes, warns, err := s.ParseSQL(ctx, sql, charsetInfo, collation)
	if err != nil {
		s.rollbackOnError(ctx)
		logutil.Logger(ctx).Warn("parse sql error",
			zap.Error(err),
			zap.String("sql", sql))
		return nil, errors.Trace(err)
	}
	durParse := time.Since(startTS)
	s.GetSessionVars().DurationParse = durParse
	label := s.getSQLLabel()
	metrics.SessionExecuteParseDuration.WithLabelValues(label).Observe(durParse.Seconds())

	compiler := executor.Compiler{Ctx: s}
	for _, stmtNode := range stmtNodes {
		s.PrepareTxnCtx(ctx)

		// Step2: Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
		startTS = time.Now()
		// Some executions are done in compile stage, so we reset them before compile.
		if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
			return nil, errors.Trace(err)
		}
		stmt, err := compiler.Compile(ctx, stmtNode)
		if err != nil {
			s.rollbackOnError(ctx)
			logutil.Logger(ctx).Warn("compile sql error",
				zap.Error(err),
				zap.String("sql", sql))
			return nil, errors.Trace(err)
		}

		durCompile := time.Since(startTS)
		s.GetSessionVars().DurationCompile = durCompile
		metrics.SessionExecuteCompileDuration.WithLabelValues(label).Observe(durCompile.Seconds())

		// Step3: Execute the physical plan.
		if recordSets, err = s.executeStatement(ctx, connID, stmtNode, stmt, recordSets); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if s.sessionVars.ClientCapability&mysql.ClientMultiResults == 0 && len(recordSets) > 1 {
		// return the first recordset if client doesn't support ClientMultiResults.
		recordSets = recordSets[:1]
	}

	for _, warn := range warns {
		s.sessionVars.StmtCtx.AppendWarning(warn)
	}
	return recordSets, nil
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *session) rollbackOnError(ctx context.Context) {
	if !s.sessionVars.InTxn() {
		terror.Log(s.RollbackTxn(ctx))
	}
}

// PrepareStmt is used for executing prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error) {
	if s.sessionVars.TxnCtx.InfoSchema == nil {
		// We don't need to create a transaction for prepare statement, just get information schema will do.
		s.sessionVars.TxnCtx.InfoSchema = domain.GetDomain(s).InfoSchema()
	}
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		err = errors.Trace(err)
		return
	}

	ctx := context.Background()
	inTxn := s.GetSessionVars().InTxn()
	// NewPrepareExec may need startTS to build the executor, for example prepare statement has subquery in int.
	// So we have to call PrepareTxnCtx here.
	s.PrepareTxnCtx(ctx)
	prepareExec := executor.NewPrepareExec(s, executor.GetInfoSchema(s), sql)
	err = prepareExec.Next(ctx, nil)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if !inTxn {
		// We could start a transaction to build the prepare executor before, we should rollback it here.
		err = s.RollbackTxn(ctx)
		if err != nil {
			err = errors.Trace(err)
			return
		}
	}
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.Fields, nil
}

// checkArgs makes sure all the arguments' types are known and can be handled.
// integer types are converted to int64 and uint64, time.Time is converted to types.Time.
// time.Duration is converted to types.Duration, other known types are leaved as it is.
func checkArgs(args ...interface{}) error {
	for i, v := range args {
		switch x := v.(type) {
		case bool:
			if x {
				args[i] = int64(1)
			} else {
				args[i] = int64(0)
			}
		case int8:
			args[i] = int64(x)
		case int16:
			args[i] = int64(x)
		case int32:
			args[i] = int64(x)
		case int:
			args[i] = int64(x)
		case uint8:
			args[i] = uint64(x)
		case uint16:
			args[i] = uint64(x)
		case uint32:
			args[i] = uint64(x)
		case uint:
			args[i] = uint64(x)
		case int64:
		case uint64:
		case float32:
		case float64:
		case string:
		case []byte:
		case time.Duration:
			args[i] = types.Duration{Duration: x}
		case time.Time:
			args[i] = types.Time{Time: types.FromGoTime(x), Type: mysql.TypeDatetime}
		case nil:
		default:
			return errors.Errorf("cannot use arg[%d] (type %T):unsupported type", i, v)
		}
	}
	return nil
}

// ExecutePreparedStmt executes a prepared statement.
func (s *session) ExecutePreparedStmt(ctx context.Context, stmtID uint32, args ...interface{}) (sqlexec.RecordSet, error) {
	err := checkArgs(args...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.PrepareTxnCtx(ctx)
	s.sessionVars.StartTime = time.Now()
	st, err := executor.CompileExecutePreparedStmt(s, stmtID, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	logQuery(st.OriginText(), s.sessionVars)
	r, err := runStmt(ctx, s, st)
	return r, errors.Trace(err)
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	vars := s.sessionVars
	if _, ok := vars.PreparedStmts[stmtID]; !ok {
		return plannercore.ErrStmtNotFound
	}
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
}

func (s *session) Txn(active bool) (kv.Transaction, error) {
	if !s.txn.validOrPending() && active {
		return &s.txn, kv.ErrInvalidTxn
	}
	if s.txn.pending() && active {
		// Transaction is lazy initialized.
		// PrepareTxnCtx is called to get a tso future, makes s.txn a pending txn,
		// If Txn() is called later, wait for the future to get a valid txn.
		txnCap := s.getMembufCap()
		if err := s.txn.changePendingToValid(txnCap); err != nil {
			logutil.Logger(context.Background()).Error("active transaction fail",
				zap.Error(err))
			s.txn.cleanup()
			s.sessionVars.TxnCtx.StartTS = 0
			return &s.txn, errors.Trace(err)
		}
		s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
		if !s.sessionVars.IsAutocommit() {
			s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
	}
	return &s.txn, nil
}

func (s *session) NewTxn() error {
	if s.txn.Valid() {
		txnID := s.txn.StartTS()
		ctx := context.TODO()
		err := s.CommitTxn(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		vars := s.GetSessionVars()
		logutil.Logger(ctx).Info("NewTxn() inside a transaction auto commit",
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", txnID))
	}

	txn, err := s.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	txn.SetCap(s.getMembufCap())
	txn.SetVars(s.sessionVars.KVVars)
	s.txn.changeInvalidToValid(txn)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		StartTS:       txn.StartTS(),
	}
	return nil
}

func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	s.mu.Lock()
	s.mu.values[key] = value
	s.mu.Unlock()
}

func (s *session) Value(key fmt.Stringer) interface{} {
	s.mu.RLock()
	value := s.mu.values[key]
	s.mu.RUnlock()
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	s.mu.Lock()
	delete(s.mu.values, key)
	s.mu.Unlock()
}

// Close function does some clean work when session end.
func (s *session) Close() {
	if s.statsCollector != nil {
		s.statsCollector.Delete()
	}
	ctx := context.TODO()
	if err := s.RollbackTxn(ctx); err != nil {
		logutil.Logger(context.Background()).Error("session Close failed",
			zap.Error(err))
	}
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s *session) Auth(user *auth.UserIdentity, authentication []byte, salt []byte) bool {
	pm := privilege.GetPrivilegeManager(s)

	// Check IP or localhost.
	if pm.ConnectionVerification(user.Username, user.Hostname, authentication, salt) {
		s.sessionVars.User = user
		return true
	} else if user.Hostname == variable.DefHostname {
		logutil.Logger(context.Background()).Error("user connection verification failed",
			zap.Stringer("user", user))
		return false
	}

	// Check Hostname.
	for _, addr := range getHostByIP(user.Hostname) {
		if pm.ConnectionVerification(user.Username, addr, authentication, salt) {
			s.sessionVars.User = &auth.UserIdentity{
				Username: user.Username,
				Hostname: addr,
			}
			return true
		}
	}

	logutil.Logger(context.Background()).Error("user connection verification failed",
		zap.Stringer("user", user))
	return false
}

func getHostByIP(ip string) []string {
	if ip == "127.0.0.1" {
		return []string{variable.DefHostname}
	}
	addrs, err := net.LookupAddr(ip)
	terror.Log(errors.Trace(err))
	return addrs
}

// CreateSession4Test creates a new session environment for test.
func CreateSession4Test(store kv.Storage) (Session, error) {
	s, err := CreateSession(store)
	if err == nil {
		// initialize session variables for test.
		s.GetSessionVars().MaxChunkSize = 2
	}
	return s, errors.Trace(err)
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (Session, error) {
	s, err := createSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Add auth here.
	do, err := domap.Get(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pm := &privileges.UserPrivileges{
		Handle: do.PrivilegeHandle(),
	}
	privilege.BindPrivilegeManager(s, pm)

	// Add stats collector, and it will be freed by background stats worker
	// which periodically updates stats using the collected data.
	if do.StatsHandle() != nil && do.StatsUpdating() {
		s.statsCollector = do.StatsHandle().NewSessionStatsCollector()
	}

	return s, nil
}

// loadSystemTZ loads systemTZ from mysql.tidb
func loadSystemTZ(se *session) (string, error) {
	sql := `select variable_value from mysql.tidb where variable_name = 'system_tz'`
	rss, errLoad := se.Execute(context.Background(), sql)
	if errLoad != nil {
		return "", errLoad
	}
	// the record of mysql.tidb under where condition: variable_name = "system_tz" should shall only be one.
	defer func() {
		if err := rss[0].Close(); err != nil {
			logutil.Logger(context.Background()).Error("close result set error", zap.Error(err))
		}
	}()
	chk := rss[0].NewChunk()
	if err := rss[0].Next(context.Background(), chk); err != nil {
		return "", errors.Trace(err)
	}
	return chk.GetRow(0).GetString(0), nil
}

// BootstrapSession runs the first time when the TiDB server start.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	cfg := config.GetGlobalConfig()
	if len(cfg.Plugin.Load) > 0 {
		err := plugin.Load(context.Background(), plugin.Config{
			Plugins:        strings.Split(cfg.Plugin.Load, ","),
			PluginDir:      cfg.Plugin.Dir,
			GlobalSysVar:   &variable.SysVars,
			PluginVarNames: &variable.PluginVarNames,
		})
		if err != nil {
			return nil, err
		}
	}

	initLoadCommonGlobalVarsSQL()

	ver := getStoreBootstrapVersion(store)
	if ver == notBootstrapped {
		runInBootstrapSession(store, bootstrap)
	} else if ver < currentBootstrapVersion {
		runInBootstrapSession(store, upgrade)
	}

	se, err := createSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// get system tz from mysql.tidb
	tz, err := loadSystemTZ(se)
	if err != nil {
		return nil, errors.Trace(err)
	}

	timeutil.SetSystemTZ(tz)
	dom := domain.GetDomain(se)
	dom.InitExpensiveQueryHandle()

	if !config.GetGlobalConfig().Security.SkipGrantTable {
		err = dom.LoadPrivilegeLoop(se)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if len(cfg.Plugin.Load) > 0 {
		err := plugin.Init(context.Background(), plugin.Config{EtcdClient: dom.GetEtcdClient()})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	err = executor.LoadExprPushdownBlacklist(se)
	if err != nil {
		return nil, err
	}

	se1, err := createSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = dom.UpdateTableStatsLoop(se1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if raw, ok := store.(domain.EtcdBackend); ok {
		err = raw.StartGCWorker()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return dom, errors.Trace(err)
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domap.Get(store)
}

// runInBootstrapSession create a special session for boostrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstrap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, bootstrap func(Session)) {
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.Logger(context.Background()).Fatal("createSession error", zap.Error(err))
	}

	s.SetValue(sessionctx.Initing, true)
	bootstrap(s)
	finishBootstrap(store)
	s.ClearValue(sessionctx.Initing)

	dom := domain.GetDomain(s)
	dom.Close()
	domap.Delete(store)
}

func createSession(store kv.Storage) (*session, error) {
	dom, err := domap.Get(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s := &session{
		store:           store,
		parser:          parser.New(),
		sessionVars:     variable.NewSessionVars(),
		ddlOwnerChecker: dom.DDL().OwnerManager(),
	}
	if plannercore.PreparedPlanCacheEnabled() {
		s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity)
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.sessionVars.BinlogClient = binloginfo.GetPumpsClient()
	s.txn.init()
	return s, nil
}

// createSessionWithDomain creates a new Session and binds it with a Domain.
// We need this because when we start DDL in Domain, the DDL need a session
// to change some system tables. But at that time, we have been already in
// a lock context, which cause we can't call createSesion directly.
func createSessionWithDomain(store kv.Storage, dom *domain.Domain) (*session, error) {
	s := &session{
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	if plannercore.PreparedPlanCacheEnabled() {
		s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity)
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.txn.init()
	return s, nil
}

const (
	notBootstrapped         = 0
	currentBootstrapVersion = 25
)

func getStoreBootstrapVersion(store kv.Storage) int64 {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	// check in memory
	_, ok := storeBootstrapped[store.UUID()]
	if ok {
		return currentBootstrapVersion
	}

	var ver int64
	// check in kv store
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		var err error
		t := meta.NewMeta(txn)
		ver, err = t.GetBootstrapVersion()
		return errors.Trace(err)
	})

	if err != nil {
		logutil.Logger(context.Background()).Fatal("check bootstrapped failed",
			zap.Error(err))
	}

	if ver > notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		storeBootstrapped[store.UUID()] = true
	}

	return ver
}

func finishBootstrap(store kv.Storage) {
	storeBootstrappedLock.Lock()
	storeBootstrapped[store.UUID()] = true
	storeBootstrappedLock.Unlock()

	err := kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return errors.Trace(err)
	})
	if err != nil {
		logutil.Logger(context.Background()).Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}

const quoteCommaQuote = "', '"

var builtinGlobalVariable = []string{
	variable.AutocommitVar,
	variable.SQLModeVar,
	variable.MaxAllowedPacket,
	variable.TimeZone,
	variable.BlockEncryptionMode,
	variable.MaxExecutionTime,
	/* TiDB specific global variables: */
	variable.TiDBSkipUTF8Check,
	variable.TiDBIndexJoinBatchSize,
	variable.TiDBIndexLookupSize,
	variable.TiDBIndexLookupConcurrency,
	variable.TiDBIndexLookupJoinConcurrency,
	variable.TiDBIndexSerialScanConcurrency,
	variable.TiDBHashJoinConcurrency,
	variable.TiDBProjectionConcurrency,
	variable.TiDBHashAggPartialConcurrency,
	variable.TiDBHashAggFinalConcurrency,
	variable.TiDBBackoffLockFast,
	variable.TiDBConstraintCheckInPlace,
	variable.TiDBOptInSubqUnFolding,
	variable.TiDBDistSQLScanConcurrency,
	variable.TiDBMaxChunkSize,
	variable.TiDBRetryLimit,
	variable.TiDBDisableTxnAutoRetry,
}

var (
	loadCommonGlobalVarsSQLOnce sync.Once
	loadCommonGlobalVarsSQL     string
)

func initLoadCommonGlobalVarsSQL() {
	loadCommonGlobalVarsSQLOnce.Do(func() {
		vars := append(make([]string, 0, len(builtinGlobalVariable)+len(variable.PluginVarNames)), builtinGlobalVariable...)
		if len(variable.PluginVarNames) > 0 {
			vars = append(vars, variable.PluginVarNames...)
		}
		loadCommonGlobalVarsSQL = "select HIGH_PRIORITY * from mysql.global_variables where variable_name in ('" + strings.Join(vars, quoteCommaQuote) + "')"
	})
}

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	initLoadCommonGlobalVarsSQL()
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return nil
	}

	var err error
	// Use GlobalVariableCache if TiDB just loaded global variables within 2 second ago.
	// When a lot of connections connect to TiDB simultaneously, it can protect TiKV meta region from overload.
	gvc := domain.GetDomain(s).GetGlobalVarsCache()
	succ, rows, fields := gvc.Get()
	if !succ {
		// Set the variable to true to prevent cyclic recursive call.
		vars.CommonGlobalLoaded = true
		rows, fields, err = s.ExecRestrictedSQL(s, loadCommonGlobalVarsSQL)
		if err != nil {
			vars.CommonGlobalLoaded = false
			logutil.Logger(context.Background()).Error("failed to load common global variables.")
			return errors.Trace(err)
		}
		gvc.Update(rows, fields)
	}

	for _, row := range rows {
		varName := row.GetString(0)
		varVal := row.GetDatum(1, &fields[1].Column.FieldType)
		if _, ok := vars.GetSystemVar(varName); !ok {
			err = variable.SetSessionSystemVar(s.sessionVars, varName, varVal)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	vars.CommonGlobalLoaded = true
	return nil
}

// PrepareTxnCtx starts a goroutine to begin a transaction if needed, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *session) PrepareTxnCtx(ctx context.Context) {
	if s.txn.validOrPending() {
		return
	}

	txnFuture := s.getTxnFuture(ctx)
	s.txn.changeInvalidToPending(txnFuture)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
	}
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	if err := s.doCommit(ctx); err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(s.NewTxn())
}

// InitTxnWithStartTS create a transaction with startTS.
func (s *session) InitTxnWithStartTS(startTS uint64) error {
	if s.txn.Valid() {
		return nil
	}

	// no need to get txn from txnFutureCh since txn should init with startTs
	txn, err := s.store.BeginWithStartTS(startTS)
	if err != nil {
		return errors.Trace(err)
	}
	s.txn.changeInvalidToValid(txn)
	s.txn.SetCap(s.getMembufCap())
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	return s.store
}

func (s *session) ShowProcess() *util.ProcessInfo {
	var pi *util.ProcessInfo
	tmp := s.processInfo.Load()
	if tmp != nil {
		pi = tmp.(*util.ProcessInfo)
	}
	return pi
}

// logStmt logs some crucial SQL including: CREATE USER/GRANT PRIVILEGE/CHANGE PASSWORD/DDL etc and normal SQL
// if variable.ProcessGeneralLog is set.
func logStmt(node ast.StmtNode, vars *variable.SessionVars) {
	switch stmt := node.(type) {
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateIndexStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropIndexStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt:
		user := vars.User
		schemaVersion := vars.TxnCtx.SchemaVersion
		if ss, ok := node.(ast.SensitiveStmtNode); ok {
			logutil.Logger(context.Background()).Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("secure text", ss.SecureText()),
				zap.Stringer("user", user))
		} else {
			logutil.Logger(context.Background()).Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("cur_db", vars.CurrentDB),
				zap.String("sql", stmt.Text()),
				zap.Stringer("user", user))
		}
	default:
		logQuery(node.Text(), vars)
	}
}

func logQuery(query string, vars *variable.SessionVars) {
	if atomic.LoadUint32(&variable.ProcessGeneralLog) != 0 && !vars.InRestrictedSQL {
		query = executor.QueryReplacer.Replace(query)
		logutil.Logger(context.Background()).Info("GENERAL_LOG",
			zap.Uint64("conn", vars.ConnectionID),
			zap.Stringer("user", vars.User),
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", vars.TxnCtx.StartTS),
			zap.String("current_db", vars.CurrentDB),
			zap.String("sql", query+vars.PreparedParams.String()))
	}
}
<<<<<<< HEAD
=======

func (s *session) recordOnTransactionExecution(err error, counter int, duration float64) {
	if s.isInternal() {
		if err != nil {
			statementPerTransactionInternalError.Observe(float64(counter))
			transactionDurationInternalError.Observe(duration)
		} else {
			statementPerTransactionInternalOK.Observe(float64(counter))
			transactionDurationInternalOK.Observe(duration)
		}
	} else {
		if err != nil {
			statementPerTransactionGeneralError.Observe(float64(counter))
			transactionDurationGeneralError.Observe(duration)
		} else {
			statementPerTransactionGeneralOK.Observe(float64(counter))
			transactionDurationGeneralOK.Observe(duration)
		}
	}
}

func (s *session) recordTransactionCounter(stmtNode ast.StmtNode, err error) {
	if stmtNode == nil {
		if s.isInternal() {
			if err != nil {
				transactionCounterInternalErr.Inc()
			} else {
				transactionCounterInternalOK.Inc()
			}
		} else {
			if err != nil {
				transactionCounterGeneralErr.Inc()
			} else {
				transactionCounterGeneralOK.Inc()
			}
		}
		return
	}

	var isTxn bool
	switch stmtNode.(type) {
	case *ast.CommitStmt:
		isTxn = true
	case *ast.RollbackStmt:
		isTxn = true
	}
	if !isTxn {
		return
	}
	if s.isInternal() {
		transactionCounterInternalCommitRollback.Inc()
	} else {
		transactionCounterGeneralCommitRollback.Inc()
	}
}

type multiQueryNoDelayRecordSet struct {
	sqlexec.RecordSet

	affectedRows uint64
	lastMessage  string
	status       uint16
	warnCount    uint16
	lastInsertID uint64
}

func (c *multiQueryNoDelayRecordSet) Close() error {
	return nil
}

func (c *multiQueryNoDelayRecordSet) AffectedRows() uint64 {
	return c.affectedRows
}

func (c *multiQueryNoDelayRecordSet) LastMessage() string {
	return c.lastMessage
}

func (c *multiQueryNoDelayRecordSet) WarnCount() uint16 {
	return c.warnCount
}

func (c *multiQueryNoDelayRecordSet) Status() uint16 {
	return c.status
}

func (c *multiQueryNoDelayRecordSet) LastInsertID() uint64 {
	return c.lastInsertID
}
>>>>>>> ea6d00b... *: add a new way to calculate TPS (#12411)
