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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

var (
	statementPerTransactionPessimisticOK    = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblOK)
	statementPerTransactionPessimisticError = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblError)
	statementPerTransactionOptimisticOK     = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblOK)
	statementPerTransactionOptimisticError  = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblError)
	transactionDurationPessimisticCommit    = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblCommit)
	transactionDurationPessimisticAbort     = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblAbort)
	transactionDurationOptimisticCommit     = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblCommit)
	transactionDurationOptimisticAbort      = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblAbort)

	sessionExecuteCompileDurationInternal = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteCompileDurationGeneral  = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblGeneral)
	sessionExecuteParseDurationInternal   = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteParseDurationGeneral    = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblGeneral)

	tiKVGCAutoConcurrency = "tikv_gc_auto_concurrency"
)

var gcVariableComments = map[string]string{
	variable.TiKVGCRunInterval:  "GC run interval, at least 10m, in Go format.",
	variable.TiKVGCLifetime:     "All versions within life time will not be collected by GC, at least 10m, in Go format.",
	variable.TiKVGCConcurrency:  "How many goroutines used to do GC parallel, [1, 128], default 2",
	variable.TiKVGCEnable:       "Current GC enable status",
	variable.TiKVGCMode:         "Mode of GC, \"central\" or \"distributed\"",
	tiKVGCAutoConcurrency:       "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used",
	variable.TiKVGCScanLockMode: "Mode of scanning locks, \"physical\" or \"legacy\"",
}

// Session context, it is consistent with the lifecycle of a client connection.
type Session interface {
	sessionctx.Context
	Status() uint16       // Flag of current status, such as autocommit.
	LastInsertID() uint64 // LastInsertID is the last inserted auto_increment ID.
	LastMessage() string  // LastMessage is the info message that may be generated by last command
	AffectedRows() uint64 // Affected rows by latest executed stmt.
	// Execute is deprecated, use ExecuteStmt() instead.
	Execute(context.Context, string) ([]sqlexec.RecordSet, error)         // Execute a sql statement.
	ExecuteInternal(context.Context, string) ([]sqlexec.RecordSet, error) // Execute a internal sql statement.
	ExecuteStmt(context.Context, ast.StmtNode) (sqlexec.RecordSet, error)
	Parse(ctx context.Context, sql string) ([]ast.StmtNode, error)
	String() string // String is used to debug.
	CommitTxn(context.Context) error
	RollbackTxn(context.Context)
	// PrepareStmt executes prepare statement in binary protocol.
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// ExecutePreparedStmt executes a prepared statement.
	ExecutePreparedStmt(ctx context.Context, stmtID uint32, param []types.Datum) (sqlexec.RecordSet, error)
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
	AuthWithoutVerification(user *auth.UserIdentity) bool
	ShowProcess() *util.ProcessInfo
	// PrepareTxnCtx is exported for test.
	PrepareTxnCtx(context.Context)
	// FieldList returns fields list of a table.
	FieldList(tableName string) (fields []*ast.ResultField, err error)
}

var (
	_ Session = (*session)(nil)
)

type stmtRecord struct {
	st      sqlexec.Statement
	stmtCtx *stmtctx.StatementContext
}

// StmtHistory holds all histories of statements in a txn.
type StmtHistory struct {
	history []*stmtRecord
}

// Add appends a stmt to history list.
func (h *StmtHistory) Add(st sqlexec.Statement, stmtCtx *stmtctx.StatementContext) {
	s := &stmtRecord{
		st:      st,
		stmtCtx: stmtCtx,
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

	currentCtx  context.Context // only use for runtime.trace, Please NEVER use it.
	currentPlan plannercore.Plan

	store kv.Storage

	parser *parser.Parser

	preparedPlanCache *kvcache.SimpleLRUCache

	sessionVars    *variable.SessionVars
	sessionManager util.SessionManager

	statsCollector *handle.SessionStatsCollector
	// ddlOwnerChecker is used in `select tidb_is_ddl_owner()` statement;
	ddlOwnerChecker owner.DDLOwnerChecker
	// lockedTables use to record the table locks hold by the session.
	lockedTables map[int64]model.TableLockTpInfo

	// client shared coprocessor client per session
	client kv.Client

	mppClient kv.MPPClient

	// indexUsageCollector collects index usage information.
	idxUsageCollector *handle.SessionIndexUsageCollector
}

// AddTableLock adds table lock to the session lock map.
func (s *session) AddTableLock(locks []model.TableLockTpInfo) {
	for _, l := range locks {
		// read only lock is session unrelated, skip it when adding lock to session.
		if l.Tp != model.TableLockReadOnly {
			s.lockedTables[l.TableID] = l
		}
	}
}

// ReleaseTableLocks releases table lock in the session lock map.
func (s *session) ReleaseTableLocks(locks []model.TableLockTpInfo) {
	for _, l := range locks {
		delete(s.lockedTables, l.TableID)
	}
}

// ReleaseTableLockByTableIDs releases table lock in the session lock map by table ID.
func (s *session) ReleaseTableLockByTableIDs(tableIDs []int64) {
	for _, tblID := range tableIDs {
		delete(s.lockedTables, tblID)
	}
}

// CheckTableLocked checks the table lock.
func (s *session) CheckTableLocked(tblID int64) (bool, model.TableLockType) {
	lt, ok := s.lockedTables[tblID]
	if !ok {
		return false, model.TableLockNone
	}
	return true, lt.Tp
}

// GetAllTableLocks gets all table locks table id and db id hold by the session.
func (s *session) GetAllTableLocks() []model.TableLockTpInfo {
	lockTpInfo := make([]model.TableLockTpInfo, 0, len(s.lockedTables))
	for _, tl := range s.lockedTables {
		lockTpInfo = append(lockTpInfo, tl)
	}
	return lockTpInfo
}

// HasLockedTables uses to check whether this session locked any tables.
// If so, the session can only visit the table which locked by self.
func (s *session) HasLockedTables() bool {
	b := len(s.lockedTables) > 0
	return b
}

// ReleaseAllTableLocks releases all table locks hold by the session.
func (s *session) ReleaseAllTableLocks() {
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
}

// DDLOwnerChecker returns s.ddlOwnerChecker.
func (s *session) DDLOwnerChecker() owner.DDLOwnerChecker {
	return s.ddlOwnerChecker
}

func (s *session) cleanRetryInfo() {
	if s.sessionVars.RetryInfo.Retrying {
		return
	}

	retryInfo := s.sessionVars.RetryInfo
	defer retryInfo.Clean()
	if len(retryInfo.DroppedPreparedStmtIDs) == 0 {
		return
	}

	planCacheEnabled := plannercore.PreparedPlanCacheEnabled()
	var cacheKey kvcache.Key
	var preparedAst *ast.Prepared
	if planCacheEnabled {
		firstStmtID := retryInfo.DroppedPreparedStmtIDs[0]
		if preparedPointer, ok := s.sessionVars.PreparedStmts[firstStmtID]; ok {
			preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
			if ok {
				preparedAst = preparedObj.PreparedAst
				cacheKey = plannercore.NewPSTMTPlanCacheKey(s.sessionVars, firstStmtID, preparedAst.SchemaVersion)
			}
		}
	}
	for i, stmtID := range retryInfo.DroppedPreparedStmtIDs {
		if planCacheEnabled {
			if i > 0 && preparedAst != nil {
				plannercore.SetPstmtIDSchemaVersion(cacheKey, stmtID, preparedAst.SchemaVersion, s.sessionVars.IsolationReadEngines)
			}
			s.PreparedPlanCache().Delete(cacheKey)
		}
		s.sessionVars.RemovePreparedStmt(stmtID)
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

func (s *session) LastMessage() string {
	return s.sessionVars.StmtCtx.GetMessage()
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

func (s *session) SetCollation(coID int) error {
	cs, co, err := charset.GetCharsetInfoByID(coID)
	if err != nil {
		return err
	}
	for _, v := range variable.SetNamesVariables {
		terror.Log(s.sessionVars.SetSystemVar(v, cs))
	}
	return s.sessionVars.SetSystemVar(variable.CollationConnection, co)
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
	if fb, ok := feedback.(*statistics.QueryFeedback); !ok || fb == nil || !fb.Valid {
		return
	}
	if s.statsCollector != nil {
		do, err := GetDomain(s.store)
		if err != nil {
			logutil.BgLogger().Debug("domain not found", zap.Error(err))
			metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
			return
		}
		err = s.statsCollector.StoreQueryFeedback(feedback, do.StatsHandle())
		if err != nil {
			logutil.BgLogger().Debug("store query feedback", zap.Error(err))
			metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
			return
		}
		metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblOK).Inc()
	}
}

// StoreIndexUsage stores index usage information in idxUsageCollector.
func (s *session) StoreIndexUsage(tblID int64, idxID int64, rowsSelected int64) {
	if s.idxUsageCollector == nil {
		return
	}
	s.idxUsageCollector.Update(tblID, idxID, &handle.IndexUsageInformation{QueryCount: 1, RowsSelected: rowsSelected})
}

// FieldList returns fields list of a table.
func (s *session) FieldList(tableName string) ([]*ast.ResultField, error) {
	is := infoschema.GetInfoSchema(s)
	dbName := model.NewCIStr(s.GetSessionVars().CurrentDB)
	tName := model.NewCIStr(tableName)
	pm := privilege.GetPrivilegeManager(s)
	if pm != nil && s.sessionVars.User != nil {
		if !pm.RequestVerification(s.sessionVars.ActiveRoles, dbName.O, tName.O, "", mysql.AllPrivMask) {
			user := s.sessionVars.User
			u := user.Username
			h := user.Hostname
			if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
				u = user.AuthUsername
				h = user.AuthHostname
			}
			return nil, plannercore.ErrTableaccessDenied.GenWithStackByArgs("SELECT", u, h, tableName)
		}
	}
	table, err := is.TableByName(dbName, tName)
	if err != nil {
		return nil, err
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
	err := s.checkPlacementPolicyBeforeCommit()
	if err != nil {
		return err
	}

	// mockCommitError and mockGetTSErrorInRetry use to test PR #8743.
	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) && kv.IsMockCommitErrorEnable() {
			kv.MockCommitErrorDisable()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

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

	// Get the related table or partition IDs.
	relatedPhysicalTables := s.GetSessionVars().TxnCtx.TableDeltaMap
	physicalTableIDs := make([]int64, 0, len(relatedPhysicalTables))
	for id := range relatedPhysicalTables {
		physicalTableIDs = append(physicalTableIDs, id)
	}
	// Set this option for 2 phase commit to validate schema lease.
	s.txn.SetOption(kv.SchemaChecker, domain.NewSchemaChecker(domain.GetDomain(s), s.sessionVars.TxnCtx.SchemaVersion, physicalTableIDs))
	s.txn.SetOption(kv.InfoSchema, s.sessionVars.TxnCtx.InfoSchema)
	s.txn.SetOption(kv.CommitHook, func(info kv.TxnInfo, _ error) { s.sessionVars.LastTxnInfo = info })
	if s.GetSessionVars().EnableAmendPessimisticTxn {
		s.txn.SetOption(kv.SchemaAmender, NewSchemaAmenderForTikvTxn(s))
	}
	s.txn.SetOption(kv.EnableAsyncCommit, s.GetSessionVars().EnableAsyncCommit)
	s.txn.SetOption(kv.Enable1PC, s.GetSessionVars().Enable1PC)
	s.txn.SetOption(kv.GuaranteeExternalConsistency, s.GetSessionVars().GuaranteeExternalConsistency)

	return s.txn.Commit(sessionctx.SetCommitCtx(ctx, s))
}

func (s *session) doCommitWithRetry(ctx context.Context) error {
	defer func() {
		s.GetSessionVars().SetTxnIsolationLevelOneShotStateForNextTxn()
		s.txn.changeToInvalid()
		s.cleanRetryInfo()
	}()
	if !s.txn.Valid() {
		// If the transaction is invalid, maybe it has already been rolled back by the client.
		return nil
	}
	var err error
	txnSize := s.txn.Size()
	isPessimistic := s.txn.IsPessimistic()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.doCommitWitRetry", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	err = s.doCommit(ctx)
	if err != nil {
		commitRetryLimit := s.sessionVars.RetryLimit
		if !s.sessionVars.TxnCtx.CouldRetry {
			commitRetryLimit = 0
		}
		// Don't retry in BatchInsert mode. As a counter-example, insert into t1 select * from t2,
		// BatchInsert already commit the first batch 1000 rows, then it commit 1000-2000 and retry the statement,
		// Finally t1 will have more data than t2, with no errors return to user!
		if s.isTxnRetryableError(err) && !s.sessionVars.BatchInsert && commitRetryLimit > 0 && !isPessimistic {
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
				zap.Bool("IsPessimistic", isPessimistic),
				zap.Bool("InRestrictedSQL", s.sessionVars.InRestrictedSQL),
				zap.Int64("tidb_retry_limit", s.sessionVars.RetryLimit),
				zap.Bool("tidb_disable_txn_auto_retry", s.sessionVars.DisableTxnAutoRetry))
		}
	}
	counter := s.sessionVars.TxnCtx.StatementCount
	duration := time.Since(s.GetSessionVars().TxnCtx.CreateTime).Seconds()
	s.recordOnTransactionExecution(err, counter, duration)

	if err != nil {
		logutil.Logger(ctx).Warn("commit failed",
			zap.String("finished txn", s.txn.GoString()),
			zap.Error(err))
		return err
	}
	mapper := s.GetSessionVars().TxnCtx.TableDeltaMap
	if s.statsCollector != nil && mapper != nil {
		for _, item := range mapper {
			if item.TableID > 0 {
				s.statsCollector.Update(item.TableID, item.Delta, item.Count, &item.ColSize)
			}
		}
	}
	return nil
}

func (s *session) CommitTxn(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.CommitTxn", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var commitDetail *execdetails.CommitDetails
	ctx = context.WithValue(ctx, execdetails.CommitDetailCtxKey, &commitDetail)
	err := s.doCommitWithRetry(ctx)
	if commitDetail != nil {
		s.sessionVars.StmtCtx.MergeExecDetails(nil, commitDetail)
	}

	failpoint.Inject("keepHistory", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(err)
		}
	})
	s.sessionVars.TxnCtx.Cleanup()
	return err
}

func (s *session) RollbackTxn(ctx context.Context) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.RollbackTxn", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	if s.txn.Valid() {
		terror.Log(s.txn.Rollback())
	}
	if ctx.Value(inCloseSession{}) == nil {
		s.cleanRetryInfo()
	}
	s.txn.changeToInvalid()
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
}

func (s *session) GetClient() kv.Client {
	return s.client
}

func (s *session) GetMPPClient() kv.MPPClient {
	return s.mppClient
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
var SchemaChangedWithoutRetry uint32

func (s *session) getSQLLabel() string {
	if s.sessionVars.InRestrictedSQL {
		return metrics.LblInternal
	}
	return metrics.LblGeneral
}

func (s *session) isInternal() bool {
	return s.sessionVars.InRestrictedSQL
}

func (s *session) isTxnRetryableError(err error) bool {
	if atomic.LoadUint32(&SchemaChangedWithoutRetry) == 1 {
		return kv.IsTxnRetryableError(err)
	}
	return kv.IsTxnRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

func (s *session) checkTxnAborted(stmt sqlexec.Statement) error {
	var err error
	if atomic.LoadUint32(&s.GetSessionVars().TxnCtx.LockExpire) > 0 {
		err = tikv.ErrLockExpire
	} else {
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
	return err
}

func (s *session) retry(ctx context.Context, maxCnt uint) (err error) {
	var retryCnt uint
	defer func() {
		s.sessionVars.RetryInfo.Retrying = false
		// retryCnt only increments on retryable error, so +1 here.
		metrics.SessionRetry.Observe(float64(retryCnt + 1))
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
		if err != nil {
			s.RollbackTxn(ctx)
		}
		s.txn.changeToInvalid()
	}()

	connID := s.sessionVars.ConnectionID
	s.sessionVars.RetryInfo.Retrying = true
	if atomic.LoadUint32(&s.sessionVars.TxnCtx.ForUpdate) == 1 {
		err = ErrForUpdateCantRetry.GenWithStackByArgs(connID)
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
			s.sessionVars.StmtCtx.ResetForRetry()
			s.sessionVars.PreparedParams = s.sessionVars.PreparedParams[:0]
			schemaVersion, err = st.RebuildPlan(ctx)
			if err != nil {
				return err
			}

			if retryCnt == 0 {
				// We do not have to log the query every time.
				// We print the queries at the first try only.
				sql := sqlForLog(st.GetTextToLog())
				if !sessVars.EnableRedactLog {
					sql += sessVars.PreparedParams.String()
				}
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i),
					zap.String("sql", sql))
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
			s.StmtCommit()
		}
		logutil.Logger(ctx).Warn("transaction association",
			zap.Uint64("retrying txnStartTS", s.GetSessionVars().TxnCtx.StartTS),
			zap.Uint64("original txnStartTS", orgStartTS))
		failpoint.Inject("preCommitHook", func() {
			hook, ok := ctx.Value("__preCommitHook").(func())
			if ok {
				hook()
			}
		})
		if err == nil {
			err = s.doCommit(ctx)
			if err == nil {
				break
			}
		}
		if !s.isTxnRetryableError(err) {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Stringer("session", s),
				zap.Error(err))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblUnretryable).Inc()
			return err
		}
		retryCnt++
		if retryCnt >= maxCnt {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Uint("retry reached max count", retryCnt))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblReachMax).Inc()
			return err
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
func (s *session) ExecRestrictedSQL(sql string) ([]chunk.Row, []*ast.ResultField, error) {
	return s.ExecRestrictedSQLWithContext(context.TODO(), sql)
}

// ExecRestrictedSQLWithContext implements RestrictedSQLExecutor interface.
func (s *session) ExecRestrictedSQLWithContext(ctx context.Context, sql string, opts ...sqlexec.OptionFuncAlias) (
	[]chunk.Row, []*ast.ResultField, error) {
	var execOption sqlexec.ExecOption
	for _, opt := range opts {
		opt(&execOption)
	}
	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, err
	}
	defer s.sysSessionPool().Put(tmp)
	se := tmp.(*session)
	// The special session will share the `InspectionTableCache` with current session
	// if the current session in inspection mode.
	if cache := s.sessionVars.InspectionTableCache; cache != nil {
		se.sessionVars.InspectionTableCache = cache
		defer func() { se.sessionVars.InspectionTableCache = nil }()
	}
	if ok := s.sessionVars.OptimizerUseInvisibleIndexes; ok {
		se.sessionVars.OptimizerUseInvisibleIndexes = true
		defer func() { se.sessionVars.OptimizerUseInvisibleIndexes = false }()
	}
	prePruneMode := se.sessionVars.PartitionPruneMode.Load()
	defer func() {
		if !execOption.IgnoreWarning {
			if se != nil && se.GetSessionVars().StmtCtx.WarningCount() > 0 {
				warnings := se.GetSessionVars().StmtCtx.GetWarnings()
				s.GetSessionVars().StmtCtx.AppendWarnings(warnings)
			}
		}
		se.sessionVars.PartitionPruneMode.Store(prePruneMode)
	}()

	if execOption.SnapshotTS != 0 {
		se.sessionVars.SnapshotInfoschema, err = domain.GetDomain(s).GetSnapshotInfoSchema(execOption.SnapshotTS)
		if err != nil {
			return nil, nil, err
		}
		if err := se.sessionVars.SetSystemVar(variable.TiDBSnapshot, strconv.FormatUint(execOption.SnapshotTS, 10)); err != nil {
			return nil, nil, err
		}
		defer func() {
			if err := se.sessionVars.SetSystemVar(variable.TiDBSnapshot, ""); err != nil {
				logutil.BgLogger().Error("set tidbSnapshot error", zap.Error(err))
			}
			se.sessionVars.SnapshotInfoschema = nil
		}()
	}

	// for analyze stmt we need let worker session follow user session that executing stmt.
	se.sessionVars.PartitionPruneMode.Store(s.sessionVars.PartitionPruneMode.Load())
	metrics.SessionRestrictedSQLCounter.Inc()

	return execRestrictedSQL(ctx, se, sql)
}

// ExecRestrictedSQLWithSnapshot implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements with snapshot.
// If current session sets the snapshot timestamp, then execute with this snapshot timestamp.
// Otherwise, execute with the current transaction start timestamp if the transaction is valid.
func (s *session) ExecRestrictedSQLWithSnapshot(sql string) ([]chunk.Row, []*ast.ResultField, error) {
	var snapshot uint64
	txn, err := s.Txn(false)
	if err != nil {
		return nil, nil, err
	}
	if txn.Valid() {
		snapshot = s.txn.StartTS()
	}
	if s.sessionVars.SnapshotTS != 0 {
		snapshot = s.sessionVars.SnapshotTS
	}
	return s.ExecRestrictedSQLWithContext(context.TODO(), sql, sqlexec.ExecOptionWithSnapshot(snapshot))
}

func execRestrictedSQL(ctx context.Context, se *session, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	ctx = context.WithValue(ctx, execdetails.StmtExecDetailKey, &execdetails.StmtExecDetails{})
	startTime := time.Now()
	recordSets, err := se.Execute(ctx, sql)
	defer func() {
		for _, rs := range recordSets {
			closeErr := rs.Close()
			if closeErr != nil && err == nil {
				err = closeErr
			}
		}
	}()
	if err != nil {
		return nil, nil, err
	}

	var (
		rows   []chunk.Row
		fields []*ast.ResultField
	)
	// Execute all recordset, take out the first one as result.
	for i, rs := range recordSets {
		tmp, err := drainRecordSet(ctx, se, rs)
		if err != nil {
			return nil, nil, err
		}

		if i == 0 {
			rows = tmp
			fields = rs.Fields()
		}
	}
	metrics.QueryDurationHistogram.WithLabelValues(metrics.LblInternal).Observe(time.Since(startTime).Seconds())
	return rows, fields, err
}

func createSessionFunc(store kv.Storage) pools.Factory {
	return func() (pools.Resource, error) {
		se, err := createSession(store)
		if err != nil {
			return nil, err
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.AutoCommit, types.NewStringDatum("1"))
		if err != nil {
			return nil, err
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.MaxExecutionTime, types.NewUintDatum(0))
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.MaxAllowedPacket, types.NewStringDatum("67108864"))
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
		se, err := CreateSessionWithDomain(store, dom)
		if err != nil {
			return nil, err
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.AutoCommit, types.NewStringDatum("1"))
		if err != nil {
			return nil, err
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
	req := rs.NewChunk()
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, se.sessionVars.MaxChunkSize)
	}
}

// getExecRet executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getExecRet(ctx sessionctx.Context, sql string) (string, error) {
	rows, fields, err := s.ExecRestrictedSQL(sql)
	if err != nil {
		return "", err
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
	rows, _, err := s.ExecRestrictedSQL(sql)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]string, len(rows))
	for _, r := range rows {
		k, v := r.GetString(0), r.GetString(1)
		if v, err = s.GetTiKVGlobalSysVar(k, v); err != nil {
			ret[k] = v
		}
	}
	return ret, nil
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (s *session) GetGlobalSysVar(name string) (string, error) {
	if name == variable.TiDBSlowLogMasking {
		name = variable.TiDBRedactLog
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name)
	sysVar, err := s.getExecRet(s, sql)
	if err != nil {
		if executor.ErrResultIsEmpty.Equal(err) {
			sv := variable.GetSysVar(name)
			if sv != nil {
				return sv.Value, nil
			}
			return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
		}
		return "", err
	}
	// Update mysql.tidb values if required
	return s.GetTiKVGlobalSysVar(name, sysVar)
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(name, value string) error {
	if name == variable.TiDBSlowLogMasking {
		name = variable.TiDBRedactLog
	}
	if name == variable.SQLModeVar {
		value = mysql.FormatSQLModeStr(value)
		if _, err := mysql.GetSQLMode(value); err != nil {
			return err
		}
	}
	if name == variable.TiDBPartitionPruneMode && value == string(variable.DynamicOnly) {
		err := s.ensureFullGlobalStats()
		if err != nil {
			return err
		}
	}
	var sVal string
	var err error
	sVal, err = variable.ValidateSetSystemVar(s.sessionVars, name, value, variable.ScopeGlobal)
	if err != nil {
		return err
	}
	name = strings.ToLower(name)
	// update mysql.tidb if required.
	if err = s.SetTiKVGlobalSysVar(name, sVal); err != nil {
		return err
	}
	variable.CheckDeprecationSetSystemVar(s.sessionVars, name)
	sql := fmt.Sprintf(`REPLACE %s.%s VALUES ('%s', '%s');`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name, escapeUserString(sVal))
	_, _, err = s.ExecRestrictedSQL(sql)
	return err
}

// escape user supplied string for internal SQL. Not safe for all cases, since it doesn't
// handle quote-type, sql-mode, character set breakout.
func escapeUserString(str string) string {
	return strings.ReplaceAll(str, `'`, `\'`)
}

// SetTiKVGlobalSysVar handles tikv_* sysvars which need to update mysql.tidb
// for backwards compatibility. Validation has already been performed.
func (s *session) SetTiKVGlobalSysVar(name, val string) error {
	switch name {
	case variable.TiKVGCConcurrency:
		autoConcurrency := "false"
		if val == "-1" {
			autoConcurrency = "true"
		}
		sql := fmt.Sprintf(`INSERT INTO mysql.tidb (variable_name, variable_value, comment) VALUES ('%[1]s', '%[2]s', '%[3]s')
			ON DUPLICATE KEY UPDATE variable_value = '%[2]s'`, tiKVGCAutoConcurrency, autoConcurrency, gcVariableComments[name])
		_, _, err := s.ExecRestrictedSQL(sql)
		if err != nil {
			return err
		}
		fallthrough
	case variable.TiKVGCEnable, variable.TiKVGCRunInterval, variable.TiKVGCLifetime, variable.TiKVGCMode, variable.TiKVGCScanLockMode:
		val = onOffToTrueFalse(val)
		sql := fmt.Sprintf(`INSERT INTO mysql.tidb (variable_name, variable_value, comment) VALUES ('%[1]s', '%[2]s', '%[3]s')
			ON DUPLICATE KEY UPDATE variable_value = '%[2]s'`, name, escapeUserString(val), gcVariableComments[name])
		_, _, err := s.ExecRestrictedSQL(sql)
		return err
	}
	return nil // not a TiKV sysVar
}

// In mysql.tidb the convention has been to store the string value "true"/"false",
// but sysvars use the convention ON/OFF.
func trueFalseToOnOff(str string) string {
	if strings.EqualFold("true", str) {
		return variable.BoolOn
	} else if strings.EqualFold("false", str) {
		return variable.BoolOff
	}
	return str
}

// In mysql.tidb the convention has been to store the string value "true"/"false",
// but sysvars use the convention ON/OFF.
func onOffToTrueFalse(str string) string {
	if strings.EqualFold("ON", str) {
		return "true"
	} else if strings.EqualFold("OFF", str) {
		return "false"
	}
	return str
}

// GetTiKVGlobalSysVar handles tikv_* sysvars which need
// to read from mysql.tidb for backwards compatibility.
func (s *session) GetTiKVGlobalSysVar(name, val string) (string, error) {
	switch name {
	case variable.TiKVGCConcurrency:
		// Check if autoconcurrency is set
		sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='%s';`, tiKVGCAutoConcurrency)
		autoConcurrencyVal, err := s.getExecRet(s, sql)
		if err == nil && strings.EqualFold(autoConcurrencyVal, "true") {
			return "-1", nil // convention for "AUTO"
		}
		fallthrough
	case variable.TiKVGCEnable, variable.TiKVGCRunInterval, variable.TiKVGCLifetime,
		variable.TiKVGCMode, variable.TiKVGCScanLockMode:
		sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='%s';`, name)
		tblValue, err := s.getExecRet(s, sql)
		if err != nil {
			return val, nil // mysql.tidb value does not exist.
		}
		// Run validation on the tblValue. This will return an error if it can't be validated,
		// but will also make it more consistent: disTribuTeD -> DISTRIBUTED etc
		tblValue = trueFalseToOnOff(tblValue)
		validatedVal, err := variable.ValidateSetSystemVar(s.sessionVars, name, tblValue, variable.ScopeGlobal)
		if err != nil {
			logutil.Logger(context.Background()).Warn("restoring sysvar value since validating mysql.tidb value failed",
				zap.Error(err),
				zap.String("name", name),
				zap.String("tblValue", tblValue),
				zap.String("restoredValue", val))
			sql := fmt.Sprintf(`REPLACE INTO mysql.tidb (variable_name, variable_value, comment)
			VALUES ('%s', '%s', '%s')`, name, escapeUserString(val), gcVariableComments[name])
			_, _, err = s.ExecRestrictedSQL(sql)
			return val, err
		}
		if validatedVal != val {
			// The sysvar value is out of sync.
			sql := fmt.Sprintf(`REPLACE %s.%s VALUES ('%s', '%s');`,
				mysql.SystemDB, mysql.GlobalVariablesTable, name, escapeUserString(validatedVal))
			_, _, err = s.ExecRestrictedSQL(sql)
			return validatedVal, err
		}
		return validatedVal, nil
	}
	return val, nil // not a TiKV sysVar
}

func (s *session) ensureFullGlobalStats() error {
	rows, _, err := s.ExecRestrictedSQL(`select count(1) from information_schema.tables t where t.create_options = 'partitioned'
		and not exists (select 1 from mysql.stats_meta m where m.table_id = t.tidb_table_id)`)
	if err != nil {
		return err
	}
	row := rows[0]
	count := row.GetInt64(0)
	if count > 0 {
		return errors.New("need analyze all partition table in 'static-collect-dynamic' mode before switch to 'dynamic-only'")
	}
	return nil
}

func (s *session) ParseSQL(ctx context.Context, sql, charset, collation string) ([]ast.StmtNode, []error, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.ParseSQL", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	defer trace.StartRegion(ctx, "ParseSQL").End()
	s.parser.SetSQLMode(s.sessionVars.SQLMode)
	s.parser.SetParserConfig(s.sessionVars.BuildParserConfig())
	return s.parser.Parse(sql, charset, collation)
}

func (s *session) SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64) {
	// If command == mysql.ComSleep, it means the SQL execution is finished. The processinfo is reset to SLEEP.
	// If the SQL finished and the session is not in transaction, the current start timestamp need to reset to 0.
	// Otherwise, it should be set to the transaction start timestamp.
	// Why not reset the transaction start timestamp to 0 when transaction committed?
	// Because the select statement and other statements need this timestamp to read data,
	// after the transaction is committed. e.g. SHOW MASTER STATUS;
	var curTxnStartTS uint64
	if command != mysql.ComSleep || s.GetSessionVars().InTxn() {
		curTxnStartTS = s.sessionVars.TxnCtx.StartTS
	}
	p := s.currentPlan
	if explain, ok := p.(*plannercore.Explain); ok && explain.Analyze && explain.TargetPlan != nil {
		p = explain.TargetPlan
	}
	pi := util.ProcessInfo{
		ID:               s.sessionVars.ConnectionID,
		DB:               s.sessionVars.CurrentDB,
		Command:          command,
		Plan:             p,
		PlanExplainRows:  plannercore.GetExplainRowsForPlan(p),
		RuntimeStatsColl: s.sessionVars.StmtCtx.RuntimeStatsColl,
		Time:             t,
		State:            s.Status(),
		Info:             sql,
		CurTxnStartTS:    curTxnStartTS,
		StmtCtx:          s.sessionVars.StmtCtx,
		StatsInfo:        plannercore.GetStatsInfo,
		MaxExecutionTime: maxExecutionTime,
		RedactSQL:        s.sessionVars.EnableRedactLog,
	}
	oldPi := s.ShowProcess()
	if p == nil {
		// Store the last valid plan when the current plan is nil.
		// This is for `explain for connection` statement has the ability to query the last valid plan.
		if oldPi != nil && oldPi.Plan != nil && len(oldPi.PlanExplainRows) > 0 {
			pi.Plan = oldPi.Plan
			pi.PlanExplainRows = oldPi.PlanExplainRows
			pi.RuntimeStatsColl = oldPi.RuntimeStatsColl
		}
	}
	// We set process info before building plan, so we extended execution time.
	if oldPi != nil && oldPi.Info == pi.Info {
		pi.Time = oldPi.Time
	}
	_, pi.Digest = s.sessionVars.StmtCtx.SQLDigest()
	// DO NOT reset the currentPlan to nil until this query finishes execution, otherwise reentrant calls
	// of SetProcessInfo would override Plan and PlanExplainRows to nil.
	if command == mysql.ComSleep {
		s.currentPlan = nil
	}
	if s.sessionVars.User != nil {
		pi.User = s.sessionVars.User.Username
		pi.Host = s.sessionVars.User.Hostname
	}
	s.processInfo.Store(&pi)
}

func (s *session) ExecuteInternal(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	origin := s.sessionVars.InRestrictedSQL
	s.sessionVars.InRestrictedSQL = true
	defer func() {
		s.sessionVars.InRestrictedSQL = origin
	}()
	return s.Execute(ctx, sql)
}

func (s *session) Execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.Execute", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
		logutil.Eventf(ctx, "execute: %s", sql)
	}

	stmtNodes, err := s.Parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(stmtNodes) != 1 {
		return nil, errors.New("Execute() API doesn't support multiple statements any more")
	}

	rs, err := s.ExecuteStmt(ctx, stmtNodes[0])
	if err != nil {
		s.sessionVars.StmtCtx.AppendError(err)
	}
	if rs == nil {
		return nil, err
	}
	return []sqlexec.RecordSet{rs}, err
}

// Parse parses a query string to raw ast.StmtNode.
func (s *session) Parse(ctx context.Context, sql string) ([]ast.StmtNode, error) {
	charsetInfo, collation := s.sessionVars.GetCharsetInfo()
	parseStartTime := time.Now()
	stmts, warns, err := s.ParseSQL(ctx, sql, charsetInfo, collation)
	if err != nil {
		s.rollbackOnError(ctx)

		// Only print log message when this SQL is from the user.
		// Mute the warning for internal SQLs.
		if !s.sessionVars.InRestrictedSQL {
			if s.sessionVars.EnableRedactLog {
				logutil.Logger(ctx).Debug("parse SQL failed", zap.Error(err), zap.String("SQL", sql))
			} else {
				logutil.Logger(ctx).Warn("parse SQL failed", zap.Error(err), zap.String("SQL", sql))
			}
		}
		return nil, util.SyntaxError(err)
	}

	durParse := time.Since(parseStartTime)
	s.GetSessionVars().DurationParse = durParse
	isInternal := s.isInternal()
	if isInternal {
		sessionExecuteParseDurationInternal.Observe(durParse.Seconds())
	} else {
		sessionExecuteParseDurationGeneral.Observe(durParse.Seconds())
	}
	for _, warn := range warns {
		s.sessionVars.StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	return stmts, nil
}

func (s *session) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.ExecuteStmt", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	s.PrepareTxnCtx(ctx)
	err := s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return nil, err
	}

	s.sessionVars.StartTime = time.Now()

	// Some executions are done in compile stage, so we reset them before compile.
	if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
		return nil, err
	}

	// Uncorrelated subqueries will execute once when building plan, so we reset process info before building plan.
	cmd32 := atomic.LoadUint32(&s.GetSessionVars().CommandValue)
	s.SetProcessInfo(stmtNode.Text(), time.Now(), byte(cmd32), 0)

	// Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
	compiler := executor.Compiler{Ctx: s}
	stmt, err := compiler.Compile(ctx, stmtNode)
	if err != nil {
		s.rollbackOnError(ctx)

		// Only print log message when this SQL is from the user.
		// Mute the warning for internal SQLs.
		if !s.sessionVars.InRestrictedSQL {
			logutil.Logger(ctx).Warn("compile SQL failed", zap.Error(err), zap.String("SQL", stmtNode.Text()))
		}
		return nil, err
	}
	durCompile := time.Since(s.sessionVars.StartTime)
	s.GetSessionVars().DurationCompile = durCompile
	if s.isInternal() {
		sessionExecuteCompileDurationInternal.Observe(durCompile.Seconds())
	} else {
		sessionExecuteCompileDurationGeneral.Observe(durCompile.Seconds())
	}
	s.currentPlan = stmt.Plan

	// Execute the physical plan.
	logStmt(stmt, s.sessionVars)
	recordSet, err := runStmt(ctx, s, stmt)
	if err != nil {
		if !kv.ErrKeyExists.Equal(err) {
			logutil.Logger(ctx).Warn("run statement failed",
				zap.Int64("schemaVersion", s.sessionVars.TxnCtx.SchemaVersion),
				zap.Error(err),
				zap.String("session", s.String()))
		}
		return nil, err
	}
	return recordSet, nil
}

// querySpecialKeys contains the keys of special query, the special query will handled by handleQuerySpecial method.
var querySpecialKeys = []fmt.Stringer{
	executor.LoadDataVarKey,
	executor.LoadStatsVarKey,
	executor.IndexAdviseVarKey,
}

func (s *session) hasQuerySpecial() bool {
	found := false
	s.mu.RLock()
	for _, k := range querySpecialKeys {
		v := s.mu.values[k]
		if v != nil {
			found = true
			break
		}
	}
	s.mu.RUnlock()
	return found
}

// runStmt executes the sqlexec.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, se *session, s sqlexec.Statement) (rs sqlexec.RecordSet, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.runStmt", opentracing.ChildOf(span.Context()))
		span1.LogKV("sql", s.OriginText())
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	se.SetValue(sessionctx.QueryString, s.OriginText())
	if _, ok := s.(*executor.ExecStmt).StmtNode.(ast.DDLNode); ok {
		se.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		se.ClearValue(sessionctx.LastExecuteDDL)
	}

	sessVars := se.sessionVars

	// Record diagnostic information for DML statements
	if _, ok := s.(*executor.ExecStmt).StmtNode.(ast.DMLNode); ok {
		defer func() {
			sessVars.LastQueryInfo = variable.QueryInfo{
				TxnScope:    sessVars.TxnScope,
				StartTS:     sessVars.TxnCtx.StartTS,
				ForUpdateTS: sessVars.TxnCtx.GetForUpdateTS(),
			}
			if err != nil {
				sessVars.LastQueryInfo.ErrMsg = err.Error()
			}
		}()
	}

	// Save origTxnCtx here to avoid it reset in the transaction retry.
	origTxnCtx := sessVars.TxnCtx
	err = se.checkTxnAborted(s)
	if err != nil {
		return nil, err
	}
	rs, err = s.Exec(ctx)
	sessVars.TxnCtx.StatementCount++
	if rs != nil {
		return &execStmtResult{
			RecordSet: rs,
			sql:       s,
			se:        se,
		}, err
	}

	err = finishStmt(ctx, se, err, s)
	if se.hasQuerySpecial() {
		// The special query will be handled later in handleQuerySpecial,
		// then should call the ExecStmt.FinishExecuteStmt to finish this statement.
		se.SetValue(ExecStmtVarKey, s.(*executor.ExecStmt))
	} else {
		// If it is not a select statement or special query, we record its slow log here,
		// then it could include the transaction commit time.
		s.(*executor.ExecStmt).FinishExecuteStmt(origTxnCtx.StartTS, err == nil, false)
	}
	return nil, err
}

// ExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type ExecStmtVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k ExecStmtVarKeyType) String() string {
	return "exec_stmt_var_key"
}

// ExecStmtVarKey is a variable key for ExecStmt.
const ExecStmtVarKey ExecStmtVarKeyType = 0

// execStmtResult is the return value of ExecuteStmt and it implements the sqlexec.RecordSet interface.
// Why we need a struct to wrap a RecordSet and provide another RecordSet?
// This is because there are so many session state related things that definitely not belongs to the original
// RecordSet, so this struct exists and RecordSet.Close() is overrided handle that.
type execStmtResult struct {
	sqlexec.RecordSet
	se  *session
	sql sqlexec.Statement
}

func (rs *execStmtResult) Close() error {
	se := rs.se
	err := rs.RecordSet.Close()
	return finishStmt(context.Background(), se, err, rs.sql)
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *session) rollbackOnError(ctx context.Context) {
	if !s.sessionVars.InTxn() {
		s.RollbackTxn(ctx)
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
		return
	}

	ctx := context.Background()
	inTxn := s.GetSessionVars().InTxn()
	// NewPrepareExec may need startTS to build the executor, for example prepare statement has subquery in int.
	// So we have to call PrepareTxnCtx here.
	s.PrepareTxnCtx(ctx)
	s.PrepareTSFuture(ctx)
	prepareExec := executor.NewPrepareExec(s, infoschema.GetInfoSchema(s), sql)
	err = prepareExec.Next(ctx, nil)
	if err != nil {
		return
	}
	if !inTxn {
		// We could start a transaction to build the prepare executor before, we should rollback it here.
		s.RollbackTxn(ctx)
	}
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.Fields, nil
}

func (s *session) preparedStmtExec(ctx context.Context,
	stmtID uint32, prepareStmt *plannercore.CachedPrepareStmt, args []types.Datum) (sqlexec.RecordSet, error) {
	st, err := executor.CompileExecutePreparedStmt(ctx, s, stmtID, args)
	if err != nil {
		return nil, err
	}
	sessionExecuteCompileDurationGeneral.Observe(time.Since(s.sessionVars.StartTime).Seconds())
	logQuery(st.OriginText(), s.sessionVars)
	return runStmt(ctx, s, st)
}

// cachedPlanExec short path currently ONLY for cached "point select plan" execution
func (s *session) cachedPlanExec(ctx context.Context,
	stmtID uint32, prepareStmt *plannercore.CachedPrepareStmt, args []types.Datum) (sqlexec.RecordSet, error) {
	prepared := prepareStmt.PreparedAst
	// compile ExecStmt
	is := infoschema.GetInfoSchema(s)
	execAst := &ast.ExecuteStmt{ExecID: stmtID}
	if err := executor.ResetContextOfStmt(s, execAst); err != nil {
		return nil, err
	}
	execAst.BinaryArgs = args
	execPlan, err := planner.OptimizeExecStmt(ctx, s, execAst, is)
	if err != nil {
		return nil, err
	}

	stmtCtx := s.GetSessionVars().StmtCtx
	stmt := &executor.ExecStmt{
		GoCtx:       ctx,
		InfoSchema:  is,
		Plan:        execPlan,
		StmtNode:    execAst,
		Ctx:         s,
		OutputNames: execPlan.OutputNames(),
		PsStmt:      prepareStmt,
	}
	compileDuration := time.Since(s.sessionVars.StartTime)
	sessionExecuteCompileDurationGeneral.Observe(compileDuration.Seconds())
	s.GetSessionVars().DurationCompile = compileDuration

	stmt.Text = prepared.Stmt.Text()
	stmtCtx.OriginalSQL = stmt.Text
	stmtCtx.InitSQLDigest(prepareStmt.NormalizedSQL, prepareStmt.SQLDigest)
	stmtCtx.SetPlanDigest(prepareStmt.NormalizedPlan, prepareStmt.PlanDigest)
	logQuery(stmt.GetTextToLog(), s.sessionVars)

	// run ExecStmt
	var resultSet sqlexec.RecordSet
	switch prepared.CachedPlan.(type) {
	case *plannercore.PointGetPlan:
		resultSet, err = stmt.PointGet(ctx, is)
		s.txn.changeToInvalid()
	case *plannercore.Update:
		s.PrepareTSFuture(ctx)
		stmtCtx.Priority = kv.PriorityHigh
		resultSet, err = runStmt(ctx, s, stmt)
	default:
		prepared.CachedPlan = nil
		return nil, errors.Errorf("invalid cached plan type")
	}
	return resultSet, err
}

// IsCachedExecOk check if we can execute using plan cached in prepared structure
// Be careful for the short path, current precondition is ths cached plan satisfying
// IsPointGetWithPKOrUniqueKeyByAutoCommit
func (s *session) IsCachedExecOk(ctx context.Context, preparedStmt *plannercore.CachedPrepareStmt) (bool, error) {
	prepared := preparedStmt.PreparedAst
	if prepared.CachedPlan == nil {
		return false, nil
	}
	// check auto commit
	if !s.GetSessionVars().IsAutocommit() {
		return false, nil
	}
	// check schema version
	is := infoschema.GetInfoSchema(s)
	if prepared.SchemaVersion != is.SchemaMetaVersion() {
		prepared.CachedPlan = nil
		return false, nil
	}
	// maybe we'd better check cached plan type here, current
	// only point select/update will be cached, see "getPhysicalPlan" func
	var ok bool
	var err error
	switch prepared.CachedPlan.(type) {
	case *plannercore.PointGetPlan:
		ok = true
	case *plannercore.Update:
		pointUpdate := prepared.CachedPlan.(*plannercore.Update)
		_, ok = pointUpdate.SelectPlan.(*plannercore.PointGetPlan)
		if !ok {
			err = errors.Errorf("cached update plan not point update")
			prepared.CachedPlan = nil
			return false, err
		}
	default:
		ok = false
	}
	return ok, err
}

// ExecutePreparedStmt executes a prepared statement.
func (s *session) ExecutePreparedStmt(ctx context.Context, stmtID uint32, args []types.Datum) (sqlexec.RecordSet, error) {
	s.PrepareTxnCtx(ctx)
	var err error
	s.sessionVars.StartTime = time.Now()
	preparedPointer, ok := s.sessionVars.PreparedStmts[stmtID]
	if !ok {
		err = plannercore.ErrStmtNotFound
		logutil.Logger(ctx).Error("prepared statement not found", zap.Uint32("stmtID", stmtID))
		return nil, err
	}
	preparedStmt, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
	if !ok {
		return nil, errors.Errorf("invalid CachedPrepareStmt type")
	}
	executor.CountStmtNode(preparedStmt.PreparedAst.Stmt, s.sessionVars.InRestrictedSQL)
	ok, err = s.IsCachedExecOk(ctx, preparedStmt)
	if err != nil {
		return nil, err
	}
	if ok {
		return s.cachedPlanExec(ctx, stmtID, preparedStmt, args)
	}
	return s.preparedStmtExec(ctx, stmtID, preparedStmt, args)
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
	if !active {
		return &s.txn, nil
	}
	if !s.txn.validOrPending() {
		return &s.txn, errors.AddStack(kv.ErrInvalidTxn)
	}
	if s.txn.pending() {
		defer func(begin time.Time) {
			s.sessionVars.DurationWaitTS = time.Since(begin)
		}(time.Now())
		// Transaction is lazy initialized.
		// PrepareTxnCtx is called to get a tso future, makes s.txn a pending txn,
		// If Txn() is called later, wait for the future to get a valid txn.
		if err := s.txn.changePendingToValid(s.currentCtx); err != nil {
			logutil.BgLogger().Error("active transaction fail",
				zap.Error(err))
			s.txn.cleanup()
			s.sessionVars.TxnCtx.StartTS = 0
			return &s.txn, err
		}
		s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
		if s.sessionVars.TxnCtx.IsPessimistic {
			s.txn.SetOption(kv.Pessimistic, true)
		}
		if !s.sessionVars.IsAutocommit() {
			s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
		s.sessionVars.TxnCtx.CouldRetry = s.isTxnRetryable()
		s.txn.SetVars(s.sessionVars.KVVars)
		if s.sessionVars.GetReplicaRead().IsFollowerRead() {
			s.txn.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
		}
	}
	return &s.txn, nil
}

// isTxnRetryable (if returns true) means the transaction could retry.
// If the transaction is in pessimistic mode, do not retry.
// If the session is already in transaction, enable retry or internal SQL could retry.
// If not, the transaction could always retry, because it should be auto committed transaction.
// Anyway the retry limit is 0, the transaction could not retry.
func (s *session) isTxnRetryable() bool {
	sessVars := s.sessionVars

	// The pessimistic transaction no need to retry.
	if sessVars.TxnCtx.IsPessimistic {
		return false
	}

	// If retry limit is 0, the transaction could not retry.
	if sessVars.RetryLimit == 0 {
		return false
	}

	// If the session is not InTxn, it is an auto-committed transaction.
	// The auto-committed transaction could always retry.
	if !sessVars.InTxn() {
		return true
	}

	// The internal transaction could always retry.
	if sessVars.InRestrictedSQL {
		return true
	}

	// If the retry is enabled, the transaction could retry.
	if !sessVars.DisableTxnAutoRetry {
		return true
	}

	return false
}

func (s *session) NewTxn(ctx context.Context) error {
	if s.txn.Valid() {
		txnID := s.txn.StartTS()
		txnScope := s.GetSessionVars().TxnCtx.TxnScope
		err := s.CommitTxn(ctx)
		if err != nil {
			return err
		}
		vars := s.GetSessionVars()
		logutil.Logger(ctx).Info("NewTxn() inside a transaction auto commit",
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", txnID),
			zap.String("txnScope", txnScope))
	}

	txn, err := s.store.BeginWithTxnScope(s.sessionVars.CheckAndGetTxnScope())
	if err != nil {
		return err
	}
	txn.SetVars(s.sessionVars.KVVars)
	if s.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		txn.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	s.txn.changeInvalidToValid(txn)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		StartTS:       txn.StartTS(),
		ShardStep:     int(s.sessionVars.ShardAllocateStep),
		IsStaleness:   false,
		TxnScope:      s.sessionVars.CheckAndGetTxnScope(),
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

type inCloseSession struct{}

// Close function does some clean work when session end.
// Close should release the table locks which hold by the session.
func (s *session) Close() {
	// TODO: do clean table locks when session exited without execute Close.
	// TODO: do clean table locks when tidb-server was `kill -9`.
	if s.HasLockedTables() && config.TableLockEnabled() {
		if ds := config.TableLockDelayClean(); ds > 0 {
			time.Sleep(time.Duration(ds) * time.Millisecond)
		}
		lockedTables := s.GetAllTableLocks()
		err := domain.GetDomain(s).DDL().UnlockTables(s, lockedTables)
		if err != nil {
			logutil.BgLogger().Error("release table lock failed", zap.Uint64("conn", s.sessionVars.ConnectionID))
		}
	}
	if s.statsCollector != nil {
		s.statsCollector.Delete()
	}
	if s.idxUsageCollector != nil {
		s.idxUsageCollector.Delete()
	}
	bindValue := s.Value(bindinfo.SessionBindInfoKeyType)
	if bindValue != nil {
		bindValue.(*bindinfo.SessionHandle).Close()
	}
	ctx := context.WithValue(context.TODO(), inCloseSession{}, struct{}{})
	s.RollbackTxn(ctx)
	if s.sessionVars != nil {
		s.sessionVars.WithdrawAllPreparedStmt()
	}
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s *session) Auth(user *auth.UserIdentity, authentication []byte, salt []byte) bool {
	pm := privilege.GetPrivilegeManager(s)

	// Check IP or localhost.
	var success bool
	user.AuthUsername, user.AuthHostname, success = pm.ConnectionVerification(user.Username, user.Hostname, authentication, salt, s.sessionVars.TLSConnectionState)
	if success {
		s.sessionVars.User = user
		s.sessionVars.ActiveRoles = pm.GetDefaultRoles(user.AuthUsername, user.AuthHostname)
		return true
	} else if user.Hostname == variable.DefHostname {
		return false
	}

	// Check Hostname.
	for _, addr := range getHostByIP(user.Hostname) {
		u, h, success := pm.ConnectionVerification(user.Username, addr, authentication, salt, s.sessionVars.TLSConnectionState)
		if success {
			s.sessionVars.User = &auth.UserIdentity{
				Username:     user.Username,
				Hostname:     addr,
				AuthUsername: u,
				AuthHostname: h,
			}
			s.sessionVars.ActiveRoles = pm.GetDefaultRoles(u, h)
			return true
		}
	}
	return false
}

// AuthWithoutVerification is required by the ResetConnection RPC
func (s *session) AuthWithoutVerification(user *auth.UserIdentity) bool {
	pm := privilege.GetPrivilegeManager(s)

	// Check IP or localhost.
	var success bool
	user.AuthUsername, user.AuthHostname, success = pm.GetAuthWithoutVerification(user.Username, user.Hostname)
	if success {
		s.sessionVars.User = user
		s.sessionVars.ActiveRoles = pm.GetDefaultRoles(user.AuthUsername, user.AuthHostname)
		return true
	} else if user.Hostname == variable.DefHostname {
		return false
	}

	// Check Hostname.
	for _, addr := range getHostByIP(user.Hostname) {
		u, h, success := pm.GetAuthWithoutVerification(user.Username, addr)
		if success {
			s.sessionVars.User = &auth.UserIdentity{
				Username:     user.Username,
				Hostname:     addr,
				AuthUsername: u,
				AuthHostname: h,
			}
			s.sessionVars.ActiveRoles = pm.GetDefaultRoles(u, h)
			return true
		}
	}
	return false
}

func getHostByIP(ip string) []string {
	if ip == "127.0.0.1" {
		return []string{variable.DefHostname}
	}
	addrs, err := net.LookupAddr(ip)
	if err != nil {
		// The error is ignorable.
		// The empty line here makes the golint tool (which complains err is not checked) happy.
	}
	return addrs
}

// RefreshVars implements the sessionctx.Context interface.
func (s *session) RefreshVars(ctx context.Context) error {
	pruneMode, err := s.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBPartitionPruneMode)
	if err != nil {
		return err
	}
	s.sessionVars.PartitionPruneMode.Store(pruneMode)
	return nil
}

// CreateSession4Test creates a new session environment for test.
func CreateSession4Test(store kv.Storage) (Session, error) {
	return CreateSession4TestWithOpt(store, nil)
}

// Opt describes the option for creating session
type Opt struct {
	PreparedPlanCache *kvcache.SimpleLRUCache
}

// CreateSession4TestWithOpt creates a new session environment for test.
func CreateSession4TestWithOpt(store kv.Storage, opt *Opt) (Session, error) {
	s, err := CreateSessionWithOpt(store, opt)
	if err == nil {
		// initialize session variables for test.
		s.GetSessionVars().InitChunkSize = 2
		s.GetSessionVars().MaxChunkSize = 32
	}
	return s, err
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (Session, error) {
	return CreateSessionWithOpt(store, nil)
}

// CreateSessionWithOpt creates a new session environment with option.
// Use default option if opt is nil.
func CreateSessionWithOpt(store kv.Storage, opt *Opt) (Session, error) {
	s, err := createSessionWithOpt(store, opt)
	if err != nil {
		return nil, err
	}

	// Add auth here.
	do, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	pm := &privileges.UserPrivileges{
		Handle: do.PrivilegeHandle(),
	}
	privilege.BindPrivilegeManager(s, pm)

	sessionBindHandle := bindinfo.NewSessionBindHandle(s.parser)
	s.SetValue(bindinfo.SessionBindInfoKeyType, sessionBindHandle)
	// Add stats collector, and it will be freed by background stats worker
	// which periodically updates stats using the collected data.
	if do.StatsHandle() != nil && do.StatsUpdating() {
		s.statsCollector = do.StatsHandle().NewSessionStatsCollector()
		s.idxUsageCollector = do.StatsHandle().NewSessionIndexUsageCollector()
	}

	return s, nil
}

// loadSystemTZ loads systemTZ from mysql.tidb
func loadSystemTZ(se *session) (string, error) {
	return loadParameter(se, "system_tz")
}

// loadCollationParameter loads collation parameter from mysql.tidb
func loadCollationParameter(se *session) (bool, error) {
	para, err := loadParameter(se, tidbNewCollationEnabled)
	if err != nil {
		return false, err
	}
	if para == varTrue {
		return true, nil
	} else if para == varFalse {
		return false, nil
	}
	logutil.BgLogger().Warn(
		"Unexpected value of 'new_collation_enabled' in 'mysql.tidb', use 'False' instead",
		zap.String("value", para))
	return false, nil
}

// loadDefMemQuotaQuery loads the default value of mem-quota-query.
// We'll read a tuple if the cluster is upgraded from v3.0.x to v4.0.9+.
// An empty result will be returned if it's a newly deployed cluster whose
// version is v4.0.9.
// See the comment upon the function `upgradeToVer54` for details.
func loadDefMemQuotaQuery(se *session) (int64, error) {
	_, err := loadParameter(se, tidbDefMemoryQuotaQuery)
	if err != nil {
		if err == errResultIsEmpty {
			return 1 << 30, nil
		}
		return 1 << 30, err
	}
	// If there is a tuple in mysql.tidb, the value must be 32 << 30.
	return 32 << 30, nil
}

func loadDefOOMAction(se *session) (string, error) {
	defOOMAction, err := loadParameter(se, tidbDefOOMAction)
	if err != nil {
		if err == errResultIsEmpty {
			return config.GetGlobalConfig().OOMAction, nil
		}
		return config.GetGlobalConfig().OOMAction, err
	}
	if defOOMAction != config.OOMActionLog {
		logutil.BgLogger().Warn("Unexpected value of 'default_oom_action' in 'mysql.tidb', use 'log' instead",
			zap.String("value", defOOMAction))
	}
	return defOOMAction, nil
}

var (
	errResultIsEmpty = dbterror.ClassExecutor.NewStd(errno.ErrResultIsEmpty)
)

// loadParameter loads read-only parameter from mysql.tidb
func loadParameter(se *session, name string) (string, error) {
	sql := "select variable_value from mysql.tidb where variable_name = '" + name + "'"
	rss, errLoad := se.Execute(context.Background(), sql)
	if errLoad != nil {
		return "", errLoad
	}
	// the record of mysql.tidb under where condition: variable_name = $name should shall only be one.
	defer func() {
		if err := rss[0].Close(); err != nil {
			logutil.BgLogger().Error("close result set error", zap.Error(err))
		}
	}()
	req := rss[0].NewChunk()
	if err := rss[0].Next(context.Background(), req); err != nil {
		return "", err
	}
	if req.NumRows() == 0 {
		return "", errResultIsEmpty
	}
	return req.GetRow(0).GetString(0), nil
}

// BootstrapSession runs the first time when the TiDB server start.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	cfg := config.GetGlobalConfig()
	if len(cfg.Plugin.Load) > 0 {
		err := plugin.Load(context.Background(), plugin.Config{
			Plugins:        strings.Split(cfg.Plugin.Load, ","),
			PluginDir:      cfg.Plugin.Dir,
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
		return nil, err
	}
	// get system tz from mysql.tidb
	tz, err := loadSystemTZ(se)
	if err != nil {
		return nil, err
	}
	timeutil.SetSystemTZ(tz)

	// get the flag from `mysql`.`tidb` which indicating if new collations are enabled.
	newCollationEnabled, err := loadCollationParameter(se)
	if err != nil {
		return nil, err
	}

	if newCollationEnabled {
		collate.EnableNewCollations()
	}

	newMemoryQuotaQuery, err := loadDefMemQuotaQuery(se)
	if err != nil {
		return nil, err
	}
	if !config.IsMemoryQuotaQuerySetByUser {
		newCfg := *(config.GetGlobalConfig())
		newCfg.MemQuotaQuery = newMemoryQuotaQuery
		config.StoreGlobalConfig(&newCfg)
		variable.SetSysVar(variable.TIDBMemQuotaQuery, strconv.FormatInt(newCfg.MemQuotaQuery, 10))
	}
	newOOMAction, err := loadDefOOMAction(se)
	if err != nil {
		return nil, err
	}
	if !config.IsOOMActionSetByUser {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.OOMAction = newOOMAction
		})
	}

	dom := domain.GetDomain(se)
	dom.InitExpensiveQueryHandle()

	se2, err := createSession(store)
	if err != nil {
		return nil, err
	}
	se3, err := createSession(store)
	if err != nil {
		return nil, err
	}
	// We should make the load bind-info loop before other loops which has internal SQL.
	// Because the internal SQL may access the global bind-info handler. As the result, the data race occurs here as the
	// LoadBindInfoLoop inits global bind-info handler.
	err = dom.LoadBindInfoLoop(se2, se3)
	if err != nil {
		return nil, err
	}

	if !config.GetGlobalConfig().Security.SkipGrantTable {
		err = dom.LoadPrivilegeLoop(se)
		if err != nil {
			return nil, err
		}
	}

	if len(cfg.Plugin.Load) > 0 {
		err := plugin.Init(context.Background(), plugin.Config{EtcdClient: dom.GetEtcdClient()})
		if err != nil {
			return nil, err
		}
	}

	se4, err := createSession(store)
	if err != nil {
		return nil, err
	}
	err = executor.LoadExprPushdownBlacklist(se4)
	if err != nil {
		return nil, err
	}

	err = executor.LoadOptRuleBlacklist(se4)
	if err != nil {
		return nil, err
	}

	dom.TelemetryLoop(se4)

	se5, err := createSession(store)
	if err != nil {
		return nil, err
	}
	err = dom.UpdateTableStatsLoop(se5)
	if err != nil {
		return nil, err
	}
	if raw, ok := store.(tikv.EtcdBackend); ok {
		err = raw.StartGCWorker()
		if err != nil {
			return nil, err
		}
	}

	return dom, err
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domap.Get(store)
}

// runInBootstrapSession create a special session for bootstrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstrap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, bootstrap func(Session)) {
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("createSession error", zap.Error(err))
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
	return createSessionWithOpt(store, nil)
}

func createSessionWithOpt(store kv.Storage, opt *Opt) (*session, error) {
	dom, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	s := &session{
		store:           store,
		parser:          parser.New(),
		sessionVars:     variable.NewSessionVars(),
		ddlOwnerChecker: dom.DDL().OwnerManager(),
		client:          store.GetClient(),
		mppClient:       store.GetMPPClient(),
	}
	if plannercore.PreparedPlanCacheEnabled() {
		if opt != nil && opt.PreparedPlanCache != nil {
			s.preparedPlanCache = opt.PreparedPlanCache
		} else {
			s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity,
				plannercore.PreparedPlanCacheMemoryGuardRatio, plannercore.PreparedPlanCacheMaxMemory.Load())
		}
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.sessionVars.BinlogClient = binloginfo.GetPumpsClient()
	s.txn.init()

	sessionBindHandle := bindinfo.NewSessionBindHandle(s.parser)
	s.SetValue(bindinfo.SessionBindInfoKeyType, sessionBindHandle)
	return s, nil
}

// CreateSessionWithDomain creates a new Session and binds it with a Domain.
// We need this because when we start DDL in Domain, the DDL need a session
// to change some system tables. But at that time, we have been already in
// a lock context, which cause we can't call createSesion directly.
func CreateSessionWithDomain(store kv.Storage, dom *domain.Domain) (*session, error) {
	s := &session{
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
		client:      store.GetClient(),
		mppClient:   store.GetMPPClient(),
	}
	if plannercore.PreparedPlanCacheEnabled() {
		s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity,
			plannercore.PreparedPlanCacheMemoryGuardRatio, plannercore.PreparedPlanCacheMaxMemory.Load())
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.txn.init()
	return s, nil
}

const (
	notBootstrapped = 0
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
		return err
	})

	if err != nil {
		logutil.BgLogger().Fatal("check bootstrapped failed",
			zap.Error(err))
	}

	if ver > notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		storeBootstrapped[store.UUID()] = true
	}

	return ver
}

func finishBootstrap(store kv.Storage) {
	setStoreBootstrapped(store.UUID())

	err := kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}

const quoteCommaQuote = "', '"

var builtinGlobalVariable = []string{
	variable.AutoCommit,
	variable.SQLModeVar,
	variable.MaxAllowedPacket,
	variable.TimeZone,
	variable.BlockEncryptionMode,
	variable.WaitTimeout,
	variable.InteractiveTimeout,
	variable.MaxPreparedStmtCount,
	variable.InitConnect,
	variable.TxnIsolation,
	variable.TxReadOnly,
	variable.TransactionIsolation,
	variable.TransactionReadOnly,
	variable.NetBufferLength,
	variable.QueryCacheType,
	variable.QueryCacheSize,
	variable.CharacterSetServer,
	variable.AutoIncrementIncrement,
	variable.AutoIncrementOffset,
	variable.CollationServer,
	variable.NetWriteTimeout,
	variable.MaxExecutionTime,
	variable.InnodbLockWaitTimeout,
	variable.WindowingUseHighPrecision,
	variable.SQLSelectLimit,
	variable.DefaultWeekFormat,

	/* TiDB specific global variables: */
	variable.TiDBSkipASCIICheck,
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
	variable.TiDBWindowConcurrency,
	variable.TiDBMergeJoinConcurrency,
	variable.TiDBStreamAggConcurrency,
	variable.TiDBExecutorConcurrency,
	variable.TiDBBackoffLockFast,
	variable.TiDBBackOffWeight,
	variable.TiDBConstraintCheckInPlace,
	variable.TiDBDDLReorgWorkerCount,
	variable.TiDBDDLReorgBatchSize,
	variable.TiDBDDLErrorCountLimit,
	variable.TiDBOptInSubqToJoinAndAgg,
	variable.TiDBOptPreferRangeScan,
	variable.TiDBOptCorrelationThreshold,
	variable.TiDBOptCorrelationExpFactor,
	variable.TiDBOptCPUFactor,
	variable.TiDBOptCopCPUFactor,
	variable.TiDBOptNetworkFactor,
	variable.TiDBOptScanFactor,
	variable.TiDBOptDescScanFactor,
	variable.TiDBOptMemoryFactor,
	variable.TiDBOptDiskFactor,
	variable.TiDBOptConcurrencyFactor,
	variable.TiDBDistSQLScanConcurrency,
	variable.TiDBInitChunkSize,
	variable.TiDBMaxChunkSize,
	variable.TiDBEnableCascadesPlanner,
	variable.TiDBRetryLimit,
	variable.TiDBDisableTxnAutoRetry,
	variable.TiDBEnableWindowFunction,
	variable.TiDBEnableStrictDoubleTypeCheck,
	variable.TiDBEnableTablePartition,
	variable.TiDBEnableVectorizedExpression,
	variable.TiDBEnableFastAnalyze,
	variable.TiDBExpensiveQueryTimeThreshold,
	variable.TiDBEnableNoopFuncs,
	variable.TiDBEnableIndexMerge,
	variable.TiDBTxnMode,
	variable.TiDBAllowBatchCop,
	variable.TiDBAllowMPPExecution,
	variable.TiDBOptBCJ,
	variable.TiDBBCJThresholdSize,
	variable.TiDBBCJThresholdCount,
	variable.TiDBRowFormatVersion,
	variable.TiDBEnableStmtSummary,
	variable.TiDBStmtSummaryInternalQuery,
	variable.TiDBStmtSummaryRefreshInterval,
	variable.TiDBStmtSummaryHistorySize,
	variable.TiDBStmtSummaryMaxStmtCount,
	variable.TiDBStmtSummaryMaxSQLLength,
	variable.TiDBMaxDeltaSchemaCount,
	variable.TiDBCapturePlanBaseline,
	variable.TiDBUsePlanBaselines,
	variable.TiDBEvolvePlanBaselines,
	variable.TiDBEnableExtendedStats,
	variable.TiDBIsolationReadEngines,
	variable.TiDBStoreLimit,
	variable.TiDBAllowAutoRandExplicitInsert,
	variable.TiDBEnableClusteredIndex,
	variable.TiDBPartitionPruneMode,
	variable.TiDBRedactLog,
	variable.TiDBEnableTelemetry,
	variable.TiDBShardAllocateStep,
	variable.TiDBEnableChangeColumnType,
	variable.TiDBEnableChangeMultiSchema,
	variable.TiDBEnablePointGetCache,
	variable.TiDBEnableAlterPlacement,
	variable.TiDBEnableAmendPessimisticTxn,
	variable.TiDBMemQuotaApplyCache,
	variable.TiDBEnableParallelApply,
	variable.TiDBMemoryUsageAlarmRatio,
	variable.TiDBEnableRateLimitAction,
	variable.TiDBEnableAsyncCommit,
	variable.TiDBEnable1PC,
	variable.TiDBGuaranteeExternalConsistency,
	variable.TiDBAnalyzeVersion,
	variable.TiDBEnableIndexMergeJoin,
	variable.TiDBTrackAggregateMemoryUsage,
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
	loadFunc := func() ([]chunk.Row, []*ast.ResultField, error) {
		return s.ExecRestrictedSQL(loadCommonGlobalVarsSQL)
	}
	rows, fields, err := gvc.LoadGlobalVariables(loadFunc)
	if err != nil {
		logutil.BgLogger().Warn("failed to load global variables",
			zap.Uint64("conn", s.sessionVars.ConnectionID), zap.Error(err))
		return err
	}
	vars.CommonGlobalLoaded = true

	for _, row := range rows {
		varName := row.GetString(0)
		varVal := row.GetDatum(1, &fields[1].Column.FieldType)
		if _, ok := vars.GetSystemVar(varName); !ok {
			err = variable.SetSessionSystemVar(s.sessionVars, varName, varVal)
			if err != nil {
				return err
			}
		}
	}

	// when client set Capability Flags CLIENT_INTERACTIVE, init wait_timeout with interactive_timeout
	if vars.ClientCapability&mysql.ClientInteractive > 0 {
		if varVal, ok := vars.GetSystemVar(variable.InteractiveTimeout); ok {
			if err := vars.SetSystemVar(variable.WaitTimeout, varVal); err != nil {
				return err
			}
		}
	}

	vars.CommonGlobalLoaded = true
	return nil
}

// PrepareTxnCtx starts a goroutine to begin a transaction if needed, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *session) PrepareTxnCtx(ctx context.Context) {
	s.currentCtx = ctx
	if s.txn.validOrPending() {
		return
	}

	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		ShardStep:     int(s.sessionVars.ShardAllocateStep),
		TxnScope:      s.GetSessionVars().CheckAndGetTxnScope(),
	}
	if !s.sessionVars.IsAutocommit() || s.sessionVars.RetryInfo.Retrying {
		if s.sessionVars.TxnMode == ast.Pessimistic {
			s.sessionVars.TxnCtx.IsPessimistic = true
		}
	}
}

// PrepareTSFuture uses to try to get ts future.
func (s *session) PrepareTSFuture(ctx context.Context) {
	if !s.txn.validOrPending() {
		// Prepare the transaction future if the transaction is invalid (at the beginning of the transaction).
		txnFuture := s.getTxnFuture(ctx)
		s.txn.changeInvalidToPending(txnFuture)
	} else if s.txn.Valid() && s.GetSessionVars().IsPessimisticReadConsistency() {
		// Prepare the statement future if the transaction is valid in RC transactions.
		s.GetSessionVars().TxnCtx.SetStmtFutureForRC(s.getTxnFuture(ctx).future)
	}
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	var commitDetail *execdetails.CommitDetails
	ctx = context.WithValue(ctx, execdetails.CommitDetailCtxKey, &commitDetail)
	err := s.doCommit(ctx)
	if commitDetail != nil {
		s.GetSessionVars().StmtCtx.MergeExecDetails(nil, commitDetail)
	}
	if err != nil {
		return err
	}

	return s.NewTxn(ctx)
}

// InitTxnWithStartTS create a transaction with startTS.
func (s *session) InitTxnWithStartTS(startTS uint64) error {
	if s.txn.Valid() {
		return nil
	}

	// no need to get txn from txnFutureCh since txn should init with startTs
	txn, err := s.store.BeginWithStartTS(s.GetSessionVars().CheckAndGetTxnScope(), startTS)
	if err != nil {
		return err
	}
	txn.SetVars(s.sessionVars.KVVars)
	s.txn.changeInvalidToValid(txn)
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return err
	}
	return nil
}

// NewTxnWithStalenessOption create a transaction with Staleness option
func (s *session) NewTxnWithStalenessOption(ctx context.Context, option sessionctx.StalenessTxnOption) error {
	if s.txn.Valid() {
		txnID := s.txn.StartTS()
		txnScope := s.txn.GetUnionStore().GetOption(kv.TxnScope).(string)
		err := s.CommitTxn(ctx)
		if err != nil {
			return err
		}
		vars := s.GetSessionVars()
		logutil.Logger(ctx).Info("InitTxnWithExactStaleness() inside a transaction auto commit",
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", txnID),
			zap.String("txnScope", txnScope))
	}
	var txn kv.Transaction
	var err error
	txnScope := s.GetSessionVars().TxnScope
	switch option.Mode {
	case ast.TimestampBoundReadTimestamp:
		txn, err = s.store.BeginWithStartTS(txnScope, option.StartTS)
		if err != nil {
			return err
		}
	case ast.TimestampBoundExactStaleness:
		txn, err = s.store.BeginWithExactStaleness(txnScope, option.PrevSec)
		if err != nil {
			return err
		}
	default:
		// For unsupported staleness txn cases, fallback to NewTxn
		return s.NewTxn(ctx)
	}
	txn.SetVars(s.sessionVars.KVVars)
	txn.SetOption(kv.IsStalenessReadOnly, true)
	txn.SetOption(kv.TxnScope, txnScope)
	s.txn.changeInvalidToValid(txn)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		StartTS:       txn.StartTS(),
		ShardStep:     int(s.sessionVars.ShardAllocateStep),
		IsStaleness:   true,
		TxnScope:      txnScope,
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
func logStmt(execStmt *executor.ExecStmt, vars *variable.SessionVars) {
	switch stmt := execStmt.StmtNode.(type) {
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateIndexStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropIndexStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt:
		user := vars.User
		schemaVersion := vars.TxnCtx.SchemaVersion
		if ss, ok := execStmt.StmtNode.(ast.SensitiveStmtNode); ok {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("secure text", ss.SecureText()),
				zap.Stringer("user", user))
		} else {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("cur_db", vars.CurrentDB),
				zap.String("sql", stmt.Text()),
				zap.Stringer("user", user))
		}
	default:
		logQuery(execStmt.GetTextToLog(), vars)
	}
}

func logQuery(query string, vars *variable.SessionVars) {
	if variable.ProcessGeneralLog.Load() && !vars.InRestrictedSQL {
		query = executor.QueryReplacer.Replace(query)
		if !vars.EnableRedactLog {
			query += vars.PreparedParams.String()
		}
		logutil.BgLogger().Info("GENERAL_LOG",
			zap.Uint64("conn", vars.ConnectionID),
			zap.Stringer("user", vars.User),
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", vars.TxnCtx.StartTS),
			zap.Uint64("forUpdateTS", vars.TxnCtx.GetForUpdateTS()),
			zap.Bool("isReadConsistency", vars.IsIsolation(ast.ReadCommitted)),
			zap.String("current_db", vars.CurrentDB),
			zap.String("txn_mode", vars.GetReadableTxnMode()),
			zap.String("sql", query))
	}
}

func (s *session) recordOnTransactionExecution(err error, counter int, duration float64) {
	if s.sessionVars.TxnCtx.IsPessimistic {
		if err != nil {
			statementPerTransactionPessimisticError.Observe(float64(counter))
			transactionDurationPessimisticAbort.Observe(duration)
		} else {
			statementPerTransactionPessimisticOK.Observe(float64(counter))
			transactionDurationPessimisticCommit.Observe(duration)
		}
	} else {
		if err != nil {
			statementPerTransactionOptimisticError.Observe(float64(counter))
			transactionDurationOptimisticAbort.Observe(duration)
		} else {
			statementPerTransactionOptimisticOK.Observe(float64(counter))
			transactionDurationOptimisticCommit.Observe(duration)
		}
	}
}

func (s *session) checkPlacementPolicyBeforeCommit() error {
	var err error
	// Get the txnScope of the transaction we're going to commit.
	txnScope := s.GetSessionVars().TxnCtx.TxnScope
	if txnScope == "" {
		txnScope = oracle.GlobalTxnScope
	}
	if txnScope != oracle.GlobalTxnScope {
		is := infoschema.GetInfoSchema(s)
		deltaMap := s.GetSessionVars().TxnCtx.TableDeltaMap
		for physicalTableID := range deltaMap {
			var tableName string
			var partitionName string
			tblInfo, _, partInfo := is.FindTableByPartitionID(physicalTableID)
			if tblInfo != nil && partInfo != nil {
				tableName = tblInfo.Meta().Name.String()
				partitionName = partInfo.Name.String()
			} else {
				tblInfo, _ := is.TableByID(physicalTableID)
				tableName = tblInfo.Meta().Name.String()
			}
			bundle, ok := is.BundleByName(placement.GroupID(physicalTableID))
			if !ok {
				errMsg := fmt.Sprintf("table %v doesn't have placement policies with txn_scope %v",
					tableName, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v doesn't have placement policies with txn_scope %v",
						tableName, partitionName, txnScope)
				}
				err = ddl.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			dcLocation, ok := placement.GetLeaderDCByBundle(bundle, placement.DCLabelKey)
			if !ok {
				errMsg := fmt.Sprintf("table %v's leader placement policy is not defined", tableName)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader placement policy is not defined", tableName, partitionName)
				}
				err = ddl.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			if dcLocation != txnScope {
				errMsg := fmt.Sprintf("table %v's leader location %v is out of txn_scope %v", tableName, dcLocation, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader location %v is out of txn_scope %v",
						tableName, partitionName, dcLocation, txnScope)
				}
				err = ddl.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			// FIXME: currently we assume the physicalTableID is the partition ID. In future, we should consider the situation
			// if the physicalTableID belongs to a Table.
			partitionID := physicalTableID
			tbl, _, partitionDefInfo := is.FindTableByPartitionID(partitionID)
			if tbl != nil {
				tblInfo := tbl.Meta()
				state := tblInfo.Partition.GetStateByID(partitionID)
				if state == model.StateGlobalTxnOnly {
					err = ddl.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(
						fmt.Sprintf("partition %s of table %s can not be written by local transactions when its placement policy is being altered",
							tblInfo.Name, partitionDefInfo.Name))
					break
				}
			}
		}
	}
	return err
}
