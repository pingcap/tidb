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

	"github.com/juju/errors"
	"github.com/ngaut/pools"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/kvcache"
	binlog "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// Session context
type Session interface {
	sessionctx.Context
	Status() uint16                                           // Flag of current status, such as autocommit.
	LastInsertID() uint64                                     // LastInsertID is the last inserted auto_increment ID.
	AffectedRows() uint64                                     // Affected rows by latest executed stmt.
	Execute(context.Context, string) ([]ast.RecordSet, error) // Execute a sql statement.
	String() string                                           // String is used to debug.
	CommitTxn(context.Context) error
	RollbackTxn(context.Context) error
	// PrepareStmt executes prepare statement in binary protocol.
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// ExecutePreparedStmt executes a prepared statement.
	ExecutePreparedStmt(ctx context.Context, stmtID uint32, param ...interface{}) (ast.RecordSet, error)
	DropPreparedStmt(stmtID uint32) error
	SetClientCapability(uint32) // Set client capability flags.
	SetConnectionID(uint64)
	SetTLSState(*tls.ConnectionState)
	SetCollation(coID int) error
	SetSessionManager(util.SessionManager)
	Close()
	Auth(user *auth.UserIdentity, auth []byte, salt []byte) bool
	ShowProcess() util.ProcessInfo
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
	st      ast.Statement
	stmtCtx *stmtctx.StatementContext
	params  []interface{}
}

// StmtHistory holds all histories of statements in a txn.
type StmtHistory struct {
	history []*stmtRecord
}

// Add appends a stmt to history list.
func (h *StmtHistory) Add(stmtID uint32, st ast.Statement, stmtCtx *stmtctx.StatementContext, params ...interface{}) {
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
}

func (s *session) getMembufCap() int {
	if s.sessionVars.ImportingData {
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
	if s.sessionVars.LastInsertID > 0 {
		return s.sessionVars.LastInsertID
	}
	return s.sessionVars.InsertID
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
		s.statsCollector.StoreQueryFeedback(feedback)
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

type schemaLeaseChecker struct {
	domain.SchemaValidator
	schemaVer       int64
	relatedTableIDs []int64
}

var (
	// SchemaOutOfDateRetryInterval is the sleeping time when we fail to try.
	SchemaOutOfDateRetryInterval = int64(500 * time.Millisecond)
	// SchemaOutOfDateRetryTimes is upper bound of retry times when the schema is out of date.
	SchemaOutOfDateRetryTimes = int32(10)
)

func (s *schemaLeaseChecker) Check(txnTS uint64) error {
	schemaOutOfDateRetryInterval := atomic.LoadInt64(&SchemaOutOfDateRetryInterval)
	schemaOutOfDateRetryTimes := int(atomic.LoadInt32(&SchemaOutOfDateRetryTimes))
	for i := 0; i < schemaOutOfDateRetryTimes; i++ {
		result := s.SchemaValidator.Check(txnTS, s.schemaVer, s.relatedTableIDs)
		switch result {
		case domain.ResultSucc:
			return nil
		case domain.ResultFail:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return domain.ErrInfoSchemaChanged
		case domain.ResultUnknown:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
			time.Sleep(time.Duration(schemaOutOfDateRetryInterval))
		}

	}
	return domain.ErrInfoSchemaExpired
}

// mockCommitErrorOnce use to make sure gofail mockCommitError only mock commit error once.
var mockCommitErrorOnce = true

func (s *session) doCommit(ctx context.Context) error {
	if !s.txn.Valid() {
		return nil
	}
	defer func() {
		s.txn.changeToInvalid()
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()

	// mockCommitError and mockGetTSErrorInRetry use to test PR #8743.
	// gofail: var mockCommitError bool
	//if mockCommitError && mockCommitErrorOnce {
	//	mockCommitErrorOnce = false
	//	return kv.ErrRetryable
	//}

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
				Client: s.sessionVars.BinlogClient.(binlog.PumpClient),
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
	s.txn.SetOption(kv.SchemaLeaseChecker, &schemaLeaseChecker{
		SchemaValidator: domain.GetDomain(s).SchemaValidator,
		schemaVer:       s.sessionVars.TxnCtx.SchemaVersion,
		relatedTableIDs: tableIDs,
	})
	if err := s.txn.Commit(sessionctx.SetConnID2Ctx(ctx, s)); err != nil {
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
		commitRetryLimit := globalCommitRetryLimit
		if s.sessionVars.DisableTxnAutoRetry && !s.sessionVars.InRestrictedSQL {
			// Do not retry non-autocommit transactions.
			// For autocommit single statement transactions, the history count is always 1.
			// For explicit transactions, the statement count is more than 1.
			history := GetHistory(s)
			if history.Count() > 1 {
				commitRetryLimit = 0
			}
		}
		// Don't retry in BatchInsert mode. As a counter-example, insert into t1 select * from t2,
		// BatchInsert already commit the first batch 1000 rows, then it commit 1000-2000 and retry the statement,
		// Finally t1 will have more data than t2, with no errors return to user!
		if s.isRetryableError(err) && !s.sessionVars.BatchInsert && commitRetryLimit > 0 {
			log.Warnf("con:%d retryable error: %v, txn: %v", s.sessionVars.ConnectionID, err, s.txn)
			// Transactions will retry 2 ~ commitRetryLimit times.
			// We make larger transactions retry less times to prevent cluster resource outage.
			txnSizeRate := float64(txnSize) / float64(kv.TxnTotalSizeLimit)
			maxRetryCount := commitRetryLimit - uint(float64(commitRetryLimit-1)*txnSizeRate)
			err = s.retry(ctx, maxRetryCount)
		}
	}
	counter := s.sessionVars.TxnCtx.StatementCount
	duration := time.Since(s.GetSessionVars().TxnCtx.CreateTime).Seconds()
	metrics.StatementPerTransaction.WithLabelValues(metrics.RetLabel(err)).Observe(float64(counter))
	metrics.TransactionDuration.WithLabelValues(metrics.RetLabel(err)).Observe(float64(duration))
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
		log.Warnf("con:%d finished txn:%v, %v", s.sessionVars.ConnectionID, s.txn, err)
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
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("session.CommitTxn", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	stmt := executor.ExecStmt{
		Text:      "commit",
		Ctx:       s,
		StartTime: time.Now(),
	}
	err := s.doCommitWithRetry(ctx)
	stmt.LogSlowQuery(s.sessionVars.TxnCtx.StartTS, err == nil)
	label := metrics.LblOK
	if err != nil {
		label = metrics.LblError
	}
	s.sessionVars.TxnCtx.Cleanup()
	metrics.TransactionCounter.WithLabelValues(label).Inc()
	return errors.Trace(err)
}

func (s *session) RollbackTxn(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("session.RollbackTxn", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	var err error
	if s.txn.Valid() {
		terror.Log(s.txn.Rollback())
		metrics.TransactionCounter.WithLabelValues(metrics.LblRollback).Inc()
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
	if sessVars.LastInsertID > 0 {
		data["lastInsertID"] = sessVars.LastInsertID
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

func (s *session) isRetryableError(err error) bool {
	if SchemaChangedWithoutRetry {
		return kv.IsRetryableError(err)
	}
	return kv.IsRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

func (s *session) retry(ctx context.Context, maxCnt uint) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "retry")
	defer span.Finish()

	connID := s.sessionVars.ConnectionID
	if s.sessionVars.TxnCtx.ForUpdate {
		return errors.Errorf("[%d] can not retry select for update statement", connID)
	}
	s.sessionVars.RetryInfo.Retrying = true
	var retryCnt uint
	defer func() {
		s.sessionVars.RetryInfo.Retrying = false
		s.txn.changeToInvalid()
		// retryCnt only increments on retryable error, so +1 here.
		metrics.SessionRetry.Observe(float64(retryCnt + 1))
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()

	nh := GetHistory(s)
	var err error
	orgStartTS := s.GetSessionVars().TxnCtx.StartTS
	for {
		s.PrepareTxnCtx(ctx)
		s.sessionVars.RetryInfo.ResetOffset()
		for i, sr := range nh.history {
			st := sr.st
			if st.IsReadOnly() {
				continue
			}
			err = st.RebuildPlan()
			if err != nil {
				return errors.Trace(err)
			}
			if retryCnt == 0 {
				// We do not have to log the query every time.
				// We print the queries at the first try only.
				log.Warnf("con:%d retry_cnt:%d query_num:%d sql:%s", connID, retryCnt, i, sqlForLog(st.OriginText()))
			} else {
				log.Warnf("con:%d retry_cnt:%d query_num:%d", connID, retryCnt, i)
			}
			s.sessionVars.StmtCtx = sr.stmtCtx
			s.sessionVars.StmtCtx.ResetForRetry()
			_, err = st.Exec(ctx)
			if err != nil {
				s.StmtRollback()
				break
			}
			s.StmtCommit()
		}
		log.Warnf("con:%d retrying_txn_start_ts:%d original_txn_start_ts:(%d)",
			connID, s.GetSessionVars().TxnCtx.StartTS, orgStartTS)
		if hook := ctx.Value("preCommitHook"); hook != nil {
			// For testing purpose.
			hook.(func())()
		}

		if err == nil && s.txn.fail != nil {
			err = s.txn.fail
			s.txn.cleanup()
			s.txn.fail = nil
		}

		if err == nil {
			err = s.doCommit(ctx)
			if err == nil {
				break
			}
		}
		if !s.isRetryableError(err) {
			log.Warnf("con:%d session:%v, err:%v in retry", connID, s, err)
			metrics.SessionRetryErrorCounter.WithLabelValues(metrics.LblUnretryable)
			return errors.Trace(err)
		}
		retryCnt++
		if retryCnt >= maxCnt {
			log.Warnf("con:%d Retry reached max count %d", connID, retryCnt)
			metrics.SessionRetryErrorCounter.WithLabelValues(metrics.LblReachMax)
			return errors.Trace(err)
		}
		log.Warnf("con:%d retryable error: %v, txn: %v", connID, err, s.txn)
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
func (s *session) ExecRestrictedSQL(sctx sessionctx.Context, sql string) ([]types.Row, []*ast.ResultField, error) {
	var span opentracing.Span
	ctx := context.TODO()
	span, ctx = opentracing.StartSpanFromContext(ctx, "session.ExecRestrictedSQL")
	defer span.Finish()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	se := tmp.(*session)
	defer s.sysSessionPool().Put(tmp)
	metrics.SessionRestrictedSQLCounter.Inc()

	recordSets, err := se.Execute(ctx, sql)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var (
		rows   []types.Row
		fields []*ast.ResultField
	)
	// Execute all recordset, take out the first one as result.
	for i, rs := range recordSets {
		tmp, err := drainRecordSet(ctx, rs)
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
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		return se, nil
	}
}

func drainRecordSet(ctx context.Context, rs ast.RecordSet) ([]types.Row, error) {
	var rows []types.Row
	for {
		chk := rs.NewChunk()
		err := rs.Next(ctx, chk)
		if err != nil || chk.NumRows() == 0 {
			return rows, errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(chk)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
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
		return "", errors.Trace(err)
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
			return "", variable.UnknownSystemVar.GenByArgs(name)
		}
		return "", errors.Trace(err)
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(name string, value string) error {
	if name == variable.SQLModeVar {
		value = mysql.FormatSQLModeStr(value)
		if _, err := mysql.GetSQLMode(value); err != nil {
			return errors.Trace(err)
		}
	}
	sql := fmt.Sprintf(`REPLACE %s.%s VALUES ('%s', '%s');`,
		mysql.SystemDB, mysql.GlobalVariablesTable, strings.ToLower(name), value)
	_, _, err := s.ExecRestrictedSQL(s, sql)
	return errors.Trace(err)
}

func (s *session) ParseSQL(ctx context.Context, sql, charset, collation string) ([]ast.StmtNode, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span1 := opentracing.StartSpan("session.ParseSQL", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	s.parser.SetSQLMode(s.sessionVars.SQLMode)
	return s.parser.Parse(sql, charset, collation)
}

func (s *session) SetProcessInfo(sql string) {
	pi := util.ProcessInfo{
		ID:      s.sessionVars.ConnectionID,
		DB:      s.sessionVars.CurrentDB,
		Command: "Query",
		Time:    time.Now(),
		State:   s.Status(),
		Info:    sql,
	}
	if s.sessionVars.User != nil {
		pi.User = s.sessionVars.User.Username
		pi.Host = s.sessionVars.User.Hostname
	}
	s.processInfo.Store(pi)
}

func (s *session) executeStatement(ctx context.Context, connID uint64, stmtNode ast.StmtNode, stmt ast.Statement, recordSets []ast.RecordSet) ([]ast.RecordSet, error) {
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
			log.Warnf("con:%d session error:\n%v\n%s", connID, errors.ErrorStack(err), s)
		}
		return nil, errors.Trace(err)
	}
	metrics.SessionExecuteRunDuration.Observe(time.Since(startTime).Seconds())

	if recordSet != nil {
		recordSets = append(recordSets, recordSet)
	}
	return recordSets, nil
}

func (s *session) Execute(ctx context.Context, sql string) (recordSets []ast.RecordSet, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span, ctx = opentracing.StartSpanFromContext(ctx, "session.Execute")
		defer span.Finish()
	}

	s.PrepareTxnCtx(ctx)
	var (
		cacheKey         kvcache.Key
		cacheValue       kvcache.Value
		hitCache         = false
		connID           = s.sessionVars.ConnectionID
		planCacheEnabled = s.sessionVars.PlanCacheEnabled // Its value is read from the global configuration, and it will be only updated in tests.
	)

	if planCacheEnabled {
		schemaVersion := domain.GetDomain(s).InfoSchema().SchemaMetaVersion()
		readOnly := s.Txn(true) == nil || s.Txn(true).IsReadOnly()

		cacheKey = plan.NewSQLCacheKey(s.sessionVars, sql, schemaVersion, readOnly)
		cacheValue, hitCache = plan.GlobalPlanCache.Get(cacheKey)
	}

	if hitCache {
		metrics.PlanCacheCounter.WithLabelValues("select").Inc()
		stmtNode := cacheValue.(*plan.SQLCacheValue).StmtNode
		stmt := &executor.ExecStmt{
			InfoSchema: executor.GetInfoSchema(s),
			Plan:       cacheValue.(*plan.SQLCacheValue).Plan,
			Expensive:  cacheValue.(*plan.SQLCacheValue).Expensive,
			Text:       stmtNode.Text(),
			StmtNode:   stmtNode,
			Ctx:        s,
		}

		s.PrepareTxnCtx(ctx)
		executor.ResetStmtCtx(s, stmtNode)
		if recordSets, err = s.executeStatement(ctx, connID, stmtNode, stmt, recordSets); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		err = s.loadCommonGlobalVariablesIfNeeded()
		if err != nil {
			return nil, errors.Trace(err)
		}

		charsetInfo, collation := s.sessionVars.GetCharsetInfo()

		// Step1: Compile query string to abstract syntax trees(ASTs).
		startTS := time.Now()
		stmtNodes, err := s.ParseSQL(ctx, sql, charsetInfo, collation)
		if err != nil {
			s.rollbackOnError(ctx)
			log.Warnf("con:%d parse error:\n%v\n%s", connID, err, sql)
			return nil, errors.Trace(err)
		}
		metrics.SessionExecuteParseDuration.Observe(time.Since(startTS).Seconds())

		compiler := executor.Compiler{Ctx: s}
		for _, stmtNode := range stmtNodes {
			s.PrepareTxnCtx(ctx)

			// Step2: Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
			startTS = time.Now()
			// Some executions are done in compile stage, so we reset them before compile.
			executor.ResetStmtCtx(s, stmtNode)
			stmt, err := compiler.Compile(ctx, stmtNode)
			if err != nil {
				s.rollbackOnError(ctx)
				log.Warnf("con:%d compile error:\n%v\n%s", connID, err, sql)
				return nil, errors.Trace(err)
			}
			metrics.SessionExecuteCompileDuration.Observe(time.Since(startTS).Seconds())

			// Step3: Cache the physical plan if possible.
			if planCacheEnabled && stmt.Cacheable && len(stmtNodes) == 1 && !s.GetSessionVars().StmtCtx.HistogramsNotLoad() {
				plan.GlobalPlanCache.Put(cacheKey, plan.NewSQLCacheValue(stmtNode, stmt.Plan, stmt.Expensive))
			}

			// Step4: Execute the physical plan.
			if recordSets, err = s.executeStatement(ctx, connID, stmtNode, stmt, recordSets); err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	if s.sessionVars.ClientCapability&mysql.ClientMultiResults == 0 && len(recordSets) > 1 {
		// return the first recordset if client doesn't support ClientMultiResults.
		recordSets = recordSets[:1]
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
func (s *session) ExecutePreparedStmt(ctx context.Context, stmtID uint32, args ...interface{}) (ast.RecordSet, error) {
	err := checkArgs(args...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.PrepareTxnCtx(ctx)
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
		return plan.ErrStmtNotFound
	}
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
}

func (s *session) Txn(active bool) kv.Transaction {
	if s.txn.pending() && active {
		// Transaction is lazy intialized.
		// PrepareTxnCtx is called to get a tso future, makes s.txn a pending txn,
		// If Txn() is called later, wait for the future to get a valid txn.
		txnCap := s.getMembufCap()
		if err := s.txn.changePendingToValid(txnCap); err != nil {
			log.Error("active transaction fail, err = ", err)
			s.txn.fail = errors.Trace(err)
			s.txn.cleanup()
		} else {
			s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
		}
		if !s.sessionVars.IsAutocommit() {
			s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
	}
	return &s.txn
}

func (s *session) NewTxn() error {
	if s.txn.Valid() {
		txnID := s.txn.StartTS()
		ctx := context.TODO()
		err := s.CommitTxn(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("con:%d NewTxn() inside a transaction auto commit: %d", s.GetSessionVars().ConnectionID, txnID)
	}

	txn, err := s.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	txn.SetCap(s.getMembufCap())
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
		log.Error("session Close error:", errors.ErrorStack(err))
	}
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s *session) Auth(user *auth.UserIdentity, authentication []byte, salt []byte) bool {
	pm := privilege.GetPrivilegeManager(s)

	// Check IP.
	if pm.ConnectionVerification(user.Username, user.Hostname, authentication, salt) {
		s.sessionVars.User = user
		return true
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

	log.Errorf("User connection verification failed %s", user)
	return false
}

func getHostByIP(ip string) []string {
	if ip == "127.0.0.1" {
		return []string{"localhost"}
	}
	addrs, err := net.LookupAddr(ip)
	terror.Log(errors.Trace(err))
	return addrs
}

func chooseMinLease(n1 time.Duration, n2 time.Duration) time.Duration {
	if n1 <= n2 {
		return n1
	}
	return n2
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

// BootstrapSession runs the first time when the TiDB server start.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
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
	dom := domain.GetDomain(se)
	err = dom.LoadPrivilegeLoop(se)
	if err != nil {
		return nil, errors.Trace(err)
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
// TODO: Using a bootstap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, bootstrap func(Session)) {
	saveLease := schemaLease
	schemaLease = chooseMinLease(schemaLease, 100*time.Millisecond)
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		log.Fatal(errors.ErrorStack(err))
	}
	schemaLease = saveLease

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
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	if plan.PreparedPlanCacheEnabled {
		s.preparedPlanCache = kvcache.NewSimpleLRUCache(plan.PreparedPlanCacheCapacity)
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.sessionVars.BinlogClient = binloginfo.GetPumpClient()
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
	if plan.PreparedPlanCacheEnabled {
		s.preparedPlanCache = kvcache.NewSimpleLRUCache(plan.PreparedPlanCacheCapacity)
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
	currentBootstrapVersion = 21
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
		log.Fatalf("check bootstrapped err %v", err)
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
		log.Fatalf("finish bootstrap err %v", err)
	}
}

const quoteCommaQuote = "', '"
const loadCommonGlobalVarsSQL = "select HIGH_PRIORITY * from mysql.global_variables where variable_name in ('" +
	variable.AutocommitVar + quoteCommaQuote +
	variable.SQLModeVar + quoteCommaQuote +
	variable.MaxAllowedPacket + quoteCommaQuote +
	variable.TimeZone + quoteCommaQuote +
	/* TiDB specific global variables: */
	variable.TiDBSkipUTF8Check + quoteCommaQuote +
	variable.TiDBIndexJoinBatchSize + quoteCommaQuote +
	variable.TiDBIndexLookupSize + quoteCommaQuote +
	variable.TiDBIndexLookupConcurrency + quoteCommaQuote +
	variable.TiDBIndexLookupJoinConcurrency + quoteCommaQuote +
	variable.TiDBIndexSerialScanConcurrency + quoteCommaQuote +
	variable.TiDBHashJoinConcurrency + quoteCommaQuote +
	variable.TiDBDDLReorgWorkerCount + quoteCommaQuote +
	variable.TiDBDistSQLScanConcurrency + quoteCommaQuote +
	variable.TiDBMaxChunkSize + quoteCommaQuote +
	variable.TiDBDisableTxnAutoRetry + "')"

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return nil
	}
	// Set the variable to true to prevent cyclic recursive call.
	vars.CommonGlobalLoaded = true
	rows, fields, err := s.ExecRestrictedSQL(s, loadCommonGlobalVarsSQL)
	if err != nil {
		vars.CommonGlobalLoaded = false
		log.Errorf("Failed to load common global variables.")
		return errors.Trace(err)
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

func (s *session) ShowProcess() util.ProcessInfo {
	var pi util.ProcessInfo
	tmp := s.processInfo.Load()
	if tmp != nil {
		pi = tmp.(util.ProcessInfo)
		pi.Mem = s.GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
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
		if ss, ok := node.(ast.SensitiveStmtNode); ok {
			log.Infof("[CRUCIAL OPERATION] con:%d %s (by %s).", vars.ConnectionID, ss.SecureText(), user)
		} else {
			log.Infof("[CRUCIAL OPERATION] con:%d %s (by %s).", vars.ConnectionID, stmt.Text(), user)
		}
	default:
		logQuery(node.Text(), vars)
	}
}

func logQuery(query string, vars *variable.SessionVars) {
	if atomic.LoadUint32(&variable.ProcessGeneralLog) != 0 && !vars.InRestrictedSQL {
		query = executor.QueryReplacer.Replace(query)
		log.Infof("[GENERAL_LOG] con:%d user:%s schema_ver:%d start_ts:%d sql:%s", vars.ConnectionID, vars.User, vars.TxnCtx.SchemaVersion, vars.TxnCtx.StartTS, query)
	}
}
