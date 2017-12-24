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

package tidb

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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

// Session context
type Session interface {
	context.Context
	Status() uint16                                         // Flag of current status, such as autocommit.
	LastInsertID() uint64                                   // LastInsertID is the last inserted auto_increment ID.
	AffectedRows() uint64                                   // Affected rows by latest executed stmt.
	Execute(goctx.Context, string) ([]ast.RecordSet, error) // Execute a sql statement.
	String() string                                         // String is used to debug.
	CommitTxn(goctx.Context) error
	RollbackTxn(goctx.Context) error
	// PrepareStmt executes prepare statement in binary protocol.
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// ExecutePreparedStmt executes a prepared statement.
	ExecutePreparedStmt(goCtx goctx.Context, stmtID uint32, param ...interface{}) (ast.RecordSet, error)
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
	PrepareTxnCtx(goctx.Context)
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

type session struct {
	// processInfo is used by ShowProcess(), and should be modified atomically.
	processInfo atomic.Value
	txn         kv.Transaction // current transaction
	txnFuture   *txnFuture

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
		s.sessionVars.Systems[v] = cs
	}
	s.sessionVars.Systems[variable.CollationConnection] = co
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
	s.statsCollector.StoreQueryFeedback(feedback)
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
			schemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return domain.ErrInfoSchemaChanged
		case domain.ResultUnknown:
			schemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
			time.Sleep(time.Duration(schemaOutOfDateRetryInterval))
		}

	}
	return domain.ErrInfoSchemaExpired
}

func (s *session) doCommit(ctx goctx.Context) error {
	if s.txn == nil || !s.txn.Valid() {
		return nil
	}
	defer func() {
		s.txn = nil
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()
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
	if err := s.txn.Commit(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *session) doCommitWithRetry(ctx goctx.Context) error {
	var txnSize int
	if s.txn != nil && s.txn.Valid() {
		txnSize = s.txn.Size()
	}
	err := s.doCommit(ctx)
	if err != nil {
		if s.isRetryableError(err) {
			log.Warnf("[%d] retryable error: %v, txn: %v", s.sessionVars.ConnectionID, err, s.txn)
			// Transactions will retry 2 ~ commitRetryLimit times.
			// We make larger transactions retry less times to prevent cluster resource outage.
			txnSizeRate := float64(txnSize) / float64(kv.TxnTotalSizeLimit)
			maxRetryCount := commitRetryLimit - int(float64(commitRetryLimit-1)*txnSizeRate)
			err = s.retry(ctx, maxRetryCount)
		}
	}
	s.cleanRetryInfo()
	if err != nil {
		log.Warnf("[%d] finished txn:%v, %v", s.sessionVars.ConnectionID, s.txn, err)
		return errors.Trace(err)
	}
	mapper := s.GetSessionVars().TxnCtx.TableDeltaMap
	if s.statsCollector != nil && mapper != nil {
		for id, item := range mapper {
			s.statsCollector.Update(id, item.Delta, item.Count)
		}
	}
	return nil
}

func (s *session) CommitTxn(ctx goctx.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("session.CommitTxn", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	err := s.doCommitWithRetry(ctx)
	label := "OK"
	if err != nil {
		label = "Error"
	}
	transactionCounter.WithLabelValues(label).Inc()
	return errors.Trace(err)
}

func (s *session) RollbackTxn(ctx goctx.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("session.RollbackTxn", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	var err error
	if s.txn != nil && s.txn.Valid() {
		err = s.txn.Rollback()
	}
	s.cleanRetryInfo()
	s.txn = nil
	s.txnFuture = nil
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
	if s.txn != nil {
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

func (s *session) retry(goCtx goctx.Context, maxCnt int) error {
	span, ctx := opentracing.StartSpanFromContext(goCtx, "retry")
	defer span.Finish()

	connID := s.sessionVars.ConnectionID
	if s.sessionVars.TxnCtx.ForUpdate {
		return errors.Errorf("[%d] can not retry select for update statement", connID)
	}
	s.sessionVars.RetryInfo.Retrying = true
	retryCnt := 0
	defer func() {
		s.sessionVars.RetryInfo.Retrying = false
		sessionRetry.Observe(float64(retryCnt))
		s.txn = nil
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()

	nh := GetHistory(s)
	var err error
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
				log.Warnf("[%d] Retry [%d] query [%d] %s", connID, retryCnt, i, sqlForLog(st.OriginText()))
			} else {
				log.Warnf("[%d] Retry [%d] query [%d]", connID, retryCnt, i)
			}
			s.sessionVars.StmtCtx = sr.stmtCtx
			s.sessionVars.StmtCtx.ResetForRetry()
			_, err = st.Exec(goCtx)
			if err != nil {
				break
			}
		}
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
			log.Warnf("[%d] session:%v, err:%v", connID, s, err)
			return errors.Trace(err)
		}
		retryCnt++
		if retryCnt >= maxCnt {
			log.Warnf("[%d] Retry reached max count %d", connID, retryCnt)
			return errors.Trace(err)
		}
		log.Warnf("[%d] retryable error: %v, txn: %v", connID, err, s.txn)
		kv.BackOff(retryCnt)
		s.txn = nil
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}
	return err
}

func sqlForLog(sql string) string {
	if len(sql) > sqlLogMaxLen {
		return sql[:sqlLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	return sql
}

func (s *session) sysSessionPool() *pools.ResourcePool {
	return domain.GetDomain(s).SysSessionPool()
}

// ExecRestrictedSQL implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements, usually executed during a normal statement execution.
// Unlike normal Exec, it doesn't reset statement status, doesn't commit or rollback the current transaction
// and doesn't write binlog.
func (s *session) ExecRestrictedSQL(ctx context.Context, sql string) ([]types.Row, []*ast.ResultField, error) {
	var span opentracing.Span
	goCtx := goctx.TODO()
	span, goCtx = opentracing.StartSpanFromContext(goCtx, "session.ExecRestrictedSQL")
	defer span.Finish()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	se := tmp.(*session)
	defer s.sysSessionPool().Put(tmp)

	recordSets, err := se.Execute(goCtx, sql)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var (
		rows   []types.Row
		fields []*ast.ResultField
	)
	// Execute all recordset, take out the first one as result.
	for i, rs := range recordSets {
		tmp, err := drainRecordSet(goCtx, rs)
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
		err = varsutil.SetSessionSystemVar(se.sessionVars, variable.AutocommitVar, types.NewStringDatum("1"))
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
		err = varsutil.SetSessionSystemVar(se.sessionVars, variable.AutocommitVar, types.NewStringDatum("1"))
		if err != nil {
			return nil, errors.Trace(err)
		}
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		return se, nil
	}
}

func drainRecordSet(goCtx goctx.Context, rs ast.RecordSet) ([]types.Row, error) {
	var rows []types.Row
	for {
		row, err := rs.Next(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// getExecRet executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getExecRet(ctx context.Context, sql string) (string, error) {
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
	if s.Value(context.Initing) != nil {
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
	if s.Value(context.Initing) != nil {
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

func (s *session) ParseSQL(goCtx goctx.Context, sql, charset, collation string) ([]ast.StmtNode, error) {
	if span := opentracing.SpanFromContext(goCtx); span != nil {
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

func (s *session) executeStatement(goCtx goctx.Context, connID uint64, stmtNode ast.StmtNode, stmt ast.Statement, recordSets []ast.RecordSet) ([]ast.RecordSet, error) {
	s.SetValue(context.QueryString, stmt.OriginText())
	if _, ok := stmtNode.(ast.DDLNode); ok {
		s.SetValue(context.LastExecuteDDL, true)
	} else {
		s.ClearValue(context.LastExecuteDDL)
	}

	startTS := time.Now()
	recordSet, err := runStmt(goCtx, s, stmt)
	if err != nil {
		if !kv.ErrKeyExists.Equal(err) {
			log.Warnf("[%d] session error:\n%v\n%s", connID, errors.ErrorStack(err), s)
		}
		return nil, errors.Trace(err)
	}
	sessionExecuteRunDuration.Observe(time.Since(startTS).Seconds())

	if recordSet != nil {
		recordSets = append(recordSets, recordSet)
	}
	logCrucialStmt(stmtNode, s.sessionVars.User)
	return recordSets, nil
}

func (s *session) Execute(goCtx goctx.Context, sql string) (recordSets []ast.RecordSet, err error) {
	if span := opentracing.SpanFromContext(goCtx); span != nil {
		span, goCtx = opentracing.StartSpanFromContext(goCtx, "session.Execute")
		defer span.Finish()
	}

	s.PrepareTxnCtx(goCtx)
	var (
		cacheKey      kvcache.Key
		cacheValue    kvcache.Value
		useCachedPlan = false
		connID        = s.sessionVars.ConnectionID
	)

	if plan.PlanCacheEnabled {
		schemaVersion := domain.GetDomain(s).InfoSchema().SchemaMetaVersion()
		readOnly := s.Txn() == nil || s.Txn().IsReadOnly()

		cacheKey = plan.NewSQLCacheKey(s.sessionVars, sql, schemaVersion, readOnly)
		cacheValue, useCachedPlan = plan.GlobalPlanCache.Get(cacheKey)
	}

	if useCachedPlan {
		stmtNode := cacheValue.(*plan.SQLCacheValue).StmtNode
		stmt := &executor.ExecStmt{
			InfoSchema: executor.GetInfoSchema(s),
			Plan:       cacheValue.(*plan.SQLCacheValue).Plan,
			Expensive:  cacheValue.(*plan.SQLCacheValue).Expensive,
			Text:       stmtNode.Text(),
			StmtNode:   stmtNode,
			Ctx:        s,
		}

		s.PrepareTxnCtx(goCtx)
		executor.ResetStmtCtx(s, stmtNode)
		if recordSets, err = s.executeStatement(goCtx, connID, stmtNode, stmt, recordSets); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		charset, collation := s.sessionVars.GetCharsetInfo()

		// Step1: Compile query string to abstract syntax trees(ASTs).
		startTS := time.Now()
		stmtNodes, err := s.ParseSQL(goCtx, sql, charset, collation)
		if err != nil {
			log.Warnf("[%d] parse error:\n%v\n%s", connID, err, sql)
			return nil, errors.Trace(err)
		}
		sessionExecuteParseDuration.Observe(time.Since(startTS).Seconds())

		compiler := executor.Compiler{Ctx: s}
		for _, stmtNode := range stmtNodes {
			s.PrepareTxnCtx(goCtx)

			// Step2: Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
			startTS = time.Now()
			// Some executions are done in compile stage, so we reset them before compile.
			executor.ResetStmtCtx(s, stmtNode)
			stmt, err := compiler.Compile(goCtx, stmtNode)
			if err != nil {
				log.Warnf("[%d] compile error:\n%v\n%s", connID, err, sql)
				terror.Log(errors.Trace(s.RollbackTxn(goCtx)))
				return nil, errors.Trace(err)
			}
			sessionExecuteCompileDuration.Observe(time.Since(startTS).Seconds())

			// Step3: Cache the physical plan if possible.
			if plan.PlanCacheEnabled && stmt.Cacheable && len(stmtNodes) == 1 && !s.GetSessionVars().StmtCtx.HistogramsNotLoad() {
				plan.GlobalPlanCache.Put(cacheKey, plan.NewSQLCacheValue(stmtNode, stmt.Plan, stmt.Expensive))
			}

			// Step4: Execute the physical plan.
			if recordSets, err = s.executeStatement(goCtx, connID, stmtNode, stmt, recordSets); err != nil {
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

// PrepareStmt is used for executing prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error) {
	if s.sessionVars.TxnCtx.InfoSchema == nil {
		// We don't need to create a transaction for prepare statement, just get information schema will do.
		s.sessionVars.TxnCtx.InfoSchema = domain.GetDomain(s).InfoSchema()
	}
	prepareExec := executor.NewPrepareExec(s, executor.GetInfoSchema(s), sql)
	err = prepareExec.DoPrepare()
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.Fields, errors.Trace(err)
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
func (s *session) ExecutePreparedStmt(goCtx goctx.Context, stmtID uint32, args ...interface{}) (ast.RecordSet, error) {
	err := checkArgs(args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.PrepareTxnCtx(goCtx)
	st, err := executor.CompileExecutePreparedStmt(s, stmtID, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r, err := runStmt(goCtx, s, st)
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

func (s *session) Txn() kv.Transaction {
	return s.txn
}

func (s *session) NewTxn() error {
	if s.txn != nil && s.txn.Valid() {
		goCtx := goctx.TODO()
		err := s.CommitTxn(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	txn, err := s.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	s.txn = txn
	s.sessionVars.TxnCtx.StartTS = txn.StartTS()
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
	goCtx := goctx.TODO()
	if err := s.RollbackTxn(goCtx); err != nil {
		log.Error("session Close error:", errors.ErrorStack(err))
	}
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s *session) getPassword(name, host string) (string, error) {
	// Get password for name and host.
	authSQL := fmt.Sprintf("SELECT Password FROM %s.%s WHERE User='%s' and Host='%s';", mysql.SystemDB, mysql.UserTable, name, host)
	pwd, err := s.getExecRet(s, authSQL)
	if err == nil {
		return pwd, nil
	} else if !executor.ErrResultIsEmpty.Equal(err) {
		return "", errors.Trace(err)
	}
	//Try to get user password for name with any host(%).
	authSQL = fmt.Sprintf("SELECT Password FROM %s.%s WHERE User='%s' and Host='%%';", mysql.SystemDB, mysql.UserTable, name)
	pwd, err = s.getExecRet(s, authSQL)
	return pwd, errors.Trace(err)
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

	// Add statsUpdateHandle.
	if do.StatsHandle() != nil {
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

	s.SetValue(context.Initing, true)
	bootstrap(s)
	finishBootstrap(store)
	s.ClearValue(context.Initing)

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
	return s, nil
}

const (
	notBootstrapped         = 0
	currentBootstrapVersion = 16
)

func getStoreBootstrapVersion(store kv.Storage) int64 {
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
	storeBootstrapped[store.UUID()] = true

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
	variable.TiDBIndexSerialScanConcurrency + quoteCommaQuote +
	variable.TiDBDistSQLScanConcurrency + "')"

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(context.Initing) != nil {
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
		if _, ok := vars.Systems[varName]; !ok {
			err = varsutil.SetSessionSystemVar(s.sessionVars, varName, varVal)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	vars.CommonGlobalLoaded = true
	return nil
}

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	future oracle.Future
	store  kv.Storage
	span   opentracing.Span
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
	startTS, err := tf.future.Wait()
	tf.span.Finish()
	if err == nil {
		return tf.store.BeginWithStartTS(startTS)
	}

	// It would retry get timestamp.
	return tf.store.Begin()
}

func (s *session) getTxnFuture(ctx goctx.Context) *txnFuture {
	span, ctx := opentracing.StartSpanFromContext(ctx, "session.getTxnFuture")
	oracle := s.store.GetOracle()
	tsFuture := oracle.GetTimestampAsync(ctx)
	return &txnFuture{tsFuture, s.store, span}
}

// PrepareTxnCtx starts a goroutine to begin a transaction if needed, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *session) PrepareTxnCtx(ctx goctx.Context) {
	if s.txn != nil && s.txn.Valid() {
		return
	}
	if s.txnFuture != nil {
		return
	}

	s.txnFuture = s.getTxnFuture(ctx)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
	}
	if !s.sessionVars.IsAutocommit() {
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
	}
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(goCtx goctx.Context) error {
	if err := s.doCommit(goCtx); err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(s.NewTxn())
}

// ActivePendingTxn implements Context.ActivePendingTxn interface.
func (s *session) ActivePendingTxn() error {
	if s.txn != nil && s.txn.Valid() {
		return nil
	}
	if s.txnFuture == nil {
		return errors.New("transaction future is not set")
	}
	future := s.txnFuture
	s.txnFuture = nil
	txn, err := future.wait()
	if err != nil {
		return errors.Trace(err)
	}
	s.txn = txn
	s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return errors.Trace(err)
	}
	if s.sessionVars.Systems[variable.TxnIsolation] == ast.ReadCommitted {
		txn.SetOption(kv.IsolationLevel, kv.RC)
	}
	return nil
}

// InitTxnWithStartTS create a transaction with startTS.
func (s *session) InitTxnWithStartTS(startTS uint64) error {
	if s.txn != nil && s.txn.Valid() {
		return nil
	}
	if s.txnFuture == nil {
		return errors.New("transaction channel is not set")
	}
	// no need to get txn from txnFutureCh since txn should init with startTs
	s.txnFuture = nil
	var err error
	s.txn, err = s.store.BeginWithStartTS(startTS)
	if err != nil {
		return errors.Trace(err)
	}
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
	}
	return pi
}

// logCrucialStmt logs some crucial SQL including: CREATE USER/GRANT PRIVILEGE/CHANGE PASSWORD/DDL etc.
func logCrucialStmt(node ast.StmtNode, user *auth.UserIdentity) {
	switch stmt := node.(type) {
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateIndexStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropIndexStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt:
		if ss, ok := node.(ast.SensitiveStmtNode); ok {
			log.Infof("[CRUCIAL OPERATION] %s (by %s).", ss.SecureText(), user)
		} else {
			log.Infof("[CRUCIAL OPERATION] %s (by %s).", stmt.Text(), user)
		}
	}
}
