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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-binlog"
)

// Session context
type Session interface {
	context.Context
	Status() uint16                              // Flag of current status, such as autocommit.
	LastInsertID() uint64                        // Last inserted auto_increment id.
	AffectedRows() uint64                        // Affected rows by latest executed stmt.
	Execute(sql string) ([]ast.RecordSet, error) // Execute a sql statement.
	String() string                              // For debug
	CommitTxn() error
	RollbackTxn() error
	// For execute prepare statement in binary protocol.
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// Execute a prepared statement.
	ExecutePreparedStmt(stmtID uint32, param ...interface{}) (ast.RecordSet, error)
	DropPreparedStmt(stmtID uint32) error
	SetClientCapability(uint32) // Set client capability flags.
	SetConnectionID(uint64)
	Close() error
	Auth(user string, auth []byte, salt []byte) bool
}

var (
	_         Session = (*session)(nil)
	sessionMu sync.Mutex
)

type stmtRecord struct {
	stmtID uint32
	st     ast.Statement
	params []interface{}
}

type stmtHistory struct {
	history []*stmtRecord
}

func (h *stmtHistory) add(stmtID uint32, st ast.Statement, params ...interface{}) {
	s := &stmtRecord{
		stmtID: stmtID,
		st:     st,
		params: append(([]interface{})(nil), params...),
	}
	h.history = append(h.history, s)
}

type session struct {
	txn    kv.Transaction // current transaction
	txnCh  chan *txnWithErr
	values map[fmt.Stringer]interface{}
	store  kv.Storage

	// Used for test only.
	unlimitedRetryCount bool

	// For performance_schema only.
	stmtState *perfschema.StatementState
	parser    *parser.Parser

	sessionVars *variable.SessionVars
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
	return s.sessionVars.LastInsertID
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

type schemaLeaseChecker struct {
	domain.SchemaValidator
	schemaVer int64
}

const (
	schemaOutOfDateRetryInterval = 500 * time.Millisecond
	schemaOutOfDateRetryTimes    = 10
)

func (s *schemaLeaseChecker) Check(txnTS uint64) error {
	for i := 0; i < schemaOutOfDateRetryTimes; i++ {
		err := s.checkOnce(txnTS)
		switch err {
		case nil:
			return nil
		case domain.ErrInfoSchemaChanged:
			schemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return errors.Trace(err)
		default:
			schemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
			time.Sleep(schemaOutOfDateRetryInterval)
		}
	}
	return domain.ErrInfoSchemaExpired
}

func (s *schemaLeaseChecker) checkOnce(txnTS uint64) error {
	succ := s.SchemaValidator.Check(txnTS, s.schemaVer)
	if !succ {
		if s.SchemaValidator.Latest() > s.schemaVer {
			return domain.ErrInfoSchemaChanged
		}
		return domain.ErrInfoSchemaExpired
	}
	return nil
}

func (s *session) doCommit() error {
	if s.txn == nil || !s.txn.Valid() {
		return nil
	}
	defer func() {
		s.txn = nil
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()
	if binloginfo.PumpClient != nil {
		prewriteValue := binloginfo.GetPrewriteValue(s, false)
		if prewriteValue != nil {
			prewriteData, err := prewriteValue.Marshal()
			if err != nil {
				return errors.Trace(err)
			}
			bin := &binlog.Binlog{
				Tp:            binlog.BinlogType_Prewrite,
				PrewriteValue: prewriteData,
			}
			s.txn.SetOption(kv.BinlogData, bin)
		}
	}

	// Set this option for 2 phase commit to validate schema lease.
	s.txn.SetOption(kv.SchemaLeaseChecker, &schemaLeaseChecker{
		SchemaValidator: sessionctx.GetDomain(s).SchemaValidator,
		schemaVer:       s.sessionVars.TxnCtx.SchemaVersion,
	})
	if err := s.txn.Commit(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *session) doCommitWithRetry() error {
	var txnSize int
	if s.txn != nil && s.txn.Valid() {
		txnSize = s.txn.Size()
	}
	err := s.doCommit()
	if err != nil {
		if s.isRetryableError(err) {
			// Transactions will retry 2 ~ 10 times.
			// We make larger transactions retry less times to prevent cluster resource outage.
			txnSizeRate := float64(txnSize) / float64(kv.BufferSizeLimit)
			maxRetryCount := 10 - int(txnSizeRate*9.0)
			err = s.retry(maxRetryCount)
		}
	}
	s.cleanRetryInfo()
	if err != nil {
		log.Warnf("[%d] finished txn:%s, %v", s.sessionVars.ConnectionID, s.txn, err)
		return errors.Trace(err)
	}
	return nil
}

func (s *session) CommitTxn() error {
	return s.doCommitWithRetry()
}

func (s *session) RollbackTxn() error {
	var err error
	if s.txn != nil && s.txn.Valid() {
		err = s.txn.Rollback()
	}
	s.cleanRetryInfo()
	s.txn = nil
	s.txnCh = nil
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
		"stauts":     sessVars.Status,
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
	b, _ := json.MarshalIndent(data, "", "  ")
	return string(b)
}

const sqlLogMaxLen = 1024

func (s *session) isRetryableError(err error) bool {
	return kv.IsRetryableError(err) || terror.ErrorEqual(err, domain.ErrInfoSchemaChanged)
}

func (s *session) retry(maxCnt int) error {
	connID := s.sessionVars.ConnectionID
	if s.sessionVars.TxnCtx.ForUpdate {
		return errors.Errorf("[%d] can not retry select for update statement", connID)
	}
	s.sessionVars.RetryInfo.Retrying = true
	retryCnt := 0
	defer func() {
		s.sessionVars.RetryInfo.Retrying = false
		sessionRetry.Observe(float64(retryCnt))
	}()
	nh := getHistory(s)
	var err error
	for {
		s.prepareTxnCtx()
		s.sessionVars.RetryInfo.ResetOffset()
		for _, sr := range nh.history {
			st := sr.st
			txt := st.OriginText()
			log.Warnf("[%d] Retry %s", connID, sqlForLog(txt))
			_, err = st.Exec(s)
			if err != nil {
				break
			}
		}
		if err == nil {
			err = s.doCommit()
			if err == nil {
				break
			}
		}
		if !s.isRetryableError(err) {
			log.Warnf("[%d] session:%v, err:%v", connID, s, err)
			return errors.Trace(err)
		}
		retryCnt++
		if !s.unlimitedRetryCount && (retryCnt >= maxCnt) {
			log.Warnf("[%id] Retry reached max count %d", connID, retryCnt)
			return errors.Trace(err)
		}
		kv.BackOff(retryCnt)
	}
	return err
}

func sqlForLog(sql string) string {
	if len(sql) > sqlLogMaxLen {
		return sql[:sqlLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	return sql
}

// ExecRestrictedSQL implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements, usually executed during a normal statement execution.
// Unlike normal Exec, it doesn't reset statement status, doesn't commit or rollback the current transaction
// and doesn't write binlog.
func (s *session) ExecRestrictedSQL(ctx context.Context, sql string) (ast.RecordSet, error) {
	s.prepareTxnCtx()
	charset, collation := s.sessionVars.GetCharsetInfo()
	rawStmts, err := s.ParseSQL(sql, charset, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rawStmts) != 1 {
		log.Errorf("ExecRestrictedSQL only executes one statement. Too many/few statement in %s", sql)
		return nil, errors.New("wrong number of statement")
	}
	// Some execution is done in compile stage, so we reset it before compile.
	st, err := Compile(s, rawStmts[0])
	if err != nil {
		log.Errorf("Compile %s with error: %v", sql, err)
		return nil, errors.Trace(err)
	}
	// Check statement for some restrictions.
	// For example only support DML on system meta table.
	// TODO: Add more restrictions.
	log.Debugf("Executing %s [%s]", st.OriginText(), sql)
	s.sessionVars.InRestrictedSQL = true
	rs, err := st.Exec(ctx)
	s.sessionVars.InRestrictedSQL = false
	return rs, errors.Trace(err)
}

// getExecRet executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getExecRet(ctx context.Context, sql string) (string, error) {
	cleanTxn := s.txn == nil && s.txnCh == nil
	rs, err := s.ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rs.Close()
	row, err := rs.Next()
	if err != nil {
		return "", errors.Trace(err)
	}
	if row == nil {
		return "", terror.ExecResultIsEmpty
	}
	value, err := types.ToString(row.Data[0].GetValue())
	if err != nil {
		return "", errors.Trace(err)
	}
	if cleanTxn {
		// This function has some side effect. Run select may create new txn.
		// We should make environment unchanged.
		s.txn = nil
		s.txnCh = nil
	}
	return value, nil
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
		if terror.ExecResultIsEmpty.Equal(err) {
			return "", variable.UnknownSystemVar.GenByArgs(name)
		}
		return "", errors.Trace(err)
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(name string, value string) error {
	sql := fmt.Sprintf(`UPDATE  %s.%s SET VARIABLE_VALUE="%s" WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, value, strings.ToLower(name))
	_, err := s.ExecRestrictedSQL(s, sql)
	return errors.Trace(err)
}

func (s *session) ParseSQL(sql, charset, collation string) ([]ast.StmtNode, error) {
	return s.parser.Parse(sql, charset, collation)
}

func (s *session) Execute(sql string) ([]ast.RecordSet, error) {
	s.prepareTxnCtx()
	startTS := time.Now()
	charset, collation := s.sessionVars.GetCharsetInfo()
	connID := s.sessionVars.ConnectionID
	rawStmts, err := s.ParseSQL(sql, charset, collation)
	if err != nil {
		log.Warnf("[%d] parse error:\n%v\n%s", connID, err, sql)
		return nil, errors.Trace(err)
	}
	sessionExecuteParseDuration.Observe(time.Since(startTS).Seconds())

	var rs []ast.RecordSet
	ph := sessionctx.GetDomain(s).PerfSchema()
	for i, rst := range rawStmts {
		s.prepareTxnCtx()
		startTS := time.Now()
		// Some execution is done in compile stage, so we reset it before compile.
		resetStmtCtx(s, rawStmts[0])
		st, err1 := Compile(s, rst)
		if err1 != nil {
			log.Warnf("[%d] compile error:\n%v\n%s", connID, err1, sql)
			s.RollbackTxn()
			return nil, errors.Trace(err1)
		}
		sessionExecuteCompileDuration.Observe(time.Since(startTS).Seconds())

		s.stmtState = ph.StartStatement(sql, connID, perfschema.CallerNameSessionExecute, rawStmts[i])
		s.SetValue(context.QueryString, st.OriginText())

		startTS = time.Now()
		r, err := runStmt(s, st)
		ph.EndStatement(s.stmtState)
		if err != nil {
			log.Warnf("[%d] session error:\n%v\n%s", connID, err, s)
			return nil, errors.Trace(err)
		}
		sessionExecuteRunDuration.Observe(time.Since(startTS).Seconds())
		if r != nil {
			rs = append(rs, r)
		}
	}

	if s.sessionVars.ClientCapability&mysql.ClientMultiResults == 0 && len(rs) > 1 {
		// return the first recordset if client doesn't support ClientMultiResults.
		rs = rs[:1]
	}
	return rs, nil
}

// For execute prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error) {
	if s.sessionVars.TxnCtx.InfoSchema == nil {
		// We don't need to create a transaction for prepare statement, just get information schema will do.
		s.sessionVars.TxnCtx.InfoSchema = sessionctx.GetDomain(s).InfoSchema()
	}
	prepareExec := &executor.PrepareExec{
		IS:      executor.GetInfoSchema(s),
		Ctx:     s,
		SQLText: sql,
	}
	prepareExec.DoPrepare()
	return prepareExec.ID, prepareExec.ParamCount, nil, prepareExec.Err
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
func (s *session) ExecutePreparedStmt(stmtID uint32, args ...interface{}) (ast.RecordSet, error) {
	err := checkArgs(args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.prepareTxnCtx()
	st := executor.CompileExecutePreparedStmt(s, stmtID, args...)
	r, err := runStmt(s, st)
	return r, errors.Trace(err)
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	vars := s.sessionVars
	if _, ok := vars.PreparedStmts[stmtID]; !ok {
		return executor.ErrStmtNotFound
	}
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
}

// If forceNew is true, GetTxn() must return a new transaction.
// In this situation, if current transaction is still in progress,
// there will be an implicit commit and create a new transaction.
func (s *session) GetTxn(forceNew bool) (kv.Transaction, error) {
	if s.txn != nil && !forceNew {
		return s.txn, nil
	}

	var err error
	var force string
	if s.txn == nil {
		err = s.loadCommonGlobalVariablesIfNeeded()
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else if forceNew {
		err = s.CommitTxn()
		if err != nil {
			return nil, errors.Trace(err)
		}
		force = "force"
	}
	s.txn, err = s.store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ac := s.sessionVars.IsAutocommit()
	if !ac {
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
	}
	log.Infof("[%d] %s new txn:%s", s.sessionVars.ConnectionID, force, s.txn)
	return s.txn, nil
}

func (s *session) Txn() kv.Transaction {
	return s.txn
}

func (s *session) NewTxn() error {
	if s.txn != nil && s.txn.Valid() {
		err := s.doCommitWithRetry()
		if err != nil {
			return errors.Trace(err)
		}
	}
	txn, err := s.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	s.txn = txn
	return nil
}

func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	s.values[key] = value
}

func (s *session) Value(key fmt.Stringer) interface{} {
	value := s.values[key]
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	delete(s.values, key)
}

// Close function does some clean work when session end.
func (s *session) Close() error {
	return s.RollbackTxn()
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
	} else if !terror.ExecResultIsEmpty.Equal(err) {
		return "", errors.Trace(err)
	}
	//Try to get user password for name with any host(%).
	authSQL = fmt.Sprintf("SELECT Password FROM %s.%s WHERE User='%s' and Host='%%';", mysql.SystemDB, mysql.UserTable, name)
	pwd, err = s.getExecRet(s, authSQL)
	return pwd, errors.Trace(err)
}

func (s *session) Auth(user string, auth []byte, salt []byte) bool {
	strs := strings.Split(user, "@")
	if len(strs) != 2 {
		log.Warnf("Invalid format for user: %s", user)
		return false
	}
	// Get user password.
	name := strs[0]
	host := strs[1]
	pwd, err := s.getPassword(name, host)
	if err != nil {
		if terror.ExecResultIsEmpty.Equal(err) {
			log.Errorf("User [%s] not exist %v", name, err)
		} else {
			log.Errorf("Get User [%s] password from SystemDB error %v", name, err)
		}
		return false
	}
	if len(pwd) != 0 && len(pwd) != 40 {
		log.Errorf("User [%s] password from SystemDB not like a sha1sum", name)
		return false
	}
	hpwd, err := util.DecodePassword(pwd)
	if err != nil {
		log.Errorf("Decode password string error %v", err)
		return false
	}
	checkAuth := util.CalcPassword(salt, hpwd)
	if !bytes.Equal(auth, checkAuth) {
		return false
	}
	s.sessionVars.User = user
	return true
}

// Some vars name for debug.
const (
	retryEmptyHistoryList = "RetryEmptyHistoryList"
)

func chooseMinLease(n1 time.Duration, n2 time.Duration) time.Duration {
	if n1 <= n2 {
		return n1
	}
	return n2
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (Session, error) {
	ver := getStoreBootstrapVersion(store)
	if ver == notBootstrapped {
		runInBootstrapSession(store, bootstrap)
	} else if ver < currentBootstrapVersion {
		runInBootstrapSession(store, upgrade)
	}

	return createSession(store)
}

// runInBootstrapSession create a special session for boostrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, bootstrap func(Session)) {
	saveLease := schemaLease
	if !localstore.IsLocalStore(store) {
		schemaLease = chooseMinLease(schemaLease, 100*time.Millisecond)
	}
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

	domain := sessionctx.GetDomain(s)
	domain.Close()
	domap.Delete(store)
}

func createSession(store kv.Storage) (*session, error) {
	s := &session{
		values:      make(map[fmt.Stringer]interface{}),
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	domain, err := domap.Get(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sessionctx.BindDomain(s, domain)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s

	// TODO: Add auth here
	privChecker := &privileges.UserPrivileges{}
	privilege.BindPrivilegeChecker(s, privChecker)
	return s, nil
}

const (
	notBootstrapped         = 0
	currentBootstrapVersion = 3
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

const loadCommonGlobalVarsSQL = "select * from mysql.global_variables where variable_name in ('" +
	variable.AutocommitVar + "', '" +
	variable.SQLModeVar + "', '" +
	variable.DistSQLJoinConcurrencyVar + "', '" +
	variable.MaxAllowedPacket + "', '" +
	variable.DistSQLScanConcurrencyVar + "')"

// LoadCommonGlobalVariableIfNeeded loads and applies commonly used global variables for the session.
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
	rs, err := s.ExecRestrictedSQL(s, loadCommonGlobalVarsSQL)
	if err != nil {
		vars.CommonGlobalLoaded = false
		log.Errorf("Failed to load common global variables.")
		return errors.Trace(err)
	}
	for {
		row, err1 := rs.Next()
		if err1 != nil {
			vars.CommonGlobalLoaded = false
			log.Errorf("Failed to load common global variables.")
			return errors.Trace(err1)
		}
		if row == nil {
			break
		}
		varName := row.Data[0].GetString()
		if _, ok := vars.Systems[varName]; !ok {
			varsutil.SetSessionSystemVar(s.sessionVars, varName, row.Data[1])
		}
	}
	vars.CommonGlobalLoaded = true
	return nil
}

type txnWithErr struct {
	txn kv.Transaction
	err error
}

// prepareTxnCtx starts a goroutine to begin a transaction if needed, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *session) prepareTxnCtx() {
	if s.txn != nil && s.txn.Valid() {
		return
	}
	if s.txnCh != nil {
		return
	}
	txnCh := make(chan *txnWithErr, 1)
	go func() {
		txn, err := s.store.Begin()
		txnCh <- &txnWithErr{txn: txn, err: err}
	}()
	s.txnCh = txnCh
	is := sessionctx.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
	}
	if !s.sessionVars.IsAutocommit() {
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
	}
}

// ActivePendingTxn implements Session.ActivePendingTxn interface.
func (s *session) ActivePendingTxn() error {
	if s.txn != nil && s.txn.Valid() {
		return nil
	}
	if s.txnCh == nil {
		return errors.New("transaction channel is not set")
	}
	txnWithErr := <-s.txnCh
	s.txnCh = nil
	if txnWithErr.err != nil {
		return errors.Trace(txnWithErr.err)
	}
	s.txn = txnWithErr.txn
	err := s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
