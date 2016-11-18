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
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/forupdate"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-binlog"
)

// Session context
type Session interface {
	Status() uint16                               // Flag of current status, such as autocommit.
	LastInsertID() uint64                         // Last inserted auto_increment id.
	AffectedRows() uint64                         // Affected rows by latest executed stmt.
	SetValue(key fmt.Stringer, value interface{}) // SetValue saves a value associated with this session for key.
	Value(key fmt.Stringer) interface{}           // Value returns the value associated with this session for key.
	Execute(sql string) ([]ast.RecordSet, error)  // Execute a sql statement.
	String() string                               // For debug
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
	Retry() error
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

func (h *stmtHistory) reset() {
	if len(h.history) > 0 {
		h.history = h.history[:0]
	}
}

func (h *stmtHistory) clone() *stmtHistory {
	nh := *h
	nh.history = make([]*stmtRecord, len(h.history))
	copy(nh.history, h.history)
	return &nh
}

const unlimitedRetryCnt = -1

type session struct {
	txn         kv.Transaction // Current transaction
	values      map[fmt.Stringer]interface{}
	store       kv.Storage
	history     stmtHistory
	maxRetryCnt int // Max retry times. If maxRetryCnt <=0, there is no limitation for retry times.

	debugInfos map[string]interface{} // Vars for debug and unit tests.

	// For performance_schema only.
	stmtState *perfschema.StatementState
	parser    *parser.Parser
}

func (s *session) cleanRetryInfo() {
	if !variable.GetSessionVars(s).RetryInfo.Retrying {
		variable.GetSessionVars(s).RetryInfo.Clean()
	}
}

// If the schema is invalid, we need to rollback the current transaction.
func (s *session) checkSchemaValidOrRollback() error {
	var ts uint64
	if s.txn != nil {
		ts = s.txn.StartTS()
	}
	err := sessionctx.GetDomain(s).SchemaValidity.Check(ts)
	if err == nil {
		return nil
	}

	if err1 := s.RollbackTxn(); err1 != nil {
		// TODO: handle this error.
		log.Errorf("[%d] rollback txn failed, err:%v", variable.GetSessionVars(s).ConnectionID, errors.ErrorStack(err1))
		errMsg := fmt.Sprintf("schema is invalid, rollback txn err:%v", err1.Error())
		return domain.ErrLoadSchemaTimeOut.Gen(errMsg)
	}
	return errors.Trace(err)
}

func (s *session) Status() uint16 {
	return variable.GetSessionVars(s).Status
}

func (s *session) LastInsertID() uint64 {
	return variable.GetSessionVars(s).LastInsertID
}

func (s *session) AffectedRows() uint64 {
	return variable.GetSessionVars(s).AffectedRows
}

func (s *session) resetHistory() {
	s.ClearValue(forupdate.ForUpdateKey)
	s.history.reset()
}

func (s *session) SetClientCapability(capability uint32) {
	variable.GetSessionVars(s).ClientCapability = capability
}

func (s *session) SetConnectionID(connectionID uint64) {
	variable.GetSessionVars(s).ConnectionID = connectionID
}

func (s *session) finishTxn(rollback bool) error {
	// transaction has already been committed or rolled back
	if s.txn == nil {
		return nil
	}
	defer func() {
		s.ClearValue(executor.DirtyDBKey)
		s.txn = nil
		variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, false)
		binloginfo.ClearBinlog(s)
	}()

	if rollback {
		s.resetHistory()
		s.cleanRetryInfo()
		return s.txn.Rollback()
	}
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
	err := s.txn.Commit()
	if err != nil {
		if !variable.GetSessionVars(s).RetryInfo.Retrying && kv.IsRetryableError(err) {
			err = s.Retry()
		}
		if err != nil {
			log.Warnf("txn:%s, %v", s.txn, err)
			return errors.Trace(err)
		}
	}

	s.resetHistory()
	s.cleanRetryInfo()
	return nil
}

func (s *session) CommitTxn() error {
	if err := s.checkSchemaValidOrRollback(); err != nil {
		return errors.Trace(err)
	}

	return s.finishTxn(false)
}

func (s *session) RollbackTxn() error {
	return s.finishTxn(true)
}

func (s *session) GetClient() kv.Client {
	return s.store.GetClient()
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := variable.GetSessionVars(s)
	data := map[string]interface{}{
		"id":         sessVars.ConnectionID,
		"user":       sessVars.User,
		"currDBName": db.GetCurrentSchema(s),
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

func (s *session) Retry() error {
	variable.GetSessionVars(s).RetryInfo.Retrying = true
	nh := s.history.clone()
	// Debug infos.
	if len(nh.history) == 0 {
		s.debugInfos[retryEmptyHistoryList] = true
	} else {
		s.debugInfos[retryEmptyHistoryList] = false
	}
	defer func() {
		s.history.history = nh.history
		variable.GetSessionVars(s).RetryInfo.Retrying = false
	}()

	if forUpdate := s.Value(forupdate.ForUpdateKey); forUpdate != nil {
		return errors.Errorf("can not retry select for update statement")
	}
	var err error
	retryCnt := 0
	for {
		variable.GetSessionVars(s).RetryInfo.Attempts = retryCnt + 1
		s.resetHistory()
		log.Info("RollbackTxn for retry txn.")
		err = s.RollbackTxn()
		if err != nil {
			// TODO: handle this error.
			log.Errorf("rollback txn failed, err:%v", errors.ErrorStack(err))
		}
		success := true
		variable.GetSessionVars(s).RetryInfo.ResetOffset()
		for _, sr := range nh.history {
			st := sr.st
			txt := st.OriginText()
			if len(txt) > sqlLogMaxLen {
				txt = txt[:sqlLogMaxLen]
			}
			log.Warnf("Retry %s (len:%d)", txt, len(st.OriginText()))
			_, err = runStmt(s, st)
			if err != nil {
				if kv.IsRetryableError(err) {
					success = false
					break
				}
				log.Warnf("session:%v, err:%v", s, err)
				return errors.Trace(err)
			}
		}
		if success {
			err = s.CommitTxn()
			if !kv.IsRetryableError(err) {
				break
			}
		}
		retryCnt++
		if (s.maxRetryCnt != unlimitedRetryCnt) && (retryCnt >= s.maxRetryCnt) {
			return errors.Trace(err)
		}
		kv.BackOff(retryCnt)
	}
	return err
}

// ExecRestrictedSQL implements SQLHelper interface.
// This is used for executing some restricted sql statements.
func (s *session) ExecRestrictedSQL(ctx context.Context, sql string) (ast.RecordSet, error) {
	if err := s.checkSchemaValidOrRollback(); err != nil {
		return nil, errors.Trace(err)
	}
	charset, collation := getCtxCharsetInfo(s)
	rawStmts, err := s.ParseSQL(sql, charset, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rawStmts) != 1 {
		log.Errorf("ExecRestrictedSQL only executes one statement. Too many/few statement in %s", sql)
		return nil, errors.New("wrong number of statement")
	}
	st, err := Compile(s, rawStmts[0])
	if err != nil {
		log.Errorf("Compile %s with error: %v", sql, err)
		return nil, errors.Trace(err)
	}
	// Check statement for some restrictions.
	// For example only support DML on system meta table.
	// TODO: Add more restrictions.
	log.Debugf("Executing %s [%s]", st.OriginText(), sql)
	sessVar := variable.GetSessionVars(ctx)
	sessVar.InRestrictedSQL = true
	rs, err := st.Exec(ctx)
	sessVar.InRestrictedSQL = false
	return rs, errors.Trace(err)
}

// getExecRet executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getExecRet(ctx context.Context, sql string) (string, error) {
	cleanTxn := s.txn == nil
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
	}
	return value, nil
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (s *session) GetGlobalSysVar(ctx context.Context, name string) (string, error) {
	if ctx.Value(context.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name)
	sysVar, err := s.getExecRet(ctx, sql)
	if err != nil {
		if terror.ExecResultIsEmpty.Equal(err) {
			return "", variable.UnknownSystemVar.Gen("unknown sys variable:%s", name)
		}
		return "", errors.Trace(err)
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(ctx context.Context, name string, value string) error {
	sql := fmt.Sprintf(`UPDATE  %s.%s SET VARIABLE_VALUE="%s" WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, value, strings.ToLower(name))
	_, err := s.ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// IsAutocommit checks if it is in the auto-commit mode.
func (s *session) isAutocommit(ctx context.Context) bool {
	sessionVar := variable.GetSessionVars(ctx)
	return sessionVar.GetStatusFlag(mysql.ServerStatusAutocommit)
}

func (s *session) ShouldAutocommit(ctx context.Context) bool {
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK.
	sessVar := variable.GetSessionVars(ctx)
	isAutomcommit := sessVar.GetStatusFlag(mysql.ServerStatusAutocommit)
	inTransaction := sessVar.GetStatusFlag(mysql.ServerStatusInTrans)
	return isAutomcommit && !inTransaction
}

func (s *session) ParseSQL(sql, charset, collation string) ([]ast.StmtNode, error) {
	return s.parser.Parse(sql, charset, collation)
}

func (s *session) Execute(sql string) ([]ast.RecordSet, error) {
	if err := s.checkSchemaValidOrRollback(); err != nil {
		return nil, errors.Trace(err)
	}
	startTS := time.Now()
	charset, collation := getCtxCharsetInfo(s)
	connID := variable.GetSessionVars(s).ConnectionID
	rawStmts, err := s.ParseSQL(sql, charset, collation)
	if err != nil {
		log.Warnf("[%d] parse error:\n%v\n%s", connID, err, sql)
		return nil, errors.Trace(err)
	}
	sessionExecuteParseDuration.Observe(time.Since(startTS).Seconds())

	var rs []ast.RecordSet
	ph := sessionctx.GetDomain(s).PerfSchema()
	for i, rst := range rawStmts {
		startTS := time.Now()
		st, err1 := Compile(s, rst)
		if err1 != nil {
			log.Warnf("[%d] compile error:\n%v\n%s", connID, err1, sql)
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

	if variable.GetSessionVars(s).ClientCapability&mysql.ClientMultiResults == 0 && len(rs) > 1 {
		// return the first recordset if client doesn't support ClientMultiResults.
		rs = rs[:1]
	}
	return rs, nil
}

// For execute prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error) {
	if err := s.checkSchemaValidOrRollback(); err != nil {
		return 0, 0, nil, errors.Trace(err)
	}
	prepareExec := &executor.PrepareExec{
		IS:      sessionctx.GetDomain(s).InfoSchema(),
		Ctx:     s,
		SQLText: sql,
	}
	prepareExec.DoPrepare()
	return prepareExec.ID, prepareExec.ParamCount, nil, prepareExec.Err
}

// checkArgs makes sure all the arguments' types are known and can be handled.
// integer types are converted to int64 and uint64, time.Time is converted to mysql.Time.
// time.Duration is converted to mysql.Duration, other known types are leaved as it is.
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
			args[i] = mysql.Duration{Duration: x}
		case time.Time:
			args[i] = mysql.Time{Time: x, Type: mysql.TypeDatetime}
		case nil:
		default:
			return errors.Errorf("cannot use arg[%d] (type %T):unsupported type", i, v)
		}
	}
	return nil
}

// ExecutePreparedStmt executes a prepared statement.
func (s *session) ExecutePreparedStmt(stmtID uint32, args ...interface{}) (ast.RecordSet, error) {
	if err := s.checkSchemaValidOrRollback(); err != nil {
		return nil, errors.Trace(err)
	}
	err := checkArgs(args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	st := executor.CompileExecutePreparedStmt(s, stmtID, args...)
	r, err := runStmt(s, st, args...)
	return r, errors.Trace(err)
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	if err := s.checkSchemaValidOrRollback(); err != nil {
		return errors.Trace(err)
	}
	vars := variable.GetSessionVars(s)
	if _, ok := vars.PreparedStmts[stmtID]; !ok {
		return executor.ErrStmtNotFound
	}
	delete(vars.PreparedStmts, stmtID)
	return nil
}

// If forceNew is true, GetTxn() must return a new transaction.
// In this situation, if current transaction is still in progress,
// there will be an implicit commit and create a new transaction.
func (s *session) GetTxn(forceNew bool) (kv.Transaction, error) {
	var (
		err error
		ac  bool
	)
	sessVars := variable.GetSessionVars(s)
	if s.txn == nil {
		err = s.loadCommonGlobalVariablesIfNeeded()
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.resetHistory()
		s.txn, err = s.store.Begin()
		if err != nil {
			return nil, errors.Trace(err)
		}
		ac = s.isAutocommit(s)
		if !ac {
			variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
		log.Infof("[%d] new txn:%s", sessVars.ConnectionID, s.txn)
	} else if forceNew {
		err = s.CommitTxn()
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.txn, err = s.store.Begin()
		if err != nil {
			return nil, errors.Trace(err)
		}
		ac = s.isAutocommit(s)
		if !ac {
			variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
		log.Warnf("[%d] force new txn:%s", sessVars.ConnectionID, s.txn)
	}
	retryInfo := variable.GetSessionVars(s).RetryInfo
	if retryInfo.Retrying {
		s.txn.SetOption(kv.RetryAttempts, retryInfo.Attempts)
	}
	return s.txn, nil
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
	variable.GetSessionVars(s).SetCurrentUser(user)
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
	s := &session{
		values:      make(map[fmt.Stringer]interface{}),
		store:       store,
		debugInfos:  make(map[string]interface{}),
		maxRetryCnt: 10,
		parser:      parser.New(),
	}
	domain, err := domap.Get(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sessionctx.BindDomain(s, domain)

	variable.BindSessionVars(s)
	variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusAutocommit, true)

	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	variable.BindGlobalVarAccessor(s, s)

	// session implements autocommit.Checker. Bind it to ctx
	autocommit.BindAutocommitChecker(s, s)
	sessionMu.Lock()
	defer sessionMu.Unlock()

	ver := getStoreBootstrapVersion(store)
	if ver == notBootstrapped {
		// if no bootstrap and storage is remote, we must use a little lease time to
		// bootstrap quickly, after bootstrapped, we will reset the lease time.
		// TODO: Using a bootstap tool for doing this may be better later.
		if !localstore.IsLocalStore(store) {
			sessionctx.GetDomain(s).SetLease(chooseMinLease(100*time.Millisecond, schemaLease))
		}

		s.SetValue(context.Initing, true)
		bootstrap(s)
		s.ClearValue(context.Initing)

		if !localstore.IsLocalStore(store) {
			sessionctx.GetDomain(s).SetLease(schemaLease)
		}

		finishBootstrap(store)
	} else if ver < currentBootstrapVersion {
		s.SetValue(context.Initing, true)
		upgrade(s)
		s.ClearValue(context.Initing)
	}

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
	variable.DistSQLScanConcurrencyVar + "')"

// LoadCommonGlobalVariableIfNeeded loads and applies commonly used global variables for the session
// right before creating a transaction for the first time.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	vars := variable.GetSessionVars(s)
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
		if d := vars.GetSystemVar(varName); d.IsNull() {
			vars.SetSystemVar(varName, row.Data[1])
		}
	}
	vars.CommonGlobalLoaded = true
	return nil
}
