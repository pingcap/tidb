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
	"sync/atomic"
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
	sessionID int64
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
	sid         int64
	history     stmtHistory
	initing     bool // Running bootstrap using this session.
	maxRetryCnt int  // Max retry times. If maxRetryCnt <=0, there is no limitation for retry times.

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
		log.Errorf("rollback txn failed, err:%v", errors.ErrorStack(err1))
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
		bin := binloginfo.GetPrewriteValue(s, false)
		if bin != nil {
			binlogData, err := bin.Marshal()
			if err != nil {
				return errors.Trace(err)
			}
			s.txn.SetOption(kv.BinlogData, binlogData)
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
	data := map[string]interface{}{
		"currDBName": db.GetCurrentSchema(s),
		"sid":        s.sid,
	}

	if s.txn != nil {
		// if txn is committed or rolled back, txn is nil.
		data["txn"] = s.txn.String()
	}

	b, _ := json.MarshalIndent(data, "", "  ")
	return string(b)
}

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
			log.Warnf("Retry %s", st.OriginText())
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
		return nil, errors.New("Wrong number of statement.")
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
	if s.initing {
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
func (s *session) isAutocommit(ctx context.Context) (bool, error) {
	sessionVar := variable.GetSessionVars(ctx)
	autocommit := sessionVar.GetSystemVar("autocommit")
	if autocommit.IsNull() {
		if s.initing {
			return false, nil
		}
		autocommitStr, err := s.GetGlobalSysVar(ctx, "autocommit")
		if err != nil {
			return false, errors.Trace(err)
		}
		autocommit.SetString(autocommitStr)
		err = sessionVar.SetSystemVar("autocommit", autocommit)
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	autocommitStr := autocommit.GetString()
	if autocommitStr == "ON" || autocommitStr == "on" || autocommitStr == "1" {
		variable.GetSessionVars(ctx).SetStatusFlag(mysql.ServerStatusAutocommit, true)
		return true, nil
	}
	variable.GetSessionVars(ctx).SetStatusFlag(mysql.ServerStatusAutocommit, false)
	return false, nil
}

func (s *session) ShouldAutocommit(ctx context.Context) (bool, error) {
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK.
	ac, err := s.isAutocommit(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if variable.GetSessionVars(ctx).Status&mysql.ServerStatusInTrans == 0 && ac {
		return true, nil
	}
	return false, nil
}

func (s *session) ParseSQL(sql, charset, collation string) ([]ast.StmtNode, error) {
	return s.parser.Parse(sql, charset, collation)
}

func (s *session) Execute(sql string) ([]ast.RecordSet, error) {
	if err := s.checkSchemaValidOrRollback(); err != nil {
		return nil, errors.Trace(err)
	}
	charset, collation := getCtxCharsetInfo(s)
	rawStmts, err := s.ParseSQL(sql, charset, collation)
	if err != nil {
		log.Warnf("compiling %s, error: %v", sql, err)
		return nil, errors.Trace(err)
	}

	var rs []ast.RecordSet
	ph := sessionctx.GetDomain(s).PerfSchema()
	for i, rst := range rawStmts {
		st, err1 := Compile(s, rst)
		if err1 != nil {
			log.Errorf("Syntax error: %s", sql)
			log.Errorf("Error occurs at %s.", err1)
			return nil, errors.Trace(err1)
		}
		id := variable.GetSessionVars(s).ConnectionID
		s.stmtState = ph.StartStatement(sql, id, perfschema.CallerNameSessionExecute, rawStmts[i])
		r, err := runStmt(s, st)
		ph.EndStatement(s.stmtState)
		if err != nil {
			log.Warnf("session:%v, err:%v", s, err)
			return nil, errors.Trace(err)
		}
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
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.ResultFields, prepareExec.Err
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
	if s.txn == nil {
		s.resetHistory()
		s.txn, err = s.store.Begin()
		if err != nil {
			return nil, errors.Trace(err)
		}
		ac, err = s.isAutocommit(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ac {
			variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
		log.Infof("New txn:%s in session:%d", s.txn, s.sid)
	} else if forceNew {
		err = s.CommitTxn()
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.txn, err = s.store.Begin()
		if err != nil {
			return nil, errors.Trace(err)
		}
		ac, err = s.isAutocommit(s)
		if !ac {
			variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		log.Warnf("Force new txn:%s in session:%d", s.txn, s.sid)
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
	log.Info("RollbackTxn for session close.")
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
		sid:         atomic.AddInt64(&sessionID, 1),
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

	ok := isBootstrapped(store)
	if !ok {
		// if no bootstrap and storage is remote, we must use a little lease time to
		// bootstrap quickly, after bootstrapped, we will reset the lease time.
		// TODO: Using a bootstap tool for doing this may be better later.
		if !localstore.IsLocalStore(store) {
			sessionctx.GetDomain(s).SetLease(chooseMinLease(100*time.Millisecond, schemaLease))
		}

		s.initing = true
		bootstrap(s)
		s.initing = false

		if !localstore.IsLocalStore(store) {
			sessionctx.GetDomain(s).SetLease(schemaLease)
		}

		finishBootstrap(store)
	}

	// TODO: Add auth here
	privChecker := &privileges.UserPrivileges{}
	privilege.BindPrivilegeChecker(s, privChecker)
	return s, nil
}

func isBootstrapped(store kv.Storage) bool {
	// check in memory
	_, ok := storeBootstrapped[store.UUID()]
	if ok {
		return true
	}

	// check in kv store
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		var err error
		t := meta.NewMeta(txn)
		ok, err = t.IsBootstrapped()
		return errors.Trace(err)
	})

	if err != nil {
		log.Fatalf("check bootstrapped err %v", err)
	}

	if ok {
		// here mean memory is not ok, but other server has already finished it
		storeBootstrapped[store.UUID()] = true
	}

	return ok
}

func finishBootstrap(store kv.Storage) {
	storeBootstrapped[store.UUID()] = true

	err := kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.FinishBootstrap()
		return errors.Trace(err)
	})
	if err != nil {
		log.Fatalf("finish bootstrap err %v", err)
	}
}
