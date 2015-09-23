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
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/forupdate"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/stmt/stmts"
	"github.com/pingcap/tidb/util/errors2"
)

// Session context
type Session interface {
	Status() uint16                               // Flag of current status, such as autocommit
	LastInsertID() uint64                         // Last inserted auto_increment id
	AffectedRows() uint64                         // Affected rows by lastest executed stmt
	Execute(sql string) ([]rset.Recordset, error) // Execute a sql statement
	String() string                               // For debug
	FinishTxn(rollback bool) error
	// For execute prepare statement in binary protocol
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*field.ResultField, err error)
	// Execute a prepared statement
	ExecutePreparedStmt(stmtID uint32, param ...interface{}) (rset.Recordset, error)
	DropPreparedStmt(stmtID uint32) error
	SetClientCapability(uint32) // Set client capability flags
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
	st     stmt.Statement
	params []interface{}
}

type stmtHistory struct {
	history []*stmtRecord
}

func (h *stmtHistory) add(stmtID uint32, st stmt.Statement, params ...interface{}) {
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

type session struct {
	txn     kv.Transaction // Current transaction
	args    []interface{}  // Statment execution args, this should be cleaned up after exec
	values  map[fmt.Stringer]interface{}
	store   kv.Storage
	sid     int64
	history stmtHistory
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

func (s *session) FinishTxn(rollback bool) error {
	// transaction has already been committed or rolled back
	if s.txn == nil {
		return nil
	}
	defer func() {
		s.txn = nil
		variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()

	if rollback {
		return s.txn.Rollback()
	}

	err := s.txn.Commit()
	if err != nil {
		log.Warnf("txn:%s, %v", s.txn, err)
		return errors.Trace(err)
	}

	s.resetHistory()
	return nil
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

func needRetry(st stmt.Statement) bool {
	switch st.(type) {
	case *stmts.PreparedStmt, *stmts.ShowStmt, *stmts.DoStmt:
		return false
	default:
		return true
	}
}

func isPreparedStmt(st stmt.Statement) bool {
	switch st.(type) {
	case *stmts.PreparedStmt:
		return true
	default:
		return false
	}
}

func (s *session) Retry() error {
	nh := s.history.clone()
	defer func() {
		s.history.history = nh.history
	}()

	if forUpdate := s.Value(forupdate.ForUpdateKey); forUpdate != nil {
		return errors.Errorf("can not retry select for update statement")
	}

	var err error
	for {
		s.resetHistory()
		s.FinishTxn(true)
		success := true
		for _, sr := range nh.history {
			st := sr.st
			// Skip prepare statement
			if !needRetry(st) {
				continue
			}
			log.Warnf("Retry %s", st.OriginText())
			_, err = runStmt(s, st)
			if err != nil {
				if errors2.ErrorEqual(err, kv.ErrConditionNotMatch) {
					success = false
					break
				}
				log.Warnf("session:%v, err:%v", s, err)
				return errors.Trace(err)
			}
		}
		if success {
			break
		}
	}

	return nil
}

func (s *session) Execute(sql string) ([]rset.Recordset, error) {
	statements, err := Compile(sql)
	if err != nil {
		log.Errorf("Syntax error: %s", sql)
		log.Errorf("Error occurs at %s.", err)
		return nil, errors.Trace(err)
	}

	var rs []rset.Recordset

	for _, st := range statements {
		r, err := runStmt(s, st)
		if err != nil {
			log.Warnf("session:%v, err:%v", s, err)
			return nil, errors.Trace(err)
		}

		// Record executed query
		if isPreparedStmt(st) {
			ps := st.(*stmts.PreparedStmt)
			s.history.add(ps.ID, st)
		} else {
			s.history.add(0, st)
		}

		if r != nil {
			rs = append(rs, r)
		}
	}

	return rs, nil
}

// For execute prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*field.ResultField, err error) {
	return prepareStmt(s, sql)
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

// Execute a prepared statement
func (s *session) ExecutePreparedStmt(stmtID uint32, args ...interface{}) (rset.Recordset, error) {
	err := checkArgs(args...)
	if err != nil {
		return nil, err
	}

	st := &stmts.ExecuteStmt{ID: stmtID}
	s.history.add(stmtID, st, args...)
	return runStmt(s, st, args...)
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	return dropPreparedStmt(s, stmtID)
}

// If forceNew is true, GetTxn() must return a new transaction.
// In this situation, if current transaction is still in progress,
// there will be an implicit commit and create a new transaction.
func (s *session) GetTxn(forceNew bool) (kv.Transaction, error) {
	var err error
	if s.txn == nil {
		s.resetHistory()
		s.txn, err = s.store.Begin()
		if err != nil {
			return nil, err
		}
		if !variable.IsAutocommit(s) {
			variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
		log.Infof("New txn:%s in session:%d", s.txn, s.sid)
		return s.txn, nil
	}
	if forceNew {
		err = s.txn.Commit()
		variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, false)
		if err != nil {
			return nil, err
		}
		s.resetHistory()
		s.txn, err = s.store.Begin()
		if err != nil {
			return nil, err
		}
		if !variable.IsAutocommit(s) {
			variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
		log.Warnf("Force new txn:%s in session:%d", s.txn, s.sid)
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
	return s.FinishTxn(true)
}

func calcPassword(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
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
	authSQL := fmt.Sprintf("SELECT Password FROM %s.%s WHERE User=\"%s\" and Host=\"%s\";", mysql.SystemDB, mysql.UserTable, name, host)
	rs, err := s.Execute(authSQL)
	if err != nil {
		log.Warnf("Encounter error when auth user %s. Error: %v", user, err)
		return false
	}
	if len(rs) == 0 {
		return false
	}
	row, err := rs[0].Next()
	if err != nil {
		log.Warnf("Encounter error when auth user %s. Error: %v", user, err)
		return false
	}
	if row == nil || len(row.Data) == 0 {
		return false
	}
	pwd, ok := row.Data[0].(string)
	if !ok {
		return false
	}
	checkAuth := calcPassword(salt, []byte(pwd))
	if !bytes.Equal(auth, checkAuth) {
		return false
	}
	variable.GetSessionVars(s).SetCurrentUser(user)
	return true
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (Session, error) {
	s := &session{
		values: make(map[fmt.Stringer]interface{}),
		store:  store,
		sid:    atomic.AddInt64(&sessionID, 1),
	}
	domain, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	sessionctx.BindDomain(s, domain)

	variable.BindSessionVars(s)
	variable.GetSessionVars(s).SetStatusFlag(mysql.ServerStatusAutocommit, true)
	sessionMu.Lock()
	defer sessionMu.Unlock()
	_, ok := storeBootstrapped[store.UUID()]
	if !ok {
		bootstrap(s)
		storeBootstrapped[store.UUID()] = true
	}
	// Add auth here
	return s, nil
}
