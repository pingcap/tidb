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
	"encoding/json"
	"fmt"
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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// Session context
type Session interface {
	Status() uint16                               // Flag of current status, such as autocommit
	LastInsertID() uint64                         // Last inserted auto_increment id
	AffectedRows() uint64                         // Affected rows by lastest executed stmt
	Execute(sql string) ([]rset.Recordset, error) // Execute a sql statement
	SetUsername(name string)                      // Current user name
	String() string                               // For debug
	FinishTxn(rollback bool) error
	// For execute prepare statement in binary protocol
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*field.ResultField, err error)
	// Execute a prepared statement
	ExecutePreparedStmt(stmtID uint32, param ...interface{}) (rset.Recordset, error)
	DropPreparedStmt(stmtID uint32) error
	SetClientCapability(uint32) // Set client capability flags
	Close() error
}

var (
	_         Session = (*session)(nil)
	sessionID int64
)

type session struct {
	txn      kv.Transaction // Current transaction
	userName string
	args     []interface{} // Statment execution args, this should be cleaned up after exec

	values map[fmt.Stringer]interface{}
	store  kv.Storage
	sid    int64
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

func (s *session) SetUsername(name string) {
	s.userName = name
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
	}()

	if rollback {
		return s.txn.Rollback()
	}

	err := s.txn.Commit()
	if err != nil {
		log.Errorf("txn:%s, %v", s.txn, err)
	}

	return errors.Trace(err)
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	data := map[string]interface{}{
		"userName":   s.userName,
		"currDBName": db.GetCurrentSchema(s),
		"sid":        s.sid,
		"txn":        s.txn.String(),
	}

	b, _ := json.MarshalIndent(data, "", "  ")
	return string(b)
}

func (s *session) Execute(sql string) ([]rset.Recordset, error) {
	stmts, err := Compile(sql)
	if err != nil {
		log.Errorf("Syntax error: %s", sql)
		log.Errorf("Error occurs at %s.", err)
		return nil, errors.Trace(err)
	}

	var rs []rset.Recordset

	for _, si := range stmts {
		r, err := runStmt(s, si)
		if err != nil {
			log.Warnf("session:%v, err:%v", s, err)
			return nil, errors.Trace(err)
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

func checkArgs(args ...interface{}) error {
	for i, v := range args {
		switch v.(type) {
		case bool:
			// We do not handle bool as int8 in tidb.
			vv, err := types.ToBool(v)
			if err != nil {
				return errors.Trace(err)
			}
			args[i] = vv
		case nil, float32, float64, string,
			int8, int16, int32, int64, int,
			uint8, uint16, uint32, uint64, uint,
			[]byte, time.Duration, time.Time:
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
	//convert args to param
	rs, err := executePreparedStmt(s, stmtID, args...)
	if err != nil {
		return nil, err
	}
	return rs, nil
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
		s.txn, err = s.store.Begin()
		if err != nil {
			return nil, err
		}

		log.Warnf("New txn:%s in session:%d", s.txn, s.sid)
		return s.txn, nil
	}
	if forceNew {
		err = s.txn.Commit()
		if err != nil {
			return nil, err
		}
		s.txn, err = s.store.Begin()
		if err != nil {
			return nil, err
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
	variable.GetSessionVars(s).SetStatus(mysql.ServerStatusAutocommit)
	return s, nil
}
