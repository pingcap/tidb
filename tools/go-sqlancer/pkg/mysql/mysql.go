// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/pingcap/log"
)

// DBConnect wraps db
type DBConnect struct {
	sync.Mutex
	dsn   string
	db    *sql.DB
	txn   *sql.Tx
	begin time.Time
}

// DBAccessor can be txn snapshot or db it self
type DBAccessor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// MustExec must execute sql or fatal
func (conn *DBConnect) MustExec(query string, args ...interface{}) sql.Result {
	r, err := conn.db.Exec(query, args...)
	if err != nil {
		log.Error(fmt.Sprintf("exec %s err %v", query, err))
	}
	return r
}

// Exec execute sql
func (conn *DBConnect) Exec(query string, args ...interface{}) (sql.Result, error) {
	conn.Lock()
	defer conn.Unlock()
	return conn.GetDBAccessor().Exec(query, args...)
}

// Query execute select statement
func (conn *DBConnect) Query(query string, args ...interface{}) (*sql.Rows, error) {
	conn.Lock()
	defer conn.Unlock()
	return conn.GetDBAccessor().Query(query, args...)
}

// GetDB get real db object
func (conn *DBConnect) GetDB() *sql.DB {
	return conn.db
}

// GetDBAccessor get DBAccessor interface
func (conn *DBConnect) GetDBAccessor() DBAccessor {
	if conn.txn != nil {
		return conn.txn
	}
	return conn.db
}

// IfTxn show if in a transaction
func (conn *DBConnect) IfTxn() bool {
	return conn.txn != nil
}

// GetTiDBTS get the txn begin timestamp from TiDB
func (conn *DBConnect) GetTiDBTS() (uint64, error) {
	conn.Lock()
	defer conn.Unlock()

	if !conn.IfTxn() {
		return 0, nil
	}

	var tso uint64
	if err := conn.GetDBAccessor().QueryRow("SELECT @@TIDB_CURRENT_TS").Scan(&tso); err != nil {
		return 0, err
	}
	return tso, nil
}

// GetBeginTime get the begin time of a transaction
// if not in transaction, return 0
func (conn *DBConnect) GetBeginTime() time.Time {
	conn.Lock()
	defer conn.Unlock()
	if conn.IfTxn() {
		return conn.begin
	}
	return time.Time{}
}

// Begin a transaction
func (conn *DBConnect) Begin() error {
	conn.Lock()
	defer conn.Unlock()
	if conn.txn != nil {
		return nil
	}
	txn, err := conn.db.Begin()
	if err != nil {
		return err
	}
	conn.txn = txn
	conn.begin = time.Now()
	return nil
}

// Commit a transaction
func (conn *DBConnect) Commit() error {
	conn.Lock()
	defer func() {
		conn.txn = nil
		conn.Unlock()
	}()
	if conn.txn == nil {
		return nil
	}
	return conn.txn.Commit()
}

// Rollback a transaction
func (conn *DBConnect) Rollback() error {
	conn.Lock()
	defer func() {
		conn.txn = nil
		conn.Unlock()
	}()
	if conn.txn == nil {
		return nil
	}
	return conn.txn.Rollback()
}

// CloseDB turn off db connection
func (conn *DBConnect) CloseDB() error {
	return conn.db.Close()
}

// ReConnect rebuild connection
func (conn *DBConnect) ReConnect() error {
	if err := conn.CloseDB(); err != nil {
		return err
	}
	db, err := sql.Open("mysql", conn.dsn)
	if err != nil {
		return err
	}
	conn.db = db
	return nil
}

// RunWithRetry tries to run func in specified count
func RunWithRetry(ctx context.Context, retryCnt int, interval time.Duration, f func() error) error {
	var (
		err error
	)
	for i := 0; retryCnt < 0 || i < retryCnt; i++ {
		err = f()
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
	return err
}

// OpenDB opens db
func OpenDB(dsn string, maxIdleConns int) (*DBConnect, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdleConns)
	// log.Info("DB opens successfully")
	return &DBConnect{
		db:  db,
		dsn: dsn,
	}, nil
}

// IsErrDupEntry returns true if error code = 1062
func IsErrDupEntry(err error) bool {
	return isMySQLError(err, 1062)
}

func isMySQLError(err error, code uint16) bool {
	err = originError(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

// originError return original error
func originError(err error) error {
	for {
		e := errors.Cause(err)
		if e == err {
			break
		}
		err = e
	}
	return err
}
