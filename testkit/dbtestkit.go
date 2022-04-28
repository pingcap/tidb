// Copyright 2021 PingCAP, Inc.
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

//go:build !codes

package testkit

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DBTestKit is a utility to run sql with a db connection.
type DBTestKit struct {
	require *require.Assertions
	assert  *assert.Assertions
	db      *sql.DB
}

// NewDBTestKit returns a new *DBTestKit.
func NewDBTestKit(t *testing.T, db *sql.DB) *DBTestKit {
	return &DBTestKit{
		require: require.New(t),
		assert:  assert.New(t),
		db:      db,
	}
}

// MustPrepare creates a prepared statement for later queries or executions.
func (tk *DBTestKit) MustPrepare(query string) *sql.Stmt {
	stmt, err := tk.db.Prepare(query)
	tk.require.NoErrorf(err, "Prepare %s", query)
	return stmt
}

// MustExecPrepared executes a prepared statement with the given arguments and
// returns a Result summarizing the effect of the statement.
func (tk *DBTestKit) MustExecPrepared(stmt *sql.Stmt, args ...interface{}) sql.Result {
	res, err := stmt.Exec(args...)
	tk.require.NoErrorf(err, "Execute prepared with args: %s", args)
	return res
}

// MustQueryPrepared executes a prepared query statement with the given arguments
// and returns the query results as a *Rows.
func (tk *DBTestKit) MustQueryPrepared(stmt *sql.Stmt, args ...interface{}) *sql.Rows {
	rows, err := stmt.Query(args...)
	tk.require.NoErrorf(err, "Query prepared with args: %s", args)
	return rows
}

// MustExec query the statements and returns the result.
func (tk *DBTestKit) MustExec(sql string, args ...interface{}) sql.Result {
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	rs, err := tk.db.Exec(sql, args...)
	tk.require.NoError(err, comment)
	tk.require.NotNil(rs, comment)
	return rs
}

// MustQuery query the statements and returns result rows.
func (tk *DBTestKit) MustQuery(sql string, args ...interface{}) *sql.Rows {
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	rows, err := tk.db.Query(sql, args...)
	tk.require.NoError(err, comment)
	tk.require.NotNil(rows, comment)
	return rows
}

// MustQueryRows query the statements
func (tk *DBTestKit) MustQueryRows(query string, args ...interface{}) {
	rows := tk.MustQuery(query, args...)
	tk.require.True(rows.Next())
	tk.require.NoError(rows.Err())
	rows.Close()
}

// GetDB returns the underlay sql.DB instance.
func (tk *DBTestKit) GetDB() *sql.DB {
	return tk.db
}
