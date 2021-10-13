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
// +build !codes

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
