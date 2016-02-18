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

package testkit

import (
	"fmt"
	"strings"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/rset"
)

// TestKit is a utility to run sql test.
type TestKit struct {
	c     *check.C
	store kv.Storage
	Se    tidb.Session
}

// Result is the result returned by MustQuery.
type Result struct {
	rows    [][]interface{}
	comment check.CommentInterface
	c       *check.C
}

// Check asserts the result equals the expected results.
func (res *Result) Check(expected [][]interface{}) {
	got := fmt.Sprintf("%v", res.rows)
	need := fmt.Sprintf("%v", expected)
	res.c.Assert(got, check.Equals, need, res.comment)
}

// Rows returns the result data.
func (res *Result) Rows() [][]interface{} {
	return res.rows
}

// NewTestKit returns a new *TestKit.
func NewTestKit(c *check.C, store kv.Storage) *TestKit {
	return &TestKit{
		c:     c,
		store: store,
	}
}

// Exec executes a sql statement.
func (tk *TestKit) Exec(sql string, args ...interface{}) (rset.Recordset, error) {
	var err error
	if tk.Se == nil {
		tk.Se, err = tidb.CreateSession(tk.store)
		tk.c.Assert(err, check.IsNil)
	}
	if len(args) == 0 {
		var rss []rset.Recordset
		rss, err = tk.Se.Execute(sql)
		if err == nil && len(rss) > 0 {
			return rss[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := tk.Se.PrepareStmt(sql)
	if err != nil {
		return nil, err
	}
	rs, err := tk.Se.ExecutePreparedStmt(stmtID, args...)
	if err != nil {
		return nil, err
	}
	err = tk.Se.DropPreparedStmt(stmtID)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// MustExec executes a sql statement and asserts nil error.
func (tk *TestKit) MustExec(sql string, args ...interface{}) {
	_, err := tk.Exec(sql, args...)
	tk.c.Assert(err, check.IsNil, check.Commentf("sql:%s, %v", sql, args))
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *TestKit) MustQuery(sql string, args ...interface{}) *Result {
	comment := check.Commentf("sql:%s, %v", sql, args)
	rs, err := tk.Exec(sql, args...)
	tk.c.Assert(err, check.IsNil, comment)
	tk.c.Assert(rs, check.NotNil, comment)
	rows, err := rs.Rows(-1, 0)
	tk.c.Assert(err, check.IsNil, comment)
	return &Result{rows: rows, c: tk.c, comment: comment}
}

// RowsWithSep is a convenient function to wrap args to a slice of []interface.
// The arg represents a row, split by sep.
func RowsWithSep(sep string, args ...string) [][]interface{} {
	rows := make([][]interface{}, len(args))
	for i, v := range args {
		strs := strings.Split(v, sep)
		row := make([]interface{}, len(strs))
		for j, s := range strs {
			row[j] = s
		}
		rows[i] = row
	}
	return rows
}

// Rows is similar to RowsWithSep, use white space as seperator string.
func Rows(args ...string) [][]interface{} {
	return RowsWithSep(" ", args...)
}
