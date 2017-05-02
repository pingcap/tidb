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
	"sort"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/testutil"
)

// TestKit is a utility to run sql test.
type TestKit struct {
	c     *check.C
	store kv.Storage
	Se    tidb.Session
}

// Result is the result returned by MustQuery.
type Result struct {
	rows    [][]string
	comment check.CommentInterface
	c       *check.C
}

// Check asserts the result equals the expected results.
func (res *Result) Check(expected [][]interface{}) {
	got := fmt.Sprintf("%s", res.rows)
	need := fmt.Sprintf("%s", expected)
	res.c.Assert(got, check.Equals, need, res.comment)
}

// Rows returns the result data.
func (res *Result) Rows() [][]interface{} {
	ifacesSlice := make([][]interface{}, len(res.rows))
	for i := range res.rows {
		ifaces := make([]interface{}, len(res.rows[i]))
		for j := range res.rows[i] {
			ifaces[j] = res.rows[i][j]
		}
		ifacesSlice[i] = ifaces
	}
	return ifacesSlice
}

// Sort sorts and return the result.
func (res *Result) Sort() *Result {
	sort.Slice(res.rows, func(i, j int) bool {
		a := res.rows[i]
		b := res.rows[j]
		for i := range a {
			if a[i] < b[i] {
				return true
			}
		}
		return false
	})
	return res
}

// NewTestKit returns a new *TestKit.
func NewTestKit(c *check.C, store kv.Storage) *TestKit {
	return &TestKit{
		c:     c,
		store: store,
	}
}

var connectionID uint64

// Exec executes a sql statement.
func (tk *TestKit) Exec(sql string, args ...interface{}) (ast.RecordSet, error) {
	var err error
	if tk.Se == nil {
		tk.Se, err = tidb.CreateSession(tk.store)
		tk.c.Assert(err, check.IsNil)
		id := atomic.AddUint64(&connectionID, 1)
		tk.Se.SetConnectionID(id)
		tk.Se.GetSessionVars().SkipDDLWait = true
	}
	if len(args) == 0 {
		var rss []ast.RecordSet
		rss, err = tk.Se.Execute(sql)
		if err == nil && len(rss) > 0 {
			return rss[0], nil
		}
		return nil, errors.Trace(err)
	}
	stmtID, _, _, err := tk.Se.PrepareStmt(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rs, err := tk.Se.ExecutePreparedStmt(stmtID, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = tk.Se.DropPreparedStmt(stmtID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rs, nil
}

// CheckExecResult checks the affected rows and the insert id after executing MustExec.
func (tk *TestKit) CheckExecResult(affectedRows, insertID int64) {
	tk.c.Assert(affectedRows, check.Equals, int64(tk.Se.AffectedRows()))
	tk.c.Assert(insertID, check.Equals, int64(tk.Se.LastInsertID()))
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
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(rs, check.NotNil, comment)
	rows, err := tidb.GetRows(rs)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	sRows := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
		iRow := make([]string, len(row))
		for j := range row {
			if row[j].IsNull() {
				iRow[j] = "<nil>"
			} else {
				iRow[j], err = row[j].ToString()
				tk.c.Assert(err, check.IsNil)
			}
		}
		sRows[i] = iRow
	}
	return &Result{rows: sRows, c: tk.c, comment: comment}
}

// Rows is similar to RowsWithSep, use white space as separator string.
func Rows(args ...string) [][]interface{} {
	return testutil.RowsWithSep(" ", args...)
}
