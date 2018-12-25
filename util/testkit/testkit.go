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
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testutil"
)

// TestKit is a utility to run sql test.
type TestKit struct {
	c     *check.C
	store kv.Storage
	Se    session.Session
}

// Result is the result returned by MustQuery.
type Result struct {
	rows    [][]string
	comment check.CommentInterface
	c       *check.C
}

// Check asserts the result equals the expected results.
func (res *Result) Check(expected [][]interface{}) {
	resBuff := bytes.NewBufferString("")
	for _, row := range res.rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	needBuff := bytes.NewBufferString("")
	for _, row := range expected {
		fmt.Fprintf(needBuff, "%s\n", row)
	}
	res.c.Assert(resBuff.String(), check.Equals, needBuff.String(), res.comment)
}

// CheckAt asserts the result of selected columns equals the expected results.
func (res *Result) CheckAt(cols []int, expected [][]interface{}) {
	for _, e := range expected {
		res.c.Assert(len(cols), check.Equals, len(e))
	}

	rows := make([][]string, 0, len(expected))
	for i := range res.rows {
		row := make([]string, 0, len(cols))
		for _, r := range cols {
			row = append(row, res.rows[i][r])
		}
		rows = append(rows, row)
	}
	got := fmt.Sprintf("%s", rows)
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
			} else if a[i] > b[i] {
				return false
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

// NewTestKitWithInit returns a new *TestKit and creates a session.
func NewTestKitWithInit(c *check.C, store kv.Storage) *TestKit {
	tk := NewTestKit(c, store)
	// Use test and prepare a session.
	tk.MustExec("use test")
	return tk
}

var connectionID uint64

// Exec executes a sql statement.
func (tk *TestKit) Exec(sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	var err error
	if tk.Se == nil {
		tk.Se, err = session.CreateSession4Test(tk.store)
		tk.c.Assert(err, check.IsNil)
		id := atomic.AddUint64(&connectionID, 1)
		tk.Se.SetConnectionID(id)
	}
	ctx := context.Background()
	if len(args) == 0 {
		var rss []sqlexec.RecordSet
		rss, err = tk.Se.Execute(ctx, sql)
		if err == nil && len(rss) > 0 {
			return rss[0], nil
		}
		return nil, errors.Trace(err)
	}
	stmtID, _, _, err := tk.Se.PrepareStmt(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rs, err := tk.Se.ExecutePreparedStmt(ctx, stmtID, args...)
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

// CheckLastMessage checks last message after executing MustExec
func (tk *TestKit) CheckLastMessage(msg string) {
	tk.c.Assert(tk.Se.LastMessage(), check.Equals, msg)
}

// MustExec executes a sql statement and asserts nil error.
func (tk *TestKit) MustExec(sql string, args ...interface{}) {
	res, err := tk.Exec(sql, args...)
	tk.c.Assert(err, check.IsNil, check.Commentf("sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err)))
	if res != nil {
		tk.c.Assert(res.Close(), check.IsNil)
	}
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *TestKit) MustQuery(sql string, args ...interface{}) *Result {
	comment := check.Commentf("sql:%s, args:%v", sql, args)
	rs, err := tk.Exec(sql, args...)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(rs, check.NotNil, comment)
	return tk.ResultSetToResult(rs, comment)
}

// ExecToErr executes a sql statement and discard results.
func (tk *TestKit) ExecToErr(sql string, args ...interface{}) error {
	res, err := tk.Exec(sql, args...)
	if res != nil {
		tk.c.Assert(res.Close(), check.IsNil)
	}
	return err
}

// ResultSetToResult converts sqlexec.RecordSet to testkit.Result.
// It is used to check results of execute statement in binary mode.
func (tk *TestKit) ResultSetToResult(rs sqlexec.RecordSet, comment check.CommentInterface) *Result {
	rows, err := session.GetRows4Test(context.Background(), tk.Se, rs)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	err = rs.Close()
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	sRows := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
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
