// Copyright 2019 PingCAP, Inc.
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

// +build !codes

package testkit

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/session"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util"
	"github.com/pingcap/tidb/v4/util/sqlexec"
)

type sessionCtxKeyType struct{}

var sessionKey = sessionCtxKeyType{}

func getSession(ctx context.Context) session.Session {
	s := ctx.Value(sessionKey)
	if s == nil {
		return nil
	}
	return s.(session.Session)
}

func setSession(ctx context.Context, se session.Session) context.Context {
	return context.WithValue(ctx, sessionKey, se)
}

// CTestKit is a utility to run sql test with concurrent execution support.
type CTestKit struct {
	c     *check.C
	store kv.Storage
}

// NewCTestKit returns a new *CTestKit.
func NewCTestKit(c *check.C, store kv.Storage) *CTestKit {
	return &CTestKit{
		c:     c,
		store: store,
	}
}

// OpenSession opens new session ctx if no exists one.
func (tk *CTestKit) OpenSession(ctx context.Context) context.Context {
	if getSession(ctx) == nil {
		se, err := session.CreateSession4Test(tk.store)
		tk.c.Assert(err, check.IsNil)
		id := atomic.AddUint64(&connectionID, 1)
		se.SetConnectionID(id)
		ctx = setSession(ctx, se)
	}
	return ctx
}

// OpenSessionWithDB opens new session ctx if no exists one and use db.
func (tk *CTestKit) OpenSessionWithDB(ctx context.Context, db string) context.Context {
	ctx = tk.OpenSession(ctx)
	tk.MustExec(ctx, "use "+db)
	return ctx
}

// CloseSession closes exists session from ctx.
func (tk *CTestKit) CloseSession(ctx context.Context) {
	se := getSession(ctx)
	tk.c.Assert(se, check.NotNil)
	se.Close()
}

// Exec executes a sql statement.
func (tk *CTestKit) Exec(ctx context.Context, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	var err error
	tk.c.Assert(getSession(ctx), check.NotNil)
	if len(args) == 0 {
		var rss []sqlexec.RecordSet
		rss, err = getSession(ctx).Execute(ctx, sql)
		if err == nil && len(rss) > 0 {
			return rss[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := getSession(ctx).PrepareStmt(sql)
	if err != nil {
		return nil, err
	}
	params := make([]types.Datum, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewDatum(args[i])
	}
	rs, err := getSession(ctx).ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, err
	}
	err = getSession(ctx).DropPreparedStmt(stmtID)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// CheckExecResult checks the affected rows and the insert id after executing MustExec.
func (tk *CTestKit) CheckExecResult(ctx context.Context, affectedRows, insertID int64) {
	tk.c.Assert(getSession(ctx), check.NotNil)
	tk.c.Assert(affectedRows, check.Equals, int64(getSession(ctx).AffectedRows()))
	tk.c.Assert(insertID, check.Equals, int64(getSession(ctx).LastInsertID()))
}

// MustExec executes a sql statement and asserts nil error.
func (tk *CTestKit) MustExec(ctx context.Context, sql string, args ...interface{}) {
	res, err := tk.Exec(ctx, sql, args...)
	tk.c.Assert(err, check.IsNil, check.Commentf("sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err)))
	if res != nil {
		tk.c.Assert(res.Close(), check.IsNil)
	}
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *CTestKit) MustQuery(ctx context.Context, sql string, args ...interface{}) *Result {
	comment := check.Commentf("sql:%s, args:%v", sql, args)
	rs, err := tk.Exec(ctx, sql, args...)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(rs, check.NotNil, comment)
	return tk.resultSetToResult(ctx, rs, comment)
}

// resultSetToResult converts ast.RecordSet to testkit.Result.
// It is used to check results of execute statement in binary mode.
func (tk *CTestKit) resultSetToResult(ctx context.Context, rs sqlexec.RecordSet, comment check.CommentInterface) *Result {
	rows, err := session.GetRows4Test(context.Background(), getSession(ctx), rs)
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

// ConcurrentRun run test in current.
// - concurrent: controls the concurrent worker count.
// - loops: controls run test how much times.
// - prepareFunc: provide test data and will be called for every loop.
// - checkFunc: used to do some check after all workers done.
// works like create table better be put in front of this method calling.
// see more example at TestBatchInsertWithOnDuplicate
func (tk *CTestKit) ConcurrentRun(c *check.C, concurrent int, loops int,
	prepareFunc func(ctx context.Context, tk *CTestKit, concurrent int, currentLoop int) [][][]interface{},
	writeFunc func(ctx context.Context, tk *CTestKit, input [][]interface{}),
	checkFunc func(ctx context.Context, tk *CTestKit)) {
	var (
		channel = make([]chan [][]interface{}, concurrent)
		ctxs    = make([]context.Context, concurrent)
		dones   = make([]context.CancelFunc, concurrent)
	)
	for i := 0; i < concurrent; i++ {
		w := i
		channel[w] = make(chan [][]interface{}, 1)
		ctxs[w], dones[w] = context.WithCancel(context.Background())
		ctxs[w] = tk.OpenSessionWithDB(ctxs[w], "test")
		go func() {
			defer func() {
				r := recover()
				if r != nil {
					c.Fatal(r, string(util.GetStack()))
				}
				dones[w]()
			}()
			for input := range channel[w] {
				writeFunc(ctxs[w], tk, input)
			}
		}()
	}
	defer func() {
		for i := 0; i < concurrent; i++ {
			tk.CloseSession(ctxs[i])
		}
	}()

	ctx := tk.OpenSessionWithDB(context.Background(), "test")
	defer tk.CloseSession(ctx)
	tk.MustExec(ctx, "use test")

	for j := 0; j < loops; j++ {
		datas := prepareFunc(ctx, tk, concurrent, j)
		for i := 0; i < concurrent; i++ {
			channel[i] <- datas[i]
		}
	}

	for i := 0; i < concurrent; i++ {
		close(channel[i])
	}

	for i := 0; i < concurrent; i++ {
		<-ctxs[i].Done()
	}
	checkFunc(ctx, tk)
}

// PermInt returns, as a slice of n ints, a pseudo-random permutation of the integers [0,n).
func (tk *CTestKit) PermInt(n int) []interface{} {
	randPermSlice := rand.Perm(n)
	v := make([]interface{}, 0, len(randPermSlice))
	for _, i := range randPermSlice {
		v = append(v, i)
	}
	return v
}

// IgnoreError ignores error and make errcheck tool happy.
// Deprecated: it's normal to ignore some error in concurrent test, but please don't use this method in other place.
func (tk *CTestKit) IgnoreError(_ error) {}
