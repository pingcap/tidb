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
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

var asyncTestKitIDGenerator atomic.Uint64

// AsyncTestKit is a utility to run sql concurrently.
type AsyncTestKit struct {
	require *require.Assertions
	assert  *assert.Assertions
	store   kv.Storage
}

// NewAsyncTestKit returns a new *AsyncTestKit.
func NewAsyncTestKit(t *testing.T, store kv.Storage) *AsyncTestKit {
	return &AsyncTestKit{
		require: require.New(t),
		assert:  assert.New(t),
		store:   store,
	}
}

// OpenSession opens new session ctx if no exists one and use db.
func (tk *AsyncTestKit) OpenSession(ctx context.Context, db string) context.Context {
	if TryRetrieveSession(ctx) == nil {
		se, err := session.CreateSession4Test(tk.store)
		tk.require.NoError(err)
		se.SetConnectionID(asyncTestKitIDGenerator.Inc())
		ctx = context.WithValue(ctx, sessionKey, se)
	}
	tk.MustExec(ctx, fmt.Sprintf("use %s", db))
	return ctx
}

// CloseSession closes exists session from ctx.
func (tk *AsyncTestKit) CloseSession(ctx context.Context) {
	se := TryRetrieveSession(ctx)
	tk.require.NotNil(se)
	se.Close()
}

// GetStack gets the stacktrace.
func GetStack() []byte {
	const size = 4096
	buf := make([]byte, size)
	stackSize := runtime.Stack(buf, false)
	buf = buf[:stackSize]
	return buf
}

// ConcurrentRun run test in current.
// - concurrent: controls the concurrent worker count.
// - loops: controls run test how much times.
// - prepareFunc: provide test data and will be called for every loop.
// - checkFunc: used to do some check after all workers done.
// works like create table better be put in front of this method calling.
// see more example at TestBatchInsertWithOnDuplicate
func (tk *AsyncTestKit) ConcurrentRun(
	concurrent int,
	loops int,
	prepareFunc func(ctx context.Context, tk *AsyncTestKit, concurrent int, currentLoop int) [][][]interface{},
	writeFunc func(ctx context.Context, tk *AsyncTestKit, input [][]interface{}),
	checkFunc func(ctx context.Context, tk *AsyncTestKit),
) {
	channel := make([]chan [][]interface{}, concurrent)
	contextList := make([]context.Context, concurrent)
	doneList := make([]context.CancelFunc, concurrent)

	for i := 0; i < concurrent; i++ {
		w := i
		channel[w] = make(chan [][]interface{}, 1)
		contextList[w], doneList[w] = context.WithCancel(context.Background())
		contextList[w] = tk.OpenSession(contextList[w], "test")
		go func() {
			defer func() {
				r := recover()
				tk.require.Nil(r, string(GetStack()))
				doneList[w]()
			}()

			for input := range channel[w] {
				writeFunc(contextList[w], tk, input)
			}
		}()
	}

	defer func() {
		for i := 0; i < concurrent; i++ {
			tk.CloseSession(contextList[i])
		}
	}()

	ctx := tk.OpenSession(context.Background(), "test")
	defer tk.CloseSession(ctx)
	tk.MustExec(ctx, "use test")

	for j := 0; j < loops; j++ {
		data := prepareFunc(ctx, tk, concurrent, j)
		for i := 0; i < concurrent; i++ {
			channel[i] <- data[i]
		}
	}

	for i := 0; i < concurrent; i++ {
		close(channel[i])
	}

	for i := 0; i < concurrent; i++ {
		<-contextList[i].Done()
	}
	checkFunc(ctx, tk)
}

// Exec executes a sql statement.
func (tk *AsyncTestKit) Exec(ctx context.Context, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	se := TryRetrieveSession(ctx)
	tk.require.NotNil(se)

	if len(args) == 0 {
		rss, err := se.Execute(ctx, sql)
		if err == nil && len(rss) > 0 {
			return rss[0], nil
		}
		return nil, err
	}

	stmtID, _, _, err := se.PrepareStmt(sql)
	if err != nil {
		return nil, err
	}

	params := make([]types.Datum, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewDatum(args[i])
	}

	rs, err := se.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, err
	}

	err = se.DropPreparedStmt(stmtID)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// MustExec executes a sql statement and asserts nil error.
func (tk *AsyncTestKit) MustExec(ctx context.Context, sql string, args ...interface{}) {
	res, err := tk.Exec(ctx, sql, args...)
	tk.require.NoErrorf(err, "sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err))
	if res != nil {
		tk.require.NoError(res.Close())
	}
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *AsyncTestKit) MustQuery(ctx context.Context, sql string, args ...interface{}) *Result {
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	rs, err := tk.Exec(ctx, sql, args...)
	tk.require.NoError(err, comment)
	tk.require.NotNil(rs, comment)
	return tk.resultSetToResult(ctx, rs, comment)
}

// resultSetToResult converts ast.RecordSet to testkit.Result.
// It is used to check results of execute statement in binary mode.
func (tk *AsyncTestKit) resultSetToResult(ctx context.Context, rs sqlexec.RecordSet, comment string) *Result {
	rows, err := session.GetRows4Test(context.Background(), TryRetrieveSession(ctx), rs)
	tk.require.NoError(err, comment)

	err = rs.Close()
	tk.require.NoError(err, comment)

	result := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
		resultRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				resultRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				resultRow[j], err = d.ToString()
				tk.require.NoError(err, comment)
			}
		}
		result[i] = resultRow
	}
	return &Result{rows: result, comment: comment, assert: tk.assert, require: tk.require}
}

type sessionCtxKeyType struct{}

var sessionKey = sessionCtxKeyType{}

// TryRetrieveSession tries retrieve session from context.
func TryRetrieveSession(ctx context.Context) session.Session {
	s := ctx.Value(sessionKey)
	if s == nil {
		return nil
	}
	return s.(session.Session)
}
