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
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !codes

package testkit

import (
	"context"
	"fmt"
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

var testKitIDGenerator atomic.Uint64

// TestKit is a utility to run sql test.
type TestKit struct {
	require *require.Assertions
	assert  *assert.Assertions
	store   kv.Storage
	session session.Session
}

// NewTestKit returns a new *TestKit.
func NewTestKit(t *testing.T, store kv.Storage) *TestKit {
	return &TestKit{
		require: require.New(t),
		assert:  assert.New(t),
		store:   store,
		session: newSession(t, store),
	}
}

// Session return a session
func (tk *TestKit) Session() session.Session {
	return tk.session
}

// MustExec executes a sql statement and asserts nil error.
func (tk *TestKit) MustExec(sql string, args ...interface{}) {
	res, err := tk.Exec(sql, args...)
	comment := fmt.Sprintf("sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err))
	tk.require.Nil(err, comment)

	if res != nil {
		tk.require.Nil(res.Close())
	}
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *TestKit) MustQuery(sql string, args ...interface{}) *Result {
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	rs, err := tk.Exec(sql, args...)
	tk.require.NoError(err, comment)
	tk.require.NotNil(rs, comment)
	return tk.ResultSetToResult(rs, comment)
}

// ResultSetToResult converts sqlexec.RecordSet to testkit.Result.
// It is used to check results of execute statement in binary mode.
func (tk *TestKit) ResultSetToResult(rs sqlexec.RecordSet, comment string) *Result {
	return tk.ResultSetToResultWithCtx(context.Background(), rs, comment)
}

// ResultSetToResultWithCtx converts sqlexec.RecordSet to testkit.Result.
func (tk *TestKit) ResultSetToResultWithCtx(ctx context.Context, rs sqlexec.RecordSet, comment string) *Result {
	rows, err := session.ResultSetToStringSlice(ctx, tk.session, rs)
	tk.require.NoError(err, comment)
	return &Result{rows: rows, comment: comment, assert: tk.assert, require: tk.require}
}

// Exec executes a sql statement using the prepared stmt API
func (tk *TestKit) Exec(sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	ctx := context.Background()
	if len(args) == 0 {
		sc := tk.session.GetSessionVars().StmtCtx
		prevWarns := sc.GetWarnings()
		stmts, err := tk.session.Parse(ctx, sql)
		if err != nil {
			return nil, errors.Trace(err)
		}
		warns := sc.GetWarnings()
		parserWarns := warns[len(prevWarns):]
		var rs0 sqlexec.RecordSet
		for i, stmt := range stmts {
			rs, err := tk.session.ExecuteStmt(ctx, stmt)
			if i == 0 {
				rs0 = rs
			}
			if err != nil {
				tk.session.GetSessionVars().StmtCtx.AppendError(err)
				return nil, errors.Trace(err)
			}
		}
		if len(parserWarns) > 0 {
			tk.session.GetSessionVars().StmtCtx.AppendWarnings(parserWarns)
		}
		return rs0, nil
	}

	stmtID, _, _, err := tk.session.PrepareStmt(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	params := make([]types.Datum, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewDatum(args[i])
	}
	rs, err := tk.session.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = tk.session.DropPreparedStmt(stmtID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rs, nil
}

func newSession(t *testing.T, store kv.Storage) session.Session {
	se, err := session.CreateSession4Test(store)
	require.Nil(t, err)
	se.SetConnectionID(testKitIDGenerator.Inc())
	return se
}
