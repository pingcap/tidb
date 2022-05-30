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
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
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
	t       testing.TB
	store   kv.Storage
	session session.Session
}

// NewTestKit returns a new *TestKit.
func NewTestKit(t testing.TB, store kv.Storage) *TestKit {
	return &TestKit{
		require: require.New(t),
		assert:  assert.New(t),
		t:       t,
		store:   store,
		session: newSession(t, store),
	}
}

// NewTestKitWithSession returns a new *TestKit.
func NewTestKitWithSession(t testing.TB, store kv.Storage, se session.Session) *TestKit {
	return &TestKit{
		require: require.New(t),
		assert:  assert.New(t),
		t:       t,
		store:   store,
		session: se,
	}
}

// RefreshSession set a new session for the testkit
func (tk *TestKit) RefreshSession() {
	tk.session = newSession(tk.t, tk.store)
}

// SetSession set the session of testkit
func (tk *TestKit) SetSession(session session.Session) {
	tk.session = session
}

// Session return the session associated with the testkit
func (tk *TestKit) Session() session.Session {
	return tk.session
}

// MustExec executes a sql statement and asserts nil error.
func (tk *TestKit) MustExec(sql string, args ...interface{}) {
	res, err := tk.Exec(sql, args...)
	comment := fmt.Sprintf("sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err))
	tk.require.NoError(err, comment)

	if res != nil {
		tk.require.NoError(res.Close())
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

// MustIndexLookup checks whether the plan for the sql is IndexLookUp.
func (tk *TestKit) MustIndexLookup(sql string, args ...interface{}) *Result {
	tk.require.True(tk.HasPlan(sql, "IndexLookUp", args...))
	return tk.MustQuery(sql, args...)
}

// MustPartition checks if the result execution plan must read specific partitions.
func (tk *TestKit) MustPartition(sql string, partitions string, args ...interface{}) *Result {
	rs := tk.MustQuery("explain "+sql, args...)
	ok := len(partitions) == 0
	for i := range rs.rows {
		if len(partitions) == 0 && strings.Contains(rs.rows[i][3], "partition:") {
			ok = false
		}
		if len(partitions) != 0 && strings.Compare(rs.rows[i][3], "partition:"+partitions) == 0 {
			ok = true
		}
	}
	tk.require.True(ok)
	return tk.MustQuery(sql, args...)
}

// MustPartitionByList checks if the result execution plan must read specific partitions by list.
func (tk *TestKit) MustPartitionByList(sql string, partitions []string, args ...interface{}) *Result {
	rs := tk.MustQuery("explain "+sql, args...)
	ok := len(partitions) == 0
	for i := range rs.rows {
		if ok {
			tk.require.NotContains(rs.rows[i][3], "partition:")
		}
		for index, partition := range partitions {
			if !ok && strings.Contains(rs.rows[i][3], "partition:"+partition) {
				partitions = append(partitions[:index], partitions[index+1:]...)
			}
		}

	}
	if !ok {
		tk.require.Len(partitions, 0)
	}
	return tk.MustQuery(sql, args...)
}

// QueryToErr executes a sql statement and discard results.
func (tk *TestKit) QueryToErr(sql string, args ...interface{}) error {
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	res, err := tk.Exec(sql, args...)
	tk.require.NoError(err, comment)
	tk.require.NotNil(res, comment)
	_, resErr := session.GetRows4Test(context.Background(), tk.session, res)
	tk.require.NoError(res.Close())
	return resErr
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

// HasPlan checks if the result execution plan contains specific plan.
func (tk *TestKit) HasPlan(sql string, plan string, args ...interface{}) bool {
	rs := tk.MustQuery("explain "+sql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][0], plan) {
			return true
		}
	}
	return false
}

// HasPlan4ExplainFor checks if the result execution plan contains specific plan.
func (tk *TestKit) HasPlan4ExplainFor(result *Result, plan string) bool {
	for i := range result.rows {
		if strings.Contains(result.rows[i][0], plan) {
			return true
		}
	}
	return false
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
			var rs sqlexec.RecordSet
			var err error
			if s, ok := stmt.(*ast.NonTransactionalDeleteStmt); ok {
				rs, err = session.HandleNonTransactionalDelete(ctx, s, tk.Session())
			} else {
				rs, err = tk.Session().ExecuteStmt(ctx, stmt)
			}
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

// ExecToErr executes a sql statement and discard results.
func (tk *TestKit) ExecToErr(sql string, args ...interface{}) error {
	res, err := tk.Exec(sql, args...)
	if res != nil {
		tk.require.NoError(res.Close())
	}
	return err
}

func newSession(t testing.TB, store kv.Storage) session.Session {
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	se.SetConnectionID(testKitIDGenerator.Inc())
	return se
}

// RefreshConnectionID refresh the connection ID for session of the testkit
func (tk *TestKit) RefreshConnectionID() {
	if tk.session != nil {
		tk.session.SetConnectionID(testKitIDGenerator.Inc())
	}
}

// MustGetErrCode executes a sql statement and assert it's error code.
func (tk *TestKit) MustGetErrCode(sql string, errCode int) {
	_, err := tk.Exec(sql)
	tk.require.Errorf(err, "sql: %s", sql)
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	tk.require.Truef(ok, "sql: %s, expect type 'terror.Error', but obtain '%T': %v", sql, originErr, originErr)
	sqlErr := terror.ToSQLError(tErr)
	tk.require.Equalf(errCode, int(sqlErr.Code), "sql: %s, Assertion failed, origin err:\n  %v", sql, sqlErr)
}

// MustGetErrMsg executes a sql statement and assert its error message.
func (tk *TestKit) MustGetErrMsg(sql string, errStr string) {
	err := tk.ExecToErr(sql)
	tk.require.EqualError(err, errStr)
}

// MustContainErrMsg executes a sql statement and assert its error message containing errStr.
func (tk *TestKit) MustContainErrMsg(sql string, errStr interface{}) {
	err := tk.ExecToErr(sql)
	tk.require.Error(err)
	tk.require.Contains(err.Error(), errStr)
}

// MustMatchErrMsg executes a sql statement and assert its error message matching errRx.
func (tk *TestKit) MustMatchErrMsg(sql string, errRx interface{}) {
	err := tk.ExecToErr(sql)
	tk.require.Error(err)
	tk.require.Regexp(errRx, err.Error())
}

// MustUseIndex checks if the result execution plan contains specific index(es).
func (tk *TestKit) MustUseIndex(sql string, index string, args ...interface{}) bool {
	rs := tk.MustQuery("explain "+sql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][3], "index:"+index) {
			return true
		}
	}
	return false
}

// MustUseIndex4ExplainFor checks if the result execution plan contains specific index(es).
func (tk *TestKit) MustUseIndex4ExplainFor(result *Result, index string) bool {
	for i := range result.rows {
		// It depends on whether we enable to collect the execution info.
		if strings.Contains(result.rows[i][3], "index:"+index) {
			return true
		}
		if strings.Contains(result.rows[i][4], "index:"+index) {
			return true
		}
	}
	return false
}

// CheckExecResult checks the affected rows and the insert id after executing MustExec.
func (tk *TestKit) CheckExecResult(affectedRows, insertID int64) {
	tk.require.Equal(int64(tk.Session().AffectedRows()), affectedRows)
	tk.require.Equal(int64(tk.Session().LastInsertID()), insertID)
}

// MustPointGet checks whether the plan for the sql is Point_Get.
func (tk *TestKit) MustPointGet(sql string, args ...interface{}) *Result {
	rs := tk.MustQuery("explain "+sql, args...)
	tk.require.Len(rs.rows, 1)
	tk.require.Contains(rs.rows[0][0], "Point_Get", "plan %v", rs.rows[0][0])
	return tk.MustQuery(sql, args...)
}

// UsedPartitions returns the partition names that will be used or all/dual.
func (tk *TestKit) UsedPartitions(sql string, args ...interface{}) *Result {
	rs := tk.MustQuery("explain "+sql, args...)
	var usedPartitions [][]string
	for i := range rs.rows {
		index := strings.Index(rs.rows[i][3], "partition:")
		if index != -1 {
			p := rs.rows[i][3][index+len("partition:"):]
			partitions := strings.Split(strings.SplitN(p, " ", 2)[0], ",")
			usedPartitions = append(usedPartitions, partitions)
		}
	}
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	return &Result{rows: usedPartitions, comment: comment, assert: tk.assert, require: tk.require}
}

// WithPruneMode run test case under prune mode.
func WithPruneMode(tk *TestKit, mode variable.PartitionPruneMode, f func()) {
	tk.MustExec("set @@tidb_partition_prune_mode=`" + string(mode) + "`")
	tk.MustExec("set global tidb_partition_prune_mode=`" + string(mode) + "`")
	f()
}

func containGlobal(rs *Result) bool {
	partitionNameCol := 2
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][partitionNameCol], "global") {
			return true
		}
	}
	return false
}

// MustNoGlobalStats checks if there is no global stats.
func (tk *TestKit) MustNoGlobalStats(table string) bool {
	if containGlobal(tk.MustQuery("show stats_meta where table_name like '" + table + "'")) {
		return false
	}
	if containGlobal(tk.MustQuery("show stats_buckets where table_name like '" + table + "'")) {
		return false
	}
	if containGlobal(tk.MustQuery("show stats_histograms where table_name like '" + table + "'")) {
		return false
	}
	return true
}

// CheckLastMessage checks last message after executing MustExec
func (tk *TestKit) CheckLastMessage(msg string) {
	tk.require.Equal(tk.Session().LastMessage(), msg)
}
