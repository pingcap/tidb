// Copyright 2024 PingCAP, Inc.
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

package pipelineddmltest

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestVariable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.Equal(t, tk.Session().GetSessionVars().BulkDMLEnabled, false)
	tk.MustExec("set session tidb_dml_type = bulk")
	require.Equal(t, tk.Session().GetSessionVars().BulkDMLEnabled, true)
	tk.MustExec("set session tidb_dml_type = standard")
	require.Equal(t, tk.Session().GetSessionVars().BulkDMLEnabled, false)
	// not supported yet.
	tk.MustExecToErr("set session tidb_dml_type = bulk(10)")
}

// We limit this feature only for cases meet all the following conditions:
// 1. tidb_dml_type is set to bulk for the current session
// 2. the session is running an auto-commit txn
// 3. (removed, it is now overridden by tidb_dml_type=bulk) pessimistic-auto-commit ~~if off~~
// 4. binlog is disabled
// 5. the statement is not running inside a transaction
// 6. the session is external used
// 7. the statement is insert, update or delete
func TestPipelinedDMLPositive(t *testing.T) {
	// the test is a little tricky, only when pipelined dml is enabled, the failpoint panics and the panic message will be returned as error
	// TODO: maybe save the pipelined DML usage into TxnInfo, so we can check from it.
	require.NoError(t, failpoint.Enable("tikvclient/pipelinedCommitFail", `panic("pipelined memdb is enabled")`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/pipelinedCommitFail"))
	}()

	panicToErr := func(fn func() error) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()
		err = fn()
		if err != nil {
			return err
		}
		return
	}

	stmts := []string{
		"insert into t values(2, 2)",
		"update t set b = b + 1",
		"delete from t",
	}

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("set session tidb_dml_type = bulk")
	for _, stmt := range stmts {
		// text protocol
		err := panicToErr(func() error {
			_, err := tk.Exec(stmt)
			return err
		})
		require.Error(t, err, stmt)
		require.True(t, strings.Contains(err.Error(), "pipelined memdb is enabled"), err.Error(), stmt)
		// binary protocol
		ctx := context.Background()
		parsedStmts, err := tk.Session().Parse(ctx, stmt)
		require.NoError(t, err)
		err = panicToErr(func() error {
			_, err := tk.Session().ExecuteStmt(ctx, parsedStmts[0])
			return err
		})
		require.Error(t, err, stmt)
		require.True(t, strings.Contains(err.Error(), "pipelined memdb is enabled"), err.Error(), stmt)
	}

	// pessimistic-auto-commit is on
	origPessimisticAutoCommit := config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load()
	config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(true)
	defer func() {
		config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(origPessimisticAutoCommit)
	}()
	err := panicToErr(
		func() error {
			_, err := tk.Exec("insert into t values(3, 3)")
			return err
		},
	)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "pipelined memdb is enabled"), err.Error())
	tk.MustQuery("show warnings").CheckContain("pessimistic-auto-commit config is ignored in favor of Pipelined DML")
	config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(false)

	// enable by hint
	// Hint works for DELETE and UPDATE, but not for INSERT if the hint is in its select clause.
	tk.MustExec("set @@tidb_dml_type = standard")
	err = panicToErr(
		func() error {
			_, err := tk.Exec("delete /*+ SET_VAR(tidb_dml_type=bulk) */ from t")
			// "insert into t select /*+ SET_VAR(tidb_dml_type=bulk) */ * from t" won't work
			return err
		},
	)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "pipelined memdb is enabled"), err.Error())
}

func TestPipelinedDMLNegative(t *testing.T) {
	// fail when pipelined memdb is enabled for negative cases.
	require.NoError(t, failpoint.Enable("tikvclient/beforePipelinedFlush", `panic("pipelined memdb should not be enabled")`))
	require.NoError(t, failpoint.Enable("tikvclient/pipelinedCommitFail", `panic("pipelined memdb should not be enabled")`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/beforePipelinedFlush"))
		require.NoError(t, failpoint.Disable("tikvclient/pipelinedCommitFail"))
	}()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")

	// tidb_dml_type is not set
	tk.MustExec("insert into t values(1, 1)")

	// not in auto-commit txn
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("begin")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can only be used for auto-commit INSERT, REPLACE, UPDATE or DELETE. Fallback to standard mode")
	tk.MustExec("insert into t values(2, 2)")
	tk.MustExec("commit")

	// binlog is enabled
	tk.Session().GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&testkit.MockPumpClient{})
	tk.MustExec("insert into t values(4, 4)")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used with Binlog: BinlogClient != nil. Fallback to standard mode")
	tk.Session().GetSessionVars().BinlogClient = nil

	// in a running txn
	tk.MustExec("set session tidb_dml_type = standard")
	tk.MustExec("begin")
	// turn on bulk dml in a txn doesn't affect the current txn.
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("insert into t values(5, 5)")
	tk.MustExec("commit")

	// in an internal txn
	tk.Session().GetSessionVars().InRestrictedSQL = true
	tk.MustExec("insert into t values(6, 6)")
	tk.Session().GetSessionVars().InRestrictedSQL = false
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used for internal SQL. Fallback to standard mode")

	// it's a read statement
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 1", "2 2", "4 4", "5 5", "6 6"))

	// for deprecated batch-dml
	tk.Session().GetSessionVars().BatchDelete = true
	tk.Session().GetSessionVars().DMLBatchSize = 1
	variable.EnableBatchDML.Store(true)
	tk.MustExec("insert into t values(7, 7)")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used with the deprecated Batch DML. Fallback to standard mode")
	tk.Session().GetSessionVars().BatchDelete = false
	tk.Session().GetSessionVars().DMLBatchSize = 0
	variable.EnableBatchDML.Store(false)

	// for explain and explain analyze
	tk.Session().GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&testkit.MockPumpClient{})
	tk.MustExec("explain insert into t values(8, 8)")
	// explain is read-only, so it doesn't warn.
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("explain analyze insert into t values(9, 9)")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used with Binlog: BinlogClient != nil. Fallback to standard mode")
	tk.Session().GetSessionVars().BinlogClient = nil

	// disable MDL
	tk.MustExec("set global tidb_enable_metadata_lock = off")
	tk.MustExec("insert into t values(10, 10)")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used without Metadata Lock. Fallback to standard mode")
	tk.MustExec("set global tidb_enable_metadata_lock = on")

	// tidb_constraint_check_in_place = ON
	tk.MustExec("set @@tidb_constraint_check_in_place = 1")
	tk.MustExec("insert into t values(11, 11)")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used when tidb_constraint_check_in_place=ON. Fallback to standard mode")
	tk.MustExec("set @@tidb_constraint_check_in_place = 0")
}

func compareTables(t *testing.T, tk *testkit.TestKit, t1, t2 string) {
	t1Rows := tk.MustQuery("select * from " + t1).Sort().Rows()
	t2Rows := tk.MustQuery("select * from " + t2)
	require.Equal(t, len(t1Rows), len(t2Rows.Rows()))
	t2Rows.Sort().Check(t1Rows)
}

func prepareData(tk *testkit.TestKit) {
	tk.MustExec("drop table if exists t, _t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("create table _t like t")
	results := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
		results = append(results, fmt.Sprintf("%d %d", i, i))
	}
	tk.MustQuery("select * from t order by a asc").Check(testkit.Rows(results...))
}

func TestPipelinedDMLInsert(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(10)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(100)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(10240)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("insert into _t select * from t")
	compareTables(t, tk, "t", "_t")

	tk.MustExec("insert into t select a + 10000, b from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(100))
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("200"))

	// simulate multi regions by splitting table.
	tk.MustExec("truncate table _t")
	tk.MustQuery("split table _t between (0) and (10000) regions 10").Check(testkit.Rows("9 1"))
	tk.MustQuery("select count(1) from _t").Check(testkit.Rows("0"))
	tk.MustExec("insert into _t select * from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(200))
	compareTables(t, tk, "t", "_t")
}

func TestPipelinedDMLInsertIgnore(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(10)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(100)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(10240)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("insert into _t values(0, -1), (19, -1), (29, -1), (39, -1), (49, -1), (59, -1), (69, -1), (79, -1), (89, -1), (99, -1)")
	tk.MustExec("insert ignore into _t select * from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(90))
	tk.MustQuery("select count(1) from _t").Check(testkit.Rows("100"))
	tk.MustQuery("select * from _t where a in (0, 19, 29, 39, 49, 59, 69, 79, 89, 99)").Sort().
		Check(testkit.Rows("0 -1", "19 -1", "29 -1", "39 -1", "49 -1", "59 -1", "69 -1", "79 -1", "89 -1", "99 -1"))
}

func TestPipelinedDMLInsertOnDuplicateKeyUpdate(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(10)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(100)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(10240)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustExec("insert into _t values(0, -1), (19, -1), (29, -1), (39, -1), (49, -1), (59, -1), (69, -1), (79, -1), (89, -1), (99, -1)")
	tk.MustExec("insert into _t select * from t on duplicate key update b = values(b)")
	require.Equal(t, tk.Session().AffectedRows(), uint64(110))
	compareTables(t, tk, "t", "_t")
}

func TestPipelinedDMLInsertRPC(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tables := []string{
		"create table _t1 (a int, b int)",                                  // no index, auto generated handle
		"create table _t1 (a int primary key, b int)",                      // clustered handle
		"create table _t1 (a int, b int, unique index idx(b))",             // unique index
		"create table _t1 (a int primary key, b int, unique index idx(b))", // clustered handle + unique index
	}
	for _, table := range tables {
		for _, tableSource := range []bool{true, false} {
			hasPK := strings.Contains(table, "primary key")
			hasUK := strings.Contains(table, "unique index")
			tk.MustExec("drop table if exists t1, _t1")
			var values string
			if tableSource {
				tk.MustExec("create table t1 (a int, b int)")
				tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
				values = " select * from t1"
			} else {
				values = " values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)"
			}
			tk.MustExec(table)

			// Test with standard dml.
			tk.MustExec("set session tidb_dml_type = standard")
			res := tk.MustQuery("explain analyze insert ignore into _t1" + values)
			explain := getExplainResult(res)
			if hasPK || hasUK {
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{BatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{BatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
			} else {
				require.NotRegexp(t, "Insert.* check_insert: {total_time: .* rpc:{BatchGet:{num_rpc:.*, total_time:.*}}}.*", explain)
				require.NotRegexp(t, "Insert.* check_insert: {total_time: .* rpc:{BatchGet:{num_rpc:.*, total_time:.*}}}.*", explain)
			}

			// Test with bulk dml.
			tk.MustExec("set session tidb_dml_type = bulk")

			// Test normal insert.
			tk.MustExec("truncate table _t1")
			res = tk.MustQuery("explain analyze insert into _t1" + values)
			explain = getExplainResult(res)
			// no BufferBatchGet with lazy check
			require.NotRegexp(t, "Insert.* insert:.*, rpc:{BufferBatchGet:{num_rpc:.*, total_time:.*}}.*", explain)

			// Test insert ignore.
			tk.MustExec("truncate table _t1")
			res = tk.MustQuery("explain analyze insert ignore into _t1" + values)
			explain = getExplainResult(res)
			// with bulk dml, it will 1 BatchGet and 1 BufferBatchGet RPCs in prefetch phase.
			// but no need to prefetch when there are no unique indexes and no primary key.
			if hasPK || hasUK {
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BufferBatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
			} else {
				require.NotRegexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BufferBatchGet:{num_rpc:.*, total_time:.*}}}.*", explain)
				require.NotRegexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BatchGet:{num_rpc:.*, total_time:.*}}}.*", explain)
			}
			// The ignore takes effect now.
			res = tk.MustQuery("explain analyze insert ignore into _t1" + values)
			explain = getExplainResult(res)
			if hasPK || hasUK {
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BufferBatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
			} else {
				require.NotRegexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BufferBatchGet:{num_rpc:.*, total_time:.*}}}.*", explain)
				require.NotRegexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BatchGet:{num_rpc:.*, total_time:.*}}}.*", explain)
			}

			// Test insert on duplicate key update.
			res = tk.MustQuery("explain analyze insert into _t1 " + values + " on duplicate key update a = values(a) + 5")
			explain = getExplainResult(res)
			if hasUK {
				// 2 rounds checks are required: read handles by unique keys and read rows by handles
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BufferBatchGet:{num_rpc:2, total_time:.*}}}.*", explain)
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BatchGet:{num_rpc:2, total_time:.*}}}.*", explain)
			} else if hasPK {
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BufferBatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
				require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
			} else {
				require.NotRegexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BufferBatchGet:{num_rpc:.*, total_time:.*}}}.*", explain)
				require.NotRegexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BatchGet:{num_rpc:.*, total_time:.*}}}.*", explain)
			}

			// Test replace into. replace checks in the same way with insert on duplicate key update.
			// However, the format of explain result is little different.
			res = tk.MustQuery("explain analyze replace into _t1" + values)
			explain = getExplainResult(res)
			if hasUK {
				require.Regexp(t, "Insert.* rpc: {.*BufferBatchGet:{num_rpc:2, total_time:.*}}.*", explain)
				require.Regexp(t, "Insert.* rpc: {.*BatchGet:{num_rpc:2, total_time:.*}}.*", explain)
			} else if hasPK {
				require.Regexp(t, "Insert.* rpc: {.*BufferBatchGet:{num_rpc:1, total_time:.*}}.*", explain)
				require.Regexp(t, "Insert.* rpc: {.*BatchGet:{num_rpc:1, total_time:.*}}.*", explain)
			} else {
				require.NotRegexp(t, "Insert.* rpc: {.*BufferBatchGet:{num_rpc:.*, total_time:.*}}.*", explain)
				require.NotRegexp(t, "Insert.* rpc: {.*BatchGet:{num_rpc:.*, total_time:.*}}.*", explain)
			}
		}
	}
}

func getExplainResult(res *testkit.Result) string {
	resBuff := bytes.NewBufferString("")
	for _, row := range res.Rows() {
		_, _ = fmt.Fprintf(resBuff, "%s\t", row)
	}
	return resBuff.String()
}

func TestPipelinedDMLInsertOnDuplicateKeyUpdateInTxn(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(10)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(100)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(10240)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, c varchar(128), unique index idx(b))")
	cnt := 2000
	values := bytes.NewBuffer(make([]byte, 0, 10240))
	for i := 0; i < cnt; i++ {
		if i > 0 {
			values.WriteString(", ")
		}
		if i == 1500 {
			values.WriteString(fmt.Sprintf("(%d, %d, 'abcdefghijklmnopqrstuvwxyz1234567890,.?+-=_!@#$&*()_+')", i, 250))
		} else {
			values.WriteString(fmt.Sprintf("(%d, %d, 'abcdefghijklmnopqrstuvwxyz1234567890,.?+-=_!@#$&*()_+')", i, i))
		}
	}
	tk.MustExec("set session tidb_dml_type = bulk")
	// Test insert meet duplicate key error.
	tk.MustGetErrMsg("insert into t1 values "+values.String(), "[kv:1062]Duplicate entry '250' for key 't1.idx'")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	// Test insert ignore
	tk.MustExec("insert ignore into t1 values " + values.String())
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '250' for key 't1.idx'"))
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("1999"))
	tk.MustQuery("select a, b from t1 where a in (250, 1500)").Check(testkit.Rows("250 250"))
	if !*realtikvtest.WithRealTiKV {
		// TODO: fix me. skip for real TiKV because now we have assertion issue.
		// Test replace into.
		tk.MustExec("delete from t1")
		tk.MustExec("replace into t1 values " + values.String())
		tk.MustQuery("select count(*) from t1").Check(testkit.Rows("1999"))
		tk.MustQuery("select a, b from t1 where a in (250, 1500)").Check(testkit.Rows("1500 250"))
		// Test insert on duplicate key update.
		// TODO: fix me. skip for real TiKV because now we have assertion issue.
		tk.MustExec("delete from t1")
		tk.MustExec("insert into t1 values " + values.String() + " on duplicate key update b = values(b) + 2000")
		tk.MustQuery("select count(*) from t1").Check(testkit.Rows("1999"))
		tk.MustQuery("select a, b from t1 where a in (250, 1500)").Check(testkit.Rows("250 2250"))
	}
}

func TestPipelinedDMLDelete(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("delete from t where a % 2 = 0")
	require.Equal(t, tk.Session().AffectedRows(), uint64(50))
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("50"))

	// simulate multi regions by splitting table.
	tk.MustExec("update t set a = a * 101")
	tk.MustQuery("split table t between (0) and (10000) regions 10").Check(testkit.Rows("9 1"))
	tk.MustExec("delete from t where a % 2 = 1")
	require.Equal(t, tk.Session().AffectedRows(), uint64(50))
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("0"))
}

func TestPipelinedDMLUpdate(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(10)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(100)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(10240)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("4950"))
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("update t set b = b + 1")
	require.Equal(t, tk.Session().AffectedRows(), uint64(100))
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("5050"))

	// simulate multi regions by splitting table.
	tk.MustExec("update t set a = a * 100")
	tk.MustQuery("split table t between (0) and (10000) regions 10").Check(testkit.Rows("9 1"))
	tk.MustExec("update t set b = b + 1")
	require.Equal(t, tk.Session().AffectedRows(), uint64(100))
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("5150"))
}

func TestPipelinedDMLCommitFailed(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(10)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(100)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(10240)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	require.NoError(t, failpoint.Enable("tikvclient/pipelinedCommitFail", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/pipelinedCommitFail"))
	}()

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExecToErr("insert into _t select * from t")
	// TODO: fix the affected rows
	//require.Equal(t, tk.Session().AffectedRows(), uint64(0))
	tk1.MustQuery("select * from _t").Check(testkit.Rows())

	tk.MustExecToErr("insert into t select a + 100, b from t")
	//require.Equal(t, tk.Session().AffectedRows(), uint64(0))
	tk1.MustQuery("select count(1) from t").Check(testkit.Rows("100"))
}

func TestPipelinedDMLCommitSkipSecondaries(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(10)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(100)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(10240)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	require.NoError(t, failpoint.Enable("tikvclient/pipelinedSkipResolveLock", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/pipelinedSkipResolveLock"))
	}()

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("insert into _t select * from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(100))
	compareTables(t, tk, "t", "_t")

	tk.MustExec("insert into t select a + 100, b from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(100))
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("200"))
}

func TestPipelinedDMLDisableRetry(t *testing.T) {
	// the case tests that
	// 1. auto-retry for pipelined dml is disabled
	// 2. the write conflict error message returned from a Flush (instead of from Commit) is correct
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(1)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(1)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(1)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.Session().GetSessionVars().InitChunkSize = 1
	tk1.Session().GetSessionVars().MaxChunkSize = 1
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(a int primary key, b int)")
	// we need to avoid inserting *literals* into t, so let t2 be the source table.
	tk1.MustExec("create table t2(a int, b int)")
	tk1.MustExec("insert into t2 values (1, 1), (2, 1)")
	require.Nil(t, failpoint.Enable("tikvclient/beforePipelinedFlush", `pause`))
	tk1.MustExec("set session tidb_dml_type = bulk")
	errCh := make(chan error)
	go func() {
		// we expect that this stmt triggers 2 flushes, each containing only 1 row.
		errCh <- tk1.ExecToErr("insert into t1 select * from t2 order by a")
	}()
	time.Sleep(500 * time.Millisecond)
	tk2.MustExec("insert into t1 values (1,2)")
	require.Nil(t, failpoint.Disable("tikvclient/beforePipelinedFlush"))
	err := <-errCh
	require.Error(t, err)
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("error: %s", err))
	require.ErrorContains(t, err, "tableName=test.t1")
}

func TestReplaceRowCheck(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t1, _t1")
	tk1.MustExec("create table t1(a int, b int)")
	tk1.MustExec("create table _t1(a int primary key, b int)")
	tk1.MustExec("insert into t1 values(1, 1), (2, 2), (1, 2), (2, 1)")
	tk1.MustExec("set session tidb_dml_type = bulk")
	tk1.MustExec("replace into _t1 select * from t1")
	tk1.MustExec("admin check table _t1")
	tk1.MustQuery("select a from _t1").Sort().Check(testkit.Rows("1", "2"))

	tk1.MustExec("truncate table _t1")
	tk1.MustExec("insert ignore into _t1 select * from t1")
	tk1.MustExec("admin check table _t1")
	tk1.MustQuery("select a from _t1").Sort().Check(testkit.Rows("1", "2"))

	tk1.MustExec("truncate table _t1")
	tk1.MustExec("insert into _t1 select * from t1 on duplicate key update b = values(b)")
	tk1.MustExec("admin check table _t1")
	tk1.MustQuery("select a from _t1").Sort().Check(testkit.Rows("1", "2"))
}

func TestDuplicateKeyErrorMessage(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("insert into t1 values(1, 1)")
	err1 := tk.ExecToErr("insert into t1 values(1, 1)")
	require.Error(t, err1)
	tk.MustExec("set session tidb_dml_type = bulk")
	err2 := tk.ExecToErr("insert into t1 values(1, 1)")
	require.Error(t, err2)
	require.Equal(t, err1.Error(), err2.Error())
}

func TestInsertIgnoreOnDuplicateKeyUpdate(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, unique index u1(a, b), unique index u2(a))")
	tk.MustExec("insert into t1 values(0, 0), (1, 1)")
	tk.MustExec("insert ignore into t1 values (0, 2) ,(1, 3) on duplicate key update b = 5, a = 0")
	// if the statement execute successful, the following check should pass.
	tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("0 5", "1 1"))
}

func TestConflictError(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(10)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(128)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(128)`))
	defer func() {
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
		require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	}()
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, _t1")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table _t1(a int primary key, b int)")
	var insert strings.Builder
	insert.WriteString("insert into t1 values")
	for i := 0; i < 100; i++ {
		if i > 0 {
			insert.WriteString(",")
		}
		insert.WriteString(fmt.Sprintf("(%d, %d)", i, i))
	}
	tk.MustExec(insert.String())
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("insert into _t1 select * from t1")
	tk.MustExec("set session tidb_max_chunk_size = 32")
	err := tk.ExecToErr("insert into _t1 select * from t1 order by rand()")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "Duplicate entry"), err.Error())
}

func TestRejectUnsupportedTables(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set session tidb_dml_type = bulk")

	// FKs are not supported if foreign_key_checks=ON
	tk.MustExec("drop table if exists parent, child")
	tk.MustExec("create table parent(a int primary key)")
	tk.MustExec("create table child(a int, foreign key (a) references parent(a))")
	err := tk.ExecToErr("insert into parent values(1)")
	require.NoError(t, err)
	err = tk.ExecToErr("insert into child values(1)")
	require.NoError(t, err)
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode")
	err = tk.ExecToErr("insert into child values(2)")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "foreign key constraint fails"), err.Error())
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode")

	// test a delete sql that deletes two tables
	tk.MustExec("insert into parent values(2)")
	tk.MustExec("delete parent, child from parent left join child on parent.a = child.a")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode")

	// swap the order of the two tables
	tk.MustExec("insert into parent values(3)")
	tk.MustExec("delete child, parent from child left join parent on parent.a = child.a")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode")

	tk.MustExec("set @@foreign_key_checks=false")
	tk.MustExec("insert into parent values(4)")
	tk.MustExec("insert into child values(4)")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// temp tables are not supported
	tk.MustExec("create temporary table temp(a int)")
	tk.MustExec("insert into temp values(1)")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used on temporary tables. Fallback to standard mode")

	// cached tables are not supported
	tk.MustExec("create table cached(a int)")
	tk.MustExec("alter table cached cache")
	tk.MustExec("insert into cached values(1)")
	tk.MustQuery("show warnings").CheckContain("Pipelined DML can not be used on cached tables. Fallback to standard mode")
}
