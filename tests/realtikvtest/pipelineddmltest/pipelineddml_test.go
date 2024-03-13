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
// 3. pessimistic-auto-commit is turned off
// 4. binlog is disabled
// 5. the statement is not running inside a transaction
// 6. the session is external used
// 7. the statement is insert, update or delete
func TestPipelinedDMLPositive(t *testing.T) {
	// the test is a little tricky, only when pipelined dml is enabled, the failpoint panics and the panic message will be returned as error
	// TODO: maybe save the pipelined DML usage into TxnInfo, so we can check from it.
	require.NoError(t, failpoint.Enable("tikvclient/pipelinedCommitFail", `panic("pipelined memdb is be enabled")`))
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
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("set session tidb_dml_type = bulk")
	for _, stmt := range stmts {
		// text protocol
		err := panicToErr(func() error {
			_, err := tk.Exec(stmt)
			return err
		})
		require.Error(t, err, stmt)
		require.True(t, strings.Contains(err.Error(), "pipelined memdb is be enabled"), err.Error(), stmt)
		// binary protocol
		ctx := context.Background()
		parsedStmts, err := tk.Session().Parse(ctx, stmt)
		require.NoError(t, err)
		err = panicToErr(func() error {
			_, err := tk.Session().ExecuteStmt(ctx, parsedStmts[0])
			return err
		})
		require.Error(t, err, stmt)
		require.True(t, strings.Contains(err.Error(), "pipelined memdb is be enabled"), err.Error(), stmt)
	}
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
	tk.MustExec("insert into t values(2, 2)")
	tk.MustExec("commit")

	// pessimistic-auto-commit is on
	origPessimisticAutoCommit := config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load()
	config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(true)
	defer func() {
		config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(origPessimisticAutoCommit)
	}()
	tk.MustExec("insert into t values(3, 3)")
	config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(false)

	// binlog is enabled
	tk.Session().GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&testkit.MockPumpClient{})
	tk.MustExec("insert into t values(4, 4)")
	tk.Session().GetSessionVars().BinlogClient = nil

	// in a running txn
	tk.MustExec("set session tidb_dml_type = standard")
	tk.MustExec("begin")
	tk.MustExec("set session tidb_dml_type = bulk") // turn on bulk dml in a txn doesn't effect the current txn.
	tk.MustExec("insert into t values(5, 5)")
	tk.MustExec("commit")

	// in an internal txn
	tk.Session().GetSessionVars().InRestrictedSQL = true
	tk.MustExec("insert into t values(6, 6)")
	tk.Session().GetSessionVars().InRestrictedSQL = false

	// it's a read statement
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6"))
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
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, unique index idx(b))")
	res := tk.MustQuery("explain analyze insert ignore into t1 values (1,1), (2,2), (3,3), (4,4), (5,5)")
	explain := getExplainResult(res)
	require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{BatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
	// Test with bulk dml.
	tk.MustExec("set session tidb_dml_type = bulk")
	// Test normal insert.
	tk.MustExec("truncate table t1")
	res = tk.MustQuery("explain analyze insert into t1 values (1,1), (2,2), (3,3), (4,4), (5,5)")
	explain = getExplainResult(res)
	// TODO: try to optimize the rpc count, when use bulk dml, insert will send many BufferBatchGet rpc.
	require.Regexp(t, "Insert.* insert:.*, rpc:{BufferBatchGet:{num_rpc:10, total_time:.*}}.*", explain)
	// Test insert ignore.
	tk.MustExec("truncate table t1")
	res = tk.MustQuery("explain analyze insert ignore into t1 values (1,1), (2,2), (3,3), (4,4), (5,5)")
	explain = getExplainResult(res)
	// TODO: try to optimize the rpc count, when use bulk dml, insert ignore will send 5 BufferBatchGet and  1 BatchGet rpc.
	// but without bulk dml, it will only use 1 BatchGet rpcs.
	require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BufferBatchGet:{num_rpc:5, total_time:.*}}}.*", explain)
	require.Regexp(t, "Insert.* check_insert: {total_time: .* rpc:{.*BatchGet:{num_rpc:1, total_time:.*}}}.*", explain)
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

func TestPipelinedDMLInsertMemoryTest(t *testing.T) {
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
	tk.MustExec("create table t1 (a int, b int, c varchar(128), unique index idx(b))")
	tk.MustExec("create table _t1 like t1")
	cnt := 1000

	// insertStmt
	buf := bytes.NewBuffer(make([]byte, 0, 10240))
	buf.WriteString("insert into t1 values ")
	for i := 0; i < cnt; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("(%d, %d, 'abcdefghijklmnopqrstuvwxyz1234567890,.?+-=_!@#$&*()_+')", i, i))
	}
	tk.MustExec(buf.String())
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows(fmt.Sprintf("%d", cnt)))

	// insert
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")    // query canceled by memory controller will return error.
	tk.MustExec("set session tidb_mem_quota_query = 256 << 10") // 256KB limitation.
	tk.MustExec("set session tidb_max_chunk_size = 32")
	insertStmt := "insert into _t1 select * from t1"
	err := tk.ExecToErr(insertStmt)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query. Please try narrowing your query scope or increase the tidb_mem_quota_query limit and try again."), err.Error())
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec(insertStmt)
	tk.MustQuery("select count(*) from _t1").Check(testkit.Rows(fmt.Sprintf("%d", cnt)))

	// update
	tk.MustExec("set session tidb_mem_quota_query = 256 << 10") // 256KB limitation.
	updateStmt := "update _t1 set c = 'abcdefghijklmnopqrstuvwxyz1234567890,.?+-=_!@#$&*()_++++++'"
	tk.MustExec("set session tidb_dml_type = standard")
	err = tk.ExecToErr(updateStmt)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query. Please try narrowing your query scope or increase the tidb_mem_quota_query limit and try again."), err.Error())
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec(updateStmt)
	tk.MustQuery("select count(*) from _t1 where c = 'abcdefghijklmnopqrstuvwxyz1234567890,.?+-=_!@#$&*()_++++++'").Check(testkit.Rows(fmt.Sprintf("%d", cnt)))

	// delete
	tk.MustExec("set session tidb_mem_quota_query = 128 << 10") // 128KB limitation.
	deleteStmt := "delete from _t1"
	tk.MustExec("set session tidb_dml_type = standard")
	err = tk.ExecToErr(deleteStmt)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query. Please try narrowing your query scope or increase the tidb_mem_quota_query limit and try again."), err.Error())
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec(deleteStmt)
	tk.MustQuery("select count(*) from _t1").Check(testkit.Rows("0"))
}

func TestPipelinedDMLDisableRetry(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(a int primary key, b int)")
	tk1.MustExec("insert into t1 values(1, 1)")
	require.Nil(t, failpoint.Enable("tikvclient/beforePipelinedFlush", `pause`))
	tk1.MustExec("set session tidb_dml_type = bulk")
	errCh := make(chan error)
	go func() {
		errCh <- tk1.ExecToErr("update t1 set b = b + 20")
	}()
	time.Sleep(500 * time.Millisecond)
	tk2.MustExec("update t1 set b = b + 10")
	require.Nil(t, failpoint.Disable("tikvclient/beforePipelinedFlush"))
	err := <-errCh
	require.Error(t, err)
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("error: %s", err))
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
