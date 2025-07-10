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

package sessiontxn_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfork"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func setupTxnContextTest(t *testing.T) (kv.Storage, *domain.Domain) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerInCompile", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerInRebuildPlan", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerAfterBuildExecutor", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerAfterPessimisticLockErrorRetry", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerInShortPointGetPlan", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/assertTxnManagerInRunStmt", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/assertTxnManagerInCachedPlanExec", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/assertTxnManagerForUpdateTSEqual", "return"))

	store, do := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.Session().SetValue(sessiontxn.AssertRecordsKey, nil)
	tk.Session().SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")

	tk.MustExec("create table t1 (id int primary key, v int)")
	tk.MustExec("insert into t1 values(1, 10)")

	tk.MustExec("create table t2 (id int)")

	tk.MustExec("create temporary table tmp (id int)")
	tk.MustExec("insert into tmp values(10)")

	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerInCompile"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerInRebuildPlan"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerAfterBuildExecutor"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerAfterPessimisticLockErrorRetry"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertTxnManagerInShortPointGetPlan"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/assertTxnManagerInRunStmt"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/assertTxnManagerInCachedPlanExec"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/assertTxnManagerForUpdateTSEqual"))

		tk.Session().SetValue(sessiontxn.AssertRecordsKey, nil)
		tk.Session().SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
		tk.Session().SetValue(sessiontxn.AssertTxnInfoSchemaAfterRetryKey, nil)
	})
	return store, do
}

func checkAssertRecordExits(t *testing.T, se sessionctx.Context, name string) {
	records, ok := se.Value(sessiontxn.AssertRecordsKey).(map[string]any)
	require.True(t, ok, fmt.Sprintf("'%s' not in record, maybe failpoint not enabled?", name))
	_, ok = records[name]
	require.True(t, ok, fmt.Sprintf("'%s' not in record", name))
}

func doWithCheckPath(t *testing.T, se sessionctx.Context, names []string, do func()) {
	se.SetValue(sessiontxn.AssertRecordsKey, nil)
	do()
	for _, name := range names {
		checkAssertRecordExits(t, se, name)
	}
}

var normalPathRecords = []string{
	"assertTxnManagerInCompile",
	"assertTxnManagerInRunStmt",
	"assertTxnManagerAfterBuildExecutor",
}

func TestTxnContextForSimpleCases(t *testing.T) {
	store, do := setupTxnContextTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	is1 := do.InfoSchema()
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	// test for write
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("insert into t2 (id) values(3)")
	})
	// test for select
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 10"))
	})

	tk2.MustExec("alter table t2 add column(c1 int)")
	is2 := do.InfoSchema()
	require.True(t, is2.SchemaMetaVersion() > is1.SchemaMetaVersion())

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is2)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
}

func TestTxnContextInExplicitTxn(t *testing.T) {
	store, do := setupTxnContextTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_metadata_lock=0")
	tk.MustExec("use test")
	se := tk.Session()

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	is1 := do.InfoSchema()
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)

	tk.MustExec("begin")
	// test for write
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("insert into t2 (id) values(2)")
	})
	// test for select
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 10"))
	})

	// info schema changed when txn not finish, the info schema in old txn should not change
	tk2.MustExec("alter table t2 add column(c1 int)")
	is2 := do.InfoSchema()
	require.True(t, is2.SchemaMetaVersion() > is1.SchemaMetaVersion())

	// test for write
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("insert into t2 (id) values(2)")
	})
	// test for select
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 10"))
	})

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustGetErrCode("commit", errno.ErrInfoSchemaChanged)
	})

	// the info schema in new txn should use the newest one
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is2)
	tk.MustExec("begin")
	// test for write
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("insert into t2 (id) values(2)")
	})
	// test for select
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 10"))
	})
}

func TestTxnContextBeginInUnfinishedTxn(t *testing.T) {
	store, do := setupTxnContextTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	is1 := do.InfoSchema()
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	tk.MustExec("begin")

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})

	tk2.MustExec("alter table t2 add column(c1 int)")
	is2 := do.InfoSchema()
	require.True(t, is2.SchemaMetaVersion() > is1.SchemaMetaVersion())

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})

	tk.MustExec("begin")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is2)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	tk.MustExec("rollback")
}

func TestTxnContextWithAutocommitFalse(t *testing.T) {
	store, do := setupTxnContextTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=0")
	se := tk.Session()

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	is1 := do.InfoSchema()
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	tk.MustExec("begin")

	tk.MustExec("set autocommit=0")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, do.InfoSchema())
	// test for write
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("insert into t2 (id) values(2)")
	})

	// schema change should not affect because it is in txn
	tk2.MustExec("alter table t2 add column(c1 int)")

	// test for select
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 10"))
	})
	tk.MustExec("rollback")
}

func TestTxnContextInRC(t *testing.T) {
	store, do := setupTxnContextTest(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	is1 := do.InfoSchema()
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})

	tk.MustExec("begin pessimistic")

	// schema change should not affect even in rc isolation
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk2.MustExec("alter table t2 add column(c1 int)")

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	// test for write
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("insert into t2 (id) values(2)")
	})

	// test for select
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})

	tk2.MustExec("update t1 set v=11 where id=1")

	// test for select
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 11"))
	})

	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 11"))
	})

	tk.MustExec("rollback")
}

func TestTxnContextInPessimisticKeyConflict(t *testing.T) {
	store, do := setupTxnContextTest(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()
	is1 := do.InfoSchema()

	tk.MustExec("begin pessimistic")

	// trigger retry
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("update t1 set v=11 where id=1")
	tk2.MustExec("alter table t2 add column(c1 int)")

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	path := append([]string{"assertTxnManagerAfterPessimisticLockErrorRetry"}, normalPathRecords...)
	doWithCheckPath(t, se, path, func() {
		tk.MustExec("update t1 set v=12 where id=1")
	})

	tk.MustExec("rollback")
}

func TestTxnContextForHistoricalRead(t *testing.T) {
	store, do := setupTxnContextTest(t)
	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	safePoint := "20160102-15:04:05 -0700"
	tk.MustExec(fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%s', '') ON DUPLICATE KEY UPDATE variable_value = '%s', comment=''`, safePoint, safePoint))

	is1 := do.InfoSchema()
	tk.MustExec("do sleep(0.1)")
	tk.MustExec("set @a=now(6)")
	// change schema
	tk.MustExec("alter table t2 add column(c1 int)")
	tk.MustExec("update t1 set v=11 where id=1")

	tk.MustExec("set @@tidb_snapshot=@a")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 10"))
	})

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tidb_snapshot=''")
	tk.MustExec("begin")

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, do.InfoSchema())
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 11"))
	})

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 11"))
	})

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tidb_snapshot=@a")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 10"))
	})

	tk.MustExec("rollback")
}

func TestTxnContextForStaleRead(t *testing.T) {
	store, do := setupTxnContextTest(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	safePoint := "20160102-15:04:05 -0700"
	tk.MustExec(fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%s', '') ON DUPLICATE KEY UPDATE variable_value = '%s', comment=''`, safePoint, safePoint))

	is1 := do.InfoSchema()
	tk.MustExec("do sleep(0.1)")
	tk.MustExec("set @a=now(6)")
	time.Sleep(time.Millisecond * 1200)

	// change schema
	tk.MustExec("alter table t2 add column(c1 int)")
	tk.MustExec("update t1 set v=11 where id=1")

	// @@tidb_read_staleness
	tk.MustExec("set @@tidb_read_staleness=-1")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 as of timestamp @a").Check(testkit.Rows("1 10"))
	})
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tidb_read_staleness=''")

	// select ... as of ...
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 as of timestamp @a").Check(testkit.Rows("1 10"))
	})

	// @@tx_read_ts
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tx_read_ts=@a")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, do.InfoSchema())
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 11"))
	})

	// txn begin with @tx_read_ts
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tx_read_ts=@a")
	tk.MustExec("begin")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	tk.MustExec("rollback")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, do.InfoSchema())
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 11"))
	})

	// txn begin ... as of ...
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("start transaction read only as of timestamp @a")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	})
	tk.MustExec("rollback")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, do.InfoSchema())
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 11"))
	})
}

func TestTxnContextForPrepareExecute(t *testing.T) {
	store, do := setupTxnContextTest(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	se := tk.Session()

	stmtID, _, _, err := se.PrepareStmt("select * from t1 where id=1")
	require.NoError(t, err)

	is1 := do.InfoSchema()
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)

	// Test prepare/execute in SQL
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("prepare s from 'select * from t1 where id=1'")
	})
	doWithCheckPath(t, se, []string{"assertTxnManagerInCompile", "assertTxnManagerInShortPointGetPlan"}, func() {
		tk.MustQuery("execute s").Check(testkit.Rows("1 10"))
	})

	// Test ExecutePreparedStmt
	doWithCheckPath(t, se, []string{"assertTxnManagerInCompile", "assertTxnManagerInShortPointGetPlan"}, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})

	// Test PlanCache
	doWithCheckPath(t, se, nil, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})

	// In txn
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("begin")

	//change schema
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("alter table t2 add column(c1 int)")
	tk2.MustExec("update t1 set v=11 where id=1")

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("prepare s from 'select * from t1 where id=1'")
	})
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("execute s").Check(testkit.Rows("1 10"))
	})
	doWithCheckPath(t, se, normalPathRecords, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})

	tk.MustExec("rollback")
}

func TestStaleReadInPrepare(t *testing.T) {
	store, _ := setupTxnContextTest(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	tk.MustExec(`create table tt (id int primary key, v int)`)
	tk.MustExec(`insert into tt values(1, 10)`)
	tk.MustExec("do sleep(0.1)")
	tk.MustExec("set @a=now(6)")
	tk.MustExec("do sleep(0.1)")

	st, _, _, err := se.PrepareStmt("select v from tt where id=1")
	require.NoError(t, err)

	tk.MustExec(`update tt set v=11 where id=1`)
	rs, err := se.ExecutePreparedStmt(context.TODO(), st, nil)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("11"))

	tk.MustExec("set @@tx_read_ts=@a")
	rs, err = se.ExecutePreparedStmt(context.TODO(), st, nil)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("10"))

	tk.MustExec("update tt set v=12 where id=1")
	tk.MustExec("set @@tx_read_ts=''")
	rs, err = se.ExecutePreparedStmt(context.TODO(), st, nil)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("12"))
	rs, err = se.ExecutePreparedStmt(context.TODO(), st, nil)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("12"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestTxnContextForStaleReadInPrepare(t *testing.T) {
	store, _ := setupTxnContextTest(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	is1 := se.GetDomainInfoSchema()
	tk.MustExec("do sleep(0.1)")
	tk.MustExec("set @a=now(6)")
	tk.MustExec("prepare s1 from 'select * from t1 where id=1'")
	tk.MustExec("prepare s2 from 'select * from t1 as of timestamp @a where id=1 '")

	stmtID1, _, _, err := se.PrepareStmt("select * from t1 where id=1")
	require.NoError(t, err)

	stmtID2, _, _, err := se.PrepareStmt("select * from t1 as of timestamp @a where id=1 ")
	require.NoError(t, err)

	//change schema
	tk.MustExec("use test")
	tk.MustExec("alter table t2 add column(c1 int)")
	tk.MustExec("update t1 set v=11 where id=1")

	tk.MustExec("set @@tx_read_ts=@a")
	stmtID3, _, _, err := se.PrepareStmt("select * from t1 where id=1 ")
	require.NoError(t, err)
	tk.MustExec("set @@tx_read_ts=''")

	tk.MustExec("set @@tx_read_ts=@a")
	tk.MustExec("prepare s3 from 'select * from t1 where id=1 '")
	tk.MustExec("set @@tx_read_ts=''")

	// tx_read_ts
	tk.MustExec("set @@tx_read_ts=@a")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID1, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tx_read_ts=''")

	tk.MustExec("set @@tx_read_ts=@a")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("execute s1")
	})
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tx_read_ts=''")

	// select ... as of ...
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID2, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("execute s2")
	})

	// tx_read_ts in prepare
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID3, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("execute s3")
	})
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)

	// stale read should not use plan cache
	is2 := se.GetDomainInfoSchema()
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tx_read_ts=''")
	tk.MustExec("do sleep(0.1)")
	tk.MustExec("set @b=now(6)")
	tk.MustExec("do sleep(0.1)")
	tk.MustExec("update t1 set v=v+1 where id=1")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is2)
	doWithCheckPath(t, se, []string{"assertTxnManagerInCompile", "assertTxnManagerInShortPointGetPlan"}, func() {
		// stale-read is not used since `tx_read_ts` is empty, so the plan cache should be used in this case.
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID1, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 12"))
	})
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tx_read_ts=@b")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is2)
	doWithCheckPath(t, se, normalPathRecords, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID1, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 11"))
	})
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("set @@tx_read_ts=''")
}

func TestTxnContextPreparedStmtWithForUpdate(t *testing.T) {
	store, do := setupTxnContextTest(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_metadata_lock=0")
	tk.MustExec("use test")
	se := tk.Session()

	is1 := do.InfoSchema()

	stmtID1, _, _, err := se.PrepareStmt("select * from t1 where id=1 for update")
	require.NoError(t, err)
	tk.MustExec("prepare s from 'select * from t1 where id=1 for update'")
	tk.MustExec("begin pessimistic")

	//change schema
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("alter table t1 add column(c int default 100)")
	tk2.MustExec("update t1 set v=11 where id=1")

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 11"))
	})

	doWithCheckPath(t, se, normalPathRecords, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID1, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 11"))
	})

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("execute s").Check(testkit.Rows("1 11"))
	})

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("rollback")
}

// See issue: https://github.com/pingcap/tidb/issues/35459
func TestStillWriteConflictAfterRetry(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	queries := []string{
		"select * from t1 for update",
		"select * from t1 where id=1 for update",
		"select * from t1 where id in (1, 2, 3) for update",
		"select * from t1 where id=1 and v>0 for update",
		"select * from t1 where id=1 for update union select * from t1 where id=1 for update",
		"update t1 set v=v+1",
		"update t1 set v=v+1 where id=1",
		"update t1 set v=v+1 where id=1 and v>0",
		"update t1 set v=v+1 where id in (1, 2, 3)",
		"update t1 set v=v+1 where id in (1, 2, 3) and v>0",
	}

	testfork.RunTest(t, func(t *testfork.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("truncate table t1")
		tk.MustExec("insert into t1 values(1, 10)")
		// Fair locking avoids conflicting again after retry in this case. Disable it for this test.
		tk.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
		tk2 := testkit.NewSteppedTestKit(t, store)
		defer tk2.MustExec("rollback")

		tk2.MustExec("use test")
		tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
		tk2.MustExec(fmt.Sprintf("set tx_isolation = '%s'", testfork.PickEnum(t, ast.RepeatableRead, ast.ReadCommitted)))
		autocommit := testfork.PickEnum(t, 0, 1)
		tk2.MustExec(fmt.Sprintf("set autocommit=%d", autocommit))
		if autocommit == 1 {
			tk2.MustExec("begin")
		}

		tk2.SetBreakPoints(
			sessiontxn.BreakPointBeforeExecutorFirstRun,
			sessiontxn.BreakPointOnStmtRetryAfterLockError,
		)

		var isSelect, isUpdate bool
		query := testfork.Pick(t, queries)
		switch {
		case strings.HasPrefix(query, "select"):
			isSelect = true
			tk2.SteppedMustQuery(query)
		case strings.HasPrefix(query, "update"):
			isUpdate = true
			tk2.SteppedMustExec(query)
		default:
			require.FailNowf(t, "invalid query: ", query)
		}

		// Pause the session before the executor first run and then update the record in another session
		tk2.ExpectStopOnBreakPoint(sessiontxn.BreakPointBeforeExecutorFirstRun)
		tk.MustExec("update t1 set v=v+1")

		// Session continues, it should get a lock error and retry, we pause the session before the executor's next run
		// and then update the record in another session again.
		tk2.Continue().ExpectStopOnBreakPoint(sessiontxn.BreakPointOnStmtRetryAfterLockError)
		tk.MustExec("update t1 set v=v+1")

		// Because the record is updated by another session again, when this session continues, it will get a lock error again.
		tk2.Continue().ExpectStopOnBreakPoint(sessiontxn.BreakPointOnStmtRetryAfterLockError)
		tk2.Continue().ExpectIdle()
		switch {
		case isSelect:
			tk2.GetQueryResult().Check(testkit.Rows("1 12"))
		case isUpdate:
			tk2.MustExec("commit")
			tk2.MustQuery("select * from t1").Check(testkit.Rows("1 13"))
		}
	})
}

func TestOptimisticTxnRetryInPessimisticMode(t *testing.T) {
	store, _ := setupTxnContextTest(t)

	queries := []string{
		"update t1 set v=v+1",
		"update t1 set v=v+1 where id=1",
		"update t1 set v=v+1 where id=1 and v>0",
		"update t1 set v=v+1 where id in (1, 2, 3)",
		"update t1 set v=v+1 where id in (1, 2, 3) and v>0",
	}

	testfork.RunTest(t, func(t *testfork.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("truncate table t1")
		tk.MustExec("insert into t1 values(1, 10)")
		tk2 := testkit.NewSteppedTestKit(t, store)
		defer tk2.MustExec("rollback")

		tk2.MustExec("use test")
		tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk2.MustExec("set autocommit = 1")

		// When autocommit meets write conflict, it will retry in pessimistic mode.
		// conflictAfterTransfer being true means we encounter a write-conflict again during
		// the pessimistic mode.
		// doubleConflictAfterTransfer being true means we encounter a write-conflict again
		// during the pessimistic retry phase.
		// And only conflictAfterTransfer being true allows doubleConflictAfterTransfer being true.
		conflictAfterTransfer := testfork.PickEnum(t, true, false)
		doubleConflictAfterTransfer := testfork.PickEnum(t, true, false)
		if !conflictAfterTransfer && doubleConflictAfterTransfer {
			return
		}

		// If `tidb_pessimistic_txn_fair_locking` is enabled, the double conflict case is
		// avoided. Disable it to run this test.
		if doubleConflictAfterTransfer {
			tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
		}

		tk2.SetBreakPoints(
			sessiontxn.BreakPointBeforeExecutorFirstRun,
			sessiontxn.BreakPointOnStmtRetryAfterLockError,
		)

		query := testfork.Pick(t, queries)

		tk2.SteppedMustExec(query)

		// Pause the session before the executor first run and then update the record in another session
		tk2.ExpectStopOnBreakPoint(sessiontxn.BreakPointBeforeExecutorFirstRun)
		// After this update, tk2's statement will encounter write conflict. As it's an autocommit transaction,
		// it will transfer to pessimistic transaction mode.
		tk.MustExec("update t1 set v=v+1")

		if conflictAfterTransfer {
			tk2.Continue().ExpectStopOnBreakPoint(sessiontxn.BreakPointBeforeExecutorFirstRun)
			tk.MustExec("update t1 set v=v+1")

			if doubleConflictAfterTransfer {
				// Session continues, it should get a lock error and retry, we pause the session before the executor's next run
				// and then update the record in another session again.
				tk2.Continue().ExpectStopOnBreakPoint(sessiontxn.BreakPointOnStmtRetryAfterLockError)
				tk.MustExec("update t1 set v=v+1")
			}

			// Because the record is updated by another session again, when this session continues, it will get a lock error again.
			tk2.Continue().ExpectStopOnBreakPoint(sessiontxn.BreakPointOnStmtRetryAfterLockError)
			tk2.Continue().ExpectIdle()

			if doubleConflictAfterTransfer {
				tk2.MustQuery("select * from t1").Check(testkit.Rows("1 14"))
			} else {
				tk2.MustQuery("select * from t1").Check(testkit.Rows("1 13"))
			}
		} else {
			tk2.Continue().ExpectStopOnBreakPoint(sessiontxn.BreakPointBeforeExecutorFirstRun)
			tk2.Continue().ExpectIdle()

			tk2.MustQuery("select * from t1").Check(testkit.Rows("1 12"))
		}
	})
}

func TestTSOCmdCountForPrepareExecute(t *testing.T) {
	// This is a mock workload mocks one which discovers that the tso request count is abnormal.
	// After the bug fix, the tso request count recovers, so we use this workload to record the current tso request count
	// to reject future works that accidentally causes tso request increasing.
	// Note, we do not record all tso requests but some typical requests.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD"))
	}()
	store := testkit.CreateMockStore(t)

	ctx := context.Background()
	tk := testkit.NewTestKit(t, store)
	sctx := tk.Session()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")

	tk.MustExec("create table t1(id int, v int, v2 int, primary key (id), unique key uk (v))")
	tk.MustExec("create table t2(id int, v int, unique key i1(v))")
	tk.MustExec("create table t3(id int, v int, key i1(v))")

	sqlSelectID, _, _, _ := tk.Session().PrepareStmt("select * from t1 where id = ? for update")
	sqlUpdateID, _, _, _ := tk.Session().PrepareStmt("update t1 set v = v + 10 where id = ?")
	sqlInsertID1, _, _, _ := tk.Session().PrepareStmt("insert into t2 values(?, ?)")
	sqlInsertID2, _, _, _ := tk.Session().PrepareStmt("insert into t3 values(?, ?)")

	tk.MustExec("insert into t1 values (1, 1, 1)")
	sctx.SetValue(sessiontxn.TsoRequestCount, 0)

	for i := 1; i < 100; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlSelectID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.Nil(t, stmt)

		val := i * 10
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlInsertID1, expression.Args2Expressions4Test(val, val))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlInsertID2, expression.Args2Expressions4Test(val, val))
		require.NoError(t, err)
		require.Nil(t, stmt)
		tk.MustExec("commit")
	}
	count := sctx.Value(sessiontxn.TsoRequestCount)
	require.Equal(t, uint64(99), count)
}

func TestTSOCmdCountForTextSql(t *testing.T) {
	// This is a mock workload mocks one which discovers that the tso request count is abnormal.
	// After the bug fix, the tso request count recovers, so we use this workload to record the current tso request count
	// to reject future works that accidentally causes tso request increasing.
	// Note, we do not record all tso requests but some typical requests.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD"))
	}()
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sctx := tk.Session()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")

	tk.MustExec("create table t1(id int, v int, v2 int, primary key (id), unique key uk (v))")
	tk.MustExec("create table t2(id int, v int, unique key i1(v))")
	tk.MustExec("create table t3(id int, v int, key i1(v))")

	tk.MustExec("insert into t1 values (1, 1, 1)")
	sctx.SetValue(sessiontxn.TsoRequestCount, 0)
	for i := 1; i < 100; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustQuery("select * from t1 where id = 1 for update")
		tk.MustExec("update t1 set v = v + 10 where id = 1")
		val := i * 10
		tk.MustExec(fmt.Sprintf("insert into t2 values(%v, %v)", val, val))
		tk.MustExec(fmt.Sprintf("insert into t3 values(%v, %v)", val, val))
		tk.MustExec("commit")
	}
	count := sctx.Value(sessiontxn.TsoRequestCount)
	require.Equal(t, uint64(99), count)
}
