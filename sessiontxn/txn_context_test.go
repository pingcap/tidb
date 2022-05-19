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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func setupTxnContextTest(t *testing.T) (kv.Storage, *domain.Domain, func()) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertTxnManagerInCompile", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertTxnManagerInRebuildPlan", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertTxnManagerAfterBuildExecutor", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertTxnManagerAfterPessimisticLockErrorRetry", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertTxnManagerInShortPointGetPlan", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertStaleReadValuesSameWithExecuteAndBuilder", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertNotStaleReadForExecutorGetReadTS", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/assertTxnManagerInRunStmt", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/assertTxnManagerInPreparedStmtExec", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/assertTxnManagerInCachedPlanExec", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/core/assertStaleReadForOptimizePreparedPlan", "return"))

	store, do, clean := testkit.CreateMockStoreAndDomain(t)

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

	return store, do, func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertTxnManagerInCompile"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertTxnManagerInRebuildPlan"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertTxnManagerAfterBuildExecutor"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertTxnManagerAfterPessimisticLockErrorRetry"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertTxnManagerInShortPointGetPlan"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertStaleReadValuesSameWithExecuteAndBuilder"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertNotStaleReadForExecutorGetReadTS"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/assertTxnManagerInRunStmt"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/assertTxnManagerInPreparedStmtExec"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/assertTxnManagerInCachedPlanExec"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/core/assertStaleReadForOptimizePreparedPlan"))

		tk.Session().SetValue(sessiontxn.AssertRecordsKey, nil)
		tk.Session().SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
		tk.Session().SetValue(sessiontxn.AssertTxnInfoSchemaAfterRetryKey, nil)
		clean()
	}
}

func checkAssertRecordExits(t *testing.T, se sessionctx.Context, name string) {
	records, ok := se.Value(sessiontxn.AssertRecordsKey).(map[string]interface{})
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
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()

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
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()

	tk := testkit.NewTestKit(t, store)
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
		tk.MustExec("commit")
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
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()

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
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()

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
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()
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

func TestTxnContextInOptimisticRetry(t *testing.T) {
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_disable_txn_auto_retry=0")
	se := tk.Session()
	is1 := do.InfoSchema()

	tk.MustExec("begin optimistic")

	// trigger retry
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("update t1 set v=11 where id=1")
	tk2.MustExec("alter table t2 add column(c1 int)")

	tk.MustExec("update t1 set v=12 where id=1")

	// check retry context
	path := append([]string{"assertTxnManagerInRebuildPlan"}, normalPathRecords...)
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	se.SetValue(sessiontxn.AssertTxnInfoSchemaAfterRetryKey, do.InfoSchema())
	doWithCheckPath(t, se, path, func() {
		tk.MustExec("commit")
	})

	tk.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 12"))
}

func TestTxnContextForHistoricalRead(t *testing.T) {
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()
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
		tk.MustQuery("select * from t1 where id=1 for update").Check(testkit.Rows("1 11"))
	})

	tk.MustExec("rollback")
}

func TestTxnContextForStaleRead(t *testing.T) {
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()
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
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()
	orgEnable := core.PreparedPlanCacheEnabled()
	defer core.SetPreparedPlanCache(orgEnable)
	core.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	stmtID, _, _, err := se.PrepareStmt("select * from t1 where id=1")
	require.NoError(t, err)

	is1 := do.InfoSchema()
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)

	// Test prepare/execute in SQL
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("prepare s from 'select * from t1 where id=1'")
	})
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("execute s").Check(testkit.Rows("1 10"))
	})

	// Test ExecutePreparedStmt
	path := append([]string{"assertTxnManagerInPreparedStmtExec"}, normalPathRecords...)
	doWithCheckPath(t, se, path, func() {
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
	path = append([]string{"assertTxnManagerInPreparedStmtExec"}, normalPathRecords...)
	doWithCheckPath(t, se, path, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})

	tk.MustExec("rollback")
}

func TestTxnContextForStaleReadInPrepare(t *testing.T) {
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	is1 := do.InfoSchema()
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
	path := append([]string{"assertTxnManagerInPreparedStmtExec"}, normalPathRecords...)
	doWithCheckPath(t, se, path, func() {
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
	doWithCheckPath(t, se, path, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID2, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("execute s2")
	})

	// tx_read_ts in prepare
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is1)
	doWithCheckPath(t, se, path, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID3, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	})
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("execute s3")
	})
}

func TestTxnContextPreparedStmtWithForUpdate(t *testing.T) {
	store, do, deferFunc := setupTxnContextTest(t)
	defer deferFunc()
	tk := testkit.NewTestKit(t, store)
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

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, do.InfoSchema())
	path := append([]string{"assertTxnManagerInPreparedStmtExec"}, normalPathRecords...)
	doWithCheckPath(t, se, path, func() {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID1, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 11 100"))
	})

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("execute s").Check(testkit.Rows("1 11 100"))
	})

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
	tk.MustExec("rollback")
}
