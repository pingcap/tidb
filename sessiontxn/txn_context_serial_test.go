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
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func setupTxnContextTest(t *testing.T) (kv.Storage, *domain.Domain, func()) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertTxnManagerAfterCompile", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertTxnManagerInRebuildPlan", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertTxnManagerAfterBuildExecutor", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/assertTxnManagerInRunStmt", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/assertTxnManagerInPreparedStmtExec", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/assertTxnManagerInCachedPlanExec", "return"))

	store, do, clean := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.Session().SetValue(sessiontxn.AssertRecordsKey, nil)
	tk.Session().SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")

	tk.MustExec("create table t1 (id int)")
	tk.MustExec("insert into t1 values(1)")

	tk.MustExec("create table t2 (id int)")

	tk.MustExec("create temporary table tmp (id int)")
	tk.MustExec("insert into tmp values(10)")

	return store, do, func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertTxnManagerAfterCompile"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertTxnManagerInRebuildPlan"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertTxnManagerAfterBuildExecutor"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/assertTxnManagerInRunStmt"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/assertTxnManagerInPreparedStmtExec"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/assertTxnManagerInCachedPlanExec"))

		tk.Session().SetValue(sessiontxn.AssertRecordsKey, nil)
		tk.Session().SetValue(sessiontxn.AssertTxnInfoSchemaKey, nil)
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
	"assertTxnManagerAfterCompile",
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
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 for update").Check(testkit.Rows("1"))
	})

	tk2.MustExec("alter table t2 add column(c1 int)")
	is2 := do.InfoSchema()
	require.True(t, is2.SchemaMetaVersion() > is1.SchemaMetaVersion())

	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is2)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
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
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 for update").Check(testkit.Rows("1"))
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
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 for update").Check(testkit.Rows("1"))
	})

	tk.MustExec("rollback")

	// the info schema in new txn should use the newest one
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is2)
	tk.MustExec("begin")
	// test for write
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustExec("insert into t2 (id) values(2)")
	})
	// test for select
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 for update").Check(testkit.Rows("1"))
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
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	})

	tk2.MustExec("alter table t2 add column(c1 int)")
	is2 := do.InfoSchema()
	require.True(t, is2.SchemaMetaVersion() > is1.SchemaMetaVersion())

	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	})

	tk.MustExec("begin")
	se.SetValue(sessiontxn.AssertTxnInfoSchemaKey, is2)
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
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
		tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	})
	// test for select for update
	doWithCheckPath(t, se, normalPathRecords, func() {
		tk.MustQuery("select * from t1 for update").Check(testkit.Rows("1"))
	})
	tk.MustExec("rollback")
}
