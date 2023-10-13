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

package ddl_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/stretchr/testify/require"
)

func checkTableCacheStatus(t *testing.T, tk *testkit.TestKit, dbName, tableName string, status model.TableCacheStatusType) {
	tb := external.GetTableByName(t, tk, dbName, tableName)
	dom := domain.GetDomain(tk.Session())
	err := dom.Reload()
	require.NoError(t, err)
	require.Equal(t, status, tb.Meta().TableCacheStatusType)
}

func TestAlterTableCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	dom.SetStatsUpdating(true)

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk2.MustExec("use test")
	/* Test of cache table */
	tk.MustExec("create table t1 ( n int auto_increment primary key)")
	tk.MustGetErrCode("alter table t1 ca", errno.ErrParse)
	tk.MustGetErrCode("alter table t2 cache", errno.ErrNoSuchTable)
	tk.MustExec("alter table t1 cache")
	checkTableCacheStatus(t, tk, "test", "t1", model.TableCacheStatusEnable)
	tk.MustExec("alter table t1 nocache")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("set global tidb_enable_metadata_lock=0")
	/*Test can't skip schema checker*/
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 cache;")
	tk.MustGetDBError("commit", domain.ErrInfoSchemaChanged)
	/* Test can skip schema checker */
	tk.MustExec("begin")
	tk.MustExec("alter table t1 nocache")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("insert into t1 set a=2;")
	tk2.MustExec("alter table t2 cache")
	tk.MustExec("commit")
	// Test if a table is not exists
	tk.MustExec("drop table if exists t")
	tk.MustGetErrCode("alter table t cache", errno.ErrNoSuchTable)
	tk.MustExec("create table t (a int)")
	tk.MustExec("alter table t cache")
	// Multiple alter cache is okay
	tk.MustExec("alter table t cache")
	tk.MustExec("alter table t cache")
	// Test a temporary table
	tk.MustExec("alter table t nocache")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create temporary table t (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("drop table if exists tmp1")
	// local temporary table alter is not supported
	tk.MustGetErrCode("alter table t cache", errno.ErrUnsupportedDDLOperation)
	// test global temporary table
	tk.MustExec("create global temporary table tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))" +
		"on commit delete rows")
	tk.MustGetErrMsg("alter table tmp1 cache", dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache").Error())
}

func TestCacheTableSizeLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists cache_t1")
	tk.MustExec("create table cache_t1 (c1 int, c varchar(1024))")
	tk.MustExec("create table cache_t2 (c1 int, c varchar(1024))")
	tk.MustExec("create table tmp (c1 int, c varchar(1024))")
	defer tk.MustExec("drop table if exists cache_t1")

	for i := 0; i < 64; i++ {
		tk.MustExec("insert into tmp values (?, repeat('x', 1024));", i)
	}

	// Make the cache_t1 size large than 64K
	for i := 0; i < 1024; i++ {
		tk.MustExec("insert into cache_t1 select * from tmp;")
		if i == 900 {
			tk.MustExec("insert into cache_t2 select * from cache_t1;")
		}
	}
	// Check 'alter table cache' fail
	tk.MustGetErrCode("alter table cache_t1 cache", errno.ErrOptOnCacheTable)

	// Check 'alter table cache' success
	tk.MustExec("alter table cache_t2 cache")

	// But after continuously insertion, the table reachs the size limit
	for i := 0; i < 124; i++ {
		_, err := tk.Exec("insert into cache_t2 select * from tmp;")
		// The size limit check is not accurate, so it's not detected here.
		require.NoError(t, err)
	}

	lastReadFromCache := func(tk *testkit.TestKit) bool {
		return tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
	}

	cached := false
	for i := 0; i < 200; i++ {
		tk.MustQuery("select count(*) from (select * from cache_t2 limit 1) t1").Check(testkit.Rows("1"))
		if lastReadFromCache(tk) {
			cached = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// require.True(t, cached)
	if !cached {
		// cached should be true, but it depends on the hardward.
		// If the CI environment is too slow, 200 iteration would not be enough,
		// check the result makes this test unstable, so skip the following.
		return
	}

	// Forbit the insert once the table size limit is detected.
	tk.MustGetErrCode("insert into cache_t2 select * from tmp;", errno.ErrOptOnCacheTable)
}

func TestIssue34069(t *testing.T) {
	store := testkit.CreateMockStore(t)
	sem.Enable()
	defer sem.Disable()

	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test;")
	tk.MustExec("create table t_34069 (t int);")
	// No error when SEM is enabled.
	tk.MustExec("alter table t_34069 cache")
}
