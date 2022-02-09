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

package tables_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/stmtsummary"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func lastReadFromCache(tk *testkit.TestKit) bool {
	return tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
}

func TestCacheTableBasicScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp1")
	tk.MustExec("create  table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values" +
		"(1, 101, 1001), (3, 113, 1003), (5, 105, 1005), (7, 117, 1007), (9, 109, 1009)," +
		"(10, 110, 1010), (12, 112, 1012), (14, 114, 1014), (16, 116, 1016), (18, 118, 1018)",
	)
	tk.MustExec("alter table tmp1 cache")

	// For TableReader
	// First read will read from original table
	tk.MustQuery("select * from tmp1 where id>3 order by id").Check(testkit.Rows(
		"5 105 1005", "7 117 1007", "9 109 1009",
		"10 110 1010", "12 112 1012", "14 114 1014", "16 116 1016", "18 118 1018",
	))
	// Test for join two cache table
	tk.MustExec("drop table if exists join_t1, join_t2, join_t3")
	tk.MustExec("create table join_t1  (id int)")
	tk.MustExec("insert into join_t1 values(1)")
	tk.MustExec("alter table join_t1 cache")
	tk.MustQuery("select *from join_t1").Check(testkit.Rows("1"))
	tk.MustExec("create table join_t2  (id int)")
	tk.MustExec("insert into join_t2 values(2)")
	tk.MustExec("alter table join_t2 cache")
	tk.MustQuery("select *from join_t2").Check(testkit.Rows("2"))
	tk.MustExec("create table join_t3 (id int)")
	tk.MustExec("insert into join_t3 values(3)")
	planUsed := false
	for i := 0; i < 10; i++ {
		tk.MustQuery("select *from join_t1 join join_t2").Check(testkit.Rows("1 2"))
		if lastReadFromCache(tk) {
			planUsed = true
			break
		}
	}
	require.True(t, planUsed)

	// Test for join a cache table and a normal table
	for i := 0; i < 10; i++ {
		tk.MustQuery("select * from join_t1 join join_t3").Check(testkit.Rows("1 3"))
		if lastReadFromCache(tk) {
			// if tk.HasPlan("select *from join_t1 join join_t3", "UnionScan") {
			planUsed = true
			break
		}
	}
	require.True(t, planUsed)

	// Second read will from cache table
	for i := 0; i < 100; i++ {
		tk.MustQuery("select * from tmp1 where id>4 order by id").Check(testkit.Rows(
			"5 105 1005", "7 117 1007", "9 109 1009",
			"10 110 1010", "12 112 1012", "14 114 1014", "16 116 1016", "18 118 1018",
		))
		if lastReadFromCache(tk) {
			// if tk.HasPlan("select * from tmp1 where id>4 order by id", "UnionScan") {
			planUsed = true
			break
		}
	}
	require.True(t, planUsed)

	// For IndexLookUpReader
	for i := 0; i < 10; i++ {
		tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u").Check(testkit.Rows(
			"5 105 1005", "9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))
		if lastReadFromCache(tk) {
			planUsed = true
			break
		}
	}
	require.True(t, planUsed)

	// For IndexReader
	tk.MustQuery("select /*+ use_index(tmp1, u) */ id,u from tmp1 where u>101 order by id").Check(testkit.Rows(
		"3 113", "5 105", "7 117", "9 109", "10 110",
		"12 112", "14 114", "16 116", "18 118",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// For IndexMerge, cache table should not use index merge
	tk.MustQuery("select /*+ use_index_merge(tmp1, primary, u) */ * from tmp1 where id>5 or u>110 order by u").Check(testkit.Rows(
		"9 109 1009", "10 110 1010",
		"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
	))

	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestCacheCondition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (id int primary key, v int)")
	tk.MustExec("alter table t2 cache")

	// Explain should not trigger cache.
	for i := 0; i < 10; i++ {
		tk.MustQuery("explain select * from t2")
		time.Sleep(100 * time.Millisecond)
		require.False(t, lastReadFromCache(tk))
	}

	// Insert should not trigger cache.
	tk.MustExec("insert into t2 values (1,1)")
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		require.False(t, lastReadFromCache(tk))
	}

	// Update should not trigger cache.
	tk.MustExec("update t2 set v = v + 1 where id > 0")
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		require.False(t, lastReadFromCache(tk))
	}
	// Contains PointGet Update should not trigger cache.
	tk.MustExec("update t2 set v = v + 1 where id = 2")
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		require.False(t, lastReadFromCache(tk))
	}

	// Contains PointGet Delete should not trigger cache.
	tk.MustExec("delete from t2 where id = 2")
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		require.False(t, lastReadFromCache(tk))
	}

	// Normal query should trigger cache.
	tk.MustQuery("select * from t2")
	cacheUsed := false
	for i := 0; i < 100; i++ {
		tk.MustQuery("select * from t2")
		if lastReadFromCache(tk) {
			cacheUsed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, cacheUsed)
}

func TestCacheTableBasicReadAndWrite(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk.MustExec("drop table if exists write_tmp1")
	tk.MustExec("create  table write_tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into write_tmp1 values" +
		"(1, 101, 1001), (3, 113, 1003)",
	)

	tk.MustExec("alter table write_tmp1 cache")
	// Read and add read lock
	tk.MustQuery("select * from write_tmp1").Check(testkit.Rows("1 101 1001", "3 113 1003"))
	// read lock should valid
	var i int
	for i = 0; i < 10; i++ {
		if lastReadFromCache(tk) {
			break
		}
		// Wait for the cache to be loaded.
		time.Sleep(50 * time.Millisecond)
		tk.MustQuery("select * from write_tmp1").Check(testkit.Rows("1 101 1001", "3 113 1003"))
	}
	require.True(t, i < 10)

	tk.MustExec("use test")
	tk1.MustExec("insert into write_tmp1 values (2, 222, 222)")
	// write lock exists
	tk.MustQuery("select * from write_tmp1").Check(testkit.Rows("1 101 1001",
		"2 222 222",
		"3 113 1003"))
	require.False(t, lastReadFromCache(tk))

	// wait write lock expire and check cache can be used again
	for !lastReadFromCache(tk) {
		tk.MustQuery("select * from write_tmp1").Check(testkit.Rows(
			"1 101 1001",
			"2 222 222",
			"3 113 1003"))
	}
	tk.MustQuery("select * from write_tmp1").Check(testkit.Rows("1 101 1001", "2 222 222", "3 113 1003"))
	tk1.MustExec("update write_tmp1 set v = 3333 where id = 2")
	for !lastReadFromCache(tk) {
		tk.MustQuery("select * from write_tmp1").Check(testkit.Rows("1 101 1001", "2 222 222", "3 113 1003"))
	}
	tk.MustQuery("select * from write_tmp1").Check(testkit.Rows("1 101 1001", "2 222 3333", "3 113 1003"))
}

func TestCacheTableComplexRead(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("create table complex_cache (id int primary key auto_increment, u int unique, v int)")
	tk1.MustExec("insert into complex_cache values" + "(5, 105, 1005), (7, 117, 1007), (9, 109, 1009)")
	tk1.MustExec("alter table complex_cache cache")
	tk1.MustQuery("select * from complex_cache where id > 7").Check(testkit.Rows("9 109 1009"))
	var i int
	for i = 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		tk1.MustQuery("select * from complex_cache where id > 7").Check(testkit.Rows("9 109 1009"))
		if lastReadFromCache(tk1) {
			break
		}
	}
	require.True(t, i < 10)

	tk1.MustExec("begin")
	tk2.MustExec("begin")
	tk2.MustQuery("select * from complex_cache where id > 7").Check(testkit.Rows("9 109 1009"))
	for i = 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		tk2.MustQuery("select * from complex_cache where id > 7").Check(testkit.Rows("9 109 1009"))
		if lastReadFromCache(tk2) {
			break
		}
	}
	require.True(t, i < 10)
	tk2.MustExec("commit")

	tk1.MustQuery("select * from complex_cache where id > 7").Check(testkit.Rows("9 109 1009"))
	require.True(t, lastReadFromCache(tk1))
	tk1.MustExec("commit")
}

func TestBeginSleepABA(t *testing.T) {
	// During the change "cache1 -> no cache -> cache2",
	// cache1 and cache2 may be not the same anymore
	// A transaction should not only check the cache exists, but also check the cache unchanged.

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists aba")
	tk1.MustExec("create table aba (id int, v int)")
	tk1.MustExec("insert into aba values (1, 1)")
	tk1.MustExec("alter table aba cache")
	tk1.MustQuery("select * from aba").Check(testkit.Rows("1 1"))
	cacheUsed := false
	for i := 0; i < 100; i++ {
		tk1.MustQuery("select * from aba").Check(testkit.Rows("1 1"))
		if lastReadFromCache(tk1) {
			cacheUsed = true
			break
		}
	}
	require.True(t, cacheUsed)

	// Begin, read from cache.
	tk1.MustExec("begin")
	cacheUsed = false
	for i := 0; i < 100; i++ {
		tk1.MustQuery("select * from aba").Check(testkit.Rows("1 1"))
		if lastReadFromCache(tk1) {
			cacheUsed = true
			break
		}
	}
	require.True(t, cacheUsed)

	// Another session change the data and make the cache unavailable.
	tk2.MustExec("update aba set v = 2")

	// And then make the cache available again.
	cacheUsed = false
	for i := 0; i < 100; i++ {
		tk2.MustQuery("select * from aba").Check(testkit.Rows("1 2"))
		if lastReadFromCache(tk2) {
			cacheUsed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, cacheUsed)

	// tk1 should not use the staled cache, because the data is changed.
	tk1.MustQuery("select * from aba").Check(testkit.Rows("1 1"))
	require.False(t, lastReadFromCache(tk1))
}

func TestCacheTablePointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cache_point (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into cache_point values(1, 11, 101)")
	tk.MustExec("insert into cache_point values(2, 12, 102)")
	tk.MustExec("alter table cache_point cache")
	// check point get out transaction
	tk.MustQuery("select * from cache_point where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from cache_point where u=11").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from cache_point where id=2").Check(testkit.Rows("2 12 102"))
	tk.MustQuery("select * from cache_point where u=12").Check(testkit.Rows("2 12 102"))
	tk.MustQuery("select * from cache_point where u > 10 and u < 12").Check(testkit.Rows("1 11 101"))

	// check point get in transaction
	tk.MustExec("begin")
	tk.MustQuery("select * from cache_point where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from cache_point where u=11").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from cache_point where id=2").Check(testkit.Rows("2 12 102"))
	tk.MustQuery("select * from cache_point where u=12").Check(testkit.Rows("2 12 102"))
	tk.MustExec("insert into cache_point values(3, 13, 103)")
	tk.MustQuery("select * from cache_point where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from cache_point where u=13").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from cache_point where u > 12 and u < 14").Check(testkit.Rows("3 13 103"))
	tk.MustExec("update cache_point set v=999 where id=2")
	tk.MustQuery("select * from cache_point where id=2").Check(testkit.Rows("2 12 999"))
	tk.MustExec("commit")

	// check point get after transaction
	tk.MustQuery("select * from cache_point where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from cache_point where u=13").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from cache_point where id=2").Check(testkit.Rows("2 12 999"))
}

func TestCacheTableBatchPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create  table bp_cache_tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into bp_cache_tmp1 values(1, 11, 101)")
	tk.MustExec("insert into bp_cache_tmp1 values(2, 12, 102)")
	tk.MustExec("insert into bp_cache_tmp1 values(3, 13, 103)")
	tk.MustExec("insert into bp_cache_tmp1 values(4, 14, 104)")
	tk.MustExec("alter table bp_cache_tmp1 cache")
	// check point get out transaction
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 3, 5)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 13, 15)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 13) and u in (12, 13)").Check(testkit.Rows("3 13 103"))
	// check point get in transaction
	tk.MustExec("begin")
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 3, 5)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 13, 15)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustExec("insert into bp_cache_tmp1 values(6, 16, 106)")
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 6)").Check(testkit.Rows("1 11 101", "6 16 106"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 16)").Check(testkit.Rows("1 11 101", "6 16 106"))
	tk.MustExec("update bp_cache_tmp1 set v=999 where id=3")
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 999"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 999"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 13) and u in (12, 13)").Check(testkit.Rows("3 13 999"))
	tk.MustExec("delete from bp_cache_tmp1 where id=4")
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 4)").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 14)").Check(testkit.Rows("1 11 101"))
	tk.MustExec("commit")

	// check point get after transaction
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 3, 6)").Check(testkit.Rows("1 11 101", "3 13 999", "6 16 106"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 13, 16)").Check(testkit.Rows("1 11 101", "3 13 999", "6 16 106"))
	tk.MustQuery("select * from bp_cache_tmp1 where id in (1, 4)").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from bp_cache_tmp1 where u in (11, 14)").Check(testkit.Rows("1 11 101"))
}

func TestRenewLease(t *testing.T) {
	// Test RenewLeaseForRead
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()
	tk.MustExec("create table cache_renew_t (id int)")
	tk.MustExec("alter table cache_renew_t cache")
	tbl, err := se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("cache_renew_t"))
	require.NoError(t, err)
	var i int
	tk.MustExec("select * from cache_renew_t")

	tk1 := testkit.NewTestKit(t, store)
	remote := tables.NewStateRemote(tk1.Session())
	var leaseBefore uint64
	for i = 0; i < 20; i++ {
		time.Sleep(200 * time.Millisecond)
		lockType, lease, err := remote.Load(context.Background(), tbl.Meta().ID)
		require.NoError(t, err)
		if lockType == tables.CachedTableLockRead {
			leaseBefore = lease
			break
		}
	}
	require.True(t, i < 20)

	for i = 0; i < 20; i++ {
		time.Sleep(200 * time.Millisecond)
		tk.MustExec("select * from cache_renew_t")
		lockType, lease, err := remote.Load(context.Background(), tbl.Meta().ID)
		require.NoError(t, err)
		require.Equal(t, lockType, tables.CachedTableLockRead)
		if leaseBefore != lease {
			break
		}
	}
	require.True(t, i < 20)
}

func TestCacheTableWriteOperatorWaitLockLease(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	se := tk.Session()

	// This line is a hack, if auth user string is "", the statement summary is skipped,
	// so it's added to make the later code been covered.
	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))

	tk.MustExec("drop table if exists wait_tb1")
	tk.MustExec("create table wait_tb1(id int)")
	tk.MustExec("alter table wait_tb1 cache")
	var i int
	for i = 0; i < 10; i++ {
		tk.MustQuery("select * from wait_tb1").Check(testkit.Rows())
		if lastReadFromCache(tk) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, i < 10)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("insert into wait_tb1 values(1)")
	require.True(t, se.GetSessionVars().StmtCtx.WaitLockLeaseTime > 0)

	tk.MustQuery("select DIGEST_TEXT from INFORMATION_SCHEMA.STATEMENTS_SUMMARY where MAX_BACKOFF_TIME > 0 or MAX_WAIT_TIME > 0").Check(testkit.Rows("insert into `wait_tb1` values ( ? )"))
}

func TestTableCacheLeaseVariable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Check default value.
	tk.MustQuery("select @@global.tidb_table_cache_lease").Check(testkit.Rows("3"))

	// Check a valid value.
	tk.MustExec("set @@global.tidb_table_cache_lease = 1;")
	tk.MustQuery("select @@global.tidb_table_cache_lease").Check(testkit.Rows("1"))

	// Check a invalid value, the valid range is [2, 10]
	tk.MustExec("set @@global.tidb_table_cache_lease = 111;")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_table_cache_lease value: '111'"))
	tk.MustQuery("select @@global.tidb_table_cache_lease").Check(testkit.Rows("10"))

	// Change to a non-default value and verify the behaviour.
	tk.MustExec("set @@global.tidb_table_cache_lease = 2;")

	tk.MustExec("drop table if exists test_lease_variable;")
	tk.MustExec(`create table test_lease_variable(c0 int, c1 varchar(20), c2 varchar(20), unique key uk(c0));`)
	tk.MustExec(`insert into test_lease_variable(c0, c1, c2) values (1, null, 'green');`)
	tk.MustExec(`alter table test_lease_variable cache;`)

	cached := false
	for i := 0; i < 20; i++ {
		tk.MustQuery("select * from test_lease_variable").Check(testkit.Rows("1 <nil> green"))
		if lastReadFromCache(tk) {
			cached = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, cached)

	start := time.Now()
	tk.MustExec("update test_lease_variable set c0 = 2")
	duration := time.Since(start)

	// The lease is 2s, check how long the write operation takes.
	require.True(t, duration > time.Second)
	require.True(t, duration < 3*time.Second)
}

func TestMetrics(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_metrics;")
	tk.MustExec(`create table test_metrics(c0 int, c1 varchar(20), c2 varchar(20), unique key uk(c0));`)
	tk.MustExec(`create table nt (c0 int, c1 varchar(20), c2 varchar(20), unique key uk(c0));`)
	tk.MustExec(`insert into test_metrics(c0, c1, c2) values (1, null, 'green');`)
	tk.MustExec(`alter table test_metrics cache;`)

	tk.MustQuery("select * from test_metrics").Check(testkit.Rows("1 <nil> green"))
	cached := false
	for i := 0; i < 20; i++ {
		if tk.HasPlan("select * from test_metrics", "UnionScan") {
			cached = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, cached)

	counter := metrics.ReadFromTableCacheCounter
	pb := &dto.Metric{}

	queries := []string{
		// Table scan
		"select * from test_metrics",
		// Index scan
		"select c0 from test_metrics use index(uk) where c0 > 1",
		// Index Lookup
		"select c1 from test_metrics use index(uk) where c0 = 1",
		// Point Get
		"select c0 from test_metrics use index(uk) where c0 = 1",
		// // Aggregation
		"select count(*) from test_metrics",
		// Join
		"select * from test_metrics as a join test_metrics as b on a.c0 = b.c0 where a.c1 != 'xxx'",
	}
	counter.Write(pb)
	i := pb.GetCounter().GetValue()

	for _, query := range queries {
		tk.MustQuery(query)
		i++
		counter.Write(pb)
		hit := pb.GetCounter().GetValue()
		require.Equal(t, float64(i), hit)
	}

	// A counter-example that doesn't increase metrics.ReadFromTableCacheCounter.
	tk.MustQuery("select * from nt")
	counter.Write(pb)
	hit := pb.GetCounter().GetValue()
	require.Equal(t, float64(i), hit)
}
