// Copyright 2016 PingCAP, Inc.
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

package executor_test

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/stretchr/testify/require"
)

func TestUnionScanForMemBufferReader(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_partition_prune_mode = dynamic")

	for i := 0; i < 2; i++ {
		suffix := ""
		if i == 1 {
			suffix = "PARTITION BY HASH(a) partitions 4"
		}
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec(fmt.Sprintf("create table t (a int,b int, index idx(b)) %s", suffix))
		tk.MustExec("analyze table t")
		tk.MustExec("insert t values (1,1),(2,2)")

		// Test for delete in union scan
		tk.MustExec("begin")
		tk.MustExec("delete from t")
		tk.MustQuery("select * from t").Check(testkit.Rows())
		tk.MustExec("insert t values (1,1)")
		tk.MustQuery("select a,b from t").Check(testkit.Rows("1 1"))
		tk.MustQuery("select a,b from t use index(idx)").Check(testkit.Rows("1 1"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t")

		// Test update with untouched index columns.
		tk.MustExec("delete from t")
		tk.MustExec("insert t values (1,1),(2,2)")
		tk.MustExec("begin")
		tk.MustExec("update t set a=a+1")
		tk.MustQuery("select * from t").Sort().Check(testkit.Rows("2 1", "3 2"))
		tk.MustQuery("select * from t use index (idx)").Sort().Check(testkit.Rows("2 1", "3 2"))
		tk.MustQuery("select * from t use index (idx) order by b desc").Check(testkit.Rows("3 2", "2 1"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t")

		// Test update with index column.
		tk.MustQuery("select * from t").Sort().Check(testkit.Rows("2 1", "3 2"))
		tk.MustExec("begin")
		tk.MustExec("update t set b=b+1 where a=2")
		tk.MustQuery("select * from t").Sort().Check(testkit.Rows("2 2", "3 2"))
		tk.MustQuery("select * from t use index(idx)").Sort().Check(testkit.Rows("2 2", "3 2"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t")

		// Test index reader order.
		tk.MustQuery("select * from t").Sort().Check(testkit.Rows("2 2", "3 2"))
		tk.MustExec("begin")
		tk.MustExec("insert t values (3,3),(1,1),(4,4),(-1,-1);")
		tk.MustQuery("select * from t use index (idx)").Sort().Check(testkit.Rows("-1 -1", "1 1", "2 2", "3 2", "3 3", "4 4"))
		tk.MustQuery("select b from t use index (idx) order by b desc").Check(testkit.Rows("4", "3", "2", "2", "1", "-1"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t")

		// global index not support.
		if i == 0 {
			// test for update unique index.
			tk.MustExec("drop table if exists t")
			tk.MustExec("create table t (a int,b int, unique index idx(b))")
			tk.MustExec("insert t values (1,1),(2,2)")
			tk.MustExec("begin")
			tk.MustGetErrMsg("update t set b=b+1", "[kv:1062]Duplicate entry '2' for key 't.idx'")
			// update with unchange index column.
			tk.MustExec("update t set a=a+1")
			tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("2 1", "3 2"))
			tk.MustQuery("select b from t use index (idx)").Check(testkit.Rows("1", "2"))
			tk.MustExec("update t set b=b+2 where a=2")
			tk.MustQuery("select * from t").Check(testkit.Rows("2 3", "3 2"))
			tk.MustQuery("select * from t use index (idx) order by b desc").Check(testkit.Rows("2 3", "3 2"))
			tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("3 2", "2 3"))
			tk.MustExec("commit")
			tk.MustExec("admin check table t")
		}

		// Test for getMissIndexRowsByHandle return nil.
		tk.MustExec("drop table if exists t")
		tk.MustExec(fmt.Sprintf("create table t (a int,b int, index idx(a)) %s", suffix))
		tk.MustExec("analyze table t")
		tk.MustExec("insert into t values (1,1),(2,2),(3,3)")
		tk.MustExec("begin")
		tk.MustExec("update t set b=0 where a=2")
		tk.MustQuery("select * from t ignore index (idx) where a>0 and b>0;").Sort().Check(testkit.Rows("1 1", "3 3"))
		tk.MustQuery("select * from t use index (idx) where a>0 and b>0;").Sort().Check(testkit.Rows("1 1", "3 3"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t")

		// Test index lookup reader corner case.
		tk.MustExec("drop table if exists tt")
		tk.MustExec(fmt.Sprintf("create table tt (a bigint, b int,c int,primary key (a,b)) %s;", suffix))
		tk.MustExec("analyze table tt")
		tk.MustExec("insert into tt set a=1,b=1;")
		tk.MustExec("begin;")
		tk.MustExec("update tt set c=1;")
		tk.MustQuery("select * from tt use index (PRIMARY) where c is not null;").Check(testkit.Rows("1 1 1"))
		tk.MustExec("commit")
		tk.MustExec("admin check table tt")

		// Test index reader corner case.
		tk.MustExec("drop table if exists t1")
		tk.MustExec(fmt.Sprintf("create table t1 (a int,b int,primary key(a,b)) %s;", suffix))
		tk.MustExec("analyze table t1")
		tk.MustExec("begin;")
		tk.MustExec("insert into t1 values(1, 1);")
		tk.MustQuery("select * from t1 use index(primary) where a=1;").Check(testkit.Rows("1 1"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t1;")

		// Test index reader with pk handle.
		tk.MustExec("drop table if exists t1")
		tk.MustExec(fmt.Sprintf("create table t1 (a int unsigned key,b int,c varchar(10), index idx(b,a,c)) %s;", suffix))
		tk.MustExec("analyze table t1")
		tk.MustExec("begin;")
		tk.MustExec("insert into t1 (a,b) values (0, 0), (1, 1);")
		tk.MustQuery("select a,b from t1 use index(idx) where b>0;").Check(testkit.Rows("1 1"))
		tk.MustQuery("select a,b,c from t1 ignore index(idx) where a>=1 order by a desc").Check(testkit.Rows("1 1 <nil>"))
		tk.MustExec("insert into t1 values (2, 2, null), (3, 3, 'a');")
		tk.MustQuery("select a,b from t1 use index(idx) where b>1 and c is not null;").Check(testkit.Rows("3 3"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t1;")

		// Test insert and update with untouched index.
		tk.MustExec("drop table if exists t1")
		tk.MustExec(fmt.Sprintf("create table t1 (a int,b int,c int,index idx(b)) %s;", suffix))
		tk.MustExec("analyze table t1")
		tk.MustExec("begin;")
		tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2);")
		tk.MustExec("update t1 set c=c+1 where a=1;")
		tk.MustQuery("select * from t1 use index(idx);").Sort().Check(testkit.Rows("1 1 2", "2 2 2"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t1;")

		if i == 0 {
			// Test insert and update with untouched unique index.
			tk.MustExec("drop table if exists t1")
			tk.MustExec(fmt.Sprintf("create table t1 (a int,b int,c int,unique index idx(b)) %s;", suffix))
			tk.MustExec("begin;")
			tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2);")
			tk.MustExec("update t1 set c=c+1 where a=1;")
			tk.MustQuery("select * from t1 use index(idx);").Check(testkit.Rows("1 1 2", "2 2 2"))
			tk.MustExec("commit")
			tk.MustExec("admin check table t1;")
		}

		// Test update with 2 index, one untouched, the other index is touched.
		tk.MustExec("drop table if exists t1")
		tk.MustExec(fmt.Sprintf("create table t1 (a int,b int,c int,unique index idx1(a), index idx2(b)) %s;", suffix))
		tk.MustExec("analyze table t1")
		tk.MustExec("insert into t1 values (1, 1, 1);")
		tk.MustExec("update t1 set b=b+1 where a=1;")
		tk.MustQuery("select * from t1 use index(idx2);").Check(testkit.Rows("1 2 1"))
		tk.MustExec("admin check table t1;")
	}
}

func TestIssue28073(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c_int int, c_str varchar(40), primary key (c_int, c_str) , key(c_int)) partition by hash (c_int) partitions 4")
	tk.MustExec("create table t2 like t1")
	tk.MustExec("insert into t1 values (1, 'flamboyant mcclintock')")
	tk.MustExec("insert into t2 select * from t1")

	tk.MustExec("begin")
	tk.MustExec("insert into t2 (c_int, c_str) values (2, 'romantic grothendieck')")
	tk.MustQuery("select * from t2 left join t1 on t1.c_int = t2.c_int for update").Sort().Check(
		testkit.Rows(
			"1 flamboyant mcclintock 1 flamboyant mcclintock",
			"2 romantic grothendieck <nil> <nil>",
		))
	tk.MustExec("commit")

	// Check no key is written to table ID 0
	txn, err := store.Begin()
	require.NoError(t, err)
	start := tablecodec.EncodeTablePrefix(0)
	end := tablecodec.EncodeTablePrefix(1)
	iter, err := txn.Iter(start, end)
	require.NoError(t, err)

	exist := false
	for iter.Valid() {
		require.Nil(t, iter.Next())
		exist = true
		break
	}
	require.False(t, exist)

	// Another case, left join on partition table should not generate locks on physical ID = 0
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int, c_str));")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key (c_int)) partition by hash (c_int) partitions 4;")
	tk.MustExec("insert into t1 (`c_int`, `c_str`) values (1, 'upbeat solomon'), (5, 'sharp rubin');")
	tk.MustExec("insert into t2 (`c_int`, `c_str`) values (1, 'clever haibt'), (4, 'kind margulis');")
	tk.MustExec("begin pessimistic;")
	tk.MustQuery("select * from t1 left join t2 on t1.c_int = t2.c_int for update;").Check(testkit.Rows(
		"1 upbeat solomon 1 clever haibt",
		"5 sharp rubin <nil> <nil>",
	))
	key, err := hex.DecodeString("7480000000000000005F728000000000000000")
	require.NoError(t, err)
	h := helper.NewHelper(store.(helper.Storage))
	resp, err := h.GetMvccByEncodedKey(key)
	require.NoError(t, err)
	require.Nil(t, resp.Info.Lock)
	require.Len(t, resp.Info.Writes, 0)
	require.Len(t, resp.Info.Values, 0)

	tk.MustExec("rollback;")
}

func TestIssue32422(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (id int, c int, index(id));")
	tk.MustExec("insert into t values (3,3), (4,4), (5,5);")
	tk.MustExec("alter table t cache;")

	var cacheUsed bool
	for i := 0; i < 20; i++ {
		tk.MustQuery("select id+1, c from t where c = 4;").Check(testkit.Rows("5 4"))
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			cacheUsed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, cacheUsed)

	tk.MustQuery("select id+1, c from t where c = 4;").Check(testkit.Rows("5 4"))

	// Some extra tests.
	// Since cached table use UnionScanExec utilities, check what happens when they work together.
	// In these cases, the cache data serve as the snapshot, tikv is skipped, and txn membuffer works the same way.
	tk.MustExec("begin")
	tk.MustQuery("select id+1, c from t where c = 4;").Check(testkit.Rows("5 4"))
	tk.MustExec("insert into t values (6, 6)")
	// Check for the new added data.
	tk.MustHavePlan("select id+1, c from t where c = 6;", "UnionScan")
	tk.MustQuery("select id+1, c from t where c = 6;").Check(testkit.Rows("7 6"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)
	// Check for the old data.
	tk.MustQuery("select id+1, c from t where c = 4;").Check(testkit.Rows("5 4"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)

	// Index Lookup
	tk.MustHavePlan("select id+1, c from t where id = 6", "IndexLookUp")
	tk.MustQuery("select id+1, c from t use index(id) where id = 6").Check(testkit.Rows("7 6"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)
	tk.MustQuery("select id+1, c from t use index(id) where id = 4").Check(testkit.Rows("5 4"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)

	// Index Reader
	tk.MustHavePlan("select id from t where id = 6", "IndexReader")
	tk.MustQuery("select id from t use index(id) where id = 6").Check(testkit.Rows("6"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)
	tk.MustQuery("select id from t use index(id) where id = 4").Check(testkit.Rows("4"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)

	tk.MustExec("rollback")
}

func BenchmarkUnionScanRead(b *testing.B) {
	store := testkit.CreateMockStore(b)

	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t_us (
c1 varchar(10),
c2 varchar(30),
c3 varchar(1),
c4 varchar(12),
c5 varchar(10),
c6 datetime);`)
	tk.MustExec(`begin;`)
	for i := 0; i < 8000; i++ {
		tk.MustExec("insert into t_us values ('54321', '1234', '1', '000000', '7518', '2014-05-08')")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustQuery("select * from t_us where c1 = '12345'").Check(testkit.Rows())
	}
	b.StopTimer()
	tk.MustExec("rollback")
}

func BenchmarkUnionScanIndexReadDescRead(b *testing.B) {
	store := testkit.CreateMockStore(b)

	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int, b int, c int, primary key(a), index k(b))`)
	tk.MustExec(`begin;`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d, %d)", i, i, i))
	}

	tk.MustHavePlan("select b from t use index(k) where b > 50 order by b desc", "IndexReader")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// indexReader
		tk.MustQuery("select b from t use index(k) where b > 50 order by b desc")
	}
	b.StopTimer()
	tk.MustExec("rollback")
}

func BenchmarkUnionScanTableReadDescRead(b *testing.B) {
	store := testkit.CreateMockStore(b)

	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int, b int, c int, primary key(a), index k(b))`)
	tk.MustExec(`begin;`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d, %d)", i, i, i))
	}

	tk.MustHavePlan("select * from t where a > 50 order by a desc", "TableReader")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// tableReader
		tk.MustQuery("select * from t where a > 50 order by a desc")
	}
	b.StopTimer()
	tk.MustExec("rollback")
}

func BenchmarkUnionScanIndexLookUpDescRead(b *testing.B) {
	store := testkit.CreateMockStore(b)

	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int, b int, c int, primary key(a), index k(b))`)
	tk.MustExec(`begin;`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d, %d)", i, i, i))
	}

	tk.MustHavePlan("select * from t use index(k) where b > 50 order by b desc", "IndexLookUp")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// indexLookUp
		tk.MustQuery("select * from t use index(k) where b > 50 order by b desc")
	}
	b.StopTimer()
	tk.MustExec("rollback")
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		executor.BenchmarkReadLastLinesOfHugeLine,
		executor.BenchmarkCompleteInsertErr,
		executor.BenchmarkCompleteLoadErr,
		BenchmarkUnionScanRead,
		BenchmarkUnionScanIndexReadDescRead,
		BenchmarkUnionScanTableReadDescRead,
		BenchmarkUnionScanIndexLookUpDescRead,
	)
}
