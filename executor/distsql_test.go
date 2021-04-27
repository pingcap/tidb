// Copyright 2017 PingCAP, Inc.
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

package executor_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"runtime/pprof"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	err := profile.WriteTo(buf, 1)
	if err != nil {
		panic(err)
	}
	str := buf.String()
	return strings.Contains(str, keyword)
}

func (s *testSuite3) TestCopClientSend(c *C) {
	c.Skip("not stable")
	if _, ok := s.store.GetClient().(*copr.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table copclient (id int primary key)")

	// Insert 1000 rows.
	var values []string
	for i := 0; i < 1000; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert copclient values " + strings.Join(values, ","))

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("copclient"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	s.cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)

	ctx := context.Background()
	// Send coprocessor request when the table split.
	rs, err := tk.Exec("select sum(id) from copclient")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.GetRow(0).GetMyDecimal(0).String(), Equals, "499500")
	c.Assert(rs.Close(), IsNil)

	// Split one region.
	key := tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(500))
	region, _ := s.cluster.GetRegionByKey(key)
	peerID := s.cluster.AllocID()
	s.cluster.Split(region.GetId(), s.cluster.AllocID(), key, []uint64{peerID}, peerID)

	// Check again.
	rs, err = tk.Exec("select sum(id) from copclient")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.GetRow(0).GetMyDecimal(0).String(), Equals, "499500")
	c.Assert(rs.Close(), IsNil)

	// Check there is no goroutine leak.
	rs, err = tk.Exec("select * from copclient order by id")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(rs.Close(), IsNil)
	keyword := "(*copIterator).work"
	c.Check(checkGoroutineExists(keyword), IsFalse)
}

func (s *testSuite3) TestGetLackHandles(c *C) {
	expectedHandles := []kv.Handle{kv.IntHandle(1), kv.IntHandle(2), kv.IntHandle(3), kv.IntHandle(4),
		kv.IntHandle(5), kv.IntHandle(6), kv.IntHandle(7), kv.IntHandle(8), kv.IntHandle(9), kv.IntHandle(10)}
	handlesMap := kv.NewHandleMap()
	for _, h := range expectedHandles {
		handlesMap.Set(h, true)
	}

	// expected handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	// obtained handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	diffHandles := executor.GetLackHandles(expectedHandles, handlesMap)
	c.Assert(diffHandles, HasLen, 0)
	c.Assert(handlesMap.Len(), Equals, 0)

	// expected handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	// obtained handles 2, 3, 4, 6, 7, 8, 9
	retHandles := []kv.Handle{kv.IntHandle(2), kv.IntHandle(3), kv.IntHandle(4), kv.IntHandle(6),
		kv.IntHandle(7), kv.IntHandle(8), kv.IntHandle(9)}
	handlesMap = kv.NewHandleMap()
	handlesMap.Set(kv.IntHandle(1), true)
	handlesMap.Set(kv.IntHandle(5), true)
	handlesMap.Set(kv.IntHandle(10), true)
	diffHandles = executor.GetLackHandles(expectedHandles, handlesMap)
	c.Assert(retHandles, DeepEquals, diffHandles)
}

func (s *testSuite3) TestBigIntPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint unsigned primary key, b int, c int, index idx(a, b))")
	tk.MustExec("insert into t values(1, 1, 1), (9223372036854775807, 2, 2)")
	tk.MustQuery("select * from t use index(idx) order by a").Check(testkit.Rows("1 1 1", "9223372036854775807 2 2"))
}

func (s *testSuite3) TestCorColToRanges(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only-full-group-by
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2 ,2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8), (9, 9, 9)")
	tk.MustExec("analyze table t")
	// Test single read on table.
	tk.MustQuery("select t.c in (select count(*) from t s ignore index(idx), t t1 where s.a = t.a and s.a = t1.a) from t order by 1 desc").Check(testkit.Rows("1", "0", "0", "0", "0", "0", "0", "0", "0"))
	// Test single read on index.
	tk.MustQuery("select t.c in (select count(*) from t s use index(idx), t t1 where s.b = t.a and s.a = t1.a) from t order by 1 desc").Check(testkit.Rows("1", "0", "0", "0", "0", "0", "0", "0", "0"))
	// Test IndexLookUpReader.
	tk.MustQuery("select t.c in (select count(*) from t s use index(idx), t t1 where s.b = t.a and s.c = t1.a) from t order by 1 desc").Check(testkit.Rows("1", "0", "0", "0", "0", "0", "0", "0", "0"))
}

func (s *testSuiteP1) TestUniqueKeyNullValueSelect(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	// test null in unique-key
	tk.MustExec("create table t (id int default null, c varchar(20), unique id (id));")
	tk.MustExec("insert t (c) values ('a'), ('b'), ('c');")
	res := tk.MustQuery("select * from t where id is null;")
	res.Check(testkit.Rows("<nil> a", "<nil> b", "<nil> c"))

	// test null in mul unique-key
	tk.MustExec("drop table t")
	tk.MustExec("create table t (id int default null, b int default 1, c varchar(20), unique id_c(id, b));")
	tk.MustExec("insert t (c) values ('a'), ('b'), ('c');")
	res = tk.MustQuery("select * from t where id is null and b = 1;")
	res.Check(testkit.Rows("<nil> 1 a", "<nil> 1 b", "<nil> 1 c"))

	tk.MustExec("drop table t")
	// test null in non-unique-key
	tk.MustExec("create table t (id int default null, c varchar(20), key id (id));")
	tk.MustExec("insert t (c) values ('a'), ('b'), ('c');")
	res = tk.MustQuery("select * from t where id is null;")
	res.Check(testkit.Rows("<nil> a", "<nil> b", "<nil> c"))
}

// TestIssue10178 contains tests for https://github.com/pingcap/tidb/issues/10178 .
func (s *testSuite3) TestIssue10178(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned primary key)")
	tk.MustExec("insert into t values(9223372036854775807), (18446744073709551615)")
	tk.MustQuery("select max(a) from t").Check(testkit.Rows("18446744073709551615"))
	tk.MustQuery("select * from t where a > 9223372036854775807").Check(testkit.Rows("18446744073709551615"))
	tk.MustQuery("select * from t where a < 9223372036854775808").Check(testkit.Rows("9223372036854775807"))
}

func (s *testSuite3) TestInconsistentIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	is := s.domain.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	idx := tbl.Meta().FindIndexByName("idx_a")
	idxOp := tables.NewIndex(tbl.Meta().ID, tbl.Meta(), idx)
	ctx := mock.NewContext()
	ctx.Store = s.store

	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i+10, i))
		c.Assert(tk.QueryToErr("select * from t where a>=0"), IsNil)
	}

	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("update t set a=%d where a=%d", i, i+10))
		c.Assert(tk.QueryToErr("select * from t where a>=0"), IsNil)
	}

	for i := 0; i < 10; i++ {
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		_, err = idxOp.Create(ctx, txn, types.MakeDatums(i+10), kv.IntHandle(100+i), nil)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)

		err = tk.QueryToErr("select * from t use index(idx_a) where a >= 0")
		c.Assert(err.Error(), Equals, fmt.Sprintf("inconsistent index idx_a handle count %d isn't equal to value count 10", i+11))

		// if has other conditions, the inconsistent index check doesn't work.
		err = tk.QueryToErr("select * from t where a>=0 and b<10")
		c.Assert(err, IsNil)
	}

	// fix inconsistent problem to pass CI
	for i := 0; i < 10; i++ {
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		err = idxOp.Delete(ctx.GetSessionVars().StmtCtx, txn.GetUnionStore(), types.MakeDatums(i+10), kv.IntHandle(100+i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
	}
}

func (s *testSuite3) TestPushLimitDownIndexLookUpReader(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl")
	tk.MustExec("create table tbl(a int, b int, c int, key idx_b_c(b,c))")
	tk.MustExec("insert into tbl values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 1 limit 2,1").Check(testkit.Rows("4 4 4"))
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 4 limit 2,1").Check(testkit.Rows())
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 3 limit 2,1").Check(testkit.Rows())
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 2 limit 2,1").Check(testkit.Rows("5 5 5"))
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 1 limit 1").Check(testkit.Rows("2 2 2"))
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 1 order by b desc limit 2,1").Check(testkit.Rows("3 3 3"))
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 1 and c > 1 limit 2,1").Check(testkit.Rows("4 4 4"))
}

func (s *testSuite3) TestPartitionTableIndexLookUpReader(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a int, b int, key(a))
	partition by range (a) (
	partition p1 values less than (10),
	partition p2 values less than (20),
	partition p3 values less than (30),
	partition p4 values less than (40))`)
	tk.MustExec(`insert into t values (1, 1), (2, 2), (11, 11), (12, 12), (21, 21), (22, 22), (31, 31), (32, 32)`)
	tk.MustExec(`set tidb_partition_prune_mode='dynamic'`)

	tk.MustQuery("select * from t where a>=1 and a<=1").Sort().Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t where a>=1 and a<=2").Sort().Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select * from t where a>=1 and a<12").Sort().Check(testkit.Rows("1 1", "11 11", "2 2"))
	tk.MustQuery("select * from t where a>=1 and a<15").Sort().Check(testkit.Rows("1 1", "11 11", "12 12", "2 2"))
	tk.MustQuery("select * from t where a>15 and a<32").Sort().Check(testkit.Rows("21 21", "22 22", "31 31"))
	tk.MustQuery("select * from t where a>30").Sort().Check(testkit.Rows("31 31", "32 32"))
	tk.MustQuery("select * from t where a>=1 and a<15 order by a").Check(testkit.Rows("1 1", "2 2", "11 11", "12 12"))
	tk.MustQuery("select * from t where a>=1 and a<15 order by a limit 1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t where a>=1 and a<15 order by a limit 3").Check(testkit.Rows("1 1", "2 2", "11 11"))
}

func (s *testSuite3) TestPartitionTableRandomlyIndexLookUpReader(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a int, b int, key(a))
		partition by range (a) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30),
		partition p4 values less than (40))`)
	tk.MustExec("create table tnormal (a int, b int, key(a))")
	values := make([]string, 0, 128)
	for i := 0; i < 128; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", rand.Intn(40), rand.Intn(40)))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tnormal values %v", strings.Join(values, ", ")))

	randRange := func() (int, int) {
		a, b := rand.Intn(40), rand.Intn(40)
		if a > b {
			return b, a
		}
		return a, b
	}
	for i := 0; i < 256; i++ {
		la, ra := randRange()
		lb, rb := randRange()
		cond := fmt.Sprintf("(a between %v and %v) or (b between %v and %v)", la, ra, lb, rb)
		tk.MustQuery("select * from t use index(a) where " + cond).Sort().Check(
			tk.MustQuery("select * from tnormal where " + cond).Sort().Rows())
	}
}

func (s *testSuite3) TestIndexLookUpStats(c *C) {
	stats := &executor.IndexLookUpRunTimeStats{
		FetchHandleTotal: int64(5 * time.Second),
		FetchHandle:      int64(2 * time.Second),
		TaskWait:         int64(2 * time.Second),
		TableRowScan:     int64(2 * time.Second),
		TableTaskNum:     2,
		Concurrency:      1,
	}
	c.Assert(stats.String(), Equals, "index_task: {total_time: 5s, fetch_handle: 2s, build: 1s, wait: 2s}, table_task: {total_time: 2s, num: 2, concurrency: 1}")
	c.Assert(stats.String(), Equals, stats.Clone().String())
	stats.Merge(stats.Clone())
	c.Assert(stats.String(), Equals, "index_task: {total_time: 10s, fetch_handle: 4s, build: 2s, wait: 4s}, table_task: {total_time: 4s, num: 4, concurrency: 2}")
}

func (s *testSuite3) TestIndexLookUpGetResultChunk(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl")
	tk.MustExec("create table tbl(a int, b int, c int, key idx_a(a))")
	for i := 0; i < 101; i++ {
		tk.MustExec(fmt.Sprintf("insert into tbl values(%d,%d,%d)", i, i, i))
	}
	tk.MustQuery("select * from tbl use index(idx_a) where a > 99 order by a asc limit 1").Check(testkit.Rows("100 100 100"))
	tk.MustQuery("select * from tbl use index(idx_a) where a > 10 order by a asc limit 4,1").Check(testkit.Rows("15 15 15"))
}
