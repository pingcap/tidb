// Copyright 2020 PingCAP, Inc.
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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

type tiflashTestSuite struct {
	store kv.Storage
	dom   *domain.Domain
	*parser.Parser
}

func (s *tiflashTestSuite) SetUpSuite(c *C) {
	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < 2 {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)

	c.Assert(err, IsNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom.SetStatsUpdating(true)
}

func (s *tiflashTestSuite) TestReadPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null) partition by hash(a) partitions 2")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1 0", "2 0", "3 0"))

	// test union scan
	tk.MustExec("begin")
	tk.MustExec("insert into t values(4,0)")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Rows("4"))
	tk.MustExec("insert into t values(5,0)")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Rows("5"))
	tk.MustExec("insert into t values(6,0)")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Rows("6"))
	tk.MustExec("commit")
}

func (s *tiflashTestSuite) TestReadUnsigedPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(a bigint unsigned not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("insert into t values(18446744073709551606,0)")
	tk.MustExec("insert into t values(9223372036854775798,0)")

	tk.MustExec("create table t1(a bigint unsigned not null primary key, b int not null)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t1")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t1 values(1,0)")
	tk.MustExec("insert into t1 values(2,0)")
	tk.MustExec("insert into t1 values(3,0)")
	tk.MustExec("insert into t1 values(18446744073709551606,0)")
	tk.MustExec("insert into t1 values(9223372036854775798,0)")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	tk.MustExec("set @@session.tidb_opt_broadcast_join=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")

	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("5"))
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and ((t1.a < 9223372036854775800 and t1.a > 2) or (t1.a <= 1 and t1.a > -1))").Check(testkit.Rows("3"))
}

func (s *tiflashTestSuite) TestMppExecution(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test because of long running")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")

	tk.MustExec("create table t1(a int primary key, b int not null)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t1")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t1 values(1,0)")
	tk.MustExec("insert into t1 values(2,0)")
	tk.MustExec("insert into t1 values(3,0)")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	for i := 0; i < 20; i++ {
		// test if it is stable.
		tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("3"))
	}
	// test multi-way join
	tk.MustExec("create table t2(a int primary key, b int not null)")
	tk.MustExec("alter table t2 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t2")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("insert into t2 values(1,0)")
	tk.MustExec("insert into t2 values(2,0)")
	tk.MustExec("insert into t2 values(3,0)")
	tk.MustQuery("select count(*) from t1 , t, t2 where t1.a = t.a and t2.a = t.a").Check(testkit.Rows("3"))

	// test avg
	tk.MustQuery("select avg(t1.a) from t1 , t where t1.a = t.a").Check(testkit.Rows("2.0000"))
	// test proj and selection
	tk.MustQuery("select count(*) from (select a * 2 as a from t1) t1 , (select b + 4 as a from t)t where t1.a = t.a").Check(testkit.Rows("3"))

	// test shuffle hash join.
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size=1")
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("3"))
	tk.MustQuery("select count(*) from t1 , t, t2 where t1.a = t.a and t2.a = t.a").Check(testkit.Rows("3"))

	// test agg by expression
	tk.MustExec("insert into t1 values(4,0)")
	tk.MustQuery("select count(*) k, t2.b from t1 left join t2 on t1.a = t2.a group by t2.b order by k").Check(testkit.Rows("1 <nil>", "3 0"))
	tk.MustQuery("select count(*) k, t2.b+1 from t1 left join t2 on t1.a = t2.a group by t2.b+1 order by k").Check(testkit.Rows("1 <nil>", "3 1"))
	tk.MustQuery("select count(*) k, t2.b * t2.a from t2 group by t2.b * t2.a").Check(testkit.Rows("3 0"))
	tk.MustQuery("select count(*) k, t2.a/2 m from t2 group by t2.a / 2 order by m").Check(testkit.Rows("1 0.5000", "1 1.0000", "1 1.5000"))
	tk.MustQuery("select count(*) k, t2.a div 2 from t2 group by t2.a div 2 order by k").Check(testkit.Rows("1 0", "2 1"))
	// test task id for same start ts.
	tk.MustExec("begin")
	tk.MustQuery("select count(*) from ( select * from t2 group by a, b) A group by A.b").Check(testkit.Rows("3"))
	tk.MustQuery("select count(*) from t1 where t1.a+100 > ( select count(*) from t2 where t1.a=t2.a and t1.b=t2.b) group by t1.b").Check(testkit.Rows("4"))
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	ts := txn.StartTS()
	taskID := tk.Se.GetSessionVars().AllocMPPTaskID(ts)
	c.Assert(taskID, Equals, int64(6))
	tk.MustExec("commit")
	taskID = tk.Se.GetSessionVars().AllocMPPTaskID(ts + 1)
	c.Assert(taskID, Equals, int64(1))

	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(3)`)
	// all the data is related to one store, so there are three tasks.
	tk.MustQuery("select avg(t.a) from t join t t1 on t.a = t1.a").Check(testkit.Rows("2.0000"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 decimal(8, 5) not null, c2 decimal(9, 5), c3 decimal(9, 4) , c4 decimal(8, 4) not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1.00000,1.00000,1.0000,1.0000)")
	tk.MustExec("insert into t values(1.00010,1.00010,1.0001,1.0001)")
	tk.MustExec("insert into t values(1.00001,1.00001,1.0000,1.0002)")
	tk.MustQuery("select t1.c1 from t t1 join t t2 on t1.c1 = t2.c1 order by t1.c1").Check(testkit.Rows("1.00000", "1.00001", "1.00010"))
	tk.MustQuery("select t1.c1 from t t1 join t t2 on t1.c1 = t2.c3 order by t1.c1").Check(testkit.Rows("1.00000", "1.00000", "1.00010"))
	tk.MustQuery("select t1.c4 from t t1 join t t2 on t1.c4 = t2.c3 order by t1.c4").Check(testkit.Rows("1.0000", "1.0000", "1.0001"))
	// let this query choose hash join
	tk.MustQuery("select /*+ nth_plan(2) */ t1.c1 from t t1 join t t2 on t1.c1 = t2.c3 order by t1.c1").Check(testkit.Rows("1.00000", "1.00000", "1.00010"))
}

func (s *tiflashTestSuite) TestInjectExtraProj(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(20))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")

	tk.MustQuery("select avg(a) from t").Check(testkit.Rows("9223372036854775807.0000"))
	tk.MustQuery("select avg(a), a from t group by a").Check(testkit.Rows("9223372036854775807.0000 9223372036854775807"))
}

func (s *tiflashTestSuite) TestTiFlashPartitionTableShuffledHashJoin(c *C) {
	c.Skip("too slow")

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`create database tiflash_partition_SHJ`)
	tk.MustExec("use tiflash_partition_SHJ")
	tk.MustExec(`create table thash (a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int) partition by range(a) (
		partition p0 values less than (100), partition p1 values less than (200),
		partition p2 values less than (300), partition p3 values less than (400))`)
	listPartitions := make([]string, 4)
	for i := 0; i < 400; i++ {
		idx := i % 4
		if listPartitions[idx] != "" {
			listPartitions[idx] += ", "
		}
		listPartitions[idx] = listPartitions[idx] + fmt.Sprintf("%v", i)
	}
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (
		partition p0 values in (` + listPartitions[0] + `), partition p1 values in (` + listPartitions[1] + `),
		partition p2 values in (` + listPartitions[2] + `), partition p3 values in (` + listPartitions[3] + `))`)
	tk.MustExec(`create table tnormal (a int, b int)`)

	for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
		tk.MustExec("alter table " + tbl + " set tiflash replica 1")
		tb := testGetTableByName(c, tk.Se, "tiflash_partition_SHJ", tbl)
		err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
		c.Assert(err, IsNil)
	}

	vals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(400), rand.Intn(400)))
	}
	for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
		tk.MustExec(fmt.Sprintf("insert into %v values %v", tbl, strings.Join(vals, ", ")))
		tk.MustExec(fmt.Sprintf("analyze table %v", tbl))
	}

	tk.MustExec("SET tidb_enforce_mpp=1")
	tk.MustExec("SET tidb_opt_broadcast_join=0")
	tk.MustExec("SET tidb_broadcast_join_threshold_count=0")
	tk.MustExec("SET tidb_broadcast_join_threshold_size=0")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")

	lr := func() (int, int) {
		l, r := rand.Intn(400), rand.Intn(400)
		if l > r {
			l, r = r, l
		}
		return l, r
	}
	for i := 0; i < 2; i++ {
		l1, r1 := lr()
		l2, r2 := lr()
		cond := fmt.Sprintf("t1.b>=%v and t1.b<=%v and t2.b>=%v and t2.b<=%v", l1, r1, l2, r2)
		var res [][]interface{}
		for _, mode := range []string{"static", "dynamic"} {
			tk.MustExec(fmt.Sprintf("set @@tidb_partition_prune_mode = '%v'", mode))
			for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
				q := fmt.Sprintf("select count(*) from %v t1 join %v t2 on t1.a=t2.a where %v", tbl, tbl, cond)
				if res == nil {
					res = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Check(res)
				}
			}
		}
	}
}

func (s *tiflashTestSuite) TestTiFlashPartitionTableReader(c *C) {
	c.Skip("too slow")

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`create database tiflash_partition_tablereader`)
	tk.MustExec("use tiflash_partition_tablereader")
	tk.MustExec(`create table thash (a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int) partition by range(a) (
		partition p0 values less than (100), partition p1 values less than (200),
		partition p2 values less than (300), partition p3 values less than (400))`)
	listPartitions := make([]string, 4)
	for i := 0; i < 400; i++ {
		idx := i % 4
		if listPartitions[idx] != "" {
			listPartitions[idx] += ", "
		}
		listPartitions[idx] = listPartitions[idx] + fmt.Sprintf("%v", i)
	}
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (
		partition p0 values in (` + listPartitions[0] + `), partition p1 values in (` + listPartitions[1] + `),
		partition p2 values in (` + listPartitions[2] + `), partition p3 values in (` + listPartitions[3] + `))`)
	tk.MustExec(`create table tnormal (a int, b int)`)

	for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
		tk.MustExec("alter table " + tbl + " set tiflash replica 1")
		tb := testGetTableByName(c, tk.Se, "tiflash_partition_tablereader", tbl)
		err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
		c.Assert(err, IsNil)
	}
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")

	vals := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(400), rand.Intn(400)))
	}
	for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
		tk.MustExec(fmt.Sprintf("insert into %v values %v", tbl, strings.Join(vals, ", ")))
	}

	tk.MustExec("SET tidb_enforce_mpp=1")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	for i := 0; i < 10; i++ {
		l, r := rand.Intn(400), rand.Intn(400)
		if l > r {
			l, r = r, l
		}
		cond := fmt.Sprintf("a>=%v and a<=%v", l, r)
		var res [][]interface{}
		for _, mode := range []string{"static", "dynamic"} {
			tk.MustExec(fmt.Sprintf("set @@tidb_partition_prune_mode = '%v'", mode))
			for _, tbl := range []string{"thash", "trange", "tlist", "tnormal"} {
				q := fmt.Sprintf("select * from %v where %v", tbl, cond)
				if res == nil {
					res = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Sort().Check(res)
				}
			}
		}
	}
}

func (s *tiflashTestSuite) TestPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t(a int not null primary key, b int not null) partition by hash(a+1) partitions 4")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("insert into t values(4,0)")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkUseMPP", `return(true)`)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(4)`)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")
	tk.MustExec("set @@session.tidb_partition_prune_mode='static-only'")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkUseMPP", `return(false)`)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	tk.MustExec("set @@session.tidb_partition_prune_mode='dynamic-only'")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkUseMPP", `return(true)`)

	tk.MustExec("create table t1(a int not null primary key, b int not null) partition by hash(a) partitions 4")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t1")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t1 values(1,4)")
	tk.MustExec("insert into t1 values(2,3)")
	tk.MustExec("insert into t1 values(3,2)")
	tk.MustExec("insert into t1 values(4,1)")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	tk.MustExec("set @@session.tidb_opt_broadcast_join=ON")
	// test if it is really work.
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(8)`)
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("4"))
	// test partition prune
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and t1.a < 2 and t.a < 2").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and t1.a < -1 and t.a < 2").Check(testkit.Rows("0"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")
	// test multi-way join
	tk.MustExec("create table t2(a int not null primary key, b int not null)")
	tk.MustExec("alter table t2 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t2")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("insert into t2 values(1,0)")
	tk.MustExec("insert into t2 values(2,0)")
	tk.MustExec("insert into t2 values(3,0)")
	tk.MustExec("insert into t2 values(4,0)")
	// test with no partition table
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(9)`)
	tk.MustQuery("select count(*) from t1 , t, t2 where t1.a = t.a and t2.a = t.a").Check(testkit.Rows("4"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")

	tk.MustExec(`create table t3(a int not null, b int not null) PARTITION BY RANGE (b) (
		PARTITION p0 VALUES LESS THAN (1),
		PARTITION p1 VALUES LESS THAN (3),
		PARTITION p2 VALUES LESS THAN (5),
		PARTITION p3 VALUES LESS THAN (7)
	);`)
	tk.MustExec("alter table t3 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t3")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("insert into t3 values(1,0)")
	tk.MustExec("insert into t3 values(2,2)")
	tk.MustExec("insert into t3 values(3,4)")
	tk.MustExec("insert into t3 values(4,6)")

	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(7)`)
	tk.MustQuery("select count(*) from t, t3 where t3.a = t.a and t3.b <= 4").Check(testkit.Rows("3"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(5)`)
	tk.MustQuery("select count(*) from t, t3 where t3.a = t.a and t3.b > 10").Check(testkit.Rows("0"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")
	failpoint.Disable("github.com/pingcap/tidb/executor/checkUseMPP")
}

func (s *tiflashTestSuite) TestMppEnum(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b enum('aca','bca','zca'))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,'aca')")
	tk.MustExec("insert into t values(2,'bca')")
	tk.MustExec("insert into t values(3,'zca')")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	tk.MustQuery("select t1.b from t t1 join t t2 on t1.a = t2.a order by t1.b").Check(testkit.Rows("aca", "bca", "zca"))
}

func (s *tiflashTestSuite) TestDispatchTaskRetry(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("insert into t values(4,0)")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("set @@session.tidb_enforce_mpp=ON")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/mppDispatchTimeout", "3*return(true)"), IsNil)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/mppDispatchTimeout"), IsNil)
}

func (s *tiflashTestSuite) TestCancelMppTasks(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	var hang = "github.com/pingcap/tidb/store/mockstore/unistore/mppRecvHang"
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("insert into t values(4,0)")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	atomic.StoreUint32(&tk.Se.GetSessionVars().Killed, 0)
	c.Assert(failpoint.Enable(hang, `return(true)`), IsNil)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tk.QueryToErr("select count(*) from t as t1 , t where t1.a = t.a")
		c.Assert(err, NotNil)
		c.Assert(int(terror.ToSQLError(errors.Cause(err).(*terror.Error)).Code), Equals, int(executor.ErrQueryInterrupted.Code()))
	}()
	time.Sleep(1 * time.Second)
	atomic.StoreUint32(&tk.Se.GetSessionVars().Killed, 1)
	wg.Wait()
	c.Assert(failpoint.Disable(hang), IsNil)
}

// all goroutines exit if one goroutine hangs but another return errors
func (s *tiflashTestSuite) TestMppGoroutinesExitFromErrors(c *C) {
	// mock non-root tasks return error
	var mppNonRootTaskError = "github.com/pingcap/tidb/store/copr/mppNonRootTaskError"
	// mock root tasks hang
	var hang = "github.com/pingcap/tidb/store/mockstore/unistore/mppRecvHang"
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int not null primary key, b int not null)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t1")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t1 values(1,0)")
	tk.MustExec("insert into t1 values(2,0)")
	tk.MustExec("insert into t1 values(3,0)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	c.Assert(failpoint.Enable(mppNonRootTaskError, `return(true)`), IsNil)
	c.Assert(failpoint.Enable(hang, `return(true)`), IsNil)

	// generate 2 root tasks, one will hang and another will return errors
	err = tk.QueryToErr("select count(*) from t as t1 , t where t1.a = t.a")
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable(mppNonRootTaskError), IsNil)
	c.Assert(failpoint.Disable(hang), IsNil)
}

func (s *tiflashTestSuite) TestMppUnionAll(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists x1")
	tk.MustExec("create table x1(a int , b int);")
	tk.MustExec("alter table x1 set tiflash replica 1")
	tk.MustExec("drop table if exists x2")
	tk.MustExec("create table x2(a int , b int);")
	tk.MustExec("alter table x2 set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "x1")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tb = testGetTableByName(c, tk.Se, "test", "x2")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("insert into x1 values (1, 1), (2, 2), (3, 3), (4, 4)")
	tk.MustExec("insert into x2 values (5, 1), (2, 2), (3, 3), (4, 4)")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")

	// test join + union (join + select)
	tk.MustQuery("select x1.a, x.a from x1 left join (select x2.b a, x1.b from x1 join x2 on x1.a = x2.b union all select * from x1 ) x on x1.a = x.a order by x1.a").Check(testkit.Rows("1 1", "1 1", "2 2", "2 2", "3 3", "3 3", "4 4", "4 4"))
	tk.MustQuery("select x1.a, x.a from x1 left join (select count(*) a, sum(b) b from x1 group by a union all select * from x2 ) x on x1.a = x.a order by x1.a").Check(testkit.Rows("1 1", "1 1", "1 1", "1 1", "2 2", "3 3", "4 4"))

	tk.MustExec("drop table if exists x3")
	tk.MustExec("create table x3(a int , b int);")
	tk.MustExec("alter table x3 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "x3")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("insert into x3 values (2, 2), (2, 3), (2, 4)")
	// test nested union all
	tk.MustQuery("select count(*) from (select a, b from x1 union all select a, b from x3 union all (select x1.a, x3.b from (select * from x3 union all select * from x2) x3 left join x1 on x3.a = x1.b))").Check(testkit.Rows("14"))
	// test union all join union all
	tk.MustQuery("select count(*) from (select * from x1 union all select * from x2 union all select * from x3) x join (select * from x1 union all select * from x2 union all select * from x3) y on x.a = y.b").Check(testkit.Rows("29"))
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count=100000")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(6)`)
	tk.MustQuery("select count(*) from (select * from x1 union all select * from x2 union all select * from x3) x join (select * from x1 union all select * from x2 union all select * from x3) y on x.a = y.b").Check(testkit.Rows("29"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")

	tk.MustExec("drop table if exists x4")
	tk.MustExec("create table x4(a int not null, b int not null);")
	tk.MustExec("alter table x4 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "x4")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("insert into x4 values (2, 2), (2, 3)")
	tk.MustQuery("(select * from x1 union all select * from x4) order by a, b").Check(testkit.Rows("1 1", "2 2", "2 2", "2 3", "3 3", "4 4"))

}

func (s *tiflashTestSuite) TestMppApply(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists x1")
	tk.MustExec("create table x1(a int primary key, b int);")
	tk.MustExec("alter table x1 set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "x1")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into x1 values(1, 1),(2, 10),(0,11);")

	tk.MustExec("create table x2(a int primary key, b int);")
	tk.MustExec("alter table x2 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "x2")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into x2 values(1,2),(0,1),(2,-3);")
	tk.MustExec("analyze table x1, x2;")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	// table full scan with correlated filter
	tk.MustQuery("select /*+ agg_to_cop(), hash_agg()*/ count(*) from x1 where a >= any (select a from x2 where x1.a = x2.a) order by 1;").Check(testkit.Rows("3"))
	// table range scan with correlated access conditions
	tk.MustQuery("select /*+ agg_to_cop(), hash_agg()*/ count(*) from x1 where b > any (select x2.a from x2 where x1.a = x2.a);").Check(testkit.Rows("2"))
}

func (s *tiflashTestSuite) TestTiFlashVirtualColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t1 (a bit(4), b bit(4), c bit(4) generated always as (a) virtual)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t1")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t1(a,b) values(b'01',b'01'),(b'10',b'10'),(b'11',b'11')")

	tk.MustExec("create table t2 (a int, b int, c int generated always as (a) virtual)")
	tk.MustExec("alter table t2 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t2")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t2(a,b) values(1,1),(2,2),(3,3)")

	tk.MustExec("create table t3 (a bit(4), b bit(4), c bit(4) generated always as (b'01'+b'10') virtual)")
	tk.MustExec("alter table t3 set tiflash replica 1")
	tb = testGetTableByName(c, tk.Se, "test", "t3")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t3(a,b) values(b'01',b'01'),(b'10',b'10'),(b'11',b'11')")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")

	tk.MustQuery("select /*+ hash_agg() */ count(*) from t1 where c > b'01'").Check(testkit.Rows("2"))
	tk.MustQuery("select /*+ hash_agg() */ count(*) from t2 where c > 1").Check(testkit.Rows("2"))
	tk.MustQuery("select /*+ hash_agg() */ count(*) from t3 where c > b'01'").Check(testkit.Rows("3"))
}

func (s *tiflashTestSuite) TestTiFlashPartitionTableShuffledHashAggregation(c *C) {
	c.Skip("too slow")

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database tiflash_partition_AGG")
	tk.MustExec("use tiflash_partition_AGG")
	tk.MustExec(`create table thash (a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int) partition by range(a) (
		partition p0 values less than (100), partition p1 values less than (200),
		partition p2 values less than (300), partition p3 values less than (400))`)
	listPartitions := make([]string, 4)
	for i := 0; i < 400; i++ {
		idx := i % 4
		if listPartitions[idx] != "" {
			listPartitions[idx] += ", "
		}
		listPartitions[idx] = listPartitions[idx] + fmt.Sprintf("%v", i)
	}
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (
		partition p0 values in (` + listPartitions[0] + `), partition p1 values in (` + listPartitions[1] + `),
		partition p2 values in (` + listPartitions[2] + `), partition p3 values in (` + listPartitions[3] + `))`)
	tk.MustExec(`create table tnormal (a int, b int) partition by hash(a) partitions 4`)

	for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
		tk.MustExec("alter table " + tbl + " set tiflash replica 1")
		tb := testGetTableByName(c, tk.Se, "tiflash_partition_AGG", tbl)
		err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
		c.Assert(err, IsNil)
	}

	vals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(400), rand.Intn(400)))
	}
	for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
		tk.MustExec(fmt.Sprintf("insert into %v values %v", tbl, strings.Join(vals, ", ")))
		tk.MustExec(fmt.Sprintf("analyze table %v", tbl))
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enforce_mpp=1")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")

	lr := func() (int, int) {
		l, r := rand.Intn(400), rand.Intn(400)
		if l > r {
			l, r = r, l
		}
		return l, r
	}
	for i := 0; i < 2; i++ {
		l1, r1 := lr()
		cond := fmt.Sprintf("t1.b>=%v and t1.b<=%v", l1, r1)
		var res [][]interface{}
		for _, mode := range []string{"static", "dynamic"} {
			tk.MustExec(fmt.Sprintf("set @@tidb_partition_prune_mode = '%v'", mode))
			for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
				q := fmt.Sprintf("select /*+ HASH_AGG() */ count(*) from %v t1 where %v", tbl, cond)
				c.Assert(tk.HasPlan(q, "HashAgg"), IsTrue)
				if res == nil {
					res = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Check(res)
				}
			}
		}
	}
}

func (s *tiflashTestSuite) TestTiFlashPartitionTableBroadcastJoin(c *C) {
	c.Skip("too slow")

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database tiflash_partition_BCJ")
	tk.MustExec("use tiflash_partition_BCJ")
	tk.MustExec(`create table thash (a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int) partition by range(a) (
		partition p0 values less than (100), partition p1 values less than (200),
		partition p2 values less than (300), partition p3 values less than (400))`)
	listPartitions := make([]string, 4)
	for i := 0; i < 400; i++ {
		idx := i % 4
		if listPartitions[idx] != "" {
			listPartitions[idx] += ", "
		}
		listPartitions[idx] = listPartitions[idx] + fmt.Sprintf("%v", i)
	}
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (
		partition p0 values in (` + listPartitions[0] + `), partition p1 values in (` + listPartitions[1] + `),
		partition p2 values in (` + listPartitions[2] + `), partition p3 values in (` + listPartitions[3] + `))`)
	tk.MustExec(`create table tnormal (a int, b int) partition by hash(a) partitions 4`)

	for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
		tk.MustExec("alter table " + tbl + " set tiflash replica 1")
		tb := testGetTableByName(c, tk.Se, "tiflash_partition_BCJ", tbl)
		err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
		c.Assert(err, IsNil)
	}

	vals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(400), rand.Intn(400)))
	}
	for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
		tk.MustExec(fmt.Sprintf("insert into %v values %v", tbl, strings.Join(vals, ", ")))
		tk.MustExec(fmt.Sprintf("analyze table %v", tbl))
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enforce_mpp=1")
	tk.MustExec("set @@session.tidb_opt_broadcast_join=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")

	lr := func() (int, int) {
		l, r := rand.Intn(400), rand.Intn(400)
		if l > r {
			l, r = r, l
		}
		return l, r
	}
	for i := 0; i < 2; i++ {
		l1, r1 := lr()
		l2, r2 := lr()
		cond := fmt.Sprintf("t1.b>=%v and t1.b<=%v and t2.b>=%v and t2.b<=%v", l1, r1, l2, r2)
		var res [][]interface{}
		for _, mode := range []string{"static", "dynamic"} {
			tk.MustExec(fmt.Sprintf("set @@tidb_partition_prune_mode = '%v'", mode))
			for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
				q := fmt.Sprintf("select count(*) from %v t1 join %v t2 on t1.a=t2.a where %v", tbl, tbl, cond)
				if res == nil {
					res = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Check(res)
				}
			}
		}
	}
}

func (s *tiflashTestSuite) TestForbidTiflashDuringStaleRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(20))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	time.Sleep(2 * time.Second)
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	rows := tk.MustQuery("explain select avg(a) from t").Rows()
	resBuff := bytes.NewBufferString("")
	for _, row := range rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	res := resBuff.String()
	c.Assert(strings.Contains(res, "tiflash"), IsTrue)
	c.Assert(strings.Contains(res, "tikv"), IsFalse)
	tk.MustExec("set transaction read only as of timestamp now(1)")
	rows = tk.MustQuery("explain select avg(a) from t").Rows()
	resBuff = bytes.NewBufferString("")
	for _, row := range rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	res = resBuff.String()
	c.Assert(strings.Contains(res, "tiflash"), IsFalse)
	c.Assert(strings.Contains(res, "tikv"), IsTrue)
}
