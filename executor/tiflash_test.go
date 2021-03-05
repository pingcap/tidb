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
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
	"github.com/pingcap/tidb/util/testkit"
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

	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("5"))
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and ((t1.a < 9223372036854775800 and t1.a > 2) or (t1.a <= 1 and t1.a > -1))").Check(testkit.Rows("3"))
}

func (s *tiflashTestSuite) TestMppExecution(c *C) {
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
	for i := 0; i < 20; i++ {
		// test if it is stable.
		tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("3"))
	}
	// test multi-way join
	tk.MustExec("create table t2(a int not null primary key, b int not null)")
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

	tk.MustExec("insert into t1 values(4,0)")
	tk.MustQuery("select count(*), t2.b from t1 left join t2 on t1.a = t2.a group by t2.b").Check(testkit.Rows("3 0", "1 <nil>"))
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
