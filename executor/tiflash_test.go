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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

// withMockTiFlash sets the mockStore to have N TiFlash stores (naming as tiflash0, tiflash1, ...).
func withMockTiFlash(nodes int) mockstore.MockTiKVStoreOption {
	return mockstore.WithMultipleOptions(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < nodes {
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
}

func TestNonsupportCharsetTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b char(10) charset gbk collate gbk_bin)")
	err := tk.ExecToErr("alter table t set tiflash replica 1")
	require.Error(t, err)
	require.Equal(t, "[ddl:8200]Unsupported ALTER table replica for table contain gbk charset", err.Error())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10) charset utf8)")
	tk.MustExec("alter table t set tiflash replica 1")
}

func TestReadPartitionTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null) partition by hash(a) partitions 2")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
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
	// test dynamic prune + union scan
	tk.MustExec("set tidb_partition_prune_mode=dynamic")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Rows("6"))
	// test dynamic prune + batch cop + union scan
	tk.MustExec("set tidb_allow_batch_cop=2")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Rows("6"))
	tk.MustExec("commit")
}

func TestReadUnsigedPK(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(a bigint unsigned not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("insert into t values(18446744073709551606,0)")
	tk.MustExec("insert into t values(9223372036854775798,0)")

	tk.MustExec("create table t1(a bigint unsigned not null primary key, b int not null)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t1")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t1 values(1,0)")
	tk.MustExec("insert into t1 values(2,0)")
	tk.MustExec("insert into t1 values(3,0)")
	tk.MustExec("insert into t1 values(18446744073709551606,0)")
	tk.MustExec("insert into t1 values(9223372036854775798,0)")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")

	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("5"))
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and ((t1.a < 9223372036854775800 and t1.a > 2) or (t1.a <= 1 and t1.a > -1))").Check(testkit.Rows("3"))
}

// to fix https://github.com/pingcap/tidb/issues/27952
func TestJoinRace(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("insert into t values(2,1)")
	tk.MustExec("insert into t values(3,1)")
	tk.MustExec("insert into t values(1,2)")
	tk.MustExec("insert into t values(2,2)")
	tk.MustExec("insert into t values(3,2)")
	tk.MustExec("insert into t values(1,2)")
	tk.MustExec("insert into t values(2,2)")
	tk.MustExec("insert into t values(3,2)")
	tk.MustExec("insert into t values(1,3)")
	tk.MustExec("insert into t values(2,3)")
	tk.MustExec("insert into t values(3,4)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_enforce_mpp=ON")
	tk.MustExec("set @@tidb_opt_broadcast_cartesian_join=0")
	tk.MustQuery("select count(*) from (select count(a) x from t group by b) t1 join (select count(a) x from t group by b) t2 on t1.x > t2.x").Check(testkit.Rows("6"))

}

func TestMppExecution(t *testing.T) {
	if israce.RaceEnabled {
		t.Skip("skip race test because of long running")
	}
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")

	tk.MustExec("create table t1(a int primary key, b int not null)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t1")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
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
	tb = external.GetTableByName(t, tk, "test", "t2")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

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
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	ts := txn.StartTS()
	taskID := tk.Session().GetSessionVars().AllocMPPTaskID(ts)
	require.Equal(t, int64(6), taskID)
	tk.MustExec("commit")
	taskID = tk.Session().GetSessionVars().AllocMPPTaskID(ts + 1)
	require.Equal(t, int64(1), taskID)

	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(3)`)
	// all the data is related to one store, so there are three tasks.
	tk.MustQuery("select avg(t.a) from t join t t1 on t.a = t1.a").Check(testkit.Rows("2.0000"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 decimal(8, 5) not null, c2 decimal(9, 5), c3 decimal(9, 4) , c4 decimal(8, 4) not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1.00000,1.00000,1.0000,1.0000)")
	tk.MustExec("insert into t values(1.00010,1.00010,1.0001,1.0001)")
	tk.MustExec("insert into t values(1.00001,1.00001,1.0000,1.0002)")
	tk.MustQuery("select t1.c1 from t t1 join t t2 on t1.c1 = t2.c1 order by t1.c1").Check(testkit.Rows("1.00000", "1.00001", "1.00010"))
	tk.MustQuery("select t1.c1 from t t1 join t t2 on t1.c1 = t2.c3 order by t1.c1").Check(testkit.Rows("1.00000", "1.00000", "1.00010"))
	tk.MustQuery("select t1.c4 from t t1 join t t2 on t1.c4 = t2.c3 order by t1.c4").Check(testkit.Rows("1.0000", "1.0000", "1.0001"))
	// let this query choose hash join
	tk.MustQuery("select /*+ nth_plan(2) */ t1.c1 from t t1 join t t2 on t1.c1 = t2.c3 order by t1.c1").Check(testkit.Rows("1.00000", "1.00000", "1.00010"))
}

func TestInjectExtraProj(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(20))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")
	tk.MustExec("insert into t values (9223372036854775807)")

	tk.MustQuery("select avg(a) from t").Check(testkit.Rows("9223372036854775807.0000"))
	tk.MustQuery("select avg(a), a from t group by a").Check(testkit.Rows("9223372036854775807.0000 9223372036854775807"))
}

func TestTiFlashPartitionTableShuffledHashJoin(t *testing.T) {
	t.Skip("too slow")

	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
		tb := external.GetTableByName(t, tk, "tiflash_partition_SHJ", tbl)
		err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
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

func TestTiFlashPartitionTableReader(t *testing.T) {
	t.Skip("too slow")

	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
		tb := external.GetTableByName(t, tk, "tiflash_partition_tablereader", tbl)
		err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
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

func TestPartitionTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t(a int not null primary key, b int not null) partition by hash(a+1) partitions 4")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
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
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(1)`)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")
	tk.MustExec("set @@session.tidb_partition_prune_mode='static-only'")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkUseMPP", `return(false)`)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	tk.MustExec("set @@session.tidb_partition_prune_mode='dynamic-only'")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkUseMPP", `return(true)`)

	tk.MustExec("create table t1(a int not null primary key, b int not null) partition by hash(a) partitions 4")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t1")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t1 values(1,4)")
	tk.MustExec("insert into t1 values(2,3)")
	tk.MustExec("insert into t1 values(3,2)")
	tk.MustExec("insert into t1 values(4,1)")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// test if it is really work.
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(2)`)
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("4"))
	// test partition prune
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and t1.a < 2 and t.a < 2").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and t1.a < -1 and t.a < 2").Check(testkit.Rows("0"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")
	// test multi-way join
	tk.MustExec("create table t2(a int not null primary key, b int not null)")
	tk.MustExec("alter table t2 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t2")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("insert into t2 values(1,0)")
	tk.MustExec("insert into t2 values(2,0)")
	tk.MustExec("insert into t2 values(3,0)")
	tk.MustExec("insert into t2 values(4,0)")
	// test with no partition table
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(3)`)
	tk.MustQuery("select count(*) from t1 , t, t2 where t1.a = t.a and t2.a = t.a").Check(testkit.Rows("4"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")

	tk.MustExec(`create table t3(a int not null, b int not null) PARTITION BY RANGE (b) (
		PARTITION p0 VALUES LESS THAN (1),
		PARTITION p1 VALUES LESS THAN (3),
		PARTITION p2 VALUES LESS THAN (5),
		PARTITION p3 VALUES LESS THAN (7)
	);`)
	tk.MustExec("alter table t3 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t3")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("insert into t3 values(1,0)")
	tk.MustExec("insert into t3 values(2,2)")
	tk.MustExec("insert into t3 values(3,4)")
	tk.MustExec("insert into t3 values(4,6)")

	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(2)`)
	tk.MustQuery("select count(*) from t, t3 where t3.a = t.a and t3.b <= 4").Check(testkit.Rows("3"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")
	failpoint.Enable("github.com/pingcap/tidb/executor/checkTotalMPPTasks", `return(2)`)
	tk.MustQuery("select count(*) from t, t3 where t3.a = t.a and t3.b > 10").Check(testkit.Rows("0"))
	failpoint.Disable("github.com/pingcap/tidb/executor/checkTotalMPPTasks")
	failpoint.Disable("github.com/pingcap/tidb/executor/checkUseMPP")
}

func TestMppEnum(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b enum('aca','bca','zca'))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
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

func TestTiFlashPlanCacheable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)

	sess, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	tk.SetSession(sess)
	require.NoError(t, err)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("alter table test.t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv, tiflash'")
	tk.MustExec("insert into t values(1);")
	tk.MustExec("prepare stmt from 'select /*+ read_from_storage(tiflash[t]) */ * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	// The TiFlash plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("prepare stmt from 'select /*+ read_from_storage(tikv[t]) */ * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	// The TiKV plan can be cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// test the mpp plan
	tk.MustExec("set @@session.tidb_allow_mpp = 1;")
	tk.MustExec("set @@session.tidb_enforce_mpp = 1;")
	tk.MustExec("prepare stmt from 'select count(t1.a) from t t1 join t t2 on t1.a = t2.a where t1.a > ?;';")
	tk.MustExec("set @a = 0;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))

	tk.MustExec("set @a = 1;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("0"))
	// The TiFlash plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestDispatchTaskRetry(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("insert into t values(4,0)")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_enforce_mpp=ON")
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/mppDispatchTimeout", "3*return(true)"))
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/mppDispatchTimeout"))
}

func TestCancelMppTasks(t *testing.T) {
	var hang = "github.com/pingcap/tidb/store/mockstore/unistore/mppRecvHang"
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("insert into t values(4,0)")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	atomic.StoreUint32(&tk.Session().GetSessionVars().Killed, 0)
	require.Nil(t, failpoint.Enable(hang, `return(true)`))
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tk.QueryToErr("select count(*) from t as t1 , t where t1.a = t.a")
		require.Error(t, err)
		require.Equal(t, int(executor.ErrQueryInterrupted.Code()), int(terror.ToSQLError(errors.Cause(err).(*terror.Error)).Code))
	}()
	time.Sleep(1 * time.Second)
	atomic.StoreUint32(&tk.Session().GetSessionVars().Killed, 1)
	wg.Wait()
	require.Nil(t, failpoint.Disable(hang))
}

// all goroutines exit if one goroutine hangs but another return errors
func TestMppGoroutinesExitFromErrors(t *testing.T) {
	// mock non-root tasks return error
	var mppNonRootTaskError = "github.com/pingcap/tidb/store/copr/mppNonRootTaskError"
	// mock root tasks hang
	var hang = "github.com/pingcap/tidb/store/mockstore/unistore/mppRecvHang"
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int not null primary key, b int not null)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t1")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t1 values(1,0)")
	tk.MustExec("insert into t1 values(2,0)")
	tk.MustExec("insert into t1 values(3,0)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	require.Nil(t, failpoint.Enable(mppNonRootTaskError, `return(true)`))
	require.Nil(t, failpoint.Enable(hang, `return(true)`))

	// generate 2 root tasks, one will hang and another will return errors
	err = tk.QueryToErr("select count(*) from t as t1 , t where t1.a = t.a")
	require.Error(t, err)
	require.Nil(t, failpoint.Disable(mppNonRootTaskError))
	require.Nil(t, failpoint.Disable(hang))
}

func TestMppUnionAll(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists x1")
	tk.MustExec("create table x1(a int , b int);")
	tk.MustExec("alter table x1 set tiflash replica 1")
	tk.MustExec("drop table if exists x2")
	tk.MustExec("create table x2(a int , b int);")
	tk.MustExec("alter table x2 set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "x1")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tb = external.GetTableByName(t, tk, "test", "x2")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

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
	tb = external.GetTableByName(t, tk, "test", "x3")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

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
	tb = external.GetTableByName(t, tk, "test", "x4")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("set @@tidb_enforce_mpp=1")
	tk.MustExec("insert into x4 values (2, 2), (2, 3)")
	tk.MustQuery("(select * from x1 union all select * from x4) order by a, b").Check(testkit.Rows("1 1", "2 2", "2 2", "2 3", "3 3", "4 4"))

}

func TestUnionWithEmptyDualTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t (a int not null, b int, c varchar(20))")
	tk.MustExec("create table t1 (a int, b int not null, c double)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tb = external.GetTableByName(t, tk, "test", "t1")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1,2,3)")
	tk.MustExec("insert into t1 values(1,2,3)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_enforce_mpp=ON")
	tk.MustQuery("select count(*) from (select a , b from t union all select a , c from t1 where false) tt").Check(testkit.Rows("1"))
}

func TestAvgOverflow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// avg int
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a decimal(1,0))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(9)")
	for i := 0; i < 16; i++ {
		tk.MustExec("insert into t select * from t")
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_enforce_mpp=ON")
	tk.MustQuery("select avg(a) from t group by a").Check(testkit.Rows("9.0000"))
	tk.MustExec("drop table if exists t")

	// avg decimal
	tk.MustExec("drop table if exists td;")
	tk.MustExec("create table td (col_bigint bigint(20), col_smallint smallint(6));")
	tk.MustExec("alter table td set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "td")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into td values (null, 22876);")
	tk.MustExec("insert into td values (9220557287087669248, 32767);")
	tk.MustExec("insert into td values (28030, 32767);")
	tk.MustExec("insert into td values (-3309864251140603904,32767);")
	tk.MustExec("insert into td values (4,0);")
	tk.MustExec("insert into td values (null,0);")
	tk.MustExec("insert into td values (4,-23828);")
	tk.MustExec("insert into td values (54720,32767);")
	tk.MustExec("insert into td values (0,29815);")
	tk.MustExec("insert into td values (10017,-32661);")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_enforce_mpp=ON")
	tk.MustQuery(" SELECT AVG( col_bigint / col_smallint) AS field1 FROM td;").Sort().Check(testkit.Rows("25769363061037.62077260"))
	tk.MustQuery(" SELECT AVG(col_bigint) OVER (PARTITION BY col_smallint) as field2 FROM td where col_smallint = -23828;").Sort().Check(testkit.Rows("4.0000"))
	tk.MustExec("drop table if exists td;")
}

func TestMppApply(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists x1")
	tk.MustExec("create table x1(a int primary key, b int);")
	tk.MustExec("alter table x1 set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "x1")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into x1 values(1, 1),(2, 10),(0,11);")

	tk.MustExec("create table x2(a int primary key, b int);")
	tk.MustExec("alter table x2 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "x2")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
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

func TestTiFlashVirtualColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t1 (a bit(4), b bit(4), c bit(4) generated always as (a) virtual)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t1")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t1(a,b) values(b'01',b'01'),(b'10',b'10'),(b'11',b'11')")

	tk.MustExec("create table t2 (a int, b int, c int generated always as (a) virtual)")
	tk.MustExec("alter table t2 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t2")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t2(a,b) values(1,1),(2,2),(3,3)")

	tk.MustExec("create table t3 (a bit(4), b bit(4), c bit(4) generated always as (b'01'+b'10') virtual)")
	tk.MustExec("alter table t3 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t3")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
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

func TestTiFlashPartitionTableShuffledHashAggregation(t *testing.T) {
	t.Skip("too slow")

	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
		tb := external.GetTableByName(t, tk, "tiflash_partition_AGG", tbl)
		err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
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
				require.True(t, tk.HasPlan(q, "HashAgg"))
				if res == nil {
					res = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Check(res)
				}
			}
		}
	}
}

func TestTiFlashPartitionTableBroadcastJoin(t *testing.T) {
	t.Skip("too slow")

	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
		tb := external.GetTableByName(t, tk, "tiflash_partition_BCJ", tbl)
		err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
		require.NoError(t, err)
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

func TestForbidTiflashDuringStaleRead(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(20))")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
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
	require.Contains(t, res, "tiflash")
	require.NotContains(t, res, "tikv")
	tk.MustExec("set transaction read only as of timestamp now(1)")
	rows = tk.MustQuery("explain select avg(a) from t").Rows()
	resBuff = bytes.NewBufferString("")
	for _, row := range rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	res = resBuff.String()
	require.NotContains(t, res, "tiflash")
	require.Contains(t, res, "tikv")
}

func TestForbidTiFlashIfExtraPhysTableIDIsNeeded(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null) partition by hash(a) partitions 2")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set tidb_partition_prune_mode=dynamic")
	tk.MustExec("set tidb_enforce_mpp=1")

	rows := tk.MustQuery("explain select count(*) from t").Rows()
	resBuff := bytes.NewBufferString("")
	for _, row := range rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	res := resBuff.String()
	require.Contains(t, res, "tiflash")
	require.NotContains(t, res, "tikv")

	rows = tk.MustQuery("explain select count(*) from t for update").Rows()
	resBuff = bytes.NewBufferString("")
	for _, row := range rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	res = resBuff.String()
	require.NotContains(t, res, "tiflash")
	require.Contains(t, res, "tikv")

	tk.MustExec("begin")
	rows = tk.MustQuery("explain select count(*) from t").Rows()
	resBuff = bytes.NewBufferString("")
	for _, row := range rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	res = resBuff.String()
	require.Contains(t, res, "tiflash")
	require.NotContains(t, res, "tikv")
	tk.MustExec("insert into t values(1,2)")
	rows = tk.MustQuery("explain select count(*) from t").Rows()
	resBuff = bytes.NewBufferString("")
	for _, row := range rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	res = resBuff.String()
	require.Contains(t, res, "tiflash")
	require.NotContains(t, res, "tikv")
	tk.MustExec("rollback")
}

func TestTiflashPartitionTableScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(\n    a int,\n    primary key(a)\n) partition by range(a) (\n    partition p1 values less than (10),\n    partition p2 values less than (20),\n    partition p3 values less than (30),\n    partition p4 values less than (40),\n    partition p5 values less than (50)\n);")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	time.Sleep(2 * time.Second)
	tk.MustExec("insert into t values(1),(11),(21),(31),(41);")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\";")
	// MPP
	tk.MustExec("set @@session.tidb_allow_mpp=ON;")
	tk.MustQuery("select count(*) from t where a < 12;").Check(testkit.Rows("2"))

	// BatchCop
	tk.MustExec("set @@session.tidb_allow_mpp=OFF;")
	tk.MustExec("set @@tidb_allow_batch_cop = 2;")
	tk.MustQuery("select count(*) from t where a < 12;").Check(testkit.Rows("2"))

	// test retry batch cop
	// MPP
	wg := sync.WaitGroup{}
	wg.Add(1)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcServerBusy", `return(true)`))
	go func() {
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcServerBusy"))
		wg.Done()
	}()
	tk.MustExec("set @@session.tidb_allow_mpp=ON;")
	tk.MustQuery("select count(*) from t where a < 12;").Check(testkit.Rows("2"))
	wg.Wait()

	// BatchCop
	wg.Add(1)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcServerBusy", `return(true)`))
	go func() {
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcServerBusy"))
		wg.Done()
	}()
	tk.MustExec("set @@session.tidb_allow_mpp=OFF;")
	tk.MustExec("set @@tidb_allow_batch_cop = 2;")
	tk.MustQuery("select count(*) from t where a < 12;").Check(testkit.Rows("2"))
	wg.Wait()
}

func TestAggPushDownCountStar(t *testing.T) {
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(2))
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists c")
	tk.MustExec("drop table if exists o")
	tk.MustExec("create table c(c_id bigint primary key)")
	tk.MustExec("create table o(o_id bigint primary key, c_id bigint not null)")
	tk.MustExec("alter table c set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "c")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("alter table o set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "o")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into c values(1),(2),(3),(4),(5)")
	tk.MustExec("insert into o values(1,1),(2,1),(3,2),(4,2),(5,2)")

	tk.MustExec("set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_opt_agg_push_down=1")

	tk.MustQuery("select count(*) from c, o where c.c_id=o.c_id").Check(testkit.Rows("5"))
}
