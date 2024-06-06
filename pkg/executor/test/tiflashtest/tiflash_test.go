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

package tiflashtest

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b char(10) charset gbk collate gbk_bin)")
	err := tk.ExecToErr("alter table t set tiflash replica 1")
	require.Error(t, err)
	require.Equal(t, "[ddl:8200]Unsupported ALTER TiFlash settings for tables not supported by TiFlash: table contains gbk charset", err.Error())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10) charset utf8)")
	tk.MustExec("alter table t set tiflash replica 1")
}

func TestReadPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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

func TestAggPushDownApplyAll(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists foo")
	tk.MustExec("drop table if exists bar")
	tk.MustExec("create table foo(a int, b int)")
	tk.MustExec("create table bar(a double not null, b decimal(65,0) not null)")
	tk.MustExec("alter table foo set tiflash replica 1")
	tk.MustExec("alter table bar set tiflash replica 1")
	tk.MustExec("insert into foo values(0, NULL)")
	tk.MustExec("insert into bar values(0, 0)")

	tk.MustExec("set @@session.tidb_allow_mpp=1")
	tk.MustExec("set @@session.tidb_enforce_mpp=1")

	tk.MustQuery("select * from foo where a=all(select a from bar where bar.b=foo.b)").Check(testkit.Rows("0 <nil>"))
}

func TestReadUnsigedPK(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 2")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")

	tk.MustExec("create table t1(a int primary key, b int not null)")
	tk.MustExec("alter table t1 set tiflash replica 2")
	tb = external.GetTableByName(t, tk, "test", "t1")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t1 values(1,0)")
	tk.MustExec("insert into t1 values(2,0)")
	tk.MustExec("insert into t1 values(3,0)")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	tk.MustExec("set @@session.tidb_opt_enable_late_materialization=OFF")
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
	taskID := plannercore.AllocMPPTaskID(tk.Session())
	require.Equal(t, int64(1), taskID)
	tk.MustExec("commit")

	tk.MustQuery("select avg(t.a) from t join t t1 on t.a = t1.a").Check(testkit.Rows("2.0000"))

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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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

	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
		var res [][]any
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

	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
		var res [][]any
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t(a int not null primary key, b int not null) partition by hash(a+1) partitions 4")
	// Looks like setting replica number of a region is not supported in mock store, a region always has n replicas(where n
	// is the number of stores), in this test, there are 2 TiFlash store, so the TiFlash replica is always 2, change the
	// TiFlash replica to 2 to make it consist with mock store.
	tk.MustExec("alter table t set tiflash replica 2")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("insert into t values(4,0)")
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/checkUseMPP", `return(true)`)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	tk.MustExec("set @@session.tidb_opt_enable_late_materialization=OFF")
	// mock executor does not support use outer table as build side for outer join, so need to
	// force the inner table as build side
	tk.MustExec("set tidb_opt_mpp_outer_join_fixed_build_side=1")
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", `return(2)`)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks")
	tk.MustExec("set @@session.tidb_partition_prune_mode='static-only'")
	// when we lift the restriction of partition table can not take MPP path, here should `return(true)`
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/checkUseMPP", `return(true)`)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	tk.MustExec("set @@session.tidb_partition_prune_mode='dynamic-only'")
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/checkUseMPP", `return(true)`)

	tk.MustExec("create table t1(a int not null primary key, b int not null) partition by hash(a) partitions 4")
	tk.MustExec("alter table t1 set tiflash replica 2")
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", `return(4)`)
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a").Check(testkit.Rows("4"))
	// test partition prune
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and t1.a < 2 and t.a < 2").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t1 , t where t1.a = t.a and t1.a < -1 and t.a < 2").Check(testkit.Rows("0"))
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks")
	// test multi-way join
	tk.MustExec("create table t2(a int not null primary key, b int not null)")
	tk.MustExec("alter table t2 set tiflash replica 2")
	tb = external.GetTableByName(t, tk, "test", "t2")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("insert into t2 values(1,0)")
	tk.MustExec("insert into t2 values(2,0)")
	tk.MustExec("insert into t2 values(3,0)")
	tk.MustExec("insert into t2 values(4,0)")
	// test with no partition table
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", `return(5)`)
	tk.MustQuery("select count(*) from t1 , t, t2 where t1.a = t.a and t2.a = t.a").Check(testkit.Rows("4"))
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks")

	tk.MustExec(`create table t3(a int not null, b int not null) PARTITION BY RANGE (b) (
		PARTITION p0 VALUES LESS THAN (1),
		PARTITION p1 VALUES LESS THAN (3),
		PARTITION p2 VALUES LESS THAN (5),
		PARTITION p3 VALUES LESS THAN (7)
	);`)
	tk.MustExec("alter table t3 set tiflash replica 2")
	tb = external.GetTableByName(t, tk, "test", "t3")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("insert into t3 values(1,0)")
	tk.MustExec("insert into t3 values(2,2)")
	tk.MustExec("insert into t3 values(3,4)")
	tk.MustExec("insert into t3 values(4,6)")

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", `return(4)`)
	tk.MustQuery("select count(*) from t, t3 where t3.a = t.a and t3.b <= 4").Check(testkit.Rows("3"))
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks")
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", `return(3)`)
	tk.MustQuery("select count(*) from t, t3 where t3.a = t.a and t3.b > 10").Check(testkit.Rows("0"))
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks")
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/checkUseMPP")
}

func TestMppEnum(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("alter table test.t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout", "3*return(true)"))
	tk.MustQuery("select count(*) from t group by b").Check(testkit.Rows("4"))
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppDispatchTimeout"))
}

func TestMppVersionError(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("insert into t values(1,0),(2,0),(3,0),(4,0)")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_enforce_mpp=ON")
	{
		item := fmt.Sprintf("return(%d)", kv.GetNewestMppVersion()+1)
		require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/MppVersionError", item))
	}
	{
		err := tk.QueryToErr("select count(*) from t group by b")
		require.Error(t, err)
	}
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/MppVersionError"))
	{
		item := fmt.Sprintf("return(%d)", kv.GetNewestMppVersion())
		require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/MppVersionError", item))
	}
	{
		tk.MustQuery("select count(*) from t group by b").Check(testkit.Rows("4"))
	}
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/MppVersionError"))
}

func TestCancelMppTasks(t *testing.T) {
	var hang = "github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppRecvHang"
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	tk.Session().GetSessionVars().SQLKiller.Reset()
	require.Nil(t, failpoint.Enable(hang, `return(true)`))
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tk.QueryToErr("select count(*) from t as t1 , t where t1.a = t.a")
		require.Error(t, err)
		require.Equal(t, int(exeerrors.ErrQueryInterrupted.Code()), int(terror.ToSQLError(errors.Cause(err).(*terror.Error)).Code))
	}()
	time.Sleep(1 * time.Second)
	tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	wg.Wait()
	require.Nil(t, failpoint.Disable(hang))
}

// all goroutines exit if one goroutine hangs but another return errors
func TestMppGoroutinesExitFromErrors(t *testing.T) {
	// mock non-root tasks return error
	var mppNonRootTaskError = "github.com/pingcap/tidb/pkg/executor/internal/mpp/mppNonRootTaskError"
	// mock root tasks hang
	var hang = "github.com/pingcap/tidb/pkg/store/mockstore/unistore/mppRecvHang"
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists x1")
	tk.MustExec("create table x1(a int , b int);")
	tk.MustExec("alter table x1 set tiflash replica 2")
	tk.MustExec("drop table if exists x2")
	tk.MustExec("create table x2(a int , b int);")
	tk.MustExec("alter table x2 set tiflash replica 2")
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks", `return(6)`)
	tk.MustQuery("select count(*) from (select * from x1 union all select * from x2 union all select * from x3) x join (select * from x1 union all select * from x2 union all select * from x3) y on x.a = y.b").Check(testkit.Rows("29"))
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/checkTotalMPPTasks")

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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	tk.MustExec("set @@session.tidb_opt_enable_late_materialization=OFF")
	tk.MustQuery(" SELECT AVG( col_bigint / col_smallint) AS field1 FROM td;").Sort().Check(testkit.Rows("25769363061037.62077260"))
	tk.MustQuery(" SELECT AVG(col_bigint) OVER (PARTITION BY col_smallint) as field2 FROM td where col_smallint = -23828;").Sort().Check(testkit.Rows("4.0000"))
	tk.MustExec("drop table if exists td;")
}

func TestMppApply(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
		var res [][]any
		for _, mode := range []string{"static", "dynamic"} {
			tk.MustExec(fmt.Sprintf("set @@tidb_partition_prune_mode = '%v'", mode))
			for _, tbl := range []string{`thash`, `trange`, `tlist`, `tnormal`} {
				q := fmt.Sprintf("select /*+ HASH_AGG() */ count(*) from %v t1 where %v", tbl, cond)
				tk.MustHavePlan(q, "HashAgg")
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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
		var res [][]any
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

func TestTiflashSupportStaleRead(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	require.Contains(t, res, "tiflash")
	require.NotContains(t, res, "tikv")
}

func TestForbidTiFlashIfExtraPhysTableIDIsNeeded(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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
	tk.MustExec("set tidb_cost_model_version=2")

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
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(\n    a int,\n    primary key(a)\n) partition by range(a) (\n    partition p1 values less than (10),\n    partition p2 values less than (20),\n    partition p3 values less than (30),\n    partition p4 values less than (40),\n    partition p5 values less than (50)\n);")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t values(1),(11),(21),(31),(41);")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\";")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy", `return(true)`))
	go func() {
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy"))
		wg.Done()
	}()
	tk.MustExec("set @@session.tidb_allow_mpp=ON;")
	tk.MustQuery("select count(*) from t where a < 12;").Check(testkit.Rows("2"))
	wg.Wait()

	// BatchCop
	wg.Add(1)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy", `return(true)`))
	go func() {
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy"))
		wg.Done()
	}()
	tk.MustExec("set @@session.tidb_allow_mpp=OFF;")
	tk.MustExec("set @@tidb_allow_batch_cop = 2;")
	tk.MustQuery("select count(*) from t where a < 12;").Check(testkit.Rows("2"))
	wg.Wait()
}

func TestAggPushDownCountStar(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
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

func TestAggPushDownUnionAndMPP(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("set @@tidb_allow_mpp=1;")
	tk.MustExec("set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_opt_agg_push_down=1")

	tk.MustExec("create table c(c_id int)")
	tk.MustExec("create table o(o_id int, c_id int)")
	tk.MustExec("insert into c values(1),(1),(1),(1)")
	tk.MustExec("insert into o values(1,1),(1,1),(1,2)")
	tk.MustExec("alter table c set tiflash replica 1")
	tk.MustExec("alter table o set tiflash replica 1")

	tk.MustQuery("select a, count(*) from (select a, b from t " +
		"union all " +
		"select a, b from t" +
		") t group by a order by a limit 10;").Check(testkit.Rows("1 10"))

	tk.MustQuery("select o.o_id, count(*) from c, o where c.c_id=o.o_id group by o.o_id").Check(testkit.Rows("1 12"))
}

func TestGroupStreamAggOnTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists foo")
	tk.MustExec("create table foo(a int, b int, c int, d int, primary key(a,b,c,d))")
	tk.MustExec("alter table foo set tiflash replica 1")
	tk.MustExec("insert into foo values(1,2,3,1),(1,2,3,6),(1,2,3,5)," +
		"(1,2,3,2),(1,2,3,4),(1,2,3,7),(1,2,3,3),(1,2,3,0)")
	tb := external.GetTableByName(t, tk, "test", "foo")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@tidb_allow_mpp=0")
	sql := "select a,b,c,count(*) from foo group by a,b,c order by a,b,c"
	tk.MustQuery(sql).Check(testkit.Rows("1 2 3 8"))
	rows := tk.MustQuery("explain " + sql).Rows()

	for _, row := range rows {
		resBuff := bytes.NewBufferString("")
		fmt.Fprintf(resBuff, "%s\n", row)
		res := resBuff.String()
		// StreamAgg with group keys on TiFlash is not supported
		if strings.Contains(res, "tiflash") {
			require.NotContains(t, res, "StreamAgg")
		}
	}
}

// TestIssue41014 test issue that can't find proper physical plan
func TestIssue41014(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("CREATE TABLE `tai1` (\n  `aid` int(11) DEFAULT NULL,\n  `rid` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("CREATE TABLE `tai2` (\n  `rid` int(11) DEFAULT NULL,\n  `prilan` varchar(20) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("alter table tai1 set tiflash replica 1")
	tk.MustExec("alter table tai2 set tiflash replica 1")
	tk.MustExec("alter table tai2 add index idx((lower(prilan)));")
	tk.MustExec("set @@tidb_opt_distinct_agg_push_down = 1;")

	tk.MustQuery("explain select count(distinct tai1.aid) as cb from tai1 inner join tai2 on tai1.rid = tai2.rid where lower(prilan)  LIKE LOWER('%python%');").Check(
		testkit.Rows("HashAgg_11 1.00 root  funcs:count(distinct test.tai1.aid)->Column#8",
			"└─HashJoin_15 9990.00 root  inner join, equal:[eq(test.tai2.rid, test.tai1.rid)]",
			"  ├─Selection_20(Build) 8000.00 root  like(lower(test.tai2.prilan), \"%python%\", 92)",
			"  │ └─Projection_19 10000.00 root  test.tai2.rid, lower(test.tai2.prilan)",
			"  │   └─TableReader_18 9990.00 root  data:Selection_17",
			"  │     └─Selection_17 9990.00 cop[tikv]  not(isnull(test.tai2.rid))",
			"  │       └─TableFullScan_16 10000.00 cop[tikv] table:tai2 keep order:false, stats:pseudo",
			"  └─TableReader_23(Probe) 9990.00 root  data:Selection_22",
			"    └─Selection_22 9990.00 cop[tikv]  not(isnull(test.tai1.rid))",
			"      └─TableFullScan_21 10000.00 cop[tikv] table:tai1 keep order:false, stats:pseudo"))
	tk.MustQuery("select count(distinct tai1.aid) as cb from tai1 inner join tai2 on tai1.rid = tai2.rid where lower(prilan)  LIKE LOWER('%python%');").Check(
		testkit.Rows("0"))
}

func TestTiflashEmptyDynamicPruneResult(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `IDT_RP24833` (  `COL1` bigint(16) DEFAULT '15' COMMENT 'NUMERIC UNIQUE INDEX',\n  `COL2` varchar(20) DEFAULT NULL,\n  `COL4` datetime DEFAULT NULL,\n  `COL3` bigint(20) DEFAULT NULL,\n  `COL5` float DEFAULT NULL,\n  KEY `UK_COL1` (`COL1`) /*!80000 INVISIBLE */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE ((`COL1`-57))\n(PARTITION `P0` VALUES LESS THAN (-3503857335115112215),\n PARTITION `P1` VALUES LESS THAN (-2987877108151063747),\n PARTITION `P2` VALUES LESS THAN (-1981049919102122710),\n PARTITION `P3` VALUES LESS THAN (-1635802972727465681),\n PARTITION `P4` VALUES LESS THAN (1186020639986357714),\n PARTITION `P5` VALUES LESS THAN (1220018677454711359),\n PARTITION `PMX` VALUES LESS THAN (MAXVALUE));")
	tk.MustExec("alter table IDT_RP24833 set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "IDT_RP24833")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("insert into IDT_RP24833 values(-8448770111093677011, \"郇鋺篤堯擈斥鍮啸赠璭饱磟朅闑傒聎疫ᛄ怖霃\", \"8781-05-02 04:23:03\", -27252736532807028, -1.34554e38);")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\";")
	tk.MustExec("set @@session.tidb_allow_mpp=ON;")
	tk.MustExec("set @@session.tidb_enforce_mpp = on;")
	tk.MustQuery("select /*+ read_from_storage(tiflash[t1]) */  * from IDT_RP24833 partition(p3, p4) t1 where t1. col1 between -8448770111093677011 and -8448770111093677011;").Check(testkit.Rows())
	tk.MustQuery("select /*+ read_from_storage(tiflash[t2]) */  * from IDT_RP24833 partition(p2) t2 where t2. col1 <= -8448770111093677011;").Check(testkit.Rows())
	tk.MustQuery("select /*+ read_from_storage(tiflash[t1, t2]) */  * from IDT_RP24833 partition(p3, p4) t1 join IDT_RP24833 partition(p2) t2 on t1.col1 = t2.col1 where t1. col1 between -8448770111093677011 and -8448770111093677011 and t2. col1 <= -8448770111093677011;").Check(testkit.Rows())
}

func TestDisaggregatedTiFlash(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = true
		conf.UseAutoScaler = true
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = false
		conf.UseAutoScaler = false
	})
	err := tiflashcompute.InitGlobalTopoFetcher(tiflashcompute.TestASStr, "tmpAddr", "tmpClusterID", false)
	require.NoError(t, err)

	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")

	err = tk.ExecToErr("select * from t;")
	// Expect error, because TestAutoScaler return empty topo.
	require.Contains(t, err.Error(), "Cannot find proper topo to dispatch MPPTask: topo from AutoScaler is empty")

	err = tiflashcompute.InitGlobalTopoFetcher(tiflashcompute.AWSASStr, "tmpAddr", "tmpClusterID", false)
	require.NoError(t, err)
	err = tk.ExecToErr("select * from t;")
	// Expect error, because AWSAutoScaler is not setup, so http request will fail.
	require.Contains(t, err.Error(), "[util:1815]Internal : get tiflash_compute topology failed")
}

// todo: remove this after AutoScaler is stable.
func TestDisaggregatedTiFlashNonAutoScaler(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = true
		conf.UseAutoScaler = false
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = false
		conf.UseAutoScaler = true
	})

	// Setting globalTopoFetcher to nil to can make sure cannot fetch topo from AutoScaler.
	err := tiflashcompute.InitGlobalTopoFetcher(tiflashcompute.InvalidASStr, "tmpAddr", "tmpClusterID", false)
	require.Contains(t, err.Error(), "unexpected topo fetch type. expect: mock or aws or gcp, got invalid")

	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")

	err = tk.ExecToErr("select * from t;")
	// This error message means we use PD instead of AutoScaler.
	require.Contains(t, err.Error(), "tiflash_compute node is unavailable")
}

func TestDisaggregatedTiFlashQuery(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = true
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = false
	})

	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl_1")
	tk.MustExec(`create table tbl_1 ( col_1 bigint not null default -1443635317331776148,
		col_2 text ( 176 ) collate utf8mb4_bin not null,
		col_3 decimal ( 8, 3 ),
		col_4 varchar ( 128 ) collate utf8mb4_bin not null,
		col_5 varchar ( 377 ) collate utf8mb4_bin,
		col_6 double,
		col_7 varchar ( 459 ) collate utf8mb4_bin,
		col_8 tinyint default -88 ) charset utf8mb4 collate utf8mb4_bin ;`)
	tk.MustExec("alter table tbl_1 set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "tbl_1")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")

	tk.MustExec("explain select max( tbl_1.col_1 ) as r0 , sum( tbl_1.col_1 ) as r1 , sum( tbl_1.col_8 ) as r2 from tbl_1 where tbl_1.col_8 != 68 or tbl_1.col_3 between null and 939 order by r0,r1,r2;")

	tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("create table t1(c1 int, c2 int) partition by hash(c1) partitions 3")
	tk.MustExec("insert into t1 values(1, 1), (2, 2), (3, 3)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t1")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustQuery("explain select * from t1 where c1 < 2").Check(testkit.Rows(
		"PartitionUnion_11 9970.00 root  ",
		"├─TableReader_16 3323.33 root  MppVersion: 2, data:ExchangeSender_15",
		"│ └─ExchangeSender_15 3323.33 mpp[tiflash]  ExchangeType: PassThrough",
		"│   └─Selection_14 3323.33 mpp[tiflash]  lt(test.t1.c1, 2)",
		"│     └─TableFullScan_13 10000.00 mpp[tiflash] table:t1, partition:p0 pushed down filter:empty, keep order:false, stats:pseudo",
		"├─TableReader_20 3323.33 root  MppVersion: 2, data:ExchangeSender_19",
		"│ └─ExchangeSender_19 3323.33 mpp[tiflash]  ExchangeType: PassThrough",
		"│   └─Selection_18 3323.33 mpp[tiflash]  lt(test.t1.c1, 2)",
		"│     └─TableFullScan_17 10000.00 mpp[tiflash] table:t1, partition:p1 pushed down filter:empty, keep order:false, stats:pseudo",
		"└─TableReader_24 3323.33 root  MppVersion: 2, data:ExchangeSender_23",
		"  └─ExchangeSender_23 3323.33 mpp[tiflash]  ExchangeType: PassThrough",
		"    └─Selection_22 3323.33 mpp[tiflash]  lt(test.t1.c1, 2)",
		"      └─TableFullScan_21 10000.00 mpp[tiflash] table:t1, partition:p2 pushed down filter:empty, keep order:false, stats:pseudo"))
}

func TestMPPMemoryTracker(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mem_quota_query = 1 << 30")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set tidb_enforce_mpp = on;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/mpp/testMPPOOMPanic", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/mpp/testMPPOOMPanic"))
	}()
	err = tk.QueryToErr("select * from t")
	require.NotNil(t, err)
	require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
}

func TestTiFlashComputeDispatchPolicy(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = true
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = false
	})

	var err error
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Default policy is 'consistent_hash'
	tk.MustQuery("select @@tiflash_compute_dispatch_policy").Check(testkit.Rows("consistent_hash"))

	// tiflash_compute_dispatch_policy is global variable.
	tk.MustExec("set @@session.tiflash_compute_dispatch_policy = 'consistent_hash';")
	tk.MustQuery("select @@tiflash_compute_dispatch_policy").Check(testkit.Rows("consistent_hash"))
	tk.MustExec("set @@session.tiflash_compute_dispatch_policy = 'round_robin';")
	tk.MustQuery("select @@tiflash_compute_dispatch_policy").Check(testkit.Rows("round_robin"))
	err = tk.ExecToErr("set @@session.tiflash_compute_dispatch_policy = 'error_dispatch_policy';")
	require.Error(t, err)
	require.Equal(t, "unexpected tiflash_compute dispatch policy, expect [consistent_hash round_robin], got error_dispatch_policy", err.Error())

	// Invalid values.
	err = tk.ExecToErr("set global tiflash_compute_dispatch_policy = 'error_dispatch_policy';")
	require.Error(t, err)
	require.Equal(t, "unexpected tiflash_compute dispatch policy, expect [consistent_hash round_robin], got error_dispatch_policy", err.Error())
	err = tk.ExecToErr("set global tiflash_compute_dispatch_policy = '';")
	require.Error(t, err)
	require.Equal(t, "unexpected tiflash_compute dispatch policy, expect [consistent_hash round_robin], got ", err.Error())
	err = tk.ExecToErr("set global tiflash_compute_dispatch_policy = 100;")
	require.Error(t, err)
	require.Equal(t, "unexpected tiflash_compute dispatch policy, expect [consistent_hash round_robin], got 100", err.Error())

	tk.MustExec("create table t(c1 int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")

	err = tiflashcompute.InitGlobalTopoFetcher(tiflashcompute.TestASStr, "tmpAddr", "tmpClusterID", false)
	require.NoError(t, err)

	useASs := []bool{true, false}
	// Valid values.
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/testWhichDispatchPolicy")
	for _, useAS := range useASs {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.UseAutoScaler = useAS
		})
		validPolicies := tiflashcompute.GetValidDispatchPolicy()
		for _, p := range validPolicies {
			tk.MustExec(fmt.Sprintf("set global tiflash_compute_dispatch_policy = '%s';", p))
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			tk1.MustQuery("select @@tiflash_compute_dispatch_policy").Check(testkit.Rows(p))
			require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/testWhichDispatchPolicy", fmt.Sprintf(`return("%s")`, p)))
			err = tk1.ExecToErr("select * from t;")
			if useAS {
				// Expect error, because TestAutoScaler return empty topo.
				require.Contains(t, err.Error(), "Cannot find proper topo to dispatch MPPTask: topo from AutoScaler is empty")
			} else {
				// This error message means we use PD instead of AutoScaler.
				require.Contains(t, err.Error(), "tiflash_compute node is unavailable")
			}
			require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/testWhichDispatchPolicy"))
		}
	}
}

func TestDisaggregatedTiFlashGeneratedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 varchar(100), c2 varchar(100) AS (lower(c1)));")
	tk.MustExec("insert into t1(c1) values('ABC');")
	tk.MustExec("alter table t1 set tiflash replica 1;")
	tb := external.GetTableByName(t, tk, "test", "t1")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2(c1 int, c2 varchar(100));")
	tk.MustExec("insert into t2 values(1, 'xhy'), (2, 'abc');")
	tk.MustExec("alter table t2 set tiflash replica 1;")
	tb = external.GetTableByName(t, tk, "test", "t2")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("alter table t2 add index idx2((lower(c2)));")

	nthPlan := 100
	test1 := func(forceTiFlash bool) {
		if forceTiFlash {
			tk.MustExec("set tidb_isolation_read_engines = 'tiflash'")
		} else {
			tk.MustExec("set tidb_isolation_read_engines = 'tikv,tiflash'")
		}
		sqls := []string{
			"explain select /*+ nth_plan(%d) */ * from t2 where lower(c2) = 'abc';",
			"explain select /*+ nth_plan(%d) */ count(*) from t2 where lower(c2) = 'abc';",
			"explain select /*+ nth_plan(%d) */ count(c1) from t2 where lower(c2) = 'abc';",
		}
		for _, sql := range sqls {
			var genTiFlashPlan bool
			var selectionPushdownTiFlash bool
			var aggPushdownTiFlash bool

			for i := 0; i < nthPlan; i++ {
				s := fmt.Sprintf(sql, i)
				rows := tk.MustQuery(s).Rows()
				for _, row := range rows {
					line := fmt.Sprintf("%v", row)
					if strings.Contains(line, "tiflash") {
						genTiFlashPlan = true
					}
					if strings.Contains(line, "Selection") && strings.Contains(line, "tiflash") {
						selectionPushdownTiFlash = true
					}
					if strings.Contains(line, "Agg") && strings.Contains(line, "tiflash") {
						aggPushdownTiFlash = true
					}
				}
			}
			if forceTiFlash {
				// Can generate tiflash plan, also Agg/Selection can push down to tiflash.
				require.True(t, genTiFlashPlan)
				require.True(t, selectionPushdownTiFlash)
				if strings.Contains(sql, "count") {
					require.True(t, aggPushdownTiFlash)
				}
			} else {
				// Can generate tiflash plan, but Agg/Selection cannot push down to tiflash.
				require.True(t, genTiFlashPlan)
				require.False(t, selectionPushdownTiFlash)
				if strings.Contains(sql, "count") {
					require.False(t, aggPushdownTiFlash)
				}
			}
		}
	}

	test2 := func() {
		// Can generate tiflash plan when select generated column.
		// But Agg cannot push down to tiflash.
		sqls := []string{
			"explain select /*+ nth_plan(%d) */ * from t1;",
			"explain select /*+ nth_plan(%d) */ c2 from t1;",
			"explain select /*+ nth_plan(%d) */ count(c2) from t1;",
		}
		for _, sql := range sqls {
			var genTiFlashPlan bool
			var aggPushdownTiFlash bool
			for i := 0; i < nthPlan; i++ {
				s := fmt.Sprintf(sql, i)
				rows := tk.MustQuery(s).Rows()
				for _, row := range rows {
					line := fmt.Sprintf("%v", row)
					if strings.Contains(line, "tiflash") {
						genTiFlashPlan = true
					}
					if strings.Contains(line, "tiflash") && strings.Contains(line, "Agg") {
						aggPushdownTiFlash = true
					}
				}
			}
			require.True(t, genTiFlashPlan)
			if strings.Contains(sql, "count") {
				require.False(t, aggPushdownTiFlash)
			}
		}
	}

	config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = false
	})
	test1(true)
	test1(false)
	test2()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = true
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.DisaggregatedTiFlash = false
	})
	test1(true)
	test1(false)
	test2()
}

func TestMppStoreCntWithErrors(t *testing.T) {
	// mock non-root tasks return error
	var mppStoreCountPDError = "github.com/pingcap/tidb/pkg/store/copr/mppStoreCountPDError"
	var mppStoreCountSetMPPCnt = "github.com/pingcap/tidb/pkg/store/copr/mppStoreCountSetMPPCnt"
	var mppStoreCountSetLastUpdateTime = "github.com/pingcap/tidb/pkg/store/copr/mppStoreCountSetLastUpdateTime"
	var mppStoreCountSetLastUpdateTimeP2 = "github.com/pingcap/tidb/pkg/store/copr/mppStoreCountSetLastUpdateTimeP2"

	store := testkit.CreateMockStore(t, withMockTiFlash(3))
	{
		mppCnt, err := store.GetMPPClient().GetMPPStoreCount()
		require.Nil(t, err)
		require.Equal(t, mppCnt, 3)
	}
	require.Nil(t, failpoint.Enable(mppStoreCountSetMPPCnt, `return(1000)`))
	{
		mppCnt, err := store.GetMPPClient().GetMPPStoreCount()
		require.Nil(t, err)
		// meet cache
		require.Equal(t, mppCnt, 3)
	}
	require.Nil(t, failpoint.Enable(mppStoreCountSetLastUpdateTime, `return("0")`))
	{
		mppCnt, err := store.GetMPPClient().GetMPPStoreCount()
		require.Nil(t, err)
		// update cache
		require.Equal(t, mppCnt, 1000)
	}
	require.Nil(t, failpoint.Enable(mppStoreCountPDError, `return(true)`))
	{
		_, err := store.GetMPPClient().GetMPPStoreCount()
		require.Error(t, err)
	}
	require.Nil(t, failpoint.Disable(mppStoreCountPDError))
	require.Nil(t, failpoint.Enable(mppStoreCountSetMPPCnt, `return(2222)`))
	// set last update time to the latest
	require.Nil(t, failpoint.Enable(mppStoreCountSetLastUpdateTime, fmt.Sprintf(`return("%d")`, time.Now().UnixMicro())))
	{
		mppCnt, err := store.GetMPPClient().GetMPPStoreCount()
		require.Nil(t, err)
		// still update cache
		require.Equal(t, mppCnt, 2222)
	}
	require.Nil(t, failpoint.Enable(mppStoreCountSetLastUpdateTime, `return("1")`))
	// fail to get lock and old cache
	require.Nil(t, failpoint.Enable(mppStoreCountSetLastUpdateTimeP2, `return("2")`))
	require.Nil(t, failpoint.Enable(mppStoreCountPDError, `return(true)`))
	{
		mppCnt, err := store.GetMPPClient().GetMPPStoreCount()
		require.Nil(t, err)
		require.Equal(t, mppCnt, 2222)
	}
	require.Nil(t, failpoint.Disable(mppStoreCountSetMPPCnt))
	require.Nil(t, failpoint.Disable(mppStoreCountSetLastUpdateTime))
	require.Nil(t, failpoint.Disable(mppStoreCountSetLastUpdateTimeP2))
	require.Nil(t, failpoint.Disable(mppStoreCountPDError))
}

func TestMPP47766(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(1))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_allow_mpp=1")
	tk.MustExec("set @@session.tidb_enforce_mpp=1")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=off")

	tk.MustExec("CREATE TABLE `traces` (" +
		"  `test_time` timestamp NOT NULL," +
		"  `test_time_gen` date GENERATED ALWAYS AS (date(`test_time`)) VIRTUAL," +
		"  KEY `traces_date_idx` (`test_time_gen`)" +
		")")
	tk.MustExec("alter table `traces` set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "traces")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustQuery("explain select date(test_time), count(1) as test_date from `traces` group by 1").Check(testkit.Rows(
		"Projection_4 8000.00 root  test.traces.test_time_gen->Column#5, Column#4",
		"└─HashAgg_8 8000.00 root  group by:test.traces.test_time_gen, funcs:count(1)->Column#4, funcs:firstrow(test.traces.test_time_gen)->test.traces.test_time_gen",
		"  └─TableReader_20 10000.00 root  MppVersion: 2, data:ExchangeSender_19",
		"    └─ExchangeSender_19 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"      └─TableFullScan_18 10000.00 mpp[tiflash] table:traces keep order:false, stats:pseudo"))
	tk.MustQuery("explain select /*+ read_from_storage(tiflash[traces]) */ date(test_time) as test_date, count(1) from `traces` group by 1").Check(testkit.Rows(
		"TableReader_31 8000.00 root  MppVersion: 2, data:ExchangeSender_30",
		"└─ExchangeSender_30 8000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Projection_5 8000.00 mpp[tiflash]  date(test.traces.test_time)->Column#5, Column#4",
		"    └─Projection_26 8000.00 mpp[tiflash]  Column#4, test.traces.test_time",
		"      └─HashAgg_27 8000.00 mpp[tiflash]  group by:Column#13, funcs:sum(Column#14)->Column#4, funcs:firstrow(Column#15)->test.traces.test_time",
		"        └─ExchangeReceiver_29 8000.00 mpp[tiflash]  ",
		"          └─ExchangeSender_28 8000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#13, collate: binary]",
		"            └─HashAgg_25 8000.00 mpp[tiflash]  group by:Column#17, funcs:count(1)->Column#14, funcs:firstrow(Column#16)->Column#15",
		"              └─Projection_32 10000.00 mpp[tiflash]  test.traces.test_time->Column#16, date(test.traces.test_time)->Column#17",
		"                └─TableFullScan_15 10000.00 mpp[tiflash] table:traces keep order:false, stats:pseudo"))
}

func TestUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_allow_mpp=1")
	tk.MustExec("set @@session.tidb_enforce_mpp=1")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=off")

	for x := 0; x < 2; x++ {
		tk.MustExec("drop table if exists t")
		if x == 0 {
			// Test cache table.
			tk.MustExec("create table t(a int not null primary key, b int not null)")
			tk.MustExec("alter table t set tiflash replica 1")
			tb := external.GetTableByName(t, tk, "test", "t")
			err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
			require.NoError(t, err)
			tk.MustExec("alter table t cache")
		} else {
			// Test dirty transaction.
			tk.MustExec("create table t(a int not null primary key, b int not null) partition by hash(a) partitions 2")
			tk.MustExec("alter table t set tiflash replica 1")
			tb := external.GetTableByName(t, tk, "test", "t")
			err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
			require.NoError(t, err)
		}

		insertStr := "insert into t values(0, 0)"
		for i := 1; i < 10; i++ {
			insertStr += fmt.Sprintf(",(%d, %d)", i, i)
		}
		tk.MustExec(insertStr)

		if x != 0 {
			// Test dirty transaction.
			tk.MustExec("begin")
		}

		// Test Basic.
		sql := "select /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t"
		checkMPPInExplain(t, tk, "explain "+sql)
		tk.MustQuery(sql).Check(testkit.Rows("10"))

		// Test Delete.
		tk.MustExec("delete from t where a = 0")

		sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t"
		checkMPPInExplain(t, tk, "explain "+sql)
		tk.MustQuery(sql).Check(testkit.Rows("9"))

		sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ a, b from t order by 1"
		checkMPPInExplain(t, tk, "explain "+sql)
		tk.MustQuery(sql).Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6", "7 7", "8 8", "9 9"))

		// Test Insert.
		tk.MustExec("insert into t values(100, 100)")

		sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t"
		checkMPPInExplain(t, tk, "explain "+sql)
		tk.MustQuery(sql).Check(testkit.Rows("10"))

		sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ a, b from t order by 1, 2"
		checkMPPInExplain(t, tk, "explain "+sql)
		tk.MustQuery(sql).Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6", "7 7", "8 8", "9 9", "100 100"))

		// Test Update
		tk.MustExec("update t set b = 200 where a = 100")

		sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t"
		checkMPPInExplain(t, tk, "explain "+sql)
		tk.MustQuery(sql).Check(testkit.Rows("10"))

		sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ a, b from t order by 1, 2"
		checkMPPInExplain(t, tk, "explain "+sql)
		tk.MustQuery(sql).Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6", "7 7", "8 8", "9 9", "100 200"))

		if x != 0 {
			// Test dirty transaction.
			tk.MustExec("commit")
		}

		sql = "select  /*+ READ_FROM_STORAGE(tiflash[t]) */ count(1) from t"
		checkMPPInExplain(t, tk, "explain "+sql)
		tk.MustQuery(sql).Check(testkit.Rows("10"))

		if x == 0 {
			tk.MustExec("alter table t nocache")
		}
	}
}

func checkMPPInExplain(t *testing.T, tk *testkit.TestKit, sql string) {
	rows := tk.MustQuery(sql).Rows()
	resBuff := bytes.NewBufferString("")
	for _, row := range rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	res := resBuff.String()
	require.Contains(t, res, "mpp[tiflash]")
}

func TestMPPRecovery(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	checkStrs := []string{"0 0"}
	insertStr := "insert into t values(0, 0)"
	for i := 1; i < 1500; i++ {
		insertStr += fmt.Sprintf(",(%d, %d)", i, i)
		checkStrs = append(checkStrs, fmt.Sprintf("%d %d", i, i))
	}
	tk.MustExec(insertStr)
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	sql := "select * from t order by 1, 2"
	const packagePath = "github.com/pingcap/tidb/pkg/executor/internal/mpp/"

	require.NoError(t, failpoint.Enable(packagePath+"mpp_recovery_test_mock_enable", "return()"))
	require.NoError(t, failpoint.Enable(packagePath+"mpp_recovery_test_ignore_recovery_err", "return()"))
	// Test different chunk size. And force one mpp err.
	{
		require.NoError(t, failpoint.Enable(packagePath+"mpp_recovery_test_max_err_times", "return(1)"))

		tk.MustExec("set @@tidb_max_chunk_size = default")
		tk.MustQuery(sql).Check(testkit.Rows(checkStrs...))
		tk.MustExec("set @@tidb_max_chunk_size = 32")
		tk.MustQuery(sql).Check(testkit.Rows(checkStrs...))

		require.NoError(t, failpoint.Disable(packagePath+"mpp_recovery_test_max_err_times"))
	}

	// Test exceeds max recovery times. Default max times is 3.
	{
		require.NoError(t, failpoint.Enable(packagePath+"mpp_recovery_test_max_err_times", "return(5)"))

		tk.MustExec("set @@tidb_max_chunk_size = 32")
		err := tk.QueryToErr(sql)
		strings.Contains(err.Error(), "mock mpp err")

		require.NoError(t, failpoint.Disable(packagePath+"mpp_recovery_test_max_err_times"))
	}

	{
		// When AllowFallbackToTiKV, mpp err recovery should be disabled.
		// So event we inject mock err multiple times, the query should be ok.
		tk.MustExec("set @@tidb_allow_fallback_to_tikv = \"tiflash\"")
		require.NoError(t, failpoint.Enable(packagePath+"mpp_recovery_test_max_err_times", "return(5)"))

		tk.MustExec("set @@tidb_max_chunk_size = 32")
		tk.MustQuery(sql).Check(testkit.Rows(checkStrs...))

		tk.MustExec("set @@tidb_allow_fallback_to_tikv = default")
		require.NoError(t, failpoint.Disable(packagePath+"mpp_recovery_test_max_err_times"))
	}

	// Test hold logic. Default hold 4 * MaxChunkSize rows.
	{
		require.NoError(t, failpoint.Enable(packagePath+"mpp_recovery_test_max_err_times", "return(0)"))

		tk.MustExec("set @@tidb_max_chunk_size = 32")
		expectedHoldSize := 2
		require.NoError(t, failpoint.Enable(packagePath+"mpp_recovery_test_hold_size", fmt.Sprintf("1*return(%d)", expectedHoldSize)))
		tk.MustQuery(sql).Check(testkit.Rows(checkStrs...))
		require.NoError(t, failpoint.Disable(packagePath+"mpp_recovery_test_hold_size"))

		require.NoError(t, failpoint.Disable(packagePath+"mpp_recovery_test_max_err_times"))
	}
	require.NoError(t, failpoint.Disable(packagePath+"mpp_recovery_test_ignore_recovery_err"))
	require.NoError(t, failpoint.Disable(packagePath+"mpp_recovery_test_mock_enable"))

	{
		// We have 2 mock tiflash, but the table is small, so only 1 tiflash node is in computation.
		require.NoError(t, failpoint.Enable(packagePath+"mpp_recovery_test_check_node_cnt", "return(1)"))

		tk.MustExec("set @@tidb_max_chunk_size = 32")
		tk.MustQuery(sql).Check(testkit.Rows(checkStrs...))

		require.NoError(t, failpoint.Disable(packagePath+"mpp_recovery_test_check_node_cnt"))
	}

	tk.MustExec("set @@tidb_max_chunk_size = default")
}

func TestIssue50358(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(1))
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

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c int not null primary key)")
	tk.MustExec("alter table t1 set tiflash replica 1")
	tb = external.GetTableByName(t, tk, "test", "t1")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("insert into t1 values(3)")

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=ON")
	for i := 0; i < 20; i++ {
		// test if it is stable.
		tk.MustQuery("select 8 from t join t1").Check(testkit.Rows("8", "8"))
	}
}

func TestMppAggShouldAlignFinalMode(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(1))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (" +
		"  d date," +
		"  v int," +
		"  primary key(d, v)" +
		") partition by range columns (d) (" +
		"  partition p1 values less than ('2023-07-02')," +
		"  partition p2 values less than ('2023-07-03')" +
		");")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec(`set tidb_partition_prune_mode='static';`)
	err = failpoint.Enable("github.com/pingcap/tidb/pkg/expression/aggregation/show-agg-mode", "return(true)")
	require.Nil(t, err)

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustQuery("explain format='brief' select 1 from (" +
		"  select /*+ read_from_storage(tiflash[t]) */ sum(1)" +
		"  from t where d BETWEEN '2023-07-01' and '2023-07-03' group by d" +
		") total;").Check(testkit.Rows("Projection 400.00 root  1->Column#4",
		"└─HashAgg 400.00 root  group by:test.t.d, funcs:count(complete,1)->Column#8",
		"  └─PartitionUnion 400.00 root  ",
		"    ├─Projection 200.00 root  test.t.d",
		"    │ └─HashAgg 200.00 root  group by:test.t.d, funcs:firstrow(partial2,test.t.d)->test.t.d, funcs:count(final,Column#12)->Column#9",
		"    │   └─TableReader 200.00 root  MppVersion: 2, data:ExchangeSender",
		"    │     └─ExchangeSender 200.00 mpp[tiflash]  ExchangeType: PassThrough",
		"    │       └─HashAgg 200.00 mpp[tiflash]  group by:test.t.d, funcs:count(partial1,1)->Column#12",
		"    │         └─TableRangeScan 250.00 mpp[tiflash] table:t, partition:p1 range:[2023-07-01,2023-07-03], keep order:false, stats:pseudo",
		"    └─Projection 200.00 root  test.t.d",
		"      └─HashAgg 200.00 root  group by:test.t.d, funcs:firstrow(partial2,test.t.d)->test.t.d, funcs:count(final,Column#14)->Column#10",
		"        └─TableReader 200.00 root  MppVersion: 2, data:ExchangeSender",
		"          └─ExchangeSender 200.00 mpp[tiflash]  ExchangeType: PassThrough",
		"            └─HashAgg 200.00 mpp[tiflash]  group by:test.t.d, funcs:count(partial1,1)->Column#14",
		"              └─TableRangeScan 250.00 mpp[tiflash] table:t, partition:p2 range:[2023-07-01,2023-07-03], keep order:false, stats:pseudo"))

	err = failpoint.Disable("github.com/pingcap/tidb/pkg/expression/aggregation/show-agg-mode")
	require.Nil(t, err)
}
