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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestExplainCostTrace(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1)")

	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustQuery("explain format='cost_trace' select * from t").Check(testkit.Rows(
		`TableReader_5 10000.00 177906.67 ((scan(10000*logrowsize(32)*tikv_scan_factor(40.7))) + (net(10000*rowsize(16)*tidb_kv_net_factor(3.96))))/15.00 root  data:TableFullScan_4`,
		`└─TableFullScan_4 10000.00 2035000.00 scan(10000*logrowsize(32)*tikv_scan_factor(40.7)) cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery("explain analyze format='cost_trace' select * from t").CheckAt([]int{0, 1, 2, 3, 4}, [][]interface{}{
		{"TableReader_5", "10000.00", "177906.67", "((scan(10000*logrowsize(32)*tikv_scan_factor(40.7))) + (net(10000*rowsize(16)*tidb_kv_net_factor(3.96))))/15.00", "1"},
		{"└─TableFullScan_4", "10000.00", "2035000.00", "scan(10000*logrowsize(32)*tikv_scan_factor(40.7))", "1"},
	})

	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustQuery("explain format='cost_trace' select * from t").Check(testkit.Rows(
		// cost trace on model ver1 is not supported
		`TableReader_5 10000.00 34418.00 N/A root  data:TableFullScan_4`,
		`└─TableFullScan_4 10000.00 435000.00 N/A cop[tikv] table:t keep order:false, stats:pseudo`,
	))
	tk.MustQuery("explain analyze format='cost_trace' select * from t").CheckAt([]int{0, 1, 2, 3, 4}, [][]interface{}{
		{"TableReader_5", "10000.00", "34418.00", "N/A", "1"},
		{"└─TableFullScan_4", "10000.00", "435000.00", "N/A", "1"},
	})
}

func TestExplainAnalyze(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tk.MustExec("create table t1(a int, b int, c int, key idx(a, b))")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5)")
	tk.MustExec("insert into t2 values (2, 22), (3, 33), (5, 55), (233, 2), (333, 3), (3434, 5)")
	tk.MustExec("analyze table t1, t2")
	rs := tk.MustQuery("explain analyze select t1.a, t1.b, sum(t1.c) from t1 join t2 on t1.a = t2.b where t1.a > 1")
	require.Len(t, rs.Rows(), 10)
	for _, row := range rs.Rows() {
		require.Len(t, row, 9)
		execInfo := row[5].(string)
		require.Contains(t, execInfo, "time")
		require.Contains(t, execInfo, "loops")
		if strings.Contains(row[0].(string), "Reader") || strings.Contains(row[0].(string), "IndexLookUp") {
			require.Contains(t, execInfo, "cop_task")
		}
	}
}

func constructInsertSQL(i, n int) string {
	sql := "insert into t (a,b,c,e)values "
	for j := 0; j < n; j++ {
		sql += fmt.Sprintf("(%d, %d, '%d', %d)", i*n+j, i, i+j, i*n+j)
		if j != n-1 {
			sql += ", "
		}
	}
	return sql
}

func BenchmarkOptimize(b *testing.B) {
	store := testkit.CreateMockStore(b)

	testKit := testkit.NewTestKit(b, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("create index d on t (d)")
	testKit.MustExec("create index e on t (e)")
	testKit.MustExec("create index b_c on t (b,c)")
	testKit.MustExec("create index ts on t (ts)")
	for i := 0; i < 100; i++ {
		testKit.MustExec(constructInsertSQL(i, 100))
	}
	testKit.MustExec("analyze table t")
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select count(*) from t group by e",
			best: "IndexReader(Index(t.e)[[NULL,+inf]])->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e <= 10 group by e",
			best: "IndexReader(Index(t.e)[[-inf,10]])->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e <= 50",
			best: "IndexReader(Index(t.e)[[-inf,50]]->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(*) from t where c > '1' group by b",
			best: "IndexReader(Index(t.b_c)[[NULL,+inf]]->Sel([gt(test.t.c, 1)]))->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e = 1 group by b",
			best: "IndexLookUp(Index(t.e)[[1,1]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(*) from t where e > 1 group by b",
			best: "TableReader(Table(t)->Sel([gt(test.t.e, 1)])->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 20",
			best: "IndexLookUp(Index(t.b)[[-inf,20]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 30",
			best: "IndexLookUp(Index(t.b)[[-inf,30]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 40",
			best: "IndexLookUp(Index(t.b)[[-inf,40]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)])->HashAgg)->HashAgg",
		},
		{
			sql:  "select * from t where t.b <= 40",
			best: "IndexLookUp(Index(t.b)[[-inf,40]], Table(t))",
		},
		{
			sql:  "select * from t where t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)]))",
		},
		// test panic
		{
			sql:  "select * from t where 1 and t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)]))",
		},
		{
			sql:  "select * from t where t.b <= 100 order by t.a limit 1",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 100)])->Limit)->Limit",
		},
		{
			sql:  "select * from t where t.b <= 1 order by t.a limit 10",
			best: "IndexLookUp(Index(t.b)[[-inf,1]]->TopN([test.t.a],0,10), Table(t))->TopN([test.t.a],0,10)",
		},
		{
			sql:  "select * from t use index(b) where b = 1 order by a",
			best: "IndexLookUp(Index(t.b)[[1,1]], Table(t))->Sort",
		},
		// test datetime
		{
			sql:  "select * from t where d < cast('1991-09-05' as datetime)",
			best: "IndexLookUp(Index(t.d)[[-inf,1991-09-05 00:00:00)], Table(t))",
		},
		// test timestamp
		{
			sql:  "select * from t where ts < '1991-09-05'",
			best: "IndexLookUp(Index(t.ts)[[-inf,1991-09-05 00:00:00)], Table(t))",
		},
	}
	for _, tt := range tests {
		ctx := testKit.Session()
		stmts, err := session.Parse(ctx, tt.sql)
		require.NoError(b, err)
		require.Len(b, stmts, 1)
		stmt := stmts[0]
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(context.Background(), ctx, stmt, core.WithPreprocessorReturn(ret))
		require.NoError(b, err)

		b.Run(tt.sql, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
				require.NoError(b, err)
			}
			b.ReportAllocs()
		})
	}
}

func TestIssue9805(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`
		create table t1 (
			id bigint primary key,
			a bigint not null,
			b varchar(100) not null,
			c varchar(10) not null,
			d bigint as (a % 30) not null,
			key (d, b, c)
		)
	`)
	tk.MustExec(`
		create table t2 (
			id varchar(50) primary key,
			a varchar(100) unique,
			b datetime,
			c varchar(45),
			d int not null unique auto_increment
		)
	`)
	// Test when both tables are empty, EXPLAIN ANALYZE for IndexLookUp would not panic.
	tk.MustQuery("explain analyze select /*+ TIDB_INLJ(t2) */ t1.id, t2.a from t1 join t2 on t1.a = t2.d where t1.b = 't2' and t1.d = 4")
}

func TestUpdateProjEliminate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("explain update t t1, (select distinct b from t) t2 set t1.b = t2.b")

	tk.MustExec("drop table if exists tb1, tb2")
	tk.MustExec("create table tb1(a int, b int, primary key(a))")
	tk.MustExec("create table tb2 (a int, b int, c int, d datetime, primary key(c),key idx_u(a));")
	tk.MustExec("update tb1 set tb1.b=(select tb2.b from tb2 where tb2.a=tb1.a order by c desc limit 1);")
}

func TestBatchPointGetTablePartition(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t1,t2,t3,t4,t5,t6")

	testKit.MustExec("create table t1(a int, b int, primary key(a,b) nonclustered) partition by hash(b) partitions 2")
	testKit.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t1 where a in (1,2) and b = 1").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t1, index:PRIMARY(a, b) keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t1 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─Batch_Point_Get 2.00 root table:t1, index:PRIMARY(a, b) keep order:false, desc:false",
		"└─Batch_Point_Get 2.00 root table:t1, index:PRIMARY(a, b) keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t1 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t1 where a in (1,2) and b = 1").Check(testkit.Rows(
		"IndexReader 2.00 root partition:p1 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t1, index:PRIMARY(a, b) range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t1 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"IndexReader 2.00 root partition:p0,p1 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t1, index:PRIMARY(a, b) range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t1 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))

	testKit.MustExec("create table t2(a int, b int, primary key(a,b) nonclustered) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
	testKit.MustExec("insert into t2 values(1,1),(1,2),(2,1),(2,2)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t2 where a in (1,2) and b = 1").Check(testkit.Rows(
		"IndexReader 2.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t2, partition:p0, index:PRIMARY(a, b) range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t2 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t2 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─IndexReader 2.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 2.00 cop[tikv] table:t2, partition:p0, index:PRIMARY(a, b) range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
		"└─IndexReader 2.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 2.00 cop[tikv] table:t2, partition:p1, index:PRIMARY(a, b) range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t2 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t2 where a in (1,2) and b = 1").Check(testkit.Rows(
		"IndexReader 2.00 root partition:p0 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t2, index:PRIMARY(a, b) range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t2 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t2 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"IndexReader 2.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t2, index:PRIMARY(a, b) range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t2 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))

	testKit.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	testKit.MustExec("create table t3(a int, b int, primary key(a,b)) partition by hash(b) partitions 2")
	testKit.MustExec("insert into t3 values(1,1),(1,2),(2,1),(2,2)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t3 where a in (1,2) and b = 1").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t3, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t3 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t3 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"PartitionUnion 0.04 root  ",
		"├─Batch_Point_Get 2.00 root table:t3, clustered index:PRIMARY(a, b) keep order:false, desc:false",
		"└─Batch_Point_Get 2.00 root table:t3, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t3 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t3 where a in (1,2) and b = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t3 range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t3 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t3 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"TableReader 2.00 root partition:p0,p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t3 range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t3 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))

	testKit.MustExec("create table t4(a int, b int, primary key(a,b)) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
	testKit.MustExec("insert into t4 values(1,1),(1,2),(2,1),(2,2)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t4 where a in (1,2) and b = 1").Check(testkit.Rows(
		"TableReader 2.00 root  data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t4, partition:p0 range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t4 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t4 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"PartitionUnion 0.04 root  ",
		"├─TableReader 2.00 root  data:TableRangeScan",
		"│ └─TableRangeScan 2.00 cop[tikv] table:t4, partition:p0 range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
		"└─TableReader 2.00 root  data:TableRangeScan",
		"  └─TableRangeScan 2.00 cop[tikv] table:t4, partition:p1 range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t4 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t4 where a in (1,2) and b = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p0 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t4 range:[1 1,1 1], [2 1,2 1], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t4 where a in (1,2) and b = 1").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("explain format = 'brief' select * from t4 where a = 1 and b in (1,2)").Check(testkit.Rows(
		"TableReader 2.00 root partition:all data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t4 range:[1 1,1 1], [1 2,1 2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t4 where a = 1 and b in (1,2)").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))

	testKit.MustExec("create table t5(a int, b int, primary key(a)) partition by hash(a) partitions 2")
	testKit.MustExec("insert into t5 values(1,0),(2,0),(3,0),(4,0)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t5 where a in (1,2) and 1 = 1").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─Batch_Point_Get 2.00 root table:t5 handle:[1 2], keep order:false, desc:false",
		"└─Batch_Point_Get 2.00 root table:t5 handle:[1 2], keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t5 where a in (1,2) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("explain format = 'brief' select * from t5 where a in (1,3) and 1 = 1").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t5 handle:[1 3], keep order:false, desc:false",
	))
	testKit.MustQuery("select * from t5 where a in (1,3) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"3 0",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t5 where a in (1,2) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p0,p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t5 range:[1,1], [2,2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t5 where a in (1,2) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("explain format = 'brief' select * from t5 where a in (1,3) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p1 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t5 range:[1,1], [3,3], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t5 where a in (1,3) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"3 0",
	))

	testKit.MustExec("create table t6(a int, b int, primary key(a)) partition by range(a) (partition p0 values less than (3), partition p1 values less than maxvalue)")
	testKit.MustExec("insert into t6 values(1,0),(2,0),(3,0),(4,0)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustQuery("explain format = 'brief' select * from t6 where a in (1,2) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root  data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t6, partition:p0 range:[1,1], [2,2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t6 where a in (1,2) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("explain format = 'brief' select * from t6 where a in (1,3) and 1 = 1").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─TableReader 2.00 root  data:TableRangeScan",
		"│ └─TableRangeScan 2.00 cop[tikv] table:t6, partition:p0 range:[1,1], [3,3], keep order:false, stats:pseudo",
		"└─TableReader 2.00 root  data:TableRangeScan",
		"  └─TableRangeScan 2.00 cop[tikv] table:t6, partition:p1 range:[1,1], [3,3], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t6 where a in (1,3) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"3 0",
	))
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	testKit.MustQuery("explain format = 'brief' select * from t6 where a in (1,2) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:p0 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t6 range:[1,1], [2,2], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t6 where a in (1,2) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("explain format = 'brief' select * from t6 where a in (1,3) and 1 = 1").Check(testkit.Rows(
		"TableReader 2.00 root partition:all data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t6 range:[1,1], [3,3], keep order:false, stats:pseudo",
	))
	testKit.MustQuery("select * from t6 where a in (1,3) and 1 = 1").Sort().Check(testkit.Rows(
		"1 0",
		"3 0",
	))
}

// TestAppendIntPkToIndexTailForRangeBuilding tests for issue25219 https://github.com/pingcap/tidb/issues/25219.
func TestAppendIntPkToIndexTailForRangeBuilding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t25219(a int primary key, col3 int, col1 int, index idx(col3))")
	tk.MustExec("insert into t25219 values(1, 1, 1)")
	tk.MustExec("analyze table t25219")
	tk.MustQuery("select * from t25219 WHERE (col3 IS NULL OR col1 IS NOT NULL AND col3 <= 6659) AND col3 = 1;").Check(testkit.Rows("1 1 1"))
}
