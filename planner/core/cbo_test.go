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

	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
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
