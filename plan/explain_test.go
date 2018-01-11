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
// See the License for the specific language governing permissions and
// limitations under the License.

package plan_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testExplainSuite{})

type testExplainSuite struct {
}

func (s *testExplainSuite) TestExplain(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index c2 (c2))")
	tk.MustExec("create table t2 (c1 int unique, c2 int)")
	tk.MustExec("insert into t2 values(1, 0), (2, 1)")
	tk.MustExec("create table t3 (a bigint, b bigint, c bigint, d bigint)")

	tests := []struct {
		sql    string
		expect []string
	}{
		{
			"select * from t3 where exists (select s.a from t3 s having sum(s.a) = t3.a )",
			[]string{
				"TableScan_15   cop table:t3, range:(-inf,+inf), keep order:false 8000",
				"TableReader_16 Projection_12  root data:TableScan_15 8000",
				"Projection_12 HashSemiJoin_14 TableReader_16 root test.t3.a, test.t3.b, test.t3.c, test.t3.d, cast(test.t3.a) 8000",
				"TableScan_18 HashAgg_17  cop table:s, range:(-inf,+inf), keep order:false 8000",
				"HashAgg_17  TableScan_18 cop type:complete, funcs:sum(s.a) 1",
				"TableReader_20 HashAgg_19  root data:HashAgg_17 1",
				"HashAgg_19 HashSemiJoin_14 TableReader_20 root type:final, funcs:sum(col_0) 1",
				"HashSemiJoin_14 Projection_11 Projection_12,HashAgg_19 root right:HashAgg_19, equal:[eq(cast(test.t3.a), sel_agg_1)] 6400",
				"Projection_11  HashSemiJoin_14 root test.t3.a, test.t3.b, test.t3.c, test.t3.d 6400",
			},
		},
		{
			"select * from t1",
			[]string{
				"TableScan_3   cop table:t1, range:(-inf,+inf), keep order:false 8000",
				"TableReader_4   root data:TableScan_3 8000",
			},
		},
		{
			"select * from t1 order by c2",
			[]string{
				"IndexScan_10   cop table:t1, index:c2, range:[<nil>,+inf], out of order:false 8000",
				"TableScan_11   cop table:t1, keep order:false 8000",
				"IndexLookUp_12   root index:IndexScan_10, table:TableScan_11 8000",
			},
		},
		{
			"select * from t2 order by c2",
			[]string{
				"TableScan_4   cop table:t2, range:(-inf,+inf), keep order:false 8000",
				"TableReader_5 Sort_3  root data:TableScan_4 8000",
				"Sort_3  TableReader_5 root test.t2.c2:asc 8000",
			},
		},
		{
			"select * from t1 where t1.c1 > 0",
			[]string{
				"TableScan_4   cop table:t1, range:[1,+inf), keep order:false 3333.333333333333",
				"TableReader_5   root data:TableScan_4 3333.333333333333",
			},
		},
		{
			"select t1.c1, t1.c2 from t1 where t1.c2 = 1",
			[]string{
				"IndexScan_7   cop table:t1, index:c2, range:[1,1], out of order:true 10",
				"IndexReader_8   root index:IndexScan_7 10",
			},
		},
		{
			"select * from t1 left join t2 on t1.c2 = t2.c1 where t1.c1 > 1",
			[]string{
				"TableScan_25   cop table:t1, range:[2,+inf), keep order:false 3333.333333333333",
				"TableReader_26 HashLeftJoin_11  root data:TableScan_25 3333.333333333333",
				"TableScan_31   cop table:t2, range:(-inf,+inf), keep order:false 8000",
				"TableReader_32 HashLeftJoin_11  root data:TableScan_31 8000",
				"HashLeftJoin_11  TableReader_26,TableReader_32 root left outer join, small:TableReader_32, equal:[eq(test.t1.c2, test.t2.c1)] 4166.666666666666",
			},
		},
		{
			"update t1 set t1.c2 = 2 where t1.c1 = 1",
			[]string{
				"TableScan_4   cop table:t1, range:[1,1], keep order:false 10",
				"TableReader_5 Update_3  root data:TableScan_4 10",
				"Update_3  TableReader_5 root  10",
			},
		},
		{
			"delete from t1 where t1.c2 = 1",
			[]string{
				"IndexScan_7   cop table:t1, index:c2, range:[1,1], out of order:true 10",
				"TableScan_8   cop table:t1, keep order:false 10",
				"IndexLookUp_9 Delete_3  root index:IndexScan_7, table:TableScan_8 10",
				"Delete_3  IndexLookUp_9 root  10",
			},
		},
		{
			"select count(b.c2) from t1 a, t2 b where a.c1 = b.c2 group by a.c1",
			[]string{
				"TableScan_15   cop table:a, range:(-inf,+inf), keep order:false 8000",
				"TableReader_16 HashLeftJoin_13  root data:TableScan_15 8000",
				"TableScan_18 HashAgg_17  cop table:b, range:(-inf,+inf), keep order:false 8000",
				"HashAgg_17  TableScan_18 cop type:complete, group by:b.c2, funcs:count(b.c2), firstrow(b.c2) 6400",
				"TableReader_20 HashAgg_19  root data:HashAgg_17 6400",
				"HashAgg_19 HashLeftJoin_13 TableReader_20 root type:final, group by:, funcs:count(col_0), firstrow(col_1) 6400",
				"HashLeftJoin_13 Projection_9 TableReader_16,HashAgg_19 root inner join, small:HashAgg_19, equal:[eq(a.c1, b.c2)] 8000",
				"Projection_9  HashLeftJoin_13 root cast(join_agg_0) 8000",
			},
		},
		{
			"select * from t2 order by t2.c2 limit 0, 1",
			[]string{
				"TableScan_7 TopN_5  cop table:t2, range:(-inf,+inf), keep order:false 8000",
				"TopN_5  TableScan_7 cop  1",
				"TableReader_8 TopN_5  root data:TopN_5 1",
				"TopN_5  TableReader_8 root  1",
			},
		},
		{
			"select * from t1 where c1 > 1 and c2 = 1 and c3 < 1",
			[]string{
				"IndexScan_7 Selection_9  cop table:t1, index:c2, range:[1,1], out of order:true 10",
				"Selection_9  IndexScan_7 cop gt(test.t1.c1, 1) 10",
				"TableScan_8 Selection_10  cop table:t1, keep order:false 10",
				"Selection_10  TableScan_8 cop lt(test.t1.c3, 1) 10",
				"IndexLookUp_11   root index:Selection_9, table:Selection_10 10",
			},
		},
		{
			"select * from t1 where c1 = 1 and c2 > 1",
			[]string{
				"TableScan_4 Selection_5  cop table:t1, range:[1,1], keep order:false 10",
				"Selection_5  TableScan_4 cop gt(test.t1.c2, 1) 10",
				"TableReader_6   root data:Selection_5 10",
			},
		},
		{
			"select sum(t1.c1 in (select c1 from t2)) from t1",
			[]string{
				"TableScan_10 HashAgg_8  cop table:t1, range:(-inf,+inf), keep order:false 8000",
				"HashAgg_8  TableScan_10 cop type:complete, funcs:sum(in(test.t1.c1, 1, 2)) 1",
				"TableReader_12 HashAgg_11  root data:HashAgg_8 1",
				"HashAgg_11  TableReader_12 root type:final, funcs:sum(col_0) 1",
			},
		},
		{
			"select c1 from t1 where c1 in (select c2 from t2)",
			[]string{
				"TableScan_8   cop table:t1, range:[0,0], [1,1], keep order:false 20",
				"TableReader_9   root data:TableScan_8 20",
			},
		},
		{
			"select (select count(1) k from t1 s where s.c1 = t1.c1 having k != 0) from t1",
			[]string{
				"TableScan_13   cop table:t1, range:(-inf,+inf), keep order:false 8000",
				"TableReader_14 Apply_12  root data:TableScan_13 8000",
				"TableScan_20   cop table:s, range:(-inf,+inf), keep order:true 8000",
				"TableReader_21 Selection_4  root data:TableScan_20 8000",
				"Selection_4 StreamAgg_16 TableReader_21 root eq(s.c1, test.t1.c1) 6400",
				"StreamAgg_16 Selection_10 Selection_4 root type:stream, funcs:count(1) 1",
				"Selection_10 Apply_12 StreamAgg_16 root ne(k, 0) 0.8",
				"Apply_12 Projection_2 TableReader_14,Selection_10 root left outer join, small:Selection_10, right:Selection_10 8000",
				"Projection_2  Apply_12 root k 8000",
			},
		},
		{
			"select * from information_schema.columns",
			[]string{
				"MemTableScan_3   root  8000",
			},
		},
		{
			"select c2 = (select c2 from t2 where t1.c1 = t2.c1 order by c1 limit 1) from t1",
			[]string{
				"TableScan_14   cop table:t1, range:(-inf,+inf), keep order:false 8000",
				"TableReader_15 Apply_13  root data:TableScan_14 8000",
				"IndexScan_23   cop table:t2, index:c1, range:[<nil>,+inf], out of order:false 1.25",
				"TableScan_24   cop table:t2, keep order:false 1.25",
				"IndexLookUp_25 Selection_4  root index:IndexScan_23, table:TableScan_24 1.25",
				"Selection_4 Limit_16 IndexLookUp_25 root eq(test.t1.c1, test.t2.c1) 1",
				"Limit_16 MaxOneRow_9 Selection_4 root offset:0, count:1 1",
				"MaxOneRow_9 Apply_13 Limit_16 root  1",
				"Apply_13 Projection_2 TableReader_15,MaxOneRow_9 root left outer join, small:MaxOneRow_9, right:MaxOneRow_9 8000",
				"Projection_2  Apply_13 root eq(test.t1.c2, test.t2.c2) 8000",
			},
		},
		{
			"select * from t1 order by c1 desc limit 1",
			[]string{
				"TableScan_11 Limit_14  cop table:t1, range:(-inf,+inf), keep order:true, desc 1.25",
				"Limit_14  TableScan_11 cop offset:0, count:1 1",
				"TableReader_15 Limit_6  root data:Limit_14 1",
				"Limit_6  TableReader_15 root offset:0, count:1 1",
			},
		},
	}
	tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 1")
	tk.MustExec("set @@session.tidb_opt_agg_push_down = 1")
	for _, tt := range tests {
		result := tk.MustQuery("explain " + tt.sql)
		result.Check(testkit.Rows(tt.expect...))
	}

	insubqueryFoldTests := []struct {
		sql    string
		expect []string
	}{
		{
			"select sum(t1.c1 in (select c1 from t2)) from t1",
			[]string{
				"TableScan_18   cop table:t1, range:(-inf,+inf), keep order:true 8000",
				"TableReader_19 HashSemiJoin_16  root data:TableScan_18 8000",
				"TableScan_14   cop table:t2, range:(-inf,+inf), keep order:false 8000",
				"TableReader_15 HashSemiJoin_16  root data:TableScan_14 8000",
				"HashSemiJoin_16 StreamAgg_9 TableReader_19,TableReader_15 root right:TableReader_15, aux, equal:[eq(test.t1.c1, test.t2.c1)] 8000",
				"StreamAgg_9  HashSemiJoin_16 root type:stream, funcs:sum(5_aux_0) 1",
			},
		},
		{
			"select 1 in (select c2 from t2) from t1",
			[]string{
				"TableScan_8   cop table:t1, range:(-inf,+inf), keep order:false 8000",
				"TableReader_9 HashSemiJoin_7  root data:TableScan_8 8000",
				"TableScan_10 Selection_11  cop table:t2, range:(-inf,+inf), keep order:false 10",
				"Selection_11  TableScan_10 cop eq(1, test.t2.c2) 10",
				"TableReader_12 HashSemiJoin_7  root data:Selection_11 10",
				"HashSemiJoin_7 Projection_2 TableReader_9,TableReader_12 root right:TableReader_12, aux 8000",
				"Projection_2  HashSemiJoin_7 root 5_aux_0 8000",
			},
		},
		{
			"select sum(6 in (select c2 from t2)) from t1",
			[]string{
				"TableScan_11   cop table:t1, range:(-inf,+inf), keep order:false 8000",
				"TableReader_12 HashSemiJoin_10  root data:TableScan_11 8000",
				"TableScan_13 Selection_14  cop table:t2, range:(-inf,+inf), keep order:false 10",
				"Selection_14  TableScan_13 cop eq(6, test.t2.c2) 10",
				"TableReader_15 HashSemiJoin_10  root data:Selection_14 10",
				"HashSemiJoin_10 HashAgg_8 TableReader_12,TableReader_15 root right:TableReader_15, aux 8000",
				"HashAgg_8  HashSemiJoin_10 root type:complete, funcs:sum(5_aux_0) 1",
			},
		},
	}
	tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 0")
	for _, tt := range insubqueryFoldTests {
		result := tk.MustQuery("explain " + tt.sql)
		result.Check(testkit.Rows(tt.expect...))
	}

	dotFormatTests := []struct {
		sql    string
		expect string
	}{
		{
			sql: "select sum(t1.c1 in (select c1 from t2)) from t1",
			expect: "\n" +
				"digraph StreamAgg_9 {\n" +
				"subgraph cluster9{\n" +
				"node [style=filled, color=lightgrey]\n" +
				"color=black\n" +
				"label = \"root\"\n" +
				"\"StreamAgg_9\" -> \"HashSemiJoin_16\"\n" +
				"\"HashSemiJoin_16\" -> \"TableReader_19\"\n" +
				"\"HashSemiJoin_16\" -> \"TableReader_15\"\n" +
				"}\n" +
				"subgraph cluster18{\n" +
				"node [style=filled, color=lightgrey]\n" +
				"color=black\n" +
				"label = \"cop\"\n" +
				"\"TableScan_18\"\n" +
				"}\n" +
				"subgraph cluster14{\n" +
				"node [style=filled, color=lightgrey]\n" +
				"color=black\n" +
				"label = \"cop\"\n" +
				"\"TableScan_14\"\n" +
				"}\n" +
				"\"TableReader_19\" -> \"TableScan_18\"\n" +
				"\"TableReader_15\" -> \"TableScan_14\"\n" +
				"}\n",
		},
		{
			sql: "select 1 in (select c2 from t2) from t1",
			expect: "\n" +
				"digraph Projection_2 {\n" +
				"subgraph cluster2{\n" +
				"node [style=filled, color=lightgrey]\n" +
				"color=black\n" +
				"label = \"root\"\n" +
				"\"Projection_2\" -> \"HashSemiJoin_7\"\n" +
				"\"HashSemiJoin_7\" -> \"TableReader_9\"\n" +
				"\"HashSemiJoin_7\" -> \"TableReader_12\"\n" +
				"}\n" +
				"subgraph cluster8{\n" +
				"node [style=filled, color=lightgrey]\n" +
				"color=black\n" +
				"label = \"cop\"\n" +
				"\"TableScan_8\"\n" +
				"}\n" +
				"subgraph cluster11{\n" +
				"node [style=filled, color=lightgrey]\n" +
				"color=black\n" +
				"label = \"cop\"\n" +
				"\"Selection_11\" -> \"TableScan_10\"\n" +
				"}\n" +
				"\"TableReader_9\" -> \"TableScan_8\"\n" +
				"\"TableReader_12\" -> \"Selection_11\"\n" +
				"}\n",
		},
	}
	for i, length := 0, len(dotFormatTests); i < length; i++ {
		result := tk.MustQuery(`explain format = "dot" ` + dotFormatTests[i].sql)
		result.Check(testkit.Rows(dotFormatTests[i].expect))
	}
}
