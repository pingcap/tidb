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
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		testleak.AfterTest(c)()
		tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 0")
	}()
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
				"Sort_3  TableReader_5 root t2.c2:asc 8000",
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
				"TableScan_22   cop table:t1, range:[2,+inf), keep order:false 3333.333333333333",
				"TableReader_23 HashLeftJoin_8  root data:TableScan_22 3333.333333333333",
				"TableScan_37   cop table:t2, range:(-inf,+inf), keep order:false 8000",
				"TableReader_38 HashLeftJoin_8  root data:TableScan_37 8000",
				"HashLeftJoin_8  TableReader_23,TableReader_38 root left outer join, small:TableReader_38, equal:[eq(test.t1.c2, test.t2.c1)] 4166.666666666666",
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
				"TableScan_25   cop table:a, range:(-inf,+inf), keep order:false 8000",
				"TableReader_26 HashLeftJoin_10  root data:TableScan_25 8000",
				"TableScan_17 HashAgg_16  cop table:b, range:(-inf,+inf), keep order:false 8000",
				"HashAgg_16  TableScan_17 cop type:complete, group by:b.c2, funcs:count(b.c2), firstrow(b.c2) 6400",
				"TableReader_19 HashAgg_18  root data:HashAgg_16 6400",
				"HashAgg_18 HashLeftJoin_10 TableReader_19 root type:final, group by:, funcs:count(col_0), firstrow(col_1) 6400",
				"HashLeftJoin_10 Projection_8 TableReader_26,HashAgg_18 root inner join, small:HashAgg_18, equal:[eq(a.c1, b.c2)] 8000",
				"Projection_8  HashLeftJoin_10 root cast(join_agg_0) 8000",
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
			"select * from t1 where c1 =1 and c2 > 1",
			[]string{
				"TableScan_4 Selection_5  cop table:t1, range:[1,1], keep order:false 10",
				"Selection_5  TableScan_4 cop gt(test.t1.c2, 1) 10",
				"TableReader_6   root data:Selection_5 10",
			},
		},
		{
			"select sum(t1.c1 in (select c1 from t2)) from t1",
			[]string{
				"TableScan_9 HashAgg_8  cop table:t1, range:(-inf,+inf), keep order:false 8000",
				"HashAgg_8  TableScan_9 cop type:complete, funcs:sum(in(test.t1.c1, 1, 2)) 1",
				"TableReader_11 HashAgg_10  root data:HashAgg_8 1",
				"HashAgg_10  TableReader_11 root type:final, funcs:sum(col_0) 1",
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
				"TableScan_16   cop table:s, range:(-inf,+inf), keep order:false 8000",
				"TableReader_17 Selection_4  root data:TableScan_16 8000",
				"Selection_4 HashAgg_15 TableReader_17 root eq(s.c1, test.t1.c1) 6400",
				"HashAgg_15 Selection_10 Selection_4 root type:complete, funcs:count(1) 1",
				"Selection_10 Apply_12 HashAgg_15 root ne(k, 0) 0.8",
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
				"Selection_4 Limit_16 IndexLookUp_25 root eq(test.t1.c1, test.t2.c1) 6400",
				"Limit_16 MaxOneRow_9 Selection_4 root offset:0, count:1 1",
				"MaxOneRow_9 Apply_13 Limit_16 root  1",
				"Apply_13 Projection_2 TableReader_15,MaxOneRow_9 root left outer join, small:MaxOneRow_9, right:MaxOneRow_9 8000",
				"Projection_2  Apply_13 root eq(test.t1.c2, test.t2.c2) 8000",
			},
		},
	}
	tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 1")
	for _, tt := range tests {
		result := tk.MustQuery("explain " + tt.sql)
		result.Check(testkit.Rows(tt.expect...))
	}
}
