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
				"TableScan_15  cop table:t3, range:(-inf,+inf), keep order:false",
				"TableReader_16 Projection_12 root data:TableScan_15",
				"Projection_12 HashSemiJoin_14 root test.t3.a, test.t3.b, test.t3.c, test.t3.d, cast(test.t3.a)",
				"TableScan_18 HashAgg_17 cop table:s, range:(-inf,+inf), keep order:false",
				"HashAgg_17  cop type:complete, funcs:sum(s.a)",
				"TableReader_20 HashAgg_19 root data:HashAgg_17",
				"HashAgg_19 HashSemiJoin_14 root type:final, funcs:sum(col_0)",
				"HashSemiJoin_14 Projection_11 root right:HashAgg_19, equal:[eq(cast(test.t3.a), sel_agg_1)]",
				"Projection_11  root test.t3.a, test.t3.b, test.t3.c, test.t3.d",
			},
		},
		{
			"select * from t1",
			[]string{
				"TableScan_3  cop table:t1, range:(-inf,+inf), keep order:false",
				"TableReader_4  root data:TableScan_3",
			},
		},
		{
			"select * from t1 order by c2",
			[]string{
				"IndexScan_13  cop table:t1, index:c2, range:[<nil>,+inf], out of order:false",
				"TableScan_14  cop table:t1, keep order:false",
				"IndexLookUp_15  root index:IndexScan_13, table:TableScan_14",
			},
		},
		{
			"select * from t2 order by c2",
			[]string{
				"TableScan_4  cop table:t2, range:(-inf,+inf), keep order:false",
				"TableReader_5 Sort_3 root data:TableScan_4",
				"Sort_3  root t2.c2:asc",
			},
		},
		{
			"select * from t1 where t1.c1 > 0",
			[]string{
				"TableScan_4  cop table:t1, range:[1,+inf), keep order:false",
				"TableReader_5  root data:TableScan_4",
			},
		},
		{
			"select t1.c1, t1.c2 from t1 where t1.c2 = 1",
			[]string{
				"IndexScan_7  cop table:t1, index:c2, range:[1,1], out of order:true",
				"IndexReader_8  root index:IndexScan_7",
			},
		},
		{
			"select * from t1 left join t2 on t1.c2 = t2.c1 where t1.c1 > 1",
			[]string{
				"TableScan_22  cop table:t1, range:[2,+inf), keep order:false",
				"TableReader_23 IndexJoin_7 root data:TableScan_22",
				"IndexScan_33  cop table:t2, index:c1, range:[<nil>,+inf], out of order:false",
				"TableScan_34  cop table:t2, keep order:false",
				"IndexLookUp_35 IndexJoin_7 root index:IndexScan_33, table:TableScan_34",
				"IndexJoin_7  root outer:TableReader_23, outer key:test.t1.c2, inner key:test.t2.c1",
			},
		},
		{
			"update t1 set t1.c2 = 2 where t1.c1 = 1",
			[]string{
				"TableScan_4  cop table:t1, range:[1,1], keep order:false",
				"TableReader_5 Update_3 root data:TableScan_4",
				"Update_3  root ",
			},
		},
		{
			"delete from t1 where t1.c2 = 1",
			[]string{
				"IndexScan_7  cop table:t1, index:c2, range:[1,1], out of order:true",
				"TableScan_8  cop table:t1, keep order:false",
				"IndexLookUp_9 Delete_3 root index:IndexScan_7, table:TableScan_8",
				"Delete_3  root ",
			},
		},
		{
			"select count(b.c2) from t1 a, t2 b where a.c1 = b.c2 group by a.c1",
			[]string{
				"TableScan_17 HashAgg_16 cop table:b, range:(-inf,+inf), keep order:false",
				"HashAgg_16  cop type:complete, group by:b.c2, funcs:count(b.c2), firstrow(b.c2)",
				"TableReader_21 HashAgg_20 root data:HashAgg_16",
				"HashAgg_20 IndexJoin_9 root type:final, group by:, funcs:count(col_0), firstrow(col_1)",
				"TableScan_12  cop table:a, range:(-inf,+inf), keep order:true",
				"TableReader_31 IndexJoin_9 root data:TableScan_12",
				"IndexJoin_9 Projection_8 root outer:TableReader_31, outer key:b.c2, inner key:a.c1",
				"Projection_8  root cast(join_agg_0)",
			},
		},
		{
			"select * from t2 order by t2.c2 limit 0, 1",
			[]string{
				"TableScan_7 TopN_5 cop table:t2, range:(-inf,+inf), keep order:false",
				"TopN_5  cop ",
				"TableReader_10 TopN_5 root data:TopN_5",
				"TopN_5  root ",
			},
		},
		{
			"select * from t1 where c1 > 1 and c2 = 1 and c3 < 1",
			[]string{
				"IndexScan_7 Selection_9 cop table:t1, index:c2, range:[1,1], out of order:true",
				"Selection_9  cop gt(test.t1.c1, 1)",
				"TableScan_8 Selection_10 cop table:t1, keep order:false",
				"Selection_10  cop lt(test.t1.c3, 1)",
				"IndexLookUp_11  root index:Selection_9, table:Selection_10",
			},
		},
		{
			"select * from t1 where c1 =1 and c2 > 1",
			[]string{
				"TableScan_4 Selection_5 cop table:t1, range:[1,1], keep order:false",
				"Selection_5  cop gt(test.t1.c2, 1)",
				"TableReader_6  root data:Selection_5",
			},
		},
		{
			"select sum(t1.c1 in (select c1 from t2)) from t1",
			[]string{
				"TableScan_11 HashAgg_10 cop table:t1, range:(-inf,+inf), keep order:false",
				"HashAgg_10  cop type:complete, funcs:sum(in(test.t1.c1, 1, 2))",
				"TableReader_14 HashAgg_13 root data:HashAgg_10",
				"HashAgg_13  root type:final, funcs:sum(col_0)",
			},
		},
		{
			"select c1 from t1 where c1 in (select c2 from t2)",
			[]string{
				"TableScan_11  cop table:t1, range:[0,0], [1,1], keep order:false",
				"TableReader_12  root data:TableScan_11",
			},
		},
		{
			"select (select count(1) k from t1 s where s.c1 = t1.c1 having k != 0) from t1",
			[]string{
				"TableScan_13  cop table:t1, range:(-inf,+inf), keep order:false",
				"TableReader_14 Apply_12 root data:TableScan_13",
				"TableScan_18  cop table:s, range:(-inf,+inf), keep order:false",
				"TableReader_19 Selection_4 root data:TableScan_18",
				"Selection_4 HashAgg_17 root eq(s.c1, test.t1.c1)",
				"HashAgg_17 Selection_10 root type:complete, funcs:count(1)",
				"Selection_10 Apply_12 root ne(k, 0)",
				"Apply_12 Projection_2 root left outer join, small:Selection_10, right:Selection_10",
				"Projection_2  root k",
			},
		},
		{
			"select * from information_schema.columns",
			[]string{
				"MemTableScan_3  root ",
			},
		},
	}
	tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 1")
	for _, tt := range tests {
		result := tk.MustQuery("explain " + tt.sql)
		result.Check(testkit.Rows(tt.expect...))
	}
}
