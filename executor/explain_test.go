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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestExplain(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
		tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 0")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index c2 (c2))")
	tk.MustExec("create table t2 (c1 int unique, c2 int)")
	tk.MustExec("insert into t2 values(1, 0), (2, 1)")

	tests := []struct {
		sql    string
		expect []string
	}{
		{
			"select * from t1",
			[]string{
				"TableScan_3  cop ",
				"TableReader_4  root ",
			},
		},
		{
			"select * from t1 order by c2",
			[]string{
				"IndexScan_13  cop ",
				"TableScan_14  cop ",
				"IndexLookUp_15  root ",
			},
		},
		{
			"select * from t2 order by c2",
			[]string{
				"TableScan_4  cop ",
				"TableReader_5 Sort_3 root ",
				"Sort_3  root t2.c2:asc",
			},
		},
		{
			"select * from t1 where t1.c1 > 0",
			[]string{
				"TableScan_4  cop ",
				"TableReader_5  root ",
			},
		},
		{
			"select t1.c1, t1.c2 from t1 where t1.c2 = 1",
			[]string{
				"IndexScan_7  cop ",
				"IndexReader_8  root ",
			},
		},
		{
			"select * from t1 left join t2 on t1.c2 = t2.c1 where t1.c1 > 1",
			[]string{
				"TableScan_22  cop ",
				"TableReader_23 IndexJoin_7 root ",
				"IndexScan_33  cop ",
				"TableScan_34  cop ",
				"IndexLookUp_35 IndexJoin_7 root ",
				"IndexJoin_7  root outer:TableReader_23, outer key:test.t1.c2, inner key:test.t2.c1",
			},
		},
		{
			"update t1 set t1.c2 = 2 where t1.c1 = 1",
			[]string{
				"TableScan_4  cop ",
				"TableReader_5 Update_3 root ",
				"Update_3  root ",
			},
		},
		{
			"delete from t1 where t1.c2 = 1",
			[]string{
				"IndexScan_7  cop ",
				"TableScan_8  cop ",
				"IndexLookUp_9 Delete_3 root ",
				"Delete_3  root ",
			},
		},
		{
			"select count(b.c2) from t1 a, t2 b where a.c1 = b.c2 group by a.c1",
			[]string{
				"TableScan_17 HashAgg_16 cop ",
				"HashAgg_16  cop type:complete, group by:b.c2, funcs:count(b.c2), firstrow(b.c2)",
				"TableReader_21 HashAgg_20 root ",
				"HashAgg_20 IndexJoin_9 root type:final, group by:, funcs:count(col_0), firstrow(col_1)",
				"TableScan_12  cop ",
				"TableReader_31 IndexJoin_9 root ",
				"IndexJoin_9 Projection_8 root outer:TableReader_31, outer key:b.c2, inner key:a.c1",
				"Projection_8  root cast(join_agg_0)",
			},
		},
		{
			"select * from t2 order by t2.c2 limit 0, 1",
			[]string{
				"TableScan_7 TopN_5 cop ",
				"TopN_5  cop ",
				"TableReader_10 TopN_5 root ",
				"TopN_5  root ",
			},
		},
		{
			"select * from t1 where c1 > 1 and c2 = 1 and c3 < 1",
			[]string{
				"IndexScan_7 Selection_9 cop ",
				"Selection_9  cop gt(test.t1.c1, 1)",
				"TableScan_8 Selection_10 cop ",
				"Selection_10  cop lt(test.t1.c3, 1)",
				"IndexLookUp_11  root ",
			},
		},
		{
			"select * from t1 where c1 =1 and c2 > 1",
			[]string{
				"TableScan_4 Selection_5 cop ",
				"Selection_5  cop gt(test.t1.c2, 1)",
				"TableReader_6  root ",
			},
		},
		{
			"select sum(t1.c1 in (select c1 from t2)) from t1",
			[]string{
				"TableScan_11 HashAgg_10 cop ",
				"HashAgg_10  cop type:complete, funcs:sum(in(test.t1.c1, 1, 2))",
				"TableReader_14 HashAgg_13 root ",
				"HashAgg_13  root type:final, funcs:sum(col_0)",
			},
		},
		{
			"select c1 from t1 where c1 in (select c2 from t2)",
			[]string{
				"TableScan_11  cop ",
				"TableReader_12  root ",
			},
		},
		{
			"select (select count(1) k from t1 s where s.c1 = t1.c1 having k != 0) from t1",
			[]string{
				"TableScan_13  cop ",
				"TableReader_14 Apply_12 root ",
				"TableScan_18  cop ",
				"TableReader_19 Selection_4 root ",
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
