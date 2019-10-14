// Copyright 2018 PingCAP, Inc.
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

package expression_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
	dom   *domain.Domain
	ctx   sessionctx.Context
}

func (s *testSuite) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuite) SetUpSuite(c *C) {
	var err error
	testleak.BeforeTest()
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
}

func (s *testSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testSuite) TestOuterJoinPropConst(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(id bigint primary key, a int, b int);")
	tk.MustExec("create table t2(id bigint primary key, a int, b int);")

	// Positive tests.
	tk.MustQuery("explain select * from t1 left join t2 on t1.a > t2.a and t1.a = 1;").Check(testkit.Rows(
		"HashLeftJoin_6 33233333.33 root CARTESIAN left outer join, inner:TableReader_11, left cond:[eq(Column#2, 1)]",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 3323.33 root data:Selection_10",
		"  └─Selection_10 3323.33 cop[tikv] gt(1, Column#5)",
		"    └─TableScan_9 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t1.a > t2.a where t1.a = 1;").Check(testkit.Rows(
		"HashLeftJoin_7 33233.33 root CARTESIAN left outer join, inner:TableReader_13",
		"├─TableReader_10 10.00 root data:Selection_9",
		"│ └─Selection_9 10.00 cop[tikv] eq(Column#2, 1)",
		"│   └─TableScan_8 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_13 3323.33 root data:Selection_12",
		"  └─Selection_12 3323.33 cop[tikv] gt(1, Column#5)",
		"    └─TableScan_11 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t1.a = t2.a and t1.a > 1;").Check(testkit.Rows(
		"HashLeftJoin_6 10000.00 root left outer join, inner:TableReader_11, equal:[eq(Column#2, Column#5)], left cond:[gt(Column#2, 1)]",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 3333.33 root data:Selection_10",
		"  └─Selection_10 3333.33 cop[tikv] gt(Column#5, 1), not(isnull(Column#5))",
		"    └─TableScan_9 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t1.a = t2.a where t1.a > 1;").Check(testkit.Rows(
		"HashLeftJoin_7 4166.67 root left outer join, inner:TableReader_13, equal:[eq(Column#2, Column#5)]",
		"├─TableReader_10 3333.33 root data:Selection_9",
		"│ └─Selection_9 3333.33 cop[tikv] gt(Column#2, 1)",
		"│   └─TableScan_8 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_13 3333.33 root data:Selection_12",
		"  └─Selection_12 3333.33 cop[tikv] gt(Column#5, 1), not(isnull(Column#5))",
		"    └─TableScan_11 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 right join t2 on t1.a > t2.a where t2.a = 1;").Check(testkit.Rows(
		"HashRightJoin_7 33333.33 root CARTESIAN right outer join, inner:TableReader_10",
		"├─TableReader_10 3333.33 root data:Selection_9",
		"│ └─Selection_9 3333.33 cop[tikv] gt(Column#2, 1)",
		"│   └─TableScan_8 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_13 10.00 root data:Selection_12",
		"  └─Selection_12 10.00 cop[tikv] eq(Column#5, 1)",
		"    └─TableScan_11 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 right join t2 on t1.a = t2.a where t2.a > 1;").Check(testkit.Rows(
		"HashRightJoin_7 4166.67 root right outer join, inner:TableReader_10, equal:[eq(Column#2, Column#5)]",
		"├─TableReader_10 3333.33 root data:Selection_9",
		"│ └─Selection_9 3333.33 cop[tikv] gt(Column#2, 1), not(isnull(Column#2))",
		"│   └─TableScan_8 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_13 3333.33 root data:Selection_12",
		"  └─Selection_12 3333.33 cop[tikv] gt(Column#5, 1)",
		"    └─TableScan_11 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 right join t2 on t1.a = t2.a and t2.a > 1;").Check(testkit.Rows(
		"HashRightJoin_6 10000.00 root right outer join, inner:TableReader_9, equal:[eq(Column#2, Column#5)], right cond:gt(Column#5, 1)",
		"├─TableReader_9 3333.33 root data:Selection_8",
		"│ └─Selection_8 3333.33 cop[tikv] gt(Column#2, 1), not(isnull(Column#2))",
		"│   └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 10000.00 root data:TableScan_10",
		"  └─TableScan_10 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 right join t2 on t1.a > t2.a and t2.a = 1;").Check(testkit.Rows(
		"HashRightJoin_6 33333333.33 root CARTESIAN right outer join, inner:TableReader_9, right cond:eq(Column#5, 1)",
		"├─TableReader_9 3333.33 root data:Selection_8",
		"│ └─Selection_8 3333.33 cop[tikv] gt(Column#2, 1)",
		"│   └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 10000.00 root data:TableScan_10",
		"  └─TableScan_10 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	// Negative tests.
	tk.MustQuery("explain select * from t1 left join t2 on t1.a = t2.a and t2.a > 1;").Check(testkit.Rows(
		"HashLeftJoin_6 10000.00 root left outer join, inner:TableReader_11, equal:[eq(Column#2, Column#5)]",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 3333.33 root data:Selection_10",
		"  └─Selection_10 3333.33 cop[tikv] gt(Column#5, 1), not(isnull(Column#5))",
		"    └─TableScan_9 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t1.a > t2.a and t2.a = 1;").Check(testkit.Rows(
		"HashLeftJoin_6 100000.00 root CARTESIAN left outer join, inner:TableReader_11, other cond:gt(Column#2, Column#5)",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 10.00 root data:Selection_10",
		"  └─Selection_10 10.00 cop[tikv] eq(Column#5, 1), not(isnull(Column#5))",
		"    └─TableScan_9 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 right join t2 on t1.a > t2.a and t1.a = 1;").Check(testkit.Rows(
		"HashRightJoin_6 100000.00 root CARTESIAN right outer join, inner:TableReader_9, other cond:gt(Column#2, Column#5)",
		"├─TableReader_9 10.00 root data:Selection_8",
		"│ └─Selection_8 10.00 cop[tikv] eq(Column#2, 1), not(isnull(Column#2))",
		"│   └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 10000.00 root data:TableScan_10",
		"  └─TableScan_10 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 right join t2 on t1.a = t2.a and t1.a > 1;").Check(testkit.Rows(
		"HashRightJoin_6 10000.00 root right outer join, inner:TableReader_9, equal:[eq(Column#2, Column#5)]",
		"├─TableReader_9 3333.33 root data:Selection_8",
		"│ └─Selection_8 3333.33 cop[tikv] gt(Column#2, 1), not(isnull(Column#2))",
		"│   └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 10000.00 root data:TableScan_10",
		"  └─TableScan_10 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t1.a = t1.b and t1.a > 1;").Check(testkit.Rows(
		"HashLeftJoin_6 100000000.00 root CARTESIAN left outer join, inner:TableReader_10, left cond:[eq(Column#2, Column#3) gt(Column#2, 1)]",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_10 10000.00 root data:TableScan_9",
		"  └─TableScan_9 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t2.a = t2.b and t2.a > 1;").Check(testkit.Rows(
		"HashLeftJoin_6 26666666.67 root CARTESIAN left outer join, inner:TableReader_11",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 2666.67 root data:Selection_10",
		"  └─Selection_10 2666.67 cop[tikv] eq(Column#5, Column#6), gt(Column#5, 1)",
		"    └─TableScan_9 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	// Constant equal condition merge in outer join.
	tk.MustQuery("explain select * from t1 left join t2 on true where t1.a = 1 and false;").Check(testkit.Rows(
		"TableDual_8 0.00 root rows:0",
	))
	tk.MustQuery("explain select * from t1 left join t2 on true where t1.a = 1 and null;").Check(testkit.Rows(
		"TableDual_8 0.00 root rows:0",
	))
	tk.MustQuery("explain select * from t1 left join t2 on true where t1.a = null;").Check(testkit.Rows(
		"TableDual_8 0.00 root rows:0",
	))
	tk.MustQuery("explain select * from t1 left join t2 on true where t1.a = 1 and t1.a = 2;").Check(testkit.Rows(
		"TableDual_8 0.00 root rows:0",
	))
	tk.MustQuery("explain select * from t1 left join t2 on true where t1.a = 1 and t1.a = 1;").Check(testkit.Rows(
		"HashLeftJoin_7 80000.00 root CARTESIAN left outer join, inner:TableReader_12",
		"├─TableReader_10 10.00 root data:Selection_9",
		"│ └─Selection_9 10.00 cop[tikv] eq(Column#2, 1)",
		"│   └─TableScan_8 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_12 10000.00 root data:TableScan_11",
		"  └─TableScan_11 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on false;").Check(testkit.Rows(
		"HashLeftJoin_6 80000000.00 root CARTESIAN left outer join, inner:TableDual_9",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableDual_9 8000.00 root rows:0",
	))
	tk.MustQuery("explain select * from t1 right join t2 on false;").Check(testkit.Rows(
		"HashRightJoin_6 80000000.00 root CARTESIAN right outer join, inner:TableDual_7",
		"├─TableDual_7 8000.00 root rows:0",
		"└─TableReader_9 10000.00 root data:TableScan_8",
		"  └─TableScan_8 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t1.a = 1 and t1.a = 2;").Check(testkit.Rows(
		"HashLeftJoin_6 80000000.00 root CARTESIAN left outer join, inner:TableDual_9",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableDual_9 8000.00 root rows:0",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t1.a =1 where t1.a = 2;").Check(testkit.Rows(
		"HashLeftJoin_7 80000.00 root CARTESIAN left outer join, inner:TableDual_11",
		"├─TableReader_10 10.00 root data:Selection_9",
		"│ └─Selection_9 10.00 cop[tikv] eq(Column#2, 2)",
		"│   └─TableScan_8 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableDual_11 8000.00 root rows:0",
	))
	tk.MustQuery("explain select * from t1 left join t2 on t2.a = 1 and t2.a = 2;").Check(testkit.Rows(
		"HashLeftJoin_6 10000.00 root CARTESIAN left outer join, inner:TableReader_11",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_11 0.00 root data:Selection_10",
		"  └─Selection_10 0.00 cop[tikv] eq(Column#5, 1), eq(Column#5, 2)",
		"    └─TableScan_9 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	// Constant propagation for DNF in outer join.
	tk.MustQuery("explain select * from t1 left join t2 on t1.a = 1 or (t1.a = 2 and t1.a = 3);").Check(testkit.Rows(
		"HashLeftJoin_6 100000000.00 root CARTESIAN left outer join, inner:TableReader_10, left cond:[or(eq(Column#2, 1), 0)]",
		"├─TableReader_8 10000.00 root data:TableScan_7",
		"│ └─TableScan_7 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_10 10000.00 root data:TableScan_9",
		"  └─TableScan_9 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("explain select * from t1 left join t2 on true where t1.a = 1 or (t1.a = 2 and t1.a = 3);").Check(testkit.Rows(
		"HashLeftJoin_7 80000.00 root CARTESIAN left outer join, inner:TableReader_12",
		"├─TableReader_10 10.00 root data:Selection_9",
		"│ └─Selection_9 10.00 cop[tikv] or(eq(Column#2, 1), 0)",
		"│   └─TableScan_8 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_12 10000.00 root data:TableScan_11",
		"  └─TableScan_11 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	// Constant propagation over left outer semi join, filter with aux column should not be derived.
	tk.MustQuery("explain select * from t1 where t1.b > 1 or t1.b in (select b from t2);").Check(testkit.Rows(
		"Projection_7 8000.00 root Column#1, Column#2, Column#3",
		"└─Selection_8 8000.00 root or(gt(Column#3, 1), Column#8)",
		"  └─HashLeftJoin_9 10000.00 root CARTESIAN left outer semi join, inner:TableReader_13, other cond:eq(Column#3, Column#6)",
		"    ├─TableReader_11 10000.00 root data:TableScan_10",
		"    │ └─TableScan_10 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"    └─TableReader_13 10000.00 root data:TableScan_12",
		"      └─TableScan_12 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
}
