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
		sql       string
		ids       []string
		parentIds []string
		result    []string
	}{
		{
			"select * from t1",
			[]string{
				"TableScan_3",
			},
			[]string{
				"",
			},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
			},
		},
		{
			"select * from t1 order by c2",
			[]string{
				"IndexScan_5",
			},
			[]string{
				"",
			},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "index": "c2",
    "ranges": "[[\u003cnil\u003e,+inf]]",
    "desc": false,
    "out of order": false,
    "double read": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
			},
		},
		{
			"select * from t2 order by c2",
			[]string{
				"TableScan_6", "Sort_3",
			},
			[]string{
				"Sort_3", "",
			},
			[]string{
				`{
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "exprs": [
        {
            "Expr": "t2.c2",
            "Desc": false
        }
    ],
    "limit": null,
    "child": "TableScan_6"
}`,
			},
		},
		{
			"select * from t1 where t1.c1 > 0",
			[]string{
				"TableScan_4",
			},
			[]string{
				"",
			},
			[]string{`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "gt(test.t1.c1, 0)"
        ],
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
			},
		},
		{
			"select t1.c1, t1.c2 from t1 where t1.c2 = 1",
			[]string{
				"IndexScan_5",
			},
			[]string{
				"",
			},
			[]string{`{
    "db": "test",
    "table": "t1",
    "index": "c2",
    "ranges": "[[1,1]]",
    "desc": false,
    "out of order": true,
    "double read": false,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "eq(test.t1.c2, 1)"
        ],
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
			},
		},
		{
			"select * from t1 left join t2 on t1.c2 = t2.c1 where t1.c1 > 1",
			[]string{
				"TableScan_7", "TableScan_10", "HashLeftJoin_9",
			},
			[]string{
				"HashLeftJoin_9", "HashLeftJoin_9", "",
			},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "gt(test.t1.c1, 1)"
        ],
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "eqCond": [
        "eq(test.t1.c2, test.t2.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": null,
    "leftPlan": "TableScan_7",
    "rightPlan": "TableScan_10"
}`,
			},
		},
		{
			"update t1 set t1.c2 = 2 where t1.c1 = 1",
			[]string{
				"TableScan_4", "Update_3",
			},
			[]string{
				"Update_3", "",
			},
			[]string{`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "eq(test.t1.c1, 1)"
        ],
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "children": [
        "TableScan_4"
    ]
}`,
			},
		},
		{
			"delete from t1 where t1.c2 = 1",
			[]string{
				"IndexScan_5", "Delete_3",
			},
			[]string{
				"Delete_3", "",
			},
			[]string{`{
    "db": "test",
    "table": "t1",
    "index": "c2",
    "ranges": "[[1,1]]",
    "desc": false,
    "out of order": true,
    "double read": true,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "eq(test.t1.c2, 1)"
        ],
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "children": [
        "IndexScan_5"
    ]
}`,
			},
		},
		{
			"select count(b.c2) from t1 a, t2 b where a.c1 = b.c2 group by a.c1",
			[]string{
				"TableScan_16", "TableScan_10", "HashAgg_11", "HashLeftJoin_15", "Projection_9",
			},
			[]string{
				"HashLeftJoin_15", "HashAgg_11", "HashLeftJoin_15", "Projection_9", "",
			},
			[]string{`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "aggregated push down": true,
        "gby items": [
            "b.c2"
        ],
        "agg funcs": [
            "count(b.c2)",
            "firstrow(b.c2)"
        ],
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "AggFuncs": [
        "count([b.c2])",
        "firstrow([b.c2])"
    ],
    "GroupByItems": [
        "[b.c2]"
    ],
    "child": "TableScan_10"
}`,
				`{
    "eqCond": [
        "eq(a.c1, b.c2)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": null,
    "leftPlan": "TableScan_16",
    "rightPlan": "HashAgg_11"
}`,
				`{
    "exprs": [
        "cast(join_agg_0)"
    ],
    "child": "HashLeftJoin_15"
}`,
			},
		},
		{
			"select * from t2 order by t2.c2 limit 0, 1",
			[]string{
				"TableScan_5", "Sort_6",
			},
			[]string{
				"Sort_6", "",
			},
			[]string{
				`{
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 1,
        "sort items": [
            {
                "Expr": "test.t2.c2",
                "Desc": false
            }
        ],
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "exprs": [
        {
            "Expr": "test.t2.c2",
            "Desc": false
        }
    ],
    "limit": 1,
    "child": "TableScan_5"
}`,
			},
		},
		{
			"select * from t1 where c1 > 1 and c2 = 1 and c3 < 1",
			[]string{
				"IndexScan_5",
			},
			[]string{
				"",
			},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "index": "c2",
    "ranges": "[[1,1]]",
    "desc": false,
    "out of order": true,
    "double read": true,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "eq(test.t1.c2, 1)"
        ],
        "index filter conditions": [
            "gt(test.t1.c1, 1)"
        ],
        "table filter conditions": [
            "lt(test.t1.c3, 1)"
        ]
    }
}`,
			},
		},
		{
			"select * from t1 where c1 =1 and c2 > 1",
			[]string{
				"TableScan_4",
			},
			[]string{
				"",
			},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "eq(test.t1.c1, 1)"
        ],
        "index filter conditions": null,
        "table filter conditions": [
            "gt(test.t1.c2, 1)"
        ]
    }
}`,
			},
		},
		{
			"select sum(t1.c1 in (select c1 from t2)) from t1",
			[]string{"TableScan_7", "HashAgg_8"},
			[]string{"HashAgg_8", ""},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "aggregated push down": true,
        "gby items": null,
        "agg funcs": [
            "sum(in(test.t1.c1, 1, 2))"
        ],
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "AggFuncs": [
        "sum([in(test.t1.c1, 1, 2)])"
    ],
    "GroupByItems": [
        "[]"
    ],
    "child": "TableScan_7"
}`,
			},
		},
		{
			"select sum(t1.c1 in (select c1 from t2 where false)) from t1",
			[]string{"TableScan_8", "HashAgg_9"},
			[]string{"HashAgg_9", ""},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "aggregated push down": true,
        "gby items": null,
        "agg funcs": [
            "sum(0)"
        ],
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "AggFuncs": [
        "sum([0])"
    ],
    "GroupByItems": [
        "[]"
    ],
    "child": "TableScan_8"
}`,
			},
		},
		{
			"select c1 from t1 where c1 in (select c2 from t2)",
			[]string{"TableScan_7"},
			[]string{""},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "in(test.t1.c1, 0, 1)"
        ],
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
			},
		},
		{
			"select (select count(1) k from t1 s where s.c1 = t1.c1 having k != 0) from t1",
			[]string{
				"TableScan_12", "TableScan_13", "Selection_4", "StreamAgg_15", "Selection_10", "Apply_16", "Projection_2",
			},
			[]string{
				"Apply_16", "Selection_4", "StreamAgg_15", "Selection_10", "Apply_16", "Projection_2", "",
			},
			[]string{
				`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "condition": [
        "eq(s.c1, test.t1.c1)"
    ],
    "scanController": true,
    "child": "TableScan_13"
}`,
				`{
    "AggFuncs": [
        "count(1)"
    ],
    "GroupByItems": null,
    "child": "Selection_4"
}`,
				`{
    "condition": [
        "ne(aggregation_5_col_0, 0)"
    ],
    "scanController": false,
    "child": "StreamAgg_15"
}`,
				`{
    "innerPlan": "Selection_10",
    "outerPlan": "TableScan_12",
    "join": {
        "eqCond": null,
        "leftCond": null,
        "rightCond": null,
        "otherCond": null,
        "leftPlan": "TableScan_12",
        "rightPlan": "Selection_10"
    }
}`,
				`{
    "exprs": [
        "k"
    ],
    "child": "Apply_16"
}`,
			},
		},
		{
			"select * from information_schema.columns",
			[]string{"MemTableScan_3"},
			[]string{""},
			[]string{
				`{
    "db": "information_schema",
    "table": "COLUMNS"
}`,
			},
		},
		{
			"select s.c1 from t2 s left outer join t2 t on s.c2 = t.c2 limit 10",
			[]string{"TableScan_6", "Limit_7", "TableScan_10", "HashLeftJoin_9", "Limit_11", "Projection_4"},
			[]string{"Limit_7", "HashLeftJoin_9", "HashLeftJoin_9", "Limit_11", "Projection_4", ""},
			[]string{
				`{
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 10,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "limit": 10,
    "offset": 0,
    "child": "TableScan_6"
}`,
				`{
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
}`,
				`{
    "eqCond": [
        "eq(s.c2, t.c2)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": null,
    "leftPlan": "Limit_7",
    "rightPlan": "TableScan_10"
}`,
				`{
    "limit": 10,
    "offset": 0,
    "child": "HashLeftJoin_9"
}`,
				`{
    "exprs": [
        "s.c1"
    ],
    "child": "Limit_11"
}`,
			},
		},
	}
	tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 1")
	for _, tt := range tests {
		result := tk.MustQuery("explain " + tt.sql)
		var resultList []string
		for i := range tt.ids {
			resultList = append(resultList, tt.ids[i]+" "+tt.result[i]+" "+tt.parentIds[i])
		}
		result.Check(testkit.Rows(resultList...))
	}
}
