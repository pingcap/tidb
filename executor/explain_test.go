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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, index c2 (c2))")
	tk.MustExec("create table t2 (c1 int unique, c2 int)")

	cases := []struct {
		sql    string
		result string
	}{
		{
			"select * from t1",
			`{
    "type": "TableScan",
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "filter conditions": null
    }
}`,
		},
		{
			"select * from t1 order by c2",
			`{
    "type": "IndexScan",
    "db": "test",
    "table": "t1",
    "index": "c2",
    "ranges": "[[\u003cnil\u003e,+inf]]",
    "desc": false,
    "out of order": false,
    "double read": false,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "filter conditions": null
    }
}`,
		},
		{
			"select * from t2 order by c2",
			`{
    "type": "Sort",
    "exprs": [
        {
            "Expr": "t2.c2",
            "Desc": false
        }
    ],
    "limit": null,
    "child": {
        "type": "TableScan",
        "db": "test",
        "table": "t2",
        "desc": false,
        "keep order": false,
        "push down info": {
            "limit": 0,
            "access conditions": null,
            "filter conditions": null
        }
    }
}`,
		},
		{
			"select * from t1 where t1.c1 > 0",
			`{
    "type": "TableScan",
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": false,
    "push down info": {
        "limit": 0,
        "access conditions": [
            "gt(test.t1.c1, 0)"
        ],
        "filter conditions": null
    }
}`,
		},
		{
			"select * from t1 where t1.c2 = 1",
			`{
    "type": "IndexScan",
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
        "filter conditions": null
    }
}`,
		},
		{
			"select * from t1 left join t2 on t1.c2 = t2.c1 where t1.c1 > 1",
			`{
    "type": "LeftJoin",
    "eqCond": [
        "eq(test.t1.c2, test.t2.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": null,
    "leftPlan": {
        "type": "TableScan",
        "db": "test",
        "table": "t1",
        "desc": false,
        "keep order": false,
        "push down info": {
            "limit": 0,
            "access conditions": [
                "gt(test.t1.c1, 1)"
            ],
            "filter conditions": null
        }
    },
    "rightPlan": {
        "type": "TableScan",
        "db": "test",
        "table": "t2",
        "desc": false,
        "keep order": false,
        "push down info": {
            "limit": 0,
            "access conditions": null,
            "filter conditions": null
        }
    }
}`,
		},
		{
			"update t1 set t1.c2 = 2 where t1.c1 = 1",
			`{
    "id": "Update_3",
    "children": [
        {
            "type": "TableScan",
            "db": "test",
            "table": "t1",
            "desc": false,
            "keep order": false,
            "push down info": {
                "limit": 0,
                "access conditions": [
                    "eq(test.t1.c1, 1)"
                ],
                "filter conditions": null
            }
        }
    ]
}`,
		},
		{
			"delete from t1 where t1.c2 = 1",
			`{
    "id": "Delete_3",
    "children": [
        {
            "type": "IndexScan",
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
                "filter conditions": null
            }
        }
    ]
}`,
		},
		{
			"select count(b.c2) from t1 a, t2 b where a.c1 = b.c2 group by a.c1",
			`{
    "type": "CompleteAgg",
    "AggFuncs": [
        "count(join_agg_0)"
    ],
    "GroupByItems": [
        "a.c1"
    ],
    "child": {
        "type": "InnerJoin",
        "eqCond": [
            "eq(a.c1, b.c2)"
        ],
        "leftCond": null,
        "rightCond": null,
        "otherCond": null,
        "leftPlan": {
            "type": "TableScan",
            "db": "test",
            "table": "t1",
            "desc": false,
            "keep order": false,
            "push down info": {
                "limit": 0,
                "access conditions": null,
                "filter conditions": null
            }
        },
        "rightPlan": {
            "type": "FinalAgg",
            "AggFuncs": [
                "count([b.c2])",
                "firstrow([b.c2])"
            ],
            "GroupByItems": [
                "[b.c2]"
            ],
            "child": {
                "type": "TableScan",
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
                    "filter conditions": null
                }
            }
        }
    }
}`,
		},
	}
	for _, ca := range cases {
		result := tk.MustQuery("explain " + ca.sql)
		result.Check(testkit.Rows("EXPLAIN " + ca.result))
	}
}
