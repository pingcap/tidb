// Copyright 2025 PingCAP, Inc.
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

package simplify

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIsTruthXXXWithCast(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	// https://github.com/pingcap/tidb/issues/61062
	tk.MustExec(`CREATE TABLE t0(c0 FLOAT UNSIGNED);`)
	tk.MustExec(`CREATE TABLE t1 LIKE t0;`)
	tk.MustExec(`INSERT IGNORE  INTO t0 VALUES (0.5);`)
	tk.MustExec(`INSERT IGNORE  INTO t1 VALUES (NULL);`)
	tk.MustQuery(`SELECT t0.c0, t1.c0 FROM t0 INNER JOIN t1 ON true WHERE (NOT (CAST(t0.c0 AS DATETIME)));`).
		Check(testkit.Rows())
	tk.MustQuery(`explain format=brief SELECT t0.c0, t1.c0 FROM t0 INNER JOIN t1 ON true WHERE (NOT (CAST(t0.c0 AS DATETIME)));`).
		Check(testkit.Rows("HashJoin 100000.00 root  CARTESIAN inner join",
			"├─TableReader(Build) 10.00 root  data:Selection",
			"│ └─Selection 10.00 cop[tikv]  not(istrue_with_null(test.t0.c0))",
			"│   └─TableFullScan 10000.00 cop[tikv] table:t0 keep order:false, stats:pseudo",
			"└─TableReader(Probe) 10000.00 root  data:TableFullScan",
			"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery(`SELECT t0.c0, t1.c0 FROM t0 LEFT JOIN t1 ON true WHERE (NOT (CAST(t0.c0 AS DATETIME))) 
INTERSECT 
SELECT t0.c0, t1.c0 FROM t0 RIGHT JOIN t1 ON true WHERE (NOT (CAST(t0.c0 AS DATETIME)));`).Check(testkit.Rows())
	tk.MustQuery(`explain format=brief SELECT t0.c0, t1.c0 FROM t0 LEFT JOIN t1 ON true WHERE (NOT (CAST(t0.c0 AS DATETIME))) 
INTERSECT 
SELECT t0.c0, t1.c0 FROM t0 RIGHT JOIN t1 ON true WHERE (NOT (CAST(t0.c0 AS DATETIME)));`).Check(testkit.Rows(
		"HashJoin 6400.00 root  semi join, left side:HashAgg, equal:[nulleq(test.t0.c0, test.t0.c0) nulleq(test.t1.c0, test.t1.c0)]",
		"├─HashJoin(Build) 100000.00 root  CARTESIAN inner join",
		"│ ├─TableReader(Build) 10.00 root  data:Selection",
		"│ │ └─Selection 10.00 cop[tikv]  not(istrue_with_null(test.t0.c0))",
		"│ │   └─TableFullScan 10000.00 cop[tikv] table:t0 keep order:false, stats:pseudo",
		"│ └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"│   └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─HashAgg(Probe) 8000.00 root  group by:test.t0.c0, test.t1.c0, funcs:firstrow(test.t0.c0)->test.t0.c0, funcs:firstrow(test.t1.c0)->test.t1.c0",
		"  └─HashJoin 100000.00 root  CARTESIAN left outer join, left side:TableReader",
		"    ├─TableReader(Build) 10.00 root  data:Selection",
		"    │ └─Selection 10.00 cop[tikv]  not(istrue_with_null(test.t0.c0))",
		"    │   └─TableFullScan 10000.00 cop[tikv] table:t0 keep order:false, stats:pseudo",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	// https://github.com/pingcap/tidb/issues/51359
	tk.MustExec("DROP TABLE t0;")
	tk.MustExec("CREATE TABLE t0(c0 BOOL);")
	tk.MustExec("REPLACE INTO t0(c0) VALUES (false), (true);")
	tk.MustExec("CREATE VIEW v0(c0) AS SELECT (REGEXP_LIKE(t0.c0, t0.c0)) FROM t0 WHERE t0.c0 GROUP BY t0.c0 HAVING 1;")
	tk.MustQuery(`SELECT t0.c0 FROM v0, t0 WHERE (SUBTIME('2001-11-28 06', '252 10') OR ('' IS NOT NULL));`).Check(testkit.Rows("0", "1"))
	tk.MustQuery(`explain format='brief' SELECT t0.c0 FROM v0, t0 WHERE (SUBTIME('2001-11-28 06', '252 10') OR ('' IS NOT NULL));`).Check(testkit.Rows(
		"HashJoin 27265706.67 root  CARTESIAN inner join",
		"├─Selection(Build) 3408.21 root  or(istrue_with_null(cast(subtime(\"2001-11-28 06\", \"252 10\"), double BINARY)), 1)",
		"│ └─HashAgg 4260.27 root  group by:test.t0.c0, funcs:firstrow(1)->Column#7",
		"│   └─Selection 5325.33 root  or(istrue_with_null(cast(subtime(\"2001-11-28 06\", \"252 10\"), double BINARY)), 1)",
		"│     └─TableReader 6656.67 root  data:Selection",
		"│       └─Selection 6656.67 cop[tikv]  test.t0.c0",
		"│         └─TableFullScan 10000.00 cop[tikv] table:t0 keep order:false, stats:pseudo",
		"└─Selection(Probe) 8000.00 root  or(istrue_with_null(cast(subtime(\"2001-11-28 06\", \"252 10\"), double BINARY)), 1)",
		"  └─TableReader 10000.00 root  data:TableFullScan",
		"    └─TableFullScan 10000.00 cop[tikv] table:t0 keep order:false, stats:pseudo"))
	tk.MustQuery(`SELECT t0.c0 FROM v0, t0 WHERE (SUBTIME('2001-11-28 06', '252 10') OR ('' IS NOT NULL)) AND v0.c0;`).
		Check(testkit.Rows("0", "1"))
	tk.MustQuery(`explain format='brief' SELECT t0.c0 FROM v0, t0 WHERE (SUBTIME('2001-11-28 06', '252 10') OR ('' IS NOT NULL)) AND v0.c0;`).
		Check(testkit.Rows())
}
