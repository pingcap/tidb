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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestPredicateSimplification(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// create table
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
    id VARCHAR(64) PRIMARY KEY
);`)
	tk.MustExec(`CREATE TABLE t2 (
    c1 VARCHAR(64) NOT NULL,
    c2 VARCHAR(64) NOT NULL,
    c3 VARCHAR(64) NOT NULL,
    PRIMARY KEY (c1, c2, c3),
    KEY c3 (c3)
);`)
	tk.MustExec(`CREATE TABLE t3 (
    c1 VARCHAR(64) NOT NULL,
    c2 VARCHAR(64) NOT NULL,
    c3 VARCHAR(64) NOT NULL,
    PRIMARY KEY (c1, c2, c3),
    KEY c3 (c3)
);`)
	tk.MustExec(`CREATE TABLE t4 (
    c1 VARCHAR(64) NOT NULL,
    c2 VARCHAR(64) NOT NULL,
    c3 VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL DEFAULT 'ACTIVE',
    PRIMARY KEY (c1, c2, c3),
    KEY c3 (c3)
);`)
	tk.MustExec(`CREATE TABLE t5 (
    c1 VARCHAR(64) NOT NULL,
    c2 VARCHAR(64) NOT NULL,
    PRIMARY KEY (c1, c2)
);`)
	tk.MustQuery(`explain format='verbose' SELECT i.id, ip_products.products
FROM t1 AS i
LEFT JOIN t4 ON i.id = t4.c3
LEFT JOIN (
    SELECT t4.c3,
           GROUP_CONCAT(DISTINCT t2.c3 ORDER BY t2.c3 ASC) AS products
    FROM t4
    JOIN t3 ON t4.c1 = t3.c1
           AND t4.c2 = t3.c2
    LEFT JOIN t2 ON t4.c1 = t2.c1
                AND t4.c2 = t2.c2
    WHERE t3.c3 = 'production'
      AND t4.state = 'ACTIVE'
    GROUP BY t4.c3, t4.c1, t4.c2
) AS ip_products ON t4.c3 = ip_products.c3
LEFT JOIN t5 ON i.id = t5.c1
            AND t5.c2 = 'production'
WHERE t4.state = 'ACTIVE'
  AND t5.c1 IS NULL
GROUP BY i.id, ip_products.products
HAVING FIND_IN_SET('info', products)
ORDER BY i.id ASC
LIMIT 500 OFFSET 5500;
`).Check(testkit.Rows(
		`TopN_36 8.00 1177779.51 root  test.t1.id, offset:5500, count:500`,
		`└─HashAgg_41 8.00 495969.26 root  group by:Column#16, test.t1.id, funcs:firstrow(test.t1.id)->test.t1.id, funcs:firstrow(Column#16)->Column#16`,
		`  └─Selection_42 8.00 493651.46 root  isnull(test.t5.c1)`,
		`    └─Projection_43 10.00 493152.46 root  test.t1.id, Column#16, test.t5.c1`,
		`      └─HashJoin_56 10.00 493149.46 root  inner join, equal:[eq(test.t4.c3, test.t4.c3)]`,
		`        ├─Selection_123(Build) 6.40 18442.06 root  find_in_set("info", Column#16)`,
		`        │ └─HashAgg_124 8.00 18042.86 root  group by:test.t4.c1, test.t4.c2, test.t4.c3, funcs:group_concat(distinct test.t2.c3 order by test.t2.c3 separator ",")->Column#16, funcs:firstrow(test.t4.c3)->test.t4.c3`,
		`        │   └─IndexJoin_127 15.62 15105.72 root  left outer join, inner:TableReader_177, left side:IndexJoin_160, outer key:test.t4.c1, test.t4.c2, inner key:test.t2.c1, test.t2.c2, equal cond:eq(test.t4.c1, test.t2.c1), eq(test.t4.c2, test.t2.c2)`,
		`        │     ├─IndexJoin_160(Build) 12.50 7150.52 root  inner join, inner:TableReader_173, outer key:test.t3.c1, test.t3.c2, inner key:test.t4.c1, test.t4.c2, equal cond:eq(test.t3.c1, test.t4.c1), eq(test.t3.c2, test.t4.c2)`,
		`        │     │ ├─IndexReader_175(Build) 10.00 655.36 root  index:IndexRangeScan_174`,
		`        │     │ │ └─IndexRangeScan_174 10.00 3177.59 cop[tikv] table:t3, index:c3(c3) range:["production","production"], keep order:false, stats:pseudo`,
		`        │     │ └─TableReader_173(Probe) 0.01 23.96 root  data:Selection_172`,
		`        │     │   └─Selection_172 0.01 358.61 cop[tikv]  eq(test.t4.state, "ACTIVE")`,
		`        │     │     └─TableRangeScan_171 10.00 308.71 cop[tikv] table:t4 range: decided by [eq(test.t4.c1, test.t3.c1) eq(test.t4.c2, test.t3.c2)], keep order:false, stats:pseudo`,
		`        │     └─TableReader_177(Probe) 12.50 57.47 root  data:TableRangeScan_176`,
		`        │       └─TableRangeScan_176 12.50 291.82 cop[tikv] table:t2 range: decided by [eq(test.t2.c1, test.t4.c1) eq(test.t2.c2, test.t4.c2)], keep order:false, stats:pseudo`,
		`        └─IndexJoin_59(Probe) 12.50 471600.26 root  left outer join, inner:TableReader_113, left side:IndexJoin_90, outer key:test.t1.id, inner key:test.t5.c1, equal cond:eq(test.t1.id, test.t5.c1)`,
		`          ├─IndexJoin_90(Build) 12.50 463856.75 root  inner join, inner:TableReader_108, outer key:test.t4.c3, inner key:test.t1.id, equal cond:eq(test.t4.c3, test.t1.id)`,
		`          │ ├─TableReader_106(Build) 10.00 457241.47 root  data:Selection_105`,
		`          │ │ └─Selection_105 10.00 6854186.91 cop[tikv]  eq(test.t4.state, "ACTIVE")`,
		`          │ │   └─TableFullScan_104 10000.00 6355186.91 cop[tikv] table:t4 keep order:false, stats:pseudo`,
		`          │ └─TableReader_108(Probe) 10.00 27.83 root  data:TableRangeScan_107`,
		`          │   └─TableRangeScan_107 10.00 227.31 cop[tikv] table:i range: decided by [eq(test.t1.id, test.t4.c3)], keep order:false, stats:pseudo`,
		`          └─TableReader_113(Probe) 0.01 21.22 root  data:Selection_112`,
		`            └─Selection_112 0.01 317.91 cop[tikv]  eq(test.t5.c2, "production")`,
		`              └─TableRangeScan_111 12.50 268.01 cop[tikv] table:t5 range: decided by [eq(test.t5.c1, test.t1.id) eq(test.t5.c2, production)], keep order:false, stats:pseudo`))
}
