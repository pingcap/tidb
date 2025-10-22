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
<<<<<<< HEAD
	tk.MustQuery(`explain format= 'brief' SELECT i.id, ip_products.products FROM t1 AS i LEFT JOIN t4 ON i.id = t4.c3 LEFT JOIN (SELECT t4.c3, GROUP_CONCAT(DISTINCT t2.c3 ORDER BY t2.c3 ASC) AS products FROM t4 JOIN t3 ON t4.c1 = t3.c1 AND t4.c2 = t3.c2 LEFT JOIN t2 ON t4.c1 = t2.c1 AND t4.c2 = t2.c2 WHERE t3.c3 = 'production' AND t4.state = 'ACTIVE' GROUP BY t4.c3, t4.c1, t4.c2) AS ip_products ON t4.c3 = ip_products.c3 LEFT JOIN t5 ON i.id = t5.c1 AND t5.c2 = 'production' WHERE t4.state = 'ACTIVE' AND t5.c1 IS NULL GROUP BY i.id, ip_products.products HAVING FIND_IN_SET('info', products) ORDER BY i.id ASC LIMIT 500 OFFSET 5500;`).
		Check(testkit.Rows(
			`TopN 8.00 root  test.t1.id, offset:5500, count:500`,
			`└─HashAgg 8.00 root  group by:Column#16, test.t1.id, funcs:firstrow(test.t1.id)->test.t1.id, funcs:firstrow(Column#16)->Column#16`,
			`  └─Selection 8.00 root  isnull(test.t5.c1)`,
			`    └─Projection 10.00 root  test.t1.id, Column#16, test.t5.c1`,
			`      └─HashJoin 10.00 root  inner join, equal:[eq(test.t4.c3, test.t4.c3)]`,
			`        ├─Selection(Build) 6.40 root  find_in_set("info", Column#16)`,
			`        │ └─HashAgg 8.00 root  group by:test.t4.c1, test.t4.c2, test.t4.c3, funcs:group_concat(distinct test.t2.c3 order by test.t2.c3 separator ",")->Column#16, funcs:firstrow(test.t4.c3)->test.t4.c3`,
			`        │   └─IndexJoin 15.62 root  left outer join, inner:TableReader, outer key:test.t4.c1, test.t4.c2, inner key:test.t2.c1, test.t2.c2, equal cond:eq(test.t4.c1, test.t2.c1), eq(test.t4.c2, test.t2.c2)`,
			`        │     ├─IndexJoin(Build) 12.50 root  inner join, inner:TableReader, outer key:test.t3.c1, test.t3.c2, inner key:test.t4.c1, test.t4.c2, equal cond:eq(test.t3.c1, test.t4.c1), eq(test.t3.c2, test.t4.c2)`,
			`        │     │ ├─IndexReader(Build) 10.00 root  index:IndexRangeScan`,
			`        │     │ │ └─IndexRangeScan 10.00 cop[tikv] table:t3, index:c3(c3) range:["production","production"], keep order:false, stats:pseudo`,
			`        │     │ └─TableReader(Probe) 0.01 root  data:Selection`,
			`        │     │   └─Selection 0.01 cop[tikv]  eq(test.t4.state, "ACTIVE")`,
			`        │     │     └─TableRangeScan 10.00 cop[tikv] table:t4 range: decided by [eq(test.t4.c1, test.t3.c1) eq(test.t4.c2, test.t3.c2)], keep order:false, stats:pseudo`,
			`        │     └─TableReader(Probe) 12.50 root  data:TableRangeScan`,
			`        │       └─TableRangeScan 12.50 cop[tikv] table:t2 range: decided by [eq(test.t2.c1, test.t4.c1) eq(test.t2.c2, test.t4.c2)], keep order:false, stats:pseudo`,
			`        └─IndexJoin(Probe) 12.50 root  left outer join, inner:TableReader, outer key:test.t1.id, inner key:test.t5.c1, equal cond:eq(test.t1.id, test.t5.c1)`,
			`          ├─IndexJoin(Build) 12.50 root  inner join, inner:TableReader, outer key:test.t4.c3, inner key:test.t1.id, equal cond:eq(test.t4.c3, test.t1.id)`,
			`          │ ├─TableReader(Build) 10.00 root  data:Selection`,
			`          │ │ └─Selection 10.00 cop[tikv]  eq(test.t4.state, "ACTIVE")`,
			`          │ │   └─TableFullScan 10000.00 cop[tikv] table:t4 keep order:false, stats:pseudo`,
			`          │ └─TableReader(Probe) 10.00 root  data:TableRangeScan`,
			`          │   └─TableRangeScan 10.00 cop[tikv] table:i range: decided by [eq(test.t1.id, test.t4.c3)], keep order:false, stats:pseudo`,
			`          └─TableReader(Probe) 0.01 root  data:Selection`,
			`            └─Selection 0.01 cop[tikv]  eq(test.t5.c2, "production")`,
			`              └─TableRangeScan 12.50 cop[tikv] table:t5 range: decided by [eq(test.t5.c1, test.t1.id) eq(test.t5.c2, production)], keep order:false, stats:pseudo`,
		))
=======
		tk.MustExec(`create table t6(a int, b int, c int, d int, index(a,b));`)
		tk.MustExec(`CREATE TABLE t7c899916 (
  col_37 text COLLATE gbk_bin DEFAULT NULL,
  col_38 datetime DEFAULT CURRENT_TIMESTAMP,
  col_39 tinyint unsigned NOT NULL,
  col_40 json NOT NULL,
  col_41 char(140) COLLATE gbk_bin NOT NULL,
  col_42 json DEFAULT NULL,
  col_43 tinytext COLLATE gbk_bin DEFAULT NULL,
  col_44 json DEFAULT NULL,
  col_45 date DEFAULT '2010-01-29',
  col_46 char(221) COLLATE gbk_bin DEFAULT NULL,
  col_47 timestamp,
  UNIQUE KEY idx_15 (col_41,col_39,col_38)
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_bin`)
		tk.MustExec(`CREATE TABLE tlfdfece63 (
  col_41 timestamp NULL DEFAULT NULL,
  col_42 json NOT NULL,
  col_43 varchar(330) COLLATE utf8_general_ci DEFAULT NULL,
  col_44 char(192) COLLATE utf8_general_ci NOT NULL DEFAULT '^_',
  col_45 text COLLATE utf8_general_ci DEFAULT NULL,
  col_46 double DEFAULT '8900.485367052326',
  col_47 decimal(59,2) DEFAULT NULL,
  col_48 varchar(493) COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (col_44) /*T![clustered_index] NONCLUSTERED */,
  KEY idx_20 (col_41,col_47),
  UNIQUE KEY idx_21 ((cast(col_42 as char(64) array)),col_45(4),col_43(3)),
  KEY idx_22 (col_48(4),col_46)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;`)
		tk.MustExec(`CREATE TABLE tad03b424 (
  col_41 timestamp NULL DEFAULT NULL,
  col_42 json NOT NULL,
  col_43 varchar(330) COLLATE utf8_general_ci DEFAULT NULL,
  col_44 char(192) COLLATE utf8_general_ci NOT NULL DEFAULT '^_',
  col_45 text COLLATE utf8_general_ci DEFAULT NULL,
  col_46 double DEFAULT '8900.485367052326',
  col_47 decimal(59,2) DEFAULT NULL,
  col_48 varchar(493) COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (col_44) /*T![clustered_index] NONCLUSTERED */,
  KEY idx_20 (col_41,col_47),
  UNIQUE KEY idx_21 ((cast(col_42 as char(64) array)),col_45(4),col_43(3)),
  KEY idx_22 (col_48(4),col_46)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci`)
		// since the plan may differ under different planner mode, recommend to record explain result to json accordingly.
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		suite := GetPredicateSimplificationSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format=brief " + tt).Rows())
			})
			res := tk.MustQuery("explain format=brief " + tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
>>>>>>> 1d746d80a0e (range: wrongly skip the candidate in the extractBestCNFItemRanges (#62585))
}
