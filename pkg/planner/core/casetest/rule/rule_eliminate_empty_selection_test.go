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

func TestEmptySelectionEliminator(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test;")
		testKit.MustExec(`CREATE TABLE A ( col_int int(11) DEFAULT NULL, col_varchar_10 varchar(10) DEFAULT NULL, pk int(11) NOT NULL AUTO_INCREMENT, col_varchar_10_not_null varchar(10) NOT NULL, col_int_not_null int(11) NOT NULL, col_decimal decimal(10,0) DEFAULT NULL, col_datetime datetime DEFAULT NULL, col_decimal_not_null decimal(10,0) NOT NULL, col_varchar_1024 varchar(1024) DEFAULT NULL, col_datetime_not_null datetime NOT NULL, col_varchar_1024_not_null varchar(1024) NOT NULL, PRIMARY KEY (pk) ) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=latin1;`)
		testKit.MustExec(`CREATE TABLE F ( col_datetime_not_null datetime NOT NULL, col_decimal decimal(10,0) DEFAULT NULL, col_datetime datetime DEFAULT NULL, col_varchar_10_not_null varchar(10) NOT NULL, pk int(11) NOT NULL AUTO_INCREMENT, col_int int(11) DEFAULT NULL, col_varchar_1024_not_null varchar(1024) NOT NULL, col_decimal_not_null decimal(10,0) NOT NULL, col_varchar_1024 varchar(1024) DEFAULT NULL, col_int_not_null int(11) NOT NULL, col_varchar_10 varchar(10) DEFAULT NULL, PRIMARY KEY (pk) ) ENGINE=InnoDB AUTO_INCREMENT=71 DEFAULT CHARSET=latin1;`)
		testKit.MustExec(`CREATE TABLE G ( col_varchar_10 varchar(10) DEFAULT NULL, col_datetime_not_null datetime NOT NULL, col_int_not_null int(11) NOT NULL, col_int int(11) DEFAULT NULL, col_varchar_1024_not_null varchar(1024) NOT NULL, col_varchar_1024 varchar(1024) DEFAULT NULL, col_decimal decimal(10,0) DEFAULT NULL, col_decimal_not_null decimal(10,0) NOT NULL, pk int(11) NOT NULL AUTO_INCREMENT, col_datetime datetime DEFAULT NULL, col_varchar_10_not_null varchar(10) NOT NULL, PRIMARY KEY (pk) ) ENGINE=InnoDB DEFAULT CHARSET=latin1;`)
		testKit.MustExec(`CREATE TABLE J ( col_varchar_10 varchar(10) DEFAULT NULL, col_int int(11) DEFAULT NULL, col_varchar_10_not_null varchar(10) NOT NULL, pk int(11) NOT NULL AUTO_INCREMENT, col_datetime datetime DEFAULT NULL, col_int_not_null int(11) NOT NULL, col_decimal decimal(10,0) DEFAULT NULL, col_datetime_not_null datetime NOT NULL, col_varchar_1024_not_null varchar(1024) NOT NULL, col_varchar_1024 varchar(1024) DEFAULT NULL, col_decimal_not_null decimal(10,0) NOT NULL, PRIMARY KEY (pk) ) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=latin1;`)
		testKit.MustExec(`CREATE TABLE L ( col_decimal decimal(10,0) DEFAULT NULL, col_int_not_null int(11) NOT NULL, col_datetime_not_null datetime NOT NULL, col_decimal_not_null decimal(10,0) NOT NULL, col_datetime datetime DEFAULT NULL, col_varchar_1024_not_null varchar(1024) NOT NULL, col_varchar_10_not_null varchar(10) NOT NULL, col_int int(11) DEFAULT NULL, col_varchar_1024 varchar(1024) DEFAULT NULL, pk int(11) NOT NULL AUTO_INCREMENT, col_varchar_10 varchar(10) DEFAULT NULL, PRIMARY KEY (pk) ) ENGINE=InnoDB AUTO_INCREMENT=23 DEFAULT CHARSET=latin1;`)
		testKit.MustQuery(`explain format='brief' SELECT table1 . pk AS field1 , table2 . col_int_not_null AS field2 , table1 . pk AS field3 , table1 . col_int_not_null AS field4 FROM A AS table1 LEFT JOIN G AS table2 INNER JOIN J AS table3 INNER JOIN J AS table4 RIGHT JOIN L AS table5 ON table4 . col_datetime = table5 . col_datetime_not_null ON table3 . col_int_not_null = table5 . pk ON table2 . col_datetime_not_null = table3 . col_datetime_not_null ON table1 . col_datetime = table2 . col_datetime_not_null WHERE table1 . pk = 3 HAVING (field1 != 7 OR field2 > 1) ORDER BY field1, field2, field3, field4 ASC LIMIT 2 OFFSET 7 ;`).Check(testkit.Rows(
			`Projection 1.95 root  test.a.pk, test.g.col_int_not_null, test.a.pk, test.a.col_int_not_null`,
			`└─TopN 1.95 root  test.a.pk, test.g.col_int_not_null, test.a.col_int_not_null, offset:7, count:2`,
			`  └─Selection 1.95 root  or(ne(test.a.pk, 7), gt(test.g.col_int_not_null, 1))`,
			`    └─HashJoin 2.44 root  left outer join, left side:Point_Get, equal:[eq(test.a.col_datetime, test.g.col_datetime_not_null)]`,
			`      ├─Point_Get(Build) 1.00 root table:A handle:3`,
			`      └─HashJoin(Probe) 19511.72 root  inner join, equal:[eq(test.j.col_datetime_not_null, test.g.col_datetime_not_null)]`,
			`        ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
			`        │ └─TableFullScan 10000.00 cop[tikv] table:table2 keep order:false, stats:pseudo`,
			`        └─HashJoin(Probe) 15609.38 root  inner join, equal:[eq(test.l.pk, test.j.col_int_not_null)]`,
			`          ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
			`          │ └─TableFullScan 10000.00 cop[tikv] table:table3 keep order:false, stats:pseudo`,
			`          └─HashJoin(Probe) 12487.50 root  right outer join, left side:TableReader, equal:[eq(test.j.col_datetime, test.l.col_datetime_not_null)]`,
			`            ├─TableReader(Build) 9990.00 root  data:Selection`,
			`            │ └─Selection 9990.00 cop[tikv]  not(isnull(test.j.col_datetime))`,
			`            │   └─TableFullScan 10000.00 cop[tikv] table:table4 keep order:false, stats:pseudo`,
			`            └─TableReader(Probe) 10000.00 root  data:TableFullScan`,
			`              └─TableFullScan 10000.00 cop[tikv] table:table5 keep order:false, stats:pseudo`))
		testKit.MustQuery(`explain format='brief' SELECT SUM( table1 . pk ) AS field1 FROM F AS table1 RIGHT JOIN L AS table2 ON table1 . col_decimal = table2 . col_decimal_not_null WHERE ( ( table2 . pk <> 4 OR table1 . pk IS NOT NULL ) AND table2 . pk IN (41) ) HAVING field1 <> 4 ;`).
			Check(testkit.Rows(
				`Selection 0.80 root  ne(Column#23, 4)`,
				`└─StreamAgg 1.00 root  funcs:sum(Column#24)->Column#23`,
				`  └─Projection 1.25 root  cast(test.f.pk, decimal(10,0) BINARY)->Column#24`,
				`    └─HashJoin 1.25 root  right outer join, left side:TableReader, equal:[eq(test.f.col_decimal, test.l.col_decimal_not_null)]`,
				`      ├─Point_Get(Build) 1.00 root table:L handle:41`,
				`      └─TableReader(Probe) 9990.00 root  data:Selection`,
				`        └─Selection 9990.00 cop[tikv]  not(isnull(test.f.col_decimal))`,
				`          └─TableFullScan 10000.00 cop[tikv] table:table1 keep order:false, stats:pseudo`))
	})
}
