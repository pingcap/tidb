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

package logicalplan

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGroupBySchema(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test;")
		testKit.MustExec(`CREATE TABLE mysql_3 (
    col_int_auto_increment INT(10) AUTO_INCREMENT,
    col_pk_char CHAR(60) NOT NULL,
    col_pk_date DATE NOT NULL,
    col_datetime DATETIME,
    col_int INT,
    col_date DATE,
    PRIMARY KEY (col_int_auto_increment, col_pk_char, col_datetime, col_int, col_date)
);`)
		testKit.MustQuery(`explain format='brief' SELECT *
FROM mysql_3 t1
WHERE EXISTS
    (SELECT DISTINCT a1.*
     FROM mysql_3 a1
     WHERE (a1.col_pk_char NOT IN
              (SELECT a1.col_pk_char
               FROM mysql_3 a1 NATURAL
               RIGHT JOIN mysql_3 a2
               WHERE t1.col_pk_date IS NULL
               GROUP BY a1.col_pk_char)) )`).Check(testkit.Rows("TableDual 0.00 root  rows:0",
			"ScalarSubQuery N/A root  Output: ScalarQueryCol#29, ScalarQueryCol#30, ScalarQueryCol#31, ScalarQueryCol#32, ScalarQueryCol#33, ScalarQueryCol#34, ScalarQueryCol#35",
			"└─HashJoin 8000.00 root  Null-aware anti semi join, left side:TableReader, equal:[eq(test.mysql_3.col_pk_char, test.mysql_3.col_pk_char)]",
			"  ├─HashAgg(Build) 1.00 root  group by:test.mysql_3.col_pk_char, funcs:firstrow(test.mysql_3.col_pk_char)->test.mysql_3.col_pk_char",
			"  │ └─TableDual 0.00 root  rows:0",
			"  └─TableReader(Probe) 10000.00 root  data:TableFullScan",
			"    └─TableFullScan 10000.00 cop[tikv] table:a1 keep order:false, stats:pseudo"))
	})
}

func TestLogicalPlanTypeRegression(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table tt (c year(4) NOT NULL DEFAULT '2016', primary key(c));`)
	tk.MustExec(`insert into tt values (2016);`)
	tk.MustQuery(`select /* issue:50235 */ * from tt where c < 16212511333665770580`).Check(testkit.Rows("2016"))

	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("CREATE TABLE t1 ( c1 int);")
		tk.MustExec("CREATE TABLE t2 ( c1 int unsigned);")
		tk.MustExec("CREATE TABLE t3 ( c1 bigint unsigned);")
		tk.MustExec("INSERT INTO t1 (c1) VALUES (8);")
		tk.MustExec("INSERT INTO t2 (c1) VALUES (2454396638);")

		// issue:52472
		// union int and unsigned int will be promoted to long long
		rs, err := tk.Exec("SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t2")
		require.NoError(t, err)
		require.Len(t, rs.Fields(), 1)
		require.Equal(t, mysql.TypeLonglong, rs.Fields()[0].Column.FieldType.GetType())
		require.NoError(t, rs.Close())

		// union int (even literal) and unsigned bigint will be promoted to decimal
		rs, err = tk.Exec("SELECT 0 UNION ALL SELECT c1 FROM t3")
		require.NoError(t, err)
		require.Len(t, rs.Fields(), 1)
		require.Equal(t, mysql.TypeNewDecimal, rs.Fields()[0].Column.FieldType.GetType())
		require.NoError(t, rs.Close())
	})
}
