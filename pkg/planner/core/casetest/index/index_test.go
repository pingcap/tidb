// Copyright 2023 PingCAP, Inc.
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

package index

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util"
)

func TestNullConditionForPrefixIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
  id char(1) DEFAULT NULL,
  c1 varchar(255) DEFAULT NULL,
  c2 text DEFAULT NULL,
  KEY idx1 (c1),
  KEY idx2 (c1,c2(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t2(a int, b varchar(10), index idx(b(5)))")
	tk.MustExec("create table t3(a int, b varchar(10), c int, primary key (a, b(5)) clustered)")
	tk.MustExec("set tidb_opt_prefix_index_single_scan = 1")
	tk.MustExec("insert into t1 values ('a', '0xfff', '111111'), ('b', '0xfff', '22    '), ('c', '0xfff', ''), ('d', '0xfff', null)")
	tk.MustExec("insert into t2 values (1, 'aaaaaa'), (2, 'bb    '), (3, ''), (4, null)")
	tk.MustExec("insert into t3 values (1, 'aaaaaa', 2), (1, 'bb    ', 3), (1, '', 4)")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
	}

	// test plan cache
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec("prepare stmt from 'select count(1) from t1 where c1 = ? and c2 is not null'")
	tk.MustExec("set @a = '0xfff'")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows(
		"StreamAgg_17 1.00 root  funcs:count(Column#7)->Column#5",
		"└─IndexReader_18 1.00 root  index:StreamAgg_9",
		"  └─StreamAgg_9 1.00 cop[tikv]  funcs:count(1)->Column#7",
		"    └─IndexRangeScan_16 99.90 cop[tikv] table:t1, index:idx2(c1, c2) range:[\"0xfff\" -inf,\"0xfff\" +inf], keep order:false, stats:pseudo"))
}

func TestInvisibleIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1 ( a INT, KEY( a ) INVISIBLE );")
	tk.MustExec("INSERT INTO t1 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);")
	tk.MustQuery(`EXPLAIN SELECT a FROM t1;`).Check(
		testkit.Rows(
			`TableReader_5 10000.00 root  data:TableFullScan_4`,
			`└─TableFullScan_4 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo`))
	tk.MustExec("set session tidb_opt_use_invisible_indexes=on;")
	tk.MustQuery(`EXPLAIN SELECT a FROM t1;`).Check(
		testkit.Rows(
			`IndexReader_7 10000.00 root  index:IndexFullScan_6`,
			`└─IndexFullScan_6 10000.00 cop[tikv] table:t1, index:a(a) keep order:false, stats:pseudo`))
}
