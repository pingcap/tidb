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

package subquery

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestCollateSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, col varchar(100), key ix(col)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("create table t1(id varchar(100)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	samePlan := testkit.Rows(
		"IndexHashJoin root  inner join, inner:IndexLookUp, outer key:Column, inner key:test.t.col, equal cond:eq(Column, test.t.col)",
		"├─HashAgg(Build) root  group by:Column, funcs:firstrow(Column)->Column",
		"│ └─TableReader root  data:HashAgg",
		"│   └─HashAgg cop[tikv]  group by:cast(test.t1.id, var_string(100)), ",
		"│     └─Selection cop[tikv]  not(isnull(cast(test.t1.id, var_string(100))))",
		"│       └─TableFullScan cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─IndexLookUp(Probe) root  ",
		"  ├─Selection(Build) cop[tikv]  not(isnull(test.t.col))",
		"  │ └─IndexRangeScan cop[tikv] table:t, index:ix(col) range: decided by [eq(test.t.col, Column)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) cop[tikv] table:t keep order:false, stats:pseudo")
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
	tk.MustExec(`set collation_connection='utf8_bin';`)
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
	tk.MustExec(`set collation_connection='latin1_bin';`)
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
}
