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
		"IndexHashJoin 8000.00 root  inner join, inner:IndexLookUp, outer key:Column#6, inner key:test.t.col, equal cond:eq(Column#6, test.t.col)",
		"├─HashAgg(Build) 6400.00 root  group by:Column#12, funcs:firstrow(Column#12)->Column#6",
		"│ └─TableReader 6400.00 root  data:HashAgg",
		"│   └─HashAgg 6400.00 cop[tikv]  group by:cast(test.t1.id, var_string(5)), ",
		"│     └─Selection 8000.00 cop[tikv]  not(isnull(cast(test.t1.id, var_string(5))))",
		"│       └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─IndexLookUp(Probe) 8000.00 root  ",
		"  ├─Selection(Build) 8000.00 cop[tikv]  not(isnull(test.t.col))",
		"  │ └─IndexRangeScan 8008.01 cop[tikv] table:t, index:ix(col) range: decided by [eq(test.t.col, Column#6)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 8000.00 cop[tikv] table:t keep order:false, stats:pseudo")
	tk.MustQuery(`explain format="brief" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
	tk.MustExec(`set collation_connection='utf8_bin';`)
	tk.MustQuery(`explain format="brief" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
	tk.MustExec(`set collation_connection='latin1_bin';`)
	tk.MustQuery(`explain format="brief" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
}
