// Copyright 2022 PingCAP, Inc.
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

package core_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func checkCost(t *testing.T, tk *testkit.TestKit, q, info string) {
	//| id | estRows | estCost   | task | access object | operator info |
	tk.MustExec(`set @@tidb_enable_new_cost_interface=0`)
	rs := tk.MustQuery("explain format=verbose " + q).Rows()
	oldRoot := fmt.Sprintf("%v", rs[0])
	oldPlan := ""
	for _, r := range rs {
		oldPlan = oldPlan + fmt.Sprintf("%v\n", r)
	}
	tk.MustExec(`set @@tidb_enable_new_cost_interface=1`)
	rs = tk.MustQuery("explain format=verbose " + q).Rows()
	newRoot := fmt.Sprintf("%v", rs[0])
	newPlan := ""
	for _, r := range rs {
		newPlan = newPlan + fmt.Sprintf("%v\n", r)
	}
	if oldRoot != newRoot {
		t.Fatalf("run %v failed, info: %v, expected \n%v\n, but got \n%v\n", q, info, oldPlan, newPlan)
	}
}

func TestNewCostInterfaceTiKV(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, d int, key b(b), key cd(c, d))`)

	queries := []string{
		// table-reader
		"select * from t use index(primary)",
		"select * from t use index(primary) where a < 200",
		"select * from t use index(primary) where a = 200",
		"select * from t use index(primary) where a in (1, 2, 3, 100, 200, 300, 1000)",
		"select a, b, d from t use index(primary)",
		"select a, b, d from t use index(primary) where a < 200",
		"select a, b, d from t use index(primary) where a = 200",
		"select a, b, d from t use index(primary) where a in (1, 2, 3, 100, 200, 300, 1000)",
		"select a from t use index(primary)",
		"select a from t use index(primary) where a < 200",
		"select a from t use index(primary) where a = 200",
		"select a from t use index(primary) where a in (1, 2, 3, 100, 200, 300, 1000)",
		// index-reader
		"select b from t use index(b)",
		"select b from t use index(b) where b < 200",
		"select b from t use index(b) where b = 200",
		"select b from t use index(b) where b in (1, 2, 3, 100, 200, 300, 1000)",
		"select c, d from t use index(cd)",
		"select c, d from t use index(cd) where c < 200",
		"select c, d from t use index(cd) where c = 200",
		"select c, d from t use index(cd) where c in (1, 2, 3, 100, 200, 300, 1000)",
		"select c, d from t use index(cd) where c = 200 and d < 200",
		"select c, d from t use index(cd) where c in (1, 2, 3, 100, 200, 300, 1000) and d = 200",
		"select d from t use index(cd)",
		"select d from t use index(cd) where c < 200",
		"select d from t use index(cd) where c = 200",
		"select d from t use index(cd) where c in (1, 2, 3, 100, 200, 300, 1000)",
		"select d from t use index(cd) where c = 200 and d < 200",
		"select d from t use index(cd) where c in (1, 2, 3, 100, 200, 300, 1000) and d = 200",
		// index-lookup
		"select * from t use index(b)",
		"select * from t use index(b) where b < 200",
		"select * from t use index(b) where b = 200",
		"select * from t use index(b) where b in (1, 2, 3, 100, 200, 300, 1000)",
		"select a, b from t use index(cd)",
		"select a, b from t use index(cd) where c < 200",
		"select a, b from t use index(cd) where c = 200",
		"select a, b from t use index(cd) where c in (1, 2, 3, 100, 200, 300, 1000)",
		"select a, b from t use index(cd) where c = 200 and d < 200",
		"select a, b from t use index(cd) where c in (1, 2, 3, 100, 200, 300, 1000) and d = 200",
		"select * from t use index(cd)",
		"select * from t use index(cd) where c < 200",
		"select * from t use index(cd) where c = 200",
		"select * from t use index(cd) where c in (1, 2, 3, 100, 200, 300, 1000)",
		"select * from t use index(cd) where c = 200 and d < 200",
		"select * from t use index(cd) where c in (1, 2, 3, 100, 200, 300, 1000) and d = 200",
		// index merge
		"select /*+ use_index_merge(t, b, cd) */ * from t where b<100 or c<100",
		"select /*+ use_index_merge(t, b, cd) */ * from t where b<100 or c=100 and d<100",
		"select /*+ use_index_merge(t, b, cd) */ * from t where b<100 or c=100 and d<100 and a<100",
		"select /*+ use_index_merge(t, b, cd) */ * from t where b<100 or c<100 and mod(a, 3)=1",
		"select /*+ use_index_merge(t, b, cd) */ * from t where b<100 or c=100 and d<100 and mod(a, 3)=1",
		"select /*+ use_index_merge(t, b, cd) */ * from t where (b<100 or c=100 and d<100) and mod(a, 3)=1",
		"select /*+ use_index_merge(t, b, cd) */ * from t where b<100 or c=100 and d<100 and a<100 and mod(a, 3)=1",
		"select /*+ use_index_merge(t, primary, b) */ * from t where a<100 or b<100",
		"select /*+ use_index_merge(t, primary, b, cd) */ * from t where a<100 or b<100 or c=100 and d<100",
		// selection + projection
		"select * from t use index(primary) where a+200 < 1000",      // pushed down to table-scan
		"select * from t use index(primary) where mod(a, 200) < 100", // not pushed down
		"select b from t use index(b) where b+200 < 1000",            // pushed down to index-scan
		"select b from t use index(b) where mod(a, 200) < 100",       // not pushed down
		"select * from t use index(b) where b+200 < 1000",            // pushed down to lookup index-side
		"select * from t use index(b) where c+200 < 1000",            // pushed down to lookup table-side
		"select * from t use index(b) where mod(b+c, 200) < 100",     // not pushed down
		// sort
		"select * from t use index(primary) where a < 200 order by a", // table-scan + sort
		"select * from t use index(primary) where a = 200  order by a",
		"select a, b, d from t use index(primary) where a < 200 order by a",
		"select a, b, d from t use index(primary) where a = 200 order by a",
		"select a from t use index(primary) where a < 200 order by a",
		"select a from t use index(primary) where a = 200 order by a",
		"select b from t use index(b) where b < 200 order by b", // index-scan + sort
		"select b from t use index(b) where b = 200 order by b",
		"select c, d from t use index(cd) where c < 200 order by c",
		"select c, d from t use index(cd) where c = 200 order by c",
		"select c, d from t use index(cd) where c = 200 and d < 200 order by c, d",
		"select d from t use index(cd) where c < 200 order by c",
		"select d from t use index(cd) where c = 200 order by c",
		"select d from t use index(cd) where c = 200 and d < 200 order by c, d",
		"select * from t use index(b) where b < 200 order by b", // look-up + sort
		"select * from t use index(b) where b = 200 order by b",
		"select a, b from t use index(cd) where c < 200 order by c",
		"select a, b from t use index(cd) where c = 200 order by c",
		"select a, b from t use index(cd) where c = 200 and d < 200 order by c, d",
		"select * from t use index(cd) where c < 200 order by c",
		"select * from t use index(cd) where c = 200 order by c",
		"select * from t use index(cd) where c = 200 and d < 200 order by c, d",
		// topN
		"select * from t use index(primary) where a < 200 order by a limit 10", // table-scan + topN
		"select * from t use index(primary) where a = 200  order by a limit 10",
		"select a, b, d from t use index(primary) where a < 200 order by a limit 10",
		"select a, b, d from t use index(primary) where a = 200 order by a limit 10",
		"select a from t use index(primary) where a < 200 order by a limit 10",
		"select a from t use index(primary) where a = 200 order by a limit 10",
		"select b from t use index(b) where b < 200 order by b limit 10", // index-scan + topN
		"select b from t use index(b) where b = 200 order by b limit 10",
		"select c, d from t use index(cd) where c < 200 order by c limit 10",
		"select c, d from t use index(cd) where c = 200 order by c limit 10",
		"select c, d from t use index(cd) where c = 200 and d < 200 order by c, d limit 10",
		"select d from t use index(cd) where c < 200 order by c limit 10",
		"select d from t use index(cd) where c = 200 order by c limit 10",
		"select d from t use index(cd) where c = 200 and d < 200 order by c, d limit 10",
		"select * from t use index(b) where b < 200 order by b limit 10", // look-up + topN
		"select * from t use index(b) where b = 200 order by b limit 10",
		"select a, b from t use index(cd) where c < 200 order by c limit 10",
		"select a, b from t use index(cd) where c = 200 order by c limit 10",
		"select a, b from t use index(cd) where c = 200 and d < 200 order by c, d limit 10",
		"select * from t use index(cd) where c < 200 order by c limit 10",
		"select * from t use index(cd) where c = 200 order by c limit 10",
		"select * from t use index(cd) where c = 200 and d < 200 order by c, d limit 10",
		// union all
		"select * from t use index(primary) union all select * from t use index(primary) where a < 200",
		"select b from t use index(primary) union all select b from t use index(b) where b < 200",
		"select b from t use index(b) where b < 400 union all select b from t use index(b) where b < 200",
		"select * from t use index(primary) union all select * from t use index(b) where b < 200",
	}

	for _, q := range queries {
		checkCost(t, tk, q, "")
	}
}
