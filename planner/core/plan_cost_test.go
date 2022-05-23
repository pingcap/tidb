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
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func skipPostOptimizedProjection(plan [][]interface{}) int {
	for i, r := range plan {
		cost := r[2].(string)
		if cost == "0.00" && strings.Contains(r[0].(string), "Projection") {
			// projection injected in post-optimization, whose cost is always 0 under the old cost implementation
			continue
		}
		return i
	}
	return 0
}

func checkCost(t *testing.T, tk *testkit.TestKit, q, info string) {
	//| id | estRows | estCost   | task | access object | operator info |
	tk.MustExec(`set @@tidb_enable_new_cost_interface=0`)
	rs := tk.MustQuery("explain format=verbose " + q).Rows()
	idx := skipPostOptimizedProjection(rs)
	oldRoot := fmt.Sprintf("%v", rs[idx])
	oldPlan := ""
	oldOperators := make([]string, 0, len(rs))
	for _, r := range rs {
		oldPlan = oldPlan + fmt.Sprintf("%v\n", r)
		oldOperators = append(oldOperators, r[0].(string))
	}
	tk.MustExec(`set @@tidb_enable_new_cost_interface=1`)
	rs = tk.MustQuery("explain format=verbose " + q).Rows()
	newRoot := fmt.Sprintf("%v", rs[idx])
	newPlan := ""
	newOperators := make([]string, 0, len(rs))
	for _, r := range rs {
		newPlan = newPlan + fmt.Sprintf("%v\n", r)
		newOperators = append(newOperators, r[0].(string))
	}
	sameOperators := len(oldOperators) == len(newOperators)
	for i := range newOperators {
		if oldOperators[i] != newOperators[i] {
			sameOperators = false
			break
		}
	}
	if oldRoot != newRoot || !sameOperators {
		t.Fatalf("run %v failed, info: %v, expected \n%v\n, but got \n%v\n", q, info, oldPlan, newPlan)
	}
}

func TestNewCostInterfaceTiKV(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/core/DisableProjectionPostOptimization", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/core/DisableProjectionPostOptimization"))
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, d int, k int, key b(b), key cd(c, d), unique key(k))`)

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
		// aggregation
		"select /*+ hash_agg() */ count(*) from t use index(primary) where a < 200",
		"select /*+ hash_agg() */ sum(a) from t use index(primary) where a < 200",
		"select /*+ hash_agg() */ avg(a), b from t use index(primary) where a < 200 group by b",
		"select /*+ stream_agg() */ count(*) from t use index(primary) where a < 200",
		"select /*+ stream_agg() */ sum(a) from t use index(primary) where a < 200",
		"select /*+ stream_agg() */ avg(a), b from t use index(primary) where a < 200 group by b",
		"select /*+ stream_agg() */ avg(d), c from t use index(cd) group by c",
		// limit
		"select * from t use index(primary) where a < 200 limit 10", // table-scan + limit
		"select * from t use index(primary) where a = 200  limit 10",
		"select a, b, d from t use index(primary) where a < 200 limit 10",
		"select a, b, d from t use index(primary) where a = 200 limit 10",
		"select a from t use index(primary) where a < 200 limit 10",
		"select a from t use index(primary) where a = 200 limit 10",
		"select b from t use index(b) where b < 200 limit 10", // index-scan + limit
		"select b from t use index(b) where b = 200 limit 10",
		"select c, d from t use index(cd) where c < 200 limit 10",
		"select c, d from t use index(cd) where c = 200 limit 10",
		"select c, d from t use index(cd) where c = 200 and d < 200 limit 10",
		"select d from t use index(cd) where c < 200 limit 10",
		"select d from t use index(cd) where c = 200 limit 10",
		"select d from t use index(cd) where c = 200 and d < 200 limit 10",
		"select * from t use index(b) where b < 200 limit 10", // look-up + limit
		"select * from t use index(b) where b = 200 limit 10",
		"select a, b from t use index(cd) where c < 200 limit 10",
		"select a, b from t use index(cd) where c = 200 limit 10",
		"select a, b from t use index(cd) where c = 200 and d < 200 limit 10",
		"select * from t use index(cd) where c < 200 limit 10",
		"select * from t use index(cd) where c = 200 limit 10",
		"select * from t use index(cd) where c = 200 and d < 200 limit 10",
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
		// join
		"select /*+ hash_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a=t2.a+2 and t1.b>1000",
		"select /*+ hash_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a<t2.a+2 and t1.b>1000",
		"select /*+ merge_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a=t2.a+2 and t1.b>1000",
		"select /*+ merge_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a<t2.a+2 and t1.b>1000",
		"select /*+ inl_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a=t2.a and t1.b>1000",
		"select /*+ inl_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a=t2.a and t1.b<1000 and t1.b>1000",
		"select /*+ inl_hash_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a=t2.a+2 and t1.b>1000",
		"select /*+ inl_hash_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a=t2.a and t1.b<1000 and t1.b>1000",
		"select /*+ inl_merge_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a=t2.a+2 and t1.b>1000",
		"select /*+ inl_merge_join(t1, t2), use_index(t1, primary), use_index(t2, primary) */ * from t t1, t t2 where t1.a=t2.a and t1.b<1000 and t1.b>1000",
		"select * from t t1 where t1.b in (select sum(t2.b) from t t2 where t1.a < t2.a)", // apply
		// point get
		"select * from t where a = 1", // generated in fast plan optimization
		"select * from t where a in (1, 2, 3, 4, 5)",
		"select * from t where k = 1",
		"select * from t where k in (1, 2, 3, 4, 5)",
		"select * from t where a=1 and mod(a, b)=2", // generated in physical plan optimization
		"select * from t where a in (1, 2, 3, 4, 5) and mod(a, b)=2",
		"select * from t where k=1 and mod(k, b)=2",
		"select * from t where k in (1, 2, 3, 4, 5) and mod(k, b)=2",
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

func TestNewCostInterfaceTiFlash(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/core/DisableProjectionPostOptimization", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/core/DisableProjectionPostOptimization"))
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, d int)`)
	tk.MustExec(`create table tx (id int, value decimal(6,3))`)

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" || tblInfo.Name.L == "tx" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	queries := []string{
		"select * from t",
		"select * from t where a < 200",
		"select * from t where a = 200",
		"select * from t where a in (1, 2, 3, 100, 200, 300, 1000)",
		"select a from t",
		"select a from t where a < 200",
		"select * from t where a+200 < 1000",
		"select * from t where mod(a, 200) < 100",
		"select b from t where b+200 < 1000",
		"select b from t where mod(a, 200) < 100",
		"select * from t where b+200 < 1000",
		"select * from t where c+200 < 1000",
		"select * from t where mod(b+c, 200) < 100",
		"select a, b, d from t",
		"select a, b, d from t where a < 200",
		"select a, b, d from t where a = 200",
		"select a, b, d from t where a in (1, 2, 3, 100, 200, 300, 1000)",
		"select a from t where a = 200",
		"select a from t where a in (1, 2, 3, 100, 200, 300, 1000)",
		"select b from t",
		"select b from t where b < 200",
		"select b from t where b = 200",
		"select b from t where b in (1, 2, 3, 100, 200, 300, 1000)",
		"select c, d from t",
		"select c, d from t where c < 200",
		"select c, d from t where c = 200",
		"select c, d from t where c in (1, 2, 3, 100, 200, 300, 1000)",
		"select c, d from t where c = 200 and d < 200",
		"select d from t",
		"select d from t where c < 200",
		"select d from t where c = 200",
		"select d from t where c in (1, 2, 3, 100, 200, 300, 1000)",
		"select d from t where c = 200 and d < 200",
		"select * from t where b < 200",
		"select * from t where b = 200",
		"select * from t where b in (1, 2, 3, 100, 200, 300, 1000)",
		"select a, b from t",
		"select a, b from t where c < 200",
		"select a, b from t where c = 200",
		"select a, b from t where c in (1, 2, 3, 100, 200, 300, 1000)",
		"select a, b from t where c = 200 and d < 200",
		"select * from t where c < 200",
		"select * from t where c = 200",
		"select * from t where c in (1, 2, 3, 100, 200, 300, 1000)",
		"select * from t where c = 200 and d < 200",
		"select * from t where b<100 or c<100",
		"select * from t where b<100 or c=100 and d<100",
		"select * from t where b<100 or c=100 and d<100 and a<100",
		"select count(*) from t where a < 200",
		"select max(a) from t where a < 200",
		"select avg(a) from t",
		"select sum(a) from t where a < 200",
		"select * from t where a < 200 limit 10",
		"select * from t where a = 200  limit 10",
		"select a, b, d from t where a < 200 limit 10",
		"select a, b, d from t where a = 200 limit 10",
		"select a from t where a < 200 limit 10",
		"select a from t where a = 200 limit 10",
		"select b from t where b < 200 limit 10",
		"select b from t where b = 200 limit 10",
		"select c, d from t where c < 200 limit 10",
		"select c, d from t where c = 200 limit 10",
		"select c, d from t where c = 200 and d < 200 limit 10",
		"select d from t where c < 200 limit 10",
		"select d from t where c = 200 limit 10",
		"select d from t where c = 200 and d < 200 limit 10",
		"select * from t where b < 200 limit 10",
		"select * from t where b = 200 limit 10",
		"select a, b from t where c < 200 limit 10",
		"select a, b from t where c = 200 limit 10",
		"select a, b from t where c = 200 and d < 200 limit 10",
		"select * from t where c < 200 limit 10",
		"select * from t where c = 200 limit 10",
		"select * from t where c = 200 and d < 200 limit 10",
		"select * from t where a < 200 order by a",
		"select * from t where a = 200  order by a",
		"select a, b, d from t where a < 200 order by a",
		"select a, b, d from t where a = 200 order by a",
		"select a from t where a < 200 order by a",
		"select a from t where a = 200 order by a",
		"select b from t where b < 200 order by b",
		"select b from t where b = 200 order by b",
		"select c, d from t where c < 200 order by c",
		"select c, d from t where c = 200 order by c",
		"select c, d from t where c = 200 and d < 200 order by c, d",
		"select d from t where c < 200 order by c",
		"select d from t where c = 200 order by c",
		"select d from t where c = 200 and d < 200 order by c, d",
		"select * from t where b < 200 order by b",
		"select * from t where b = 200 order by b",
		"select a, b from t where c < 200 order by c",
		"select a, b from t where c = 200 order by c",
		"select a, b from t where c = 200 and d < 200 order by c, d",
		"select * from t where c < 200 order by c",
		"select * from t where c = 200 order by c",
		"select * from t where c = 200 and d < 200 order by c, d",
		"select * from t where a < 200 order by a limit 10",
		"select * from t where a = 200  order by a limit 10",
		"select a, b, d from t where a < 200 order by a limit 10",
		"select a, b, d from t where a = 200 order by a limit 10",
		"select a from t where a < 200 order by a limit 10",
		"select a from t where a = 200 order by a limit 10",
		"select b from t where b < 200 order by b limit 10",
		"select b from t where b = 200 order by b limit 10",
		"select c, d from t where c < 200 order by c limit 10",
		"select c, d from t where c = 200 order by c limit 10",
		"select c, d from t where c = 200 and d < 200 order by c, d limit 10",
		"select d from t where c < 200 order by c limit 10",
		"select d from t where c = 200 order by c limit 10",
		"select d from t where c = 200 and d < 200 order by c, d limit 10",
		"select * from t where b < 200 order by b limit 10",
		"select * from t where b = 200 order by b limit 10",
		"select a, b from t where c < 200 order by c limit 10",
		"select a, b from t where c = 200 order by c limit 10",
		"select a, b from t where c = 200 and d < 200 order by c, d limit 10",
		"select * from t where c < 200 order by c limit 10",
		"select * from t where c = 200 order by c limit 10",
		"select * from t where c = 200 and d < 200 order by c, d limit 10",
		"select * from t t1, t t2 where t1.a=t2.a+2 and t1.b>1000",
		"select * from t t1, t t2 where t1.a<t2.a+2 and t1.b>1000",
		"select * from t t1, t t2 where t1.a=t2.a and t1.b>1000",
		"select * from t t1, t t2 where t1.a=t2.a and t1.b<1000 and t1.b>1000",
		"select * from t t1 where t1.b in (select sum(t2.b) from t t2 where t1.a < t2.a)",
		"select * from t where a = 1",
		"select * from t where a in (1, 2, 3, 4, 5)",
		"select * from tx join ( select count(*), id from tx group by id) as A on A.id = tx.id",
		`select * from tx join ( select count(*), id from tx group by id) as A on A.id = tx.id`,
		`select * from tx join ( select count(*)+id as v from tx group by id) as A on A.v = tx.id`,
		`select * from tx join ( select count(*) as v, id from tx group by value,id having value+v <10) as A on A.id = tx.id`,
		`select * from tx join ( select /*+ hash_agg()*/  count(*) as a from t) as A on A.a = tx.id`,
		`select sum(b) from (select tx.id, t1.id as b from tx join tx t1 on tx.id=t1.id)A group by id`,
		`select * from (select id from tx group by id) C join (select sum(value),id from tx group by id)B on C.id=B.id`,
		`select * from (select id from tx group by id) C join (select sum(b),id from (select tx.id, t1.id as b from tx join (select id, count(*) as c from tx group by id) t1 on tx.id=t1.id)A group by id)B on C.id=b.id`,
		`select * from tx join tx t1 on tx.id = t1.id order by tx.value limit 1`,
		`select * from tx join tx t1 on tx.id = t1.id order by tx.value % 100 limit 1`,
		`select count(*) from (select tx.id, tx.value v1 from tx join tx t1 on tx.id = t1.id order by tx.value limit 20) v group by v.v1`,
		`select * from tx join tx t1 on tx.id = t1.id limit 1`,
		`select * from tx join tx t1 on tx.id = t1.id limit 1`,
		`select count(*) from (select tx.id, tx.value v1 from tx join tx t1 on tx.id = t1.id limit 20) v group by v.v1`,
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")

	for mppMode := 0; mppMode < 3; mppMode++ {
		switch mppMode {
		case 0: // not allow MPP
			tk.MustExec(`set @@session.tidb_allow_mpp=0`)
			tk.MustExec(`set @@session.tidb_enforce_mpp=0`)
		case 1: // allow MPP
			tk.MustExec(`set @@session.tidb_allow_mpp=1`)
			tk.MustExec(`set @@session.tidb_enforce_mpp=0`)
		case 2: // allow and enforce MPP
			tk.MustExec(`set @@session.tidb_allow_mpp=1`)
			tk.MustExec(`set @@session.tidb_enforce_mpp=1`)
		}
		for _, q := range queries {
			checkCost(t, tk, q, fmt.Sprintf("mpp-mode=%v", mppMode))
		}
	}
}

func TestNewCostInterfaceRandGen(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/core/DisableProjectionPostOptimization", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/core/DisableProjectionPostOptimization"))
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, d int, k int, key b(b), key cd(c, d), unique key(k))`)

	queries := []string{
		`SELECT a FROM t WHERE a is null AND d in (5307, 15677, 57970)`,
		`SELECT a FROM t WHERE a < 5249 AND d < 53853`,
		`SELECT a FROM t WHERE a = 37558 AND k is null`,
		`SELECT a FROM t WHERE a is null AND k > 47109`,
		`SELECT a FROM t WHERE a>=81757 and a<=52503 AND k>=44299 and k<=56690`,
		`SELECT a FROM t WHERE b is null AND c>=11402 and c<=11808`,
		`SELECT a FROM t WHERE b = 22377 AND c in (36373, 89195, 85525) AND d > 46984`,
		`SELECT a FROM t WHERE b = 41268 AND c < 16741 AND d > 60460`,
		`SELECT a FROM t WHERE b is null AND c is null AND d>=70293 and d<=42665`,
		`SELECT a FROM t WHERE b is null AND c>=85486 and c<=52455 AND d > 72382`,
		`SELECT a FROM t WHERE b in (15884, 95530, 25409) AND c>=18838 and c<=82501 AND d is null`,
		`SELECT a FROM t WHERE b > 73974 AND c in (22075, 111, 47238) AND d in (4552, 55871, 5300)`,
		`SELECT a FROM t WHERE b > 49384 AND c > 30848 AND d > 30358`,
		`SELECT a FROM t WHERE b > 20339 AND c < 55367 AND d < 30160`,
		`SELECT a FROM t WHERE b < 16987 AND c > 92433 AND d is null`,
		`SELECT a FROM t WHERE b < 18308 AND c < 65133 AND d < 53450`,
		`SELECT a FROM t WHERE b < 45995 AND c>=75665 and c<=25323 AND d > 15314`,
		`SELECT a FROM t WHERE k > 57433 AND b in (84257, 58984, 9925)`,
		`SELECT a FROM t WHERE k>=45822 and k<=11279 AND b in (97811, 9875, 21247)`,
		`SELECT a FROM t WHERE a = 63351 AND d is null AND mod(a, b) = 78178`,
		`SELECT a FROM t WHERE a is null AND d is null AND a*d < 43948`,
		`SELECT a FROM t WHERE a > 86402 AND d = 35984 AND a*d < 97541`,
		`SELECT a FROM t WHERE a < 66073 AND d = 48387 AND a+b > 90131`,
		`SELECT a FROM t WHERE a>=63993 and a<=57839 AND d = 40123 AND a*d < 92348`,
		`SELECT a FROM t WHERE a>=69337 and a<=50009 AND d>=57342 and d<=73038 AND a+b > 21281`,
		`SELECT a FROM t WHERE a = 46600 AND k = 4045 AND a+b > 63377`,
		`SELECT a FROM t WHERE a = 64907 AND k > 21564 AND a+b > 90210`,
		`SELECT a FROM t WHERE a is null AND k>=71815 and k<=29287 AND mod(a, b) = 87618`,
		`SELECT a FROM t WHERE a in (12133, 80985, 95957) AND k < 29578 AND mod(a, b) = 53639`,
		`SELECT a FROM t WHERE a > 90291 AND k = 17931 AND a*d < 15660`,
		`SELECT a FROM t WHERE a > 72900 AND k in (43761, 93036, 58629) AND a+b > 28365`,
		`SELECT a FROM t WHERE a>=40268 and a<=95327 AND k = 85816 AND a*d < 82116`,
		`SELECT a FROM t WHERE a>=25591 and a<=95960 AND k>=65083 and k<=55574 AND a+b > 49028`,
		`SELECT a FROM t WHERE k is null AND b in (4043, 9375, 71444) AND a+b > 27115`,
		`SELECT a FROM t WHERE k is null AND b > 66682 AND a*d < 56174`,
		`SELECT a FROM t WHERE k in (10080, 37620, 6509) AND b>=57439 and b<=27636 AND mod(a, b) = 69063`,
		`SELECT a FROM t WHERE k > 92407 AND b is null AND mod(a, b) = 3304`,
		`SELECT a FROM t WHERE k>=23611 and k<=99531 AND b > 9494 AND mod(a, b) = 63211`,
		`SELECT k FROM t WHERE a = 1444 AND d in (4080, 8475, 17179)`,
		`SELECT k FROM t WHERE a > 80986 AND d in (94778, 90733, 57569)`,
		`SELECT k FROM t WHERE a > 36090 AND d > 27522`,
		`SELECT k FROM t WHERE a>=32611 and a<=36664 AND d < 48923`,
		`SELECT k FROM t WHERE b is null AND c < 13448`,
		`SELECT k FROM t WHERE b > 37710 AND c > 66725`,
		`SELECT k FROM t WHERE b>=64517 and b<=83568 AND c = 67007`,
		`SELECT k FROM t WHERE b = 80057 AND c < 46024 AND d>=42030 and d<=78788`,
		`SELECT k FROM t WHERE b in (58936, 76487, 14545) AND c in (72779, 39420, 87715) AND d>=79545 and d<=45834`,
		`SELECT k FROM t WHERE b > 57692 AND c = 90484 AND d < 16159`,
		`SELECT k FROM t WHERE b > 50424 AND c < 8679 AND d>=15846 and d<=92372`,
		`SELECT k FROM t WHERE b < 66102 AND c = 83018 AND d is null`,
		`SELECT k FROM t WHERE d > 50996 AND mod(a, b) = 67158`,
		`SELECT k FROM t WHERE d < 12095 AND mod(a, b) = 61433`,
		`SELECT k FROM t WHERE a in (56774, 63236, 99447) AND d > 7297 AND mod(a, b) = 14057`,
		`SELECT k FROM t WHERE a > 5308 AND d>=60994 and d<=42130 AND a+b > 73261`,
		`SELECT k FROM t WHERE a < 98957 AND d is null AND a*d < 67352`,
		`SELECT k FROM t WHERE a = 34960 AND k = 72815 AND mod(a, b) = 66744`,
		`SELECT k FROM t WHERE a is null AND k>=31371 and k<=52885 AND a+b > 46149`,
		`SELECT k FROM t WHERE a > 59580 AND k > 38759 AND a*d < 67577`,
		`SELECT k FROM t WHERE a < 62053 AND k = 34325 AND mod(a, b) = 56602`,
		`SELECT k FROM t WHERE a < 38680 AND k = 81605 AND a*d < 91267`,
		`SELECT k FROM t WHERE a < 59369 AND k > 18795 AND a+b > 15317`,
		`SELECT k FROM t WHERE k = 16018 AND b is null AND mod(a, b) = 84777`,
		`SELECT k FROM t WHERE k = 46621 AND b > 4598 AND a+b > 43586`,
		`SELECT k FROM t WHERE k > 36229 AND b > 29444 AND a+b > 95339`,
		`SELECT k FROM t WHERE k < 62452 AND b is null AND a+b > 98062`,
		`SELECT k FROM t WHERE k < 98934 AND b is null AND a*d < 1677`,
		`SELECT k FROM t WHERE k < 47276 AND b > 84606 AND mod(a, b) = 51185`,
		`SELECT k FROM t WHERE k < 44279 AND b>=91658 and b<=16368 AND a*d < 21147`,
		`SELECT k FROM t WHERE k>=32503 and k<=21982 AND b>=26002 and b<=97673 AND a+b > 6503`,
		`SELECT b, c FROM t WHERE a is null AND d is null`,
		`SELECT b, c FROM t WHERE a > 2310 AND d is null`,
		`SELECT b, c FROM t WHERE a>=31833 and a<=68436 AND k is null`,
		`SELECT b, c FROM t WHERE b in (9066, 6439, 81364) AND c in (6604, 1963, 72711)`,
		`SELECT b, c FROM t WHERE b > 54280 AND c > 94284`,
		`SELECT b, c FROM t WHERE b = 79657 AND c = 24796 AND d in (72758, 85376, 60944)`,
		`SELECT b, c FROM t WHERE b = 43767 AND c is null AND d in (1119, 75500, 37511)`,
		`SELECT b, c FROM t WHERE b is null AND c = 22194 AND d = 3568`,
		`SELECT b, c FROM t WHERE b is null AND c is null AND d in (75374, 29057, 85369)`,
		`SELECT b, c FROM t WHERE b is null AND c>=89860 and c<=76380 AND d is null`,
		`SELECT b, c FROM t WHERE b in (58408, 84563, 61612) AND c = 47150 AND d < 33985`,
		`SELECT b, c FROM t WHERE b > 58424 AND c = 33455 AND d is null`,
		`SELECT b, c FROM t WHERE b > 54134 AND c is null AND d = 48023`,
		`SELECT b, c FROM t WHERE b > 25866 AND c is null AND d in (96524, 26364, 61697)`,
		`SELECT b, c FROM t WHERE b > 44773 AND c in (31886, 4842, 32154) AND d < 92505`,
		`SELECT b, c FROM t WHERE b > 18874 AND c in (81329, 50805, 27202) AND d>=22864 and d<=84263`,
		`SELECT b, c FROM t WHERE b>=43136 and b<=8599 AND c in (41890, 84900, 88125) AND d < 99741`,
		`SELECT b, c FROM t WHERE b>=93644 and b<=35556 AND c < 93075 AND d is null`,
		`SELECT b, c FROM t WHERE k < 73782 AND b > 26700`,
		`SELECT b, c FROM t WHERE k>=72979 and k<=79198 AND b is null`,
		`SELECT b, c FROM t WHERE a = 62561 AND a*d < 33664`,
		`SELECT b, c FROM t WHERE b in (27868, 78367, 8168) AND a+b > 18427`,
		`SELECT b, c FROM t WHERE b>=3235 and b<=79051 AND mod(a, b) = 44383`,
		`SELECT b, c FROM t WHERE c < 72536 AND mod(a, b) = 66604`,
		`SELECT b, c FROM t WHERE d>=96940 and d<=16368 AND a*d < 2679`,
		`SELECT b, c FROM t WHERE a = 72719 AND d>=98240 and d<=5463 AND a+b > 26597`,
		`SELECT b, c FROM t WHERE a is null AND d in (27789, 16829, 67669) AND a*d < 88825`,
		`SELECT b, c FROM t WHERE a in (24327, 48968, 55371) AND d in (52338, 88223, 85163) AND a*d < 21444`,
		`SELECT b, c FROM t WHERE a in (77672, 15113, 39595) AND d>=33925 and d<=73243 AND mod(a, b) = 77672`,
		`SELECT b, c FROM t WHERE a > 56895 AND d < 29082 AND a*d < 70355`,
		`SELECT b, c FROM t WHERE a = 24548 AND k is null AND a*d < 87761`,
		`SELECT b, c FROM t WHERE a = 98143 AND k>=87572 and k<=77163 AND mod(a, b) = 66931`,
		`SELECT b, c FROM t WHERE a is null AND k in (7657, 60599, 95977) AND mod(a, b) = 37418`,
		`SELECT b, c FROM t WHERE a in (44973, 84813, 44282) AND k = 1324 AND a+b > 94476`,
		`SELECT b, c FROM t WHERE a in (70779, 68043, 31062) AND k>=13796 and k<=53745 AND a+b > 31424`,
		`SELECT b, c FROM t WHERE a > 15307 AND k = 50524 AND mod(a, b) = 41837`,
		`SELECT b, c FROM t WHERE k in (53340, 21679, 60521) AND b is null AND a*d < 67443`,
		`SELECT b, c FROM t WHERE k in (64616, 95206, 30908) AND b > 22418 AND a+b > 31226`,
		`SELECT b, c FROM t WHERE k > 1637 AND b is null AND a*d < 98258`,
		`SELECT b, c FROM t WHERE k < 58191 AND b is null AND mod(a, b) = 7008`,
		`SELECT b, c FROM t WHERE k>=3591 and k<=79869 AND b in (33405, 75533, 7432) AND mod(a, b) = 59529`,
		`SELECT b, c FROM t WHERE k>=26177 and k<=10246 AND b < 64168 AND a*d < 22865`,
		`SELECT b, c FROM t WHERE k>=89765 and k<=93233 AND b>=11956 and b<=95573 AND a*d < 88686`,
		`SELECT b, c FROM t WHERE a > 29622 AND d>=36318 and d<=23499`,
		`SELECT b, c FROM t WHERE a is null AND k is null`,
		`SELECT b, c FROM t WHERE a in (74287, 13193, 57157) AND k < 79127`,
		`SELECT b, c FROM t WHERE a < 48025 AND k = 67429`,
		`SELECT b, c FROM t WHERE a < 53946 AND k < 67198`,
		`SELECT b, c FROM t WHERE b < 60379 AND c is null`,
		`SELECT b, c FROM t WHERE b = 3558 AND c is null AND d is null`,
		`SELECT b, c FROM t WHERE b = 55577 AND c>=27402 and c<=48957 AND d in (98469, 23938, 88789)`,
		`SELECT b, c FROM t WHERE b = 31200 AND c>=31416 and c<=54397 AND d < 35013`,
		`SELECT b, c FROM t WHERE b > 94911 AND c < 69486 AND d < 25258`,
		`SELECT b, c FROM t WHERE b < 69960 AND c = 28117 AND d = 49407`,
		`SELECT b, c FROM t WHERE b>=8587 and b<=96 AND c is null AND d is null`,
		`SELECT b, c FROM t WHERE b>=16553 and b<=49995 AND c in (76789, 74982, 59323) AND d in (45514, 62786, 25146)`,
		`SELECT b, c FROM t WHERE b>=45349 and b<=5511 AND c < 57934 AND d in (69319, 69893, 8480)`,
		`SELECT b, c FROM t WHERE b>=79848 and b<=47746 AND c>=65103 and c<=9240 AND d > 40899`,
		`SELECT b, c FROM t WHERE k = 96439 AND b = 39519`,
		`SELECT b, c FROM t WHERE k < 29688 AND b is null`,
		`SELECT b, c FROM t WHERE k>=14847 and k<=93580 AND b is null`,
		`SELECT b, c FROM t WHERE k>=92856 and k<=63024 AND b > 66859`,
		`SELECT b, c FROM t WHERE a < 41511 AND mod(a, b) = 72554`,
		`SELECT b, c FROM t WHERE b = 56703 AND mod(a, b) = 38329`,
		`SELECT b, c FROM t WHERE c>=62037 and c<=32402 AND a+b > 41459`,
		`SELECT b, c FROM t WHERE d < 79597 AND a+b > 98510`,
		`SELECT b, c FROM t WHERE a = 38578 AND d in (34724, 4018, 3991) AND mod(a, b) = 83305`,
		`SELECT b, c FROM t WHERE a is null AND d > 70794 AND mod(a, b) = 65278`,
		`SELECT b, c FROM t WHERE a > 25996 AND d>=55060 and d<=64846 AND a+b > 72518`,
		`SELECT b, c FROM t WHERE a < 29860 AND d in (52649, 19257, 33624) AND a+b > 70480`,
		`SELECT b, c FROM t WHERE a = 29927 AND k is null AND a*d < 30724`,
		`SELECT b, c FROM t WHERE a = 37641 AND k > 88669 AND a*d < 58430`,
		`SELECT b, c FROM t WHERE a is null AND k < 86831 AND mod(a, b) = 61406`,
		`SELECT b, c FROM t WHERE a in (26051, 71924, 28887) AND k = 64597 AND mod(a, b) = 39169`,
		`SELECT b, c FROM t WHERE a > 26547 AND k in (67170, 89094, 92496) AND a*d < 54517`,
		`SELECT b, c FROM t WHERE a>=53488 and a<=38514 AND k < 6889 AND mod(a, b) = 61936`,
		`SELECT b, c FROM t WHERE k = 11542 AND b>=38255 and b<=18494 AND mod(a, b) = 79651`,
		`SELECT b, c FROM t WHERE k is null AND b = 65701 AND a+b > 46076`,
		`SELECT b, c FROM t WHERE k is null AND b > 47792 AND a*d < 2507`,
		`SELECT b, c FROM t WHERE k is null AND b < 64615 AND a*d < 12386`,
		`SELECT b, c FROM t WHERE k in (63665, 16915, 76535) AND b > 3641 AND a*d < 10111`,
		`SELECT b, c FROM t WHERE k < 65866 AND b is null AND a+b > 49267`,
		`SELECT b, c FROM t WHERE k < 64925 AND b > 63685 AND a+b > 81239`,
		`SELECT k FROM t WHERE a in (45672, 96060, 73816)`,
		`SELECT k FROM t WHERE b>=14713 and b<=9979`,
		`SELECT k FROM t WHERE a in (31686, 86729, 24963) AND d in (8904, 55479, 80621)`,
		`SELECT k FROM t WHERE a in (11639, 90961, 16171) AND d < 27402`,
		`SELECT k FROM t WHERE a in (88312, 63747, 73656) AND d>=6825 and d<=41417`,
		`SELECT k FROM t WHERE a>=62439 and a<=15814 AND d in (15518, 57193, 87241)`,
		`SELECT k FROM t WHERE b>=57589 and b<=91190 AND c is null`,
		`SELECT k FROM t WHERE b is null AND c is null AND d = 14997`,
		`SELECT k FROM t WHERE b is null AND c is null AND d > 94280`,
		`SELECT k FROM t WHERE b is null AND c in (51646, 62772, 18326) AND d > 9874`,
		`SELECT k FROM t WHERE b in (38014, 32450, 94110) AND c is null AND d = 49878`,
		`SELECT k FROM t WHERE b > 92762 AND c = 87829 AND d < 39713`,
		`SELECT k FROM t WHERE b > 16563 AND c > 81018 AND d>=43265 and d<=68929`,
		`SELECT k FROM t WHERE b>=41299 and b<=12051 AND c = 28794 AND d < 87563`,
		`SELECT k FROM t WHERE b>=17635 and b<=40487 AND c is null AND d>=2853 and d<=52997`,
		`SELECT k FROM t WHERE b>=85878 and b<=80259 AND c < 58841 AND d = 23592`,
		`SELECT k FROM t WHERE k in (50371, 65602, 98419) AND b > 7605`,
		`SELECT k FROM t WHERE b in (61191, 50945, 67191) AND a*d < 97927`,
		`SELECT k FROM t WHERE d < 17416 AND a*d < 87600`,
		`SELECT k FROM t WHERE a = 78156 AND d < 24844 AND mod(a, b) = 95281`,
		`SELECT k FROM t WHERE a is null AND d>=31812 and d<=32053 AND a+b > 70136`,
		`SELECT k FROM t WHERE a in (51299, 85247, 70792) AND d in (51415, 7884, 33717) AND a+b > 98245`,
		`SELECT k FROM t WHERE a in (37081, 28332, 93844) AND d < 90867 AND mod(a, b) = 14996`,
		`SELECT k FROM t WHERE a > 8094 AND d in (67166, 99613, 20070) AND a+b > 74297`,
		`SELECT k FROM t WHERE a = 6587 AND k > 68782 AND a*d < 45700`,
		`SELECT k FROM t WHERE a in (20456, 90182, 35535) AND k is null AND mod(a, b) = 48039`,
		`SELECT k FROM t WHERE a in (66256, 3995, 46771) AND k is null AND a*d < 12729`,
		`SELECT k FROM t WHERE a > 93753 AND k is null AND a+b > 97967`,
		`SELECT k FROM t WHERE a > 15415 AND k is null AND a*d < 93788`,
		`SELECT k FROM t WHERE a < 4241 AND k = 56792 AND a+b > 25880`,
		`SELECT k FROM t WHERE a>=3658 and a<=64400 AND k in (38725, 62765, 51002) AND a*d < 19430`,
		`SELECT k FROM t WHERE a>=46926 and a<=94720 AND k>=44233 and k<=5806 AND a*d < 77329`,
		`SELECT k FROM t WHERE k = 11953 AND b = 11957 AND a+b > 78118`,
		`SELECT k FROM t WHERE k = 10309 AND b is null AND mod(a, b) = 68942`,
		`SELECT k FROM t WHERE k in (35154, 57436, 80665) AND b in (21732, 49481, 28793) AND a+b > 781`,
		`SELECT k FROM t WHERE k < 79198 AND b in (53078, 60906, 20011) AND a*d < 80188`,
		`SELECT k FROM t WHERE k>=94116 and k<=30770 AND b = 39293 AND a*d < 8054`,
		`SELECT k FROM t WHERE k>=43524 and k<=76789 AND b < 2902 AND a*d < 19011`,
		`SELECT k FROM t WHERE k>=29446 and k<=49666 AND b>=818 and b<=3649 AND mod(a, b) = 93299`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a = 29203 AND d is null GROUP BY a`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a in (52302, 85259, 18111) AND d = 30058 GROUP BY a`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a > 12746 AND d = 31957 GROUP BY a`,
		`SELECT a, sum(b), avg(d) FROM t WHERE b = 75831 GROUP BY a`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a in (34318, 16711, 57252) AND d < 85650 GROUP BY a`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a > 5347 AND d is null GROUP BY a`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a < 69276 AND d < 72925 GROUP BY a`,
		`SELECT a, sum(b), avg(d) FROM t WHERE k is null AND b < 44762 GROUP BY a`,
		`SELECT a, sum(b), count(d) FROM t WHERE a = 40075 GROUP BY a`,
		`SELECT a, sum(b), count(d) FROM t WHERE a>=24838 and a<=31763 GROUP BY a`,
		`SELECT a, sum(b), count(d) FROM t WHERE a < 13834 AND d>=60723 and d<=1645 GROUP BY a`,
		`SELECT a, sum(b), count(d) FROM t WHERE k = 99018 AND b = 87451 GROUP BY a`,
		`SELECT a, sum(b), count(d) FROM t WHERE k is null AND b = 75924 GROUP BY a`,
		`SELECT a, sum(b), count(d) FROM t WHERE k is null AND b < 28775 GROUP BY a`,
		`SELECT a, sum(b), count(d) FROM t WHERE k>=45446 and k<=57981 AND b in (7129, 42941, 20115) GROUP BY a`,
		`SELECT a, avg(b), sum(d) FROM t WHERE k = 50825 AND b is null GROUP BY a`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a = 41298 AND d < 36937 GROUP BY a`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a > 23184 AND d is null GROUP BY a`,
		`SELECT a, avg(b), avg(d) FROM t WHERE k in (41049, 61733, 74407) AND b is null GROUP BY a`,
		`SELECT a, avg(b), avg(d) FROM t WHERE k>=29858 and k<=5339 AND b = 55985 GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE a is null AND d > 50159 GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE a > 77364 AND d in (37426, 16633, 79522) GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE a < 69545 AND d>=4791 and d<=3019 GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE k = 53006 AND b > 25003 GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE k = 88950 AND b < 21108 GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE k is null AND b in (64431, 63288, 96777) GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE k in (6109, 87863, 89475) AND b>=73295 and b<=53543 GROUP BY a`,
		`SELECT a, count(b), sum(d) FROM t WHERE a in (50224, 814, 46931) GROUP BY a`,
		`SELECT a, count(b), sum(d) FROM t WHERE d in (40743, 1342, 29680) GROUP BY a`,
		`SELECT a, count(b), avg(d) FROM t WHERE a is null AND d < 39836 GROUP BY a`,
		`SELECT a, count(b), avg(d) FROM t WHERE a > 76270 AND d < 35260 GROUP BY a`,
		`SELECT a, count(b), avg(d) FROM t WHERE k = 54401 AND b < 32495 GROUP BY a`,
		`SELECT a, count(b), avg(d) FROM t WHERE k in (36621, 48235, 11138) AND b = 35343 GROUP BY a`,
		`SELECT a, count(b), avg(d) FROM t WHERE k in (72741, 70696, 87096) AND b > 71057 GROUP BY a`,
		`SELECT a, count(b), avg(d) FROM t WHERE k>=26161 and k<=62715 AND b < 28692 GROUP BY a`,
		`SELECT a, count(b), count(d) FROM t WHERE a = 61125 AND d = 74036 GROUP BY a`,
		`SELECT a, count(b), count(d) FROM t WHERE a is null AND d < 99907 GROUP BY a`,
		`SELECT a, count(b), count(d) FROM t WHERE a < 17273 AND d>=61586 and d<=80771 GROUP BY a`,
		`SELECT a, count(b), count(d) FROM t WHERE k = 66030 AND b > 78141 GROUP BY a`,
		`SELECT a, count(b), count(d) FROM t WHERE k > 3442 AND b > 29855 GROUP BY a`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a > 56534 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a>=44013 and a<=73513 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a = 78818 AND d > 55065 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a in (46621, 10316, 174) AND d in (82462, 19114, 32537) GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a < 57773 AND d is null GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a>=43624 and a<=46546 AND d in (6690, 63739, 86097) GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), sum(d) FROM t WHERE k is null AND b < 31691 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), sum(d) FROM t WHERE k in (6245, 22493, 86056) AND b is null GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), avg(d) FROM t WHERE b is null GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), avg(d) FROM t WHERE b is null GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a = 24680 AND d = 97829 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a = 87901 AND d < 34460 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a = 68318 AND d>=34393 and d<=2089 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a > 80949 AND d is null GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), avg(d) FROM t WHERE k = 11100 AND b>=52798 and b<=10586 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), avg(d) FROM t WHERE k is null AND b in (74950, 30896, 47626) GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), avg(d) FROM t WHERE k in (63639, 5023, 14713) AND b = 10188 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), avg(d) FROM t WHERE k in (95650, 50612, 1021) AND b > 72317 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), avg(d) FROM t WHERE k in (10995, 14763, 2872) AND b>=78497 and b<=47931 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), avg(d) FROM t WHERE k>=18536 and k<=93859 AND b = 82179 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), count(d) FROM t WHERE b>=42779 and b<=76415 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), count(d) FROM t WHERE a < 58687 AND d > 50961 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), count(d) FROM t WHERE k = 15047 AND b > 96233 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), count(d) FROM t WHERE k in (4914, 98160, 7769) AND b = 63421 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), count(d) FROM t WHERE k < 15623 AND b is null GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), count(d) FROM t WHERE k < 58846 AND b>=8417 and b<=20559 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), count(d) FROM t WHERE k>=43683 and k<=88041 AND b = 8314 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), sum(d) FROM t WHERE a in (96595, 13017, 91993) GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), sum(d) FROM t WHERE a > 13866 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), sum(d) FROM t WHERE a = 68362 AND d > 58300 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), sum(d) FROM t WHERE a < 24957 AND d is null GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), sum(d) FROM t WHERE k = 97871 AND b < 71292 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), sum(d) FROM t WHERE k is null AND b > 28251 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), sum(d) FROM t WHERE k is null AND b < 88260 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), sum(d) FROM t WHERE k in (27478, 46616, 58608) AND b in (16937, 34625, 46811) GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), sum(d) FROM t WHERE k>=76245 and k<=5651 AND b in (35857, 57023, 24886) GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a in (80560, 19804, 17052) GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), avg(d) FROM t WHERE d > 45855 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a is null AND d = 1518 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a is null AND d > 67987 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a is null AND d>=77896 and d<=5643 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a in (22688, 31401, 82728) AND d in (98019, 52205, 55770) GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a in (68847, 94184, 76599) AND d in (67580, 84081, 93073) GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a < 87888 AND d>=32614 and d<=18763 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), avg(d) FROM t WHERE k < 1808 AND b > 39287 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), count(d) FROM t WHERE a = 34719 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), count(d) FROM t WHERE a in (62859, 88488, 59791) GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), count(d) FROM t WHERE a < 13709 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), count(d) FROM t WHERE a is null AND d > 79915 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), count(d) FROM t WHERE a is null AND d>=38462 and d<=71304 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), count(d) FROM t WHERE a in (29058, 77234, 95017) AND d > 27707 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), count(d) FROM t WHERE a>=7011 and a<=3910 AND d>=35438 and d<=98383 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), count(d) FROM t WHERE k = 60461 AND b < 18192 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), count(d) FROM t WHERE k is null AND b = 25155 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), sum(d) FROM t WHERE a < 95714 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), sum(d) FROM t WHERE b is null GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), sum(d) FROM t WHERE b > 41169 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), sum(d) FROM t WHERE k = 73295 AND b in (53840, 94807, 7869) GROUP BY a LIMIT 10`,
		`SELECT a, count(b), sum(d) FROM t WHERE k is null AND b < 85824 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), sum(d) FROM t WHERE k in (2640, 73371, 64567) AND b < 66312 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), sum(d) FROM t WHERE k > 89101 AND b in (45727, 59411, 38014) GROUP BY a LIMIT 10`,
		`SELECT a, count(b), avg(d) FROM t WHERE a = 66628 AND d is null GROUP BY a LIMIT 10`,
		`SELECT a, count(b), avg(d) FROM t WHERE a = 90037 AND d in (9738, 70720, 719) GROUP BY a LIMIT 10`,
		`SELECT a, count(b), avg(d) FROM t WHERE a < 7981 AND d is null GROUP BY a LIMIT 10`,
		`SELECT a, count(b), avg(d) FROM t WHERE a < 63025 AND d < 51511 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), avg(d) FROM t WHERE a>=81254 and a<=37014 AND d = 92168 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), avg(d) FROM t WHERE k in (5027, 52600, 67111) AND b = 7797 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), avg(d) FROM t WHERE k < 27980 AND b < 75184 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), avg(d) FROM t WHERE k>=1988 and k<=12394 AND b > 20912 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE a is null GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE a in (1525, 76503, 35042) GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE a > 91116 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE b is null GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE d > 34273 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE a is null AND d in (66924, 58678, 4003) GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE a in (38419, 7040, 22743) AND d < 44234 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE a > 4660 AND d>=88313 and d<=47242 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE a < 43478 AND d>=93445 and d<=6877 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE k = 24701 AND b > 97459 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE k = 29309 AND b < 26036 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE k < 48000 AND b is null GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE k < 24868 AND b > 41502 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE k>=15022 and k<=93122 AND b in (95599, 66247, 61048) GROUP BY a LIMIT 10, 100`,
		`SELECT k, sum(b), sum(d) FROM t WHERE b in (51268, 70191, 42012) GROUP BY k`,
		`SELECT k, sum(b), sum(d) FROM t WHERE a is null AND d > 45399 GROUP BY k`,
		`SELECT a, sum(b), avg(d) FROM t WHERE b is null GROUP BY a`,
		`SELECT a, sum(b), avg(d) FROM t WHERE d in (2248, 83296, 35076) GROUP BY a`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a = 70230 AND d = 77678 GROUP BY a`,
		`SELECT a, sum(b), count(d) FROM t WHERE a>=36138 and a<=8962 AND d > 94937 GROUP BY a`,
		`SELECT a, avg(b), sum(d) FROM t WHERE a is null AND d < 68643 GROUP BY a`,
		`SELECT a, avg(b), sum(d) FROM t WHERE a is null AND d>=17339 and d<=91815 GROUP BY a`,
		`SELECT a, avg(b), avg(d) FROM t WHERE k > 41787 AND b = 85708 GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE a>=72233 and a<=82430 AND d < 74782 GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE k in (43456, 14938, 2836) AND b>=59014 and b<=42379 GROUP BY a`,
		`SELECT a, avg(b), count(d) FROM t WHERE k < 94524 AND b is null GROUP BY a`,
		`SELECT a, count(b), sum(d) FROM t WHERE b < 32152 GROUP BY a`,
		`SELECT a, count(b), sum(d) FROM t WHERE k > 87526 AND b>=66020 and b<=33990 GROUP BY a`,
		`SELECT a, count(b), avg(d) FROM t WHERE a = 88701 AND d in (57344, 28960, 75810) GROUP BY a`,
		`SELECT a, count(b), avg(d) FROM t WHERE k is null AND b>=39956 and b<=79330 GROUP BY a`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a = 69824 AND d in (36759, 70922, 68442) GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a is null AND d = 60609 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), sum(d) FROM t WHERE a in (69392, 44280, 87211) AND d>=41827 and d<=70058 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a = 35212 AND d = 90924 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a in (75569, 78024, 91137) AND d>=77000 and d<=34670 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), avg(d) FROM t WHERE a > 1677 AND d > 21208 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), avg(d) FROM t WHERE k < 69851 AND b < 82123 GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), count(d) FROM t WHERE a in (4748, 15710, 23059) AND d < 5230 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), count(d) FROM t WHERE a > 9392 AND d>=96304 and d<=83400 GROUP BY a LIMIT 10`,
		`SELECT a, sum(b), count(d) FROM t WHERE k is null AND b is null GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), count(d) FROM t WHERE k in (90954, 35072, 92005) AND b in (20382, 42836, 49946) GROUP BY a LIMIT 10, 100`,
		`SELECT a, sum(b), count(d) FROM t WHERE k in (22872, 75041, 43211) AND b > 97537 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), sum(d) FROM t WHERE a = 15276 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), sum(d) FROM t WHERE k = 93243 AND b > 49775 GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), sum(d) FROM t WHERE k = 5807 AND b>=59830 and b<=3705 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), avg(d) FROM t WHERE b > 56235 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), avg(d) FROM t WHERE d < 16573 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a = 51995 AND d is null GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a > 60915 AND d is null GROUP BY a LIMIT 10`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a < 21665 AND d is null GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), avg(d) FROM t WHERE a>=54752 and a<=2685 AND d in (24217, 38438, 62159) GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), count(d) FROM t WHERE a is null AND d > 26828 GROUP BY a LIMIT 10, 100`,
		`SELECT a, avg(b), count(d) FROM t WHERE a in (4773, 38292, 31296) AND d < 4798 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), sum(d) FROM t WHERE a is null AND d is null GROUP BY a LIMIT 10`,
		`SELECT a, count(b), sum(d) FROM t WHERE a is null AND d < 42510 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), sum(d) FROM t WHERE a in (96124, 65999, 40505) AND d = 57305 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), sum(d) FROM t WHERE a < 91075 AND d < 27748 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), sum(d) FROM t WHERE a>=41070 and a<=63441 AND d>=92748 and d<=54159 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), sum(d) FROM t WHERE k < 4598 AND b in (87638, 84310, 74328) GROUP BY a LIMIT 10`,
		`SELECT a, count(b), avg(d) FROM t WHERE a > 2202 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), avg(d) FROM t WHERE b in (87832, 93783, 49601) GROUP BY a LIMIT 10`,
		`SELECT a, count(b), avg(d) FROM t WHERE b>=30722 and b<=77947 GROUP BY a LIMIT 10`,
		`SELECT a, count(b), count(d) FROM t WHERE a > 26825 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE a > 39963 AND d = 80701 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE a < 90170 AND d>=52364 and d<=20837 GROUP BY a LIMIT 10, 100`,
		`SELECT a, count(b), count(d) FROM t WHERE k > 26370 AND b is null GROUP BY a LIMIT 10`,
		`SELECT k, sum(b), sum(d) FROM t WHERE a > 67503 GROUP BY k`,
		`SELECT k, sum(b), sum(d) FROM t WHERE a>=78526 and a<=16653 AND d is null GROUP BY k`,
		`SELECT k, sum(b), sum(d) FROM t WHERE k = 88111 AND b = 66673 GROUP BY k`,
		`SELECT k, sum(b), sum(d) FROM t WHERE k < 81018 AND b > 23525 GROUP BY k`,
		`SELECT k, sum(b), avg(d) FROM t WHERE b > 12679 GROUP BY k`,
		`SELECT k, sum(b), avg(d) FROM t WHERE a>=25103 and a<=42298 AND d = 78559 GROUP BY k`,
		`SELECT k, sum(b), avg(d) FROM t WHERE k > 37175 AND b > 41914 GROUP BY k`,
		`SELECT k, sum(b), avg(d) FROM t WHERE k>=91640 and k<=69886 AND b in (7549, 93298, 63753) GROUP BY k`,
		`SELECT k, sum(b), count(d) FROM t WHERE a < 91940 AND d in (86848, 16004, 44675) GROUP BY k`,
		`SELECT k, avg(b), sum(d) FROM t WHERE k is null AND b > 68997 GROUP BY k`,
		`SELECT k, avg(b), sum(d) FROM t WHERE k>=8105 and k<=83268 AND b < 6866 GROUP BY k`,
		`SELECT k, avg(b), count(d) FROM t WHERE a = 34808 AND d in (75160, 26300, 54672) GROUP BY k`,
		`SELECT k, avg(b), count(d) FROM t WHERE a is null AND d = 15817 GROUP BY k`,
		`SELECT k, avg(b), count(d) FROM t WHERE k < 79505 AND b > 84884 GROUP BY k`,
		`SELECT k, count(b), sum(d) FROM t WHERE a > 6445 AND d is null GROUP BY k`,
		`SELECT k, count(b), avg(d) FROM t WHERE k is null AND b < 56969 GROUP BY k`,
		`SELECT k, count(b), count(d) FROM t WHERE k in (26798, 16685, 50665) AND b < 75359 GROUP BY k`,
		`SELECT k, sum(b), sum(d) FROM t WHERE a in (95385, 84836, 4169) AND d is null GROUP BY k LIMIT 10`,
		`SELECT k, sum(b), avg(d) FROM t WHERE k > 33994 AND b = 28539 GROUP BY k LIMIT 10, 100`,
		`SELECT k, sum(b), avg(d) FROM t WHERE k>=30848 and k<=40961 AND b = 66265 GROUP BY k LIMIT 10`,
		`SELECT k, sum(b), avg(d) FROM t WHERE k>=7658 and k<=93561 AND b>=26544 and b<=62174 GROUP BY k LIMIT 10, 100`,
		`SELECT k, sum(b), count(d) FROM t WHERE a is null AND d in (80955, 9464, 88737) GROUP BY k LIMIT 10, 100`,
		`SELECT k, sum(b), count(d) FROM t WHERE a in (56523, 17684, 74240) AND d is null GROUP BY k LIMIT 10`,
		`SELECT k, sum(b), count(d) FROM t WHERE k>=87085 and k<=17995 AND b > 95778 GROUP BY k LIMIT 10, 100`,
		`SELECT k, avg(b), sum(d) FROM t WHERE a is null GROUP BY k LIMIT 10, 100`,
		`SELECT k, avg(b), sum(d) FROM t WHERE a = 2422 AND d = 92159 GROUP BY k LIMIT 10, 100`,
		`SELECT k, avg(b), sum(d) FROM t WHERE a in (44669, 98509, 52972) AND d in (80339, 63761, 27757) GROUP BY k LIMIT 10`,
		`SELECT k, avg(b), sum(d) FROM t WHERE a > 72997 AND d>=54536 and d<=39707 GROUP BY k LIMIT 10`,
		`SELECT k, avg(b), sum(d) FROM t WHERE a>=47446 and a<=59380 AND d < 26403 GROUP BY k LIMIT 10`,
		`SELECT k, avg(b), avg(d) FROM t WHERE a = 3584 AND d>=27957 and d<=16239 GROUP BY k LIMIT 10, 100`,
		`SELECT k, avg(b), avg(d) FROM t WHERE a > 52521 AND d>=30079 and d<=19862 GROUP BY k LIMIT 10`,
		`SELECT k, avg(b), avg(d) FROM t WHERE k > 14619 AND b>=18083 and b<=74295 GROUP BY k LIMIT 10, 100`,
		`SELECT k, count(b), sum(d) FROM t WHERE a = 94171 AND d > 14044 GROUP BY k LIMIT 10, 100`,
		`SELECT k, count(b), sum(d) FROM t WHERE a is null AND d = 37740 GROUP BY k LIMIT 10`,
		`SELECT k, count(b), sum(d) FROM t WHERE a in (232, 82512, 20686) AND d is null GROUP BY k LIMIT 10, 100`,
		`SELECT k, count(b), sum(d) FROM t WHERE a > 60419 AND d is null GROUP BY k LIMIT 10, 100`,
		`SELECT k, count(b), avg(d) FROM t WHERE a = 11732 AND d is null GROUP BY k LIMIT 10`,
		`SELECT k, count(b), avg(d) FROM t WHERE a is null AND d in (12615, 3757, 21416) GROUP BY k LIMIT 10`,
		`SELECT k, count(b), avg(d) FROM t WHERE a>=10973 and a<=69926 AND d is null GROUP BY k LIMIT 10`,
		`SELECT k, count(b), count(d) FROM t WHERE b < 65294 GROUP BY k LIMIT 10`,
		`SELECT k, count(b), count(d) FROM t WHERE a in (48344, 81678, 42854) AND d is null GROUP BY k LIMIT 10`,
		`SELECT k, count(b), count(d) FROM t WHERE k in (70584, 56374, 25542) AND b is null GROUP BY k LIMIT 10`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.k > t2.k WHERE t1.k = 7526 AND t1.b in (70283, 43219, 21555)`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.k > t2.k WHERE t1.k > 48601 AND t1.b in (23462, 61614, 38590)`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b > t2.b WHERE t1.d is null`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b > t2.b WHERE t1.a in (59932, 19444, 80475) AND t1.d in (2627, 92948, 29045)`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b > t2.b WHERE t1.a>=94521 and t1.a<=88167 AND t1.d > 47907`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.b = 21343`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k in (11013, 22381, 40966) AND t1.b < 92416`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k < 31613 AND t1.b>=83744 and t1.b<=10894`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.k > t2.k WHERE t1.a < 72776 AND t1.d is null`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b > t2.b WHERE t1.a > 89310 AND t1.d>=67592 and t1.d<=45360`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.a>=83081 and t1.a<=20997 AND t1.d = 12`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k in (64229, 44754, 59520) AND t1.b>=49588 and t1.b<=47719`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.a = t2.a WHERE t1.k>=88198 and t1.k<=99573 AND t1.b = 24468`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.a < 73392 AND t1.d < 17973`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.a>=96609 and t1.a<=90137 AND t1.d is null`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.k = 90202 AND t1.b < 64497`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b > t2.b WHERE t1.k is null AND t1.b < 43676`,
		`SELECT /*+ MERGE_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k > 98461 AND t1.b = 40415`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a = t2.a WHERE t1.a is null AND t1.d > 31322`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a = t2.a WHERE t1.k is null AND t1.b>=90899 and t1.b<=64406`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b > t2.b WHERE t1.a = 4324`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b > t2.b WHERE t1.k < 19660 AND t1.b > 98631`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.a = 43284 AND t1.d > 64825`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.a is null AND t1.d is null`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k is null AND t1.b is null`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.a = t2.a WHERE t1.a < 77165 AND t1.d > 15170`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.k > t2.k WHERE t1.a = 95241 AND t1.d>=39952 and t1.d<=41869`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.k > t2.k WHERE t1.a in (27777, 63970, 63208) AND t1.d = 71715`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b > t2.b WHERE t1.a>=45730 and t1.a<=54009 AND t1.d > 91552`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b > t2.b WHERE t1.k in (2343, 70489, 83886) AND t1.b > 69`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k is null AND t1.b < 8746`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.a = t2.a WHERE t1.a is null AND t1.d < 27677`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.a < 20902 AND t1.d < 50495`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.a>=47261 and t1.a<=45326 AND t1.d in (37280, 28990, 47739)`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.k = 62689 AND t1.b = 54884`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.k = 76465 AND t1.b > 30247`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b > t2.b WHERE t1.b < 34810`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b > t2.b WHERE t1.a = 38313 AND t1.d in (56687, 48892, 74221)`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b > t2.b WHERE t1.a in (83245, 35118, 95233) AND t1.d is null`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b > t2.b WHERE t1.k in (39338, 68722, 27052) AND t1.b < 49416`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.d < 1801`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.a>=54747 and t1.a<=88139 AND t1.d < 88883`,
		`SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k>=81863 and t1.k<=60855 AND t1.b in (41955, 54589, 43237)`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a = t2.a WHERE t1.k is null AND t1.b>=49906 and t1.b<=35620`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a = t2.a WHERE t1.k in (17077, 68978, 55677) AND t1.b is null`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b > t2.b WHERE t1.b > 8224`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k = 39083 AND t1.b < 71057`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.k < 71444 AND t1.b>=65385 and t1.b<=38435`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.a = t2.a WHERE t1.a = 89065 AND t1.d is null`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.a = t2.a WHERE t1.a in (54336, 54990, 89865) AND t1.d = 36896`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b > t2.b WHERE t1.k is null AND t1.b = 43603`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.a = 11784`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.b is null`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 LEFT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.a > 42704 AND t1.d > 62554`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.a = t2.a WHERE t1.a > 17133 AND t1.d>=17038 and t1.d<=74898`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.b < 92074`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.a > 87289 AND t1.d in (27730, 8946, 65527)`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.a < 41413 AND t1.d > 79707`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.k < 38024 AND t1.b is null`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.k > t2.k WHERE t1.k>=18888 and t1.k<=37329 AND t1.b = 11879`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b > t2.b WHERE t1.b > 30513`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b > t2.b WHERE t1.a = 30054 AND t1.d = 17561`,
		`SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t t1 RIGHT JOIN t t2 ON t1.b = t2.b AND t1.c = t2.c WHERE t1.a is null AND t1.d in (53427, 38405, 91931)`,
	}

	for _, q := range queries {
		checkCost(t, tk, q, "")
	}
}
