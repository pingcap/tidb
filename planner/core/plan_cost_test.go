package core_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func explainQuery(tk *testkit.TestKit, q string) (result string) {
	//| id | estRows | estCost   | task | access object | operator info |
	rs := tk.MustQuery("explain format=verbose " + q).Rows()
	for _, r := range rs {
		result = result + fmt.Sprintf("%v\n", r)
	}
	return
}

func TestNewCostInterface(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, d int, key b(b), key cd(c, d))`)
	var vals []string
	for i := 0; i < 1500; i++ {
		vals = append(vals, fmt.Sprintf(`(%v, %v, %v, %v)`, i, i, i, i))
		if len(vals) >= 100 {
			tk.MustExec(fmt.Sprintf(`insert into t values %v`, strings.Join(vals, ", ")))
			vals = vals[:0]
		}
	}
	tk.MustExec(`analyze table t`)
	tk.MustExec(`set @@session.tidb_stats_load_sync_wait=2000`)

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
		// TODO: index-merge
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
		"select /*+ hash_agg() */ max(a) from t use index(primary) where a < 200",
		"select /*+ hash_agg() */ avg(a), b from t use index(primary) where a < 200 group by b",
		"select /*+ stream_agg() */ count(*) from t use index(primary) where a < 200",
		"select /*+ stream_agg() */ sum(a) from t use index(primary) where a < 200",
		"select /*+ stream_agg() */ max(a) from t use index(primary) where a < 200",
		"select /*+ stream_agg() */ avg(a), b from t use index(primary) where a < 200 group by b",
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
		"select * from t t1 where t1.b in (select sum(t2.b) from t t2 where t1.a < t2.a)", // apply
		// point get
		"select * from t where a = 1",
		"select * from t where a in (1, 2, 3, 4, 5)",
		// mpp plans
		// rand-gen queries
	}

	for _, q := range queries {
		tk.MustExec(`set @@tidb_enable_new_cost_interface=0`)
		oldResult := explainQuery(tk, q)
		tk.MustExec(`set @@tidb_enable_new_cost_interface=1`)
		newResult := explainQuery(tk, q)
		if oldResult != newResult {
			t.Fatalf(`run %v failed, expected \n%v\n, but got \n%v\n`, q, oldResult, newResult)
		}
	}
}
