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
		"select * from t use index(primary)", // table-scan
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
