package core_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestNewCostInterface(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, d int, key b(b), key cd(c, d)`)
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


}
