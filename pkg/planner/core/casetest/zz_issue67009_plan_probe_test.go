package casetest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIssue67009PlanProbe(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("create table t_issue67009(c0 char unique)")
		tk.MustExec("insert into t_issue67009 values (1)")
		for _, sql := range []string{
			"explain format='brief' select /* issue:67009 boundary */ max(c0) from t_issue67009 group by c0",
			"explain format='brief' select /* issue:67009 constant */ max(42) from t_issue67009 group by c0",
		} {
			t.Log(cascades, sql)
			for _, row := range tk.MustQuery(sql).Rows() {
				t.Logf("%q", row)
			}
		}
	})
}
