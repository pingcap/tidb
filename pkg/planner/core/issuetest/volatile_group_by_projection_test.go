// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0.

package issuetest

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestVolatileGroupByProjection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("SET @@session.tidb_allow_mpp = 0")
	tk.MustExec("SET @@session.tidb_enable_cascades_planner = 0")
	tk.MustExec("CREATE TABLE t (a INT, b INT, c INT, d INT)")
	tk.MustExec("INSERT INTO t VALUES (1,1,1,10), (2,1,2,2), (3,1,1,10), (4,1,2,2)")

	const query = "SELECT b * FLOOR(2 * RAND(177)) AS e, COUNT(d) AS cnt FROM t GROUP BY e ORDER BY e, cnt"
	tk.MustQuery(query).Check(testkit.Rows("0 3", "1 1"))

	plan := tk.MustQuery("EXPLAIN FORMAT='brief' " + query).String()
	lowerPlan := strings.ToLower(plan)
	randPos := strings.Index(lowerPlan, "rand(177)")
	hashAggPos := strings.Index(plan, "HashAgg")
	if count := strings.Count(lowerPlan, "rand(177)"); count != 1 || hashAggPos < 0 || randPos < hashAggPos {
		t.Fatalf("expected RAND(177) to be evaluated once below HashAgg, found plan:\n%s", plan)
	}
}
