// Copyright 2026 PingCAP, Inc.
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

	queries := []string{
		"SELECT b * FLOOR(2 * RAND(177)) AS e, COUNT(d) AS cnt FROM t GROUP BY e ORDER BY e, cnt",
		"SELECT b * FLOOR(2 * RAND(177)) AS e, COUNT(d) AS cnt FROM t GROUP BY 1 ORDER BY e, cnt",
	}
	for _, query := range queries {
		tk.MustQuery(query).Check(testkit.Rows("0 3", "1 1"))

		plan := tk.MustQuery("EXPLAIN FORMAT='brief' " + query).String()
		lowerPlan := strings.ToLower(plan)
		randPos := strings.Index(lowerPlan, "rand(177)")
		hashAggPos := strings.Index(plan, "HashAgg")
		if count := strings.Count(lowerPlan, "rand(177)"); count != 1 || hashAggPos < 0 || randPos < hashAggPos {
			t.Fatalf("expected RAND(177) to be evaluated once below HashAgg for %s, found plan:\n%s", query, plan)
		}
	}
}
