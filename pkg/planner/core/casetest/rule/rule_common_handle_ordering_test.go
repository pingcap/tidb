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

package rule

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// explainHas scans EXPLAIN output rows and returns whether any line contains
// the given substring.
func explainHas(rows [][]any, substr string) bool {
	for _, row := range rows {
		for _, col := range row {
			if s, ok := col.(string); ok && strings.Contains(s, substr) {
				return true
			}
		}
	}
	return false
}

func TestCommonHandleIndexOrdering(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_ch")
		tk.MustExec(`CREATE TABLE t_ch (
			a1 varchar(64),
			a2 int,
			b int,
			c int,
			PRIMARY KEY(a1, a2) CLUSTERED,
			KEY ic(c),
			KEY ic_overlap(c, a1),
			KEY ic_multi(b, c),
			UNIQUE KEY uic(c)
		)`)
		tk.MustExec("insert into t_ch values ('x', 1, 10, 1), ('y', 2, 20, 2), ('a', 3, 30, 3)")

		// ---------------------------------------------------------------
		// Case 1: Basic — single-column secondary index, ORDER BY full PK.
		// The optimizer should recognise that ic stores (c, a1, a2) and
		// satisfy ORDER BY a1, a2 with keep order:true (no TopN).
		// ---------------------------------------------------------------
		rows := tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic) where c = 1 order by a1, a2 limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 1: expected keep order:true for basic CommonHandle ordering")
		require.False(t, explainHas(rows, "TopN"),
			"case 1: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_ch use index(ic) where c = 1 order by a1, a2 limit 100",
		).Check(testkit.Rows("x 1 10 1"))

		// ---------------------------------------------------------------
		// Case 2: Partial PK overlap — index ic_overlap(c, a1) already
		// contains PK column a1. Only a2 should be appended. ORDER BY
		// a1, a2 should still use keep order:true.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic_overlap) where c = 1 order by a1, a2 limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 2: expected keep order:true with partial PK overlap index")
		require.False(t, explainHas(rows, "TopN"),
			"case 2: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_ch use index(ic_overlap) where c = 1 order by a1, a2 limit 100",
		).Check(testkit.Rows("x 1 10 1"))

		// ---------------------------------------------------------------
		// Case 3: Multi-column secondary index — ic_multi(b, c) with all
		// access conditions satisfied, ORDER BY PK should use keep order.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic_multi) where b = 10 and c = 1 order by a1, a2 limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 3: expected keep order:true for multi-column secondary index")
		require.False(t, explainHas(rows, "TopN"),
			"case 3: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_ch use index(ic_multi) where b = 10 and c = 1 order by a1, a2 limit 100",
		).Check(testkit.Rows("x 1 10 1"))

		// ---------------------------------------------------------------
		// Case 4: Unique index — PK columns should NOT be appended to a
		// unique secondary index, so ORDER BY PK cannot use keep order.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(uic) where c > 0 order by a1, a2 limit 100",
		).Rows()
		// For a unique index with a range scan, the optimizer should not
		// be able to satisfy ORDER BY PK via keep order on the index.
		hasKeepOrderTrue := explainHas(rows, "keep order:true")
		hasTopN := explainHas(rows, "TopN")
		require.True(t, !hasKeepOrderTrue || hasTopN,
			"case 4: unique index should not satisfy ORDER BY PK without sort")

		// ---------------------------------------------------------------
		// Case 5: DESC ordering — the index scan should satisfy
		// ORDER BY a1 DESC, a2 DESC via a reverse scan.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic) where c = 1 order by a1 desc, a2 desc limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 5: expected keep order:true for DESC ordering")
		require.False(t, explainHas(rows, "TopN"),
			"case 5: unexpected TopN sort for DESC ordering")

		tk.MustQuery(
			"select * from t_ch use index(ic) where c = 1 order by a1 desc, a2 desc limit 100",
		).Check(testkit.Rows("x 1 10 1"))

		// ---------------------------------------------------------------
		// Case 6: Mixed ASC/DESC — ORDER BY a1 ASC, a2 DESC cannot be
		// satisfied by the index since all PK columns share one direction.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic) where c = 1 order by a1 asc, a2 desc limit 100",
		).Rows()
		hasKeepOrderTrue = explainHas(rows, "keep order:true")
		hasTopN = explainHas(rows, "TopN")
		require.True(t, !hasKeepOrderTrue || hasTopN,
			"case 6: mixed ASC/DESC should not be satisfied by keep order alone")
	})
}
