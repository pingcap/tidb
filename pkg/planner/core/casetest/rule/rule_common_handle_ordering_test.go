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
			d int,
			PRIMARY KEY(a1, a2) CLUSTERED,
			KEY ic(d),
			KEY ic_overlap(d, a1),
			KEY ic_multi(b, d),
			UNIQUE KEY uic(c)
		)`)
		// Multiple rows share d=0 and b=10,d=0 so ORDER BY assertions are meaningful.
		// Each row has a distinct c for the UNIQUE KEY uic(c).
		tk.MustExec(`insert into t_ch values
			('x', 1, 10, 1, 0),
			('y', 2, 20, 2, 0),
			('a', 3, 10, 3, 0),
			('m', 4, 30, 4, 1)`)

		// ---------------------------------------------------------------
		// Case 1: Basic — single-column secondary index, ORDER BY full PK.
		// The optimizer should recognise that ic stores (d, a1, a2) and
		// satisfy ORDER BY a1, a2 with keep order:true (no TopN).
		// ---------------------------------------------------------------
		rows := tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic) where d = 0 order by a1, a2 limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 1: expected keep order:true for basic CommonHandle ordering")
		require.False(t, explainHas(rows, "TopN"),
			"case 1: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_ch use index(ic) where d = 0 order by a1, a2 limit 100",
		).Check(testkit.Rows(
			"a 3 10 3 0",
			"x 1 10 1 0",
			"y 2 20 2 0",
		))

		// ---------------------------------------------------------------
		// Case 2: Partial PK overlap — index ic_overlap(d, a1) already
		// contains PK column a1. Only a2 should be appended. ORDER BY
		// a1, a2 should still use keep order:true.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic_overlap) where d = 0 order by a1, a2 limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 2: expected keep order:true with partial PK overlap index")
		require.False(t, explainHas(rows, "TopN"),
			"case 2: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_ch use index(ic_overlap) where d = 0 order by a1, a2 limit 100",
		).Check(testkit.Rows(
			"a 3 10 3 0",
			"x 1 10 1 0",
			"y 2 20 2 0",
		))

		// ---------------------------------------------------------------
		// Case 3: Multi-column secondary index — ic_multi(b, d) with all
		// access conditions satisfied, ORDER BY PK should use keep order.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic_multi) where b = 10 and d = 0 order by a1, a2 limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 3: expected keep order:true for multi-column secondary index")
		require.False(t, explainHas(rows, "TopN"),
			"case 3: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_ch use index(ic_multi) where b = 10 and d = 0 order by a1, a2 limit 100",
		).Check(testkit.Rows(
			"a 3 10 3 0",
			"x 1 10 1 0",
		))

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
		hasSortOp := explainHas(rows, "TopN") || explainHas(rows, "Sort")
		require.True(t, !hasKeepOrderTrue || hasSortOp,
			"case 4: unique index should not satisfy ORDER BY PK without sort")

		// ---------------------------------------------------------------
		// Case 5: DESC ordering — the index scan should satisfy
		// ORDER BY a1 DESC, a2 DESC via a reverse scan.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic) where d = 0 order by a1 desc, a2 desc limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 5: expected keep order:true for DESC ordering")
		require.False(t, explainHas(rows, "TopN"),
			"case 5: unexpected TopN sort for DESC ordering")

		tk.MustQuery(
			"select * from t_ch use index(ic) where d = 0 order by a1 desc, a2 desc limit 100",
		).Check(testkit.Rows(
			"y 2 20 2 0",
			"x 1 10 1 0",
			"a 3 10 3 0",
		))

		// ---------------------------------------------------------------
		// Case 6: Mixed ASC/DESC — ORDER BY a1 ASC, a2 DESC cannot be
		// satisfied by the index since all PK columns share one direction.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_ch use index(ic) where d = 0 order by a1 asc, a2 desc limit 100",
		).Rows()
		hasKeepOrderTrue = explainHas(rows, "keep order:true")
		hasSortOp = explainHas(rows, "TopN") || explainHas(rows, "Sort")
		require.True(t, !hasKeepOrderTrue || hasSortOp,
			"case 6: mixed ASC/DESC should not be satisfied by keep order alone")

		// ---------------------------------------------------------------
		// Case 7: Prefixed clustered PK — PRIMARY KEY(p1(2), p2) stores
		// only the first 2 bytes of p1 in the handle. ORDER BY on the
		// full p1, p2 columns must NOT use keep order:true because the
		// physical sort key is by the truncated prefix, not the full value.
		// ---------------------------------------------------------------
		tk.MustExec("drop table if exists t_prefix")
		tk.MustExec(`CREATE TABLE t_prefix (
			p1 varchar(64),
			p2 int,
			c int,
			PRIMARY KEY(p1(2), p2) CLUSTERED,
			KEY ic_p(c)
		)`)
		// ('abz',1) and ('abc',2) share the 2-char prefix 'ab', so handle
		// sort is by (prefix,p2): ('ab',1)=abz < ('ab',2)=abc < ('ax',3)=axy.
		// Full ORDER BY p1,p2 gives: abc < abz < axy — a different order.
		tk.MustExec(`insert into t_prefix values ('abz', 1, 0), ('abc', 2, 0), ('axy', 3, 0)`)
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_prefix use index(ic_p) where c = 0 order by p1, p2 limit 100",
		).Rows()
		hasKeepOrderTrue = explainHas(rows, "keep order:true")
		hasSortOp = explainHas(rows, "TopN") || explainHas(rows, "Sort")
		// The prefixed PK column cannot satisfy ORDER BY on the full column,
		// so either keep order should be false or a TopN/Sort must be present.
		require.True(t, !hasKeepOrderTrue || hasSortOp,
			"case 7: prefixed clustered PK should not satisfy ORDER BY without sort")

		tk.MustQuery(
			"select * from t_prefix use index(ic_p) where c = 0 order by p1, p2 limit 100",
		).Check(testkit.Rows(
			"abc 2 0",
			"abz 1 0",
			"axy 3 0",
		))

		// ---------------------------------------------------------------
		// Case 8: Single varchar clustered PK — a single non-integer column
		// PK uses CommonHandle. The secondary index stores (idx_col, pk_col)
		// and should satisfy ORDER BY on the PK.
		// ---------------------------------------------------------------
		tk.MustExec("drop table if exists t_varchar_pk")
		tk.MustExec(`CREATE TABLE t_varchar_pk (
			pk varchar(64) PRIMARY KEY CLUSTERED,
			c int,
			d int,
			KEY ic_vc(c)
		)`)
		tk.MustExec(`insert into t_varchar_pk values
			('banana', 0, 1),
			('apple', 0, 2),
			('cherry', 0, 3),
			('date', 1, 4)`)
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_varchar_pk use index(ic_vc) where c = 0 order by pk limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 8: expected keep order:true for single varchar clustered PK")
		require.False(t, explainHas(rows, "TopN"),
			"case 8: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_varchar_pk use index(ic_vc) where c = 0 order by pk limit 100",
		).Check(testkit.Rows(
			"apple 0 2",
			"banana 0 1",
			"cherry 0 3",
		))

		// ---------------------------------------------------------------
		// Case 9: Single varbinary clustered PK — binary types also use
		// CommonHandle. Ordering should work the same as varchar.
		// ---------------------------------------------------------------
		tk.MustExec("drop table if exists t_binary_pk")
		tk.MustExec(`CREATE TABLE t_binary_pk (
			pk varbinary(64) PRIMARY KEY CLUSTERED,
			c int,
			d int,
			KEY ic_bin(c)
		)`)
		tk.MustExec(`insert into t_binary_pk values
			(x'CC', 0, 1),
			(x'AA', 0, 2),
			(x'DD', 0, 3),
			(x'BB', 1, 4)`)
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_binary_pk use index(ic_bin) where c = 0 order by pk limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 9: expected keep order:true for single varbinary clustered PK")
		require.False(t, explainHas(rows, "TopN"),
			"case 9: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_binary_pk use index(ic_bin) where c = 0 order by pk limit 100",
		).Check(testkit.Rows(
			"\xaa 0 2",
			"\xcc 0 1",
			"\xdd 0 3",
		))

		// ---------------------------------------------------------------
		// Case 10: Multi-type composite PK — (int, varchar) to verify
		// ordering works with mixed types in the PK.
		// ---------------------------------------------------------------
		tk.MustExec("drop table if exists t_int_varchar_pk")
		tk.MustExec(`CREATE TABLE t_int_varchar_pk (
			pk1 int,
			pk2 varchar(64),
			c int,
			KEY ic_iv(c)
		, PRIMARY KEY(pk1, pk2) CLUSTERED)`)
		tk.MustExec(`insert into t_int_varchar_pk values
			(1, 'b', 0),
			(1, 'a', 0),
			(2, 'a', 0),
			(3, 'x', 1)`)
		rows = tk.MustQuery(
			"explain format = 'brief' select * from t_int_varchar_pk use index(ic_iv) where c = 0 order by pk1, pk2 limit 100",
		).Rows()
		require.True(t, explainHas(rows, "keep order:true"),
			"case 10: expected keep order:true for (int, varchar) clustered PK")
		require.False(t, explainHas(rows, "TopN"),
			"case 10: unexpected TopN sort")

		tk.MustQuery(
			"select * from t_int_varchar_pk use index(ic_iv) where c = 0 order by pk1, pk2 limit 100",
		).Check(testkit.Rows(
			"1 a 0",
			"1 b 0",
			"2 a 0",
		))
	})
}
