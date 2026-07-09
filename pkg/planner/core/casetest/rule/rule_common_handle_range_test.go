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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestCommonHandleIndexRanges verifies that predicates on clustered (common handle)
// primary key columns are turned into scan ranges on non-unique secondary indexes,
// mirroring the long-standing behavior for int handles. The physical key of a
// non-unique secondary index on a clustered table ends with the full common handle,
// so ranger can seek directly instead of scanning and filtering.
func TestCommonHandleIndexRanges(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_chr")
		tk.MustExec(`CREATE TABLE t_chr (
			tenant int,
			seq bigint,
			a int,
			b int,
			c int,
			PRIMARY KEY(tenant, seq) CLUSTERED,
			KEY ia(a),
			KEY ia_overlap(a, tenant),
			UNIQUE KEY ub(b)
		)`)
		// Several tenants share the same a values so range narrowing is observable.
		tk.MustExec(`insert into t_chr values
			(1, 100, 10, 1, 0),
			(1, 200, 10, 2, 0),
			(2, 100, 10, 3, 0),
			(2, 200, 20, 4, 0),
			(3, 100, 10, 5, 1)`)

		// ---------------------------------------------------------------
		// Case 1: Equality on all index columns plus the leading PK column
		// becomes a multi-column point range on the secondary index.
		// ---------------------------------------------------------------
		rows := tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr use index(ia) where a = 10 and tenant = 2",
		).Rows()
		require.True(t, explainHas(rows, "range:[10 2,10 2]"),
			"case 1: expected the tenant predicate to appear in the index range")
		tk.MustQuery(
			"select * from t_chr use index(ia) where a = 10 and tenant = 2",
		).Check(testkit.Rows("2 100 10 3 0"))

		// ---------------------------------------------------------------
		// Case 2: The full PK suffix participates: eq on tenant plus a
		// range on seq extends the index range across both PK columns.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr use index(ia) where a = 10 and tenant = 1 and seq > 100",
		).Rows()
		require.True(t, explainHas(rows, "range:(10 1 100,10 1 +inf]"),
			"case 2: expected both PK columns in the index range")
		tk.MustQuery(
			"select * from t_chr use index(ia) where a = 10 and tenant = 1 and seq > 100",
		).Check(testkit.Rows("1 200 10 2 0"))

		// ---------------------------------------------------------------
		// Case 3: A non-point range on the leading PK column.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr use index(ia) where a = 10 and tenant > 1",
		).Rows()
		require.True(t, explainHas(rows, "range:(10 1,10 +inf]"),
			"case 3: expected a non-point range on the tenant column")
		tk.MustQuery(
			"select * from t_chr use index(ia) where a = 10 and tenant > 1 order by tenant, seq",
		).Check(testkit.Rows("2 100 10 3 0", "3 100 10 5 1"))

		// ---------------------------------------------------------------
		// Case 4: IN list on the index column combines with the PK suffix.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr use index(ia) where a in (10, 20) and tenant = 2",
		).Rows()
		require.True(t, explainHas(rows, "range:[10 2,10 2], [20 2,20 2]"),
			"case 4: expected the PK suffix in every IN range")
		tk.MustQuery(
			"select * from t_chr use index(ia) where a in (10, 20) and tenant = 2 order by tenant, seq",
		).Check(testkit.Rows("2 100 10 3 0", "2 200 20 4 0"))

		// ---------------------------------------------------------------
		// Case 5: A predicate on a later PK column without binding the
		// earlier one cannot extend the range and stays a filter.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr use index(ia) where a = 10 and seq = 100",
		).Rows()
		require.True(t, explainHas(rows, "range:[10,10]"),
			"case 5: expected the range to stop at the index column")
		tk.MustQuery(
			"select * from t_chr use index(ia) where a = 10 and seq = 100 order by tenant, seq",
		).Check(testkit.Rows("1 100 10 1 0", "2 100 10 3 0", "3 100 10 5 1"))

		// ---------------------------------------------------------------
		// Case 6: Overlap — ia_overlap(a, tenant) already contains a PK
		// column, so the physical key holds tenant twice and the PK suffix
		// must NOT be appended. The seq predicate stays a filter and the
		// range ends at the declared index columns.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr use index(ia_overlap) where a = 10 and tenant = 1 and seq > 100",
		).Rows()
		require.True(t, explainHas(rows, "range:[10 1,10 1]"),
			"case 6: expected the range to stop at the declared index columns")
		require.False(t, explainHas(rows, "range:[10 1 100"),
			"case 6: PK suffix must not extend the range of an overlapping index")
		tk.MustQuery(
			"select * from t_chr use index(ia_overlap) where a = 10 and tenant = 1 and seq > 100",
		).Check(testkit.Rows("1 200 10 2 0"))

		// ---------------------------------------------------------------
		// Case 7: Unique secondary index — the handle lives in the index
		// value, not the key, so the PK suffix must NOT be appended.
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr use index(ub) where b > 0 and tenant = 2",
		).Rows()
		require.False(t, explainHas(rows, "range:(0 2"),
			"case 7: PK suffix must not extend the range of a unique index")
		tk.MustQuery(
			"select * from t_chr use index(ub) where b > 0 and tenant = 2 order by tenant, seq",
		).Check(testkit.Rows("2 100 10 3 0", "2 200 20 4 0"))

		// ---------------------------------------------------------------
		// Case 8: Covering read — the appended PK columns are readable from
		// the index without a duplicated schema (IndexReader, no lookup).
		// ---------------------------------------------------------------
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select tenant, seq, a from t_chr use index(ia) where a = 10 and tenant = 2",
		).Rows()
		require.True(t, explainHas(rows, "IndexReader"),
			"case 8: expected a single-scan IndexReader")
		require.True(t, explainHas(rows, "range:[10 2,10 2]"),
			"case 8: expected the tenant predicate in the index range")
		tk.MustQuery(
			"select tenant, seq, a from t_chr use index(ia) where a = 10 and tenant = 2",
		).Check(testkit.Rows("2 100 10"))

		// ---------------------------------------------------------------
		// Case 9: String PK column with a case-insensitive collation — the
		// range must honor collation equality (sortKey-encoded seek).
		// ---------------------------------------------------------------
		tk.MustExec("drop table if exists t_chr_ci")
		tk.MustExec(`CREATE TABLE t_chr_ci (
			tenant varchar(32) COLLATE utf8mb4_general_ci,
			seq int,
			a int,
			PRIMARY KEY(tenant, seq) CLUSTERED,
			KEY ia(a)
		)`)
		tk.MustExec(`insert into t_chr_ci values
			('abc', 1, 10),
			('ABC', 2, 10),
			('xyz', 1, 10),
			('abc', 3, 20)`)
		tk.MustQuery(
			"select * from t_chr_ci use index(ia) where a = 10 and tenant = 'AbC' order by seq",
		).Check(testkit.Rows("abc 1 10", "ABC 2 10"))
		tk.MustQuery(
			"select * from t_chr_ci use index(ia) where a = 10 and tenant = 'abc' and seq >= 2",
		).Check(testkit.Rows("ABC 2 10"))

		// ---------------------------------------------------------------
		// Case 10: Prefixed clustered PK — the handle stores a truncated
		// prefix of p1, so the range uses prefix semantics and the full
		// predicate is re-checked as a filter. Results must stay exact.
		// ---------------------------------------------------------------
		tk.MustExec("drop table if exists t_chr_prefix")
		tk.MustExec(`CREATE TABLE t_chr_prefix (
			p1 varchar(64),
			p2 int,
			c int,
			PRIMARY KEY(p1(2), p2) CLUSTERED,
			KEY ic(c)
		)`)
		tk.MustExec(`insert into t_chr_prefix values ('abz', 1, 0), ('abc', 2, 0), ('axy', 3, 0), ('abc', 4, 1)`)
		tk.MustQuery(
			"select * from t_chr_prefix use index(ic) where c = 0 and p1 = 'abc'",
		).Check(testkit.Rows("abc 2 0"))
		tk.MustQuery(
			"select * from t_chr_prefix use index(ic) where c = 0 and p1 = 'abz' and p2 = 1",
		).Check(testkit.Rows("abz 1 0"))

		// ---------------------------------------------------------------
		// Case 11: Single-column string common handle — a single non-int
		// clustered PK is also a common handle, so the same range
		// extension applies with a one-column suffix.
		// ---------------------------------------------------------------
		tk.MustExec("drop table if exists t_chr_single")
		tk.MustExec(`CREATE TABLE t_chr_single (
			pk varchar(32),
			a int,
			b int,
			PRIMARY KEY(pk) CLUSTERED,
			KEY ia(a)
		)`)
		tk.MustExec(`insert into t_chr_single values
			('u1', 10, 1),
			('u2', 10, 2),
			('u3', 10, 3),
			('u4', 20, 4)`)
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr_single use index(ia) where a = 10 and pk = 'u2'",
		).Rows()
		require.True(t, explainHas(rows, `range:[10 "u2",10 "u2"]`),
			"case 11: expected the PK predicate to appear in the index range")
		tk.MustQuery(
			"select * from t_chr_single use index(ia) where a = 10 and pk = 'u2'",
		).Check(testkit.Rows("u2 10 2"))
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr_single use index(ia) where a = 10 and pk > 'u1'",
		).Rows()
		require.True(t, explainHas(rows, `range:(10 "u1",10 +inf]`),
			"case 11: expected a non-point range on the PK column")
		tk.MustQuery(
			"select * from t_chr_single use index(ia) where a = 10 and pk > 'u1' order by pk",
		).Check(testkit.Rows("u2 10 2", "u3 10 3"))
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select pk, a from t_chr_single use index(ia) where a = 10 and pk = 'u2'",
		).Rows()
		require.True(t, explainHas(rows, "IndexReader"),
			"case 11: expected a single-scan IndexReader for the covering read")

		// ---------------------------------------------------------------
		// Case 12: Single-column decimal common handle — non-int, non-string
		// handle types take the same path.
		// ---------------------------------------------------------------
		tk.MustExec("drop table if exists t_chr_dec")
		tk.MustExec(`CREATE TABLE t_chr_dec (
			pk decimal(10,2),
			a int,
			PRIMARY KEY(pk) CLUSTERED,
			KEY ia(a)
		)`)
		tk.MustExec(`insert into t_chr_dec values (1.50, 10), (2.50, 10), (3.50, 20)`)
		rows = tk.MustQuery(
			"explain format = 'plan_tree' select * from t_chr_dec use index(ia) where a = 10 and pk = 2.50",
		).Rows()
		require.True(t, explainHas(rows, "range:[10 2.50,10 2.50]"),
			"case 12: expected the decimal PK predicate to appear in the index range")
		tk.MustQuery(
			"select * from t_chr_dec use index(ia) where a = 10 and pk = 2.50",
		).Check(testkit.Rows("2.50 10"))
	})
}
