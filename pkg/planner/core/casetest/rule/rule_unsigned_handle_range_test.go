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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// createUnsignedHandleTable creates a table whose clustered primary key is an unsigned
// integer handle, plus rows on both sides of the math.MaxInt64 boundary. The physical key
// of ia ends with the handle reinterpreted as an int64, so the rows above the boundary are
// stored before the ones below it.
func createUnsignedHandleTable(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_uh")
	tk.MustExec(`CREATE TABLE t_uh (
		id BIGINT UNSIGNED PRIMARY KEY CLUSTERED,
		a INT,
		b INT,
		KEY ia(a),
		KEY ia_id(a, id),
		UNIQUE KEY ub(b)
	)`)
	tk.MustExec(`insert into t_uh values
		(7, 5, 1),
		(11, 5, 2),
		(22, 5, 3),
		(9223372036854775800, 5, 4),
		(9223372036854775807, 5, 5),
		(9223372036854775808, 5, 6),
		(9223372036854775810, 5, 7),
		(18446744073709551615, 5, 8),
		(30, 6, 9)`)
}

// TestUnsignedIntHandleIndexRanges checks that predicates on an unsigned integer handle
// become ranges on a non-unique secondary index and that the rows they return are right,
// including for predicates that cross the int64 boundary the handle encoding wraps at.
func TestUnsignedIntHandleIndexRanges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createUnsignedHandleTable(tk)

	// A point predicate on the handle becomes a two-column point range.
	rows := tk.MustQuery("explain format = 'plan_tree' select b from t_uh use index(ia) where a = 5 and id = 7").Rows()
	require.True(t, explainHas(rows, "range:[5 7,5 7]"),
		"expected the handle predicate to appear in the index range")
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id = 7").Check(testkit.Rows("1"))

	// An open range that stays below the boundary.
	rows = tk.MustQuery("explain format = 'plan_tree' select b from t_uh use index(ia) where a = 5 and id > 10 and id < 25").Rows()
	require.True(t, explainHas(rows, "range:(5 10,5 25)"),
		"expected the handle range to bound the index range")
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id > 10 and id < 25 order by b").
		Check(testkit.Rows("2", "3"))

	// An open range whose upper bound is unbounded, so it spans the boundary.
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id > 10 order by b").
		Check(testkit.Rows("2", "3", "4", "5", "6", "7", "8"))

	// A closed range straddling the boundary.
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id between 9223372036854775800 and 9223372036854775810 order by b").
		Check(testkit.Rows("4", "5", "6", "7"))

	// A range entirely above the boundary.
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id >= 9223372036854775808 order by b").
		Check(testkit.Rows("6", "7", "8"))

	// A range entirely below the boundary.
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id <= 9223372036854775807 order by b").
		Check(testkit.Rows("1", "2", "3", "4", "5"))

	// An IN list mixing both sides of the boundary.
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id in (11, 22, 9223372036854775808) order by b").
		Check(testkit.Rows("2", "3", "6"))

	// The maximum handle value is reachable.
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id = 18446744073709551615").Check(testkit.Rows("8"))

	// Nothing outside the predicate leaks in when several index prefixes are scanned.
	tk.MustQuery("select b from t_uh use index(ia) where a in (5, 6) and id > 25 order by b").
		Check(testkit.Rows("4", "5", "6", "7", "8", "9"))
}

// TestUnsignedIntHandleIndexRangesKeepOrder checks the ordering contract of the appended
// unsigned handle: the index still provides order on its declared columns, but never on
// the handle, whose stored order wraps at math.MaxInt64.
func TestUnsignedIntHandleIndexRangesKeepOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createUnsignedHandleTable(tk)

	// ORDER BY on the handle must not be answered by the index order.
	rows := tk.MustQuery("explain format = 'plan_tree' select b from t_uh use index(ia) where a = 5 and id > 10 order by id").Rows()
	require.True(t, explainHas(rows, "Sort"), "the appended unsigned handle must not satisfy an ordering")
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id > 10 order by id").
		Check(testkit.Rows("2", "3", "4", "5", "6", "7", "8"))
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id > 10 order by id desc").
		Check(testkit.Rows("8", "7", "6", "5", "4", "3", "2"))

	// Order on the declared index columns is still provided while the handle predicate
	// narrows the scan, even though the scan of each `a` group is split around the
	// boundary.
	rows = tk.MustQuery("explain format = 'plan_tree' select a from t_uh use index(ia) where a in (5, 6) and id > 25 order by a").Rows()
	require.False(t, explainHas(rows, "Sort"), "the declared index column must still provide order")
	tk.MustQuery("select a from t_uh use index(ia) where a in (5, 6) and id > 25 order by a").
		Check(testkit.Rows("5", "5", "5", "5", "5", "6"))
	tk.MustQuery("select b from t_uh use index(ia) where a in (5, 6) and id > 25 order by a, b").
		Check(testkit.Rows("4", "5", "6", "7", "8", "9"))

	// When the handle is a declared index column it is encoded as an ordinary column, so
	// it does keep order.
	rows = tk.MustQuery("explain format = 'plan_tree' select b from t_uh use index(ia_id) where a = 5 and id > 10 order by id").Rows()
	require.False(t, explainHas(rows, "Sort"), "a declared unsigned index column still provides order")
	tk.MustQuery("select b from t_uh use index(ia_id) where a = 5 and id > 10 order by id").
		Check(testkit.Rows("2", "3", "4", "5", "6", "7", "8"))
}

// TestUnsignedIntHandleIndexRangesInIndexJoin covers the inner index ranges an index join
// rebuilds per outer row, which are turned into key ranges by a different path than a
// standalone index scan.
func TestUnsignedIntHandleIndexRangesInIndexJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createUnsignedHandleTable(tk)
	tk.MustExec("drop table if exists t_probe")
	tk.MustExec("create table t_probe(ka int, kid bigint unsigned)")
	tk.MustExec(`insert into t_probe values
		(5, 7),
		(5, 9223372036854775808),
		(5, 18446744073709551615),
		(6, 30)`)

	// Equality on the handle: the inner ranges are points on (a, id).
	rows := tk.MustQuery(`explain format = 'plan_tree'
		select /*+ inl_join(u) */ u.b from t_probe p, t_uh u use index(ia) where u.a = p.ka and u.id = p.kid`).Rows()
	require.True(t, explainHas(rows, "IndexJoin"), "expected an index join")
	tk.MustQuery(`select /*+ inl_join(u) */ u.b from t_probe p, t_uh u use index(ia)
		where u.a = p.ka and u.id = p.kid order by u.b`).Check(testkit.Rows("1", "6", "8", "9"))

	// A correlated range on the handle, which crosses the boundary for some outer rows.
	tk.MustQuery(`select /*+ inl_join(u) */ u.b from t_probe p, t_uh u use index(ia)
		where u.a = p.ka and u.id >= p.kid order by u.b`).
		Check(testkit.Rows("1", "2", "3", "4", "5", "6", "6", "7", "7", "8", "8", "8", "9"))
}

// TestUnsignedIntHandleIndexRangesWithPlanCache covers ranges rebuilt from a cached plan:
// the parameter can cross the int64 boundary even when the parameter the plan was built
// with did not.
func TestUnsignedIntHandleIndexRangesWithPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createUnsignedHandleTable(tk)

	tk.MustExec("prepare stmt from 'select b from t_uh use index(ia) where a = 5 and id > ? order by b'")
	tk.MustExec("set @p = 10")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("2", "3", "4", "5", "6", "7", "8"))
	tk.MustExec("set @p = 9223372036854775807")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("6", "7", "8"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	// A parameter above math.MaxInt64 is an unsigned literal, so it gets its own cache
	// entry; the entry is then reused by the next parameter of the same type.
	tk.MustExec("set @p = 9223372036854775810")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("8"))
	tk.MustExec("set @p = 9223372036854775808")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("7", "8"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

// TestUnsignedIntHandleIndexRangesWithUncommittedRows covers the membuffer reader, which
// scans the uncommitted rows of the current transaction over the same key ranges.
func TestUnsignedIntHandleIndexRangesWithUncommittedRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createUnsignedHandleTable(tk)

	tk.MustExec("begin")
	tk.MustExec("insert into t_uh values (9223372036854775809, 5, 10), (12, 5, 11)")
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id > 10 order by b").
		Check(testkit.Rows("2", "3", "4", "5", "6", "7", "8", "10", "11"))
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id = 9223372036854775809").Check(testkit.Rows("10"))
	tk.MustExec("rollback")
}

// TestUnsignedIntHandleIndexRangesInIndexMerge covers the partial index scans of an index
// merge, whose key ranges are built by their own path.
func TestUnsignedIntHandleIndexRangesInIndexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createUnsignedHandleTable(tk)

	rows := tk.MustQuery(`explain format = 'plan_tree'
		select /*+ use_index_merge(t_uh, ia, ub) */ b from t_uh
		where (a = 5 and id > 9223372036854775800) or b = 1`).Rows()
	require.True(t, explainHas(rows, "IndexMerge"), "expected an index merge")
	tk.MustQuery(`select /*+ use_index_merge(t_uh, ia, ub) */ b from t_uh
		where (a = 5 and id > 9223372036854775800) or b = 1 order by b`).
		Check(testkit.Rows("1", "5", "6", "7", "8"))
}

// TestUnsignedIntHandleIndexRangesOnPartitionedTable covers per-partition key ranges,
// which are built once per physical table id.
func TestUnsignedIntHandleIndexRangesOnPartitionedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_uhp")
	tk.MustExec(`CREATE TABLE t_uhp (
		id BIGINT UNSIGNED PRIMARY KEY CLUSTERED,
		a INT,
		b INT,
		KEY ia(a)
	) PARTITION BY HASH(id) PARTITIONS 4`)
	tk.MustExec(`insert into t_uhp values
		(7, 5, 1),
		(11, 5, 2),
		(9223372036854775808, 5, 6),
		(9223372036854775810, 5, 7),
		(18446744073709551615, 5, 8)`)

	tk.MustQuery("select b from t_uhp use index(ia) where a = 5 and id > 10 order by b").
		Check(testkit.Rows("2", "6", "7", "8"))
	tk.MustQuery("select b from t_uhp use index(ia) where a = 5 and id = 9223372036854775810").Check(testkit.Rows("7"))
	tk.MustQuery("select b from t_uhp use index(ia) where a = 5 and id in (7, 18446744073709551615) order by b").
		Check(testkit.Rows("1", "8"))
}

// TestUnsignedIntHandleIndexRangesDescScan covers a descending index scan, which reads the
// same key ranges backwards.
func TestUnsignedIntHandleIndexRangesDescScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createUnsignedHandleTable(tk)

	rows := tk.MustQuery(`explain format = 'plan_tree'
		select a from t_uh use index(ia) where a in (5, 6) and id > 25 order by a desc`).Rows()
	require.False(t, explainHas(rows, "Sort"), "the declared index column must still provide descending order")
	tk.MustQuery("select a from t_uh use index(ia) where a in (5, 6) and id > 25 order by a desc").
		Check(testkit.Rows("6", "5", "5", "5", "5", "5"))
	tk.MustQuery("select b from t_uh use index(ia) where a = 5 and id > 25 order by a desc, b desc").
		Check(testkit.Rows("8", "7", "6", "5", "4"))
}

// TestUnsignedIntHandleIndexRangesOnRangePartitionedTable covers a table partitioned on the
// unsigned handle itself, so one predicate both prunes partitions and narrows the index
// range. Partition p1 spans math.MaxInt64, which is where the stored handle order wraps, so
// a range confined to that partition still has to be split. Both prune modes are exercised
// because they reach the key ranges through different executor paths: dynamic pruning builds
// them once per physical table id, static pruning builds a reader per partition.
func TestUnsignedIntHandleIndexRangesOnRangePartitionedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_uhr")
	tk.MustExec(`CREATE TABLE t_uhr (
		id BIGINT UNSIGNED PRIMARY KEY CLUSTERED,
		a INT,
		b INT,
		KEY ia(a)
	) PARTITION BY RANGE (id) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (10000000000000000000),
		PARTITION p2 VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec(`insert into t_uhr values
		(7, 5, 1),
		(11, 5, 2),
		(22, 5, 3),
		(30, 6, 9),
		(9223372036854775800, 5, 4),
		(9223372036854775807, 5, 5),
		(9223372036854775808, 5, 6),
		(9223372036854775810, 5, 7),
		(18446744073709551615, 5, 8)`)

	for _, pruneMode := range []string{"dynamic", "static"} {
		t.Run(pruneMode, func(t *testing.T) {
			tk.MustExec("set @@tidb_partition_prune_mode = '" + pruneMode + "'")

			// A range that straddles the int64 boundary inside a single partition: the
			// handle predicate prunes down to p1 and still narrows the index range there.
			rows := tk.MustQuery(`explain format = 'plan_tree' select b from t_uhr use index(ia)
				where a = 5 and id between 9223372036854775800 and 9223372036854775810`).Rows()
			require.True(t, explainHas(rows, "range:[5 9223372036854775800,5 9223372036854775810]"),
				"the handle predicate must reach the index range")
			require.True(t, explainHas(rows, "partition:p1"), "the handle predicate must prune partitions")
			tk.MustQuery(`select b from t_uhr use index(ia)
				where a = 5 and id between 9223372036854775800 and 9223372036854775810 order by b`).
				Check(testkit.Rows("4", "5", "6", "7"))

			// Spanning every partition, and the boundary with it.
			tk.MustQuery("select b from t_uhr use index(ia) where a = 5 and id > 10 order by b").
				Check(testkit.Rows("2", "3", "4", "5", "6", "7", "8"))

			// Entirely above the boundary, so every handle is stored as a negative int64.
			tk.MustQuery("select b from t_uhr use index(ia) where a = 5 and id >= 9223372036854775808 order by b").
				Check(testkit.Rows("6", "7", "8"))
			tk.MustQuery("select b from t_uhr use index(ia) where a = 5 and id = 18446744073709551615").
				Check(testkit.Rows("8"))

			// Confined to the partition below the boundary, over two index prefixes.
			tk.MustQuery("select b from t_uhr use index(ia) where a in (5, 6) and id < 100 order by a, b").
				Check(testkit.Rows("1", "2", "3", "9"))
		})
	}
	tk.MustExec("set @@tidb_partition_prune_mode = default")
}

// TestUnsignedIntHandleIndexRangesOnGlobalIndex covers a global index on a partitioned table
// whose primary key is an unsigned integer handle. A global index on a clustered table is
// created at version 0, whose key ends with the plain handle; from version 1 the key carries
// a partition id between the index columns and the handle, which would put the handle at a
// different dimension, and GenIndexKey rejects that combination rather than writing such a
// key. The assertion on the version pins the layout this range building assumes.
func TestUnsignedIntHandleIndexRangesOnGlobalIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_uhg")
	tk.MustExec(`CREATE TABLE t_uhg (
		id BIGINT UNSIGNED PRIMARY KEY CLUSTERED,
		a INT,
		b INT
	) PARTITION BY RANGE (id) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("alter table t_uhg add index ia_g(a) global")
	tk.MustExec(`insert into t_uhg values
		(7, 5, 1),
		(11, 5, 2),
		(9223372036854775808, 5, 6),
		(9223372036854775810, 5, 7),
		(18446744073709551615, 5, 8),
		(30, 6, 9)`)

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_uhg"))
	require.NoError(t, err)
	idx := tbl.Meta().FindIndexByName("ia_g")
	require.NotNil(t, idx)
	require.True(t, idx.Global)
	require.Equal(t, model.GlobalIndexVersionLegacy, idx.GlobalIndexVersion,
		"a global index on a clustered table must keep the legacy key layout, which ends with the plain handle")

	rows := tk.MustQuery("explain format = 'plan_tree' select b from t_uhg use index(ia_g) where a = 5 and id = 7").Rows()
	require.True(t, explainHas(rows, "range:[5 7,5 7]"), "the handle predicate must reach the index range")

	// Cross-check every shape against the same query without the index.
	for _, pred := range []string{
		"a = 5 and id = 7",
		"a = 5 and id > 10",
		"a = 5 and id >= 9223372036854775808",
		"a = 5 and id between 9223372036854775800 and 9223372036854775810",
		"a = 5 and id in (11, 9223372036854775810, 18446744073709551615)",
		"a = 5 and id = 18446744073709551615",
	} {
		expected := tk.MustQuery("select b from t_uhg ignore index(ia_g) where " + pred + " order by b").Rows()
		tk.MustQuery("select b from t_uhg use index(ia_g) where " + pred + " order by b").Check(expected)
	}
}
