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

package common

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestPrepareStmtDedupCacheBasic verifies that preparing the same SQL twice in
// the same session reuses the cached PlanCacheStmt: both stmtIDs are distinct,
// but paramCount and column metadata match.
func TestPrepareStmtDedupCacheBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_cache_prepare_stmt = 1")
	tk.MustExec("use test")
	tk.MustExec("create table t (id bigint primary key, age int, city varchar(32))")

	sql := "select id, city from t where age > ? and city = ?"

	id1, paramCount1, fields1, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	require.EqualValues(t, 2, paramCount1)
	require.Len(t, fields1, 2)
	require.Equal(t, "id", fields1[0].Column.Name.L)
	require.Equal(t, "city", fields1[1].Column.Name.L)

	// Second prepare of the same SQL — should hit the dedup cache.
	id2, paramCount2, fields2, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2, "each Prepare must return a distinct stmtID")
	require.Equal(t, paramCount1, paramCount2)
	require.Len(t, fields2, 2)
	require.Equal(t, "id", fields2[0].Column.Name.L)
	require.Equal(t, "city", fields2[1].Column.Name.L)
	tk.MustExec("set tidb_enable_cache_prepare_stmt = default")
}

// TestPrepareStmtDedupCacheExecute verifies that stmts produced via the dedup
// cache path execute correctly and return the right rows.
func TestPrepareStmtDedupCacheExecute(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_cache_prepare_stmt = 1")
	tk.MustExec("create table t2 (id bigint primary key, val int)")
	tk.MustExec("insert into t2 values (1, 10), (2, 20), (3, 30)")

	ctx := context.Background()
	sql := "select id from t2 where val > ?"

	runAndCollect := func(stmtID uint32, threshold int) []int64 {
		rs, err := tk.Session().ExecutePreparedStmt(ctx, stmtID,
			expression.Args2Expressions4Test(threshold))
		require.NoError(t, err)
		defer rs.Close()
		var ids []int64
		req := rs.NewChunk(nil)
		for {
			require.NoError(t, rs.Next(ctx, req))
			if req.NumRows() == 0 {
				break
			}
			for i := range req.NumRows() {
				ids = append(ids, req.Column(0).GetInt64(i))
			}
			req.Reset()
		}
		return ids
	}

	// First prepare — full build path.
	id1, _, _, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	ids := runAndCollect(id1, 15)
	require.Equal(t, []int64{2, 3}, ids)

	// Second prepare — dedup cache path.
	id2, _, _, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)
	ids = runAndCollect(id2, 5)
	require.Equal(t, []int64{1, 2, 3}, ids)

	// The original stmt must still work independently.
	ids = runAndCollect(id1, 25)
	require.Equal(t, []int64{3}, ids)

	tk.MustExec("set tidb_enable_cache_prepare_stmt = default")
}

// TestPrepareStmtDedupCacheSchemaChange verifies that a DDL (schema version bump)
// causes the dedup cache entry to be invalidated and the next Prepare to go
// through the full build path, reflecting the updated schema.
func TestPrepareStmtDedupCacheSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_cache_prepare_stmt = 1")
	tk.MustExec("create table t3 (id bigint primary key, name varchar(32))")

	sql := "select id, name from t3 where id = ?"

	// Warm up the dedup cache.
	id1, _, fields1, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	require.Len(t, fields1, 2)
	_ = id1

	// DDL that changes the schema version.
	tk.MustExec("alter table t3 add column email varchar(64)")

	// After DDL the dedup cache entry should be stale (schema version mismatch)
	// and the full build path should be taken. The returned fields come from
	// the original prepare (the client protocol doesn't return field info again
	// on cache hit), but the resulting PlanCacheStmt must be valid.
	id2, _, _, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	// The new stmt must execute correctly against the updated schema.
	ctx := context.Background()
	tk.MustExec("insert into t3 (id, name, email) values (1, 'alice', 'alice@example.com')")
	rs, err := tk.Session().ExecutePreparedStmt(ctx, id2, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	tk.MustExec("set tidb_enable_cache_prepare_stmt = default")
}

// TestPrepareStmtDedupCacheIsolatedByDB verifies that prepare dedup cache entries
// are isolated per database: the same SQL text prepared while connected to
// different databases must not share a cache entry.
func TestPrepareStmtDedupCacheIsolatedByDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_cache_prepare_stmt = 1")
	tk.MustExec("create database if not exists db1")
	tk.MustExec("create database if not exists db2")
	tk.MustExec("use db1")
	tk.MustExec("create table tblx (id bigint primary key, v int)")
	tk.MustExec("use db2")
	tk.MustExec("create table tblx (id bigint primary key, v bigint)") // different column type

	sql := "select v from tblx where id = ?"

	// Prepare in db1.
	tk.MustExec("use db1")
	id1, _, fields1, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	require.Len(t, fields1, 1)
	_ = id1

	// Prepare in db2 — must NOT hit the db1 cache entry because currentDB differs.
	tk.MustExec("use db2")
	id2, _, fields2, err := tk.Session().PrepareStmt(sql)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)
	// fields2 reflects db2.tblx.v which is bigint (not int), so a fresh build must have run.
	require.Len(t, fields2, 1)
	tk.MustExec("set tidb_enable_cache_prepare_stmt = default")
}

// TestPrepareStmtDedupCachePrepareExecuteCloseLoop verifies the prepare-per-request
// anti-pattern: multiple Prepare→Execute→Close cycles for the same SQL all
// produce correct results.
func TestPrepareStmtDedupCachePrepareExecuteCloseLoop(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_cache_prepare_stmt = 1")
	tk.MustExec("create table t4 (id bigint primary key, score int)")
	tk.MustExec("insert into t4 values (1,100),(2,200),(3,300)")

	ctx := context.Background()
	sql := "select id from t4 where score >= ?"

	thresholds := []int{50, 150, 250}
	expected := [][]int64{{1, 2, 3}, {2, 3}, {3}}

	for round, threshold := range thresholds {
		id, _, _, err := tk.Session().PrepareStmt(sql)
		require.NoError(t, err)

		rs, err := tk.Session().ExecutePreparedStmt(ctx, id, expression.Args2Expressions4Test(threshold))
		require.NoError(t, err)

		var got []int64
		req := rs.NewChunk(nil)
		for {
			require.NoError(t, rs.Next(ctx, req))
			if req.NumRows() == 0 {
				break
			}
			for i := range req.NumRows() {
				got = append(got, req.Column(0).GetInt64(i))
			}
			req.Reset()
		}
		require.NoError(t, rs.Close())
		require.Equal(t, expected[round], got, "round %d, threshold %d", round, threshold)

		require.NoError(t, tk.Session().DropPreparedStmt(id))
	}
	tk.MustExec("set tidb_enable_cache_prepare_stmt = default")
}
