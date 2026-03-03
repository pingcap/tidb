// Copyright 2025 PingCAP, Inc.
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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// execAndBuildKey executes a SQL statement and builds the result cache key
// from the session context afterward (plan digest is set after execution).
func execAndBuildKey(t *testing.T, tk *testkit.TestKit, sql string) (table.ResultCacheKey, []byte, bool) {
	t.Helper()
	tk.MustQuery(sql)
	sctx := tk.Session().(sessionctx.Context)
	return core.BuildResultCacheKey(sctx)
}

func TestBuildResultCacheKey_NoPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Replace StmtCtx with a fresh one that has no plan digest.
	sctx := tk.Session().(sessionctx.Context)
	sctx.GetSessionVars().StmtCtx = stmtctx.NewStmtCtx()
	_, _, ok := core.BuildResultCacheKey(sctx)
	require.False(t, ok)
}

func TestBuildResultCacheKey_NonPrepared(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	// Two different queries with same shape but different constants.
	key1, pb1, ok1 := execAndBuildKey(t, tk, "select * from cached_t where id = 1")
	require.True(t, ok1)
	require.NotNil(t, pb1)

	key2, pb2, ok2 := execAndBuildKey(t, tk, "select * from cached_t where id = 2")
	require.True(t, ok2)
	require.NotNil(t, pb2)

	// With the same plan shape, the PlanDigest portion should be equal.
	require.Equal(t, key1.PlanDigest, key2.PlanDigest)
	// Different literal values must produce different ParamHash via SQL fallback.
	require.NotEqual(t, key1.ParamHash, key2.ParamHash)
	// Different param bytes for different values.
	require.NotEqual(t, pb1, pb2)

	// A structurally different query should produce a different plan digest.
	key3, _, ok3 := execAndBuildKey(t, tk, "select v from cached_t")
	require.True(t, ok3)
	require.NotEqual(t, key1.PlanDigest, key3.PlanDigest)
}

func TestBuildResultCacheKey_NonPrepared_DifferentValues(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")
	// Disable non-prepared plan cache to trigger the SQL fallback path.
	tk.MustExec("set tidb_enable_non_prepared_plan_cache = OFF")

	key1, pb1, ok1 := execAndBuildKey(t, tk, "select * from cached_t where v = 1")
	require.True(t, ok1)
	require.NotNil(t, pb1)

	key2, pb2, ok2 := execAndBuildKey(t, tk, "select * from cached_t where v = 2")
	require.True(t, ok2)
	require.NotNil(t, pb2)

	// Same plan shape → same digest.
	require.Equal(t, key1.PlanDigest, key2.PlanDigest)
	// Different literals → different ParamHash (the bug was both being 0).
	require.NotEqual(t, key1.ParamHash, key2.ParamHash)
	// Different param bytes for different values.
	require.NotEqual(t, pb1, pb2)

	// Same query twice must produce the same key and param bytes.
	key3, pb3, ok3 := execAndBuildKey(t, tk, "select * from cached_t where v = 1")
	require.True(t, ok3)
	require.Equal(t, key1, key3)
	require.Equal(t, pb1, pb3)
}

func TestBuildResultCacheKey_Prepared(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	// Use prepared statements through the SQL interface.
	tk.MustExec("prepare stmt from 'select * from cached_t where id = ?'")

	// Execute with param = 1
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt using @a")
	sctx := tk.Session().(sessionctx.Context)
	key1, pb1, ok1 := core.BuildResultCacheKey(sctx)
	require.True(t, ok1)
	require.NotNil(t, pb1)

	// Execute with param = 2
	tk.MustExec("set @a = 2")
	tk.MustQuery("execute stmt using @a")
	key2, pb2, ok2 := core.BuildResultCacheKey(sctx)
	require.True(t, ok2)
	require.NotNil(t, pb2)

	// Same plan digest but different param hash.
	require.Equal(t, key1.PlanDigest, key2.PlanDigest)
	require.NotEqual(t, key1.ParamHash, key2.ParamHash)
	// Different param bytes for different values.
	require.NotEqual(t, pb1, pb2)
}

func TestBuildResultCacheKey_SameParams(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	tk.MustExec("prepare stmt from 'select * from cached_t where id = ?'")

	// Execute twice with the same param value.
	tk.MustExec("set @a = 42")
	tk.MustQuery("execute stmt using @a")
	sctx := tk.Session().(sessionctx.Context)
	key1, pb1, ok1 := core.BuildResultCacheKey(sctx)
	require.True(t, ok1)

	tk.MustQuery("execute stmt using @a")
	key2, pb2, ok2 := core.BuildResultCacheKey(sctx)
	require.True(t, ok2)

	// Identical keys and param bytes.
	require.Equal(t, key1, key2)
	require.Equal(t, pb1, pb2)
}

func TestHashParams_DifferentTypes(t *testing.T) {
	// Verify that hashParams produces different hashes for different Datum types
	// even when the "value" might look similar (e.g., int 1 vs string "1").
	intDatum := types.NewIntDatum(1)
	strDatum := types.NewStringDatum("1")
	floatDatum := types.NewFloat64Datum(1.0)

	h1 := core.HashParamsForTest([]types.Datum{intDatum})
	h2 := core.HashParamsForTest([]types.Datum{strDatum})
	h3 := core.HashParamsForTest([]types.Datum{floatDatum})

	// All three should be different since they have different type encodings.
	require.NotEqual(t, h1, h2)
	require.NotEqual(t, h1, h3)
	require.NotEqual(t, h2, h3)

	// Same input should produce the same hash.
	h1Again := core.HashParamsForTest([]types.Datum{intDatum})
	require.Equal(t, h1, h1Again)
}

func TestHashParams_MultipleParams(t *testing.T) {
	// Different ordering of params should produce different hashes.
	d1 := types.NewIntDatum(1)
	d2 := types.NewIntDatum(2)

	h12 := core.HashParamsForTest([]types.Datum{d1, d2})
	h21 := core.HashParamsForTest([]types.Datum{d2, d1})
	require.NotEqual(t, h12, h21)
}

func TestBuildResultCacheKey_DifferentTimezones(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_tz (id int primary key, ts timestamp)")
	tk.MustExec("alter table cached_tz cache")

	// Same query executed under different timezones must produce different keys.
	tk.MustExec("set @@time_zone = '+00:00'")
	key1, pb1, ok1 := execAndBuildKey(t, tk, "select * from cached_tz")
	require.True(t, ok1)

	tk.MustExec("set @@time_zone = '+08:00'")
	key2, pb2, ok2 := execAndBuildKey(t, tk, "select * from cached_tz")
	require.True(t, ok2)

	// Same plan digest (same query shape).
	require.Equal(t, key1.PlanDigest, key2.PlanDigest)
	// Different ParamHash because timezone is included.
	require.NotEqual(t, key1.ParamHash, key2.ParamHash)
	// Different param bytes.
	require.NotEqual(t, pb1, pb2)

	// Same timezone should produce same key.
	tk.MustExec("set @@time_zone = '+00:00'")
	key3, pb3, ok3 := execAndBuildKey(t, tk, "select * from cached_tz")
	require.True(t, ok3)
	require.Equal(t, key1, key3)
	require.Equal(t, pb1, pb3)
}
