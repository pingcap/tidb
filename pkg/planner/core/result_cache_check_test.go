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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type compiledQuery struct {
	stmtNode ast.StmtNode
	plan     base.PhysicalPlan
}

// compileQuery parses and optimizes a SQL query, returning the AST and physical plan.
func compileQuery(t *testing.T, tk *testkit.TestKit, sql string) compiledQuery {
	t.Helper()
	ctx := tk.Session().(sessionctx.Context)
	stmts, err := session.Parse(ctx, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	nodeW := resolve.NewNodeW(stmts[0])
	ret := &core.PreprocessorReturn{}
	err = core.Preprocess(context.Background(), ctx, nodeW, core.WithPreprocessorReturn(ret))
	require.NoError(t, err)
	p, _, err := planner.Optimize(context.TODO(), ctx, nodeW, ret.InfoSchema)
	require.NoError(t, err)
	pp, ok := p.(base.PhysicalPlan)
	require.True(t, ok, "expected PhysicalPlan, got %T", p)
	return compiledQuery{stmtNode: stmts[0], plan: pp}
}

func TestCanCache_PointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	// PointGetPlan should not be cacheable (bypasses plan cache parameterization).
	q := compileQuery(t, tk, "select * from cached_t where id = 1")
	switch q.plan.(type) {
	case *core.PointGetPlan:
		require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
	default:
		// If the optimizer didn't choose PointGet, this test is not applicable
		// but the simple select test below covers non-PointGet plans.
		t.Logf("optimizer did not choose PointGetPlan for this query (got %T), skipping", q.plan)
	}
}

func TestCanCache_BatchPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	// BatchPointGetPlan should not be cacheable.
	q := compileQuery(t, tk, "select * from cached_t where id in (1, 2, 3)")
	switch q.plan.(type) {
	case *core.BatchPointGetPlan:
		require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
	default:
		t.Logf("optimizer did not choose BatchPointGetPlan for this query (got %T), skipping", q.plan)
	}
}

func TestCanCache_SimpleSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	// Use a non-PK filter so the optimizer picks a table scan, not a PointGetPlan.
	q := compileQuery(t, tk, "select * from cached_t where v = 1")
	require.True(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_WithNow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	q := compileQuery(t, tk, "select now(), id from cached_t")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_WithRand(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	q := compileQuery(t, tk, "select * from cached_t where v > rand()")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_ForUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	// FOR UPDATE needs a transaction context.
	tk.MustExec("begin")
	q := compileQuery(t, tk, "select * from cached_t where id = 1 for update")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
	tk.MustExec("rollback")
}

func TestCanCache_JoinAllCached(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_a (id int primary key, v int)")
	tk.MustExec("create table cached_b (id int primary key, v int)")
	tk.MustExec("alter table cached_a cache")
	tk.MustExec("alter table cached_b cache")

	q := compileQuery(t, tk, "select * from cached_a a join cached_b b on a.id = b.id")
	require.True(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_JoinMixed(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("create table normal_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	q := compileQuery(t, tk, "select * from cached_t a join normal_t b on a.id = b.id")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_SubqueryNonCached(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("create table normal_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	q := compileQuery(t, tk, "select * from cached_t where id in (select id from normal_t)")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_InDML(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	q := compileQuery(t, tk, "select * from cached_t where id = 1")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, true))
}

func TestCanCache_SessionVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	// User variable @a uses GET_VAR which is in mutableEffectsFunctions.
	q := compileQuery(t, tk, "select @a, id from cached_t")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_UUID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	q := compileQuery(t, tk, "select uuid(), id from cached_t")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_NonCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table normal_t (id int primary key, v int)")

	q := compileQuery(t, tk, "select * from normal_t where id = 1")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}

func TestCanCache_SystemVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cached_t (id int primary key, v int)")
	tk.MustExec("alter table cached_t cache")

	// System variable @@sql_mode.
	q := compileQuery(t, tk, "select @@sql_mode, id from cached_t")
	require.False(t, core.CanCacheResultSet(q.stmtNode, q.plan, false))
}
