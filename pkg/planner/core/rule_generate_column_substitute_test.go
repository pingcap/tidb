// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestGcSubstituterDedupsDuplicateExpressionIndexes covers
// https://github.com/pingcap/tidb/issues/70708: when two expression indexes
// share the identical expression, each is backed by its own hidden generated
// column with a structurally-equal but distinct VirtualExpr. Before the fix,
// collectGenerateColumn added one ExprColumnMap entry per duplicate, and later
// substitution picked among them via Go's randomized map iteration order,
// making ORDER BY binding (and therefore access path selection) unstable
// across otherwise identical query plannings. After the fix, only the first
// hidden column seen for a given expression is kept, so the map always has a
// single, deterministic entry for the shared expression.
func TestGcSubstituterDedupsDuplicateExpressionIndexes(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Experimental.AllowsExpressionIndex = true
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (ns int, status int, ct int, ct2 int, st int, run int)")
	tk.MustExec("alter table t add index default_idx (ns, (coalesce(ct, ct2)), st, run)")
	tk.MustExec("alter table t add index by_a (ns, status, (coalesce(ct, ct2)), st, run)")

	is := domain.GetDomain(tk.Session()).InfoSchema()
	ctx := context.Background()
	sql := "select * from t where ns = 1 order by coalesce(ct, ct2), st, run"
	// Reuses tk's session and domain, so it must not close them: testkit.NewTestKit
	// already registers a t.Cleanup that closes the domain (and its stats handle)
	// when the test ends.
	s := coretestsdk.CreatePlannerSuite(tk.Session(), is)

	// Rebuild and re-run GcSubstituter's Optimize from scratch many times.
	// Before the fix, collectGenerateColumn produced one ExprColumnMap entry
	// per duplicate expression index, and substitution picked among them via
	// Go's randomized map iteration order: which hidden column ORDER BY got
	// bound to (identified by its UniqueID) varied from run to run even
	// though the SQL, schema, and stats never changed. After the fix, only
	// one entry ever exists, so the bound column is always the same.
	var firstUniqueID int64 = -1
	const iterations = 200
	for i := 0; i < iterations; i++ {
		stmt, err := s.GetParser().ParseOneStmt(sql, "", "")
		require.NoError(t, err, sql)
		nodeW := resolve.NewNodeW(stmt)
		p, err := core.BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
		require.NoError(t, err)
		lp := p.(base.LogicalPlan)

		gc := &core.GcSubstituter{}
		lp, _, err = gc.Optimize(ctx, lp)
		require.NoError(t, err)

		sort := findLogicalSort(t, lp)
		require.NotEmpty(t, sort.ByItems)
		col, ok := sort.ByItems[0].Expr.(*expression.Column)
		require.True(t, ok, "expected ORDER BY to be substituted with a generated column, got %T", sort.ByItems[0].Expr)

		if firstUniqueID == -1 {
			firstUniqueID = col.UniqueID
		} else {
			require.Equal(t, firstUniqueID, col.UniqueID,
				"ORDER BY bound to a different hidden generated column on iteration %d; duplicate expression indexes must resolve deterministically", i)
		}
	}
}

func findLogicalSort(t *testing.T, lp base.LogicalPlan) *logicalop.LogicalSort {
	if sort, ok := lp.(*logicalop.LogicalSort); ok {
		return sort
	}
	for _, child := range lp.Children() {
		if sort := findLogicalSort(t, child); sort != nil {
			return sort
		}
	}
	require.Fail(t, "no LogicalSort found in plan tree")
	return nil
}

// ➜  core git:(master) ✗ go tool pprof m5.file
// File: ___5BenchmarkSubstituteExpression_in_github_com_pingcap_tidb_planner_core.test
// Build ID: ea7c603fe0cf5e18deac5bf65d36f115467fce80
// Type: alloc_space
// Time: Aug 28, 2023 at 3:58pm (CST)
// Entering interactive mode (type "help" for commands, "o" for options)
// (pprof) list BenchmarkSubstituteExpression
// Total: 1.40GB
// ROUTINE ======================== github.com/pingcap/tidb/pkg/planner/core_test.BenchmarkSubstituteExpression in /home/arenatlx/go/src/github.com/pingcap/tidb/pkg/planner/core/rule_generate_column_substitute_test.go
//
//	0   173.44MB (flat, cum) 12.12% of Total
//	.          .     29:func BenchmarkSubstituteExpression(b *testing.B) {
//	.   169.91MB     30:   store := testkit.CreateMockStore(b)
//	.          .     31:   tk := testkit.NewTestKit(b, store)
//	.          .     32:   tk.MustExec("use test")
//	.          .     33:   tk.MustExec("drop table if exists tai")
//	.   512.19kB     34:   tk.MustExec("create table tai(a varchar(256), b varchar(256), c int as (a+1), d int as (b+1))")
//	.          .     35:   is := domain.GetDomain(tk.Session()).InfoSchema()
//	.          .     36:   _, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("tai"))
//	.          .     37:   require.NoError(b, err)
//	.          .     38:   condition := "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     39:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     40:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     41:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     42:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     43:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     44:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     45:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     46:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     47:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     48:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     49:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     50:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     51:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     52:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     53:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     54:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     55:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     56:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     57:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     58:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     59:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     60:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     61:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     62:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     63:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     64:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     65:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     66:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     67:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     68:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     69:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     70:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     71:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     72:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     73:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     74:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     75:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     76:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     77:           "(tai.a='%s' AND tai.b='%s')"
//	.          .     78:   addresses := make([]interface{}, 0, 90)
//	.          .     79:   for i := 0; i < 80; i++ {
//	.          .     80:           addresses = append(addresses, "0x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC51")
//	.          .     81:   }
//	.   520.04kB     82:   condition = fmt.Sprintf(condition, addresses...)
//	.          .     83:   s := core.CreatePlannerSuite(tk.Session(), is)
//	.          .     84:   ctx := context.Background()
//	.          .     85:   sql := "select * from tai where " + condition
//	.          .     86:   fmt.Println(sql)
//	.          .     87:   stmt, err := s.GetParser().ParseOneStmt(sql, "", "")
//	.          .     88:   require.NoError(b, err, sql)
//	.   512.01kB     89:   p, err := core.BuildLogicalPlanForTest(ctx, s.GetCtx(), stmt, s.GetIS())
//	.          .     90:   require.NoError(b, err)
//	.          .     91:   selection := p.(core.LogicalPlan).Children()[0]
//	.          .     92:   m := make(core.ExprColumnMap, len(selection.Schema().Columns))
//	.          .     93:   for _, col := range selection.Schema().Columns {
//	.          .     94:           if col.VirtualExpr != nil {
//	.          .     95:                   m[col.VirtualExpr] = col
//	.          .     96:           }
//	.          .     97:   }
//	.          .     98:   b.ResetTimer()
//	.          .     99:   b.StartTimer()
//	.          .    100:   for i := 0; i < b.N; i++ {
//	.     2.02MB    101:           core.SubstituteExpression(selection.(*core.LogicalSelection).Conditions[0], selection, m, selection.Schema(), nil)
//	.          .    102:   }
//	.          .    103:   b.StopTimer()
//	.          .    104:}
//
// ****************************************************************************************************************************************************************
// after this patch
// ➜  core git:(fix-42788) ✗ go tool pprof m5.file
// File: ___5BenchmarkSubstituteExpression_in_github_com_pingcap_tidb_planner_core.test
// Build ID: dce9437cc5156c542bc642092b25b29de9b14d87
// Type: alloc_space
// Time: Aug 28, 2023 at 4:04pm (CST)
// Entering interactive mode (type "help" for commands, "o" for options)
// (pprof) list BenchmarkSubstituteExpression
// Total: 1.41GB
// ROUTINE ======================== github.com/pingcap/tidb/pkg/planner/core_test.BenchmarkSubstituteExpression in /home/arenatlx/go/src/github.com/pingcap/tidb/pkg/planner/core/rule_generate_column_substitute_test.go
//
//	0   172.22MB (flat, cum) 11.90% of Total
//	.          .     29:func BenchmarkSubstituteExpression(b *testing.B) {
//	.   170.21MB     30:   store := testkit.CreateMockStore(b)
//	.     1.01MB     31:   tk := testkit.NewTestKit(b, store)
//	.          .     32:   tk.MustExec("use test")
//	.          .     33:   tk.MustExec("drop table if exists tai")
//	.          .     34:   tk.MustExec("create table tai(a varchar(256), b varchar(256), c int as (a+1), d int as (b+1))")
//	.          .     35:   is := domain.GetDomain(tk.Session()).InfoSchema()
//	.          .     36:   _, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("tai"))
//	.          .     37:   require.NoError(b, err)
//	.          .     38:   condition := "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     39:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     40:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     41:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     42:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     43:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     44:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     45:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     46:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     47:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     48:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     49:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     50:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     51:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     52:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     53:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     54:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     55:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     56:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     57:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     58:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     59:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     60:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     61:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     62:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     63:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     64:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     65:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     66:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     67:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     68:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     69:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     70:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     71:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     72:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     73:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     74:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     75:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     76:           "(tai.a='%s' AND tai.b='%s') OR" +
//	.          .     77:           "(tai.a='%s' AND tai.b='%s')q"
//	.          .     78:   addresses := make([]interface{}, 0, 90)
//	.          .     79:   for i := 0; i < 80; i++ {
//	.          .     80:           addresses = append(addresses, "0x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC51")
//	.          .     81:   }
//	.          .     82:   condition = fmt.Sprintf(condition, addresses...)
//	.   520.04kB     83:   s := core.CreatePlannerSuite(tk.Session(), is)
//	.          .     84:   ctx := context.Background()
//	.          .     85:   sql := "select * from tai where " + condition
//	.          .     86:   fmt.Println(sql)
//	.          .     87:   stmt, err := s.GetParser().ParseOneStmt(sql, "", "")
//	.          .     88:   require.NoError(b, err, sql)
//	.   512.07kB     89:   p, err := core.BuildLogicalPlanForTest(ctx, s.GetCtx(), stmt, s.GetIS())
//	.          .     90:   require.NoError(b, err)
//	.          .     91:   selection := p.(core.LogicalPlan).Children()[0]
//	.          .     92:   m := make(core.ExprColumnMap, len(selection.Schema().Columns))
//	.          .     93:   for _, col := range selection.Schema().Columns {
//	.          .     94:           if col.VirtualExpr != nil {
//
// (pprof)
// (pprof) q
//
// We couldn't see any allocation around core.SubstituteExpression after this patch is applied when there is no generated expression substitution happened
// in the recursive dfs down. In the real environment, the contract hash address will be more complicated and embedding layer will be deeper, causing
// the more memory consumption rather than just a few 2MB as shown above.
//
// Expression hashcode is a lazy utility used for comparison in some cases, if the later usage is not exist, the re-computation of them here is also unnecessary.
func BenchmarkSubstituteExpression(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tai")
	tk.MustExec("create table tai(a varchar(256), b varchar(256), c int as (a+1), d int as (b+1))")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	_, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tai"))
	require.NoError(b, err)
	condition := "(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s') OR" +
		"(tai.a='%s' AND tai.b='%s')"
	addresses := make([]any, 0, 90)
	for range 80 {
		addresses = append(addresses, "0x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC510x6ab6Bf9117A8A9dd5a2FF203aa8a22457162fC51")
	}
	condition = fmt.Sprintf(condition, addresses...)
	s := coretestsdk.CreatePlannerSuite(tk.Session(), is)
	defer s.Close()
	ctx := context.Background()
	sql := "select * from tai where " + condition
	fmt.Println(sql)
	stmt, err := s.GetParser().ParseOneStmt(sql, "", "")
	require.NoError(b, err, sql)
	nodeW := resolve.NewNodeW(stmt)
	p, err := core.BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
	require.NoError(b, err)
	selection := p.(base.LogicalPlan).Children()[0]
	m := make(core.ExprColumnMap, len(selection.Schema().Columns))
	for _, col := range selection.Schema().Columns {
		if col.VirtualExpr != nil {
			m[col.VirtualExpr] = col
		}
	}
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		core.SubstituteExpression(selection.(*logicalop.LogicalSelection).Conditions[0], selection, m, selection.Schema())
	}
	b.StopTimer()
}
