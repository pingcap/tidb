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

package core_test

import (
	"context"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type preprocessBenchmarkCase struct {
	nodeW *resolve.NodeW
	ret   *core.PreprocessorReturn
	opts  []core.PreprocessOpt
}

func newPreprocessBenchmarkCase(node ast.Node) *preprocessBenchmarkCase {
	ret := &core.PreprocessorReturn{}
	return &preprocessBenchmarkCase{
		nodeW: resolve.NewNodeW(node),
		ret:   ret,
		opts: []core.PreprocessOpt{
			core.WithPreprocessorReturn(ret),
			core.InitTxnContextProvider,
		},
	}
}

func (testCase *preprocessBenchmarkCase) run(ctx context.Context, sctx sessionctx.Context) error {
	*testCase.ret = core.PreprocessorReturn{}
	return core.Preprocess(ctx, sctx, testCase.nodeW, testCase.opts...)
}

func parsePreprocessBenchmarkStmt(tb testing.TB, sctx sessionctx.Context, sql string) ast.StmtNode {
	stmts, err := session.Parse(sctx, sql)
	require.NoError(tb, err)
	require.Len(tb, stmts, 1)
	return stmts[0]
}

func preparePointSelectExecuteStmt(tb testing.TB, tk *testkit.TestKit) *ast.ExecuteStmt {
	stmtID, _, _, err := tk.Session().PrepareStmt("SELECT c FROM t WHERE id = ?")
	require.NoError(tb, err)
	prepared, err := tk.Session().GetSessionVars().GetPreparedStmtByID(stmtID)
	require.NoError(tb, err)
	return &ast.ExecuteStmt{PrepStmt: prepared}
}

func BenchmarkPreprocessExecute(b *testing.B) {
	logLevel := log.GetLevel()
	log.SetLevel(zap.FatalLevel)
	b.Cleanup(func() {
		log.SetLevel(logLevel)
	})

	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("USE test")
	tk.MustExec("CREATE TABLE t (id BIGINT PRIMARY KEY, c INT)")

	sctx := tk.Session()
	testCases := []struct {
		name     string
		testCase *preprocessBenchmarkCase
	}{
		{
			name:     "prepared-point-select",
			testCase: newPreprocessBenchmarkCase(preparePointSelectExecuteStmt(b, tk)),
		},
		{
			name:     "readonly-user-var",
			testCase: newPreprocessBenchmarkCase(parsePreprocessBenchmarkStmt(b, sctx, "SELECT @a")),
		},
		{
			name:     "mutable-then-read",
			testCase: newPreprocessBenchmarkCase(parsePreprocessBenchmarkStmt(b, sctx, "SELECT @a := 1, @a")),
		},
		{
			name:     "read-then-mutable",
			testCase: newPreprocessBenchmarkCase(parsePreprocessBenchmarkStmt(b, sctx, "SELECT @a, @a := 1")),
		},
	}
	ctx := context.Background()

	for _, benchmarkCase := range testCases {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := benchmarkCase.testCase.run(ctx, sctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPreprocessUserVariableTracking(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	sctx := tk.Session()

	testCases := []struct {
		sql      string
		readonly []string
	}{
		{sql: "SELECT 1"},
		{sql: "SELECT @a", readonly: []string{"a"}},
		{sql: "SELECT @a := 1, @a"},
		{sql: "SELECT @a, @a := 1"},
		{sql: "SELECT @a, @b := 1", readonly: []string{"a"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			benchmarkCase := newPreprocessBenchmarkCase(parsePreprocessBenchmarkStmt(t, sctx, testCase.sql))
			require.NoError(t, benchmarkCase.run(context.Background(), sctx))

			readonly := sctx.GetPlanCtx().GetReadonlyUserVarMap()
			require.Len(t, readonly, len(testCase.readonly))
			for _, name := range testCase.readonly {
				require.Contains(t, readonly, name)
			}
		})
	}
}

func TestPreprocessPreparedPointSelectRepeated(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("CREATE TABLE t (id BIGINT PRIMARY KEY, c INT)")

	testCase := newPreprocessBenchmarkCase(preparePointSelectExecuteStmt(t, tk))
	for range 3 {
		require.NoError(t, testCase.run(context.Background(), tk.Session()))
		require.Empty(t, tk.Session().GetPlanCtx().GetReadonlyUserVarMap())
	}
}

func TestPreprocessWithReturnPreparedPointSelectRepeated(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("CREATE TABLE t (id BIGINT PRIMARY KEY, c INT)")

	nodeW := resolve.NewNodeW(preparePointSelectExecuteStmt(t, tk))
	for range 3 {
		var ret core.PreprocessorReturn
		require.NoError(t, core.PreprocessWithReturn(
			context.Background(),
			tk.Session(),
			nodeW,
			&ret,
			core.InitTxnContextProvider,
		))
		require.NotNil(t, ret.InfoSchema)
		require.Empty(t, tk.Session().GetPlanCtx().GetReadonlyUserVarMap())
	}
}
