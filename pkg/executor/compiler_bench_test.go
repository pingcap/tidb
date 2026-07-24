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

package executor_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"go.uber.org/zap"
)

var compilerBenchmarkResult *executor.ExecStmt

func BenchmarkCompilerCompilePreparedPointSelect(b *testing.B) {
	gomaxprocs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(gomaxprocs)

	logLevel := log.GetLevel()
	log.SetLevel(zap.FatalLevel)
	b.Cleanup(func() {
		log.SetLevel(logLevel)
	})

	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("USE test")
	tk.MustExec("SET GLOBAL tidb_schema_cache_size = 536870912")
	tk.MustExec("CREATE TABLE t (id BIGINT PRIMARY KEY, c INT)")

	stmtID, _, _, err := tk.Session().PrepareStmt("SELECT c FROM t WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	prepared, err := tk.Session().GetSessionVars().GetPreparedStmtByID(stmtID)
	if err != nil {
		b.Fatal(err)
	}
	stmt := &ast.ExecuteStmt{
		BinaryArgs: expression.Args2Expressions4Test(1),
		PrepStmt:   prepared,
	}
	compiler := executor.Compiler{Ctx: tk.Session()}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		compilerBenchmarkResult, err = compiler.Compile(ctx, stmt)
		if err != nil {
			b.Fatal(err)
		}
	}
}
