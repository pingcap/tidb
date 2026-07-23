// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func BenchmarkResetContextOfStmt(b *testing.B) {
	const pointSelectSQL = "SELECT c FROM sbtest1 WHERE id = ?"

	parsePointSelect := func(b *testing.B) ast.StmtNode {
		stmt, err := parser.New().ParseOneStmt(pointSelectSQL, "", "")
		require.NoError(b, err)
		return stmt
	}
	preparedPointSelect := func(b *testing.B, stmtText string) ast.StmtNode {
		stmt := parsePointSelect(b)
		normalizedSQL, digest := parser.NormalizeDigest(pointSelectSQL)
		return &ast.ExecuteStmt{
			PrepStmt: &plannercore.PlanCacheStmt{
				PreparedAst:   &ast.Prepared{Stmt: stmt},
				NormalizedSQL: normalizedSQL,
				SQLDigest:     digest,
				StmtText:      stmtText,
			},
		}
	}

	cases := []struct {
		name      string
		stmt      func(*testing.B) ast.StmtNode
		configure func(*variable.SessionVars)
	}{
		{
			name: "prepared-point-select",
			stmt: func(b *testing.B) ast.StmtNode {
				return preparedPointSelect(b, pointSelectSQL)
			},
		},
		{
			name: "unprepared-point-select",
			stmt: parsePointSelect,
		},
		{
			name: "retry-prepared-point-select",
			stmt: func(b *testing.B) ast.StmtNode {
				return preparedPointSelect(b, pointSelectSQL)
			},
			configure: func(vars *variable.SessionVars) {
				vars.TxnCtx.CouldRetry = true
			},
		},
		{
			name: "cursor-prepared-point-select",
			stmt: func(b *testing.B) ast.StmtNode {
				return preparedPointSelect(b, pointSelectSQL)
			},
			configure: func(vars *variable.SessionVars) {
				vars.SetStatusFlag(mysql.ServerStatusCursorExists, true)
			},
		},
		{
			name: "prepared-empty-stmt-text",
			stmt: func(b *testing.B) ast.StmtNode {
				return preparedPointSelect(b, "")
			},
		},
	}

	for _, testCase := range cases {
		b.Run(testCase.name, func(b *testing.B) {
			ctx := mock.NewContext()
			ctx.BindDomain(&domain.Domain{})
			stmt := testCase.stmt(b)
			if testCase.configure != nil {
				testCase.configure(ctx.GetSessionVars())
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := executor.ResetContextOfStmt(ctx, stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestImportIntoShouldHaveSameFlagsAsInsert(t *testing.T) {
	insertStmt := &ast.InsertStmt{}
	importStmt := &ast.ImportIntoStmt{}
	insertCtx := mock.NewContext()
	importCtx := mock.NewContext()
	insertCtx.BindDomain(&domain.Domain{})
	importCtx.BindDomain(&domain.Domain{})
	for _, modeStr := range []string{
		"",
		"IGNORE_SPACE",
		"STRICT_TRANS_TABLES",
		"STRICT_ALL_TABLES",
		"ALLOW_INVALID_DATES",
		"NO_ZERO_IN_DATE",
		"NO_ZERO_DATE",
		"NO_ZERO_IN_DATE,STRICT_ALL_TABLES",
		"NO_ZERO_DATE,STRICT_ALL_TABLES",
		"NO_ZERO_IN_DATE,NO_ZERO_DATE,STRICT_ALL_TABLES",
	} {
		t.Run(fmt.Sprintf("mode %s", modeStr), func(t *testing.T) {
			mode, err := mysql.GetSQLMode(modeStr)
			require.NoError(t, err)
			insertCtx.GetSessionVars().SQLMode = mode
			require.NoError(t, executor.ResetContextOfStmt(insertCtx, insertStmt))
			importCtx.GetSessionVars().SQLMode = mode
			require.NoError(t, executor.ResetContextOfStmt(importCtx, importStmt))

			insertTypeCtx := insertCtx.GetSessionVars().StmtCtx.TypeCtx()
			importTypeCtx := importCtx.GetSessionVars().StmtCtx.TypeCtx()
			require.EqualValues(t, insertTypeCtx.Flags(), importTypeCtx.Flags())
		})
	}
}
