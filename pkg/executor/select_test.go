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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func BenchmarkResetContextOfStmt(b *testing.B) {
	stmt := &ast.SelectStmt{}
	ctx := mock.NewContext()
	ctx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	for i := 0; i < b.N; i++ {
		executor.ResetContextOfStmt(ctx, stmt)
	}
}

func TestImportIntoShouldHaveSameFlagsAsInsert(t *testing.T) {
	insertStmt := &ast.InsertStmt{}
	importStmt := &ast.ImportIntoStmt{}
	insertCtx := mock.NewContext()
	importCtx := mock.NewContext()
	insertCtx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	importCtx.BindDomainAndSchValidator(&domain.Domain{}, nil)
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

func TestYearComparisonTiKV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_allow_mpp = 0")
	tk.MustExec("create table t0(c0 year not null, index i0(c0))")
	tk.MustExec("insert into t0 values (1935), (1982)")
	tk.MustQuery("select /*+ read_from_storage(tikv[t0]) */ c0 from t0 where 0.025 <= c0").
		Sort().Check(testkit.Rows("1935", "1982"))

	tk.MustExec("truncate table t0")
	tk.MustExec("insert into t0 values (0), (1901), (1935), (1982), (2000), (2001), (2155)")
	tests := []struct {
		op       string
		constant string
	}{
		{">=", "-0.025"},
		{">=", "0.025"},
		{">", "0.025"},
		{"<=", "0.025"},
		{"<", "0.025"},
		{">=", "69.5"},
		{"<=", "1935.5"},
		{">", "1935.5"},
		{">=", "2155.5"},
		{">=", "2.5e-2"},
		{"<=", "1935.25e0"},
		{">=", "1935.000"},
	}
	for _, tt := range tests {
		indexedSQL := fmt.Sprintf("select /*+ use_index(t0, i0) */ c0 from t0 where c0 %s %s order by c0", tt.op, tt.constant)
		referenceSQL := fmt.Sprintf("select c0 from t0 where cast(c0 as decimal(10, 3)) %s %s order by c0", tt.op, tt.constant)
		require.Equal(t, tk.MustQuery(referenceSQL).Rows(), tk.MustQuery(indexedSQL).Rows(), indexedSQL)
	}
}
